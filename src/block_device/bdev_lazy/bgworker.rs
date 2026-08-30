use super::{
    ingest_pool::{IngestPool, IngestPoolConfig, IngestRequest},
    metadata::shared_state::SharedMetadataState,
    metadata_flusher::MetadataFlusher,
    push_gate::PushPermit,
    stripe_fetcher::StripeFetcher,
};

use crate::{
    block_device::BlockDevice,
    stripe_source::{StripeSource, StripeSourceBuilder},
    Result,
};
use log::{error, info};
use std::sync::{
    mpsc::{Receiver, Sender, TryRecvError},
    Arc,
};

pub enum BgWorkerRequest {
    Fetch {
        stripe_id: usize,
    },
    /// A stripe the snapshot server pushed to this fork. The permit is the
    /// subscriber's slot, released once this request has been handled, so the
    /// fork stops reading pushes it cannot keep up with.
    PushedStripe {
        stripe_id: usize,
        data: Vec<u8>,
        permit: PushPermit,
    },
    SetWritten {
        stripe_id: usize,
    },
    /// An ingest worker finished with a stripe. Only the coordinator touches
    /// metadata, so the workers hand their results back rather than writing it.
    FetchCompleted {
        stripe_id: usize,
        success: bool,
    },
    Shutdown,
}

/// Where stripes are taken in: on this thread, or on a pool of them.
enum Ingest {
    /// One fetcher, driven by the coordinator's own loop.
    Inline(Box<StripeFetcher>),
    /// Several, each on its own thread with its own range of the device. They
    /// report finished stripes back as `FetchCompleted`.
    Pool(IngestPool),
}

pub struct BgWorker {
    ingest: Ingest,
    metadata_flusher: MetadataFlusher,
    req_receiver: Receiver<BgWorkerRequest>,
    metadata_state: SharedMetadataState,
    done: bool,
}

impl BgWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stripe_source: Box<dyn StripeSource>,
        target_dev: &dyn BlockDevice,
        metadata_dev: &dyn BlockDevice,
        alignment: usize,
        autofetch: bool,
        expects_pushes: bool,
        metadata_state: SharedMetadataState,
        req_receiver: Receiver<BgWorkerRequest>,
    ) -> Result<Self> {
        let source_sector_count = stripe_source.sector_count();
        let metadata_flusher =
            MetadataFlusher::new(metadata_dev, source_sector_count, metadata_state.clone())?;
        let mut stripe_fetcher = StripeFetcher::new(
            stripe_source,
            target_dev,
            metadata_state.stripe_sector_count(),
            metadata_state.clone(),
            alignment,
            autofetch,
        )?;
        stripe_fetcher.set_expects_pushes(expects_pushes);
        Ok(BgWorker {
            ingest: Ingest::Inline(Box::new(stripe_fetcher)),
            metadata_flusher,
            req_receiver,
            done: false,
            metadata_state,
        })
    }

    /// A coordinator whose stripes are taken in by a pool of worker threads
    /// instead of by this one. `completions` must be a sender on this worker's
    /// own channel, which is how the pool reports what it finished.
    #[allow(clippy::too_many_arguments)]
    pub fn with_ingest_pool(
        target_dev: Arc<dyn BlockDevice>,
        stripe_source_builder: StripeSourceBuilder,
        metadata_dev: &dyn BlockDevice,
        alignment: usize,
        autofetch: bool,
        expects_pushes: bool,
        metadata_state: SharedMetadataState,
        req_receiver: Receiver<BgWorkerRequest>,
        completions: Sender<BgWorkerRequest>,
        workers: usize,
        connections: usize,
    ) -> Result<Self> {
        // Size the source before starting anything, so a device that cannot
        // hold its source is reported as that rather than as whichever worker
        // noticed first. One connection, dropped immediately.
        let source_sector_count = stripe_source_builder
            .build_with_connections(Some(1))?
            .sector_count();
        let metadata_flusher =
            MetadataFlusher::new(metadata_dev, source_sector_count, metadata_state.clone())?;

        let (pool, _) = IngestPool::new(IngestPoolConfig {
            target_dev,
            stripe_source_builder,
            alignment,
            autofetch,
            expects_pushes,
            shared_state: metadata_state.clone(),
            workers,
            connections,
            completions,
        })?;

        Ok(BgWorker {
            ingest: Ingest::Pool(pool),
            metadata_flusher,
            req_receiver,
            done: false,
            metadata_state,
        })
    }

    pub fn shared_state(&self) -> SharedMetadataState {
        self.metadata_state.clone()
    }

    pub fn process_request(&mut self, req: BgWorkerRequest) {
        match req {
            BgWorkerRequest::Fetch { stripe_id } => match &mut self.ingest {
                Ingest::Inline(fetcher) => fetcher.handle_fetch_request(stripe_id),
                Ingest::Pool(pool) => pool.send(stripe_id, IngestRequest::Fetch { stripe_id }),
            },
            BgWorkerRequest::PushedStripe {
                stripe_id,
                data,
                permit,
            } => match &mut self.ingest {
                Ingest::Inline(fetcher) => fetcher.accept_pushed_stripe(stripe_id, &data, permit),
                Ingest::Pool(pool) => pool.send(
                    stripe_id,
                    IngestRequest::Pushed {
                        stripe_id,
                        data,
                        permit,
                    },
                ),
            },
            BgWorkerRequest::SetWritten { stripe_id } => {
                self.metadata_flusher.set_stripe_written(stripe_id)
            }
            BgWorkerRequest::FetchCompleted { stripe_id, success } => {
                if success {
                    self.metadata_flusher.set_stripe_fetched(stripe_id);
                } else {
                    error!("Stripe {stripe_id} fetch failed");
                }
            }
            BgWorkerRequest::Shutdown => {
                info!("Received shutdown request, stopping worker");
                self.done = true;
            }
        }
    }

    pub fn receive_requests(&mut self, block: bool) {
        if block {
            match self.req_receiver.recv() {
                Ok(req) => self.process_request(req),
                Err(e) => {
                    error!("Failed to receive request: {e}, stopping worker");
                    self.done = true;
                    return;
                }
            }
        }

        loop {
            match self.req_receiver.try_recv() {
                Ok(req) => self.process_request(req),
                Err(TryRecvError::Disconnected) => {
                    error!("Request channel disconnected, stopping worker");
                    self.done = true;
                    return;
                }
                Err(TryRecvError::Empty) => break,
            }
        }
    }

    pub fn update(&mut self) {
        if let Ingest::Inline(fetcher) = &mut self.ingest {
            fetcher.update();
            for (stripe_id, success) in fetcher.take_finished_fetches() {
                if success {
                    self.metadata_flusher.set_stripe_fetched(stripe_id);
                } else {
                    error!("Stripe {stripe_id} fetch failed");
                }
            }
        }
        self.metadata_flusher.update();
        if let Ingest::Inline(fetcher) = &mut self.ingest {
            fetcher.disconnect_from_source_if_all_fetched();
        }
    }

    pub fn run(&mut self) {
        while !self.done {
            // With a pool the workers block on their own queues, so this thread
            // has nothing to spin for: it waits for a request or a completion.
            let busy = match &self.ingest {
                Ingest::Inline(fetcher) => fetcher.busy(),
                Ingest::Pool(_) => false,
            } || self.metadata_flusher.busy();
            self.receive_requests(!busy);
            self.update();
        }

        if let Ingest::Pool(pool) = &mut self.ingest {
            pool.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        block_device::{
            bdev_lazy::SharedMetadataState, bdev_test::TestBlockDevice, NullBlockDevice,
            UbiMetadata,
        },
        stripe_source,
    };
    use std::sync::mpsc::channel;

    fn build_bg_worker_with_source(
        stripe_source: Box<dyn StripeSource>,
    ) -> (BgWorker, std::sync::mpsc::Sender<BgWorkerRequest>) {
        let stripe_sector_count_shift = 11;
        let target_dev = TestBlockDevice::new(1024 * 1024);
        let metadata_dev = TestBlockDevice::new(1024 * 1024);
        let metadata = UbiMetadata::new(stripe_sector_count_shift, 16, 16);
        metadata.save_to_bdev(&metadata_dev).unwrap();
        let metadata_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };

        let (tx, rx) = channel();

        (
            BgWorker::new(
                stripe_source,
                &target_dev,
                &metadata_dev,
                4096,
                false,
                false,
                metadata_state,
                rx,
            )
            .unwrap(),
            tx,
        )
    }

    fn build_bg_worker() -> (BgWorker, std::sync::mpsc::Sender<BgWorkerRequest>) {
        let stripe_sector_count_shift = 11;
        let stripe_sector_count = 1u64 << stripe_sector_count_shift;
        let source_dev = TestBlockDevice::new(1024 * 1024);
        let stripe_source = Box::new(
            stripe_source::BlockDeviceStripeSource::new(source_dev.clone(), stripe_sector_count)
                .unwrap(),
        );
        build_bg_worker_with_source(stripe_source)
    }

    #[test]
    fn test_bg_worker_shutdown() {
        let (mut bg_worker, sender) = build_bg_worker();
        sender.send(BgWorkerRequest::Shutdown).unwrap();
        bg_worker.run();
    }

    #[test]
    fn bg_worker_supports_null_source() {
        let stripe_sector_count_shift = 11;
        let stripe_sector_count = 1u64 << stripe_sector_count_shift;
        let source_dev = NullBlockDevice::new();
        let target_dev = TestBlockDevice::new(1024 * 1024);
        let metadata_dev = TestBlockDevice::new(1024 * 1024);
        let stripe_source = Box::new(
            stripe_source::BlockDeviceStripeSource::new(source_dev, stripe_sector_count).unwrap(),
        );

        let metadata = UbiMetadata::new(stripe_sector_count_shift, 16, 0);
        metadata.save_to_bdev(&metadata_dev).unwrap();

        let metadata_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };

        let (_tx, rx) = channel();

        BgWorker::new(
            stripe_source,
            &target_dev,
            &metadata_dev,
            4096,
            false,
            false,
            metadata_state,
            rx,
        )
        .expect("BgWorker should support null source device");
    }

    #[test]
    fn bg_worker_marks_failed_stripes_with_flaky_source() {
        let stripe_sector_count_shift = 11;
        let stripe_sector_count = 1u64 << stripe_sector_count_shift;
        let source_dev = TestBlockDevice::new(1024 * 1024);
        let base_source =
            stripe_source::BlockDeviceStripeSource::new(source_dev.clone(), stripe_sector_count)
                .unwrap();
        let flaky_source =
            stripe_source::FlakyStripeSource::new(Box::new(base_source), vec![(0, 4)]);

        let (mut bg_worker, sender) = build_bg_worker_with_source(Box::new(flaky_source));
        sender
            .send(BgWorkerRequest::Fetch { stripe_id: 0 })
            .unwrap();
        bg_worker.receive_requests(false);

        for _ in 0..100 {
            bg_worker.update();
        }

        assert!(bg_worker.shared_state().is_stripe_failed(0));
    }
}
