use super::{
    ingest_pool::{IngestPool, IngestPoolConfig, IngestRequest},
    metadata::shared_state::{stripe_flags, Evicted, SharedMetadataState},
    metadata_flusher::{MetadataFlusher, PersistOutcome, PersistResult},
    push_gate::PushPermit,
    spill::{Evictor, FetchDisposition, PushDisposition},
    stripe_fetcher::StripeFetcher,
};

#[cfg(feature = "fault-injection")]
use super::spill::evictor::CrashPoint;

use crate::{
    block_device::{metadata_flags, BlockDevice},
    stripe_source::{StripeSource, StripeSourceBuilder},
    Result,
};
use log::{debug, error, info, warn};
use std::{
    collections::HashMap,
    sync::{
        mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError},
        Arc,
    },
    time::Duration,
};

/// How long the coordinator waits for a request when an evictor exists and
/// nothing is busy. The evictor has deadlines of its own (statfs refresh,
/// upload backoff), so the loop must come round without a request.
const EVICTOR_IDLE_WAIT: Duration = Duration::from_millis(250);

/// Re-issues of a re-materialised stripe's EVICTED-clearing header before the
/// coordinator gives up and leaves the stripe Evicted for the next fetch.
const MAX_RELEASE_RETRIES: u8 = 3;

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

/// A re-materialised stripe whose EVICTED-clearing header is in the flusher.
/// The guest may not see it as resident before that header is durable: the
/// startup pass punches every stripe whose header says EVICTED.
struct PendingRelease {
    stripe_id: usize,
    retries: u8,
}

pub struct BgWorker {
    ingest: Ingest,
    metadata_flusher: MetadataFlusher,
    req_receiver: Receiver<BgWorkerRequest>,
    metadata_state: SharedMetadataState,
    /// Present when `[spill]` is configured.
    evictor: Option<Evictor>,
    /// Evicted stripes whose data has landed and whose EVICTED-clearing header
    /// is in the flusher, keyed by even token.
    pending_release: HashMap<u64, PendingRelease>,
    /// Even tokens are the coordinator's; odd ones the evictor's.
    next_release_token: u64,
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
        evictor: Option<Evictor>,
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
        stripe_fetcher.set_never_disconnect(evictor.is_some());
        Ok(BgWorker {
            ingest: Ingest::Inline(Box::new(stripe_fetcher)),
            metadata_flusher,
            req_receiver,
            metadata_state,
            evictor,
            pending_release: HashMap::new(),
            next_release_token: 2,
            done: false,
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
        evictor: Option<Evictor>,
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
            never_disconnect: evictor.is_some(),
        })?;

        Ok(BgWorker {
            ingest: Ingest::Pool(pool),
            metadata_flusher,
            req_receiver,
            metadata_state,
            evictor,
            pending_release: HashMap::new(),
            next_release_token: 2,
            done: false,
        })
    }

    pub fn shared_state(&self) -> SharedMetadataState {
        self.metadata_state.clone()
    }

    pub fn process_request(&mut self, req: BgWorkerRequest) {
        match req {
            BgWorkerRequest::Fetch { stripe_id } => {
                if self
                    .pending_release
                    .values()
                    .any(|pending| pending.stripe_id == stripe_id)
                {
                    // The data is local and the header clearing EVICTED is in
                    // flight; the channel's re-send finds the stripe resident
                    // once that header is durable.
                    debug!("Fetch for stripe {stripe_id} dropped: its release is pending");
                    return;
                }
                if let Some(evictor) = &mut self.evictor {
                    match evictor.on_fetch_request(stripe_id) {
                        FetchDisposition::Forward => {}
                        FetchDisposition::Aborted
                        | FetchDisposition::Deferred
                        | FetchDisposition::HeldForSpace
                        | FetchDisposition::Refused => return,
                    }
                }
                self.route_fetch(stripe_id);
            }
            BgWorkerRequest::PushedStripe {
                stripe_id,
                data,
                permit,
            } => {
                if self.evictor.is_some() {
                    // Recorded for every push, whatever becomes of it: the
                    // server refuses a re-pull after copy-out regardless of
                    // who holds the stripe, so it is dirty from now on.
                    self.metadata_state
                        .set_stripe_flags(stripe_id, stripe_flags::PUSHED);
                    self.metadata_flusher
                        .update_stripe_header(stripe_id, metadata_flags::PUSHED, 0, 0);
                }
                let permit = match &mut self.evictor {
                    None => permit,
                    Some(evictor) => match evictor.on_pushed_stripe(stripe_id, &data, permit) {
                        (PushDisposition::Forward, Some(permit)) => permit,
                        _ => return,
                    },
                };
                self.route_push(stripe_id, data, permit);
            }
            BgWorkerRequest::SetWritten { stripe_id } => {
                self.metadata_flusher.set_stripe_written(stripe_id)
            }
            BgWorkerRequest::FetchCompleted { stripe_id, success } => {
                self.stripe_landed(stripe_id, success)
            }
            BgWorkerRequest::Shutdown => {
                info!("Received shutdown request, stopping worker");
                self.done = true;
            }
        }
    }

    fn route_fetch(&mut self, stripe_id: usize) {
        match &mut self.ingest {
            Ingest::Inline(fetcher) => fetcher.handle_fetch_request(stripe_id),
            Ingest::Pool(pool) => pool.send(stripe_id, IngestRequest::Fetch { stripe_id }),
        }
    }

    fn route_push(&mut self, stripe_id: usize, data: Vec<u8>, permit: PushPermit) {
        match &mut self.ingest {
            Ingest::Inline(fetcher) => fetcher.accept_pushed_stripe(stripe_id, &data, permit),
            Ingest::Pool(pool) => pool.send(
                stripe_id,
                IngestRequest::Pushed {
                    stripe_id,
                    data,
                    permit,
                },
            ),
        }
    }

    /// The ingest finished with a stripe. A stripe that was not evicted lands
    /// at once; an evicted one (or a Failed one whose header still says
    /// EVICTED) is released only when the header clearing EVICTED is durable,
    /// because the startup pass punches every stripe whose header says so.
    fn stripe_landed(&mut self, stripe_id: usize, success: bool) {
        if !success {
            error!("Stripe {stripe_id} fetch failed");
            return;
        }
        let state = self.metadata_state.stripe_fetch_state(stripe_id);
        if state == Evicted
            || self.metadata_state.stripe_flags(stripe_id) & stripe_flags::WAS_EVICTED != 0
        {
            let token = self.next_release_token;
            self.next_release_token += 2;
            self.pending_release
                .insert(token, PendingRelease { stripe_id, retries: 0 });
            self.metadata_flusher.update_stripe_header(
                stripe_id,
                metadata_flags::FETCHED,
                metadata_flags::EVICTED,
                token,
            );
            #[cfg(feature = "fault-injection")]
            if let Some(evictor) = &self.evictor {
                evictor.crash_if_at(CrashPoint::DuringRefetch);
            }
        } else {
            // Unblocks anything waiting on this stripe now, rather than once
            // the flusher has worked through the sweep's backlog.
            self.metadata_state.mark_stripe_fetched(stripe_id);
            self.metadata_flusher.set_stripe_fetched(stripe_id);
        }
    }

    /// The flusher finished a release op. Durable lands the stripe; anything
    /// else is retried a few times with a fresh token and then given up on,
    /// leaving the stripe Evicted for the guest's next fetch to re-write.
    fn apply_release_outcome(&mut self, outcome: &PersistOutcome) {
        let Some(pending) = self.pending_release.remove(&outcome.token) else {
            error!(
                "Release outcome for stripe {} with unknown token {}",
                outcome.stripe_id, outcome.token
            );
            return;
        };
        let stripe_id = pending.stripe_id;
        match outcome.result {
            PersistResult::Durable => {
                debug!("Stripe {stripe_id} re-materialised");
                self.metadata_state.mark_stripe_resident(stripe_id);
            }
            result if pending.retries < MAX_RELEASE_RETRIES => {
                warn!("Release header for stripe {stripe_id} ended {result:?}; retrying");
                let token = self.next_release_token;
                self.next_release_token += 2;
                self.pending_release.insert(
                    token,
                    PendingRelease {
                        stripe_id,
                        retries: pending.retries + 1,
                    },
                );
                self.metadata_flusher.update_stripe_header(
                    stripe_id,
                    metadata_flags::FETCHED,
                    metadata_flags::EVICTED,
                    token,
                );
            }
            result => {
                error!(
                    "Release header for stripe {stripe_id} ended {result:?} after                      {MAX_RELEASE_RETRIES} retries; leaving it evicted"
                );
                self.metadata_state
                    .spill()
                    .degraded_reasons
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

        self.drain_requests();
    }

    /// `receive_requests` with a bound on the wait: `None` blocks as it does,
    /// `Some(wait)` gives up after that long so the evictor's deadlines are met
    /// even when no guest asks for anything.
    pub fn receive_requests_for(&mut self, wait: Option<Duration>) {
        let first = match wait {
            None => self
                .req_receiver
                .recv()
                .map_err(|_| RecvTimeoutError::Disconnected),
            Some(wait) => self.req_receiver.recv_timeout(wait),
        };
        match first {
            Ok(req) => self.process_request(req),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                error!("Request channel disconnected, stopping worker");
                self.done = true;
                return;
            }
        }

        self.drain_requests();
    }

    /// Everything already queued, without waiting.
    fn drain_requests(&mut self) {
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
        let finished = match &mut self.ingest {
            Ingest::Inline(fetcher) => {
                fetcher.update();
                fetcher.take_finished_fetches()
            }
            Ingest::Pool(_) => Vec::new(),
        };
        for (stripe_id, success) in finished {
            self.stripe_landed(stripe_id, success);
        }
        self.metadata_flusher.update();
        let outcomes = self.metadata_flusher.take_persist_outcomes();
        for outcome in outcomes
            .iter()
            .filter(|outcome| !Evictor::owns_token(outcome.token))
        {
            self.apply_release_outcome(outcome);
        }
        let released = self.evictor.as_mut().map(|evictor| {
            evictor.update(&mut self.metadata_flusher, &outcomes);
            evictor.take_released()
        });
        if let Some((fetches, pushes)) = released {
            for stripe_id in fetches {
                self.route_fetch(stripe_id);
            }
            for (stripe_id, data, permit) in pushes {
                self.route_push(stripe_id, data, permit);
            }
        }
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
            } || self.metadata_flusher.busy()
                || self.evictor.as_ref().is_some_and(Evictor::busy);
            if self.evictor.is_some() {
                let wait = if busy {
                    Duration::ZERO
                } else {
                    EVICTOR_IDLE_WAIT
                };
                self.receive_requests_for(Some(wait));
            } else {
                self.receive_requests(!busy);
            }
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
                None,
            )
            .unwrap(),
            tx,
        )
    }

    /// A coordinator with the stub evictor attached, so the evictor branches of
    /// the run loop are exercised.
    fn build_bg_worker_with_evictor() -> (BgWorker, std::sync::mpsc::Sender<BgWorkerRequest>) {
        use crate::{
            archive::ArchiveCompressionAlgorithm,
            block_device::spill::{EvictorConfig, RecordingPuncher, SpillCodec},
            config::v2::spill::OnFull,
        };

        let stripe_sector_count_shift = 11;
        let stripe_sector_count = 1u64 << stripe_sector_count_shift;
        let source_dev = TestBlockDevice::new(1024 * 1024);
        let stripe_source = Box::new(
            stripe_source::BlockDeviceStripeSource::new(source_dev.clone(), stripe_sector_count)
                .unwrap(),
        );
        let target_dev = TestBlockDevice::new(1024 * 1024);
        let metadata_dev = TestBlockDevice::new(1024 * 1024);
        let metadata = UbiMetadata::new(stripe_sector_count_shift, 16, 16);
        metadata.save_to_bdev(&metadata_dev).unwrap();
        let metadata_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let evictor = Evictor::new(
            EvictorConfig {
                data_path: "/tmp/device.raw".into(),
                device_id: "fork-1".to_string(),
                stripe_sector_count,
                target_sector_count: target_dev.sector_count(),
                max_local_bytes: 1 << 20,
                low_water_bytes: 4096,
                hard_margin_bytes: 4096,
                min_free_bytes: 4096,
                clean_eviction: false,
                on_full: OnFull::Stall,
                max_concurrent_evictions: 1,
                sweep_batch: 4096,
                alignment: 4096,
            },
            target_dev.create_channel().unwrap(),
            None,
            SpillCodec::new(ArchiveCompressionAlgorithm::None, None, stripe_sector_count),
            Box::new(RecordingPuncher::default()),
            metadata_state.clone(),
        )
        .unwrap();

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
                Some(evictor),
            )
            .unwrap(),
            tx,
        )
    }

    #[test]
    fn receive_requests_for_returns_after_the_wait() {
        let (mut bg_worker, sender) = build_bg_worker();

        let start = std::time::Instant::now();
        bg_worker.receive_requests_for(Some(Duration::from_millis(20)));
        assert!(start.elapsed() >= Duration::from_millis(20));
        assert!(!bg_worker.done);

        sender.send(BgWorkerRequest::Shutdown).unwrap();
        bg_worker.receive_requests_for(Some(Duration::from_secs(5)));
        assert!(bg_worker.done);

        // A dropped sender stops the worker rather than spinning on it.
        let (mut bg_worker, sender) = build_bg_worker();
        drop(sender);
        bg_worker.receive_requests_for(None);
        assert!(bg_worker.done);
    }

    #[test]
    fn run_loop_with_evictor_waits_and_shuts_down() {
        let (mut bg_worker, sender) = build_bg_worker_with_evictor();
        assert!(bg_worker.evictor.is_some());
        let stop = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            sender.send(BgWorkerRequest::Shutdown).unwrap();
        });
        bg_worker.run();
        stop.join().unwrap();
        assert!(bg_worker.done);
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
            None,
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
