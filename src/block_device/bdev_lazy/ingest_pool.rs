//! Several threads taking stripes in at once.
//!
//! One thread fetching, writing and flushing is a ceiling: at a datacenter's
//! round-trip times it leaves the link and the disk both idle waiting for it.
//! The pool splits the device into contiguous ranges and gives each worker its
//! own range, its own connections and its own channel to the target, so nothing
//! is shared and no two workers ever want the same stripe.
//!
//! Ranges are contiguous rather than striped so each worker writes its own
//! region in order, which is what a disk wants.
//!
//! Workers report finished stripes back to the coordinator, which owns the
//! metadata: the bookkeeping stays in one place and only the data path
//! multiplies.

use std::{
    sync::{
        mpsc::{channel, Receiver, Sender, TryRecvError},
        Arc,
    },
    thread::JoinHandle,
};

use log::{error, info};

use super::{push_gate::PushPermit, stripe_fetcher::StripeFetcher};
use crate::{
    block_device::{BgWorkerRequest, BlockDevice, SharedMetadataState},
    stripe_source::StripeSourceBuilder,
    Result,
};

pub enum IngestRequest {
    Fetch {
        stripe_id: usize,
    },
    Pushed {
        stripe_id: usize,
        data: Vec<u8>,
        permit: PushPermit,
    },
}

pub struct IngestPool {
    senders: Vec<Sender<IngestRequest>>,
    handles: Vec<JoinHandle<()>>,
    /// First stripe of each worker's range, so a request can be routed to the
    /// worker that owns it.
    range_starts: Vec<usize>,
}

pub struct IngestPoolConfig {
    pub target_dev: Arc<dyn BlockDevice>,
    pub stripe_source_builder: StripeSourceBuilder,
    pub alignment: usize,
    pub autofetch: bool,
    pub expects_pushes: bool,
    pub shared_state: SharedMetadataState,
    pub workers: usize,
    pub connections: usize,
    pub completions: Sender<BgWorkerRequest>,
    /// Keep the source once everything is fetched: with spill, an evicted
    /// clean stripe is re-pulled from it.
    pub never_disconnect: bool,
}

impl IngestPool {
    /// Starts the workers and reports the source's size along with the pool:
    /// the coordinator validates its metadata against it, and only the workers
    /// ever build a source.
    pub fn new(config: IngestPoolConfig) -> Result<(Self, u64)> {
        let stripe_count = config.shared_state.stripe_count();
        let workers = config.workers.clamp(1, stripe_count.max(1));
        let per_worker_connections = (config.connections / workers).max(1);

        let mut source_sector_count = None;
        let mut senders = Vec::with_capacity(workers);
        let mut handles = Vec::with_capacity(workers);
        let mut range_starts = Vec::with_capacity(workers);

        for index in 0..workers {
            let start = stripe_count * index / workers;
            let end = stripe_count * (index + 1) / workers;
            range_starts.push(start);

            let (request_tx, request_rx) = channel();
            let (ready_tx, ready_rx) = channel();

            let worker = IngestWorker {
                index,
                start,
                end,
                target_dev: config.target_dev.clone(),
                stripe_source_builder: config.stripe_source_builder.clone(),
                alignment: config.alignment,
                autofetch: config.autofetch,
                expects_pushes: config.expects_pushes,
                shared_state: config.shared_state.clone(),
                connections: per_worker_connections,
                completions: config.completions.clone(),
                requests: request_rx,
                never_disconnect: config.never_disconnect,
            };

            let handle = std::thread::Builder::new()
                .name(format!("ingest-{index}"))
                .spawn(move || worker.run(ready_tx))
                .map_err(|e| crate::ubiblk_error!(ThreadCreation { source: e }))?;

            match ready_rx.recv() {
                Ok(Ok(sector_count)) => source_sector_count = Some(sector_count),
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(crate::ubiblk_error!(InvalidParameter {
                        description: format!("ingest worker {index} died before it started"),
                    }))
                }
            }

            senders.push(request_tx);
            handles.push(handle);
        }

        info!(
            "Ingest pool started with {workers} worker(s), {per_worker_connections} connection(s) each"
        );
        Ok((
            Self {
                senders,
                handles,
                range_starts,
            },
            source_sector_count.unwrap_or(0),
        ))
    }

    fn worker_for(&self, stripe_id: usize) -> usize {
        worker_for(&self.range_starts, stripe_id)
    }

    pub fn send(&self, stripe_id: usize, request: IngestRequest) {
        let worker = self.worker_for(stripe_id);
        if let Some(sender) = self.senders.get(worker) {
            if sender.send(request).is_err() {
                error!("Ingest worker {worker} is gone, dropping a request for stripe {stripe_id}");
            }
        }
    }

    /// Drop the request queues, which is how the workers learn to stop, then
    /// wait for them.
    pub fn shutdown(&mut self) {
        self.senders.clear();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

/// The worker owning `stripe_id`, given where each one's range starts.
fn worker_for(range_starts: &[usize], stripe_id: usize) -> usize {
    match range_starts.binary_search(&stripe_id) {
        Ok(index) => index,
        Err(index) => index.saturating_sub(1),
    }
}

struct IngestWorker {
    index: usize,
    start: usize,
    end: usize,
    target_dev: Arc<dyn BlockDevice>,
    stripe_source_builder: StripeSourceBuilder,
    alignment: usize,
    autofetch: bool,
    expects_pushes: bool,
    shared_state: SharedMetadataState,
    connections: usize,
    completions: Sender<BgWorkerRequest>,
    requests: Receiver<IngestRequest>,
    never_disconnect: bool,
}

impl IngestWorker {
    /// Built on the worker's own thread: a fetcher owns buffers and a channel
    /// that cannot cross threads, which is the whole reason each worker has its
    /// own rather than sharing one.
    fn build(&self) -> Result<(StripeFetcher, u64)> {
        let source = self
            .stripe_source_builder
            .build_with_connections(Some(self.connections))?;
        let source_sector_count = source.sector_count();
        let mut fetcher = StripeFetcher::new(
            source,
            &*self.target_dev,
            self.shared_state.stripe_sector_count(),
            self.shared_state.clone(),
            self.alignment,
            self.autofetch,
        )?;
        fetcher.set_expects_pushes(self.expects_pushes);
        fetcher.set_never_disconnect(self.never_disconnect);
        fetcher.restrict_autofetch_to(self.start, self.end);
        Ok((fetcher, source_sector_count))
    }

    fn run(self, ready: Sender<Result<u64>>) {
        let mut fetcher = match self.build() {
            Ok((fetcher, source_sector_count)) => {
                if ready.send(Ok(source_sector_count)).is_err() {
                    return;
                }
                fetcher
            }
            Err(e) => {
                let _ = ready.send(Err(e));
                return;
            }
        };

        loop {
            if !fetcher.busy() {
                match self.requests.recv() {
                    Ok(request) => Self::apply(&mut fetcher, request),
                    Err(_) => break,
                }
            }

            loop {
                match self.requests.try_recv() {
                    Ok(request) => Self::apply(&mut fetcher, request),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            fetcher.update();
            for (stripe_id, success) in fetcher.take_finished_fetches() {
                if self
                    .completions
                    .send(BgWorkerRequest::FetchCompleted { stripe_id, success })
                    .is_err()
                {
                    info!("Coordinator is gone, ingest worker {} stopping", self.index);
                    return;
                }
            }
            fetcher.disconnect_from_source_if_all_fetched();
        }
    }

    fn apply(fetcher: &mut StripeFetcher, request: IngestRequest) {
        match request {
            IngestRequest::Fetch { stripe_id } => fetcher.handle_fetch_request(stripe_id),
            IngestRequest::Pushed {
                stripe_id,
                data,
                permit,
            } => fetcher.accept_pushed_stripe(stripe_id, &data, permit),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write, time::Duration};

    use crate::{
        backends::SECTOR_SIZE,
        block_device::{bdev_test::TestBlockDevice, metadata_flags, Evicted, UbiMetadata},
        config::v2::{self, stripe_source::StripeSourceConfig, tuning::IoEngine},
    };

    /// Every stripe belongs to exactly one worker, and the ranges are
    /// contiguous — two workers asking for the same stripe would each fetch it,
    /// and a stripe owned by nobody would never be swept.
    #[test]
    fn every_stripe_belongs_to_exactly_one_worker() {
        let stripe_count = 1000;
        for workers in [1usize, 3, 4, 7, 16] {
            let starts: Vec<usize> = (0..workers)
                .map(|index| stripe_count * index / workers)
                .collect();
            let mut owner_counts = vec![0usize; workers];
            for stripe_id in 0..stripe_count {
                let owner = worker_for(&starts, stripe_id);
                assert!(owner < workers, "stripe {stripe_id} went to worker {owner}");
                let start = starts[owner];
                let end = starts.get(owner + 1).copied().unwrap_or(stripe_count);
                assert!(
                    (start..end).contains(&stripe_id),
                    "stripe {stripe_id} routed to worker {owner}, whose range is {start}..{end}"
                );
                owner_counts[owner] += 1;
            }
            assert_eq!(owner_counts.iter().sum::<usize>(), stripe_count);
            assert!(
                owner_counts.iter().all(|count| *count > 0),
                "with {workers} workers one of them was given nothing to do"
            );
        }
    }

    /// A stripe past the last range still goes somewhere rather than panicking.
    #[test]
    fn a_stripe_past_the_end_lands_on_the_last_worker() {
        let starts = vec![0, 50, 100];
        assert_eq!(worker_for(&starts, 10_000), 2);
    }

    const STRIPE_SHIFT: u8 = 3;
    const STRIPE_SECTORS: u64 = 1 << STRIPE_SHIFT;
    const STRIPE_BYTES: usize = STRIPE_SECTORS as usize * SECTOR_SIZE;
    const STRIPES: usize = 4;

    fn image_byte(stripe_id: usize) -> u8 {
        0xA0 + stripe_id as u8
    }

    /// A raw image the pool's workers read with O_DIRECT, so it lives under
    /// `target/` rather than a `/tmp` that may be tmpfs.
    fn raw_image() -> tempfile::NamedTempFile {
        let dir = std::env::current_dir().unwrap().join("target");
        std::fs::create_dir_all(&dir).unwrap();
        let file = tempfile::NamedTempFile::new_in(dir).unwrap();
        for stripe_id in 0..STRIPES {
            file.as_file()
                .write_all(&vec![image_byte(stripe_id); STRIPE_BYTES])
                .unwrap();
        }
        file.as_file().sync_all().unwrap();
        file
    }

    fn raw_image_config(image: &tempfile::NamedTempFile) -> v2::Config {
        v2::Config {
            device: v2::DeviceSection {
                snapshot_server: None,
                snapshot_source: None,
                snapshot_compression: Default::default(),
                data_path: "/tmp/non-existent-disk".into(),
                metadata_path: None,
                vhost_socket: None,
                rpc_socket: None,
                device_id: "ubiblk".to_string(),
                track_written: false,
            },
            tuning: v2::tuning::TuningSection {
                io_engine: IoEngine::Sync,
                ..Default::default()
            },
            encryption: None,
            danger_zone: v2::DangerZone {
                enabled: true,
                allow_unencrypted_disk: true,
                allow_inline_plaintext_secrets: true,
                allow_secret_over_regular_file: true,
                allow_unencrypted_connection: true,
                allow_env_secrets: false,
            },
            stripe_source: Some(StripeSourceConfig::Raw {
                image_path: image.path().to_path_buf(),
                autofetch: true,
                copy_on_read: true,
            }),
            spill: None,
            secrets: std::collections::HashMap::new(),
        }
    }

    fn next_completion(finished: &Receiver<BgWorkerRequest>) -> (usize, bool) {
        match finished.recv_timeout(Duration::from_secs(10)) {
            Ok(BgWorkerRequest::FetchCompleted { stripe_id, success }) => (stripe_id, success),
            Ok(_) => panic!("the pool reported something other than a fetch completion"),
            Err(e) => panic!("no fetch completion from the pool: {e}"),
        }
    }

    /// A worker disconnects from its source once every source stripe is
    /// fetched, and from then on a re-fetch fails. With spill, an evicted
    /// stripe is re-fetched from that source, so the pool's flag has to reach
    /// every worker's fetcher.
    #[test]
    fn pool_workers_honour_never_disconnect() {
        for never_disconnect in [false, true] {
            let image = raw_image();
            let target = TestBlockDevice::new((STRIPES * STRIPE_BYTES) as u64);
            let shared =
                SharedMetadataState::new(&UbiMetadata::new(STRIPE_SHIFT, STRIPES, STRIPES));
            for stripe_id in 0..STRIPES {
                shared.set_stripe_header(
                    stripe_id,
                    metadata_flags::FETCHED | metadata_flags::HAS_SOURCE,
                );
            }
            let (completions, finished) = channel();
            let (mut pool, source_sector_count) = IngestPool::new(IngestPoolConfig {
                target_dev: Arc::from(BlockDevice::clone(&target)),
                stripe_source_builder: StripeSourceBuilder::new(
                    raw_image_config(&image),
                    STRIPE_SECTORS,
                    false,
                    None,
                ),
                alignment: 4096,
                // The worker's sweep queue is filled whether or not it sweeps,
                // and only a sweep drains it; a fetcher that never drains it is
                // never idle, so it would never reach the disconnect at all.
                autofetch: true,
                expects_pushes: false,
                shared_state: shared.clone(),
                workers: 1,
                connections: 1,
                completions,
                never_disconnect,
            })
            .unwrap();
            assert_eq!(source_sector_count, STRIPES as u64 * STRIPE_SECTORS);

            // The raw state pokes leave the counters alone, so as far as the
            // worker can tell every source stripe stays fetched: that is what
            // makes it disconnect, and a real eviction here would race the
            // worker's check by lowering the fetched count first.
            //
            // A push for an evicted stripe is written without the source, so
            // its completion says nothing about the flag; it says the worker
            // has finished a pass, disconnect check included, before it takes
            // the next request.
            shared.set_stripe_fetch_state_for_test(0, Evicted);
            pool.send(
                0,
                IngestRequest::Pushed {
                    stripe_id: 0,
                    data: vec![0xEE; STRIPE_BYTES],
                    permit: PushPermit::unbounded(),
                },
            );
            assert_eq!(next_completion(&finished), (0, true));

            // A re-fetch needs the source: gone unless the flag kept it.
            shared.set_stripe_fetch_state_for_test(1, Evicted);
            pool.send(1, IngestRequest::Fetch { stripe_id: 1 });
            assert_eq!(
                next_completion(&finished),
                (1, never_disconnect),
                "never_disconnect = {never_disconnect}"
            );
            if never_disconnect {
                let mut written = vec![0u8; STRIPE_BYTES];
                target.read(STRIPE_BYTES, &mut written, STRIPE_BYTES);
                assert!(written.iter().all(|byte| *byte == image_byte(1)));
            }
            pool.shutdown();
        }
    }
}
