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
        match self.range_starts.binary_search(&stripe_id) {
            Ok(index) => index,
            Err(index) => index.saturating_sub(1),
        }
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
