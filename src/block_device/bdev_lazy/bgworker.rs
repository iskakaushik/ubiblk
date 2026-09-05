use super::{
    ingest_pool::{IngestPool, IngestPoolConfig, IngestRequest},
    metadata::shared_state::{stripe_flags, Evicted, Evicting, SharedMetadataState},
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
    collections::{HashMap, HashSet},
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

/// A stripe whose data has landed and whose header op is in the flusher: a
/// re-materialised stripe's EVICTED-clearing header, or, with spill, any
/// landed stripe's FETCHED header. The guest may not see either as resident
/// before that header is durable: the startup pass punches every stripe whose
/// header says EVICTED, and a stripe whose header does not say FETCHED is
/// fetched from base again on restart, over any write the guest was told had
/// landed meanwhile.
struct PendingRelease {
    stripe_id: usize,
    retries: u8,
    /// Evicted, or Failed with the header still saying EVICTED: lands through
    /// `mark_stripe_resident`. Otherwise a NotFetched stripe landing under
    /// spill, which the header completion itself lands.
    from_evicted: bool,
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
    /// Stripes that were not local (Evicted, or Failed with the header still
    /// saying EVICTED) when a fetch or push for them was handed to the ingest,
    /// until the ingest reports back. A re-sent `Fetch` for one of them is
    /// dropped here rather than forwarded: it could reach the fetcher after
    /// the pull or push has landed but before this thread has taken the
    /// completion in (a pool worker's `FetchCompleted` may sit behind the
    /// re-send on this channel), and the fetcher reads a `Fetched` entry under
    /// a still-Evicted stripe as stale and pulls again, a write that lands
    /// after the guest has the stripe back (`StripeFetcher::drop_stale_entry`).
    landing: HashSet<usize>,
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
            landing: HashSet::new(),
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
            landing: HashSet::new(),
            done: false,
        })
    }

    pub fn shared_state(&self) -> SharedMetadataState {
        self.metadata_state.clone()
    }

    pub fn process_request(&mut self, req: BgWorkerRequest) {
        match req {
            BgWorkerRequest::Fetch { stripe_id } => {
                if self.stripe_local_or_landing(stripe_id) {
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
                    if self.metadata_state.stripe_fetched_if_needed(stripe_id) {
                        // Resident already (the channel asked before this
                        // thread landed it): the fetcher would find nothing
                        // to do. Forwarding is not harmless with a pool. The
                        // request sits on the worker's queue while this
                        // thread may evict the stripe; the worker then reads
                        // its Fetched entry under an Evicted stripe as stale
                        // and pulls, a landing noted nowhere here, whose
                        // write can follow the guest's once a real re-fetch
                        // has released the stripe.
                        debug!("Fetch for resident stripe {stripe_id} dropped");
                        return;
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
                    self.metadata_flusher.update_stripe_header(
                        stripe_id,
                        metadata_flags::PUSHED,
                        0,
                        0,
                    );
                }
                if self.release_pending(stripe_id) {
                    // The stripe's content is local already, a pull of the
                    // same snapshot stripe or this push's twin, and its
                    // header is in flight. Forwarding would put the stripe
                    // back in `landing` with an ingest that finds it resident
                    // and reports nothing, leaving every later Fetch for it
                    // dropped. The permit goes with the push.
                    debug!("Push for stripe {stripe_id} dropped: its release is pending");
                    return;
                }
                let permit = match &mut self.evictor {
                    None => permit,
                    Some(evictor) => match evictor.on_pushed_stripe(stripe_id, &data, permit) {
                        (PushDisposition::Forward, Some(permit)) => permit,
                        _ => return,
                    },
                };
                if self.evictor.is_some() && self.metadata_state.stripe_fetched_if_needed(stripe_id)
                {
                    // The fetcher drops a push for a local stripe itself, so
                    // this only matters with a pool: forwarded, the push sits
                    // on the worker's queue while this thread may claim the
                    // stripe, and the worker would write the pre-image under
                    // the evictor's read, to be uploaded as the fork's data.
                    // The permit goes with it.
                    debug!("Push for resident stripe {stripe_id} dropped");
                    return;
                }
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

    fn release_pending(&self, stripe_id: usize) -> bool {
        self.pending_release
            .values()
            .any(|pending| pending.stripe_id == stripe_id)
    }

    /// Whether a `Fetch { S }` has nothing to do: S's data is local with its
    /// header in flight, or a pull or push for it is with the ingest already.
    /// Forwarding either would start a second pull whose write lands after the
    /// guest has the stripe back. Every route a fetch takes to the ingest,
    /// the guest's request and the evictor's released ones alike, asks here.
    fn stripe_local_or_landing(&self, stripe_id: usize) -> bool {
        if self.release_pending(stripe_id) {
            // The channel's re-send finds the stripe resident once the header
            // is durable.
            debug!("Fetch for stripe {stripe_id} dropped: its release is pending");
            return true;
        }
        if self.landing.contains(&stripe_id) {
            debug!("Fetch for stripe {stripe_id} dropped: it is being taken in");
            return true;
        }
        false
    }

    /// Remember a stripe that is not local when it goes to the ingest, so
    /// re-sent fetches for it are dropped until `stripe_landed` sees it.
    fn note_landing(&mut self, stripe_id: usize) {
        if self.metadata_state.stripe_fetch_state(stripe_id) == Evicted
            || self.metadata_state.stripe_flags(stripe_id) & stripe_flags::WAS_EVICTED != 0
        {
            self.landing.insert(stripe_id);
        }
    }

    fn route_fetch(&mut self, stripe_id: usize) {
        self.note_landing(stripe_id);
        match &mut self.ingest {
            Ingest::Inline(fetcher) => fetcher.handle_fetch_request(stripe_id),
            Ingest::Pool(pool) => pool.send(stripe_id, IngestRequest::Fetch { stripe_id }),
        }
    }

    fn route_push(&mut self, stripe_id: usize, data: Vec<u8>, permit: PushPermit) {
        self.note_landing(stripe_id);
        match &mut self.ingest {
            Ingest::Inline(fetcher) => fetcher.accept_pushed_stripe(stripe_id, &data, permit),
            // Never with an evictor: SpillSection::validate refuses
            // ingest_workers > 1. The worker dequeues this push on its own
            // schedule and may write it after its own pull of the stripe has
            // landed and this thread has released the stripe to the guest,
            // unpinned, over a guest write; nothing here sees that window.
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

    /// The ingest finished with a stripe. Without spill it lands at once and
    /// the FETCHED header follows whenever the flusher gets to it: a header
    /// that says a stripe is missing only costs fetching it again.
    ///
    /// With spill every landing waits for its header, through the tokened
    /// release op. An evicted stripe (or a Failed one whose header still says
    /// EVICTED) may not be seen as resident before the header clearing
    /// EVICTED is on disk, because the startup pass punches every stripe
    /// whose header says so (I4). A never-evicted stripe waits for FETCHED
    /// because a header that says missing no longer only costs a re-fetch:
    /// a guest write to a stripe that is resident in memory passes to base
    /// and is acknowledged, and a crash before FETCHED reaches the disk
    /// restarts the stripe NotFetched (WRITTEN alone does not make it
    /// resident) and fetches the base image over the acknowledged write.
    /// Holding only writes would need the channel to know whether the header
    /// is durable yet; holding the landing costs each fetched stripe one
    /// header write and fsync before the guest sees it, and covers a write
    /// that arrives the moment after the landing as well as one queued
    /// before it.
    fn stripe_landed(&mut self, stripe_id: usize, success: bool) {
        self.landing.remove(&stripe_id);
        if !success {
            error!("Stripe {stripe_id} fetch failed");
            return;
        }
        if self.metadata_state.stripe_fetch_state(stripe_id) == Evicting {
            // The evictor owns the stripe and its header until it finishes or
            // aborts. A landing now is a pull or push that reached the ingest
            // before this thread claimed the stripe (a request already on a
            // pool worker's queue is past the coordinator's interception);
            // its data is under the evictor's read, and a SetFetched queued
            // here would serialise behind the EVICTED header op and write
            // FETCHED over it: a disk saying local for punched blocks.
            error!(
                "Stripe {stripe_id} landed while being evicted; leaving its header to the evictor"
            );
            self.metadata_state
                .spill()
                .degraded_reasons
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        let from_evicted = self.metadata_state.stripe_fetch_state(stripe_id) == Evicted
            || self.metadata_state.stripe_flags(stripe_id) & stripe_flags::WAS_EVICTED != 0;
        let durable_first = from_evicted || self.evictor.is_some();
        if !durable_first {
            // Unblocks anything waiting on this stripe now, rather than once
            // the flusher has worked through the sweep's backlog.
            self.metadata_state.mark_stripe_fetched(stripe_id);
            self.metadata_flusher.set_stripe_fetched(stripe_id);
            return;
        }
        if self.release_pending(stripe_id) {
            // Landed twice (a released push written behind the re-fetch,
            // with the same content): the op in flight releases it once.
            debug!("Stripe {stripe_id} landed again while its release is pending");
            return;
        }
        self.issue_release(stripe_id, 0, from_evicted);
        #[cfg(feature = "fault-injection")]
        if from_evicted {
            if let Some(evictor) = &self.evictor {
                evictor.crash_if_at(CrashPoint::DuringRefetch);
            }
        }
    }

    /// Hand the flusher the tokened {set FETCHED, clear EVICTED} op that lands
    /// `stripe_id` once durable, and remember it under the token.
    fn issue_release(&mut self, stripe_id: usize, retries: u8, from_evicted: bool) {
        let token = self.next_release_token;
        self.next_release_token += 2;
        self.pending_release.insert(
            token,
            PendingRelease {
                stripe_id,
                retries,
                from_evicted,
            },
        );
        self.metadata_flusher.update_stripe_header(
            stripe_id,
            metadata_flags::FETCHED,
            metadata_flags::EVICTED,
            token,
        );
    }

    /// The flusher finished a release op. Durable lands the stripe; anything
    /// else is retried a few times with a fresh token and then given up on:
    /// an evicted stripe is left Evicted for the guest's next fetch to
    /// re-write, a written one is released the way an unwritten one lands.
    fn apply_release_outcome(&mut self, outcome: &PersistOutcome) {
        let Some(pending) = self.pending_release.remove(&outcome.token) else {
            error!(
                "Release outcome for stripe {} with unknown token {}",
                outcome.stripe_id, outcome.token
            );
            return;
        };
        let stripe_id = pending.stripe_id;
        if outcome.result != PersistResult::Durable && pending.retries < MAX_RELEASE_RETRIES {
            warn!(
                "Release header for stripe {stripe_id} ended {:?}; retrying",
                outcome.result
            );
            self.issue_release(stripe_id, pending.retries + 1, pending.from_evicted);
            return;
        }
        // The release is over either way. A push forwarded for the stripe
        // while its pull was landing may have put it back in `landing` with
        // an ingest that found it resident and reported nothing; an entry
        // left here would drop every later Fetch for the stripe.
        self.landing.remove(&stripe_id);
        match outcome.result {
            PersistResult::Durable if pending.from_evicted => {
                debug!("Stripe {stripe_id} re-materialised");
                self.metadata_state.mark_stripe_resident(stripe_id);
            }
            PersistResult::Durable => {
                // The flusher's completion landed it already (its header
                // carries FETCHED); this is the landing if it somehow did not.
                debug!("Stripe {stripe_id} landed with its FETCHED header durable");
                self.metadata_state.mark_stripe_fetched(stripe_id);
            }
            result if pending.from_evicted => {
                error!(
                    "Release header for stripe {stripe_id} ended {result:?} after \
                     {MAX_RELEASE_RETRIES} retries; leaving it evicted"
                );
                self.metadata_state
                    .spill()
                    .degraded_reasons
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            result => {
                // Left NotFetched, anything queued on it would wait for good:
                // the channel re-sends a Fetch only for an Evicting or Evicted
                // front. Release it the way a stripe lands without spill, and
                // count the exposure to a restart re-fetching over a write.
                error!(
                    "FETCHED header for stripe {stripe_id} ended {result:?} after \
                     {MAX_RELEASE_RETRIES} retries; releasing it with the header not known durable"
                );
                self.metadata_state
                    .spill()
                    .degraded_reasons
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.metadata_state.mark_stripe_fetched(stripe_id);
                self.metadata_flusher.set_stripe_fetched(stripe_id);
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
                // A fetch held for space may have been overtaken by a push
                // (pushes are not gated) that has since landed.
                if self.stripe_local_or_landing(stripe_id) {
                    continue;
                }
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
            bdev_lazy::{
                metadata::{Fetched, NotFetched},
                SharedMetadataState,
            },
            bdev_test::TestBlockDevice,
            Evicting, NullBlockDevice, UbiMetadata,
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

    /// A coordinator with a real evictor over a `TestObjectStore` and a
    /// `RecordingPuncher`, and the devices a test asserts through.
    struct EvictorRig {
        worker: BgWorker,
        sender: std::sync::mpsc::Sender<BgWorkerRequest>,
        state: SharedMetadataState,
        source_dev: TestBlockDevice,
        target_dev: TestBlockDevice,
        metadata_dev: TestBlockDevice,
    }

    const RIG_STRIPE_SECTORS: u64 = 8;
    const RIG_STRIPES: usize = 8;
    const RIG_STRIPE_BYTES: u64 = RIG_STRIPE_SECTORS * crate::backends::SECTOR_SIZE as u64;

    fn rig_devices(
        headers: &[(usize, u8)],
    ) -> (TestBlockDevice, TestBlockDevice, SharedMetadataState) {
        let size = RIG_STRIPES as u64 * RIG_STRIPE_BYTES;
        let target_dev = TestBlockDevice::new(size);
        let metadata_dev = TestBlockDevice::new(16 * 1024);
        let mut metadata = UbiMetadata::new(3, RIG_STRIPES, RIG_STRIPES);
        for (stripe_id, header) in headers {
            metadata.set_stripe_header(*stripe_id, *header);
        }
        metadata.save_to_bdev(&metadata_dev).unwrap();
        let state = SharedMetadataState::new(&UbiMetadata::load_from_bdev(&metadata_dev).unwrap());
        (target_dev, metadata_dev, state)
    }

    fn rig_evictor(
        target_dev: &TestBlockDevice,
        state: &SharedMetadataState,
        max_local_bytes: u64,
        free_bytes: u64,
    ) -> Evictor {
        use crate::{
            archive::{ArchiveCompressionAlgorithm, TestObjectStore},
            block_device::spill::{EvictorConfig, RecordingPuncher, SpillCodec},
            config::v2::spill::OnFull,
        };

        let puncher = RecordingPuncher::default();
        puncher
            .free
            .store(free_bytes, std::sync::atomic::Ordering::SeqCst);
        Evictor::new(
            EvictorConfig {
                data_path: "/tmp/device.raw".into(),
                device_id: "fork-1".to_string(),
                stripe_sector_count: RIG_STRIPE_SECTORS,
                target_sector_count: target_dev.sector_count(),
                max_local_bytes,
                low_water_bytes: 0,
                hard_margin_bytes: RIG_STRIPES as u64 * RIG_STRIPE_BYTES,
                min_free_bytes: 4096,
                clean_eviction: false,
                on_full: OnFull::Stall,
                max_concurrent_evictions: 1,
                sweep_batch: 4096,
                alignment: 4096,
            },
            target_dev.create_channel().unwrap(),
            Some(Box::new(TestObjectStore::new())),
            SpillCodec::new(ArchiveCompressionAlgorithm::None, None, RIG_STRIPE_SECTORS),
            Box::new(puncher),
            state.clone(),
        )
        .unwrap()
    }

    /// Inline ingest over a `TestBlockDevice` source.
    fn build_bg_worker_with_evictor(headers: &[(usize, u8)], max_local_bytes: u64) -> EvictorRig {
        build_bg_worker_with_evictor_opts(headers, max_local_bytes, false)
    }

    /// As above, with the fetcher's sweep on or off.
    fn build_bg_worker_with_evictor_opts(
        headers: &[(usize, u8)],
        max_local_bytes: u64,
        autofetch: bool,
    ) -> EvictorRig {
        let (target_dev, metadata_dev, state) = rig_devices(headers);
        let source_dev = TestBlockDevice::new(RIG_STRIPES as u64 * RIG_STRIPE_BYTES);
        let stripe_source = Box::new(
            stripe_source::BlockDeviceStripeSource::new(
                BlockDevice::clone(&source_dev),
                RIG_STRIPE_SECTORS,
            )
            .unwrap(),
        );
        let evictor = rig_evictor(&target_dev, &state, max_local_bytes, 1 << 40);
        let (sender, receiver) = channel();
        let worker = BgWorker::new(
            stripe_source,
            &target_dev,
            &metadata_dev,
            4096,
            autofetch,
            false,
            state.clone(),
            receiver,
            Some(evictor),
        )
        .unwrap();
        EvictorRig {
            worker,
            sender,
            state,
            source_dev,
            target_dev,
            metadata_dev,
        }
    }

    /// Inline ingest over the composite source a fork runs with, expecting
    /// pushes: a pull of an evicted stripe is refused by metadata once the
    /// stripe was pushed, and the fetcher then applies the parked push.
    fn build_bg_worker_with_spilling_source(headers: &[(usize, u8)]) -> EvictorRig {
        let (target_dev, metadata_dev, state) = rig_devices(headers);
        let source_dev = TestBlockDevice::new(RIG_STRIPES as u64 * RIG_STRIPE_BYTES);
        let base = Box::new(
            stripe_source::BlockDeviceStripeSource::new(
                BlockDevice::clone(&source_dev),
                RIG_STRIPE_SECTORS,
            )
            .unwrap(),
        );
        let stripe_source = Box::new(stripe_source::SpillingStripeSource::new(
            base,
            None,
            state.clone(),
        ));
        let evictor = rig_evictor(&target_dev, &state, 1 << 30, 1 << 40);
        let (sender, receiver) = channel();
        let worker = BgWorker::new(
            stripe_source,
            &target_dev,
            &metadata_dev,
            4096,
            false,
            true,
            state.clone(),
            receiver,
            Some(evictor),
        )
        .unwrap();
        EvictorRig {
            worker,
            sender,
            state,
            source_dev,
            target_dev,
            metadata_dev,
        }
    }

    /// The smallest config the builder accepts; with `has_fetched_all` the
    /// workers build a null source from it and never look at the paths.
    fn pool_config() -> crate::config::v2::Config {
        use crate::config::v2;
        v2::Config {
            device: v2::DeviceSection {
                snapshot_server: None,
                snapshot_source: None,
                snapshot_compression: Default::default(),
                data_path: "/tmp/non-existent-disk".into(),
                metadata_path: None,
                vhost_socket: None,
                rpc_socket: None,
                device_id: "fork-1".to_string(),
                track_written: true,
            },
            tuning: v2::tuning::TuningSection::default(),
            encryption: None,
            danger_zone: v2::DangerZone {
                enabled: true,
                allow_unencrypted_disk: true,
                allow_inline_plaintext_secrets: true,
                allow_secret_over_regular_file: true,
                allow_unencrypted_connection: true,
                allow_env_secrets: false,
            },
            stripe_source: None,
            spill: None,
            secrets: HashMap::new(),
        }
    }

    /// Pool ingest: one worker over a null source, reporting completions
    /// back on the coordinator's own channel.
    fn build_bg_worker_with_pool_and_evictor(
        headers: &[(usize, u8)],
        max_local_bytes: u64,
    ) -> EvictorRig {
        let (target_dev, metadata_dev, state) = rig_devices(headers);
        let evictor = rig_evictor(&target_dev, &state, max_local_bytes, 1 << 40);
        let builder = StripeSourceBuilder::new(pool_config(), RIG_STRIPE_SECTORS, true, None);
        let (sender, receiver) = channel();
        let shared_target: Arc<dyn BlockDevice> = Arc::from(BlockDevice::clone(&target_dev));
        let worker = BgWorker::with_ingest_pool(
            shared_target,
            builder,
            &metadata_dev,
            4096,
            false,
            false,
            state.clone(),
            receiver,
            sender.clone(),
            1,
            1,
            Some(evictor),
        )
        .unwrap();
        EvictorRig {
            worker,
            sender,
            state,
            source_dev: TestBlockDevice::new(RIG_STRIPE_BYTES),
            target_dev,
            metadata_dev,
        }
    }

    const RIG_DIRTY: u8 = crate::block_device::metadata_flags::FETCHED
        | crate::block_device::metadata_flags::WRITTEN
        | crate::block_device::metadata_flags::HAS_SOURCE;

    #[test]
    fn fetch_for_evicting_stripe_never_reaches_inline_fetcher() {
        let mut rig = build_bg_worker_with_evictor(&[(0, RIG_DIRTY)], 0);
        // Pinned guest I/O keeps the eviction draining, where a fetch aborts it.
        rig.state.pin_inflight(0, 0);
        for _ in 0..5 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Evicting);

        rig.sender
            .send(BgWorkerRequest::Fetch { stripe_id: 0 })
            .unwrap();
        rig.worker.receive_requests(false);
        assert_eq!(
            rig.state.stripe_fetch_state(0),
            Fetched,
            "aborted, resident again"
        );
        let Ingest::Inline(fetcher) = &rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        assert!(!fetcher.busy(), "nothing was queued for the fetcher");
        for _ in 0..5 {
            rig.worker.update();
        }
        assert_eq!(rig.source_dev.metrics.read().unwrap().reads, 0);
        assert_eq!(rig.target_dev.metrics.read().unwrap().writes, 0);
        assert_eq!(
            rig.state
                .spill()
                .evictions_aborted
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn fetch_for_evicting_stripe_never_reaches_pool_worker() {
        let mut rig = build_bg_worker_with_pool_and_evictor(
            &[
                (0, RIG_DIRTY),
                (1, crate::block_device::metadata_flags::HAS_SOURCE),
            ],
            0,
        );
        rig.state.pin_inflight(0, 0);
        for _ in 0..5 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Evicting);

        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
        // A worker that had been given the stripe would pull it (zeros from
        // the null source) and report back; nothing arrives.
        assert!(matches!(
            rig.worker
                .req_receiver
                .recv_timeout(Duration::from_millis(300)),
            Err(RecvTimeoutError::Timeout)
        ));
        assert_eq!(rig.target_dev.metrics.read().unwrap().writes, 0);

        // Control: a fetch for a stripe that is not being evicted does reach
        // the worker, which reports back (the null source has nothing for
        // it, so unsuccessfully); that is how the silence above is known to
        // mean something.
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 1 });
        match rig.worker.req_receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(BgWorkerRequest::FetchCompleted { stripe_id, .. }) => assert_eq!(stripe_id, 1),
            _ => panic!("the pool worker should have reported stripe 1"),
        }
        rig.worker.process_request(BgWorkerRequest::Shutdown);
        rig.worker.run();
    }

    /// A Fetch for a stripe that is resident by the time this thread sees it
    /// (the channel asked before the landing) is dropped rather than
    /// forwarded when spill is on. Forwarded to a pool worker it sits on that
    /// worker's queue while this thread may evict the stripe; the worker then
    /// finds a Fetched entry under an Evicted stripe, drops it as stale and
    /// pulls, a landing noted nowhere here, whose write can follow the
    /// guest's once a real re-fetch has released the stripe. Observed through
    /// the readahead a forwarded Fetch queues on a sweeping fetcher: the sweep
    /// is confined to stripes 4..8, so anything pulled below 4 came from the
    /// Fetch.
    #[test]
    fn fetch_for_a_resident_stripe_is_dropped_at_the_coordinator() {
        let resident = crate::block_device::metadata_flags::FETCHED
            | crate::block_device::metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor_opts(&[(0, resident)], 1 << 30, true);
        let Ingest::Inline(fetcher) = &mut rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        fetcher.restrict_autofetch_to(4, RIG_STRIPES);
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        for _ in 0..40 {
            rig.worker.update();
        }
        assert_eq!(
            rig.source_dev.metrics.read().unwrap().reads,
            RIG_STRIPES - 4,
            "only the sweep's stripes were pulled"
        );

        // Without spill the Fetch is forwarded as before: the fetcher finds
        // the stripe local and reads ahead of it.
        let (target_dev, metadata_dev, state) = rig_devices(&[(0, resident)]);
        let source_dev = TestBlockDevice::new(RIG_STRIPES as u64 * RIG_STRIPE_BYTES);
        let stripe_source = Box::new(
            stripe_source::BlockDeviceStripeSource::new(
                BlockDevice::clone(&source_dev),
                RIG_STRIPE_SECTORS,
            )
            .unwrap(),
        );
        let (_sender, receiver) = channel();
        let mut plain = BgWorker::new(
            stripe_source,
            &target_dev,
            &metadata_dev,
            4096,
            true,
            false,
            state,
            receiver,
            None,
        )
        .unwrap();
        let Ingest::Inline(fetcher) = &mut plain.ingest else {
            panic!("inline ingest expected");
        };
        fetcher.restrict_autofetch_to(4, RIG_STRIPES);
        plain.process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        for _ in 0..40 {
            plain.update();
        }
        assert_eq!(
            source_dev.metrics.read().unwrap().reads,
            RIG_STRIPES - 1,
            "the readahead pulled stripes 1..4 as well"
        );
    }

    /// A landing reported for a stripe the evictor has claimed (a pull or
    /// push that reached a pool worker before the claim) must not queue a
    /// header op: a SetFetched behind the evictor's EVICTED op would leave the
    /// disk saying local for blocks about to be punched, and the next start
    /// would serve zeros for the stripe.
    #[test]
    fn landing_for_an_evicting_stripe_touches_no_header() {
        use crate::block_device::metadata_flags;

        let mut rig = build_bg_worker_with_evictor(&[(0, RIG_DIRTY)], 1 << 30);
        let degraded_before = rig
            .state
            .spill()
            .degraded_reasons
            .load(std::sync::atomic::Ordering::Relaxed);
        rig.state.set_stripe_fetch_state_for_test(0, Evicting);
        // The evictor's committed header op: EVICTED in, FETCHED out.
        rig.worker.metadata_flusher.update_stripe_header(
            0,
            metadata_flags::EVICTED,
            metadata_flags::FETCHED,
            1,
        );

        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: true,
        });
        assert!(rig.worker.pending_release.is_empty());
        assert_eq!(rig.state.stripe_fetch_state(0), Evicting);
        assert_eq!(
            rig.state
                .spill()
                .degraded_reasons
                .load(std::sync::atomic::Ordering::Relaxed),
            degraded_before + 1
        );

        for _ in 0..6 {
            rig.worker.update();
        }
        assert!(!rig.worker.metadata_flusher.busy());
        let header = UbiMetadata::load_from_bdev(&rig.metadata_dev)
            .unwrap()
            .stripe_header(0);
        assert_ne!(header & metadata_flags::EVICTED, 0);
        assert_eq!(
            header & metadata_flags::FETCHED,
            0,
            "no SetFetched went out behind the evictor's op"
        );
        assert_eq!(rig.state.stripe_fetch_state(0), Evicting);
    }

    #[test]
    fn pushed_stripe_records_pushed_before_disposition() {
        use crate::block_device::{metadata_flags, PushGate};

        let mut rig = build_bg_worker_with_evictor(&[(0, RIG_DIRTY)], 0);
        rig.state.pin_inflight(0, 0);
        for _ in 0..5 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Evicting);
        assert!(!rig.state.stripe_pushed(0));

        let gate = PushGate::new(2);
        rig.worker.process_request(BgWorkerRequest::PushedStripe {
            stripe_id: 0,
            data: vec![0xEE; RIG_STRIPE_BYTES as usize],
            permit: gate.acquire(),
        });
        // Ignored (the fork's own data is newer), yet recorded.
        assert!(rig.state.stripe_pushed(0));
        assert_eq!(gate.queued(), 0, "the permit went with the dropped push");
        assert_eq!(rig.state.stripe_fetch_state(0), Evicting);
        for _ in 0..3 {
            rig.worker.update();
        }
        let header = UbiMetadata::load_from_bdev(&rig.metadata_dev)
            .unwrap()
            .stripe_header(0);
        assert_ne!(header & metadata_flags::PUSHED, 0, "PUSHED persisted");
        assert_eq!(rig.target_dev.metrics.read().unwrap().writes, 0);

        // Without an evictor nothing is recorded and the push goes through.
        let (mut plain, _sender) = build_bg_worker();
        plain.process_request(BgWorkerRequest::PushedStripe {
            stripe_id: 0,
            data: vec![0xEE; RIG_STRIPE_BYTES as usize],
            permit: gate.acquire(),
        });
        assert!(!plain.shared_state().stripe_pushed(0));
    }

    #[test]
    fn stripe_landed_on_evicted_stripe_uses_even_token_and_does_not_mark_fetched() {
        use crate::block_device::metadata_flags;

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor(&[(0, evicted), (1, evicted)], 1 << 30);
        assert_eq!(rig.state.stripe_fetch_state(0), Evicted);
        let (fetched, resident) = (rig.state.fetched_stripes(), rig.state.resident_stripes());

        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: true,
        });
        assert_eq!(rig.state.stripe_fetch_state(0), Evicted, "not released yet");
        assert_eq!(rig.state.fetched_stripes(), fetched);
        assert_eq!(rig.state.resident_stripes(), resident);
        assert_eq!(rig.worker.pending_release.len(), 1);
        assert_eq!(rig.worker.pending_release[&2].stripe_id, 0);
        assert_eq!(rig.worker.pending_release[&2].retries, 0);
        assert_eq!(rig.worker.next_release_token, 4);

        // A fetch for it while the header is in flight is dropped.
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        let Ingest::Inline(fetcher) = &rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        assert!(!fetcher.busy());

        for _ in 0..4 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
        assert_eq!(rig.state.fetched_stripes(), fetched + 1);
        assert_eq!(rig.state.resident_stripes(), resident + 1);
        assert!(rig.worker.pending_release.is_empty());
        let header = UbiMetadata::load_from_bdev(&rig.metadata_dev)
            .unwrap()
            .stripe_header(0);
        assert_ne!(header & metadata_flags::FETCHED, 0);
        assert_eq!(header & metadata_flags::EVICTED, 0);

        // A stripe that was never evicted waits for its FETCHED header too,
        // under the next even token; the header's completion lands it.
        rig.state.set_stripe_fetch_state_for_test(1, NotFetched);
        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 1,
            success: true,
        });
        assert_eq!(
            rig.state.stripe_fetch_state(1),
            NotFetched,
            "not released yet"
        );
        assert_eq!(rig.worker.pending_release[&4].stripe_id, 1);
        assert!(!rig.worker.pending_release[&4].from_evicted);
        for _ in 0..4 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(1), Fetched);
        assert!(rig.worker.pending_release.is_empty());
        assert_eq!(rig.worker.next_release_token, 6);
    }

    #[test]
    fn second_landing_while_release_is_pending_issues_no_second_op() {
        use crate::block_device::metadata_flags;

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor(&[(0, evicted)], 1 << 30);
        let degraded_before = rig
            .state
            .spill()
            .degraded_reasons
            .load(std::sync::atomic::Ordering::Relaxed);
        for _ in 0..2 {
            rig.worker.process_request(BgWorkerRequest::FetchCompleted {
                stripe_id: 0,
                success: true,
            });
        }
        assert_eq!(rig.worker.pending_release.len(), 1);
        assert_eq!(rig.worker.next_release_token, 4);

        for _ in 0..4 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
        assert!(rig.worker.pending_release.is_empty());
        assert_eq!(
            rig.state
                .spill()
                .degraded_reasons
                .load(std::sync::atomic::Ordering::Relaxed),
            degraded_before,
            "one release, no false anomaly"
        );
    }

    /// The fetcher cannot tell an evicted stripe that needs a pull from one
    /// whose pull has landed and is waiting on its header (both are a
    /// `Fetched` entry under an Evicted stripe), so the coordinator must not
    /// forward a re-sent Fetch in that window. Modelled the way a pool worker
    /// runs ahead of this thread: the fetcher is driven on its own until the
    /// pull lands, and the re-send arrives before the completion is taken in.
    #[test]
    fn re_sent_fetch_is_dropped_until_the_re_fetch_has_been_taken_in() {
        use crate::block_device::metadata_flags;

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor(&[(0, evicted)], 1 << 30);

        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        assert!(rig.worker.landing.contains(&0));

        let Ingest::Inline(fetcher) = &mut rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        for _ in 0..10 {
            fetcher.update();
        }
        assert_eq!(rig.source_dev.metrics.read().unwrap().reads, 1);
        assert_eq!(
            rig.state.stripe_fetch_state(0),
            Evicted,
            "landed, not yet taken in"
        );

        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        let Ingest::Inline(fetcher) = &mut rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        for _ in 0..10 {
            fetcher.update();
        }
        assert_eq!(
            rig.source_dev.metrics.read().unwrap().reads,
            1,
            "the re-send must not start a second pull"
        );

        for _ in 0..4 {
            rig.worker.update();
        }
        assert!(!rig.worker.landing.contains(&0));
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
    }

    /// A pull that fails for good clears the way for the guest's next Fetch.
    #[test]
    fn a_failed_re_fetch_lets_the_next_fetch_through() {
        use crate::block_device::metadata_flags;

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor(&[(0, evicted)], 1 << 30);

        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        assert!(rig.worker.landing.contains(&0));

        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: false,
        });
        assert!(!rig.worker.landing.contains(&0));
        assert!(rig.worker.pending_release.is_empty());

        // A stripe that is local when forwarded is never held.
        rig.state.set_stripe_fetch_state_for_test(0, NotFetched);
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        assert!(!rig.worker.landing.contains(&0));
    }

    /// A push forwarded to an evicted stripe is that stripe's only copy. A
    /// Fetch arriving while its write is with the ingest would be forwarded
    /// to a fetcher that sees a `Fetched` entry under an Evicted stripe and
    /// pulls, and the pull's write (here, the source's zeros) would replace
    /// the pushed bytes.
    #[test]
    fn a_forwarded_push_holds_off_a_pull_until_it_has_been_taken_in() {
        use crate::block_device::{metadata_flags, PushGate};

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor(&[(0, evicted)], 1 << 30);
        let pushed = vec![0xEE; RIG_STRIPE_BYTES as usize];
        let gate = PushGate::new(2);

        rig.worker.process_request(BgWorkerRequest::PushedStripe {
            stripe_id: 0,
            data: pushed.clone(),
            permit: gate.acquire(),
        });
        assert!(rig.state.stripe_pushed(0));
        assert!(rig.worker.landing.contains(&0), "forwarded, and remembered");

        let Ingest::Inline(fetcher) = &mut rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        for _ in 0..10 {
            fetcher.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Evicted);

        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        let Ingest::Inline(fetcher) = &mut rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        for _ in 0..10 {
            fetcher.update();
        }
        assert_eq!(
            rig.source_dev.metrics.read().unwrap().reads,
            0,
            "nothing is pulled over the pushed copy"
        );

        for _ in 0..4 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
        assert!(!rig.worker.landing.contains(&0));
        let mut written = vec![0u8; pushed.len()];
        rig.target_dev.read(0, &mut written, pushed.len());
        assert_eq!(written, pushed);
    }

    #[test]
    fn release_op_is_retried_then_given_up_leaving_the_stripe_evicted() {
        use crate::block_device::metadata_flags;

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor(&[(0, evicted)], 1 << 30);
        let degraded_before = rig
            .state
            .spill()
            .degraded_reasons
            .load(std::sync::atomic::Ordering::Relaxed);
        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: true,
        });

        // Every header write fails: the first attempt and three retries.
        for attempt in 0..4u64 {
            assert_eq!(rig.worker.pending_release.len(), 1);
            assert_eq!(
                rig.worker.pending_release[&(2 + 2 * attempt)].retries,
                attempt as u8
            );
            rig.metadata_dev
                .fail_next
                .store(true, std::sync::atomic::Ordering::SeqCst);
            rig.worker.update();
        }
        assert!(rig.worker.pending_release.is_empty(), "given up");
        assert_eq!(rig.worker.next_release_token, 10);
        assert_eq!(rig.state.stripe_fetch_state(0), Evicted);
        assert_eq!(
            rig.state
                .spill()
                .degraded_reasons
                .load(std::sync::atomic::Ordering::Relaxed),
            degraded_before + 1
        );
        let header = UbiMetadata::load_from_bdev(&rig.metadata_dev)
            .unwrap()
            .stripe_header(0);
        assert_eq!(header, evicted, "the disk still says evicted");

        // The next fetch is forwarded again rather than dropped.
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        let Ingest::Inline(fetcher) = &rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        assert!(fetcher.busy());
    }

    /// With spill a stripe is released only once its FETCHED header is
    /// durable, so a write the guest is then told has landed, queued before
    /// the landing or issued the moment after it, cannot be undone by a
    /// restart that finds the stripe NotFetched and fetches the base image
    /// over it.
    #[test]
    fn stripe_lands_only_when_its_fetched_header_is_durable_with_spill() {
        use crate::block_device::metadata_flags;

        let unfetched = metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor(&[(0, unfetched), (1, unfetched)], 1 << 30);
        assert_eq!(rig.state.stripe_fetch_state(0), NotFetched);
        let (fetched, resident) = (rig.state.fetched_stripes(), rig.state.resident_stripes());
        let degraded_before = rig
            .state
            .spill()
            .degraded_reasons
            .load(std::sync::atomic::Ordering::Relaxed);

        // The channel queued a write: WRITTEN in memory, before any data.
        rig.state.mark_stripe_written(0);
        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: true,
        });
        assert_eq!(
            rig.state.stripe_fetch_state(0),
            NotFetched,
            "not released yet"
        );
        assert_eq!(rig.state.fetched_stripes(), fetched);
        assert_eq!(rig.state.resident_stripes(), resident);
        assert_eq!(rig.worker.pending_release.len(), 1);
        assert_eq!(rig.worker.pending_release[&2].stripe_id, 0);
        assert!(!rig.worker.pending_release[&2].from_evicted);

        // A fetch for it meanwhile is dropped, as for a re-materialisation.
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        let Ingest::Inline(fetcher) = &rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        assert!(!fetcher.busy());

        for _ in 0..4 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
        assert!(rig.state.stripe_written(0));
        assert_eq!(rig.state.fetched_stripes(), fetched + 1);
        assert_eq!(rig.state.resident_stripes(), resident + 1);
        assert!(rig.worker.pending_release.is_empty());
        let header = UbiMetadata::load_from_bdev(&rig.metadata_dev)
            .unwrap()
            .stripe_header(0);
        assert_ne!(
            header & metadata_flags::FETCHED,
            0,
            "durable before release"
        );
        assert_eq!(
            rig.state
                .spill()
                .degraded_reasons
                .load(std::sync::atomic::Ordering::Relaxed),
            degraded_before,
            "the header completion and the outcome land it once, quietly"
        );

        // A stripe nobody has written waits the same way: a write may arrive
        // the moment it is resident in memory.
        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 1,
            success: true,
        });
        assert_eq!(
            rig.state.stripe_fetch_state(1),
            NotFetched,
            "not released yet"
        );
        assert_eq!(rig.worker.pending_release.len(), 1);
        for _ in 0..4 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(1), Fetched);
        assert!(rig.worker.pending_release.is_empty());
        let header = UbiMetadata::load_from_bdev(&rig.metadata_dev)
            .unwrap()
            .stripe_header(1);
        assert_ne!(header & metadata_flags::FETCHED, 0);

        // Without an evictor nothing changes: a written stripe lands at once.
        let (mut plain, _sender) = build_bg_worker();
        plain.shared_state().mark_stripe_written(0);
        plain.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: true,
        });
        assert_eq!(plain.shared_state().stripe_fetch_state(0), Fetched);
        assert!(plain.pending_release.is_empty());
    }

    /// Landings that arrive together share the header write and fsync they
    /// wait for: the flusher carries every queued request for a metadata
    /// sector in one write, so a burst of neighbouring demand fetches does
    /// not pay one fsync each.
    #[test]
    fn adjacent_landings_share_one_header_write_and_fsync() {
        use crate::block_device::metadata_flags;

        let headers: Vec<(usize, u8)> = (0..4).map(|s| (s, metadata_flags::HAS_SOURCE)).collect();
        let mut rig = build_bg_worker_with_evictor(&headers, 1 << 30);
        let io = |rig: &EvictorRig| {
            let metrics = rig.metadata_dev.metrics.read().unwrap();
            (metrics.writes, metrics.flushes)
        };
        let (writes, flushes) = io(&rig);

        for stripe_id in 0..4 {
            rig.worker.process_request(BgWorkerRequest::FetchCompleted {
                stripe_id,
                success: true,
            });
        }
        assert_eq!(rig.worker.pending_release.len(), 4);
        for _ in 0..6 {
            rig.worker.update();
        }
        for stripe_id in 0..4 {
            assert_eq!(rig.state.stripe_fetch_state(stripe_id), Fetched);
        }
        assert!(rig.worker.pending_release.is_empty());
        assert_eq!(
            io(&rig),
            (writes + 1, flushes + 1),
            "four landings, one write and one fsync"
        );
    }

    /// When the FETCHED header cannot be made durable the written stripe is
    /// still released, the way every stripe was released before, rather than
    /// holding the guest's write for good; the exposure is counted.
    #[test]
    fn written_stripe_landing_falls_back_after_failed_retries() {
        use crate::block_device::metadata_flags;

        let mut rig = build_bg_worker_with_evictor(&[(0, metadata_flags::HAS_SOURCE)], 1 << 30);
        let degraded_before = rig
            .state
            .spill()
            .degraded_reasons
            .load(std::sync::atomic::Ordering::Relaxed);
        rig.state.mark_stripe_written(0);
        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: true,
        });

        for attempt in 0..4u64 {
            assert_eq!(rig.worker.pending_release.len(), 1);
            assert_eq!(
                rig.worker.pending_release[&(2 + 2 * attempt)].retries,
                attempt as u8
            );
            assert_eq!(rig.state.stripe_fetch_state(0), NotFetched);
            rig.metadata_dev
                .fail_next
                .store(true, std::sync::atomic::Ordering::SeqCst);
            rig.worker.update();
        }
        assert!(rig.worker.pending_release.is_empty(), "given up");
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched, "released anyway");
        assert_eq!(
            rig.state
                .spill()
                .degraded_reasons
                .load(std::sync::atomic::Ordering::Relaxed),
            degraded_before + 1
        );
        // The fire-and-forget SetFetched goes out behind it.
        for _ in 0..4 {
            rig.worker.update();
        }
        let header = UbiMetadata::load_from_bdev(&rig.metadata_dev)
            .unwrap()
            .stripe_header(0);
        assert_ne!(header & metadata_flags::FETCHED, 0);
    }

    /// A push for a stripe whose pull has landed and whose release is pending
    /// carries the same content and is dropped with its permit. Forwarded, a
    /// pool worker could take it in after the release, find the stripe local
    /// and report nothing, and the stripe would sit in `landing` for good,
    /// every later Fetch for it dropped and the guest hung.
    #[test]
    fn push_during_pending_release_is_dropped_and_the_stripe_can_be_fetched_again() {
        use crate::block_device::{metadata_flags, PushGate};

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_pool_and_evictor(&[(0, evicted)], 1 << 30);
        let gate = PushGate::new(2);

        // The worker reported the pull; the release header is in flight.
        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: true,
        });
        assert_eq!(rig.worker.pending_release.len(), 1);
        rig.worker.process_request(BgWorkerRequest::PushedStripe {
            stripe_id: 0,
            data: vec![0xEE; RIG_STRIPE_BYTES as usize],
            permit: gate.acquire(),
        });
        assert!(rig.state.stripe_pushed(0), "recorded all the same");
        assert_eq!(gate.queued(), 0, "dropped with its permit");
        assert!(!rig.worker.landing.contains(&0));
        // Nothing reached the worker: a forwarded push would be written and
        // reported back.
        assert!(matches!(
            rig.worker
                .req_receiver
                .recv_timeout(Duration::from_millis(300)),
            Err(RecvTimeoutError::Timeout)
        ));
        assert_eq!(rig.target_dev.metrics.read().unwrap().writes, 0);

        for _ in 0..4 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
        assert!(!rig.worker.landing.contains(&0));

        // Evicted again later, a Fetch for it reaches the worker.
        rig.state.set_stripe_fetch_state_for_test(0, Evicted);
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        match rig.worker.req_receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(BgWorkerRequest::FetchCompleted { stripe_id, .. }) => assert_eq!(stripe_id, 0),
            _ => panic!("the pool worker should have been given stripe 0"),
        }
        rig.worker.process_request(BgWorkerRequest::Shutdown);
        rig.worker.run();
    }

    /// Backstop for the same hang: whatever put a stripe back in `landing`
    /// while its release was pending, the release ending takes it out.
    #[test]
    fn landing_is_cleared_when_the_release_ends() {
        use crate::block_device::metadata_flags;

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor(&[(0, evicted)], 1 << 30);
        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: true,
        });
        rig.worker.landing.insert(0);
        for _ in 0..4 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
        assert!(!rig.worker.landing.contains(&0), "cleared on Durable");

        // And when the release is given up on.
        let mut rig = build_bg_worker_with_evictor(&[(0, evicted)], 1 << 30);
        rig.worker.process_request(BgWorkerRequest::FetchCompleted {
            stripe_id: 0,
            success: true,
        });
        rig.worker.landing.insert(0);
        for _ in 0..4 {
            rig.metadata_dev
                .fail_next
                .store(true, std::sync::atomic::Ordering::SeqCst);
            rig.worker.update();
        }
        assert!(rig.worker.pending_release.is_empty());
        assert_eq!(rig.state.stripe_fetch_state(0), Evicted);
        assert!(!rig.worker.landing.contains(&0), "cleared on giving up");
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        let Ingest::Inline(fetcher) = &rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        assert!(fetcher.busy(), "the next fetch is forwarded");
    }

    /// A fetch held under GATE_HOLD can be overtaken by a push (pushes are not
    /// gated) that lands and sits in pending release. Released when the gate
    /// reopens, it must meet the same guards as a fresh Fetch: forwarded, the
    /// fetcher would see a Fetched entry under a still-Evicted stripe and
    /// pull again, over the pushed copy and after the guest has it back.
    #[test]
    fn released_fetch_is_dropped_while_the_stripe_is_in_pending_release() {
        use crate::block_device::{metadata_flags, PushGate, GATE_HOLD, GATE_OPEN};

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_evictor(&[(0, evicted)], 1 << 30);
        let pushed = vec![0xEE; RIG_STRIPE_BYTES as usize];
        let gate = PushGate::new(2);

        rig.state.set_write_gate(GATE_HOLD);
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        let Ingest::Inline(fetcher) = &rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        assert!(!fetcher.busy(), "held for space");
        assert!(!rig.worker.landing.contains(&0));

        rig.worker.process_request(BgWorkerRequest::PushedStripe {
            stripe_id: 0,
            data: pushed.clone(),
            permit: gate.acquire(),
        });
        assert!(rig.worker.landing.contains(&0), "forwarded");
        // The push lands in the fetcher before the coordinator ticks, the way
        // a pool worker runs ahead of this thread.
        let Ingest::Inline(fetcher) = &mut rig.worker.ingest else {
            panic!("inline ingest expected");
        };
        for _ in 0..10 {
            fetcher.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Evicted);

        // One tick takes the landing into pending release and, with nothing
        // under pressure, reopens the gate and releases the held fetch.
        rig.worker.update();
        assert_eq!(rig.state.write_gate(), GATE_OPEN);
        assert_eq!(rig.worker.pending_release.len(), 1);
        for _ in 0..4 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
        assert_eq!(
            rig.source_dev.metrics.read().unwrap().reads,
            0,
            "the released fetch must not pull over the pushed copy"
        );
        assert_eq!(rig.target_dev.metrics.read().unwrap().writes, 1);
        let mut written = vec![0u8; pushed.len()];
        rig.target_dev.read(0, &mut written, pushed.len());
        assert_eq!(written, pushed);
        assert!(!rig.worker.landing.contains(&0));
    }

    /// An evicted clean stripe with a guest write queued on it carries WRITTEN
    /// (set at queue time) but holds none of the fork's data: the write waits
    /// for the stripe to come back. The pull goes out, the replica copies the
    /// stripe out, pushes it and refuses the pull. The push is the only copy
    /// the fork can get, so it must land; ignored, the guest gets EIO for good.
    #[test]
    fn push_lands_an_evicted_stripe_whose_queued_write_set_written() {
        use crate::block_device::{metadata_flags, PushGate};

        let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
        let mut rig = build_bg_worker_with_spilling_source(&[(0, evicted)]);
        rig.state.set_source_live(true);
        let pushed = vec![0xEE; RIG_STRIPE_BYTES as usize];
        let gate = PushGate::new(2);

        // The guest's write queues: WRITTEN in memory, a Fetch to us.
        rig.state.mark_stripe_written(0);
        rig.worker
            .process_request(BgWorkerRequest::Fetch { stripe_id: 0 });
        assert!(
            rig.worker.landing.contains(&0),
            "the pull is with the ingest"
        );

        // The replica pushes before the pull is served.
        rig.worker.process_request(BgWorkerRequest::PushedStripe {
            stripe_id: 0,
            data: pushed.clone(),
            permit: gate.acquire(),
        });
        assert!(rig.state.stripe_pushed(0));
        assert_eq!(gate.queued(), 1, "forwarded: the ingest holds the slot");

        for _ in 0..12 {
            rig.worker.update();
        }
        assert_eq!(rig.state.stripe_fetch_state(0), Fetched);
        assert!(rig.state.stripe_written(0));
        assert_eq!(
            rig.source_dev.metrics.read().unwrap().reads,
            0,
            "the pull was refused by metadata, never served by base"
        );
        let mut written = vec![0u8; pushed.len()];
        rig.target_dev.read(0, &mut written, pushed.len());
        assert_eq!(written, pushed, "the push is the stripe's content");
        assert_eq!(gate.queued(), 0);
        assert!(!rig.worker.landing.contains(&0));
        assert!(rig.worker.pending_release.is_empty());
    }

    #[test]
    fn run_loop_ticks_without_requests_when_evictor_present() {
        let (target_dev, metadata_dev, state) = rig_devices(&[]);
        let source_dev = TestBlockDevice::new(RIG_STRIPES as u64 * RIG_STRIPE_BYTES);
        let stripe_source = Box::new(
            stripe_source::BlockDeviceStripeSource::new(
                BlockDevice::clone(&source_dev),
                RIG_STRIPE_SECTORS,
            )
            .unwrap(),
        );
        // The puncher's free-space answer only reaches the counters through
        // an evictor tick, so seeing it proves the loop ticked unprompted.
        let evictor = rig_evictor(&target_dev, &state, 1 << 30, 12345);
        let (sender, receiver) = channel();
        let mut worker = BgWorker::new(
            stripe_source,
            &target_dev,
            &metadata_dev,
            4096,
            false,
            false,
            state.clone(),
            receiver,
            Some(evictor),
        )
        .unwrap();
        assert_eq!(
            state
                .spill()
                .free_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        let observer_state = state.clone();
        let observer = std::thread::spawn(move || {
            let start = std::time::Instant::now();
            let mut seen = 0;
            while start.elapsed() < Duration::from_secs(5) {
                seen = observer_state
                    .spill()
                    .free_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
                if seen == 12345 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            sender.send(BgWorkerRequest::Shutdown).unwrap();
            seen
        });
        worker.run();
        assert_eq!(observer.join().unwrap(), 12345);
        assert!(worker.done);
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
        let rig = build_bg_worker_with_evictor(&[], 1 << 30);
        let EvictorRig {
            mut worker, sender, ..
        } = rig;
        assert!(worker.evictor.is_some());
        let stop = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            sender.send(BgWorkerRequest::Shutdown).unwrap();
        });
        worker.run();
        stop.join().unwrap();
        assert!(worker.done);
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
