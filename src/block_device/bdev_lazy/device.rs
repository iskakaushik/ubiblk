use crate::{
    block_device::{
        BlockDevice, IoChannel, SharedBuffer, SharedMetadataState, GATE_FAIL, GATE_OPEN,
    },
    Result, ResultExt,
};

use super::{
    bgworker::BgWorkerRequest,
    metadata::{Evicted, Evicting, Failed, Fetched, NoSource, NotFetched},
};

use std::{
    collections::{HashSet, VecDeque},
    sync::{atomic::Ordering, mpsc::Sender},
    time::{Duration, Instant},
};

use log::{debug, error};

/// How often a queued request whose front stripe is Evicting or Evicted asks
/// the coordinator for it again. The first Fetch may have been consumed by an
/// eviction that then completed, or never sent at all (a write waiting on
/// WRITTEN); without the re-send the request would wait forever.
const FETCH_RESEND_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestType {
    In,
    Out,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StripesFetchStatus {
    Pending,
    Complete,
    Failed { stripe_id: usize },
}

struct RWRequest {
    id: usize,
    kind: RequestType,
    sector_offset: u64,
    sector_count: u32,
    buf: SharedBuffer,
    stripe_id_first: usize,
    stripe_id_last: usize,
    /// When a Fetch was last sent on behalf of this request, if ever.
    fetch_sent_at: Option<Instant>,
}

struct LazyIoChannel {
    base: Box<dyn IoChannel>,
    image: Option<Box<dyn IoChannel>>,
    queued_rw_requests: VecDeque<RWRequest>,
    finished_requests: Vec<(usize, bool)>,
    bgworker_ch: Sender<BgWorkerRequest>,
    metadata_state: SharedMetadataState,
    stripe_fetches_requested: HashSet<usize>,
    track_written: bool,
    /// Stripe range of every read or write handed to `base`, by completion id,
    /// so `poll` unpins the right stripes. Flushes leave `None`. Same shape as
    /// CryptIoChannel::read_requests. This map, not the id, decides whether a
    /// completion unpins anything, so a stray completion for an id already
    /// released finds `None` and does nothing.
    pinned_by_id: Vec<Option<(usize, usize)>>,
    /// Ids handed to `base` since its last `submit`. If that submit fails,
    /// base will never complete them, so their pins are released here.
    unsubmitted: Vec<usize>,
}

impl LazyIoChannel {
    fn new(
        base: Box<dyn IoChannel>,
        image: Option<Box<dyn IoChannel>>,
        bgworker_ch: Sender<BgWorkerRequest>,
        metadata_state: SharedMetadataState,
        track_written: bool,
    ) -> Self {
        LazyIoChannel {
            base,
            image,
            queued_rw_requests: VecDeque::new(),
            finished_requests: Vec::new(),
            bgworker_ch,
            metadata_state,
            stripe_fetches_requested: HashSet::new(),
            track_written,
            pinned_by_id: Vec::new(),
            unsubmitted: Vec::new(),
        }
    }
}

impl LazyIoChannel {
    fn request_stripes_fetch_status(&self, request: &RWRequest) -> StripesFetchStatus {
        for stripe_id in request.stripe_id_first..=request.stripe_id_last {
            let state = self.metadata_state.stripe_fetch_state(stripe_id);
            match state {
                Fetched | NoSource => {
                    continue;
                }
                NotFetched | Evicting | Evicted => {
                    return StripesFetchStatus::Pending;
                }
                Failed => {
                    return StripesFetchStatus::Failed { stripe_id };
                }
                other => {
                    // Neither a hole (Complete) nor a hang (Pending): a state
                    // this code does not know is an I/O error.
                    error!("Stripe {stripe_id} has unknown fetch state {other}");
                    self.metadata_state
                        .spill()
                        .degraded_reasons
                        .fetch_add(1, Ordering::Relaxed);
                    return StripesFetchStatus::Failed { stripe_id };
                }
            }
        }
        StripesFetchStatus::Complete
    }

    /// Every stripe of the request is Fetched or NoSource right now. SeqCst
    /// loads, so that read after `pin_inflight` orders against the evictor's
    /// claim (section 4.2 of the spill spec). Says nothing about why a stripe
    /// is not resident; `request_stripes_fetch_status` does, and logs.
    fn request_stripes_resident(&self, request: &RWRequest) -> bool {
        (request.stripe_id_first..=request.stripe_id_last).all(|stripe_id| {
            matches!(
                self.metadata_state.stripe_fetch_state(stripe_id),
                Fetched | NoSource
            )
        })
    }

    fn request_stripes_written(&self, request: &RWRequest) -> bool {
        for stripe_id in request.stripe_id_first..=request.stripe_id_last {
            if !self.metadata_state.stripe_written(stripe_id) {
                return false;
            }
        }
        true
    }

    /// Pin, check, pass. Returns false with nothing pinned if the request
    /// cannot go to base right now: a stripe is not resident, a write's stripe
    /// is not yet WRITTEN under track_written, or the gate is not open for a
    /// write.
    ///
    /// The pin comes before the check. The evictor claims a stripe with a CAS
    /// and then looks at the in-flight count once; with both sides SeqCst
    /// either it sees this pin, or this check sees its claim. A request is
    /// therefore never on its way to base while the evictor believes the
    /// stripe idle.
    fn try_pass_to_base(&mut self, request: &RWRequest) -> bool {
        let (first, last) = (request.stripe_id_first, request.stripe_id_last);
        self.metadata_state.pin_inflight(first, last);

        let can_pass = self.request_stripes_resident(request)
            && (request.kind == RequestType::In
                || ((!self.track_written || self.request_stripes_written(request))
                    && self.metadata_state.write_gate() == GATE_OPEN));
        if !can_pass {
            self.metadata_state.unpin_inflight(first, last);
            return false;
        }

        // Only a request that actually reaches base references the stripe;
        // a queued one may wait a long time and would age the wrong stripes.
        self.metadata_state.touch(first, last);
        if request.id >= self.pinned_by_id.len() {
            self.pinned_by_id.resize(request.id + 1, None);
        }
        if let Some(slot) = self.pinned_by_id.get_mut(request.id) {
            debug_assert!(slot.is_none(), "request id {} reused in flight", request.id);
            *slot = Some((first, last));
        }
        self.unsubmitted.push(request.id);

        match request.kind {
            RequestType::In => self.base.add_read(
                request.sector_offset,
                request.sector_count,
                request.buf.clone(),
                request.id,
            ),
            RequestType::Out => self.base.add_write(
                request.sector_offset,
                request.sector_count,
                request.buf.clone(),
                request.id,
            ),
        }
        true
    }

    /// Undo the pin recorded for `id`, if there still is one.
    fn release_pin(&mut self, id: usize) {
        if let Some((first, last)) = self.pinned_by_id.get_mut(id).and_then(Option::take) {
            self.metadata_state.unpin_inflight(first, last);
        }
    }

    /// Release the pins of everything handed to base since its last submit:
    /// that submit failed, so base will never complete them.
    fn release_unsubmitted(&mut self) -> Vec<usize> {
        let ids = std::mem::take(&mut self.unsubmitted);
        for id in &ids {
            self.release_pin(*id);
        }
        ids
    }

    fn start_stripe_fetches(&mut self, request: &mut RWRequest) -> Result<()> {
        for stripe_id in request.stripe_id_first..=request.stripe_id_last {
            if !self.metadata_state.stripe_fetched_if_needed(stripe_id)
                && !self.stripe_fetches_requested.contains(&stripe_id)
            {
                self.bgworker_ch
                    .send(BgWorkerRequest::Fetch { stripe_id })
                    .context(format!(
                        "failed to send fetch request for stripe {stripe_id}"
                    ))?;
                self.stripe_fetches_requested.insert(stripe_id);
                request.fetch_sent_at = Some(Instant::now());
            }
        }
        Ok(())
    }

    /// Re-send Fetch for every stripe of a Pending front that is Evicting or
    /// Evicted, at most once per FETCH_RESEND_INTERVAL per request, ignoring
    /// `stripe_fetches_requested`: the Fetch recorded there may have been
    /// consumed by an eviction that then went through, or, for a write that
    /// waited on WRITTEN, never sent at all.
    fn resend_fetches_if_due(&mut self, front: &mut RWRequest) {
        let now = Instant::now();
        if front
            .fetch_sent_at
            .is_some_and(|sent| now.duration_since(sent) < FETCH_RESEND_INTERVAL)
        {
            return;
        }
        let mut sent = false;
        for stripe_id in front.stripe_id_first..=front.stripe_id_last {
            let state = self.metadata_state.stripe_fetch_state(stripe_id);
            if state != Evicting && state != Evicted {
                continue;
            }
            if let Err(e) = self.bgworker_ch.send(BgWorkerRequest::Fetch { stripe_id }) {
                error!("Failed to re-send fetch request for stripe {stripe_id}: {e}");
                return;
            }
            sent = true;
        }
        if sent {
            front.fetch_sent_at = Some(now);
        }
    }

    fn start_stripe_set_written(&mut self, request: &RWRequest) -> Result<()> {
        for stripe_id in request.stripe_id_first..=request.stripe_id_last {
            if !self.metadata_state.stripe_written(stripe_id) {
                self.bgworker_ch
                    .send(BgWorkerRequest::SetWritten { stripe_id })
                    .context(format!(
                        "failed to send set written request for stripe {stripe_id}"
                    ))?;
                // Persisting the bit is the flusher's job and happens later,
                // but the in-memory state is what a fork is served, and a fork
                // that is told this stripe holds nothing will read zeros there
                // rather than fetch it. Say so now.
                self.metadata_state.mark_stripe_written(stripe_id);
            }
        }
        Ok(())
    }

    fn process_queued_rw_requests(&mut self) {
        let mut added_requests = 0;

        while let Some(mut front) = self.queued_rw_requests.pop_front() {
            let gate = self.metadata_state.write_gate();
            match self.request_stripes_fetch_status(&front) {
                StripesFetchStatus::Complete => {}
                StripesFetchStatus::Pending => {
                    if gate == GATE_FAIL {
                        // Waiting on a stripe that is not here, under a gate
                        // that refuses to fetch it: fail rather than hang.
                        self.finished_requests.push((front.id, false));
                        continue;
                    }
                    self.resend_fetches_if_due(&mut front);
                    self.queued_rw_requests.push_front(front);
                    break;
                }
                StripesFetchStatus::Failed { stripe_id } => {
                    self.finished_requests.push((front.id, false));
                    error!("Failed to fetch stripe: {stripe_id}");
                    continue;
                }
            }

            if front.kind == RequestType::Out && gate == GATE_FAIL {
                self.finished_requests.push((front.id, false));
                continue;
            }

            // FIFO: a write waiting on WRITTEN or on the gate, or a request
            // whose stripe the evictor claimed since the check above, holds
            // everything behind it, as it did before.
            if !self.try_pass_to_base(&front) {
                self.queued_rw_requests.push_front(front);
                break;
            }

            for stripe_id in front.stripe_id_first..=front.stripe_id_last {
                self.stripe_fetches_requested.remove(&stripe_id);
            }
            added_requests += 1;
        }

        if added_requests > 0 {
            if let Err(e) = self.base.submit() {
                error!("Failed to submit {added_requests} queued requests: {e}");
                for id in self.release_unsubmitted() {
                    self.finished_requests.push((id, false));
                }
            }
            self.unsubmitted.clear();
        }
    }
}

impl IoChannel for LazyIoChannel {
    fn add_read(&mut self, sector_offset: u64, sector_count: u32, buf: SharedBuffer, id: usize) {
        let mut request = RWRequest {
            id,
            kind: RequestType::In,
            sector_offset,
            sector_count,
            buf: buf.clone(),
            stripe_id_first: self.metadata_state.sector_to_stripe_id(sector_offset),
            stripe_id_last: self
                .metadata_state
                .sector_to_stripe_id(sector_offset + sector_count as u64 - 1),
            fetch_sent_at: None,
        };

        if self.try_pass_to_base(&request) {
            return;
        }

        match self.request_stripes_fetch_status(&request) {
            StripesFetchStatus::Complete => {
                // Resident again already (an eviction aborted between the two
                // checks). Queue it; the next poll passes it.
            }
            StripesFetchStatus::Pending => {
                if request.stripe_id_first == request.stripe_id_last {
                    if let Some(image_channel) = &mut self.image {
                        image_channel.add_read(sector_offset, sector_count, buf, id);
                        return;
                    }
                } else {
                    debug!(
                        "cross_stripe_read: fetching stripes [{}..={}]: offset {} sectors, length {} sectors",
                        request.stripe_id_first, request.stripe_id_last, sector_offset, sector_count
                    );
                }
            }
            StripesFetchStatus::Failed { stripe_id } => {
                error!("Received a read request for a failed stripe: {stripe_id}");
                self.finished_requests.push((id, false));
                return;
            }
        }

        if let Err(e) = self.start_stripe_fetches(&mut request) {
            error!(
                "Failed to send fetch request for stripe range {}-{}: {}",
                request.stripe_id_first, request.stripe_id_last, e
            );
            self.finished_requests.push((id, false));
        } else {
            self.queued_rw_requests.push_back(request);
        }
    }

    fn add_write(&mut self, sector_offset: u64, sector_count: u32, buf: SharedBuffer, id: usize) {
        if self.metadata_state.write_gate() == GATE_FAIL {
            self.finished_requests.push((id, false));
            return;
        }

        let mut request = RWRequest {
            id,
            kind: RequestType::Out,
            sector_offset,
            sector_count,
            buf,
            stripe_id_first: self.metadata_state.sector_to_stripe_id(sector_offset),
            stripe_id_last: self
                .metadata_state
                .sector_to_stripe_id(sector_offset + sector_count as u64 - 1),
            fetch_sent_at: None,
        };

        if self.try_pass_to_base(&request) {
            return;
        }

        if let StripesFetchStatus::Failed { stripe_id } =
            self.request_stripes_fetch_status(&request)
        {
            error!("Received a write request for a failed stripe: {stripe_id}");
            self.finished_requests.push((id, false));
            return;
        }

        if let Err(e) = self.start_stripe_fetches(&mut request) {
            error!(
                "Failed to send fetch request for stripe range {}-{}: {}",
                request.stripe_id_first, request.stripe_id_last, e
            );
            self.finished_requests.push((id, false));
            return;
        }

        if self.track_written {
            if let Err(e) = self.start_stripe_set_written(&request) {
                error!(
                    "Failed to send set written request for stripe range {}-{}: {}",
                    request.stripe_id_first, request.stripe_id_last, e
                );
                self.finished_requests.push((id, false));
                return;
            }
        }

        self.queued_rw_requests.push_back(request);
    }

    fn add_flush(&mut self, id: usize) {
        self.base.add_flush(id);
    }

    fn submit(&mut self) -> Result<()> {
        if let Some(image_channel) = &mut self.image {
            image_channel.submit()?;
        }
        let result = self.base.submit();
        if result.is_err() {
            self.release_unsubmitted();
        }
        self.unsubmitted.clear();
        result
    }

    fn poll(&mut self) -> Vec<(usize, bool)> {
        self.process_queued_rw_requests();

        let mut results = std::mem::take(&mut self.finished_requests);
        for (id, ok) in self.base.poll() {
            self.release_pin(id);
            results.push((id, ok));
        }
        if let Some(image_channel) = &mut self.image {
            results.extend(image_channel.poll());
        }

        results
    }

    fn busy(&self) -> bool {
        self.base.busy()
            || self.image.as_ref().is_some_and(|ch| ch.busy())
            || !self.queued_rw_requests.is_empty()
    }
}

pub struct LazyBlockDevice {
    base: Box<dyn BlockDevice>,
    image: Option<Box<dyn BlockDevice>>,
    bgworker_ch: Sender<BgWorkerRequest>,
    metadata_state: SharedMetadataState,
    track_written: bool,
}

impl LazyBlockDevice {
    pub fn new(
        base: Box<dyn BlockDevice>,
        image: Option<Box<dyn BlockDevice>>,
        bgworker_ch: Sender<BgWorkerRequest>,
        metadata_state: SharedMetadataState,
        track_written: bool,
    ) -> Result<Box<Self>> {
        Ok(Box::new(LazyBlockDevice {
            base,
            image,
            bgworker_ch,
            metadata_state,
            track_written,
        }))
    }
}

impl BlockDevice for LazyBlockDevice {
    fn create_channel(&self) -> Result<Box<dyn IoChannel>> {
        let base_channel = self.base.create_channel()?;
        let image_channel = if let Some(image) = &self.image {
            Some(image.create_channel()?)
        } else {
            None
        };

        Ok(Box::new(LazyIoChannel::new(
            base_channel,
            image_channel,
            self.bgworker_ch.clone(),
            self.metadata_state.clone(),
            self.track_written,
        )))
    }

    fn sector_count(&self) -> u64 {
        self.base.sector_count()
    }

    fn clone(&self) -> Box<dyn BlockDevice> {
        Box::new(LazyBlockDevice {
            base: self.base.clone(),
            image: self.image.clone(),
            bgworker_ch: self.bgworker_ch.clone(),
            metadata_state: self.metadata_state.clone(),
            track_written: self.track_written,
        })
    }
}
