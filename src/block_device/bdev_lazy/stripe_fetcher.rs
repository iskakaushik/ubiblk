use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use log::{debug, error, info, warn};

use super::super::*;

use crate::{
    backends::SECTOR_SIZE,
    block_device::SharedMetadataState,
    stripe_source::{BlockDeviceStripeSource, StripeSource},
    utils::aligned_buffer_pool::AlignedBufferPool,
    Result,
};

const MAX_CONCURRENT_FETCHES: usize = 16;
const MAX_FETCH_RETRIES: u8 = 3;

/// How long a fork keeps retrying a stripe its snapshot server refuses to serve.
///
/// Prod stops serving a stripe the moment it copies it out, and the copy it
/// pushed is already on the wire by then, so the pull that just failed will
/// succeed as soon as that push has been written locally. Failing after three
/// immediate retries loses that race and fails the guest's read — and a failed
/// stripe is permanent, which on a fork means postgres cannot finish recovery.
/// Wait long enough to cover a prod that is busy serving other forks.
const PUSH_WAIT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchState {
    Queued,
    Fetching,
    Flushing,
    Fetched,
}

pub struct StripeFetcher {
    stripe_source: Box<dyn StripeSource>,
    fetch_target_channel: Box<dyn IoChannel>,
    #[cfg_attr(not(test), allow(dead_code))]
    source_sector_count: u64,
    target_sector_count: u64,
    stripe_sector_count: u64,
    fetch_queue: VecDeque<usize>,
    autofetch_queue: VecDeque<usize>,
    buffer_pool: AlignedBufferPool,
    shared_metadata_state: SharedMetadataState,
    stripe_states: HashMap<usize, FetchState>,
    stripe_fetch_retries: HashMap<usize, u8>,
    /// Set when this device forks another one, so a refused pull means "the
    /// push is on its way" rather than "this stripe is unavailable".
    expects_pushes: bool,
    first_failure: HashMap<usize, Instant>,
    allocated_buffers: HashMap<usize, SharedBuffer>,
    /// Stripes the snapshot server pushed while a pull for them was in flight.
    /// Applied once that pull finishes: after a copy-out the server refuses to
    /// serve the stripe, so the pushed copy is the only correct one.
    pending_pushes: HashMap<usize, Vec<u8>>,
    finished_fetches: Vec<(usize, bool)>,
    autofetch: bool,
    disconnected: bool,
}

impl StripeFetcher {
    pub fn new(
        stripe_source: Box<dyn StripeSource>,
        target_dev: &dyn BlockDevice,
        stripe_sector_count: u64,
        shared_metadata_state: SharedMetadataState,
        alignment: usize,
        autofetch: bool,
    ) -> Result<Self> {
        let fetch_target_channel = target_dev.create_channel()?;

        let stripe_size_u64 = stripe_sector_count
            .checked_mul(SECTOR_SIZE as u64)
            .ok_or_else(|| {
                crate::ubiblk_error!(InvalidParameter {
                    description: "stripe size too large".to_string(),
                })
            })?;
        let stripe_size = stripe_size_u64 as usize;

        let buffer_pool = AlignedBufferPool::new(alignment, MAX_CONCURRENT_FETCHES, stripe_size);
        let source_sector_count = stripe_source.sector_count();
        let target_sector_count = target_dev.sector_count();
        if target_sector_count < source_sector_count {
            return Err(crate::ubiblk_error!(InvalidParameter {
                description: format!(
                    "target device too small ({} sectors) for source device ({} sectors)",
                    target_sector_count, source_sector_count
                ),
            }));
        }

        let source_stripe_count = source_sector_count.div_ceil(stripe_sector_count);
        let autofetch_queue = if autofetch {
            (0..source_stripe_count as usize).collect()
        } else {
            VecDeque::new()
        };

        Ok(StripeFetcher {
            stripe_source,
            fetch_target_channel,
            source_sector_count,
            target_sector_count,
            stripe_sector_count,
            fetch_queue: VecDeque::new(),
            buffer_pool,
            shared_metadata_state,
            stripe_states: HashMap::new(),
            stripe_fetch_retries: HashMap::new(),
            expects_pushes: false,
            first_failure: HashMap::new(),
            allocated_buffers: HashMap::new(),
            pending_pushes: HashMap::new(),
            finished_fetches: Vec::new(),
            autofetch,
            autofetch_queue,
            disconnected: false,
        })
    }

    pub fn busy(&self) -> bool {
        !self.fetch_queue.is_empty()
            || self.stripe_source.busy()
            || self.fetch_target_channel.busy()
            || !self.finished_fetches.is_empty()
            || !self.autofetch_queue.is_empty()
    }

    pub fn handle_fetch_request(&mut self, stripe_id: usize) {
        if self
            .shared_metadata_state
            .stripe_fetched_if_needed(stripe_id)
        {
            debug!("Stripe {stripe_id} already fetched or has no source data, skipping fetch");
            return;
        }

        if self.stripe_states.contains_key(&stripe_id) {
            debug!("Stripe {stripe_id} has already been requested");
            return;
        }

        debug!("Enqueueing stripe {stripe_id} for fetch");
        self.fetch_queue.push_back(stripe_id);
        self.stripe_states.insert(stripe_id, FetchState::Queued);
    }

    /// Take a stripe the snapshot server pushed to us: the content this fork
    /// must see, handed over just before prod overwrites it.
    ///
    /// It is written to the target exactly like a fetched stripe, so the same
    /// write/flush/mark-fetched path runs and the fork never pulls it later.
    /// Tell the fetcher that this device subscribes to a snapshot, so stripes
    /// its source refuses are coming over the push channel instead.
    pub fn set_expects_pushes(&mut self, expects_pushes: bool) {
        self.expects_pushes = expects_pushes;
    }

    pub fn accept_pushed_stripe(&mut self, stripe_id: usize, data: &[u8]) {
        if self
            .shared_metadata_state
            .stripe_fetched_if_needed(stripe_id)
        {
            debug!("Stripe {stripe_id} is already local, ignoring the pushed copy");
            return;
        }

        if self.fetch_in_flight(stripe_id) {
            // Once prod has copied a stripe out it stops serving it, so a pull
            // racing with this push cannot succeed. Hold the pushed copy and
            // apply it when that pull gives up.
            debug!("Stripe {stripe_id} has a fetch in flight, holding the pushed copy");
            self.pending_pushes.insert(stripe_id, data.to_vec());
            return;
        }

        self.write_pushed_stripe(stripe_id, data);
    }

    fn fetch_in_flight(&self, stripe_id: usize) -> bool {
        matches!(
            self.stripe_states.get(&stripe_id),
            Some(FetchState::Queued) | Some(FetchState::Fetching) | Some(FetchState::Flushing)
        )
    }

    fn write_pushed_stripe(&mut self, stripe_id: usize, data: &[u8]) {
        // A pull for this stripe may still be queued: it failed because prod had
        // already copied the stripe out, and this is that copy. Retrying it
        // would fail again, and its completion would land on top of this write
        // and be taken for it, losing the only copy the fork can get.
        self.fetch_queue.retain(|queued| *queued != stripe_id);
        self.stripe_fetch_retries.remove(&stripe_id);
        self.first_failure.remove(&stripe_id);

        let Some(buf) = self.buffer_pool.get_buffer() else {
            // Every buffer is busy. Keep the copy: this stripe cannot be pulled
            // any more, so dropping it would lose it for good.
            debug!("No buffer for pushed stripe {stripe_id} yet, keeping it for later");
            self.pending_pushes.insert(stripe_id, data.to_vec());
            return;
        };

        {
            let mut target = buf.borrow_mut();
            let len = data.len().min(target.as_slice().len());
            target.as_mut_slice()[..len].copy_from_slice(&data[..len]);
        }

        self.allocated_buffers.insert(stripe_id, buf.clone());
        self.stripe_states.insert(stripe_id, FetchState::Fetching);

        if !self.start_write(buf, stripe_id) {
            self.fetch_completed(stripe_id, false);
        }
    }

    /// Write out pushed stripes whose racing pull has finished.
    fn apply_pending_pushes(&mut self) {
        if self.pending_pushes.is_empty() {
            return;
        }

        let ready: Vec<usize> = self
            .pending_pushes
            .keys()
            .copied()
            .filter(|stripe_id| !self.fetch_in_flight(*stripe_id))
            .collect();

        for stripe_id in ready {
            let Some(data) = self.pending_pushes.remove(&stripe_id) else {
                continue;
            };
            if self
                .shared_metadata_state
                .stripe_fetched_if_needed(stripe_id)
            {
                continue;
            }
            // The pull failed or was never going to succeed; this is the copy.
            self.write_pushed_stripe(stripe_id, &data);
        }
    }

    pub fn update(&mut self) {
        self.update_autofetch();
        self.apply_pending_pushes();
        self.start_fetches();
        self.poll_fetches();
        self.apply_pending_pushes();
    }

    pub fn disconnect_from_source_if_all_fetched(&mut self) {
        if !self.disconnected
            && !self.busy()
            && self.shared_metadata_state.source_stripes()
                == self.shared_metadata_state.fetched_stripes()
        {
            let prev_source_sector_count = self.stripe_source.sector_count();
            let result =
                BlockDeviceStripeSource::new(NullBlockDevice::new(), self.stripe_sector_count);
            // NullBlockDevice always succeeds, so we can ignore errors here
            if let Ok(source) = result {
                self.stripe_source = Box::new(source);
                if prev_source_sector_count != 0 {
                    info!("All stripes fetched, disconnected from source device");
                }
                self.disconnected = true;
            }
        }
    }

    #[cfg(test)]
    pub fn source_stripe_count(&self) -> u64 {
        self.source_sector_count.div_ceil(self.stripe_sector_count)
    }

    pub fn take_finished_fetches(&mut self) -> Vec<(usize, bool)> {
        std::mem::take(&mut self.finished_fetches)
    }

    pub fn update_autofetch(&mut self) {
        if self.autofetch && self.fetch_queue.is_empty() {
            if let Some(stripe_id) = self.autofetch_queue.pop_front() {
                self.handle_fetch_request(stripe_id);
            }
        }
    }

    fn start_fetches(&mut self) {
        while !self.fetch_queue.is_empty() && self.buffer_pool.has_available() {
            let stripe_id = self.fetch_queue.pop_front().unwrap();

            // The stripe may have arrived while it sat in the queue — pushed to
            // us by the snapshot, or fetched by an earlier request. Pulling it
            // again would be wasted work, and on a fork it fails outright:
            // once prod has copied a stripe out it stops serving it.
            if self
                .shared_metadata_state
                .stripe_fetched_if_needed(stripe_id)
            {
                self.stripe_states.insert(stripe_id, FetchState::Fetched);
                self.first_failure.remove(&stripe_id);
                self.stripe_fetch_retries.remove(&stripe_id);
                continue;
            }

            let buf = self.buffer_pool.get_buffer().unwrap();
            self.allocated_buffers.insert(stripe_id, buf.clone());
            if let Err(e) = self.stripe_source.request(stripe_id, buf.clone()) {
                error!("Failed to request stripe {stripe_id} from source: {e:?}");
                self.fetch_completed(stripe_id, false);
                continue;
            }

            self.stripe_states.insert(stripe_id, FetchState::Fetching);
        }
    }

    fn poll_fetches(&mut self) {
        // Overall fetching logic (assuming things go well):
        // 1. Read from the source channel.
        // 2. Write to the target channel.
        // 3. Flush the target channel.
        // 4. Mark the stripe as fetched in the shared state.

        // Handle completions from the source channel. Did any fetches from the
        // source complete? Start writing the successful ones to the target.
        for (stripe_id, success) in self.stripe_source.poll() {
            let buf = match self.allocated_buffers.get(&stripe_id) {
                Some(buf) => buf.clone(),
                None => {
                    error!("Received completion for unknown stripe {stripe_id}");
                    continue;
                }
            };

            if !success || !self.start_write(buf, stripe_id) {
                self.fetch_completed(stripe_id, false);
            }
        }

        // Handle completions from the target channel. We'll start flushing the
        // ones that were successfully written to the target.
        for (stripe_id, success) in self.fetch_target_channel.poll() {
            if !success {
                self.fetch_completed(stripe_id, false);
                continue;
            }

            match self.stripe_states.get(&stripe_id) {
                Some(FetchState::Fetching) => {
                    debug!("Stripe {stripe_id} write completed, flushing...");
                    if self.start_flush(stripe_id) {
                        self.stripe_states.insert(stripe_id, FetchState::Flushing);
                    } else {
                        self.fetch_completed(stripe_id, false);
                        continue;
                    }
                }
                Some(FetchState::Flushing) => {
                    self.fetch_completed(stripe_id, success);
                }
                _ => {
                    error!("Unexpected state for stripe {stripe_id} after write");
                }
            }
        }
    }

    fn start_write(&mut self, buf: SharedBuffer, stripe_id: usize) -> bool {
        let stripe_sector_offset = stripe_id as u64 * self.stripe_sector_count;
        let stripe_sector_count = self
            .stripe_sector_count
            .min(self.target_sector_count - stripe_sector_offset);

        self.fetch_target_channel.add_write(
            stripe_sector_offset,
            stripe_sector_count as u32,
            buf,
            stripe_id,
        );

        if let Err(e) = self.fetch_target_channel.submit() {
            error!("Failed to submit write for stripe {stripe_id}: {e:?}");
            false
        } else {
            true
        }
    }

    fn start_flush(&mut self, stripe_id: usize) -> bool {
        self.fetch_target_channel.add_flush(stripe_id);

        if let Err(e) = self.fetch_target_channel.submit() {
            error!("Failed to submit flush for stripe {stripe_id}: {e:?}");
            false
        } else {
            true
        }
    }

    fn fetch_completed(&mut self, stripe_id: usize, success: bool) {
        debug!("Fetch completed for stripe {stripe_id}, success={success}");

        if let Some(buf) = self.allocated_buffers.remove(&stripe_id) {
            self.buffer_pool.return_buffer(&buf);
        } else {
            error!("No buffer allocated for stripe {stripe_id} on completion");
        }

        if success {
            self.stripe_states.insert(stripe_id, FetchState::Fetched);
            self.stripe_fetch_retries.remove(&stripe_id);
            self.first_failure.remove(&stripe_id);
            self.finished_fetches.push((stripe_id, true));
            return;
        }

        if self.expects_pushes {
            let since = *self
                .first_failure
                .entry(stripe_id)
                .or_insert_with(Instant::now);
            if since.elapsed() < PUSH_WAIT {
                debug!("Stripe {stripe_id} is not servable yet; waiting for its push");
                self.fetch_queue.push_back(stripe_id);
                self.stripe_states.remove(&stripe_id);
                return;
            }

            if self.pending_pushes.contains_key(&stripe_id) {
                // The push is here, it just could not be written while the pull
                // was outstanding. Let the next update apply it.
                self.stripe_states.remove(&stripe_id);
                return;
            }
        }

        let retries = self.stripe_fetch_retries.entry(stripe_id).or_insert(0);
        if *retries < MAX_FETCH_RETRIES {
            *retries += 1;
            warn!("Retrying stripe {stripe_id}, attempt {retries}");
            self.fetch_queue.push_back(stripe_id);
            self.stripe_states.insert(stripe_id, FetchState::Queued);
        } else {
            error!("Stripe {stripe_id} failed after {MAX_FETCH_RETRIES} retries");
            self.shared_metadata_state.set_stripe_failed(stripe_id);
            self.stripe_states.remove(&stripe_id);
            self.stripe_fetch_retries.remove(&stripe_id);
            self.finished_fetches.push((stripe_id, false));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_device::bdev_test::TestBlockDevice;
    use crate::stripe_source::BlockDeviceStripeSource;

    struct TestState {
        source_dev: Box<TestBlockDevice>,
        target_dev: Box<TestBlockDevice>,
        fetcher: StripeFetcher,
    }

    fn prep(autofetch: bool) -> TestState {
        let source_size: u64 = 1024 * 1024; // 1 MiB
        let target_size: u64 = 2 * 1024 * 1024; // 2 MiB
        let stripe_sector_count_shift = 3; // 8 sectors per stripe
        let stripe_sector_count = 1u64 << stripe_sector_count_shift;

        let source_dev = Box::new(TestBlockDevice::new(source_size));
        let target_dev = Box::new(TestBlockDevice::new(target_size));

        let stripe_source = Box::new(
            BlockDeviceStripeSource::new(source_dev.clone(), stripe_sector_count).unwrap(),
        );

        let metadata = UbiMetadata::new(
            stripe_sector_count_shift,
            target_dev.stripe_count(stripe_sector_count),
            source_dev.stripe_count(stripe_sector_count),
        );

        let shared_metadata_state = SharedMetadataState::new(&metadata);

        let fetcher = StripeFetcher::new(
            stripe_source,
            &*target_dev,
            stripe_sector_count,
            shared_metadata_state.clone(),
            SECTOR_SIZE,
            autofetch,
        )
        .unwrap();

        TestState {
            source_dev,
            target_dev,
            fetcher,
        }
    }

    #[test]
    fn test_basic_fetch() {
        let mut state = prep(false);
        state.fetcher.handle_fetch_request(0);
        for _ in 0..10 {
            state.fetcher.update();
        }
        let finished = state.fetcher.take_finished_fetches();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0], (0, true));

        let source_metrics = state.source_dev.metrics.read().unwrap();
        assert_eq!(source_metrics.reads, 1);
        assert_eq!(source_metrics.writes, 0);
        assert_eq!(source_metrics.flushes, 0);

        let target_metrics = state.target_dev.metrics.read().unwrap();
        assert_eq!(target_metrics.reads, 0);
        assert_eq!(target_metrics.writes, 1);
        assert_eq!(target_metrics.flushes, 1);
    }

    /// A stripe pushed while a pull for it is in flight must not be dropped:
    /// once prod has copied the stripe out it stops serving it, so the pull can
    /// never succeed and the pushed copy is the only correct one.
    #[test]
    fn pushed_stripe_is_applied_after_the_racing_fetch_fails() {
        let mut state = prep(false);
        let pushed = vec![0xAB; (state.fetcher.stripe_sector_count as usize) * SECTOR_SIZE];

        // Start a pull and make it fail, the way a pull for a stripe prod has
        // already copied out fails.
        state
            .source_dev
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state.fetcher.handle_fetch_request(0);
        state.fetcher.update();

        // The push arrives while that pull is still outstanding.
        state.fetcher.accept_pushed_stripe(0, &pushed);
        assert_eq!(
            state.target_dev.metrics.read().unwrap().writes,
            0,
            "the push waits for the pull rather than racing it to the disk"
        );

        for _ in 0..10 {
            state.fetcher.update();
        }

        let mut written = vec![0u8; pushed.len()];
        state.target_dev.read(0, &mut written, pushed.len());
        assert_eq!(
            written, pushed,
            "the pushed copy must reach the disk once the pull is out of the way"
        );
    }

    /// A pull that failed because prod had already copied the stripe out is
    /// left queued for a retry. When the pushed copy then arrives, that retry
    /// has to go away: it would fail again, and its completion would arrive on
    /// top of the push's write and be mistaken for it — leaving the stripe with
    /// no way to ever be filled and failing the guest read for good.
    #[test]
    fn a_queued_retry_is_dropped_when_the_pushed_copy_arrives() {
        let mut state = prep(false);
        state.fetcher.set_expects_pushes(true);
        let pushed = vec![0x5C; (state.fetcher.stripe_sector_count as usize) * SECTOR_SIZE];

        state
            .source_dev
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state.fetcher.handle_fetch_request(0);
        state.fetcher.update();

        state.fetcher.accept_pushed_stripe(0, &pushed);
        for _ in 0..10 {
            state.fetcher.update();
        }

        assert_eq!(
            state.source_dev.metrics.read().unwrap().reads,
            0,
            "the queued retry must not reach the source once the push has arrived"
        );
        let mut written = vec![0u8; pushed.len()];
        state.target_dev.read(0, &mut written, pushed.len());
        assert_eq!(written, pushed, "the pushed copy must reach the disk");
        assert_eq!(
            state.fetcher.take_finished_fetches(),
            vec![(0, true)],
            "and the stripe must end up fetched"
        );
    }

    /// With no pull in flight the push is written straight away.
    /// A stripe that arrives while its fetch is still queued must not be
    /// pulled anyway: on a fork the source stops serving a stripe once it has
    /// been copied out, so the pull would fail and take the guest read with it.
    #[test]
    fn a_queued_fetch_is_dropped_when_the_stripe_arrives_first() {
        let mut state = prep(false);
        state.fetcher.set_expects_pushes(true);

        // Queue a fetch, then let the stripe arrive by push before it starts.
        state.fetcher.handle_fetch_request(0);
        state
            .fetcher
            .shared_metadata_state
            .set_stripe_header(0, metadata_flags::FETCHED);

        for _ in 0..10 {
            state.fetcher.update();
        }

        let source_metrics = state.source_dev.metrics.read().unwrap();
        assert_eq!(
            source_metrics.reads, 0,
            "the queued fetch must not go to the source"
        );
    }

    #[test]
    fn pushed_stripe_is_written_immediately_when_nothing_is_fetching() {
        let mut state = prep(false);
        let pushed = vec![0xCD; (state.fetcher.stripe_sector_count as usize) * SECTOR_SIZE];

        state.fetcher.accept_pushed_stripe(1, &pushed);
        for _ in 0..10 {
            state.fetcher.update();
        }

        let offset = (state.fetcher.stripe_sector_count as usize) * SECTOR_SIZE;
        let mut written = vec![0u8; pushed.len()];
        state.target_dev.read(offset, &mut written, pushed.len());
        assert_eq!(written, pushed);

        let finished = state.fetcher.take_finished_fetches();
        assert_eq!(finished, vec![(1, true)], "it counts as a completed fetch");
    }

    /// A stripe the fork already has locally does not need the pushed copy.
    #[test]
    fn pushed_stripe_is_ignored_when_the_stripe_is_already_local() {
        let mut state = prep(false);
        state.fetcher.handle_fetch_request(0);
        for _ in 0..10 {
            state.fetcher.update();
        }
        state
            .fetcher
            .shared_metadata_state
            .set_stripe_header(0, metadata_flags::FETCHED);
        let writes_before = state.target_dev.metrics.read().unwrap().writes;

        state.fetcher.accept_pushed_stripe(0, &[0xEF; 512]);
        for _ in 0..10 {
            state.fetcher.update();
        }

        assert_eq!(
            state.target_dev.metrics.read().unwrap().writes,
            writes_before
        );
    }

    #[test]
    fn test_repeat_requests_ignored() {
        let mut state = prep(false);
        state.fetcher.handle_fetch_request(1);
        for _ in 0..10 {
            state.fetcher.update();
        }
        let finished = state.fetcher.take_finished_fetches();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0], (1, true));

        state.fetcher.handle_fetch_request(1);
        for _ in 0..10 {
            state.fetcher.update();
        }
        let finished = state.fetcher.take_finished_fetches();
        assert_eq!(finished.len(), 0);

        let source_metrics = state.source_dev.metrics.read().unwrap();
        assert_eq!(source_metrics.reads, 1);
        assert_eq!(source_metrics.writes, 0);
        assert_eq!(source_metrics.flushes, 0);

        let target_metrics = state.target_dev.metrics.read().unwrap();
        assert_eq!(target_metrics.reads, 0);
        assert_eq!(target_metrics.writes, 1);
        assert_eq!(target_metrics.flushes, 1);
    }

    #[test]
    fn test_autofetch() {
        let mut state = prep(true);
        let finished = state.fetcher.take_finished_fetches();
        assert_eq!(finished.len(), 0);
        for _ in 0..1000 {
            state.fetcher.update();
        }
        let mut finished = state.fetcher.take_finished_fetches();
        let source_stripe_count = state.fetcher.source_stripe_count() as usize;
        assert_eq!(finished.len(), source_stripe_count);
        finished.sort_by_key(|(stripe_id, _)| *stripe_id);
        for (idx, (stripe_id, success)) in finished.iter().enumerate() {
            assert!(*stripe_id == idx);
            assert!(success);
        }
    }

    #[test]
    fn test_autofetch_prioritizes_manual() {
        let mut state = prep(true);
        for _ in 0..20 {
            state.fetcher.update();
        }
        let mut finished = state.fetcher.take_finished_fetches();
        assert!(!finished.is_empty());

        // We shouldn't have finished the following stripes in the first 20
        // cycles yet.
        let priority_list = vec![100, 110, 122];
        for stripe_id in &priority_list {
            assert!(finished.iter().all(|(sid, _)| *sid != *stripe_id));
        }

        // Now request those specifically.
        for stripe_id in &priority_list {
            state.fetcher.handle_fetch_request(*stripe_id);
        }

        for _ in 0..20 {
            state.fetcher.update();
        }
        let finished_2nd_batch = state.fetcher.take_finished_fetches();

        // The explicit requests should have been prioritized and completed.
        for stripe_id in &priority_list {
            assert!(finished_2nd_batch[..priority_list.len() + 1]
                .iter()
                .any(|(sid, _)| *sid == *stripe_id));
        }

        finished.extend(finished_2nd_batch);

        // Now process until all the autofetches are done.
        for _ in 0..1000 {
            state.fetcher.update();
        }

        finished.extend(state.fetcher.take_finished_fetches());

        let source_stripe_count = state.fetcher.source_stripe_count() as usize;
        assert_eq!(finished.len(), source_stripe_count);
        finished.sort_by_key(|(stripe_id, _)| *stripe_id);
        for (idx, (stripe_id, success)) in finished.iter().enumerate() {
            assert!(*stripe_id == idx);
            assert!(success);
        }
    }

    #[test]
    fn test_retry_logic() {
        let mut state = prep(false);
        let buf = state.fetcher.buffer_pool.get_buffer().unwrap();
        state.fetcher.allocated_buffers.insert(0, buf);

        state.fetcher.fetch_completed(0, false);
        assert!(!state.fetcher.fetch_queue.is_empty());
        state.fetcher.fetch_queue.pop_front();
        assert_eq!(
            state.fetcher.stripe_states.get(&0),
            Some(&FetchState::Queued)
        );
        assert_eq!(state.fetcher.stripe_fetch_retries.get(&0), Some(&1));

        let buf = state.fetcher.buffer_pool.get_buffer().unwrap();
        state.fetcher.allocated_buffers.insert(0, buf);

        assert!(!state.fetcher.shared_metadata_state.is_stripe_failed(0));

        state.fetcher.fetch_completed(0, false);
        assert!(!state.fetcher.fetch_queue.is_empty());
        state.fetcher.fetch_queue.pop_front();
        assert_eq!(state.fetcher.stripe_fetch_retries.get(&0), Some(&2));

        let buf = state.fetcher.buffer_pool.get_buffer().unwrap();
        state.fetcher.allocated_buffers.insert(0, buf);

        assert!(!state.fetcher.shared_metadata_state.is_stripe_failed(0));

        state.fetcher.fetch_completed(0, false);
        assert!(!state.fetcher.fetch_queue.is_empty());
        state.fetcher.fetch_queue.pop_front();
        assert_eq!(state.fetcher.stripe_fetch_retries.get(&0), Some(&3));

        let buf = state.fetcher.buffer_pool.get_buffer().unwrap();
        state.fetcher.allocated_buffers.insert(0, buf);

        state.fetcher.fetch_completed(0, false);
        assert!(state.fetcher.fetch_queue.is_empty());
        assert_eq!(state.fetcher.stripe_states.get(&0), None);
        assert!(state.fetcher.shared_metadata_state.is_stripe_failed(0));
    }

    #[test]
    fn test_disconnects_when_all_fetched() {
        let mut state = prep(false);
        let source_stripe_count = state.fetcher.source_stripe_count() as usize;

        // Not done yet
        state.fetcher.disconnect_from_source_if_all_fetched();
        assert_ne!(state.fetcher.stripe_source.sector_count(), 0);
        assert!(!state.fetcher.disconnected);

        // Mark all fetched
        for stripe_id in 0..source_stripe_count {
            state.fetcher.shared_metadata_state.set_stripe_header(
                stripe_id,
                metadata_flags::FETCHED | metadata_flags::HAS_SOURCE,
            );
        }

        // Now should disconnect
        state.fetcher.disconnect_from_source_if_all_fetched();
        assert_eq!(state.fetcher.stripe_source.sector_count(), 0);
        assert!(state.fetcher.disconnected);
    }
}
