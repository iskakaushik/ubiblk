//! Reading spilled stripes back, and routing a fetch to the store or the base
//! source by what the metadata says about the stripe.
//!
//! The composite routes by metadata, never by failure: an evicted stripe whose
//! IN_S3 bit is set is read from the store, an evicted clean stripe is pulled
//! from base only while the snapshot is live and the stripe was not pushed,
//! and everything else is refused rather than guessed at.

use std::{
    collections::{HashMap, HashSet},
    sync::atomic::Ordering,
    sync::Arc,
};

use log::{error, warn};

use super::StripeSource;
use crate::{
    archive::ArchiveStore,
    block_device::{
        spill::{codec::spill_object_name, SpillCodec},
        stripe_flags, Evicted, Evicting, SharedBuffer, SharedMetadataState, SpillCounters,
    },
    Result,
};

/// Stripes from the spill store. One per fetcher, each with its own GET store
/// so a demand read never queues behind the evictor's uploads.
pub struct SpillStripeSource {
    store: Box<dyn ArchiveStore>,
    codec: SpillCodec,
    device_id: String,
    /// Object name to the stripe and buffer waiting for it.
    pending: HashMap<String, (usize, SharedBuffer)>,
    finished: Vec<(usize, bool)>,
    connections: usize,
    counters: Arc<SpillCounters>,
}

impl SpillStripeSource {
    /// `store` is this source's own GET store; `connections` bounds the GETs
    /// it keeps in flight. Counters come from `state`.
    pub fn new(
        store: Box<dyn ArchiveStore>,
        codec: SpillCodec,
        device_id: String,
        connections: usize,
        state: &SharedMetadataState,
    ) -> Self {
        SpillStripeSource {
            store,
            codec,
            device_id,
            pending: HashMap::new(),
            finished: Vec::new(),
            connections,
            counters: state.spill_counters(),
        }
    }

    /// Decode a fetched object into the stripe's buffer. Any failure is a
    /// failed fetch for the guest, never wrong bytes: the codec checks the
    /// index and CRC before it produces plaintext.
    fn finish_get(&mut self, stripe_id: usize, buffer: &SharedBuffer, object: Result<Vec<u8>>) {
        let ok = match object {
            Ok(object) => {
                self.counters
                    .get_bytes
                    .fetch_add(object.len() as u64, Ordering::Relaxed);
                let mut buf = buffer.borrow_mut();
                match self.codec.decode_into(
                    stripe_id,
                    &object,
                    buf.as_mut_slice(),
                    Some(&self.counters),
                ) {
                    Ok(_) => true,
                    Err(e) => {
                        error!("Failed to decode spilled stripe {stripe_id}: {e}");
                        false
                    }
                }
            }
            Err(e) => {
                error!("Failed to fetch spilled stripe {stripe_id}: {e}");
                false
            }
        };
        if !ok {
            self.counters.get_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.finished.push((stripe_id, ok));
    }
}

impl StripeSource for SpillStripeSource {
    fn request(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
        let name = spill_object_name(&self.device_id, stripe_id);
        self.store.start_get_object(&name);
        self.counters.gets.fetch_add(1, Ordering::Relaxed);
        if self.pending.insert(name, (stripe_id, buffer)).is_some() {
            // The fetcher deduplicates in-flight pulls, so this is unexpected.
            // The first GET's completion then finds nothing waiting and is
            // dropped; the second completes the stripe.
            warn!("Spilled stripe {stripe_id} requested twice while in flight");
        }
        Ok(())
    }

    fn poll(&mut self) -> Vec<(usize, bool)> {
        for (name, result) in self.store.poll_gets() {
            match self.pending.remove(&name) {
                Some((stripe_id, buffer)) => self.finish_get(stripe_id, &buffer, result),
                None => warn!("Spill store completed {name}, which nothing was waiting for"),
            }
        }
        std::mem::take(&mut self.finished)
    }

    fn busy(&self) -> bool {
        !self.pending.is_empty() || !self.finished.is_empty()
    }

    /// Never the sizing source: the device is sized by base.
    fn sector_count(&self) -> u64 {
        0
    }

    /// Routing is the composite's job.
    fn has_stripe(&self, _stripe_id: usize) -> bool {
        false
    }

    fn max_concurrent_requests(&self) -> usize {
        self.connections
    }
}

/// Routes by metadata, never by failure.
pub struct SpillingStripeSource {
    base: Box<dyn StripeSource>,
    /// None: clean-only configuration, nothing was ever uploaded.
    spill: Option<SpillStripeSource>,
    state: SharedMetadataState,
    /// Immediate refusals.
    finished: Vec<(usize, bool)>,
    /// Stripes refused since they were last routed to a side. On a fork the
    /// fetcher waits for a push before it starts counting retries and asks
    /// again on every pass until then, so one refusal comes back thousands of
    /// times; it is counted and logged once.
    refused: HashSet<usize>,
}

impl SpillingStripeSource {
    /// Wrap `base` (the snapshot or an empty source) with the spill store.
    /// `spill` None is the clean-only configuration.
    pub fn new(
        base: Box<dyn StripeSource>,
        spill: Option<SpillStripeSource>,
        state: SharedMetadataState,
    ) -> Self {
        SpillingStripeSource {
            base,
            spill,
            state,
            finished: Vec::new(),
            refused: HashSet::new(),
        }
    }

    /// Evicted, or Failed carrying WAS_EVICTED (a fetch of the evicted stripe
    /// failed once; the header still says EVICTED and `set_stripe_failed` kept
    /// IN_S3): the stripe is not local, so base must not be asked for it by
    /// default.
    fn formerly_or_still_evicted(&self, stripe_id: usize) -> bool {
        self.state.stripe_fetch_state(stripe_id) == Evicted
            || self.state.stripe_flags(stripe_id) & stripe_flags::WAS_EVICTED != 0
    }

    /// The store holds this stripe's data: IN_S3 is authoritative only while
    /// the stripe is not local (I5).
    fn spilled(&self, stripe_id: usize) -> bool {
        self.formerly_or_still_evicted(stripe_id) && self.state.stripe_in_s3(stripe_id)
    }

    /// Complete `stripe_id` with `(id, false)`. True the first time the stripe
    /// is refused since it was last routed somewhere, so the caller counts and
    /// logs the refusal once rather than once per fetcher retry.
    fn refuse(&mut self, stripe_id: usize) -> bool {
        self.finished.push((stripe_id, false));
        self.refused.insert(stripe_id)
    }

    /// The immediate `(id, false)` refusals below end in Failed through the
    /// fetcher: an I/O error for the guest rather than wrong data. On a fork
    /// the fetcher first waits PUSH_WAIT for a push, re-asking on every pass,
    /// and only then runs its bounded retries; a refusal is decided by
    /// metadata and comes back the same way each time, so `refuse` reports
    /// whether it is new and only a new one is counted or logged.
    fn route(&mut self, stripe_id: usize, buffer: SharedBuffer, demand: bool) -> Result<()> {
        if self.spilled(stripe_id) {
            return match &mut self.spill {
                Some(spill) => {
                    self.refused.remove(&stripe_id);
                    spill.request(stripe_id, buffer)
                }
                None => {
                    if self.refuse(stripe_id) {
                        // A clean-only configuration never uploads, so an
                        // IN_S3 stripe here is an invariant violation, not a
                        // miss.
                        error!(
                            "Stripe {stripe_id} is marked IN_S3 but no spill store is configured"
                        );
                        self.state
                            .spill()
                            .degraded_reasons
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(())
                }
            };
        }
        if self.formerly_or_still_evicted(stripe_id)
            && (self.state.stripe_pushed(stripe_id) || !self.state.source_live())
        {
            // Never ask the replica for post-snapshot data: a pushed stripe
            // is refused by the server, and a dead subscription means the
            // stripe may have been copied out since.
            if self.refuse(stripe_id) {
                self.state
                    .spill()
                    .clean_unrecoverable
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        }
        if self.state.stripe_fetch_state(stripe_id) == Evicting {
            if self.refuse(stripe_id) {
                error!(
                    "Stripe {stripe_id} requested while Evicting; refusing to pull over local data"
                );
                self.state
                    .spill()
                    .degraded_reasons
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        }
        self.refused.remove(&stripe_id);
        if demand {
            self.base.request_demand(stripe_id, buffer)
        } else {
            self.base.request(stripe_id, buffer)
        }
    }
}

impl StripeSource for SpillingStripeSource {
    fn request(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
        self.route(stripe_id, buffer, false)
    }

    /// The spill store has no demand lane; base keeps its own.
    fn request_demand(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
        self.route(stripe_id, buffer, true)
    }

    /// A stripe is outstanding on exactly one side, so the results merge.
    fn poll(&mut self) -> Vec<(usize, bool)> {
        let mut results = std::mem::take(&mut self.finished);
        results.extend(self.base.poll());
        if let Some(spill) = &mut self.spill {
            results.extend(spill.poll());
        }
        results
    }

    fn busy(&self) -> bool {
        !self.finished.is_empty()
            || self.base.busy()
            || self.spill.as_ref().is_some_and(|spill| spill.busy())
    }

    /// Base sizes the device, so the flusher's and fetcher's checks are
    /// unchanged by the wrapping.
    fn sector_count(&self) -> u64 {
        self.base.sector_count()
    }

    fn has_stripe(&self, stripe_id: usize) -> bool {
        self.base.has_stripe(stripe_id) || self.spilled(stripe_id)
    }

    fn max_concurrent_requests(&self) -> usize {
        self.base.max_concurrent_requests()
            + self
                .spill
                .as_ref()
                .map_or(0, |spill| spill.max_concurrent_requests())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::{
        archive::{ArchiveCompressionAlgorithm, TestObjectStore},
        backends::SECTOR_SIZE,
        block_device::{
            bdev_test::TestBlockDevice, metadata_flags, shared_buffer, BlockDevice, UbiMetadata,
        },
        stripe_source::BlockDeviceStripeSource,
    };

    const STRIPE_SECTORS: u64 = 8;
    const STRIPE_SIZE: usize = STRIPE_SECTORS as usize * SECTOR_SIZE;
    const STRIPES: usize = 4;
    const DEVICE_ID: &str = "dev";

    fn codec() -> SpillCodec {
        SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS)
    }

    /// Four stripes with the given header bytes; the rest are plain source
    /// stripes that were never fetched.
    fn state_with(headers: &[(usize, u8)]) -> SharedMetadataState {
        let mut metadata = UbiMetadata::new(3, STRIPES, STRIPES);
        for (stripe_id, header) in headers {
            metadata.set_stripe_header(*stripe_id, *header);
        }
        SharedMetadataState::new(&metadata)
    }

    const SPILLED: u8 =
        metadata_flags::HAS_SOURCE | metadata_flags::EVICTED | metadata_flags::IN_S3;
    const EVICTED_CLEAN: u8 = metadata_flags::HAS_SOURCE | metadata_flags::EVICTED;

    fn stripe_data(seed: u8) -> Vec<u8> {
        (0..STRIPE_SIZE)
            .map(|i| (i as u8).wrapping_mul(13).wrapping_add(seed))
            .collect()
    }

    /// Put an encoded object for `stripe_id` into `objects`.
    fn put_object(objects: &Arc<Mutex<HashMap<String, Vec<u8>>>>, stripe_id: usize, data: &[u8]) {
        let object = codec().encode(stripe_id, data, None).unwrap();
        objects
            .lock()
            .unwrap()
            .insert(spill_object_name(DEVICE_ID, stripe_id), object);
    }

    fn spill_over(
        objects: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
        state: &SharedMetadataState,
        connections: usize,
    ) -> SpillStripeSource {
        SpillStripeSource::new(
            Box::new(TestObjectStore::shared(objects.clone())),
            codec(),
            DEVICE_ID.to_string(),
            connections,
            state,
        )
    }

    /// A base that records what it was asked for and completes every request
    /// with `BASE_FILL`, so a test can tell which side served a stripe.
    const BASE_FILL: u8 = 0xB5;

    #[derive(Default)]
    struct RecordingBase {
        /// (stripe, demand) in request order.
        requests: Vec<(usize, bool)>,
        pending: Vec<(usize, SharedBuffer)>,
    }

    impl StripeSource for RecordingBase {
        fn request(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
            self.requests.push((stripe_id, false));
            self.pending.push((stripe_id, buffer));
            Ok(())
        }

        fn request_demand(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
            self.requests.push((stripe_id, true));
            self.pending.push((stripe_id, buffer));
            Ok(())
        }

        fn poll(&mut self) -> Vec<(usize, bool)> {
            self.pending
                .drain(..)
                .map(|(stripe_id, buffer)| {
                    buffer.borrow_mut().as_mut_slice().fill(BASE_FILL);
                    (stripe_id, true)
                })
                .collect()
        }

        fn busy(&self) -> bool {
            !self.pending.is_empty()
        }

        fn sector_count(&self) -> u64 {
            STRIPES as u64 * STRIPE_SECTORS
        }

        fn has_stripe(&self, stripe_id: usize) -> bool {
            stripe_id < STRIPES
        }
    }

    /// A composite over a recording base and a spill store sharing `objects`;
    /// the base is reachable through the returned handle.
    fn composite(
        objects: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
        state: &SharedMetadataState,
    ) -> (SpillingStripeSource, Arc<Mutex<RecordingBase>>) {
        let base = Arc::new(Mutex::new(RecordingBase::default()));
        let source = SpillingStripeSource::new(
            Box::new(SharedBase(base.clone())),
            Some(spill_over(objects, state, 2)),
            state.clone(),
        );
        (source, base)
    }

    /// The composite owns its base, so the test reaches the recorder through
    /// this handle.
    struct SharedBase(Arc<Mutex<RecordingBase>>);

    impl StripeSource for SharedBase {
        fn request(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
            self.0.lock().unwrap().request(stripe_id, buffer)
        }
        fn request_demand(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
            self.0.lock().unwrap().request_demand(stripe_id, buffer)
        }
        fn poll(&mut self) -> Vec<(usize, bool)> {
            self.0.lock().unwrap().poll()
        }
        fn busy(&self) -> bool {
            self.0.lock().unwrap().busy()
        }
        fn sector_count(&self) -> u64 {
            self.0.lock().unwrap().sector_count()
        }
        fn has_stripe(&self, stripe_id: usize) -> bool {
            self.0.lock().unwrap().has_stripe(stripe_id)
        }
    }

    fn objects() -> Arc<Mutex<HashMap<String, Vec<u8>>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn poll_until_done(source: &mut dyn StripeSource) -> Vec<(usize, bool)> {
        let mut results = source.poll();
        while source.busy() {
            results.extend(source.poll());
        }
        results
    }

    #[test]
    fn spill_source_reads_an_object_and_counts_it() {
        let objects = objects();
        let state = state_with(&[(1, SPILLED)]);
        let data = stripe_data(1);
        put_object(&objects, 1, &data);
        let mut source = spill_over(&objects, &state, 3);
        assert_eq!(source.sector_count(), 0);
        assert!(!source.has_stripe(1), "routing is the composite's job");
        assert_eq!(source.max_concurrent_requests(), 3);
        assert!(!source.busy());

        let buffer = shared_buffer(STRIPE_SIZE);
        source.request(1, buffer.clone()).unwrap();
        assert!(source.busy());
        assert_eq!(poll_until_done(&mut source), vec![(1, true)]);
        assert!(!source.busy());
        assert_eq!(buffer.borrow().as_slice(), &data[..]);

        let counters = state.spill();
        assert_eq!(counters.gets.load(Ordering::Relaxed), 1);
        assert_eq!(counters.get_failures.load(Ordering::Relaxed), 0);
        let object_len = objects.lock().unwrap()[&spill_object_name(DEVICE_ID, 1)].len();
        assert_eq!(
            counters.get_bytes.load(Ordering::Relaxed),
            object_len as u64
        );
        assert!(counters.decode_ns.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn in_s3_routes_to_the_spill_store_and_base_sees_nothing() {
        let objects = objects();
        let state = state_with(&[(1, SPILLED)]);
        let data = stripe_data(7);
        put_object(&objects, 1, &data);
        let (mut source, base) = composite(&objects, &state);

        let buffer = shared_buffer(STRIPE_SIZE);
        source.request(1, buffer.clone()).unwrap();
        assert_eq!(poll_until_done(&mut source), vec![(1, true)]);
        assert_eq!(buffer.borrow().as_slice(), &data[..]);
        assert!(base.lock().unwrap().requests.is_empty());
        assert_eq!(state.spill().gets.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn failed_with_was_evicted_in_s3_routes_to_the_spill_store() {
        let objects = objects();
        let state = state_with(&[(1, SPILLED)]);
        // A fetch of the evicted stripe failed for good: Failed, WAS_EVICTED,
        // IN_S3 intact. Routing it to base would pull the snapshot's pre-image
        // over the fork's data.
        state.set_stripe_failed(1);
        assert_ne!(state.stripe_fetch_state(1), Evicted);
        assert_ne!(state.stripe_flags(1) & stripe_flags::WAS_EVICTED, 0);
        let data = stripe_data(9);
        put_object(&objects, 1, &data);
        let (mut source, base) = composite(&objects, &state);

        assert!(source.has_stripe(1));
        let buffer = shared_buffer(STRIPE_SIZE);
        source.request(1, buffer.clone()).unwrap();
        assert_eq!(poll_until_done(&mut source), vec![(1, true)]);
        assert_eq!(buffer.borrow().as_slice(), &data[..]);
        assert!(base.lock().unwrap().requests.is_empty());
    }

    #[test]
    fn not_in_s3_routes_to_base() {
        let objects = objects();
        // Stripe 1 was never fetched; stripe 2 was evicted clean while the
        // snapshot is live and not pushed, so the replica can serve it again.
        let state = state_with(&[(2, EVICTED_CLEAN)]);
        state.set_source_live(true);
        let (mut source, base) = composite(&objects, &state);

        let buffer = shared_buffer(STRIPE_SIZE);
        source.request(1, buffer.clone()).unwrap();
        source.request(2, shared_buffer(STRIPE_SIZE)).unwrap();
        let mut results = poll_until_done(&mut source);
        results.sort();
        assert_eq!(results, vec![(1, true), (2, true)]);
        assert!(buffer.borrow().as_slice().iter().all(|b| *b == BASE_FILL));
        assert_eq!(base.lock().unwrap().requests, vec![(1, false), (2, false)]);
        assert_eq!(state.spill().gets.load(Ordering::Relaxed), 0);
        assert_eq!(state.spill().clean_unrecoverable.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn evicted_clean_with_dead_source_is_refused_immediately() {
        let objects = objects();
        let state = state_with(&[(2, EVICTED_CLEAN)]);
        assert!(!state.source_live(), "starts false");
        let (mut source, base) = composite(&objects, &state);

        source.request(2, shared_buffer(STRIPE_SIZE)).unwrap();
        assert!(source.busy());
        assert_eq!(source.poll(), vec![(2, false)]);
        assert!(!source.busy());
        assert!(base.lock().unwrap().requests.is_empty());
        assert_eq!(state.spill().gets.load(Ordering::Relaxed), 0);
        assert_eq!(state.spill().clean_unrecoverable.load(Ordering::Relaxed), 1);
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn evicted_pushed_is_refused_immediately() {
        let objects = objects();
        let state = state_with(&[(2, EVICTED_CLEAN | metadata_flags::PUSHED)]);
        state.set_source_live(true);
        let (mut source, base) = composite(&objects, &state);

        // The demand lane refuses the same way.
        source
            .request_demand(2, shared_buffer(STRIPE_SIZE))
            .unwrap();
        assert_eq!(source.poll(), vec![(2, false)]);
        assert!(base.lock().unwrap().requests.is_empty());
        assert_eq!(state.spill().clean_unrecoverable.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn evicting_is_refused_with_error() {
        let objects = objects();
        let state = state_with(&[(1, metadata_flags::HAS_SOURCE | metadata_flags::FETCHED)]);
        state.set_source_live(true);
        assert!(state.try_begin_evicting(1).is_some());
        assert_eq!(state.stripe_fetch_state(1), Evicting);
        let (mut source, base) = composite(&objects, &state);

        source.request(1, shared_buffer(STRIPE_SIZE)).unwrap();
        assert_eq!(source.poll(), vec![(1, false)]);
        assert!(base.lock().unwrap().requests.is_empty());
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 1);
        assert_eq!(state.spill().clean_unrecoverable.load(Ordering::Relaxed), 0);
    }

    /// On a fork the fetcher re-asks for a refused stripe on every pass for
    /// PUSH_WAIT before its bounded retries begin. The refusal is decided by
    /// metadata, so every pass gets the same answer; the counters and the
    /// error log record the stripe once, not once per pass, until the stripe
    /// is routed somewhere again.
    #[test]
    fn a_repeated_refusal_is_counted_once_until_the_stripe_is_routed_again() {
        let objects = objects();
        let state = state_with(&[
            (1, metadata_flags::HAS_SOURCE | metadata_flags::FETCHED),
            (2, EVICTED_CLEAN),
            (3, EVICTED_CLEAN | metadata_flags::PUSHED),
        ]);
        let (mut source, base) = composite(&objects, &state);
        let counters = state.spill();

        // Dead source: stripe 2 is refused on every pass, counted once.
        for _ in 0..5 {
            source.request(2, shared_buffer(STRIPE_SIZE)).unwrap();
            assert_eq!(source.poll(), vec![(2, false)]);
        }
        assert_eq!(counters.clean_unrecoverable.load(Ordering::Relaxed), 1);

        // A different stripe is a different refusal.
        source.request(3, shared_buffer(STRIPE_SIZE)).unwrap();
        assert_eq!(source.poll(), vec![(3, false)]);
        assert_eq!(counters.clean_unrecoverable.load(Ordering::Relaxed), 2);

        // The source comes back for stripe 2 (the flag never does in
        // production, but a route to base is what ends a refusal): the next
        // refusal after that counts again.
        state.set_source_live(true);
        source.request(2, shared_buffer(STRIPE_SIZE)).unwrap();
        assert_eq!(poll_until_done(&mut source), vec![(2, true)]);
        assert_eq!(base.lock().unwrap().requests, vec![(2, false)]);
        state.set_source_live(false);
        source.request(2, shared_buffer(STRIPE_SIZE)).unwrap();
        assert_eq!(source.poll(), vec![(2, false)]);
        assert_eq!(counters.clean_unrecoverable.load(Ordering::Relaxed), 3);

        // The Evicting arm counts degraded_reasons the same way.
        assert!(state.try_begin_evicting(1).is_some());
        for _ in 0..5 {
            source.request(1, shared_buffer(STRIPE_SIZE)).unwrap();
            assert_eq!(source.poll(), vec![(1, false)]);
        }
        assert_eq!(counters.degraded_reasons.load(Ordering::Relaxed), 1);
        assert_eq!(counters.clean_unrecoverable.load(Ordering::Relaxed), 3);
    }

    /// The clean-only arm counts its invariant violation once as well.
    #[test]
    fn a_repeated_no_store_refusal_is_counted_once() {
        let state = state_with(&[(1, SPILLED)]);
        let base = Arc::new(Mutex::new(RecordingBase::default()));
        let mut source =
            SpillingStripeSource::new(Box::new(SharedBase(base.clone())), None, state.clone());

        for _ in 0..5 {
            source.request(1, shared_buffer(STRIPE_SIZE)).unwrap();
            assert_eq!(source.poll(), vec![(1, false)]);
        }
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 1);
        assert!(base.lock().unwrap().requests.is_empty());
    }

    #[test]
    fn in_s3_without_a_spill_store_is_refused() {
        let state = state_with(&[(1, SPILLED)]);
        let base = Arc::new(Mutex::new(RecordingBase::default()));
        let mut source =
            SpillingStripeSource::new(Box::new(SharedBase(base.clone())), None, state.clone());
        assert_eq!(source.max_concurrent_requests(), 1);

        source.request(1, shared_buffer(STRIPE_SIZE)).unwrap();
        assert_eq!(source.poll(), vec![(1, false)]);
        assert!(base.lock().unwrap().requests.is_empty());
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn decode_failure_completes_false_and_counts_get_failure() {
        let objects = objects();
        let state = state_with(&[(1, SPILLED), (2, SPILLED), (3, SPILLED)]);
        // Stripe 1: a corrupt object. Stripe 2: an object that belongs to
        // another stripe. Stripe 3: no object at all.
        let mut corrupt = codec().encode(1, &stripe_data(1), None).unwrap();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xFF;
        objects
            .lock()
            .unwrap()
            .insert(spill_object_name(DEVICE_ID, 1), corrupt);
        let elsewhere = codec().encode(0, &stripe_data(0), None).unwrap();
        objects
            .lock()
            .unwrap()
            .insert(spill_object_name(DEVICE_ID, 2), elsewhere);
        let (mut source, base) = composite(&objects, &state);

        let buffers: Vec<SharedBuffer> = (0..3)
            .map(|_| {
                let buffer = shared_buffer(STRIPE_SIZE);
                buffer.borrow_mut().as_mut_slice().fill(0xAA);
                buffer
            })
            .collect();
        for (stripe_id, buffer) in (1..=3).zip(&buffers) {
            source.request(stripe_id, buffer.clone()).unwrap();
        }
        let mut results = poll_until_done(&mut source);
        results.sort();
        assert_eq!(results, vec![(1, false), (2, false), (3, false)]);
        for buffer in &buffers {
            assert!(
                buffer.borrow().as_slice().iter().all(|b| *b == 0xAA),
                "a failed decode leaves the buffer untouched"
            );
        }
        assert!(base.lock().unwrap().requests.is_empty());
        assert_eq!(state.spill().gets.load(Ordering::Relaxed), 3);
        assert_eq!(state.spill().get_failures.load(Ordering::Relaxed), 3);
    }

    fn base_over(device: &TestBlockDevice) -> Box<dyn StripeSource> {
        Box::new(BlockDeviceStripeSource::new(BlockDevice::clone(device), STRIPE_SECTORS).unwrap())
    }

    #[test]
    fn poll_merges_both_sides() {
        let objects = objects();
        let state = state_with(&[(1, SPILLED), (3, EVICTED_CLEAN)]);
        let spilled = stripe_data(3);
        put_object(&objects, 1, &spilled);
        let device = TestBlockDevice::new(STRIPES as u64 * STRIPE_SECTORS * SECTOR_SIZE as u64);
        let local = stripe_data(5);
        device.write(2 * STRIPE_SIZE, &local, STRIPE_SIZE);
        let mut source = SpillingStripeSource::new(
            base_over(&device),
            Some(spill_over(&objects, &state, 2)),
            state.clone(),
        );
        assert_eq!(source.max_concurrent_requests(), 1 + 2);
        assert_eq!(source.sector_count(), STRIPES as u64 * STRIPE_SECTORS);

        // Stripe 1 from the store, stripe 2 from base, stripe 3 refused (a
        // clean eviction with the source dead).
        let from_store = shared_buffer(STRIPE_SIZE);
        let from_base = shared_buffer(STRIPE_SIZE);
        source.request(1, from_store.clone()).unwrap();
        source.request(2, from_base.clone()).unwrap();
        source.request(3, shared_buffer(STRIPE_SIZE)).unwrap();
        assert!(source.busy());
        let mut results = poll_until_done(&mut source);
        results.sort();
        assert_eq!(results, vec![(1, true), (2, true), (3, false)]);
        assert!(!source.busy());
        assert_eq!(from_store.borrow().as_slice(), &spilled[..]);
        assert_eq!(from_base.borrow().as_slice(), &local[..]);
    }

    #[test]
    fn has_stripe_is_union() {
        let objects = objects();
        let state = state_with(&[(3, SPILLED), (2, EVICTED_CLEAN)]);
        // Base holds two stripes; the state describes four.
        let device = TestBlockDevice::new(2 * STRIPE_SECTORS * SECTOR_SIZE as u64);
        let source = SpillingStripeSource::new(
            base_over(&device),
            Some(spill_over(&objects, &state, 1)),
            state.clone(),
        );
        assert!(source.has_stripe(0), "base");
        assert!(source.has_stripe(1), "base");
        assert!(!source.has_stripe(2), "evicted clean: neither side");
        assert!(source.has_stripe(3), "spilled");

        // IN_S3 on a resident stripe is a purge hint, not a claim (I5).
        state.mark_stripe_resident(3);
        assert!(state.stripe_in_s3(3));
        assert!(!source.has_stripe(3));
    }

    #[test]
    fn short_last_stripe_is_zero_filled() {
        let objects = objects();
        let state = state_with(&[(3, SPILLED)]);
        let short = stripe_data(11)[..SECTOR_SIZE].to_vec();
        put_object(&objects, 3, &short);
        let (mut source, _base) = composite(&objects, &state);

        let buffer = shared_buffer(STRIPE_SIZE);
        buffer.borrow_mut().as_mut_slice().fill(0xAA);
        source.request(3, buffer.clone()).unwrap();
        assert_eq!(poll_until_done(&mut source), vec![(3, true)]);
        let buf = buffer.borrow();
        assert_eq!(&buf.as_slice()[..SECTOR_SIZE], &short[..]);
        assert!(buf.as_slice()[SECTOR_SIZE..].iter().all(|b| *b == 0));
    }

    #[test]
    fn demand_routing_uses_base_request_demand() {
        let objects = objects();
        let state = state_with(&[(1, SPILLED)]);
        put_object(&objects, 1, &stripe_data(1));
        let (mut source, base) = composite(&objects, &state);

        source
            .request_demand(2, shared_buffer(STRIPE_SIZE))
            .unwrap();
        source.request(3, shared_buffer(STRIPE_SIZE)).unwrap();
        // The store has no demand lane: a demand request for a spilled stripe
        // is an ordinary GET.
        source
            .request_demand(1, shared_buffer(STRIPE_SIZE))
            .unwrap();
        let mut results = poll_until_done(&mut source);
        results.sort();
        assert_eq!(results, vec![(1, true), (2, true), (3, true)]);
        assert_eq!(base.lock().unwrap().requests, vec![(2, true), (3, false)]);
        assert_eq!(state.spill().gets.load(Ordering::Relaxed), 1);
    }
}
