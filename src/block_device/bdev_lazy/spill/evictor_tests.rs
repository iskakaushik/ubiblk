//! Tests of the eviction state machine, against a real `MetadataFlusher` over
//! a `TestBlockDevice`, a `TestObjectStore` and a `RecordingPuncher`, so the
//! order of upload, header write, header flush and punch is asserted through
//! the devices' own counters rather than a scripted double.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering},
        mpsc::{channel, Sender},
        Arc, Mutex,
    },
    time::Duration,
};

use nix::errno::Errno;

use super::{
    super::{
        metadata::{Evicted, Evicting, Failed, Fetched, NoSource, NotFetched},
        metadata_flusher::MetadataFlusher,
    },
    codec::SpillCodec,
    evictor::{Evictor, EvictorConfig, FetchDisposition, Kind, PushDisposition, Stage},
    punch::RecordingPuncher,
};
use crate::{
    archive::{ArchiveCompressionAlgorithm, ArchiveStore, TestObjectStore},
    backends::SECTOR_SIZE,
    block_device::{
        bdev_test::TestBlockDevice, metadata_flags, shared_buffer, stripe_flags, BgWorker,
        BgWorkerRequest, BlockDevice, IoChannel, LazyBlockDevice, PushGate, PushPermit,
        SharedBuffer, SharedMetadataState, UbiMetadata, GATE_FAIL, GATE_HOLD, GATE_OPEN,
    },
    config::v2::spill::OnFull,
    stripe_source::BlockDeviceStripeSource,
    Result,
};

const STRIPE_SECTORS: u64 = 8;
const STRIPE_BYTES: u64 = STRIPE_SECTORS * SECTOR_SIZE as u64;
const STRIPES: usize = 8;
const TARGET_SECTORS: u64 = STRIPE_SECTORS * STRIPES as u64;
const DEVICE_ID: &str = "fork-1";

/// A resident stripe the fork has written: it has to be uploaded.
const DIRTY: u8 = metadata_flags::FETCHED | metadata_flags::WRITTEN | metadata_flags::HAS_SOURCE;
/// A resident copy of a snapshot stripe, untouched.
const CLEAN: u8 = metadata_flags::FETCHED | metadata_flags::HAS_SOURCE;

fn object_name(stripe_id: usize) -> String {
    format!("{DEVICE_ID}/{stripe_id}")
}

fn stripes_bytes(stripes: u64) -> u64 {
    stripes * STRIPE_BYTES
}

/// The evictor owns its store, so a test that must hold, release or fail an
/// upload after construction reaches the same store through this handle.
struct SharedStore(Arc<Mutex<TestObjectStore>>);

impl ArchiveStore for SharedStore {
    fn start_put_object(&mut self, name: &str, data: Vec<u8>) {
        self.0.lock().unwrap().start_put_object(name, data)
    }

    fn start_get_object(&mut self, name: &str) {
        self.0.lock().unwrap().start_get_object(name)
    }

    fn poll_puts(&mut self) -> Vec<(String, Result<()>)> {
        self.0.lock().unwrap().poll_puts()
    }

    fn poll_gets(&mut self) -> Vec<(String, Result<Vec<u8>>)> {
        self.0.lock().unwrap().poll_gets()
    }
}

/// A metadata device whose next flush can be made to fail on its own.
/// `TestBlockDevice::fail_next` cannot express that: the flusher issues the
/// flush in the same tick as the write, so an armed flag fails the write.
struct FlushFailingDevice {
    inner: Arc<TestBlockDevice>,
    fail_next_flush: Arc<AtomicBool>,
}

impl FlushFailingDevice {
    fn new(inner: TestBlockDevice) -> Self {
        FlushFailingDevice {
            inner: Arc::new(inner),
            fail_next_flush: Arc::new(AtomicBool::new(false)),
        }
    }

    fn fail_next_flush(&self) {
        self.fail_next_flush.store(true, Ordering::SeqCst);
    }

    fn io_counts(&self) -> (usize, usize) {
        let metrics = self.inner.metrics.read().unwrap();
        (metrics.writes, metrics.flushes)
    }

    fn header(&self, stripe_id: usize) -> u8 {
        UbiMetadata::load_from_bdev(&*self.inner)
            .expect("load metadata")
            .stripe_header(stripe_id)
    }
}

struct FlushFailingChannel {
    inner: Box<dyn IoChannel>,
    fail_next_flush: Arc<AtomicBool>,
    failed: Vec<(usize, bool)>,
}

impl IoChannel for FlushFailingChannel {
    fn add_read(&mut self, sector_offset: u64, sector_count: u32, buf: SharedBuffer, id: usize) {
        self.inner.add_read(sector_offset, sector_count, buf, id)
    }

    fn add_write(&mut self, sector_offset: u64, sector_count: u32, buf: SharedBuffer, id: usize) {
        self.inner.add_write(sector_offset, sector_count, buf, id)
    }

    fn add_flush(&mut self, id: usize) {
        if self.fail_next_flush.swap(false, Ordering::SeqCst) {
            self.failed.push((id, false));
        } else {
            self.inner.add_flush(id)
        }
    }

    fn submit(&mut self) -> Result<()> {
        self.inner.submit()
    }

    fn poll(&mut self) -> Vec<(usize, bool)> {
        let mut finished = self.inner.poll();
        finished.append(&mut self.failed);
        finished
    }

    fn busy(&self) -> bool {
        self.inner.busy() || !self.failed.is_empty()
    }
}

impl BlockDevice for FlushFailingDevice {
    fn create_channel(&self) -> Result<Box<dyn IoChannel>> {
        Ok(Box::new(FlushFailingChannel {
            inner: self.inner.create_channel()?,
            fail_next_flush: self.fail_next_flush.clone(),
            failed: Vec::new(),
        }))
    }

    fn sector_count(&self) -> u64 {
        self.inner.sector_count()
    }

    fn clone(&self) -> Box<dyn BlockDevice> {
        Box::new(FlushFailingDevice {
            inner: Arc::clone(&self.inner),
            fail_next_flush: self.fail_next_flush.clone(),
        })
    }
}

fn base_config(target_sector_count: u64) -> EvictorConfig {
    EvictorConfig {
        data_path: "/tmp/device.raw".into(),
        device_id: DEVICE_ID.to_string(),
        stripe_sector_count: STRIPE_SECTORS,
        target_sector_count,
        max_local_bytes: stripes_bytes(STRIPES as u64),
        low_water_bytes: 0,
        hard_margin_bytes: STRIPE_BYTES,
        min_free_bytes: stripes_bytes(4),
        clean_eviction: false,
        on_full: OnFull::Stall,
        max_concurrent_evictions: 2,
        sweep_batch: 4096,
        alignment: 4096,
    }
}

/// Metadata for `STRIPES` stripes, every one with a source, with the given
/// headers overriding the default of NotFetched.
fn metadata_with(headers: &[(usize, u8)]) -> Box<UbiMetadata> {
    let mut metadata = UbiMetadata::new(3, STRIPES, STRIPES);
    for (stripe_id, header) in headers {
        metadata.set_stripe_header(*stripe_id, *header);
    }
    metadata
}

/// Every stripe of the target holds its own pattern, so an object can be
/// checked against what was read.
fn pattern(stripe_id: usize) -> u8 {
    stripe_id as u8 + 1
}

fn fill_with_patterns(dev: &TestBlockDevice) {
    let mut mem = dev.mem.write().unwrap();
    for (stripe_id, chunk) in mem.chunks_mut(STRIPE_BYTES as usize).enumerate() {
        chunk.fill(pattern(stripe_id));
    }
}

/// The evictor with everything it talks to, driven one tick at a time.
struct Rig {
    evictor: Evictor,
    state: SharedMetadataState,
    flusher: MetadataFlusher,
    metadata_dev: FlushFailingDevice,
    target_dev: TestBlockDevice,
    store: Arc<Mutex<TestObjectStore>>,
    punches: Arc<Mutex<Vec<(u64, u64)>>>,
    free: Arc<AtomicU64>,
    fail_next_punch: Arc<AtomicBool>,
    punch_errno: Arc<AtomicI32>,
    /// Metadata writes and flushes before the evictor did anything.
    io_baseline: (usize, usize),
}

impl Rig {
    fn build(
        headers: &[(usize, u8)],
        with_store: bool,
        tune: impl FnOnce(&mut EvictorConfig),
    ) -> Self {
        let mut cfg = base_config(TARGET_SECTORS);
        tune(&mut cfg);

        let target_dev = TestBlockDevice::new(cfg.target_sector_count * SECTOR_SIZE as u64);
        fill_with_patterns(&target_dev);
        let metadata_dev = FlushFailingDevice::new(TestBlockDevice::new(16 * 1024));
        metadata_with(headers)
            .save_to_bdev(&*metadata_dev.inner)
            .unwrap();
        let state = SharedMetadataState::new(&UbiMetadata::load_from_bdev(&metadata_dev).unwrap());
        let flusher =
            MetadataFlusher::new(&metadata_dev, cfg.target_sector_count, state.clone()).unwrap();

        let store = Arc::new(Mutex::new(TestObjectStore::new()));
        let puncher = RecordingPuncher::default();
        // Plenty of room unless a test says otherwise: a default of zero
        // would read as a full filesystem.
        puncher.free.store(1 << 40, Ordering::SeqCst);
        let punches = puncher.punches.clone();
        let free = puncher.free.clone();
        let fail_next_punch = puncher.fail_next.clone();
        let punch_errno = puncher.fail_errno.clone();

        let evictor = Evictor::new(
            cfg,
            target_dev.create_channel().unwrap(),
            with_store.then(|| Box::new(SharedStore(store.clone())) as Box<dyn ArchiveStore>),
            SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS),
            Box::new(puncher),
            state.clone(),
        )
        .unwrap();

        let io_baseline = metadata_dev.io_counts();
        Rig {
            evictor,
            state,
            flusher,
            metadata_dev,
            target_dev,
            store,
            punches,
            free,
            fail_next_punch,
            punch_errno,
            io_baseline,
        }
    }

    /// Dirty resident stripes `0..count`, store present, ceiling `max_local`
    /// stripes, one eviction at a time.
    fn dirty(count: usize, max_local: u64) -> Self {
        let headers: Vec<(usize, u8)> = (0..count).map(|s| (s, DIRTY)).collect();
        Rig::build(&headers, true, |cfg| {
            cfg.max_local_bytes = stripes_bytes(max_local);
            cfg.max_concurrent_evictions = 1;
        })
    }

    /// The coordinator's tick: flusher first, its outcomes to the evictor.
    fn tick(&mut self) {
        self.flusher.update();
        let outcomes = self.flusher.take_persist_outcomes();
        self.evictor.update(&mut self.flusher, &outcomes);
    }

    fn ticks(&mut self, count: usize) {
        for _ in 0..count {
            self.tick();
        }
    }

    /// Tick until `cond` holds, giving up after `max` ticks.
    fn run_until(&mut self, max: usize, cond: impl Fn(&Rig) -> bool) -> bool {
        for _ in 0..max {
            if cond(self) {
                return true;
            }
            self.tick();
        }
        cond(self)
    }

    fn stage(&self, stripe_id: usize) -> Option<Stage> {
        self.evictor
            .eviction_for_test(stripe_id)
            .map(|(_, stage)| stage)
    }

    fn kind(&self, stripe_id: usize) -> Option<Kind> {
        self.evictor
            .eviction_for_test(stripe_id)
            .map(|(kind, _)| kind)
    }

    fn fetch_state(&self, stripe_id: usize) -> u8 {
        self.state.stripe_fetch_state(stripe_id)
    }

    /// Metadata writes and flushes since construction.
    fn metadata_io(&self) -> (usize, usize) {
        let (writes, flushes) = self.metadata_dev.io_counts();
        (writes - self.io_baseline.0, flushes - self.io_baseline.1)
    }

    fn header(&self, stripe_id: usize) -> u8 {
        self.metadata_dev.header(stripe_id)
    }

    fn punches(&self) -> Vec<(u64, u64)> {
        self.punches.lock().unwrap().clone()
    }

    fn put_order(&self) -> Vec<String> {
        self.store.lock().unwrap().put_order.clone()
    }

    fn counter(&self, pick: impl Fn(&crate::block_device::SpillCounters) -> &AtomicU64) -> u64 {
        pick(self.state.spill()).load(Ordering::Relaxed)
    }

    fn degraded(&self) -> bool {
        self.state.spill().degraded.load(Ordering::Acquire)
    }

    /// The stripe bytes an object in the store decodes to.
    fn decode_object(&self, stripe_id: usize) -> Vec<u8> {
        let object = self
            .store
            .lock()
            .unwrap()
            .objects
            .lock()
            .unwrap()
            .get(&object_name(stripe_id))
            .cloned()
            .expect("object in store");
        let mut codec = SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS);
        let mut dst = vec![0u8; STRIPE_BYTES as usize];
        let len = codec
            .decode_into(stripe_id, &object, &mut dst, None)
            .unwrap();
        dst.truncate(len);
        dst
    }

    fn make_clean_evictable(&self, stripe_ids: &[usize]) {
        self.state.set_source_live(true);
        for stripe_id in stripe_ids {
            self.state
                .set_stripe_flags(*stripe_id, stripe_flags::FETCHED_LIVE);
        }
    }
}

fn permit_from(gate: &Arc<PushGate>) -> PushPermit {
    gate.acquire()
}

// ---- ordering

#[test]
fn dirty_eviction_puts_then_flushes_header_then_punches() {
    let mut rig = Rig::dirty(2, 1);
    rig.store.lock().unwrap().hold_puts = true;

    assert!(rig.run_until(10, |r| r.stage(0) == Some(Stage::Putting)));
    assert_eq!(rig.kind(0), Some(Kind::Dirty));
    assert_eq!(rig.fetch_state(0), Evicting);
    assert_eq!(rig.put_order(), vec![object_name(0)]);
    assert_eq!(rig.counter(|c| &c.puts), 1);

    // Held upload: nothing may move.
    rig.ticks(5);
    assert_eq!(rig.stage(0), Some(Stage::Putting));
    assert_eq!(
        rig.metadata_io(),
        (0, 0),
        "no header op before the PUT returns"
    );
    assert!(rig.punches().is_empty(), "no punch before the PUT returns");
    assert_eq!(rig.header(0), DIRTY);
    assert!(!rig.flusher.busy());

    // The PUT completes: the header op is issued, still nothing punched.
    rig.store.lock().unwrap().release_puts();
    rig.tick();
    assert!(matches!(rig.stage(0), Some(Stage::WritingHeader { .. })));
    assert!(rig.punches().is_empty());
    assert_eq!(
        rig.counter(|c| &c.put_bytes) as usize,
        rig.store.lock().unwrap().objects.lock().unwrap()[&object_name(0)].len()
    );

    // Written and flushed before the punch.
    rig.tick();
    assert_eq!(rig.metadata_io(), (1, 1));
    assert!(
        rig.punches().is_empty(),
        "the flush completion has not been seen yet"
    );
    assert_eq!(rig.fetch_state(0), Evicting);

    rig.tick();
    assert_eq!(rig.punches(), vec![(0, STRIPE_BYTES)]);
    assert_eq!(rig.fetch_state(0), Evicted);
    assert_eq!(
        rig.header(0),
        (DIRTY | metadata_flags::EVICTED | metadata_flags::IN_S3) & !metadata_flags::FETCHED
    );
    assert!(rig.state.stripe_in_s3(0));
    assert_eq!(rig.state.in_s3_stripes(), 1);
    assert_eq!(rig.state.evicted_stripes(), 1);
    assert_eq!(rig.state.resident_stripes(), 1);
    assert_eq!(rig.state.fetched_stripes(), 1);
    assert_eq!(rig.counter(|c| &c.evicted_dirty), 1);
    assert_eq!(rig.counter(|c| &c.punches), 1);
    assert_eq!(
        rig.decode_object(0),
        vec![pattern(0); STRIPE_BYTES as usize]
    );
    assert_eq!(rig.stage(0), None);

    // One stripe over the ceiling of one: done, and no longer busy.
    rig.ticks(5);
    assert_eq!(rig.state.evicted_stripes(), 1);
    assert!(!rig.evictor.busy());
}

/// A stripe carrying the store's IN_S3 hint is never clean (see
/// `in_s3_hint_makes_a_resident_stripe_dirty`), so a clean eviction starts
/// from a stripe without it; the header op still clears the bit, belt and
/// braces against a hint that would read as authoritative under EVICTED.
#[test]
fn clean_eviction_writes_header_then_punches_without_put() {
    let mut rig = Rig::build(&[(0, CLEAN)], true, |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = 0;
    });
    rig.make_clean_evictable(&[0]);

    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    assert!(
        rig.put_order().is_empty(),
        "a clean eviction uploads nothing"
    );
    assert_eq!(rig.metadata_io(), (1, 1));
    assert_eq!(rig.punches(), vec![(0, STRIPE_BYTES)]);
    assert_eq!(
        rig.header(0),
        metadata_flags::EVICTED | metadata_flags::HAS_SOURCE
    );
    assert!(!rig.state.stripe_in_s3(0));
    assert_eq!(rig.state.in_s3_stripes(), 0);
    assert_eq!(rig.counter(|c| &c.evicted_clean), 1);
    assert_eq!(rig.counter(|c| &c.evicted_dirty), 0);
    assert!(!rig.state.stripe_fetched_live(0));
}

#[test]
fn nothing_is_punched_when_put_fails_and_store_is_degraded() {
    let mut rig = Rig::dirty(2, 0);
    rig.store
        .lock()
        .unwrap()
        .fail_puts
        .push_back(object_name(0));

    assert!(rig.run_until(10, |r| r.counter(|c| &c.put_failures) == 1));
    assert_eq!(rig.fetch_state(0), Fetched, "back where it was");
    assert!(rig.punches().is_empty());
    assert_eq!(rig.metadata_io(), (0, 0));
    assert_eq!(rig.header(0), DIRTY);
    assert!(rig.degraded());
    assert_eq!(rig.counter(|c| &c.evictions_aborted), 1);
    assert!(!rig.state.stripe_in_s3(0));

    // Stripe 1 is dirty and over the ceiling, but the store is degraded.
    rig.ticks(10);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert_eq!(rig.put_order().len(), 1);
    assert_eq!(rig.state.evicted_stripes(), 0);
}

#[test]
fn nothing_is_punched_when_header_write_fails() {
    // Stripe 0 came from the source, stripe 1 is a written NoSource stripe:
    // both previous states have to come back.
    let mut rig = Rig::build(&[(0, DIRTY), (1, metadata_flags::WRITTEN)], true, |cfg| {
        cfg.max_local_bytes = 0;
        cfg.max_concurrent_evictions = 1;
    });
    assert_eq!(rig.fetch_state(1), NoSource);
    let (fetched, resident) = (rig.state.fetched_stripes(), rig.state.resident_stripes());

    for (stripe_id, previous) in [(0usize, Fetched), (1usize, NoSource)] {
        assert!(rig.run_until(10, |r| {
            matches!(r.stage(stripe_id), Some(Stage::WritingHeader { .. }))
        }));
        // The header op is queued; the write it becomes fails.
        rig.metadata_dev
            .inner
            .fail_next
            .store(true, Ordering::SeqCst);
        rig.tick();
        assert_eq!(rig.stage(stripe_id), None);
        assert_eq!(rig.fetch_state(stripe_id), previous);
        assert!(rig.punches().is_empty());
        assert_eq!(rig.metadata_io().1, 0, "nothing was flushed");
        assert_eq!(rig.flusher.header(stripe_id), rig.header(stripe_id));
        assert_eq!(rig.header(stripe_id) & metadata_flags::EVICTED, 0);
        assert_eq!(rig.state.fetched_stripes(), fetched);
        assert_eq!(rig.state.resident_stripes(), resident);
        // The aborted stripe is skipped for a second, so the hand moves on.
    }
    assert_eq!(rig.counter(|c| &c.evictions_aborted), 2);
    assert_eq!(rig.state.evicted_stripes(), 0);
}

#[test]
fn uncertain_header_outcome_is_retried_not_aborted() {
    let mut rig = Rig::dirty(1, 0);
    assert!(rig.run_until(10, |r| {
        matches!(r.stage(0), Some(Stage::WritingHeader { .. }))
    }));
    let degraded_before = rig.counter(|c| &c.degraded_reasons);

    // The write lands; the fsync after it fails.
    rig.metadata_dev.fail_next_flush();
    rig.tick();
    rig.tick();
    assert!(matches!(rig.stage(0), Some(Stage::WritingHeader { .. })));
    assert_eq!(rig.fetch_state(0), Evicting, "never aborted");
    assert!(rig.punches().is_empty());
    assert_eq!(rig.counter(|c| &c.degraded_reasons), degraded_before + 1);
    assert_eq!(rig.metadata_io().0, 1);

    // Committed: a guest asking for it now waits rather than aborting.
    assert_eq!(rig.evictor.on_fetch_request(0), FetchDisposition::Deferred);
    // A NotWritten after an Uncertain would not abort either; here the retry
    // simply has not happened yet.
    rig.ticks(3);
    assert_eq!(rig.metadata_io().0, 1, "the retry waits for its backoff");
    assert_eq!(rig.fetch_state(0), Evicting);

    rig.evictor.advance_time_for_test(Duration::from_secs(2));
    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    assert_eq!(rig.metadata_io(), (2, 1), "written twice, flushed once");
    assert_eq!(
        rig.punches(),
        vec![(0, STRIPE_BYTES)],
        "punched exactly once"
    );
    assert_eq!(
        rig.header(0) & metadata_flags::EVICTED,
        metadata_flags::EVICTED
    );
    let (fetches, pushes) = rig.evictor.take_released();
    assert_eq!(fetches, vec![0], "the deferred fetch is replayed");
    assert!(pushes.is_empty());
}

#[test]
fn punch_covers_only_the_short_last_stripe() {
    let last = STRIPES - 1;
    let short_sectors = 4;
    let mut rig = Rig::build(&[(last, DIRTY)], true, |cfg| {
        cfg.target_sector_count = TARGET_SECTORS - (STRIPE_SECTORS - short_sectors);
        cfg.max_local_bytes = 0;
    });

    assert!(rig.run_until(10, |r| r.fetch_state(last) == Evicted));
    let short_len = short_sectors * SECTOR_SIZE as u64;
    assert_eq!(rig.punches(), vec![(last as u64 * STRIPE_BYTES, short_len)]);
    assert_eq!(
        rig.decode_object(last),
        vec![pattern(last); short_len as usize],
        "only the sectors that exist are uploaded"
    );
}

#[test]
fn in_s3_is_set_only_after_the_put_succeeds() {
    let mut rig = Rig::dirty(1, 0);
    rig.store.lock().unwrap().hold_puts = true;

    assert!(rig.run_until(10, |r| r.stage(0) == Some(Stage::Putting)));
    assert!(!rig.state.stripe_in_s3(0));
    assert_eq!(rig.state.in_s3_stripes(), 0);
    assert_eq!(rig.flusher.header(0) & metadata_flags::IN_S3, 0);

    rig.store.lock().unwrap().release_puts();
    rig.tick();
    assert!(matches!(rig.stage(0), Some(Stage::WritingHeader { .. })));
    // The object is stored, but IN_S3 is authoritative only under EVICTED,
    // so memory waits for the header.
    assert!(!rig.state.stripe_in_s3(0));
    assert_eq!(rig.state.in_s3_stripes(), 0);

    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    assert!(rig.state.stripe_in_s3(0));
    assert_eq!(rig.state.in_s3_stripes(), 1);
    assert_eq!(rig.header(0) & metadata_flags::IN_S3, metadata_flags::IN_S3);
}

// ---- concurrency

#[test]
fn evictor_waits_for_inflight_zero_before_reading() {
    let mut rig = Rig::dirty(1, 0);
    rig.state.pin_inflight(0, 0);

    assert!(rig.run_until(5, |r| r.stage(0) == Some(Stage::Draining)));
    assert_eq!(rig.fetch_state(0), Evicting);
    rig.ticks(5);
    assert_eq!(rig.stage(0), Some(Stage::Draining));
    assert_eq!(rig.target_dev.metrics.read().unwrap().reads, 0);

    rig.state.unpin_inflight(0, 0);
    rig.tick();
    assert_ne!(rig.stage(0), Some(Stage::Draining));
    assert_eq!(rig.target_dev.metrics.read().unwrap().reads, 1);
    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
}

#[test]
fn drain_timeout_aborts_with_degraded_reason() {
    let mut rig = Rig::dirty(1, 0);
    rig.state.pin_inflight(0, 0);
    assert!(rig.run_until(5, |r| r.stage(0) == Some(Stage::Draining)));
    let degraded_before = rig.counter(|c| &c.degraded_reasons);

    rig.ticks(3);
    assert_eq!(rig.stage(0), Some(Stage::Draining), "within the timeout");

    rig.evictor.advance_time_for_test(Duration::from_secs(6));
    rig.tick();
    assert_eq!(rig.stage(0), None);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(rig.counter(|c| &c.degraded_reasons), degraded_before + 1);
    assert_eq!(rig.counter(|c| &c.evictions_aborted), 1);
    assert!(rig.punches().is_empty());
}

#[test]
fn fetch_during_draining_aborts_and_restores_previous_state() {
    let mut rig = Rig::build(&[(0, DIRTY), (1, metadata_flags::WRITTEN)], true, |cfg| {
        cfg.max_local_bytes = 0;
        cfg.max_concurrent_evictions = 1;
    });
    let (fetched, resident) = (rig.state.fetched_stripes(), rig.state.resident_stripes());
    rig.state.pin_inflight(0, 1);

    for (stripe_id, previous) in [(0usize, Fetched), (1usize, NoSource)] {
        assert!(rig.run_until(5, |r| r.stage(stripe_id) == Some(Stage::Draining)));
        assert_eq!(rig.fetch_state(stripe_id), Evicting);
        assert_eq!(
            rig.evictor.on_fetch_request(stripe_id),
            FetchDisposition::Aborted
        );
        assert_eq!(rig.fetch_state(stripe_id), previous);
        assert_eq!(rig.stage(stripe_id), None);
        assert_eq!(
            rig.evictor.records_for_test(),
            0,
            "nothing outstanding to drain"
        );
        assert_eq!(rig.state.fetched_stripes(), fetched);
        assert_eq!(rig.state.resident_stripes(), resident);
        assert_ne!(
            rig.state.stripe_flags(stripe_id) & stripe_flags::REFERENCED,
            0
        );
    }
    assert_eq!(rig.counter(|c| &c.evictions_aborted), 2);
    assert!(rig.punches().is_empty());
    assert_eq!(rig.metadata_io(), (0, 0));
}

#[test]
fn fetch_during_putting_aborts_and_stale_put_completion_is_ignored() {
    let mut rig = Rig::dirty(1, 0);
    rig.store.lock().unwrap().hold_puts = true;
    assert!(rig.run_until(10, |r| r.stage(0) == Some(Stage::Putting)));

    assert_eq!(rig.evictor.on_fetch_request(0), FetchDisposition::Aborted);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(rig.stage(0), None, "not an active eviction any more");
    assert_eq!(
        rig.evictor.records_for_test(),
        1,
        "the record waits for the PUT completion"
    );
    assert_eq!(rig.evictor.puts_in_flight_for_test(), 1);

    // A second fetch while the record drains is simply forwarded.
    assert_eq!(rig.evictor.on_fetch_request(0), FetchDisposition::Forward);

    rig.store.lock().unwrap().release_puts();
    rig.tick();
    assert_eq!(rig.evictor.puts_in_flight_for_test(), 0);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(
        rig.metadata_io(),
        (0, 0),
        "the stale PUT issues no header op"
    );
    assert!(rig.punches().is_empty());
    assert!(
        !rig.state.stripe_in_s3(0),
        "the orphan object is not claimed"
    );
}

#[test]
fn fetch_during_writing_header_is_deferred_and_replayed_after_evicted() {
    let mut rig = Rig::dirty(1, 0);
    assert!(rig.run_until(10, |r| {
        matches!(r.stage(0), Some(Stage::WritingHeader { .. }))
    }));

    assert_eq!(rig.evictor.on_fetch_request(0), FetchDisposition::Deferred);
    assert_eq!(rig.evictor.on_fetch_request(0), FetchDisposition::Deferred);
    assert_eq!(rig.fetch_state(0), Evicting);
    assert!(rig.evictor.take_released().0.is_empty());

    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    let (fetches, pushes) = rig.evictor.take_released();
    assert_eq!(fetches, vec![0]);
    assert!(pushes.is_empty());
    assert!(rig.evictor.take_released().0.is_empty(), "released once");
}

#[test]
fn stale_set_fetched_completion_does_not_release_an_evicting_stripe() {
    let mut rig = Rig::build(&[(0, metadata_flags::HAS_SOURCE)], true, |cfg| {
        cfg.max_local_bytes = 0;
    });
    assert_eq!(rig.fetch_state(0), NotFetched);

    // The coordinator lands the stripe in memory and queues the header; the
    // evictor claims it before the flusher gets to the queue.
    rig.state.mark_stripe_fetched(0);
    rig.state.mark_stripe_written(0);
    rig.flusher.set_stripe_fetched(0);
    rig.flusher.set_stripe_written(0);
    rig.evictor.update(&mut rig.flusher, &[]);
    assert_eq!(rig.fetch_state(0), Evicting);
    let fetched = rig.state.fetched_stripes();

    rig.tick();
    rig.tick();
    assert_ne!(
        rig.header(0) & metadata_flags::FETCHED,
        0,
        "the stale write landed"
    );
    assert_eq!(
        rig.fetch_state(0),
        Evicting,
        "but did not release the stripe"
    );
    assert_eq!(rig.state.fetched_stripes(), fetched);

    assert!(rig.run_until(15, |r| r.fetch_state(0) == Evicted));
    assert_eq!(
        rig.header(0),
        metadata_flags::EVICTED
            | metadata_flags::IN_S3
            | metadata_flags::HAS_SOURCE
            | metadata_flags::WRITTEN
    );
}

#[test]
fn abort_marks_referenced_and_skips_stripe_for_a_second() {
    let mut rig = Rig::dirty(2, 0);
    rig.state.pin_inflight(0, 0);
    assert!(rig.run_until(5, |r| r.stage(0) == Some(Stage::Draining)));
    assert_eq!(rig.state.stripe_flags(0) & stripe_flags::REFERENCED, 0);

    assert_eq!(rig.evictor.on_fetch_request(0), FetchDisposition::Aborted);
    assert_ne!(rig.state.stripe_flags(0) & stripe_flags::REFERENCED, 0);
    rig.state.unpin_inflight(0, 0);

    // The hand moves on to stripe 1 and evicts it.
    assert!(rig.run_until(15, |r| r.fetch_state(1) == Evicted));
    // Still over the ceiling, but stripe 0 was just aborted.
    rig.ticks(5);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(rig.evictor.records_for_test(), 0);

    rig.evictor.advance_time_for_test(Duration::from_secs(2));
    // Its reference bit buys one more pass; the sweep clears it and comes
    // round again within the same batch.
    assert!(rig.run_until(15, |r| r.fetch_state(0) == Evicted));
    assert_eq!(rig.state.stripe_flags(0) & stripe_flags::REFERENCED, 0);
}

// ---- pushes

#[test]
fn push_for_dirty_evicting_stripe_is_ignored_and_pushed_recorded() {
    let mut rig = CoordRig::build(&[(0, DIRTY)], |cfg| cfg.max_local_bytes = 0);
    rig.store.lock().unwrap().hold_puts = true;
    assert!(rig.run_until(15, |r| {
        r.state.stripe_fetch_state(0) == Evicting && r.store.lock().unwrap().put_order.len() == 1
    }));
    let target_writes = rig.target_dev.metrics.read().unwrap().writes;

    let gate = PushGate::new(4);
    let pre_image = vec![0xEEu8; STRIPE_BYTES as usize];
    rig.sender
        .send(BgWorkerRequest::PushedStripe {
            stripe_id: 0,
            data: pre_image,
            permit: permit_from(&gate),
        })
        .unwrap();
    rig.worker.receive_requests(false);

    assert!(
        rig.state.stripe_pushed(0),
        "PUSHED recorded before disposition"
    );
    assert_eq!(gate.queued(), 0, "the permit was dropped with the push");
    assert_eq!(
        rig.state.stripe_fetch_state(0),
        Evicting,
        "eviction continues"
    );
    rig.worker.update();
    rig.worker.update();
    assert_ne!(
        rig.metadata_dev.header(0) & metadata_flags::PUSHED,
        0,
        "PUSHED on disk"
    );
    assert_eq!(
        rig.target_dev.metrics.read().unwrap().writes,
        target_writes,
        "the pre-image never reached the device"
    );

    rig.store.lock().unwrap().release_puts();
    assert!(rig.run_until(15, |r| r.state.stripe_fetch_state(0) == Evicted));
    assert_eq!(
        rig.decode_object(0),
        vec![pattern(0); STRIPE_BYTES as usize]
    );
}

#[test]
fn push_for_clean_evicting_stripe_aborts_eviction() {
    let mut rig = Rig::build(&[(0, CLEAN)], true, |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = 0;
    });
    rig.make_clean_evictable(&[0]);
    rig.state.pin_inflight(0, 0);
    assert!(rig.run_until(5, |r| r.stage(0) == Some(Stage::Draining)));
    assert_eq!(rig.kind(0), Some(Kind::Clean));

    let gate = PushGate::new(4);
    let (disposition, permit) = rig
        .evictor
        .on_pushed_stripe(0, &[0u8; 512], permit_from(&gate));
    assert_eq!(disposition, PushDisposition::AbortedEviction);
    assert!(permit.is_none());
    assert_eq!(gate.queued(), 0, "the duplicate's permit is released");
    assert_eq!(rig.fetch_state(0), Fetched, "the local copy stands");
    assert_eq!(rig.stage(0), None);
    assert_eq!(rig.counter(|c| &c.evictions_aborted), 1);
    assert!(rig.punches().is_empty());
}

#[test]
fn push_for_committed_clean_eviction_is_deferred_then_applied() {
    let mut rig = Rig::build(&[(0, CLEAN)], true, |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = 0;
    });
    rig.make_clean_evictable(&[0]);
    assert!(rig.run_until(10, |r| {
        matches!(r.stage(0), Some(Stage::WritingHeader { .. }))
    }));

    let gate = PushGate::new(4);
    let data = vec![0xABu8; STRIPE_BYTES as usize];
    let (disposition, permit) = rig.evictor.on_pushed_stripe(0, &data, permit_from(&gate));
    assert_eq!(disposition, PushDisposition::Deferred);
    assert!(permit.is_none());
    assert_eq!(gate.queued(), 1, "the permit is held with the bytes");
    assert!(rig.evictor.take_released().1.is_empty());

    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    let (fetches, mut pushes) = rig.evictor.take_released();
    assert!(fetches.is_empty());
    assert_eq!(pushes.len(), 1);
    let (stripe_id, released, permit) = pushes.remove(0);
    assert_eq!(stripe_id, 0);
    assert_eq!(released, data);
    assert_eq!(gate.queued(), 1);
    drop(permit);
    assert_eq!(gate.queued(), 0);
}

/// WRITTEN on an evicted stripe is a write queued behind the eviction (the
/// channel sets it at queue time), not data on disk: a stripe evicted without
/// IN_S3 held the snapshot's content. The replica refuses the pull once it has
/// pushed, so the push is the only copy the fork can get and must go through.
#[test]
fn push_for_evicted_written_stripe_is_forwarded() {
    let mut rig = Rig::build(
        &[(
            0,
            metadata_flags::EVICTED | metadata_flags::WRITTEN | metadata_flags::HAS_SOURCE,
        )],
        true,
        |_| {},
    );
    assert_eq!(rig.fetch_state(0), Evicted);
    assert!(rig.state.stripe_written(0));
    let gate = PushGate::new(4);
    let (disposition, permit) = rig
        .evictor
        .on_pushed_stripe(0, &[0u8; 512], permit_from(&gate));
    assert_eq!(disposition, PushDisposition::Forward);
    assert!(permit.is_some(), "the ingest needs the permit");
    assert_eq!(gate.queued(), 1);
    drop(permit);

    // Failed with WAS_EVICTED and WRITTEN, still without IN_S3: the same.
    rig.state.set_stripe_failed(0);
    assert_eq!(rig.fetch_state(0), Failed);
    let (disposition, permit) = rig
        .evictor
        .on_pushed_stripe(0, &[0u8; 512], permit_from(&gate));
    assert_eq!(disposition, PushDisposition::Forward);
    assert!(permit.is_some());
}

#[test]
fn push_for_evicted_in_s3_stripe_is_ignored() {
    let mut rig = Rig::build(
        &[(
            0,
            metadata_flags::EVICTED | metadata_flags::IN_S3 | metadata_flags::HAS_SOURCE,
        )],
        true,
        |_| {},
    );
    let gate = PushGate::new(4);
    let (disposition, _) = rig
        .evictor
        .on_pushed_stripe(0, &[0u8; 512], permit_from(&gate));
    assert_eq!(disposition, PushDisposition::Ignore);

    // A fetch of it that failed for good leaves it Failed with WAS_EVICTED
    // and IN_S3 still set: the same hazard, the same answer.
    rig.state.set_stripe_failed(0);
    assert_eq!(rig.fetch_state(0), Failed);
    let (disposition, permit) = rig
        .evictor
        .on_pushed_stripe(0, &[0u8; 512], permit_from(&gate));
    assert_eq!(disposition, PushDisposition::Ignore);
    assert!(permit.is_none());
    assert_eq!(gate.queued(), 0);
}

#[test]
fn push_for_evicted_clean_stripe_is_forwarded() {
    let mut rig = Rig::build(
        &[(0, metadata_flags::EVICTED | metadata_flags::HAS_SOURCE)],
        true,
        |_| {},
    );
    let gate = PushGate::new(4);
    let (disposition, permit) = rig
        .evictor
        .on_pushed_stripe(0, &[0u8; 512], permit_from(&gate));
    assert_eq!(disposition, PushDisposition::Forward);
    assert!(permit.is_some(), "the ingest needs the permit");
    assert_eq!(gate.queued(), 1);
    drop(permit);

    // Failed with WAS_EVICTED and neither WRITTEN nor IN_S3: the push is the
    // only copy the fork can get.
    rig.state.set_stripe_failed(0);
    let (disposition, permit) = rig
        .evictor
        .on_pushed_stripe(0, &[0u8; 512], permit_from(&gate));
    assert_eq!(disposition, PushDisposition::Forward);
    assert!(permit.is_some());

    // And a plain resident or unfetched stripe is none of the evictor's business.
    let (disposition, permit) = rig
        .evictor
        .on_pushed_stripe(3, &[0u8; 512], permit_from(&gate));
    assert_eq!(disposition, PushDisposition::Forward);
    assert!(permit.is_some());
}

// ---- re-materialisation, through the coordinator

/// A real `BgWorker` with an inline fetcher over a `TestBlockDevice` source
/// and the real evictor, for the paths that live in the coordinator.
struct CoordRig {
    worker: BgWorker,
    sender: Sender<BgWorkerRequest>,
    state: SharedMetadataState,
    source_dev: TestBlockDevice,
    target_dev: TestBlockDevice,
    metadata_dev: FlushFailingDevice,
    store: Arc<Mutex<TestObjectStore>>,
    punches: Arc<Mutex<Vec<(u64, u64)>>>,
    /// Metadata writes and flushes before the coordinator did anything.
    io_baseline: (usize, usize),
}

impl CoordRig {
    fn build(headers: &[(usize, u8)], tune: impl FnOnce(&mut EvictorConfig)) -> Self {
        let mut cfg = base_config(TARGET_SECTORS);
        tune(&mut cfg);

        let source_dev = TestBlockDevice::new(TARGET_SECTORS * SECTOR_SIZE as u64);
        {
            // The source holds a different pattern from the fork's own data.
            let mut mem = source_dev.mem.write().unwrap();
            for (stripe_id, chunk) in mem.chunks_mut(STRIPE_BYTES as usize).enumerate() {
                chunk.fill(0x80 | pattern(stripe_id));
            }
        }
        let target_dev = TestBlockDevice::new(TARGET_SECTORS * SECTOR_SIZE as u64);
        fill_with_patterns(&target_dev);
        let metadata_dev = FlushFailingDevice::new(TestBlockDevice::new(16 * 1024));
        metadata_with(headers)
            .save_to_bdev(&*metadata_dev.inner)
            .unwrap();
        let state = SharedMetadataState::new(&UbiMetadata::load_from_bdev(&metadata_dev).unwrap());

        let store = Arc::new(Mutex::new(TestObjectStore::new()));
        let puncher = RecordingPuncher::default();
        puncher.free.store(1 << 40, Ordering::SeqCst);
        let punches = puncher.punches.clone();
        let evictor = Evictor::new(
            cfg,
            target_dev.create_channel().unwrap(),
            Some(Box::new(SharedStore(store.clone()))),
            SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS),
            Box::new(puncher),
            state.clone(),
        )
        .unwrap();

        let stripe_source = Box::new(
            BlockDeviceStripeSource::new(BlockDevice::clone(&source_dev), STRIPE_SECTORS).unwrap(),
        );
        let (sender, receiver) = channel();
        let worker = BgWorker::new(
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
        let io_baseline = metadata_dev.io_counts();
        CoordRig {
            worker,
            sender,
            state,
            source_dev,
            target_dev,
            metadata_dev,
            store,
            punches,
            io_baseline,
        }
    }

    /// Metadata writes and flushes since construction.
    fn metadata_io(&self) -> (usize, usize) {
        let (writes, flushes) = self.metadata_dev.io_counts();
        (writes - self.io_baseline.0, flushes - self.io_baseline.1)
    }

    fn run_until(&mut self, max: usize, cond: impl Fn(&CoordRig) -> bool) -> bool {
        for _ in 0..max {
            if cond(self) {
                return true;
            }
            self.worker.update();
        }
        cond(self)
    }

    fn source_reads(&self) -> usize {
        self.source_dev.metrics.read().unwrap().reads
    }

    fn target_stripe(&self, stripe_id: usize) -> Vec<u8> {
        let mem = self.target_dev.mem.read().unwrap();
        let start = stripe_id * STRIPE_BYTES as usize;
        mem[start..start + STRIPE_BYTES as usize].to_vec()
    }

    fn decode_object(&self, stripe_id: usize) -> Vec<u8> {
        let object = self
            .store
            .lock()
            .unwrap()
            .objects
            .lock()
            .unwrap()
            .get(&object_name(stripe_id))
            .cloned()
            .expect("object in store");
        let mut codec = SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS);
        let mut dst = vec![0u8; STRIPE_BYTES as usize];
        let len = codec
            .decode_into(stripe_id, &object, &mut dst, None)
            .unwrap();
        dst.truncate(len);
        dst
    }

    fn fetch(&mut self, stripe_id: usize) {
        self.sender
            .send(BgWorkerRequest::Fetch { stripe_id })
            .unwrap();
        self.worker.receive_requests(false);
    }
}

#[test]
fn rematerialised_stripe_is_not_released_before_header_is_durable() {
    let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
    let mut rig = CoordRig::build(&[(0, evicted)], |_| {});
    assert_eq!(rig.state.stripe_fetch_state(0), Evicted);
    let (resident, fetched) = (rig.state.resident_stripes(), rig.state.fetched_stripes());

    // The only flush the metadata device sees is the release op's.
    rig.metadata_dev.fail_next_flush();
    rig.fetch(0);
    // The data lands and the header write goes out, but its flush fails.
    assert!(rig.run_until(20, |r| r.metadata_io().0 >= 1));
    assert_eq!(
        rig.target_stripe(0),
        vec![0x80 | pattern(0); STRIPE_BYTES as usize]
    );
    assert!(rig.run_until(5, |r| r.metadata_io().0 >= 2), "retried");
    assert_eq!(
        rig.state.stripe_fetch_state(0),
        Evicted,
        "guest I/O still waits"
    );
    assert_eq!(rig.state.resident_stripes(), resident);

    assert!(rig.run_until(10, |r| r.state.stripe_fetch_state(0) == Fetched));
    assert_eq!(rig.state.resident_stripes(), resident + 1);
    assert_eq!(rig.state.fetched_stripes(), fetched + 1);
    assert_eq!(rig.state.evicted_stripes(), 0);
    let header = rig.metadata_dev.header(0);
    assert_ne!(header & metadata_flags::FETCHED, 0);
    assert_eq!(header & metadata_flags::EVICTED, 0);
    assert_eq!(rig.source_reads(), 1);
    assert!(rig.punches.lock().unwrap().is_empty());
}

#[test]
fn fetch_for_stripe_in_pending_release_is_dropped() {
    let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
    let mut rig = CoordRig::build(&[(0, evicted)], |_| {});

    rig.fetch(0);
    // Landed (data written) and the release header written but not yet
    // flushed: the stripe waits in pending_release.
    assert!(rig.run_until(20, |r| r.metadata_io().0 == 1));
    assert_eq!(rig.target_dev.metrics.read().unwrap().writes, 1);
    assert_eq!(rig.state.stripe_fetch_state(0), Evicted);
    assert_eq!(rig.source_reads(), 1);

    // The channel's re-send: dropped, not re-fetched.
    rig.fetch(0);
    rig.fetch(0);
    assert!(rig.run_until(10, |r| r.state.stripe_fetch_state(0) == Fetched));
    rig.run_until(5, |_| false);
    assert_eq!(rig.source_reads(), 1, "one pull for one re-materialisation");
    assert_eq!(rig.target_dev.metrics.read().unwrap().writes, 1);

    // Resident now: a fetch is forwarded and the fetcher finds nothing to do.
    rig.fetch(0);
    rig.run_until(5, |_| false);
    assert_eq!(rig.source_reads(), 1);
}

/// The coordinator's side of the FAIL-gate rule, with a channel in the loop.
/// A read queued on a missing stripe under GATE_FAIL sends a Fetch the
/// coordinator refuses. When the gate reopens before the channel polls, the
/// channel finds the gate open and its front Pending on a NotFetched stripe,
/// which it never asks for again; only the coordinator's replay of the
/// refused Fetch lands the stripe and lets the read complete.
#[test]
fn refused_fetch_is_routed_once_the_gate_reopens() {
    let headers: Vec<(usize, u8)> = (0..STRIPES - 1).map(|s| (s, DIRTY)).collect();
    let mut rig = CoordRig::build(&headers, |cfg| {
        cfg.max_local_bytes = stripes_bytes(4);
        cfg.hard_margin_bytes = STRIPE_BYTES;
        cfg.on_full = OnFull::Fail;
    });
    rig.store.lock().unwrap().hold_puts = true;
    let unfetched = STRIPES - 1;
    rig.worker.update();
    assert_eq!(rig.state.write_gate(), GATE_FAIL);

    // The guest reads the missing stripe: the channel queues the read and
    // asks for the stripe, and the coordinator refuses. The channel is not
    // polled while the gate is closed, so it never fails the read itself.
    let lazy = LazyBlockDevice::new(
        BlockDevice::clone(&rig.target_dev),
        None,
        rig.sender.clone(),
        rig.state.clone(),
        true,
    )
    .unwrap();
    let mut chan = lazy.create_channel().unwrap();
    let buf = shared_buffer(SECTOR_SIZE);
    chan.add_read(unfetched as u64 * STRIPE_SECTORS, 1, buf.clone(), 7);
    chan.submit().unwrap();
    rig.worker.receive_requests(false);
    rig.run_until(3, |_| false);
    assert_eq!(
        rig.source_reads(),
        0,
        "refused: nothing pulled while closed"
    );

    rig.store.lock().unwrap().hold_puts = false;
    rig.store.lock().unwrap().release_puts();
    assert!(rig.run_until(60, |r| r.state.write_gate() == GATE_OPEN));
    // Polled only now, the channel waits on a stripe it will not ask for
    // again.
    assert!(chan.poll().is_empty());
    assert!(chan.busy());

    assert!(rig.run_until(30, |r| r.source_reads() == 1), "replayed");
    assert!(rig.run_until(30, |r| r.target_stripe(unfetched)
        == vec![0x80 | pattern(unfetched); STRIPE_BYTES as usize]));
    // Served once the replayed fetch has landed. The stripe may be claimed
    // by the evictor the moment it is resident, in which case the channel's
    // request aborts that eviction and is served on the poll after.
    let mut served = Vec::new();
    for _ in 0..30 {
        served = chan.poll();
        if !served.is_empty() {
            break;
        }
        rig.worker.receive_requests(false);
        rig.worker.update();
    }
    assert_eq!(served, vec![(7, true)], "the queued read is served");
    assert_eq!(
        buf.borrow().as_slice()[..SECTOR_SIZE],
        vec![0x80 | pattern(unfetched); SECTOR_SIZE][..]
    );
    rig.run_until(5, |_| false);
    assert_eq!(rig.source_reads(), 1, "pulled once");
}

#[test]
fn in_s3_count_drops_when_a_spilled_stripe_is_rematerialised() {
    let spilled = metadata_flags::EVICTED | metadata_flags::IN_S3 | metadata_flags::HAS_SOURCE;
    let mut rig = CoordRig::build(&[(0, spilled), (1, spilled)], |_| {});
    assert_eq!(rig.state.in_s3_stripes(), 2);
    assert_eq!(rig.state.evicted_stripes(), 2);

    // Without the composite source the pull goes to base; the accounting is
    // the coordinator's either way.
    rig.fetch(1);
    assert!(rig.run_until(30, |r| r.state.stripe_fetch_state(1) == Fetched));
    assert_eq!(rig.state.in_s3_stripes(), 1);
    assert_eq!(rig.state.evicted_stripes(), 1);
    assert!(
        rig.state.stripe_in_s3(1),
        "the hint stays for the purge tool"
    );
    assert_eq!(
        rig.metadata_dev.header(1),
        metadata_flags::FETCHED | metadata_flags::IN_S3 | metadata_flags::HAS_SOURCE
    );
}

/// A stripe that came back from the store carries IN_S3 as a purge hint, and
/// the release sets FETCHED_LIVE because the subscription is up. It was
/// uploaded because its content could not be trusted to match the snapshot,
/// so it must never be dropped as clean afterwards: the next eviction PUTs
/// again rather than clearing IN_S3 and routing the re-fetch to base.
#[test]
fn rematerialised_from_store_is_never_clean() {
    let spilled = metadata_flags::EVICTED | metadata_flags::IN_S3 | metadata_flags::HAS_SOURCE;
    let mut rig = CoordRig::build(&[(0, spilled)], |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = 0;
    });
    rig.state.set_source_live(true);
    assert_eq!(rig.state.stripe_fetch_state(0), Evicted);
    // Nothing is resident, so nothing is under pressure yet.
    rig.run_until(3, |_| false);
    assert!(rig.punches.lock().unwrap().is_empty());

    // Re-materialised through the pending release, with the store's hint and
    // the live bit both set: exactly the shape that read as clean before.
    // The tick that releases it also finds the device over its ceiling and
    // claims it again, so "left Evicted" is what can be observed.
    rig.fetch(0);
    assert!(rig.run_until(30, |r| r.state.stripe_fetch_state(0) != Evicted));
    assert!(rig.state.stripe_in_s3(0), "the purge hint stays");
    assert!(rig.state.stripe_fetched_live(0));
    assert!(!rig.state.stripe_written(0));
    let content = rig.target_stripe(0);

    // Over the ceiling: the stripe is evicted again, and dirty.
    assert!(rig.run_until(40, |r| r.state.stripe_fetch_state(0) == Evicted));
    assert_eq!(rig.state.spill().evicted_dirty.load(Ordering::Relaxed), 1);
    assert_eq!(rig.state.spill().evicted_clean.load(Ordering::Relaxed), 0);
    assert_eq!(rig.store.lock().unwrap().put_order, vec![object_name(0)]);
    assert_eq!(rig.decode_object(0), content);
    assert_eq!(rig.metadata_dev.header(0), spilled);
    assert!(rig.state.stripe_in_s3(0));
    assert_eq!(rig.punches.lock().unwrap().len(), 1);
}

// ---- selection

#[test]
fn clean_before_dirty() {
    let mut rig = Rig::build(&[(0, DIRTY), (1, CLEAN)], true, |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = STRIPE_BYTES;
        cfg.max_concurrent_evictions = 1;
    });
    rig.make_clean_evictable(&[1]);

    rig.tick();
    assert_eq!(
        rig.kind(1),
        Some(Kind::Clean),
        "the clean stripe goes first"
    );
    assert_eq!(rig.stage(0), None);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert!(rig.run_until(10, |r| r.fetch_state(1) == Evicted));
    assert!(rig.put_order().is_empty());
    // One over the ceiling of one: satisfied without touching the dirty stripe.
    rig.ticks(3);
    assert_eq!(rig.fetch_state(0), Fetched);
}

#[test]
fn referenced_stripes_skipped_once() {
    let mut rig = Rig::build(&[(0, DIRTY), (1, DIRTY)], true, |cfg| {
        cfg.max_local_bytes = 0;
        cfg.max_concurrent_evictions = 1;
        cfg.sweep_batch = 1;
    });
    rig.state.touch(0, 0);

    rig.tick();
    assert_eq!(
        rig.evictor.records_for_test(),
        0,
        "stripe 0 had its bit set"
    );
    assert_eq!(rig.state.stripe_flags(0) & stripe_flags::REFERENCED, 0);
    assert_eq!(rig.evictor.hand_for_test(), 1);

    rig.tick();
    assert_eq!(rig.kind(1), Some(Kind::Dirty));
    assert_eq!(rig.stage(0), None);
    assert!(rig.run_until(15, |r| r.fetch_state(1) == Evicted));

    // Second time round the bit is clear and stripe 0 goes.
    assert!(rig.run_until(30, |r| r.fetch_state(0) == Evicted));
}

#[test]
fn evicting_notfetched_and_unwritten_nosource_never_claimed() {
    let mut rig = Rig::build(
        &[
            (0, metadata_flags::HAS_SOURCE),
            (1, 0),
            (2, DIRTY),
            (3, metadata_flags::EVICTED | metadata_flags::HAS_SOURCE),
        ],
        true,
        |cfg| {
            cfg.max_local_bytes = 0;
            cfg.clean_eviction = true;
        },
    );
    rig.state.set_source_live(true);
    rig.state.set_stripe_fetch_state_for_test(2, Evicting);
    assert_eq!(rig.fetch_state(0), NotFetched);
    assert_eq!(rig.fetch_state(1), NoSource);
    assert!(!rig.state.stripe_written(1));
    // Nothing counts as resident, so pressure has to come from the filesystem.
    rig.free.store(0, Ordering::SeqCst);

    rig.ticks(10);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert_eq!(rig.fetch_state(0), NotFetched);
    assert_eq!(rig.fetch_state(1), NoSource);
    assert_eq!(rig.fetch_state(2), Evicting);
    assert_eq!(rig.fetch_state(3), Evicted);
    assert!(rig.punches().is_empty());
    assert_eq!(rig.metadata_io(), (0, 0));
}

fn kind_under(headers: &[(usize, u8)], clean_eviction: bool, source_live: bool, flags: u8) -> Kind {
    let mut rig = Rig::build(headers, true, |cfg| {
        cfg.clean_eviction = clean_eviction;
        cfg.max_local_bytes = 0;
    });
    rig.state.set_source_live(source_live);
    if flags != 0 {
        rig.state.set_stripe_flags(0, flags);
    }
    rig.tick();
    rig.kind(0).expect("stripe 0 claimed")
}

#[test]
fn pushed_counts_as_dirty() {
    let kind = kind_under(
        &[(0, CLEAN)],
        true,
        true,
        stripe_flags::FETCHED_LIVE | stripe_flags::PUSHED,
    );
    assert_eq!(kind, Kind::Dirty);
}

#[test]
fn all_dirty_when_source_not_live() {
    assert_eq!(
        kind_under(&[(0, CLEAN)], true, false, stripe_flags::FETCHED_LIVE),
        Kind::Dirty
    );
}

#[test]
fn not_fetched_live_is_dirty() {
    assert_eq!(kind_under(&[(0, CLEAN)], true, true, 0), Kind::Dirty);
    // With the bit the very same stripe is clean.
    assert_eq!(
        kind_under(&[(0, CLEAN)], true, true, stripe_flags::FETCHED_LIVE),
        Kind::Clean
    );
}

/// The predicate alone: a resident stripe still carrying the store's hint is
/// dirty whatever else says clean.
#[test]
fn in_s3_hint_makes_a_resident_stripe_dirty() {
    assert_eq!(
        kind_under(
            &[(0, CLEAN | metadata_flags::IN_S3)],
            true,
            true,
            stripe_flags::FETCHED_LIVE
        ),
        Kind::Dirty
    );
}

#[test]
fn clean_eviction_off_by_default_makes_everything_dirty() {
    assert!(!base_config(TARGET_SECTORS).clean_eviction);
    assert_eq!(
        kind_under(&[(0, CLEAN)], false, true, stripe_flags::FETCHED_LIVE),
        Kind::Dirty
    );
}

#[test]
fn written_nosource_stripe_is_dirty_and_evictable() {
    let mut rig = Rig::build(&[(0, metadata_flags::WRITTEN)], true, |cfg| {
        cfg.max_local_bytes = 0;
    });
    assert_eq!(rig.fetch_state(0), NoSource);
    assert_eq!(rig.state.resident_stripes(), 1);
    assert_eq!(rig.state.fetched_stripes(), 0);

    rig.tick();
    assert_eq!(rig.kind(0), Some(Kind::Dirty));
    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    assert_eq!(rig.state.resident_stripes(), 0);
    assert_eq!(rig.state.fetched_stripes(), 0, "never counted as fetched");
    assert_eq!(rig.state.in_s3_stripes(), 1);
    assert_eq!(
        rig.header(0),
        metadata_flags::EVICTED | metadata_flags::IN_S3 | metadata_flags::WRITTEN
    );
    assert_eq!(
        rig.decode_object(0),
        vec![pattern(0); STRIPE_BYTES as usize]
    );
}

#[test]
fn hand_bounded_by_sweep_batch() {
    let headers: Vec<(usize, u8)> = (0..STRIPES).map(|s| (s, DIRTY)).collect();
    let mut rig = Rig::build(&headers, true, |cfg| {
        cfg.max_local_bytes = 0;
        cfg.max_concurrent_evictions = STRIPES;
        cfg.sweep_batch = 3;
    });
    rig.store.lock().unwrap().hold_puts = true;

    rig.tick();
    assert_eq!(rig.evictor.records_for_test(), 3);
    assert_eq!(rig.evictor.hand_for_test(), 3);
    rig.tick();
    assert_eq!(rig.evictor.records_for_test(), 6);
    assert_eq!(rig.evictor.hand_for_test(), 6);
}

#[test]
fn bounded_puts_in_flight() {
    let headers: Vec<(usize, u8)> = (0..STRIPES).map(|s| (s, DIRTY)).collect();
    let mut rig = Rig::build(&headers, true, |cfg| {
        cfg.max_local_bytes = 0;
        cfg.max_concurrent_evictions = 2;
    });
    rig.store.lock().unwrap().hold_puts = true;

    assert!(rig.run_until(10, |r| r.evictor.puts_in_flight_for_test() == 2));
    rig.ticks(10);
    assert_eq!(rig.evictor.puts_in_flight_for_test(), 2);
    assert_eq!(rig.put_order().len(), 2);
    assert_eq!(rig.evictor.records_for_test(), 2);

    // Releasing them lets the next two in, never more.
    rig.store.lock().unwrap().release_puts();
    assert!(rig.run_until(10, |r| r.state.evicted_stripes() == 2));
    assert!(rig.run_until(10, |r| r.evictor.puts_in_flight_for_test() == 2));
    assert_eq!(rig.put_order().len(), 4);
}

// ---- pressure and gate

#[test]
fn soft_pressure_evicts_to_low_water() {
    let headers: Vec<(usize, u8)> = (0..STRIPES).map(|s| (s, DIRTY)).collect();
    let mut rig = Rig::build(&headers, true, |cfg| {
        cfg.max_local_bytes = stripes_bytes(6);
        cfg.low_water_bytes = stripes_bytes(2);
        cfg.hard_margin_bytes = stripes_bytes(4);
        cfg.max_concurrent_evictions = 4;
    });

    rig.tick();
    assert!(rig.evictor.busy());
    assert!(rig.run_until(40, |r| r.state.resident_stripes() == 4));
    rig.ticks(10);
    assert_eq!(
        rig.state.resident_stripes(),
        4,
        "stops at the low-water mark"
    );
    assert_eq!(rig.state.evicted_stripes(), 4);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert!(!rig.evictor.busy());
    assert_eq!(
        rig.state.write_gate(),
        GATE_OPEN,
        "never under hard pressure"
    );
    assert_eq!(rig.counter(|c| &c.stalls), 0);
}

#[test]
fn hard_pressure_closes_gate_and_reopens_releasing_held_fetches() {
    // Seven resident, one unfetched; ceiling four, margin two.
    let headers: Vec<(usize, u8)> = (0..STRIPES - 1).map(|s| (s, DIRTY)).collect();
    let mut rig = Rig::build(&headers, true, |cfg| {
        cfg.max_local_bytes = stripes_bytes(4);
        cfg.low_water_bytes = STRIPE_BYTES;
        cfg.hard_margin_bytes = stripes_bytes(2);
        cfg.max_concurrent_evictions = 2;
    });
    rig.store.lock().unwrap().hold_puts = true;
    let unfetched = STRIPES - 1;
    assert_eq!(rig.fetch_state(unfetched), NotFetched);

    rig.tick();
    assert_eq!(rig.state.write_gate(), GATE_HOLD);
    assert_eq!(rig.counter(|c| &c.stalls), 1);
    assert_eq!(
        rig.evictor.on_fetch_request(unfetched),
        FetchDisposition::HeldForSpace
    );
    assert_eq!(
        rig.evictor.on_fetch_request(unfetched),
        FetchDisposition::HeldForSpace,
        "the channel re-sends"
    );
    assert_eq!(rig.evictor.held_for_space_for_test(), &[unfetched]);
    assert_eq!(
        rig.evictor.on_fetch_request(6),
        FetchDisposition::Forward,
        "resident stripes are not held (6 is not being evicted)"
    );
    rig.ticks(3);
    assert_eq!(rig.counter(|c| &c.stalls), 1, "warned once per transition");
    assert!(rig.evictor.take_released().0.is_empty());

    rig.store.lock().unwrap().hold_puts = false;
    rig.store.lock().unwrap().release_puts();
    assert!(rig.run_until(40, |r| r.state.write_gate() == GATE_OPEN));
    assert!(rig.state.resident_stripes() <= 6);
    let (fetches, pushes) = rig.evictor.take_released();
    assert_eq!(fetches, vec![unfetched]);
    assert!(pushes.is_empty());
    assert!(rig.evictor.held_for_space_for_test().is_empty());
    // Still soft pressure: the sweep carries on down to the low-water mark.
    assert!(rig.run_until(40, |r| r.state.resident_stripes() == 3));
}

/// Under GATE_FAIL the channel fails the request itself, but only if it polls
/// while the gate is still closed. Should the gate reopen first, the Pending
/// front waits for a fetch the channel never re-sends (NotFetched stripes
/// are not re-sent for), so the refused fetch is kept and replayed like a
/// held one.
#[test]
fn fail_gate_refuses_the_fetch_and_replays_it_when_the_gate_reopens() {
    let headers: Vec<(usize, u8)> = (0..STRIPES - 1).map(|s| (s, DIRTY)).collect();
    let mut rig = Rig::build(&headers, true, |cfg| {
        cfg.max_local_bytes = stripes_bytes(4);
        cfg.hard_margin_bytes = STRIPE_BYTES;
        cfg.on_full = OnFull::Fail;
    });
    rig.store.lock().unwrap().hold_puts = true;
    let unfetched = STRIPES - 1;
    rig.tick();
    assert_eq!(rig.state.write_gate(), GATE_FAIL);
    assert_eq!(
        rig.evictor.on_fetch_request(unfetched),
        FetchDisposition::Refused
    );
    assert_eq!(
        rig.evictor.on_fetch_request(unfetched),
        FetchDisposition::Refused,
        "the channel re-sends"
    );
    assert_eq!(rig.evictor.held_for_space_for_test(), &[unfetched]);
    assert_eq!(
        rig.evictor.on_fetch_request(6),
        FetchDisposition::Forward,
        "resident stripes pass"
    );
    rig.ticks(3);
    assert!(
        rig.evictor.take_released().0.is_empty(),
        "nothing replayed while closed"
    );

    rig.store.lock().unwrap().hold_puts = false;
    rig.store.lock().unwrap().release_puts();
    assert!(rig.run_until(40, |r| r.state.write_gate() == GATE_OPEN));
    let (fetches, pushes) = rig.evictor.take_released();
    assert_eq!(fetches, vec![unfetched]);
    assert!(pushes.is_empty());
    assert!(rig.evictor.held_for_space_for_test().is_empty());
}

#[test]
fn statfs_pressure_evicts_even_below_ceiling() {
    let mut rig = Rig::build(&[(0, DIRTY), (1, DIRTY)], true, |cfg| {
        cfg.max_local_bytes = stripes_bytes(100);
        cfg.min_free_bytes = stripes_bytes(4);
    });
    rig.free.store(STRIPE_BYTES, Ordering::SeqCst);

    rig.tick();
    assert_eq!(
        rig.counter(|c| &c.free_bytes),
        STRIPE_BYTES,
        "statfs published"
    );
    assert_eq!(rig.state.write_gate(), GATE_HOLD, "below half the minimum");
    assert!(rig.run_until(20, |r| r.state.evicted_stripes() == 2));
    assert_eq!(
        rig.state.write_gate(),
        GATE_HOLD,
        "the filesystem is still full"
    );

    // Room comes back (something else was deleted): the gate opens on the
    // next statfs, which is rate limited, so the interval is skipped.
    rig.free.store(1 << 40, Ordering::SeqCst);
    rig.evictor.advance_time_for_test(Duration::from_secs(1));
    rig.tick();
    assert_eq!(rig.state.write_gate(), GATE_OPEN);
    assert!(!rig.evictor.busy());
}

#[test]
fn degraded_store_blocks_dirty_but_not_clean() {
    let mut rig = Rig::build(&[(0, DIRTY), (1, CLEAN), (2, DIRTY)], true, |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = 0;
        cfg.max_concurrent_evictions = 1;
        cfg.sweep_batch = 1;
    });
    rig.make_clean_evictable(&[1]);
    rig.store
        .lock()
        .unwrap()
        .fail_puts
        .push_back(object_name(0));

    assert!(rig.run_until(10, |r| r.counter(|c| &c.put_failures) == 1));
    assert!(rig.degraded());
    assert_eq!(rig.fetch_state(0), Fetched);

    // The clean stripe still goes; the dirty ones wait for the store.
    assert!(rig.run_until(15, |r| r.fetch_state(1) == Evicted));
    assert_eq!(rig.counter(|c| &c.evicted_clean), 1);
    rig.ticks(20);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(rig.fetch_state(2), Fetched);
    assert_eq!(rig.counter(|c| &c.evicted_dirty), 0);
    assert_eq!(rig.put_order().len(), 1);
    assert!(rig.degraded());
    assert!(
        !rig.evictor.busy(),
        "nothing it can do until the backoff passes"
    );
}

#[test]
fn half_open_after_backoff() {
    let mut rig = Rig::build(&[(0, DIRTY), (1, DIRTY), (2, DIRTY)], true, |cfg| {
        cfg.max_local_bytes = 0;
        cfg.max_concurrent_evictions = 2;
    });
    {
        let mut store = rig.store.lock().unwrap();
        store.fail_puts.push_back(object_name(0));
        store.fail_puts.push_back(object_name(1));
    }

    assert!(rig.run_until(10, |r| r.counter(|c| &c.put_failures) == 2));
    assert!(rig.degraded());
    rig.ticks(10);
    assert_eq!(
        rig.evictor.records_for_test(),
        0,
        "paused during the backoff"
    );
    assert_eq!(rig.put_order().len(), 2);

    // The backoff passes: exactly one upload probes the store.
    rig.evictor.advance_time_for_test(Duration::from_secs(10));
    rig.tick();
    assert_eq!(rig.evictor.records_for_test(), 1, "half-open: one probe");
    rig.tick();
    assert_eq!(rig.evictor.records_for_test(), 1);
    assert!(rig.run_until(10, |r| !r.degraded()));
    assert_eq!(rig.put_order().len(), 3);

    // Recovered: full concurrency again and everything goes.
    assert!(rig.run_until(30, |r| r.state.evicted_stripes() == 3));
    assert_eq!(rig.counter(|c| &c.put_failures), 2);
    assert_eq!(rig.counter(|c| &c.evicted_dirty), 3);
}

// ---- startup

#[test]
fn startup_pass_punches_every_evicted_stripe_and_coalesces_runs() {
    let evicted = metadata_flags::EVICTED | metadata_flags::HAS_SOURCE;
    let last = STRIPES - 1;
    let mut rig = Rig::build(&[], true, |cfg| {
        cfg.target_sector_count = TARGET_SECTORS - 4;
    });
    let metadata = metadata_with(&[
        (1, evicted),
        (2, evicted),
        (3, evicted),
        (5, evicted | metadata_flags::IN_S3),
        (last, evicted),
    ]);

    assert_eq!(rig.evictor.punch_all_evicted(&metadata).unwrap(), 5);
    assert_eq!(
        rig.punches(),
        vec![
            (STRIPE_BYTES, 3 * STRIPE_BYTES),
            (5 * STRIPE_BYTES, STRIPE_BYTES),
            (last as u64 * STRIPE_BYTES, 4 * SECTOR_SIZE as u64),
        ]
    );
    assert_eq!(rig.counter(|c| &c.startup_punches), 3);
    assert_eq!(
        rig.counter(|c| &c.punches),
        0,
        "counted apart from evictions"
    );

    // Idempotent: the same calls again.
    assert_eq!(rig.evictor.punch_all_evicted(&metadata).unwrap(), 5);
    assert_eq!(rig.punches().len(), 6);
    assert_eq!(rig.counter(|c| &c.startup_punches), 6);

    // A failed run is counted and does not stop the pass.
    rig.fail_next_punch.store(true, Ordering::SeqCst);
    assert_eq!(rig.evictor.punch_all_evicted(&metadata).unwrap(), 2);
    assert_eq!(rig.counter(|c| &c.punch_failures), 1);
    assert_eq!(rig.punches().len(), 8);
    assert!(
        rig.evictor.punch_supported_for_test(),
        "EIO is not EOPNOTSUPP"
    );

    // Nothing evicted, nothing punched.
    assert_eq!(
        rig.evictor.punch_all_evicted(&metadata_with(&[])).unwrap(),
        0
    );
    assert_eq!(rig.punches().len(), 8);
}

#[test]
fn startup_pass_ignores_fetched_and_evicted_header() {
    let mut rig = Rig::build(&[], true, |_| {});
    let metadata = metadata_with(&[
        (
            2,
            metadata_flags::FETCHED | metadata_flags::EVICTED | metadata_flags::HAS_SOURCE,
        ),
        (4, metadata_flags::EVICTED | metadata_flags::HAS_SOURCE),
    ]);
    assert_eq!(rig.evictor.punch_all_evicted(&metadata).unwrap(), 1);
    assert_eq!(rig.punches(), vec![(4 * STRIPE_BYTES, STRIPE_BYTES)]);
}

// ---- the remaining transitions

#[test]
fn punch_failure_still_finishes_the_eviction() {
    let mut rig = Rig::dirty(1, 0);
    rig.fail_next_punch.store(true, Ordering::SeqCst);
    let degraded_before = rig.counter(|c| &c.degraded_reasons);

    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    // The disk says not-local, so the state follows whatever happened to the
    // blocks; the failure is loud rather than blocking.
    assert!(rig.punches().is_empty());
    assert_eq!(rig.counter(|c| &c.punch_failures), 1);
    assert_eq!(rig.counter(|c| &c.punches), 0);
    assert_eq!(rig.counter(|c| &c.degraded_reasons), degraded_before + 1);
    assert!(rig.evictor.punch_supported_for_test());
}

#[test]
fn unsupported_punch_stops_further_evictions() {
    let mut rig = Rig::dirty(2, 0);
    rig.punch_errno
        .store(Errno::EOPNOTSUPP as i32, Ordering::SeqCst);
    rig.fail_next_punch.store(true, Ordering::SeqCst);

    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    assert!(!rig.evictor.punch_supported_for_test());
    // Stripe 1 is over the ceiling too, but evicting it would free nothing.
    rig.ticks(10);
    assert_eq!(rig.fetch_state(1), Fetched);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert!(!rig.evictor.busy());
}

#[test]
fn read_failure_aborts_the_eviction() {
    let mut rig = Rig::dirty(1, 0);
    rig.state.pin_inflight(0, 0);
    assert!(rig.run_until(5, |r| r.stage(0) == Some(Stage::Draining)));
    rig.state.unpin_inflight(0, 0);
    rig.target_dev.fail_next.store(true, Ordering::SeqCst);

    rig.tick();
    assert_eq!(rig.stage(0), Some(Stage::Reading));
    rig.tick();
    assert_eq!(rig.stage(0), None);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert!(rig.put_order().is_empty());
    assert_eq!(rig.counter(|c| &c.evictions_aborted), 1);

    // The buffer came back: the retry after the skip reads and uploads.
    rig.evictor.advance_time_for_test(Duration::from_secs(2));
    assert!(rig.run_until(15, |r| r.fetch_state(0) == Evicted));
}

/// A read whose submit failed may still complete: io_uring keeps the SQE and
/// enters it with the next submit. Returning the buffer and forgetting the
/// record there would let the late completion be taken for a later eviction's
/// read of the same stripe, and bytes read before the guest wrote again be
/// uploaded as the fork's data. The record and its buffer wait for it, and
/// the stripe is not claimed again until it has drained.
#[test]
fn failed_read_submit_keeps_the_record_until_the_completion_lands() {
    let mut rig = Rig::dirty(1, 0);
    rig.state.pin_inflight(0, 0);
    assert!(rig.run_until(5, |r| r.stage(0) == Some(Stage::Draining)));
    let degraded_before = rig.counter(|c| &c.degraded_reasons);
    rig.target_dev
        .keep_requests_on_failed_submit
        .store(true, Ordering::SeqCst);
    rig.target_dev.fail_submit.store(true, Ordering::SeqCst);
    rig.target_dev
        .hold_completions
        .store(true, Ordering::SeqCst);
    rig.state.unpin_inflight(0, 0);

    rig.tick();
    assert_eq!(rig.target_dev.metrics.read().unwrap().reads, 1);
    assert_eq!(rig.stage(0), None, "aborted");
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(
        rig.evictor.records_for_test(),
        1,
        "the record waits for the read"
    );
    assert_eq!(rig.counter(|c| &c.evictions_aborted), 1);
    assert_eq!(rig.counter(|c| &c.degraded_reasons), degraded_before + 1);
    assert!(rig.put_order().is_empty());

    // The guest writes, and the hand comes round again; the stripe is not
    // re-claimed while its old read is owed.
    let written = vec![0x5A; STRIPE_BYTES as usize];
    rig.target_dev.write(0, &written, written.len());
    rig.evictor.advance_time_for_test(Duration::from_secs(2));
    rig.ticks(3);
    assert_eq!(rig.evictor.records_for_test(), 1);
    assert_eq!(rig.stage(0), None);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(
        rig.target_dev.metrics.read().unwrap().reads,
        1,
        "no second read"
    );
    assert!(rig.put_order().is_empty());

    // The late completion drains the record and uploads nothing; only then
    // is the stripe claimed again, and read afresh.
    rig.target_dev
        .hold_completions
        .store(false, Ordering::SeqCst);
    rig.tick();
    assert_eq!(rig.target_dev.metrics.read().unwrap().reads, 1);
    assert!(rig.put_order().is_empty(), "the stale read is not uploaded");
    assert!(rig.punches().is_empty());
    assert_eq!(rig.stage(0), Some(Stage::Draining), "claimed afresh");

    assert!(rig.run_until(15, |r| r.fetch_state(0) == Evicted));
    assert_eq!(rig.target_dev.metrics.read().unwrap().reads, 2);
    assert_eq!(rig.put_order(), vec![object_name(0)]);
    assert_eq!(rig.decode_object(0), written);
}

/// The owed completion is collected by the idle tick's poll as well, so an
/// aborted record must not keep the coordinator spinning for it: with a read
/// submit that keeps failing, or an aborted PUT that takes its time, that
/// would hold a core for the duration.
#[test]
fn aborted_record_awaiting_its_completion_does_not_keep_the_evictor_busy() {
    let mut rig = Rig::dirty(1, 0);
    rig.state.pin_inflight(0, 0);
    assert!(rig.run_until(5, |r| r.stage(0) == Some(Stage::Draining)));
    assert!(rig.evictor.busy(), "an eviction is in progress");
    rig.target_dev
        .keep_requests_on_failed_submit
        .store(true, Ordering::SeqCst);
    rig.target_dev.fail_submit.store(true, Ordering::SeqCst);
    rig.target_dev
        .hold_completions
        .store(true, Ordering::SeqCst);
    rig.state.unpin_inflight(0, 0);

    rig.tick();
    assert_eq!(rig.stage(0), None, "aborted");
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(
        rig.evictor.records_for_test(),
        1,
        "the record waits for the read"
    );
    assert!(!rig.evictor.busy(), "nothing to spin for");

    // The completion still drains the record on an ordinary tick.
    rig.target_dev
        .hold_completions
        .store(false, Ordering::SeqCst);
    rig.tick();
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert_eq!(rig.fetch_state(0), Fetched);
}

#[test]
fn clean_stripe_written_during_drain_is_converted_to_dirty() {
    let mut rig = Rig::build(&[(0, CLEAN)], true, |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = 0;
    });
    rig.make_clean_evictable(&[0]);
    rig.state.pin_inflight(0, 0);
    assert!(rig.run_until(5, |r| r.kind(0) == Some(Kind::Clean)));

    // A write lands while the eviction drains: the snapshot no longer holds
    // this content.
    rig.state.mark_stripe_written(0);
    rig.state.unpin_inflight(0, 0);
    rig.tick();
    assert_eq!(rig.kind(0), Some(Kind::Dirty));
    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    assert_eq!(rig.put_order(), vec![object_name(0)]);
    assert!(rig.state.stripe_in_s3(0));
    assert_eq!(rig.counter(|c| &c.evicted_dirty), 1);
    assert_eq!(rig.counter(|c| &c.evicted_clean), 0);
}

#[test]
fn clean_stripe_written_during_drain_aborts_without_store() {
    let mut rig = Rig::build(&[(0, CLEAN)], false, |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = 0;
    });
    rig.make_clean_evictable(&[0]);
    rig.state.pin_inflight(0, 0);
    assert!(rig.run_until(5, |r| r.kind(0) == Some(Kind::Clean)));

    rig.state.mark_stripe_written(0);
    rig.state.unpin_inflight(0, 0);
    rig.tick();
    assert_eq!(rig.stage(0), None);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(rig.counter(|c| &c.evictions_aborted), 1);
    // Dirty for good and nowhere to put it: never claimed again.
    rig.evictor.advance_time_for_test(Duration::from_secs(2));
    rig.ticks(5);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert!(!rig.evictor.busy());
}

#[test]
fn snapshot_ending_during_drain_converts_clean_to_dirty() {
    let mut rig = Rig::build(&[(0, CLEAN)], true, |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = 0;
    });
    rig.make_clean_evictable(&[0]);
    rig.state.pin_inflight(0, 0);
    assert!(rig.run_until(5, |r| r.kind(0) == Some(Kind::Clean)));

    rig.state.set_source_live(false);
    rig.state.unpin_inflight(0, 0);
    rig.tick();
    assert_eq!(rig.kind(0), Some(Kind::Dirty));
    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    assert_eq!(rig.put_order(), vec![object_name(0)]);
}

#[test]
fn without_a_store_only_clean_stripes_are_evicted() {
    let mut rig = Rig::build(&[(0, DIRTY), (1, CLEAN)], false, |cfg| {
        cfg.clean_eviction = true;
        cfg.max_local_bytes = 0;
    });
    rig.make_clean_evictable(&[1]);

    assert!(rig.run_until(10, |r| r.fetch_state(1) == Evicted));
    rig.ticks(10);
    assert_eq!(rig.fetch_state(0), Fetched);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert!(!rig.evictor.busy(), "a full revolution found nothing more");
}

#[test]
fn nothing_happens_with_clean_eviction_off_and_no_store() {
    let mut rig = Rig::build(&[(0, DIRTY), (1, CLEAN)], false, |cfg| {
        cfg.max_local_bytes = 0;
    });
    rig.make_clean_evictable(&[1]);
    rig.ticks(10);
    assert_eq!(rig.state.evicted_stripes(), 0);
    assert_eq!(rig.evictor.records_for_test(), 0);
    assert!(!rig.evictor.busy());
    // Only the gate acts: two stripes against a ceiling of zero plus one
    // stripe of margin is hard pressure.
    assert_eq!(rig.state.write_gate(), GATE_HOLD);
    assert_eq!(rig.counter(|c| &c.stalls), 1);
}

#[test]
fn busy_while_evicting_and_while_released_items_wait() {
    let mut rig = Rig::dirty(1, 0);
    assert!(!rig.evictor.busy(), "no pressure assessed yet");
    rig.tick();
    assert!(rig.evictor.busy(), "an eviction is in progress");
    assert!(rig.run_until(10, |r| {
        matches!(r.stage(0), Some(Stage::WritingHeader { .. }))
    }));
    assert_eq!(rig.evictor.on_fetch_request(0), FetchDisposition::Deferred);
    assert!(rig.run_until(10, |r| r.fetch_state(0) == Evicted));
    assert!(rig.evictor.busy(), "the released fetch waits to be routed");
    rig.evictor.take_released();
    assert!(!rig.evictor.busy());
}

#[test]
fn stale_header_outcome_is_ignored() {
    let mut rig = Rig::dirty(1, 0);
    rig.store.lock().unwrap().hold_puts = true;
    assert!(rig.run_until(10, |r| r.stage(0) == Some(Stage::Putting)));
    let flusher_outcome = crate::block_device::PersistOutcome {
        stripe_id: 0,
        token: 99,
        result: crate::block_device::PersistResult::Durable,
    };
    // An odd token for a stripe that is not in WritingHeader, and one for a
    // stripe with no eviction at all: neither may punch anything.
    let other = crate::block_device::PersistOutcome {
        stripe_id: 5,
        ..flusher_outcome
    };
    rig.evictor
        .update(&mut rig.flusher, &[flusher_outcome, other]);
    assert!(rig.punches().is_empty());
    assert_eq!(rig.fetch_state(0), Evicting);
}

#[test]
fn odd_tokens_belong_to_the_evictor() {
    assert!(Evictor::owns_token(1));
    assert!(Evictor::owns_token(7));
    assert!(!Evictor::owns_token(2));
    assert!(!Evictor::owns_token(0));
}

#[test]
fn evictor_rejects_a_config_it_cannot_run() {
    let build = |tune: fn(&mut EvictorConfig)| {
        let mut cfg = base_config(TARGET_SECTORS);
        tune(&mut cfg);
        let target = TestBlockDevice::new(TARGET_SECTORS * SECTOR_SIZE as u64);
        let state = SharedMetadataState::new(&metadata_with(&[]));
        Evictor::new(
            cfg,
            target.create_channel().unwrap(),
            None,
            SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS),
            Box::new(RecordingPuncher::default()),
            state,
        )
        .map(|_| ())
    };
    assert!(build(|_| {}).is_ok());
    assert!(build(|cfg| cfg.max_concurrent_evictions = 0).is_err());
    assert!(build(|cfg| cfg.sweep_batch = 0).is_err());
    assert!(build(|cfg| cfg.stripe_sector_count = 0).is_err());
    assert!(build(|cfg| cfg.alignment = 3000).is_err());
}
