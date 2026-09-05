//! The eviction state machine: which stripe goes, and the order in which its
//! data is read, uploaded, recorded and punched.
//!
//! An eviction claims a resident stripe (Fetched, or NoSource and Written) with
//! a CAS to Evicting, waits for guest I/O already handed to the device to
//! finish, then either records the stripe as EVICTED (clean) or reads it,
//! uploads it and records EVICTED | IN_S3 (dirty). Only once that header is
//! durable are the stripe's blocks punched and the state moved to Evicted, so
//! the disk never says "local" for punched blocks and never says IN_S3 for an
//! object that was not stored (I1). A guest asking for the stripe before the
//! header op is handed to the flusher aborts the eviction; afterwards it waits.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use log::{debug, error, info, warn};
use nix::errno::Errno;
use ubiblk_macros::error_context;

use crate::{
    archive::ArchiveStore,
    backends::SECTOR_SIZE,
    block_device::{
        metadata_flags, stripe_flags, Evicted, IoChannel, PushPermit, SharedBuffer,
        SharedMetadataState, UbiMetadata, GATE_FAIL, GATE_HOLD, GATE_OPEN,
    },
    config::v2::spill::OnFull,
    utils::AlignedBufferPool,
    Result,
};

use super::{
    super::metadata_flusher::{MetadataFlusher, PersistOutcome, PersistResult},
    codec::{parse_spill_object_name, spill_object_name, SpillCodec},
    punch::HolePuncher,
};

/// How long a claimed stripe may wait for its in-flight guest I/O. A request
/// pinned this long has lost its completion; the eviction is abandoned and
/// the anomaly counted rather than waiting forever.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the hand skips a stripe after an aborted eviction: the guest just
/// asked for it, so it is about to be referenced again.
const ABORT_SKIP: Duration = Duration::from_secs(1);
/// How often the filesystem is asked how much room it has left.
const STATFS_INTERVAL: Duration = Duration::from_millis(250);
/// Pause before an Uncertain header op is issued again.
const HEADER_RETRY: Duration = Duration::from_secs(1);
/// First and longest pause after an upload failure before the store is tried
/// again with a single eviction.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// What the coordinator does with a guest `Fetch { S }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchDisposition {
    /// Not the evictor's business (NotFetched, Evicted, resident): route to the ingest.
    Forward,
    /// S was Evicting in an abortable stage and is resident again; the channel
    /// finds it Complete on its next poll. Nothing to route.
    Aborted,
    /// S's eviction is committed (header op issued); the fetch is replayed once
    /// S is Evicted, via `take_released`.
    Deferred,
    /// Space is exhausted and on_full = stall; replayed when the gate opens.
    HeldForSpace,
    /// Space is exhausted and on_full = fail; the channel fails the request
    /// itself under GATE_FAIL. Nothing to route now, but the fetch is kept
    /// like a held one and replayed when the gate opens: the channel may
    /// poll only after the reopen, find the gate open and the stripe still
    /// missing, and it never asks again for a NotFetched front.
    Refused,
}

/// What the coordinator does with a `PushedStripe { S }`. PUSHED has already
/// been recorded for S by the time this is consulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushDisposition {
    /// Route to the ingest (NotFetched, resident, or Evicted clean).
    Forward,
    /// Local or spilled content is newer or equal; drop the push and its permit.
    Ignore,
    /// S was Evicting, clean and abortable: eviction aborted, local copy kept,
    /// push dropped (it is the same content).
    AbortedEviction,
    /// S is Evicting, clean and committed: bytes and permit kept, applied once
    /// S is Evicted, via `take_released`.
    Deferred,
}

/// The evictor's limits, derived by the backend from `[spill]` and the device
/// geometry. Byte fields are in bytes; see `SpillSection` for their meaning.
#[derive(Debug, Clone)]
pub struct EvictorConfig {
    /// The file whose blocks are punched.
    pub data_path: PathBuf,
    /// Prefix of every object this device writes: the key is
    /// `<device_id>/<stripe_index>`.
    pub device_id: String,
    /// Sectors per stripe.
    pub stripe_sector_count: u64,
    /// Sectors on the device, so the last stripe's punch can be shortened.
    pub target_sector_count: u64,
    /// Ceiling on resident bytes.
    pub max_local_bytes: u64,
    /// Evict down to `max_local_bytes - low_water_bytes` once over the ceiling.
    pub low_water_bytes: u64,
    /// Gate guest writes above `max_local_bytes + hard_margin_bytes`.
    pub hard_margin_bytes: u64,
    /// statfs watermark on the filesystem; writes are gated below half of it.
    pub min_free_bytes: u64,
    /// Drop clean stripes the live snapshot can serve again instead of
    /// uploading them.
    pub clean_eviction: bool,
    /// What a guest write meets when space runs out.
    pub on_full: OnFull,
    /// Evictions, and so PUTs, in flight at once.
    pub max_concurrent_evictions: usize,
    /// Stripes the CLOCK hand examines per update tick (default 4096).
    pub sweep_batch: usize,
    /// Buffer alignment for the reads that feed an upload.
    pub alignment: usize,
}

/// A push held back during an eviction and released afterwards: the stripe,
/// its bytes and the subscriber's permit.
pub type ReleasedPush = (usize, Vec<u8>, PushPermit);

/// Where an eviction is. Abortable up to and including `Putting`; once the
/// header op is with the flusher only its outcome ends the eviction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    /// Claimed; waiting for `stripe_inflight` to read zero.
    Draining,
    /// The stripe's data is being read from the local device.
    Reading,
    /// The object is with the store.
    Putting,
    /// The header op carrying this token is with the flusher.
    WritingHeader { token: u64 },
}

/// Whether the stripe is dropped (the live snapshot can serve it again) or
/// uploaded first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Clean,
    Dirty,
}

struct Eviction {
    previous: u8,
    kind: Kind,
    stage: Stage,
    /// Distinguishes this attempt's header outcome from an aborted earlier
    /// attempt on the same stripe: the token is derived from it.
    epoch: u64,
    /// Held from the read until the object is encoded.
    buf: Option<SharedBuffer>,
    object_len: u64,
    started: Instant,
    /// Set on the first Uncertain header outcome: from here only Durable ends
    /// the eviction, because the disk may already say EVICTED.
    committed: bool,
    /// When to re-issue the header op after an Uncertain outcome.
    retry_at: Option<Instant>,
    deferred_fetch: bool,
    deferred_push: Option<(Vec<u8>, PushPermit)>,
    /// A read or PUT has been started and its completion not yet seen.
    io_outstanding: bool,
    /// The read's submit failed. io_uring keeps the SQE and enters it on the
    /// next submit, so the completion is still owed: the record and its
    /// buffer wait for it, and the channel is asked to submit again each
    /// tick until one succeeds.
    resubmit_read: bool,
    /// The stripe is resident again but a completion is still owed; the
    /// record stays until it lands so the completion is recognised.
    aborted: bool,
}

/// Where the process aborts itself when `UBIBLK_SPILL_CRASH_AT` names the
/// point, for the end-to-end crash matrix.
#[cfg(feature = "fault-injection")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    /// The PUT succeeded; the header op has not been issued.
    AfterPut,
    /// The EVICTED header is durable; the blocks are not yet punched.
    AfterHeaderFlush,
    /// The blocks are punched; the in-memory state is not yet Evicted.
    AfterPunch,
    /// A re-fetched stripe's data is written; its EVICTED-clearing header is
    /// not yet durable.
    DuringRefetch,
}

#[cfg(feature = "fault-injection")]
impl CrashPoint {
    fn from_env() -> Option<Self> {
        let value = std::env::var("UBIBLK_SPILL_CRASH_AT").ok()?;
        match value.as_str() {
            "after_put" => Some(CrashPoint::AfterPut),
            "after_header_flush" => Some(CrashPoint::AfterHeaderFlush),
            "after_punch" => Some(CrashPoint::AfterPunch),
            "during_refetch" => Some(CrashPoint::DuringRefetch),
            other => {
                warn!("UBIBLK_SPILL_CRASH_AT={other} names no crash point, ignoring");
                None
            }
        }
    }
}

/// Drives stripes out of the local device when it is over its ceiling.
pub struct Evictor {
    cfg: EvictorConfig,
    state: SharedMetadataState,
    read_channel: Box<dyn IoChannel>,
    /// The PUT store; None means clean-only.
    store: Option<Box<dyn ArchiveStore>>,
    codec: SpillCodec,
    puncher: Box<dyn HolePuncher>,
    buffers: AlignedBufferPool,
    in_progress: HashMap<usize, Eviction>,
    epoch: u64,
    puts_in_flight: usize,
    /// The CLOCK hand: the next stripe the sweep examines.
    hand: usize,
    /// Skipped by the hand for `ABORT_SKIP` after an abort.
    recently_aborted: HashMap<usize, Instant>,
    /// While the store is degraded, when a single upload may probe it again.
    degraded_until: Option<Instant>,
    backoff: Duration,
    last_statfs: Option<Instant>,
    /// The last statfs answer; None until the first one succeeds, in which
    /// case the free-space rules do not apply rather than stalling the guest
    /// on a reading that was never taken.
    free_bytes: Option<u64>,
    /// Over the ceiling once; keep going down to the low-water mark.
    evicting_to_low_water: bool,
    /// The last tick found soft pressure.
    under_pressure: bool,
    /// Stripes the hand has examined since it last claimed one, so a sweep
    /// that finds nothing stops keeping the coordinator spinning after one
    /// revolution.
    idle_examined: usize,
    held_for_space: Vec<usize>,
    released_fetches: Vec<usize>,
    released_pushes: Vec<ReleasedPush>,
    /// Cleared on EOPNOTSUPP: no eviction can free space on this filesystem.
    punch_supported: bool,
    #[cfg(feature = "fault-injection")]
    crash_at: Option<CrashPoint>,
}

impl Evictor {
    /// `store` is the PUT store (None: clean-only). `read_channel` comes from
    /// `target_dev.create_channel()` so reads decrypt through crypt.
    #[error_context("Failed to build the evictor")]
    pub fn new(
        cfg: EvictorConfig,
        read_channel: Box<dyn IoChannel>,
        store: Option<Box<dyn ArchiveStore>>,
        codec: SpillCodec,
        puncher: Box<dyn HolePuncher>,
        state: SharedMetadataState,
    ) -> Result<Self> {
        if cfg.stripe_sector_count == 0 || cfg.max_concurrent_evictions == 0 || cfg.sweep_batch == 0
        {
            return Err(crate::ubiblk_error!(InvalidParameter {
                description: format!(
                    "evictor needs stripe_sector_count, max_concurrent_evictions and \
                     sweep_batch above zero (got {}, {}, {})",
                    cfg.stripe_sector_count, cfg.max_concurrent_evictions, cfg.sweep_batch
                ),
            }));
        }
        if !cfg.alignment.is_power_of_two() {
            return Err(crate::ubiblk_error!(InvalidParameter {
                description: format!(
                    "evictor buffer alignment must be a power of two, got {}",
                    cfg.alignment
                ),
            }));
        }
        let stripe_size = cfg.stripe_sector_count as usize * SECTOR_SIZE;
        let buffers =
            AlignedBufferPool::new(cfg.alignment, cfg.max_concurrent_evictions, stripe_size);
        #[cfg(feature = "fault-injection")]
        let crash_at = CrashPoint::from_env();
        Ok(Evictor {
            cfg,
            state,
            read_channel,
            store,
            codec,
            puncher,
            buffers,
            in_progress: HashMap::new(),
            epoch: 0,
            puts_in_flight: 0,
            hand: 0,
            recently_aborted: HashMap::new(),
            degraded_until: None,
            backoff: BACKOFF_MIN,
            last_statfs: None,
            free_bytes: None,
            evicting_to_low_water: false,
            under_pressure: false,
            idle_examined: 0,
            held_for_space: Vec::new(),
            released_fetches: Vec::new(),
            released_pushes: Vec::new(),
            punch_supported: true,
            #[cfg(feature = "fault-injection")]
            crash_at,
        })
    }

    /// Abort the process if `UBIBLK_SPILL_CRASH_AT` named this point.
    #[cfg(feature = "fault-injection")]
    pub fn crash_if_at(&self, point: CrashPoint) {
        if self.crash_at == Some(point) {
            error!("UBIBLK_SPILL_CRASH_AT: aborting at {point:?}");
            std::process::abort();
        }
    }

    // ---- geometry

    fn stripe_size(&self) -> u64 {
        self.cfg.stripe_sector_count * SECTOR_SIZE as u64
    }

    /// First sector and sector count of a stripe; the last stripe may be short.
    fn stripe_sectors(&self, stripe_id: usize) -> (u64, u64) {
        let offset = stripe_id as u64 * self.cfg.stripe_sector_count;
        let count = self
            .cfg
            .stripe_sector_count
            .min(self.cfg.target_sector_count.saturating_sub(offset));
        (offset, count)
    }

    /// Byte offset and length of a stripe's data in `data_path`.
    fn stripe_bytes(&self, stripe_id: usize) -> (u64, u64) {
        let (offset, count) = self.stripe_sectors(stripe_id);
        (offset * SECTOR_SIZE as u64, count * SECTOR_SIZE as u64)
    }

    // ---- startup

    /// Startup pass: punch every stripe with EVICTED set, coalescing runs of
    /// consecutive stripes into one call. Idempotent. Counts one
    /// `startup_punches` per run and returns the number of stripes covered.
    pub fn punch_all_evicted(&mut self, metadata: &UbiMetadata) -> Result<usize> {
        let mut punched = 0;
        let mut runs = 0;
        let mut run: Option<(usize, usize)> = None;
        for stripe_id in metadata.evicted_stripe_ids() {
            run = Some(match run {
                Some((start, end)) if stripe_id == end + 1 => (start, stripe_id),
                Some((start, end)) => {
                    punched += self.punch_run(start, end);
                    runs += 1;
                    (stripe_id, stripe_id)
                }
                None => (stripe_id, stripe_id),
            });
        }
        if let Some((start, end)) = run {
            punched += self.punch_run(start, end);
            runs += 1;
        }
        info!("Startup punch pass: {punched} evicted stripe(s) in {runs} run(s)");
        Ok(punched)
    }

    /// Punch stripes `start..=end` in one call; the stripes covered on success.
    fn punch_run(&mut self, start: usize, end: usize) -> usize {
        let (offset, _) = self.stripe_bytes(start);
        let (last_offset, last_len) = self.stripe_bytes(end);
        let len = last_offset.saturating_sub(offset) + last_len;
        match self.puncher.punch(offset, len) {
            Ok(()) => {
                self.state
                    .spill()
                    .startup_punches
                    .fetch_add(1, Ordering::Relaxed);
                end - start + 1
            }
            Err(e) => {
                error!("Startup punch of stripes {start}..={end} failed: {e}");
                self.record_punch_failure(e);
                0
            }
        }
    }

    fn record_punch_failure(&mut self, errno: Errno) {
        let spill = self.state.spill();
        spill.punch_failures.fetch_add(1, Ordering::Relaxed);
        spill.degraded_reasons.fetch_add(1, Ordering::Relaxed);
        if errno == Errno::EOPNOTSUPP && self.punch_supported {
            error!("Hole punching is not supported on this filesystem; no further evictions");
            self.punch_supported = false;
        }
    }

    // ---- the tick

    /// One tick: apply flusher outcomes for odd tokens, poll read and PUT
    /// completions, advance stages, punch, refresh statfs, set the gate, start
    /// new evictions while over the ceiling.
    pub fn update(&mut self, flusher: &mut MetadataFlusher, outcomes: &[PersistOutcome]) {
        let now = Instant::now();
        self.refresh_free_bytes(now);
        self.apply_outcomes(outcomes, now);
        self.resubmit_reads();
        self.poll_reads(now);
        self.poll_puts(flusher, now);
        self.advance_draining(flusher, now);
        self.retry_header_ops(flusher, now);
        self.recently_aborted
            .retain(|_, at| now.duration_since(*at) < ABORT_SKIP);
        // After the completions, so the gate and `busy` see this tick's
        // evictions rather than last tick's.
        self.assess_pressure();
        self.select_victims(now);
    }

    fn refresh_free_bytes(&mut self, now: Instant) {
        if self
            .last_statfs
            .is_some_and(|at| now.duration_since(at) < STATFS_INTERVAL)
        {
            return;
        }
        self.last_statfs = Some(now);
        match self.puncher.free_bytes() {
            Ok(free) => {
                self.free_bytes = Some(free);
                self.state.spill().free_bytes.store(free, Ordering::Relaxed);
            }
            Err(e) => warn!("statfs of {} failed: {e}", self.cfg.data_path.display()),
        }
    }

    /// Free bytes below `threshold`, if the filesystem has answered at all.
    fn free_below(&self, threshold: u64) -> bool {
        self.free_bytes.is_some_and(|free| free < threshold)
    }

    /// Soft pressure decides whether to evict; hard pressure closes the gate.
    fn assess_pressure(&mut self) {
        let resident_bytes = self
            .state
            .resident_stripes()
            .saturating_mul(self.stripe_size());
        let max = self.cfg.max_local_bytes;
        let hard = resident_bytes > max.saturating_add(self.cfg.hard_margin_bytes)
            || self.free_below(self.cfg.min_free_bytes / 2);
        let over = resident_bytes > max || self.free_below(self.cfg.min_free_bytes);
        if over {
            self.evicting_to_low_water = true;
        }
        let soft = over
            || (self.evicting_to_low_water
                && resident_bytes > max.saturating_sub(self.cfg.low_water_bytes));
        if !soft {
            self.evicting_to_low_water = false;
        }
        self.under_pressure = soft;

        let current = self.state.write_gate();
        let next = if hard {
            match self.cfg.on_full {
                OnFull::Stall => GATE_HOLD,
                OnFull::Fail => GATE_FAIL,
            }
        } else if current != GATE_OPEN && !self.free_below(self.cfg.min_free_bytes) {
            GATE_OPEN
        } else {
            current
        };
        if next == current {
            return;
        }
        self.state.set_write_gate(next);
        if next == GATE_OPEN {
            warn!(
                "Write gate reopened: {resident_bytes} resident bytes, {:?} free",
                self.free_bytes
            );
            self.released_fetches.append(&mut self.held_for_space);
        } else {
            warn!(
                "Write gate closed ({}): {resident_bytes} resident bytes against a ceiling of \
                 {max} + {} margin, {:?} free against a minimum of {}",
                if next == GATE_HOLD { "hold" } else { "fail" },
                self.cfg.hard_margin_bytes,
                self.free_bytes,
                self.cfg.min_free_bytes
            );
        }
    }

    /// Would completing the evictions already in flight leave the device
    /// where it should be? If not, more are needed.
    fn more_evictions_needed(&self) -> bool {
        let active = self.in_progress.values().filter(|e| !e.aborted).count() as u64;
        let stripe_size = self.stripe_size();
        let resident = self
            .state
            .resident_stripes()
            .saturating_sub(active)
            .saturating_mul(stripe_size);
        let free = self
            .free_bytes
            .map(|free| free.saturating_add(active.saturating_mul(stripe_size)));
        let max = self.cfg.max_local_bytes;
        resident > max
            || free.is_some_and(|free| free < self.cfg.min_free_bytes)
            || (self.evicting_to_low_water
                && resident > max.saturating_sub(self.cfg.low_water_bytes))
    }

    // ---- outcomes and completions

    fn apply_outcomes(&mut self, outcomes: &[PersistOutcome], now: Instant) {
        for outcome in outcomes.iter().filter(|o| Self::owns_token(o.token)) {
            let stripe_id = outcome.stripe_id;
            let Some(eviction) = self.in_progress.get_mut(&stripe_id) else {
                debug!("Header outcome for stripe {stripe_id} without an eviction, ignoring");
                continue;
            };
            let Stage::WritingHeader { token } = eviction.stage else {
                debug!("Header outcome for stripe {stripe_id} outside WritingHeader, ignoring");
                continue;
            };
            if token != outcome.token || eviction.aborted {
                continue;
            }
            match outcome.result {
                PersistResult::Durable => self.complete_eviction(stripe_id),
                PersistResult::NotWritten if !eviction.committed => {
                    warn!("Header write for evicting stripe {stripe_id} failed; keeping it local");
                    self.abort(stripe_id, now);
                }
                result => {
                    // The disk may already say EVICTED: releasing the stripe
                    // could let a write land that a restart would punch.
                    eviction.committed = true;
                    eviction.retry_at = Some(now + HEADER_RETRY);
                    self.state
                        .spill()
                        .degraded_reasons
                        .fetch_add(1, Ordering::Relaxed);
                    error!(
                        "Header op for evicting stripe {stripe_id} ended {result:?}; the disk may \
                         hold either byte, retrying in {HEADER_RETRY:?}"
                    );
                }
            }
        }
    }

    /// The EVICTED header is durable: punch the blocks and let the state say
    /// the stripe is gone.
    fn complete_eviction(&mut self, stripe_id: usize) {
        let Some(eviction) = self.in_progress.remove(&stripe_id) else {
            return;
        };
        #[cfg(feature = "fault-injection")]
        self.crash_if_at(CrashPoint::AfterHeaderFlush);
        let (offset, len) = self.stripe_bytes(stripe_id);
        match self.puncher.punch(offset, len) {
            Ok(()) => {
                self.state.spill().punches.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                // The disk already says not-local, so the state must follow
                // whatever happened to the blocks.
                error!("Punch of stripe {stripe_id} ({offset}, {len}) failed: {e}");
                self.record_punch_failure(e);
            }
        }
        #[cfg(feature = "fault-injection")]
        self.crash_if_at(CrashPoint::AfterPunch);
        let in_s3 = eviction.kind == Kind::Dirty;
        self.state
            .finish_evicting(stripe_id, eviction.previous, in_s3);
        let counter = match eviction.kind {
            Kind::Clean => &self.state.spill().evicted_clean,
            Kind::Dirty => &self.state.spill().evicted_dirty,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        debug!("Stripe {stripe_id} evicted ({:?})", eviction.kind);
        if eviction.deferred_fetch {
            self.released_fetches.push(stripe_id);
        }
        if let Some((data, permit)) = eviction.deferred_push {
            self.released_pushes.push((stripe_id, data, permit));
        }
    }

    /// Enter the SQE of a read whose submit failed, so its completion can
    /// drain the aborted record. A bare submit is enough: the ring still
    /// holds the entry.
    fn resubmit_reads(&mut self) {
        if !self.in_progress.values().any(|e| e.resubmit_read) {
            return;
        }
        match self.read_channel.submit() {
            Ok(()) => {
                for eviction in self.in_progress.values_mut() {
                    eviction.resubmit_read = false;
                }
            }
            Err(e) => debug!("Re-submitting the evictor's reads still fails: {e}"),
        }
    }

    fn poll_reads(&mut self, now: Instant) {
        for (stripe_id, ok) in self.read_channel.poll() {
            let Some(eviction) = self.in_progress.get_mut(&stripe_id) else {
                error!("Read completion for stripe {stripe_id} without an eviction");
                continue;
            };
            if eviction.stage != Stage::Reading {
                error!("Read completion for stripe {stripe_id} outside Reading");
                continue;
            }
            eviction.io_outstanding = false;
            let Some(buf) = eviction.buf.take() else {
                error!("Read completion for stripe {stripe_id} without a buffer");
                continue;
            };
            if eviction.aborted {
                self.buffers.return_buffer(&buf);
                self.in_progress.remove(&stripe_id);
                continue;
            }
            if !ok {
                error!("Reading stripe {stripe_id} for upload failed; keeping it local");
                self.buffers.return_buffer(&buf);
                self.abort(stripe_id, now);
                continue;
            }
            let (_, len) = self.stripe_bytes(stripe_id);
            let object = {
                let data = buf.borrow();
                match data.as_slice().get(..len as usize) {
                    Some(data) => self.codec.encode(stripe_id, data, Some(self.state.spill())),
                    None => Err(crate::ubiblk_error!(InvalidParameter {
                        description: format!(
                            "stripe {stripe_id} is {len} bytes but its buffer holds {}",
                            data.len()
                        ),
                    })),
                }
            };
            self.buffers.return_buffer(&buf);
            match object {
                Ok(object) => self.start_put(stripe_id, object, now),
                Err(e) => {
                    error!("Encoding stripe {stripe_id} failed: {e}");
                    self.abort(stripe_id, now);
                }
            }
        }
    }

    fn start_put(&mut self, stripe_id: usize, object: Vec<u8>, now: Instant) {
        let Some(store) = self.store.as_mut() else {
            error!("Stripe {stripe_id} needs an upload but there is no store");
            self.abort(stripe_id, now);
            return;
        };
        let Some(eviction) = self.in_progress.get_mut(&stripe_id) else {
            return;
        };
        eviction.object_len = object.len() as u64;
        eviction.stage = Stage::Putting;
        eviction.io_outstanding = true;
        store.start_put_object(&spill_object_name(&self.cfg.device_id, stripe_id), object);
        self.puts_in_flight += 1;
        self.state.spill().puts.fetch_add(1, Ordering::Relaxed);
    }

    fn poll_puts(&mut self, flusher: &mut MetadataFlusher, now: Instant) {
        let Some(store) = self.store.as_mut() else {
            return;
        };
        for (name, result) in store.poll_puts() {
            self.puts_in_flight = self.puts_in_flight.saturating_sub(1);
            let Some(stripe_id) = parse_spill_object_name(&name) else {
                error!("PUT completion for an object that is not a stripe: {name}");
                continue;
            };
            let Some(eviction) = self.in_progress.get_mut(&stripe_id) else {
                error!("PUT completion for stripe {stripe_id} without an eviction");
                continue;
            };
            if eviction.stage != Stage::Putting {
                error!("PUT completion for stripe {stripe_id} outside Putting");
                continue;
            }
            eviction.io_outstanding = false;
            if eviction.aborted {
                self.in_progress.remove(&stripe_id);
                continue;
            }
            match result {
                Ok(()) => {
                    self.state
                        .spill()
                        .put_bytes
                        .fetch_add(eviction.object_len, Ordering::Relaxed);
                    self.store_recovered();
                    #[cfg(feature = "fault-injection")]
                    self.crash_if_at(CrashPoint::AfterPut);
                    self.issue_header_op(flusher, stripe_id);
                }
                Err(e) => {
                    error!("Upload of stripe {stripe_id} failed: {e}");
                    self.state
                        .spill()
                        .put_failures
                        .fetch_add(1, Ordering::Relaxed);
                    self.store_failed(now);
                    self.abort(stripe_id, now);
                }
            }
        }
    }

    fn store_recovered(&mut self) {
        if self.state.spill().degraded.swap(false, Ordering::AcqRel) {
            info!("Spill store recovered");
        }
        self.degraded_until = None;
        self.backoff = BACKOFF_MIN;
    }

    fn store_failed(&mut self, now: Instant) {
        if !self.state.spill().degraded.swap(true, Ordering::AcqRel) {
            warn!(
                "Spill store degraded; dirty evictions paused for {:?}",
                self.backoff
            );
        }
        self.degraded_until = Some(now + self.backoff);
        self.backoff = (self.backoff * 2).min(BACKOFF_MAX);
    }

    /// Hand the EVICTED header op to the flusher; from here the eviction is
    /// committed unless the disk provably kept the old byte.
    fn issue_header_op(&mut self, flusher: &mut MetadataFlusher, stripe_id: usize) {
        let Some(eviction) = self.in_progress.get_mut(&stripe_id) else {
            return;
        };
        let token = (eviction.epoch << 1) | 1;
        let (set, clear) = match eviction.kind {
            // IN_S3 is cleared too: a stripe evicted dirty earlier and
            // re-materialised still carries it as a purge hint, and under
            // EVICTED it would read as authoritative.
            Kind::Clean => (
                metadata_flags::EVICTED,
                metadata_flags::FETCHED | metadata_flags::IN_S3,
            ),
            Kind::Dirty => (
                metadata_flags::EVICTED | metadata_flags::IN_S3,
                metadata_flags::FETCHED,
            ),
        };
        eviction.stage = Stage::WritingHeader { token };
        eviction.retry_at = None;
        flusher.update_stripe_header(stripe_id, set, clear, token);
    }

    fn retry_header_ops(&mut self, flusher: &mut MetadataFlusher, now: Instant) {
        let due: Vec<usize> = self
            .in_progress
            .iter()
            .filter(|(_, e)| !e.aborted && e.retry_at.is_some_and(|at| at <= now))
            .map(|(stripe_id, _)| *stripe_id)
            .collect();
        for stripe_id in due {
            error!("Re-issuing the header op for evicting stripe {stripe_id}");
            self.issue_header_op(flusher, stripe_id);
        }
    }

    // ---- draining

    /// A stripe is clean when the live snapshot can serve it again: nothing
    /// written or pushed, the source holds it, it became resident while the
    /// subscription was up, and it never went through the store. Only with
    /// clean eviction enabled.
    ///
    /// IN_S3 on a resident stripe means it came back from the store, and a
    /// stripe was uploaded because its content could not be trusted to match
    /// the snapshot (its WRITTEN bit may have been lost to a crash). The round
    /// trip sets FETCHED_LIVE again, so without this term such a stripe would
    /// be dropped next time and the snapshot's pre-image served in its place.
    fn clean_predicate(&self, stripe_id: usize) -> bool {
        let flags = self.state.stripe_flags(stripe_id);
        self.cfg.clean_eviction
            && self.state.source_live()
            && !self.state.stripe_written(stripe_id)
            && flags & stripe_flags::PUSHED == 0
            && flags & stripe_flags::IN_S3 == 0
            && flags & stripe_flags::HAS_SOURCE != 0
            && flags & stripe_flags::FETCHED_LIVE != 0
    }

    /// Whether a dirty eviction may start now: a store, PUT budget, and a
    /// store that is not degraded, or one probe once its backoff has passed.
    fn dirty_allowed(&self, now: Instant) -> bool {
        if self.store.is_none() || self.puts_in_flight >= self.cfg.max_concurrent_evictions {
            return false;
        }
        if !self.state.spill().degraded.load(Ordering::Acquire) {
            return true;
        }
        let backoff_passed = self.degraded_until.is_none_or(|until| now >= until);
        let probe_in_flight = self
            .in_progress
            .values()
            .any(|e| !e.aborted && e.kind == Kind::Dirty);
        backoff_passed && !probe_in_flight
    }

    fn advance_draining(&mut self, flusher: &mut MetadataFlusher, now: Instant) {
        let draining: Vec<usize> = self
            .in_progress
            .iter()
            .filter(|(_, e)| !e.aborted && e.stage == Stage::Draining)
            .map(|(stripe_id, _)| *stripe_id)
            .collect();
        for stripe_id in draining {
            let Some(eviction) = self.in_progress.get(&stripe_id) else {
                continue;
            };
            // SeqCst load after the CAS: a channel that pinned before this
            // load is counted; one that pins after it sees Evicting.
            if self.state.stripe_inflight(stripe_id) != 0 {
                if now.duration_since(eviction.started) > DRAIN_TIMEOUT {
                    error!(
                        "Stripe {stripe_id} still has guest I/O in flight after {DRAIN_TIMEOUT:?}; \
                         a completion was lost, abandoning its eviction"
                    );
                    self.state
                        .spill()
                        .degraded_reasons
                        .fetch_add(1, Ordering::Relaxed);
                    self.abort(stripe_id, now);
                }
                continue;
            }
            let mut kind = eviction.kind;
            if kind == Kind::Clean && !self.clean_predicate(stripe_id) {
                // A write or push landed, or the snapshot ended, since the
                // claim: the stripe has to be uploaded or left alone.
                if self.dirty_allowed(now) {
                    kind = Kind::Dirty;
                    if let Some(eviction) = self.in_progress.get_mut(&stripe_id) {
                        eviction.kind = kind;
                    }
                } else {
                    debug!("Stripe {stripe_id} is no longer clean and cannot be uploaded");
                    self.abort(stripe_id, now);
                    continue;
                }
            }
            match kind {
                Kind::Clean => self.issue_header_op(flusher, stripe_id),
                Kind::Dirty => self.start_read(stripe_id, now),
            }
        }
    }

    fn start_read(&mut self, stripe_id: usize, now: Instant) {
        let Some(buf) = self.buffers.get_buffer() else {
            // Every buffer is with an eviction that has not drained its read
            // yet; this one waits for the next tick.
            return;
        };
        let (offset, count) = self.stripe_sectors(stripe_id);
        let Some(eviction) = self.in_progress.get_mut(&stripe_id) else {
            self.buffers.return_buffer(&buf);
            return;
        };
        eviction.buf = Some(buf.clone());
        eviction.stage = Stage::Reading;
        eviction.io_outstanding = true;
        self.read_channel
            .add_read(offset, count as u32, buf.clone(), stripe_id);
        if let Err(e) = self.read_channel.submit() {
            // The SQE is in the ring and a later submit enters it, so base
            // may still carry the read out. The record keeps its buffer and
            // waits for the completion under `aborted`, as an aborted PUT
            // does. Returning the buffer now would let the late read land in
            // a buffer handed to another eviction, and dropping the record
            // would let the completion (keyed by stripe id) be taken for a
            // new read of this stripe, uploading bytes read before the
            // guest's later writes.
            error!("Submitting the read of stripe {stripe_id} failed: {e}");
            self.state
                .spill()
                .degraded_reasons
                .fetch_add(1, Ordering::Relaxed);
            if let Some(eviction) = self.in_progress.get_mut(&stripe_id) {
                eviction.resubmit_read = true;
            }
            self.abort(stripe_id, now);
        }
    }

    /// Put the stripe back where it was. Legal only before the header op is
    /// handed to the flusher, or when the flusher reports the disk provably
    /// still holds the old byte.
    fn abort(&mut self, stripe_id: usize, now: Instant) {
        let Some(eviction) = self.in_progress.get_mut(&stripe_id) else {
            return;
        };
        if eviction.aborted {
            return;
        }
        debug!(
            "Aborting eviction of stripe {stripe_id} in {:?}",
            eviction.stage
        );
        self.state.abort_evicting(stripe_id, eviction.previous);
        // The guest wanted it: give it a full revolution before the hand
        // considers it again.
        self.state
            .set_stripe_flags(stripe_id, stripe_flags::REFERENCED);
        self.recently_aborted.insert(stripe_id, now);
        self.state
            .spill()
            .evictions_aborted
            .fetch_add(1, Ordering::Relaxed);
        // The stripe is resident again: a deferred fetch finds it Complete on
        // the channel's re-send, and a deferred push is the same content
        // (PUSHED is already recorded). Dropping the push releases its permit.
        eviction.deferred_fetch = false;
        eviction.deferred_push = None;
        if eviction.io_outstanding {
            eviction.aborted = true;
        } else {
            self.in_progress.remove(&stripe_id);
        }
    }

    // ---- selection

    fn select_victims(&mut self, now: Instant) {
        if !self.under_pressure || !self.punch_supported {
            return;
        }
        let stripe_count = self.state.stripe_count();
        if stripe_count == 0 {
            return;
        }
        let clean_possible = self.cfg.clean_eviction && self.state.source_live();
        let dirty_possible = self.dirty_allowed(now);
        if !clean_possible && !dirty_possible {
            return;
        }
        // With clean eviction on, a dirty candidate waits for the rest of the
        // batch in case a clean one turns up: dropping a stripe is cheaper
        // than uploading one.
        let mut deferred_dirty: Vec<usize> = Vec::new();
        let mut examined = 0;
        let mut claimed = 0;
        while examined < self.cfg.sweep_batch
            && self.in_progress.len() < self.cfg.max_concurrent_evictions
            && self.more_evictions_needed()
        {
            let stripe_id = self.hand;
            self.hand = (self.hand + 1) % stripe_count;
            examined += 1;
            if self.in_progress.contains_key(&stripe_id)
                || self.recently_aborted.contains_key(&stripe_id)
                || !self.state.stripe_resident(stripe_id)
            {
                continue;
            }
            let kind = if self.clean_predicate(stripe_id) {
                Kind::Clean
            } else {
                Kind::Dirty
            };
            if kind == Kind::Dirty && !dirty_possible {
                continue;
            }
            // Second chance: a stripe used since the hand last passed keeps
            // its place for one more revolution.
            if self.state.take_reference(stripe_id) {
                continue;
            }
            match kind {
                Kind::Clean if clean_possible => {
                    if self.begin_eviction(stripe_id, Kind::Clean, now) {
                        claimed += 1;
                    }
                }
                Kind::Clean => {}
                Kind::Dirty if clean_possible => {
                    if deferred_dirty.len() < self.cfg.max_concurrent_evictions {
                        deferred_dirty.push(stripe_id);
                    }
                }
                Kind::Dirty => {
                    // Re-checked per claim: under half-open only one probe
                    // may be in flight, and the first claim is it.
                    if self.dirty_allowed(now) && self.begin_eviction(stripe_id, Kind::Dirty, now) {
                        claimed += 1;
                    }
                }
            }
        }
        for stripe_id in deferred_dirty {
            if self.in_progress.len() >= self.cfg.max_concurrent_evictions
                || !self.more_evictions_needed()
                || !self.dirty_allowed(now)
            {
                break;
            }
            if self.begin_eviction(stripe_id, Kind::Dirty, now) {
                claimed += 1;
            }
        }
        if claimed == 0 {
            self.idle_examined = self.idle_examined.saturating_add(examined);
        }
    }

    fn begin_eviction(&mut self, stripe_id: usize, kind: Kind, now: Instant) -> bool {
        let Some(previous) = self.state.try_begin_evicting(stripe_id) else {
            return false;
        };
        self.epoch += 1;
        self.idle_examined = 0;
        debug!("Evicting stripe {stripe_id} ({kind:?}) from state {previous}");
        self.in_progress.insert(
            stripe_id,
            Eviction {
                previous,
                kind,
                stage: Stage::Draining,
                epoch: self.epoch,
                buf: None,
                object_len: 0,
                started: now,
                committed: false,
                retry_at: None,
                deferred_fetch: false,
                deferred_push: None,
                io_outstanding: false,
                resubmit_read: false,
                aborted: false,
            },
        );
        true
    }

    // ---- the coordinator's questions

    /// A guest `Fetch { S }` reached the coordinator: abort or defer an
    /// eviction of S, hold or refuse it under a closed gate, else forward.
    pub fn on_fetch_request(&mut self, stripe_id: usize) -> FetchDisposition {
        if let Some(eviction) = self.in_progress.get_mut(&stripe_id) {
            if !eviction.aborted {
                return match eviction.stage {
                    Stage::WritingHeader { .. } => {
                        eviction.deferred_fetch = true;
                        FetchDisposition::Deferred
                    }
                    Stage::Draining | Stage::Reading | Stage::Putting => {
                        self.abort(stripe_id, Instant::now());
                        FetchDisposition::Aborted
                    }
                };
            }
        }
        if !self.state.stripe_resident(stripe_id) {
            match self.state.write_gate() {
                GATE_HOLD => {
                    // The channel re-sends while it waits, so one entry per
                    // stripe is enough.
                    if !self.held_for_space.contains(&stripe_id) {
                        self.held_for_space.push(stripe_id);
                    }
                    return FetchDisposition::HeldForSpace;
                }
                GATE_FAIL => {
                    // Kept all the same. The channel fails the request only
                    // if it polls while the gate is still closed; should the
                    // gate reopen first, its Pending front waits for a fetch
                    // nobody sends again (no re-send for a NotFetched
                    // stripe). The replay costs at most one pull the fetcher
                    // may find redundant.
                    if !self.held_for_space.contains(&stripe_id) {
                        self.held_for_space.push(stripe_id);
                    }
                    return FetchDisposition::Refused;
                }
                _ => {}
            }
        }
        FetchDisposition::Forward
    }

    /// The permit moves in; it comes back for `Forward`, is held for `Deferred`,
    /// and is dropped for `Ignore` and `AbortedEviction`.
    pub fn on_pushed_stripe(
        &mut self,
        stripe_id: usize,
        data: &[u8],
        permit: PushPermit,
    ) -> (PushDisposition, Option<PushPermit>) {
        if let Some(eviction) = self.in_progress.get_mut(&stripe_id) {
            if !eviction.aborted {
                return match (eviction.kind, eviction.stage) {
                    // Local or spilled content is the fork's own; a pre-image
                    // must not overwrite it.
                    (Kind::Dirty, _) => (PushDisposition::Ignore, None),
                    (Kind::Clean, Stage::WritingHeader { .. }) => {
                        eviction.deferred_push = Some((data.to_vec(), permit));
                        (PushDisposition::Deferred, None)
                    }
                    (Kind::Clean, _) => {
                        // The local copy is the snapshot content; keep it. The
                        // stripe is dirty by PUSHED from now on.
                        self.abort(stripe_id, Instant::now());
                        (PushDisposition::AbortedEviction, None)
                    }
                };
            }
        }
        let flags = self.state.stripe_flags(stripe_id);
        let gone = self.state.stripe_fetch_state(stripe_id) == Evicted
            || flags & stripe_flags::WAS_EVICTED != 0;
        if gone && flags & stripe_flags::IN_S3 != 0 {
            // The fork's data is in the store; a pre-image landing now would
            // replace it. IN_S3 alone decides: a dirty eviction always PUTs,
            // so a stripe evicted without it held the snapshot's content, and
            // WRITTEN on such a stripe is a write still queued behind the
            // eviction (set at queue time), whose pull the replica refuses
            // once it has pushed. The push is then the only copy the fork can
            // get; the fetcher parks it behind that pull and writes it when
            // the pull is refused.
            return (PushDisposition::Ignore, None);
        }
        (PushDisposition::Forward, Some(permit))
    }

    /// Fetches and pushes released this tick, for the coordinator to route.
    pub fn take_released(&mut self) -> (Vec<usize>, Vec<ReleasedPush>) {
        (
            std::mem::take(&mut self.released_fetches),
            std::mem::take(&mut self.released_pushes),
        )
    }

    /// True while an eviction is in progress, something released waits to be
    /// routed, or the device is over its ceiling and a sweep could still find
    /// a victim. An aborted record only waits for a completion it is owed,
    /// which the idle tick's poll collects as well; spinning for it would
    /// hold a core for as long as a failed read submit keeps failing, or an
    /// aborted PUT takes to time out.
    pub fn busy(&self) -> bool {
        self.in_progress.values().any(|e| !e.aborted)
            || !self.released_fetches.is_empty()
            || !self.released_pushes.is_empty()
            || (self.under_pressure && self.sweep_may_progress())
    }

    /// A sweep is worth spinning for until the hand has been round once
    /// without a claim; after that the idle tick is soon enough.
    fn sweep_may_progress(&self) -> bool {
        self.punch_supported
            && self.in_progress.len() < self.cfg.max_concurrent_evictions
            && self.idle_examined < self.state.stripe_count()
            && ((self.cfg.clean_eviction && self.state.source_live())
                || self.dirty_allowed(Instant::now()))
    }

    /// Odd tokens belong to the evictor; even tokens to the coordinator.
    pub fn owns_token(token: u64) -> bool {
        token & 1 == 1
    }

    // ---- test seams

    /// The kind and stage of the eviction of `stripe_id`, if one is in
    /// progress and not aborted.
    #[cfg(test)]
    pub(super) fn eviction_for_test(&self, stripe_id: usize) -> Option<(Kind, Stage)> {
        self.in_progress
            .get(&stripe_id)
            .filter(|e| !e.aborted)
            .map(|e| (e.kind, e.stage))
    }

    /// Records, aborted ones included, so a test can see one drain.
    #[cfg(test)]
    pub(super) fn records_for_test(&self) -> usize {
        self.in_progress.len()
    }

    #[cfg(test)]
    pub(super) fn hand_for_test(&self) -> usize {
        self.hand
    }

    #[cfg(test)]
    pub(super) fn puts_in_flight_for_test(&self) -> usize {
        self.puts_in_flight
    }

    #[cfg(test)]
    pub(super) fn punch_supported_for_test(&self) -> bool {
        self.punch_supported
    }

    #[cfg(test)]
    pub(super) fn held_for_space_for_test(&self) -> &[usize] {
        &self.held_for_space
    }

    /// Move every deadline `by` into the past, so a test need not wait for
    /// the drain timeout, the abort skip, the header retry or the backoff.
    #[cfg(test)]
    pub(super) fn advance_time_for_test(&mut self, by: Duration) {
        let back = |at: Instant| at.checked_sub(by).unwrap_or(at);
        for eviction in self.in_progress.values_mut() {
            eviction.started = back(eviction.started);
            eviction.retry_at = eviction.retry_at.map(back);
        }
        for at in self.recently_aborted.values_mut() {
            *at = back(*at);
        }
        self.degraded_until = self.degraded_until.map(back);
        self.last_statfs = self.last_statfs.map(back);
    }
}
