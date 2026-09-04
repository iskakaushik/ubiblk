use crate::block_device::{metadata_flags, UbiMetadata};
use log::error;
use std::sync::{
    atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicU8, Ordering},
    Arc,
};

pub const NotFetched: u8 = 0;
pub const Fetched: u8 = 1;
pub const Failed: u8 = 2;
pub const NoSource: u8 = 3;
/// Claimed by the evictor. New guest I/O queues and re-sends Fetch; the
/// coordinator may abort back to the previous state until the header write
/// is handed to the flusher.
pub const Evicting: u8 = 4;
/// Not local. The composite source routes a fetch by IN_S3, then source_live.
pub const Evicted: u8 = 5;

pub const NotWritten: u8 = 0;
pub const Written: u8 = 1;

/// Per-stripe side bits. Bits 2, 4, 5 mirror the header byte exactly so a
/// persisted header can be OR-ed in. Bits 3, 6 and 7 live only in memory.
pub mod stripe_flags {
    /// Mirrors the header's HAS_SOURCE: the snapshot holds this stripe.
    pub const HAS_SOURCE: u8 = 1 << 2;
    /// The header still says EVICTED although the state is Failed: a fetch of
    /// an evicted stripe failed for good. Set by `set_stripe_failed`, cleared
    /// by `mark_stripe_resident`. While it is set, `mark_stripe_fetched`
    /// refuses the stripe: the disk would punch it after a crash, so it may
    /// become resident only once the header clearing EVICTED is durable (I4).
    pub const WAS_EVICTED: u8 = 1 << 3;
    /// Mirrors the header's IN_S3: the spill store holds an object for this
    /// stripe. Authoritative only while the stripe is Evicted.
    pub const IN_S3: u8 = 1 << 4;
    /// Mirrors the header's PUSHED: a snapshot push for this stripe arrived,
    /// so the live replica will not serve it again.
    pub const PUSHED: u8 = 1 << 5;
    /// CLOCK reference bit: set by every request that passes to base on a
    /// resident stripe, cleared by the evictor's hand.
    pub const REFERENCED: u8 = 1 << 6;
    /// The stripe became resident (fetched or pushed) in this process while
    /// `source_live` was true. Only such stripes may be evicted clean: a
    /// stripe resident from before this run may carry a write whose WRITTEN
    /// bit was lost to a crash, and a stripe fetched before the subscription
    /// came up may have been copied out in the gap.
    pub const FETCHED_LIVE: u8 = 1 << 7;
    /// The side bits a flusher completion may OR in from a header byte.
    pub const PERSISTED_MASK: u8 = HAS_SOURCE | IN_S3 | PUSHED;
}

pub const GATE_OPEN: u8 = 0;
/// on_full = stall: the channel queues new writes; the coordinator holds
/// fetches for non-resident stripes.
pub const GATE_HOLD: u8 = 1;
/// on_full = fail: the channel fails new writes and queued requests that wait
/// on a non-resident stripe.
pub const GATE_FAIL: u8 = 2;

/// Spill activity since the process started, shared by the evictor, the spill
/// stripe sources and the status report. Monotonic except `degraded` and
/// `free_bytes`, which describe the present.
#[derive(Debug, Default)]
pub struct SpillCounters {
    /// Evictions that dropped a stripe the live snapshot can serve again.
    pub evicted_clean: AtomicU64,
    /// Evictions that uploaded the stripe first.
    pub evicted_dirty: AtomicU64,
    /// Evictions abandoned before the header op because the guest touched
    /// the stripe or a step failed.
    pub evictions_aborted: AtomicU64,
    /// PUTs started.
    pub puts: AtomicU64,
    /// PUTs that failed.
    pub put_failures: AtomicU64,
    /// GETs started.
    pub gets: AtomicU64,
    /// GETs that failed or decoded to garbage.
    pub get_failures: AtomicU64,
    /// Object bytes uploaded.
    pub put_bytes: AtomicU64,
    /// Object bytes downloaded.
    pub get_bytes: AtomicU64,
    /// Successful hole punches after an eviction.
    pub punches: AtomicU64,
    /// Failed hole punches, at eviction or at startup.
    pub punch_failures: AtomicU64,
    /// Runs of EVICTED stripes punched by the startup pass.
    pub startup_punches: AtomicU64,
    /// Gate transitions open -> hold or open -> fail.
    pub stalls: AtomicU64,
    /// Set while the store is refusing PUTs; dirty evictions pause.
    pub degraded: AtomicBool,
    /// Anomalies logged: FETCHED|EVICTED on disk, unknown fetch state, lost
    /// completion on drain, Uncertain header outcome, ...
    pub degraded_reasons: AtomicU64,
    /// Evicted clean stripes whose re-pull was refused (snapshot ended or PUSHED).
    pub clean_unrecoverable: AtomicU64,
    /// Time spent compressing and encrypting objects.
    pub encode_ns: AtomicU64,
    /// Time spent decrypting and decompressing objects.
    pub decode_ns: AtomicU64,
    /// Last statfs of the filesystem holding data_path (bytes available).
    pub free_bytes: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct SharedMetadataState {
    stripe_fetch_states: Arc<Vec<AtomicU8>>,
    stripe_write_states: Arc<Vec<AtomicU8>>,
    stripe_flags: Arc<Vec<AtomicU8>>,
    stripe_inflight: Arc<Vec<AtomicU16>>,
    stripe_sector_count_shift: u8,
    fetched_stripes_count: Arc<AtomicU64>,
    source_stripes_count: Arc<AtomicU64>,
    /// Stripes whose data occupies local blocks: Fetched, or NoSource && Written.
    resident_stripes_count: Arc<AtomicU64>,
    /// state == Evicted.
    evicted_stripes_count: Arc<AtomicU64>,
    /// state == Evicted && IN_S3.
    in_s3_stripes_count: Arc<AtomicU64>,
    /// Starts false; set by the snapshot subscriber once it is subscribed and
    /// cleared for good when that subscription ends.
    source_live: Arc<AtomicBool>,
    /// Starts GATE_OPEN; moved by the evictor under space pressure.
    write_gate: Arc<AtomicU8>,
    spill: Arc<SpillCounters>,
}

impl SharedMetadataState {
    pub fn new(metadata: &UbiMetadata) -> Self {
        let spill = SpillCounters::default();
        let (mut stripe_fetch_states, mut stripe_write_states, mut stripe_flags) =
            (Vec::new(), Vec::new(), Vec::new());
        let (mut fetched, mut source, mut resident, mut evicted, mut in_s3) = (0, 0, 0, 0, 0);
        for (stripe_id, header) in metadata.stripe_headers.iter().enumerate() {
            let flags = header & stripe_flags::PERSISTED_MASK;
            let write_state = if header & metadata_flags::WRITTEN != 0 {
                Written
            } else {
                NotWritten
            };
            let fetch_state =
                if header & metadata_flags::EVICTED != 0 && header & metadata_flags::FETCHED == 0 {
                    Evicted
                } else if header & metadata_flags::HAS_SOURCE == 0 {
                    NoSource
                } else if header & metadata_flags::FETCHED != 0 {
                    Fetched
                } else {
                    NotFetched
                };
            // No writer sets both. If both are read, the punch never happened
            // (it follows the header flush, which would have cleared FETCHED),
            // so the data is still local and FETCHED wins.
            if header & metadata_flags::EVICTED != 0 && header & metadata_flags::FETCHED != 0 {
                error!(
                    "Stripe {stripe_id} header has both FETCHED and EVICTED set; treating it as resident"
                );
                spill.degraded_reasons.fetch_add(1, Ordering::Relaxed);
            }

            if header & metadata_flags::HAS_SOURCE != 0 {
                source += 1;
            }
            if fetch_state == Fetched {
                fetched += 1;
            }
            if fetch_state == Evicted {
                evicted += 1;
                if header & metadata_flags::IN_S3 != 0 {
                    in_s3 += 1;
                }
            }
            if fetch_state == Fetched || (fetch_state == NoSource && write_state == Written) {
                resident += 1;
            }

            stripe_fetch_states.push(AtomicU8::new(fetch_state));
            stripe_write_states.push(AtomicU8::new(write_state));
            stripe_flags.push(AtomicU8::new(flags));
        }
        let stripe_inflight = (0..metadata.stripe_headers.len())
            .map(|_| AtomicU16::new(0))
            .collect();

        Self {
            stripe_fetch_states: Arc::new(stripe_fetch_states),
            stripe_write_states: Arc::new(stripe_write_states),
            stripe_flags: Arc::new(stripe_flags),
            stripe_inflight: Arc::new(stripe_inflight),
            stripe_sector_count_shift: metadata.stripe_sector_count_shift,
            fetched_stripes_count: Arc::new(AtomicU64::new(fetched)),
            source_stripes_count: Arc::new(AtomicU64::new(source)),
            resident_stripes_count: Arc::new(AtomicU64::new(resident)),
            evicted_stripes_count: Arc::new(AtomicU64::new(evicted)),
            in_s3_stripes_count: Arc::new(AtomicU64::new(in_s3)),
            source_live: Arc::new(AtomicBool::new(false)),
            write_gate: Arc::new(AtomicU8::new(GATE_OPEN)),
            spill: Arc::new(spill),
        }
    }

    pub fn stripe_sector_count(&self) -> u64 {
        1u64 << self.stripe_sector_count_shift
    }

    pub fn stripe_count(&self) -> usize {
        self.stripe_fetch_states.len()
    }

    pub fn sector_to_stripe_id(&self, sector: u64) -> usize {
        (sector >> self.stripe_sector_count_shift) as usize
    }

    pub fn stripe_fetched_if_needed(&self, stripe_id: usize) -> bool {
        let state = self.stripe_fetch_states[stripe_id].load(Ordering::Acquire);
        state == Fetched || state == NoSource
    }

    #[cfg(test)]
    pub fn stripe_fetched(&self, stripe_id: usize) -> bool {
        self.stripe_fetch_states[stripe_id].load(Ordering::Acquire) == Fetched
    }

    pub fn stripe_written(&self, stripe_id: usize) -> bool {
        self.stripe_write_states[stripe_id].load(Ordering::Acquire) == Written
    }

    /// SeqCst: pairs with `pin_inflight` on the channel and `try_begin_evicting`
    /// on the evictor, so a request is never handed to base after the evictor
    /// has seen its stripe idle.
    pub fn stripe_fetch_state(&self, stripe_id: usize) -> u8 {
        self.stripe_fetch_states[stripe_id].load(Ordering::SeqCst)
    }

    /// Poke the raw state byte. Tests use it to put a stripe into a state the
    /// production paths only reach through the evictor, or into no valid state
    /// at all. Counters are not adjusted.
    #[cfg(test)]
    pub fn set_stripe_fetch_state_for_test(&self, stripe_id: usize, state: u8) {
        self.stripe_fetch_states[stripe_id].store(state, Ordering::SeqCst);
    }

    // ---- side bits

    /// All `stripe_flags` bits of a stripe (Acquire).
    pub fn stripe_flags(&self, stripe_id: usize) -> u8 {
        self.stripe_flags[stripe_id].load(Ordering::Acquire)
    }

    /// OR `bits` into the stripe's side bits (fetch_or AcqRel).
    pub fn set_stripe_flags(&self, stripe_id: usize, bits: u8) {
        self.stripe_flags[stripe_id].fetch_or(bits, Ordering::AcqRel);
    }

    /// Clear `bits` from the stripe's side bits (fetch_and AcqRel).
    pub fn clear_stripe_flags(&self, stripe_id: usize, bits: u8) {
        self.stripe_flags[stripe_id].fetch_and(!bits, Ordering::AcqRel);
    }

    /// HAS_SOURCE: the snapshot holds this stripe.
    pub fn stripe_has_source(&self, stripe_id: usize) -> bool {
        self.stripe_flags(stripe_id) & stripe_flags::HAS_SOURCE != 0
    }

    /// IN_S3: the spill store holds an object for this stripe.
    pub fn stripe_in_s3(&self, stripe_id: usize) -> bool {
        self.stripe_flags(stripe_id) & stripe_flags::IN_S3 != 0
    }

    /// PUSHED: a snapshot push for this stripe was received.
    pub fn stripe_pushed(&self, stripe_id: usize) -> bool {
        self.stripe_flags(stripe_id) & stripe_flags::PUSHED != 0
    }

    /// FETCHED_LIVE: the stripe became resident while `source_live` was true.
    pub fn stripe_fetched_live(&self, stripe_id: usize) -> bool {
        self.stripe_flags(stripe_id) & stripe_flags::FETCHED_LIVE != 0
    }

    /// Fetched, or NoSource with WRITTEN: the stripe occupies local blocks.
    pub fn stripe_resident(&self, stripe_id: usize) -> bool {
        match self.stripe_fetch_state(stripe_id) {
            Fetched => true,
            NoSource => self.stripe_written(stripe_id),
            _ => false,
        }
    }

    // ---- CLOCK

    /// Mark every stripe in `first..=last` as recently used.
    pub fn touch(&self, first: usize, last: usize) {
        for stripe_id in first..=last {
            self.stripe_flags[stripe_id].fetch_or(stripe_flags::REFERENCED, Ordering::Relaxed);
        }
    }

    /// Hand: clear REFERENCED and report whether it was set.
    pub fn take_reference(&self, stripe_id: usize) -> bool {
        self.stripe_flags[stripe_id].fetch_and(!stripe_flags::REFERENCED, Ordering::AcqRel)
            & stripe_flags::REFERENCED
            != 0
    }

    // ---- in-flight (channel side; SeqCst so the evictor's one look at the
    // counter after its CAS is enough)

    /// Count a request that is about to pass to base on every stripe in
    /// `first..=last`. Call before the state check, so an evictor that sees
    /// the counter at zero after its CAS knows nothing can still land.
    pub fn pin_inflight(&self, first: usize, last: usize) {
        for stripe_id in first..=last {
            let prev = self.stripe_inflight[stripe_id].fetch_add(1, Ordering::SeqCst);
            debug_assert!(
                prev < u16::MAX,
                "stripe {stripe_id} in-flight counter overflow"
            );
        }
    }

    /// Undo `pin_inflight` once the request completed or was turned away.
    pub fn unpin_inflight(&self, first: usize, last: usize) {
        for stripe_id in first..=last {
            let prev = self.stripe_inflight[stripe_id].fetch_sub(1, Ordering::SeqCst);
            debug_assert!(prev > 0, "stripe {stripe_id} in-flight counter underflow");
        }
    }

    /// Requests pinned on the stripe right now (SeqCst).
    pub fn stripe_inflight(&self, stripe_id: usize) -> u16 {
        self.stripe_inflight[stripe_id].load(Ordering::SeqCst)
    }

    // ---- landing (coordinator only)

    /// The state a landed stripe rests in: Fetched if the source holds it,
    /// NoSource otherwise.
    fn landed_state(&self, stripe_id: usize) -> u8 {
        if self.stripe_has_source(stripe_id) {
            Fetched
        } else {
            NoSource
        }
    }

    /// A stripe that became resident while the snapshot is live may be evicted
    /// clean later; one that landed after the snapshot ended may not.
    fn record_landed_live(&self, stripe_id: usize) {
        if self.source_live() {
            self.set_stripe_flags(stripe_id, stripe_flags::FETCHED_LIVE);
        }
    }

    /// Record that a stripe's data is on this device, before the flusher has
    /// persisted the metadata saying so.
    ///
    /// A guest waiting for a stripe is unblocked by this state, and the flusher
    /// works through a queue that the background sweep fills, so waiting for
    /// it means waiting behind the sweep's backlog, which is as long as the
    /// device. Persisting late is safe: metadata that says a stripe is missing
    /// only costs fetching it again.
    ///
    /// NotFetched | Failed -> Fetched (HAS_SOURCE) or NoSource (!HAS_SOURCE).
    /// Never overwrites Evicting or Evicted: a late SetFetched completion for
    /// a stripe the evictor has since claimed must not release guest I/O.
    ///
    /// A Failed stripe carrying WAS_EVICTED is refused too, with an error and
    /// a degraded reason: its header still says EVICTED, so the coordinator
    /// has to land it the way it lands an Evicted stripe, through the header
    /// op clearing EVICTED and `mark_stripe_resident`.
    pub fn mark_stripe_fetched(&self, stripe_id: usize) {
        if self.stripe_flags(stripe_id) & stripe_flags::WAS_EVICTED != 0 {
            error!(
                "Stripe {stripe_id} marked fetched while its header still says EVICTED; \
                 it must be re-materialised through mark_stripe_resident"
            );
            self.spill.degraded_reasons.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let target = self.landed_state(stripe_id);
        let mut current = self.stripe_fetch_state(stripe_id);
        loop {
            if current != NotFetched && current != Failed {
                return;
            }
            match self.stripe_fetch_states[stripe_id].compare_exchange(
                current,
                target,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.record_landed_live(stripe_id);
        if target == Fetched {
            self.fetched_stripes_count.fetch_add(1, Ordering::AcqRel);
        }
        self.resident_stripes_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Evicted -> Fetched (HAS_SOURCE) or NoSource (!HAS_SOURCE). Called by the
    /// coordinator only after the header clearing EVICTED is durable: the
    /// startup pass punches every stripe whose header says EVICTED, so the
    /// guest may not see the stripe as resident until the disk no longer says
    /// so. The IN_S3 flag is left set as a purge hint.
    ///
    /// Also Failed -> resident for a stripe carrying WAS_EVICTED (an evicted
    /// stripe whose fetch failed once and then landed). `set_stripe_failed`
    /// already took it out of the evicted counts, so only the bit is cleared.
    pub fn mark_stripe_resident(&self, stripe_id: usize) {
        let target = self.landed_state(stripe_id);
        let was_evicted = self.stripe_flags(stripe_id) & stripe_flags::WAS_EVICTED != 0;
        let from = if was_evicted { Failed } else { Evicted };
        if let Err(actual) = self.stripe_fetch_states[stripe_id].compare_exchange(
            from,
            target,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            error!("Stripe {stripe_id} re-materialised while in state {actual}, not {from}");
            self.spill.degraded_reasons.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.record_landed_live(stripe_id);
        if was_evicted {
            self.clear_stripe_flags(stripe_id, stripe_flags::WAS_EVICTED);
        } else {
            self.evicted_stripes_count.fetch_sub(1, Ordering::AcqRel);
            if self.stripe_in_s3(stripe_id) {
                self.in_s3_stripes_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
        if target == Fetched {
            self.fetched_stripes_count.fetch_add(1, Ordering::AcqRel);
        }
        self.resident_stripes_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Record that a stripe has been written, before the flusher has persisted
    /// it. What this state says is what a fork is told the source holds, and a
    /// fork told a stripe holds nothing reads zeros there for good, so it has
    /// to be true the moment the write completes, not once the metadata has
    /// been written down.
    ///
    /// A first write into a NoSource stripe allocates its local blocks, so it
    /// becomes resident here.
    ///
    /// The fetch state is read before the write state is swapped. The evictor
    /// may only claim a NoSource stripe once it is Written, so a read taken
    /// before the swap cannot see Evicting; a read taken after it could, and
    /// the stripe would then be evicted (resident -= 1) without ever having
    /// been counted.
    pub fn mark_stripe_written(&self, stripe_id: usize) {
        let was_nosource = self.stripe_fetch_state(stripe_id) == NoSource;
        let previous = self.stripe_write_states[stripe_id].swap(Written, Ordering::AcqRel);
        if previous == NotWritten && was_nosource {
            self.resident_stripes_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Flusher completion. FETCHED -> `mark_stripe_fetched` (CAS semantics);
    /// WRITTEN -> store Written; HAS_SOURCE | IN_S3 | PUSHED -> OR into flags;
    /// EVICTED -> nothing (the evictor drives Evicting -> Evicted itself).
    pub fn set_stripe_header(&self, stripe_id: usize, header: u8) {
        if header & metadata_flags::FETCHED != 0 {
            self.mark_stripe_fetched(stripe_id);
        }
        if header & metadata_flags::WRITTEN != 0 {
            self.stripe_write_states[stripe_id].store(Written, Ordering::Release)
        }
        let persisted = header & stripe_flags::PERSISTED_MASK;
        if persisted != 0 {
            self.set_stripe_flags(stripe_id, persisted);
        }
    }

    /// A fetch failed for good: the stripe becomes Failed and guest I/O to it
    /// gets an error until it lands. From Evicted the stripe leaves the
    /// evicted (and in_s3) counts and keeps WAS_EVICTED, because its header
    /// still says EVICTED and a restart would punch it: it may become resident
    /// again only through `mark_stripe_resident` (I4).
    pub fn set_stripe_failed(&self, stripe_id: usize) {
        let previous = self.stripe_fetch_states[stripe_id].swap(Failed, Ordering::SeqCst);
        if previous == Evicted {
            self.set_stripe_flags(stripe_id, stripe_flags::WAS_EVICTED);
            self.evicted_stripes_count.fetch_sub(1, Ordering::AcqRel);
            if self.stripe_in_s3(stripe_id) {
                self.in_s3_stripes_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    #[cfg(test)]
    pub fn is_stripe_failed(&self, stripe_id: usize) -> bool {
        self.stripe_fetch_states[stripe_id].load(Ordering::Acquire) == Failed
    }

    // ---- eviction (coordinator only; all SeqCst CAS)

    /// Fetched -> Evicting, or (NoSource && Written) -> Evicting. Returns the
    /// previous state so an abort can restore it, or None.
    pub fn try_begin_evicting(&self, stripe_id: usize) -> Option<u8> {
        let states = &self.stripe_fetch_states[stripe_id];
        if states
            .compare_exchange(Fetched, Evicting, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Some(Fetched);
        }
        // A write state only ever moves NotWritten -> Written, so checking it
        // before the CAS cannot go stale.
        if self.stripe_written(stripe_id)
            && states
                .compare_exchange(NoSource, Evicting, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Some(NoSource);
        }
        None
    }

    /// Evicting -> previous. Logs and counts a degraded reason if not Evicting.
    pub fn abort_evicting(&self, stripe_id: usize, previous: u8) {
        if let Err(actual) = self.stripe_fetch_states[stripe_id].compare_exchange(
            Evicting,
            previous,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            error!("Stripe {stripe_id} eviction aborted while in state {actual}, not Evicting");
            self.spill.degraded_reasons.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Evicting -> Evicted after the punch. Sets IN_S3 if `in_s3`, before the
    /// state changes so nobody sees an Evicted stripe without knowing where
    /// its data went. FETCHED_LIVE describes how the stripe became resident,
    /// so it is cleared with the residency.
    pub fn finish_evicting(&self, stripe_id: usize, previous: u8, in_s3: bool) {
        if in_s3 {
            self.set_stripe_flags(stripe_id, stripe_flags::IN_S3);
        }
        self.clear_stripe_flags(stripe_id, stripe_flags::FETCHED_LIVE);
        if let Err(actual) = self.stripe_fetch_states[stripe_id].compare_exchange(
            Evicting,
            Evicted,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            error!("Stripe {stripe_id} eviction finished while in state {actual}, not Evicting");
            self.spill.degraded_reasons.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if previous == Fetched {
            self.fetched_stripes_count.fetch_sub(1, Ordering::AcqRel);
        }
        self.resident_stripes_count.fetch_sub(1, Ordering::AcqRel);
        self.evicted_stripes_count.fetch_add(1, Ordering::AcqRel);
        if in_s3 {
            self.in_s3_stripes_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    // ---- counters and flags

    pub fn fetched_stripes(&self) -> u64 {
        self.fetched_stripes_count.load(Ordering::Acquire)
    }

    pub fn source_stripes(&self) -> u64 {
        self.source_stripes_count.load(Ordering::Acquire)
    }

    /// Stripes occupying local blocks: Fetched, or NoSource and Written.
    pub fn resident_stripes(&self) -> u64 {
        self.resident_stripes_count.load(Ordering::Acquire)
    }

    /// Stripes in state Evicted.
    pub fn evicted_stripes(&self) -> u64 {
        self.evicted_stripes_count.load(Ordering::Acquire)
    }

    /// Evicted stripes whose data is in the spill store.
    pub fn in_s3_stripes(&self) -> u64 {
        self.in_s3_stripes_count.load(Ordering::Acquire)
    }

    /// Whether the snapshot subscription is up, so a clean stripe could be
    /// pulled again after being dropped.
    pub fn source_live(&self) -> bool {
        self.source_live.load(Ordering::Acquire)
    }

    /// Set by the snapshot subscriber: true once subscribed, false for good
    /// when the subscription ends.
    pub fn set_source_live(&self, live: bool) {
        self.source_live.store(live, Ordering::Release)
    }

    /// GATE_OPEN, GATE_HOLD or GATE_FAIL: what a guest write meets right now.
    pub fn write_gate(&self) -> u8 {
        self.write_gate.load(Ordering::Acquire)
    }

    /// Counts `stalls` on an open -> hold or open -> fail transition.
    pub fn set_write_gate(&self, gate: u8) {
        let previous = self.write_gate.swap(gate, Ordering::AcqRel);
        if previous == GATE_OPEN && gate != GATE_OPEN {
            self.spill.stalls.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The spill counters.
    pub fn spill(&self) -> &SpillCounters {
        &self.spill
    }

    /// The counters as a handle, for a component that outlives its borrow of
    /// this state (a stripe source owns one).
    pub fn spill_counters(&self) -> Arc<SpillCounters> {
        self.spill.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_metadata_state_initialization() {
        let stripe_sector_count_shift = 0;
        let base_stripe_count = 11;
        let image_stripe_count = 5;

        let mut metadata = UbiMetadata::new(
            stripe_sector_count_shift,
            base_stripe_count,
            image_stripe_count,
        );

        metadata.set_stripe_header(0, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(1, metadata_flags::WRITTEN | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(
            2,
            metadata_flags::FETCHED | metadata_flags::WRITTEN | metadata_flags::HAS_SOURCE,
        );
        let state = SharedMetadataState::new(&metadata);

        assert_eq!(state.stripe_fetch_state(0), Fetched);
        assert!(!state.stripe_written(0));

        assert_eq!(state.stripe_fetch_state(1), NotFetched);
        assert!(state.stripe_written(1));

        assert_eq!(state.stripe_fetch_state(2), Fetched);
        assert!(state.stripe_written(2));

        assert_eq!(state.stripe_fetch_state(4), NotFetched);
        assert!(!state.stripe_written(4));

        assert_eq!(state.stripe_fetch_state(6), NoSource);

        assert_eq!(state.fetched_stripes(), 2);

        assert_eq!(state.source_stripes(), 5);
    }

    #[test]
    fn test_state_transitions() {
        let metadata = UbiMetadata::new(0, 5, 5); // All 0 intially
        let state = SharedMetadataState::new(&metadata);

        let stripe_id = 0;

        assert_eq!(state.stripe_fetch_state(stripe_id), NotFetched);
        assert!(!state.stripe_written(stripe_id));
        assert_eq!(state.fetched_stripes(), 0);

        state.set_stripe_header(stripe_id, metadata_flags::FETCHED);
        assert_eq!(state.stripe_fetch_state(stripe_id), Fetched);
        assert_eq!(state.fetched_stripes(), 1);

        state.set_stripe_header(stripe_id, metadata_flags::WRITTEN);
        assert!(state.stripe_written(stripe_id));

        state.set_stripe_header(stripe_id, metadata_flags::FETCHED);
        assert_eq!(state.fetched_stripes(), 1);
    }

    #[test]
    fn test_stripe_fetched_if_needed() {
        let mut metadata = UbiMetadata::new(0, 10, 5);
        metadata.set_stripe_header(0, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        let state = SharedMetadataState::new(&metadata);
        assert!(state.stripe_fetched_if_needed(0));
        assert!(!state.stripe_fetched_if_needed(1));
        assert!(state.stripe_fetched_if_needed(5));
    }

    #[test]
    fn test_failure_state() {
        let metadata = UbiMetadata::new(0, 1, 1);
        let state = SharedMetadataState::new(&metadata);
        let stripe_id = 0;

        state.set_stripe_failed(stripe_id);
        assert_eq!(state.stripe_fetch_state(stripe_id), Failed);
        assert!(state.is_stripe_failed(stripe_id));
        assert!(!state.stripe_fetched(stripe_id));
    }

    /// Headers: 0 evicted with source, 1 evicted in S3 with source, 2 evicted
    /// without source (a written NoSource stripe that was spilled), 3 plain
    /// fetched, 4 no source at all.
    fn evicted_metadata() -> Box<UbiMetadata> {
        let mut metadata = UbiMetadata::new(0, 5, 4);
        metadata.set_stripe_header(0, metadata_flags::EVICTED | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(
            1,
            metadata_flags::EVICTED | metadata_flags::IN_S3 | metadata_flags::HAS_SOURCE,
        );
        metadata.set_stripe_header(
            2,
            metadata_flags::EVICTED | metadata_flags::IN_S3 | metadata_flags::WRITTEN,
        );
        metadata.set_stripe_header(3, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        metadata
    }

    #[test]
    fn new_derives_evicted_over_fetched_and_nosource() {
        let state = SharedMetadataState::new(&evicted_metadata());

        assert_eq!(state.stripe_fetch_state(0), Evicted);
        assert_eq!(state.stripe_fetch_state(1), Evicted);
        assert!(state.stripe_in_s3(1));
        assert_eq!(
            state.stripe_fetch_state(2),
            Evicted,
            "EVICTED beats NoSource"
        );
        assert!(!state.stripe_has_source(2));
        assert!(state.stripe_written(2));
        assert_eq!(state.stripe_fetch_state(3), Fetched);
        assert_eq!(state.stripe_fetch_state(4), NoSource);

        assert_eq!(state.evicted_stripes(), 3);
        assert_eq!(state.in_s3_stripes(), 2);
        assert_eq!(state.fetched_stripes(), 1);
        assert_eq!(state.source_stripes(), 3, "stripe 2 lost HAS_SOURCE");
        assert_eq!(state.resident_stripes(), 1);
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 0);
        assert!(!state.stripe_fetched_if_needed(0));
        assert!(!state.stripe_resident(0));
    }

    #[test]
    fn new_counts_resident_including_written_nosource() {
        let mut metadata = UbiMetadata::new(0, 6, 3);
        metadata.set_stripe_header(0, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(1, metadata_flags::WRITTEN | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(3, metadata_flags::WRITTEN);
        metadata.set_stripe_header(4, metadata_flags::WRITTEN);
        let state = SharedMetadataState::new(&metadata);

        // 0 is fetched; 3 and 4 are written NoSource; 1 is a written but
        // unfetched source stripe (no local blocks yet); 2 and 5 hold nothing.
        assert_eq!(state.resident_stripes(), 3);
        assert!(state.stripe_resident(0));
        assert!(!state.stripe_resident(1));
        assert!(!state.stripe_resident(2));
        assert!(state.stripe_resident(3));
        assert!(!state.stripe_resident(5));
    }

    #[test]
    fn new_logs_fetched_and_evicted_as_resident() {
        let mut metadata = UbiMetadata::new(0, 2, 2);
        metadata.set_stripe_header(
            0,
            metadata_flags::FETCHED | metadata_flags::EVICTED | metadata_flags::HAS_SOURCE,
        );
        let state = SharedMetadataState::new(&metadata);

        assert_eq!(state.stripe_fetch_state(0), Fetched);
        assert_eq!(state.fetched_stripes(), 1);
        assert_eq!(state.resident_stripes(), 1);
        assert_eq!(state.evicted_stripes(), 0);
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn set_stripe_header_does_not_clobber_evicting_or_evicted() {
        let state = SharedMetadataState::new(&evicted_metadata());
        let header = metadata_flags::FETCHED | metadata_flags::HAS_SOURCE;

        state.set_stripe_header(0, header);
        assert_eq!(state.stripe_fetch_state(0), Evicted);

        state.set_stripe_fetch_state_for_test(3, Evicting);
        state.set_stripe_header(3, header);
        assert_eq!(state.stripe_fetch_state(3), Evicting);

        assert_eq!(state.fetched_stripes(), 1);
        assert_eq!(state.resident_stripes(), 1);

        // The persisted side bits still land.
        state.set_stripe_header(0, metadata_flags::PUSHED);
        assert!(state.stripe_pushed(0));
        assert_eq!(state.stripe_fetch_state(0), Evicted);
    }

    #[test]
    fn mark_stripe_fetched_only_from_notfetched_or_failed() {
        let mut metadata = UbiMetadata::new(0, 4, 3);
        metadata.set_stripe_header(2, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        let state = SharedMetadataState::new(&metadata);

        state.mark_stripe_fetched(0);
        assert_eq!(state.stripe_fetch_state(0), Fetched);
        assert_eq!(state.fetched_stripes(), 2);
        assert_eq!(state.resident_stripes(), 2);

        state.set_stripe_failed(1);
        state.mark_stripe_fetched(1);
        assert_eq!(state.stripe_fetch_state(1), Fetched);
        assert_eq!(state.fetched_stripes(), 3);
        assert_eq!(state.resident_stripes(), 3);

        // Already Fetched: nothing moves.
        state.mark_stripe_fetched(2);
        assert_eq!(state.fetched_stripes(), 3);
        assert_eq!(state.resident_stripes(), 3);

        // NoSource stays NoSource; a stripe the source never had cannot be
        // "fetched" from it.
        state.mark_stripe_fetched(3);
        assert_eq!(state.stripe_fetch_state(3), NoSource);
        assert_eq!(state.resident_stripes(), 3);

        for evicting_or_evicted in [Evicting, Evicted] {
            state.set_stripe_fetch_state_for_test(0, evicting_or_evicted);
            state.mark_stripe_fetched(0);
            assert_eq!(state.stripe_fetch_state(0), evicting_or_evicted);
            assert_eq!(state.fetched_stripes(), 3);
        }

        // A stripe without a source that failed lands as NoSource.
        state.set_stripe_fetch_state_for_test(3, Failed);
        state.mark_stripe_fetched(3);
        assert_eq!(state.stripe_fetch_state(3), NoSource);
        assert_eq!(state.resident_stripes(), 4);
    }

    #[test]
    fn mark_stripe_resident_restores_nosource_without_source_and_adjusts_in_s3() {
        let state = SharedMetadataState::new(&evicted_metadata());
        assert_eq!(state.evicted_stripes(), 3);
        assert_eq!(state.in_s3_stripes(), 2);
        assert_eq!(state.fetched_stripes(), 1);
        assert_eq!(state.resident_stripes(), 1);

        // Spilled written NoSource stripe comes back as NoSource.
        state.mark_stripe_resident(2);
        assert_eq!(state.stripe_fetch_state(2), NoSource);
        assert_eq!(state.evicted_stripes(), 2);
        assert_eq!(state.in_s3_stripes(), 1);
        assert_eq!(state.fetched_stripes(), 1);
        assert_eq!(state.resident_stripes(), 2);
        assert!(state.stripe_in_s3(2), "IN_S3 stays as a purge hint");

        // Spilled source stripe comes back as Fetched.
        state.mark_stripe_resident(1);
        assert_eq!(state.stripe_fetch_state(1), Fetched);
        assert_eq!(state.evicted_stripes(), 1);
        assert_eq!(state.in_s3_stripes(), 0);
        assert_eq!(state.fetched_stripes(), 2);
        assert_eq!(state.resident_stripes(), 3);

        // Clean-evicted source stripe: no IN_S3 to adjust.
        state.mark_stripe_resident(0);
        assert_eq!(state.stripe_fetch_state(0), Fetched);
        assert_eq!(state.evicted_stripes(), 0);
        assert_eq!(state.in_s3_stripes(), 0);
        assert_eq!(state.fetched_stripes(), 3);

        // Not Evicted: refused and counted, nothing else moves.
        state.mark_stripe_resident(3);
        assert_eq!(state.stripe_fetch_state(3), Fetched);
        assert_eq!(state.fetched_stripes(), 3);
        assert_eq!(state.resident_stripes(), 4);
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn set_stripe_failed_from_evicted_leaves_the_evicted_counts() {
        let state = SharedMetadataState::new(&evicted_metadata());
        assert_eq!(state.evicted_stripes(), 3);
        assert_eq!(state.in_s3_stripes(), 2);

        // Clean-evicted: only the evicted count moves.
        state.set_stripe_failed(0);
        assert_eq!(state.stripe_fetch_state(0), Failed);
        assert!(state.stripe_flags(0) & stripe_flags::WAS_EVICTED != 0);
        assert_eq!(state.evicted_stripes(), 2);
        assert_eq!(state.in_s3_stripes(), 2);

        // Spilled: both move, and IN_S3 stays for the re-fetch to route by.
        state.set_stripe_failed(1);
        assert_eq!(state.evicted_stripes(), 1);
        assert_eq!(state.in_s3_stripes(), 1);
        assert!(state.stripe_in_s3(1));

        // NotFetched -> Failed carries no bit and moves nothing.
        state.set_stripe_fetch_state_for_test(4, NotFetched);
        state.set_stripe_failed(4);
        assert_eq!(state.stripe_fetch_state(4), Failed);
        assert_eq!(state.stripe_flags(4) & stripe_flags::WAS_EVICTED, 0);
        assert_eq!(state.evicted_stripes(), 1);
        assert_eq!(state.resident_stripes(), 1);
    }

    #[test]
    fn formerly_evicted_stripe_lands_only_through_mark_stripe_resident() {
        let state = SharedMetadataState::new(&evicted_metadata());
        state.set_stripe_failed(1);
        assert_eq!(state.evicted_stripes(), 2);
        assert_eq!(state.in_s3_stripes(), 1);

        // The header still says EVICTED, so the plain landing is refused.
        state.mark_stripe_fetched(1);
        assert_eq!(state.stripe_fetch_state(1), Failed);
        assert_eq!(state.fetched_stripes(), 1);
        assert_eq!(state.resident_stripes(), 1);
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 1);

        // Once the header clearing EVICTED is durable it lands as a Fetched
        // stripe; the evicted counts were adjusted when it failed.
        state.set_source_live(true);
        state.mark_stripe_resident(1);
        assert_eq!(state.stripe_fetch_state(1), Fetched);
        assert_eq!(state.stripe_flags(1) & stripe_flags::WAS_EVICTED, 0);
        assert!(state.stripe_fetched_live(1));
        assert!(state.stripe_in_s3(1), "IN_S3 stays as a purge hint");
        assert_eq!(state.evicted_stripes(), 2);
        assert_eq!(state.in_s3_stripes(), 1);
        assert_eq!(state.fetched_stripes(), 2);
        assert_eq!(state.resident_stripes(), 2);
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 1);

        // With the bit gone, a later failure and landing take the plain path.
        state.set_stripe_fetch_state_for_test(1, Failed);
        state.mark_stripe_fetched(1);
        assert_eq!(state.stripe_fetch_state(1), Fetched);

        // A plain Failed stripe (no bit) is not Evicted material for
        // mark_stripe_resident.
        state.set_stripe_failed(4);
        state.mark_stripe_resident(4);
        assert_eq!(state.stripe_fetch_state(4), Failed);
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 2);
    }

    /// A channel writes a NoSource stripe while the evictor is hunting for a
    /// Written NoSource stripe to claim. Whatever the interleaving, the
    /// stripe is counted resident exactly once before it is evicted, so the
    /// eviction's decrement never underflows.
    #[test]
    fn mark_stripe_written_counts_before_a_concurrent_eviction() {
        for _ in 0..200 {
            let metadata = UbiMetadata::new(0, 1, 0);
            let state = SharedMetadataState::new(&metadata);
            assert_eq!(state.stripe_fetch_state(0), NoSource);
            assert_eq!(state.resident_stripes(), 0);

            let channel = {
                let state = state.clone();
                std::thread::spawn(move || state.mark_stripe_written(0))
            };
            let evictor = {
                let state = state.clone();
                std::thread::spawn(move || loop {
                    if let Some(previous) = state.try_begin_evicting(0) {
                        return previous;
                    }
                    std::hint::spin_loop();
                })
            };
            channel.join().unwrap();
            let previous = evictor.join().unwrap();

            assert_eq!(previous, NoSource);
            assert_eq!(state.resident_stripes(), 1);
            state.finish_evicting(0, previous, true);
            assert_eq!(state.resident_stripes(), 0);
            assert_eq!(state.evicted_stripes(), 1);
        }
    }

    #[test]
    fn mark_stripe_written_makes_a_nosource_stripe_resident_once() {
        let metadata = UbiMetadata::new(0, 3, 1);
        let state = SharedMetadataState::new(&metadata);
        assert_eq!(state.resident_stripes(), 0);

        state.mark_stripe_written(1);
        assert_eq!(state.resident_stripes(), 1);
        state.mark_stripe_written(1);
        assert_eq!(state.resident_stripes(), 1);

        // A source stripe's blocks are allocated by its fetch, not its write.
        state.mark_stripe_written(0);
        assert_eq!(state.resident_stripes(), 1);
        state.mark_stripe_fetched(0);
        assert_eq!(state.resident_stripes(), 2);
    }

    #[test]
    fn try_begin_evicting_claims_fetched_and_written_nosource_only() {
        let mut metadata = UbiMetadata::new(0, 7, 4);
        metadata.set_stripe_header(0, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(
            1,
            metadata_flags::FETCHED | metadata_flags::WRITTEN | metadata_flags::HAS_SOURCE,
        );
        metadata.set_stripe_header(4, metadata_flags::WRITTEN);
        let state = SharedMetadataState::new(&metadata);
        state.set_stripe_failed(3);

        assert_eq!(state.try_begin_evicting(0), Some(Fetched));
        assert_eq!(state.stripe_fetch_state(0), Evicting);
        assert_eq!(state.try_begin_evicting(1), Some(Fetched));
        assert_eq!(state.try_begin_evicting(4), Some(NoSource));
        assert_eq!(state.stripe_fetch_state(4), Evicting);

        assert_eq!(state.try_begin_evicting(2), None, "NotFetched");
        assert_eq!(state.try_begin_evicting(3), None, "Failed");
        assert_eq!(state.try_begin_evicting(5), None, "unwritten NoSource");
        assert_eq!(state.try_begin_evicting(0), None, "already Evicting");
        state.set_stripe_fetch_state_for_test(6, Evicted);
        assert_eq!(state.try_begin_evicting(6), None, "Evicted");

        // Claiming changes no counter; only finishing does.
        assert_eq!(state.fetched_stripes(), 2);
        assert_eq!(state.resident_stripes(), 3);
    }

    #[test]
    fn abort_and_finish_evicting_adjust_counters_by_previous_state() {
        let mut metadata = UbiMetadata::new(0, 4, 2);
        metadata.set_stripe_header(0, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(1, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(2, metadata_flags::WRITTEN);
        let state = SharedMetadataState::new(&metadata);
        state.set_source_live(true);
        state.mark_stripe_written(0);
        assert_eq!(state.fetched_stripes(), 2);
        assert_eq!(state.resident_stripes(), 3);

        // Abort restores the previous state and moves nothing.
        let previous = state.try_begin_evicting(0).unwrap();
        state.abort_evicting(0, previous);
        assert_eq!(state.stripe_fetch_state(0), Fetched);
        assert_eq!(state.fetched_stripes(), 2);
        assert_eq!(state.resident_stripes(), 3);
        assert_eq!(state.evicted_stripes(), 0);

        // Dirty eviction of a fetched stripe.
        state.set_stripe_flags(0, stripe_flags::FETCHED_LIVE);
        let previous = state.try_begin_evicting(0).unwrap();
        state.finish_evicting(0, previous, true);
        assert_eq!(state.stripe_fetch_state(0), Evicted);
        assert!(state.stripe_in_s3(0));
        assert!(!state.stripe_fetched_live(0));
        assert_eq!(state.fetched_stripes(), 1);
        assert_eq!(state.resident_stripes(), 2);
        assert_eq!(state.evicted_stripes(), 1);
        assert_eq!(state.in_s3_stripes(), 1);

        // Clean eviction of a fetched stripe: not in S3.
        let previous = state.try_begin_evicting(1).unwrap();
        state.finish_evicting(1, previous, false);
        assert_eq!(state.stripe_fetch_state(1), Evicted);
        assert!(!state.stripe_in_s3(1));
        assert_eq!(state.fetched_stripes(), 0);
        assert_eq!(state.resident_stripes(), 1);
        assert_eq!(state.evicted_stripes(), 2);
        assert_eq!(state.in_s3_stripes(), 1);

        // Dirty eviction of a written NoSource stripe: fetched is untouched.
        let previous = state.try_begin_evicting(2).unwrap();
        assert_eq!(previous, NoSource);
        state.finish_evicting(2, previous, true);
        assert_eq!(state.stripe_fetch_state(2), Evicted);
        assert_eq!(state.fetched_stripes(), 0);
        assert_eq!(state.resident_stripes(), 0);
        assert_eq!(state.evicted_stripes(), 3);
        assert_eq!(state.in_s3_stripes(), 2);

        // Abort or finish on a stripe that is not Evicting is an anomaly.
        state.abort_evicting(3, NoSource);
        state.finish_evicting(3, NoSource, false);
        assert_eq!(state.stripe_fetch_state(3), NoSource);
        assert_eq!(state.evicted_stripes(), 3);
        assert_eq!(state.spill().degraded_reasons.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn pin_unpin_touch_take_reference() {
        let metadata = UbiMetadata::new(0, 4, 4);
        let state = SharedMetadataState::new(&metadata);

        state.pin_inflight(1, 2);
        state.pin_inflight(2, 2);
        assert_eq!(state.stripe_inflight(0), 0);
        assert_eq!(state.stripe_inflight(1), 1);
        assert_eq!(state.stripe_inflight(2), 2);
        state.unpin_inflight(1, 2);
        assert_eq!(state.stripe_inflight(1), 0);
        assert_eq!(state.stripe_inflight(2), 1);
        state.unpin_inflight(2, 2);
        assert_eq!(state.stripe_inflight(2), 0);

        assert!(!state.take_reference(0));
        state.touch(0, 1);
        assert!(state.take_reference(0));
        assert!(!state.take_reference(0), "the hand cleared it");
        assert!(state.take_reference(1));
        assert!(!state.take_reference(2));

        // The CLOCK bit is in-memory only and never leaks into persisted bits.
        state.touch(3, 3);
        assert_eq!(
            state.stripe_flags(3) & stripe_flags::PERSISTED_MASK,
            stripe_flags::HAS_SOURCE
        );
        state.clear_stripe_flags(3, stripe_flags::REFERENCED);
        assert!(!state.take_reference(3));
    }

    #[test]
    fn fetched_live_only_when_source_live() {
        let metadata = UbiMetadata::new(0, 3, 3);
        let state = SharedMetadataState::new(&metadata);
        assert!(!state.source_live());

        state.mark_stripe_fetched(0);
        assert!(!state.stripe_fetched_live(0));

        state.set_source_live(true);
        assert!(state.source_live());
        state.mark_stripe_fetched(1);
        assert!(state.stripe_fetched_live(1));

        state.set_stripe_fetch_state_for_test(2, Evicted);
        state.mark_stripe_resident(2);
        assert!(state.stripe_fetched_live(2));

        state.set_source_live(false);
        state.set_stripe_fetch_state_for_test(0, Evicted);
        state.mark_stripe_resident(0);
        assert!(!state.stripe_fetched_live(0));

        // A landing that changes nothing (stale completion on a claimed or
        // already resident stripe) does not claim the stripe landed live.
        state.set_source_live(true);
        state.set_stripe_fetch_state_for_test(0, Evicting);
        state.mark_stripe_fetched(0);
        assert!(!state.stripe_fetched_live(0));
        state.set_stripe_fetch_state_for_test(0, Fetched);
        state.mark_stripe_fetched(0);
        state.mark_stripe_resident(0);
        assert!(!state.stripe_fetched_live(0));
    }

    #[test]
    fn set_write_gate_counts_stalls_once_per_transition() {
        let metadata = UbiMetadata::new(0, 1, 1);
        let state = SharedMetadataState::new(&metadata);
        let stalls = || state.spill().stalls.load(Ordering::Relaxed);
        assert_eq!(state.write_gate(), GATE_OPEN);

        state.set_write_gate(GATE_HOLD);
        assert_eq!(state.write_gate(), GATE_HOLD);
        assert_eq!(stalls(), 1);
        state.set_write_gate(GATE_HOLD);
        assert_eq!(stalls(), 1);
        state.set_write_gate(GATE_FAIL);
        assert_eq!(stalls(), 1, "hold -> fail is not a new stall");
        state.set_write_gate(GATE_OPEN);
        assert_eq!(stalls(), 1);
        state.set_write_gate(GATE_FAIL);
        assert_eq!(state.write_gate(), GATE_FAIL);
        assert_eq!(stalls(), 2);
    }
}
