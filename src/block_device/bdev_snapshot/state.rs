use std::sync::{
    atomic::{AtomicU64, AtomicU8, Ordering},
    Arc,
};

/// Per-stripe snapshot state.
///
/// A stripe is `Free` until a snapshot is taken. Taking a snapshot marks every
/// stripe `Locked`: the snapshot needs the pre-write content of that stripe, so
/// a write has to hand the old content to the snapshot destinations before it
/// may proceed. `Copying` marks the window where that hand-off is in flight and
/// writes queue. Once every live destination has the content the stripe becomes
/// `Copied` and writes pass through again.
///
/// Forking is depth 1, so one state per stripe is enough: there is never more
/// than one snapshot generation that could still need a given stripe.
pub const FREE: u8 = 0;
pub const LOCKED: u8 = 1;
pub const COPYING: u8 = 2;
pub const COPIED: u8 = 3;

/// Whether the layer is passing I/O through or holding it while a freeze
/// drains the in-flight requests.
pub const RUNNING: u8 = 0;
pub const DRAINING: u8 = 1;

/// State shared between the `SnapshotBlockDevice`, every `SnapshotIoChannel`
/// and (later) the snapshot worker. Cloning shares the underlying atomics.
#[derive(Debug, Clone)]
pub struct SharedSnapshotState {
    stripe_states: Arc<Vec<AtomicU8>>,
    stripe_sector_count_shift: u8,
    mode: Arc<AtomicU8>,
    /// Number of channels that have reported themselves drained. The freeze
    /// waits for this to reach the channel count.
    drained_channels: Arc<AtomicU64>,
    /// Number of channels created so far, so the freeze knows what to wait for.
    channel_count: Arc<AtomicU64>,
    /// Bumped on every freeze so a destination can tell snapshots apart.
    generation: Arc<AtomicU64>,
}

impl SharedSnapshotState {
    pub fn new(stripe_count: usize, stripe_sector_count_shift: u8) -> Self {
        let mut stripe_states = Vec::with_capacity(stripe_count);
        for _ in 0..stripe_count {
            stripe_states.push(AtomicU8::new(FREE));
        }

        Self {
            stripe_states: Arc::new(stripe_states),
            stripe_sector_count_shift,
            mode: Arc::new(AtomicU8::new(RUNNING)),
            drained_channels: Arc::new(AtomicU64::new(0)),
            channel_count: Arc::new(AtomicU64::new(0)),
            generation: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn stripe_count(&self) -> usize {
        self.stripe_states.len()
    }

    pub fn sector_to_stripe_id(&self, sector: u64) -> usize {
        (sector >> self.stripe_sector_count_shift) as usize
    }

    pub fn stripe_sector_count(&self) -> u64 {
        1u64 << self.stripe_sector_count_shift
    }

    pub fn stripe_state(&self, stripe_id: usize) -> u8 {
        self.stripe_states[stripe_id].load(Ordering::Acquire)
    }

    /// Writes may proceed to a stripe that no snapshot needs, and to one whose
    /// pre-write content every destination already has.
    pub fn write_allowed(&self, stripe_id: usize) -> bool {
        matches!(self.stripe_state(stripe_id), FREE | COPIED)
    }

    /// Claim a locked stripe for copy-out. Returns true for the caller that won
    /// the race, so only one copy-out is started per stripe.
    pub fn begin_copy(&self, stripe_id: usize) -> bool {
        self.stripe_states[stripe_id]
            .compare_exchange(LOCKED, COPYING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn finish_copy(&self, stripe_id: usize) {
        self.stripe_states[stripe_id].store(COPIED, Ordering::Release);
    }

    /// Release every stripe, ending the snapshot. Used when the last
    /// destination goes away.
    pub fn release_all(&self) {
        for state in self.stripe_states.iter() {
            state.store(FREE, Ordering::Release);
        }
    }

    /// Mark every stripe as needed by a snapshot and return the new generation.
    pub fn lock_all(&self) -> u64 {
        for state in self.stripe_states.iter() {
            state.store(LOCKED, Ordering::Release);
        }
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn mode(&self) -> u8 {
        self.mode.load(Ordering::Acquire)
    }

    pub fn set_mode(&self, mode: u8) {
        self.mode.store(mode, Ordering::Release);
    }

    pub fn register_channel(&self) {
        self.channel_count.fetch_add(1, Ordering::AcqRel);
    }

    pub fn channel_count(&self) -> u64 {
        self.channel_count.load(Ordering::Acquire)
    }

    pub fn begin_drain(&self) {
        self.drained_channels.store(0, Ordering::Release);
        self.set_mode(DRAINING);
    }

    pub fn report_drained(&self) {
        self.drained_channels.fetch_add(1, Ordering::AcqRel);
    }

    pub fn all_channels_drained(&self) -> bool {
        self.drained_channels.load(Ordering::Acquire) >= self.channel_count()
    }
}
