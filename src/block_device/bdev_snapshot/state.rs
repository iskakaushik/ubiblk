use std::{
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

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

/// Where a single channel is in the freeze handshake. A channel walks
/// `Open -> Locked -> Drained -> Open`, and only passes I/O through while it is
/// `Open`.
pub const CHANNEL_OPEN: u8 = 0;
pub const CHANNEL_LOCKED: u8 = 1;
pub const CHANNEL_DRAINED: u8 = 2;

/// One channel's slot in the freeze handshake.
///
/// The channel publishes its in-flight count here, so the freeze can retire a
/// channel that is idle without waiting for it to be polled — an idle queue
/// never runs, so a scheme where each channel has to report itself drained
/// stalls until that queue happens to get work.
#[derive(Debug)]
pub struct ChannelSlot {
    state: AtomicU8,
    in_flight: AtomicU64,
}

impl ChannelSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(CHANNEL_OPEN),
            in_flight: AtomicU64::new(0),
        }
    }

    pub fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    pub fn is_open(&self) -> bool {
        self.state() == CHANNEL_OPEN
    }

    pub fn request_started(&self) {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
    }

    pub fn requests_finished(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.in_flight.fetch_sub(count as u64, Ordering::AcqRel);
        // A channel that is being drained and has just gone quiet retires
        // itself, so the freeze does not have to wait for the next poll.
        if self.in_flight.load(Ordering::Acquire) == 0
            && self.state.load(Ordering::Acquire) == CHANNEL_LOCKED
        {
            self.state.store(CHANNEL_DRAINED, Ordering::Release);
        }
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Acquire)
    }
}

/// State shared between the `SnapshotBlockDevice`, every `SnapshotIoChannel`
/// and (later) the snapshot worker. Cloning shares the underlying atomics.
#[derive(Debug, Clone)]
pub struct SharedSnapshotState {
    stripe_states: Arc<Vec<AtomicU8>>,
    stripe_sector_count_shift: u8,
    /// One slot per channel. Only touched under the lock when channels are
    /// created, which happens at startup; the I/O path only ever touches its
    /// own slot's atomics.
    channels: Arc<Mutex<Vec<Arc<ChannelSlot>>>>,
    /// Bumped on every freeze so a destination can tell snapshots apart. Never
    /// reset: a fork holding generation 3 must not be confused by a later,
    /// different snapshot also calling itself 3.
    generation: Arc<AtomicU64>,
    /// When the current snapshot was frozen, as milliseconds since the unix
    /// epoch, or 0 when none is live. Kept here rather than in the worker so a
    /// freeze does not have to wait for the worker to get to it — the worker can
    /// be inside a slow push to a fork that has gone away.
    frozen_at_ms: Arc<AtomicU64>,
    /// Destinations the worker currently serves. Published here so the RPC can
    /// report it and a subscriber can tell when it has been registered.
    destinations: Arc<AtomicU64>,
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
            channels: Arc::new(Mutex::new(Vec::new())),
            generation: Arc::new(AtomicU64::new(0)),
            destinations: Arc::new(AtomicU64::new(0)),
            frozen_at_ms: Arc::new(AtomicU64::new(0)),
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

    /// True while the live device still holds this stripe's snapshot content,
    /// i.e. before any copy-out has run for it. Once a copy-out starts, prod may
    /// overwrite the stripe, so the live device stops being a valid source for
    /// the snapshot.
    pub fn write_allowed_before_copy(&self, stripe_id: usize) -> bool {
        matches!(self.stripe_state(stripe_id), FREE | LOCKED)
    }

    /// Claim a locked stripe for copy-out. Returns true for the caller that won
    /// the race, so only one copy-out is started per stripe.
    pub fn begin_copy(&self, stripe_id: usize) -> bool {
        self.stripe_states[stripe_id]
            .compare_exchange(LOCKED, COPYING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Put a claimed stripe back: its copy-out is waiting for a destination.
    pub fn defer_copy(&self, stripe_id: usize) {
        self.stripe_states[stripe_id].store(LOCKED, Ordering::Release);
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

    /// End the snapshot: stripes are released and the generation goes back to
    /// zero, so a fork that subscribes afterwards is told there is no snapshot
    /// rather than being served a half-preserved one.
    pub fn end_snapshot(&self) {
        self.release_all();
        // Liveness is the timestamp, not the counter, so generations stay
        // monotonic across snapshots that come and go.
        self.frozen_at_ms.store(0, Ordering::Release);
    }

    /// Whether a snapshot is currently being held.
    pub fn snapshot_live(&self) -> bool {
        self.frozen_at_ms.load(Ordering::Acquire) != 0
    }

    /// Mark every stripe as needed by a snapshot and return the new generation.
    pub fn lock_all(&self) -> u64 {
        for state in self.stripe_states.iter() {
            state.store(LOCKED, Ordering::Release);
        }
        self.frozen_at_ms.store(now_ms(), Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// How long ago the current snapshot was frozen, if there is one.
    pub fn since_frozen(&self) -> Option<Duration> {
        let frozen_at = self.frozen_at_ms.load(Ordering::Acquire);
        if frozen_at == 0 {
            return None;
        }
        Some(Duration::from_millis(now_ms().saturating_sub(frozen_at)))
    }

    pub fn set_destination_count(&self, count: usize) {
        self.destinations.store(count as u64, Ordering::Release);
    }

    pub fn destination_count(&self) -> u64 {
        self.destinations.load(Ordering::Acquire)
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// (free, locked, copying, copied) — what `snapshot_status` reports.
    pub fn counts(&self) -> (usize, usize, usize, usize) {
        let (mut free, mut locked, mut copying, mut copied) = (0, 0, 0, 0);
        for state in self.stripe_states.iter() {
            match state.load(Ordering::Acquire) {
                LOCKED => locked += 1,
                COPYING => copying += 1,
                COPIED => copied += 1,
                _ => free += 1,
            }
        }
        (free, locked, copying, copied)
    }

    /// Give a new channel its slot in the handshake.
    pub fn register_channel(&self) -> Arc<ChannelSlot> {
        let slot = Arc::new(ChannelSlot::new());
        self.channels.lock().unwrap().push(slot.clone());
        slot
    }

    pub fn channel_count(&self) -> usize {
        self.channels.lock().unwrap().len()
    }

    /// Lock every channel: from here on new I/O queues instead of reaching the
    /// device below, so in-flight counts can only fall.
    pub fn begin_drain(&self) {
        for slot in self.channels.lock().unwrap().iter() {
            slot.state.store(CHANNEL_LOCKED, Ordering::Release);
        }
    }

    /// True once every channel is quiet. Retires the idle ones itself, so a
    /// queue that is not being polled does not hold the freeze up.
    pub fn drained(&self) -> bool {
        let channels = self.channels.lock().unwrap();
        let mut all_drained = true;
        for slot in channels.iter() {
            if slot.in_flight() == 0 {
                slot.state.store(CHANNEL_DRAINED, Ordering::Release);
                continue;
            }
            all_drained = false;
        }
        all_drained
    }

    /// Open every channel again and let them replay what they queued.
    pub fn resume(&self) {
        for slot in self.channels.lock().unwrap().iter() {
            slot.state.store(CHANNEL_OPEN, Ordering::Release);
        }
    }

    /// (open, locked, drained) — what `snapshot_status` reports.
    pub fn channel_states(&self) -> (usize, usize, usize) {
        let channels = self.channels.lock().unwrap();
        let (mut open, mut locked, mut drained) = (0, 0, 0);
        for slot in channels.iter() {
            match slot.state() {
                CHANNEL_LOCKED => locked += 1,
                CHANNEL_DRAINED => drained += 1,
                _ => open += 1,
            }
        }
        (open, locked, drained)
    }
}
