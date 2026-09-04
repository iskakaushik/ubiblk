//! The eviction state machine: which stripe goes, and the order in which its
//! data is read, uploaded, recorded and punched.
//!
//! This file holds the frozen public surface. The bodies are a stub that
//! evicts nothing, routes every request to the ingest and never reports itself
//! busy; the state machine lands with the coordinator work.

use std::path::PathBuf;

use crate::{
    archive::ArchiveStore,
    block_device::{IoChannel, PushPermit, SharedMetadataState, UbiMetadata},
    config::v2::spill::OnFull,
    Result,
};

use super::{
    super::metadata_flusher::{MetadataFlusher, PersistOutcome},
    codec::SpillCodec,
    punch::HolePuncher,
};

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
    /// itself under GATE_FAIL. Nothing to route.
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

/// Drives stripes out of the local device when it is over its ceiling.
pub struct Evictor {
    // Held for the state machine that lands with the coordinator work.
    #[allow(dead_code)]
    cfg: EvictorConfig,
    #[allow(dead_code)]
    read_channel: Box<dyn IoChannel>,
    #[allow(dead_code)]
    store: Option<Box<dyn ArchiveStore>>,
    #[allow(dead_code)]
    codec: SpillCodec,
    #[allow(dead_code)]
    puncher: Box<dyn HolePuncher>,
    #[allow(dead_code)]
    state: SharedMetadataState,
}

impl Evictor {
    /// `store` is the PUT store (None: clean-only). `read_channel` comes from
    /// `target_dev.create_channel()` so reads decrypt through crypt.
    pub fn new(
        cfg: EvictorConfig,
        read_channel: Box<dyn IoChannel>,
        store: Option<Box<dyn ArchiveStore>>,
        codec: SpillCodec,
        puncher: Box<dyn HolePuncher>,
        state: SharedMetadataState,
    ) -> Result<Self> {
        Ok(Evictor {
            cfg,
            read_channel,
            store,
            codec,
            puncher,
            state,
        })
    }

    /// Startup pass: punch every stripe with EVICTED set, coalescing runs of
    /// consecutive stripes into one call. Idempotent. Counts startup_punches.
    pub fn punch_all_evicted(&mut self, metadata: &UbiMetadata) -> Result<usize> {
        let _ = metadata;
        Ok(0)
    }

    /// One tick: apply flusher outcomes for odd tokens, poll read and PUT
    /// completions, advance stages, punch, refresh statfs, set the gate, start
    /// new evictions while over the ceiling.
    pub fn update(&mut self, flusher: &mut MetadataFlusher, outcomes: &[PersistOutcome]) {
        let _ = (flusher, outcomes);
    }

    /// A guest `Fetch { S }` reached the coordinator: abort or defer an
    /// eviction of S, hold or refuse it under a closed gate, else forward.
    pub fn on_fetch_request(&mut self, stripe_id: usize) -> FetchDisposition {
        let _ = stripe_id;
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
        let _ = (stripe_id, data);
        (PushDisposition::Forward, Some(permit))
    }

    /// Fetches and pushes released this tick, for the coordinator to route.
    pub fn take_released(&mut self) -> (Vec<usize>, Vec<ReleasedPush>) {
        (Vec::new(), Vec::new())
    }

    /// True while an eviction is in progress or the ceiling is exceeded.
    pub fn busy(&self) -> bool {
        false
    }

    /// Odd tokens belong to the evictor; even tokens to the coordinator.
    pub fn owns_token(token: u64) -> bool {
        token & 1 == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        archive::{ArchiveCompressionAlgorithm, TestObjectStore},
        block_device::{bdev_test::TestBlockDevice, spill::RecordingPuncher, BlockDevice},
    };

    fn config() -> EvictorConfig {
        EvictorConfig {
            data_path: "/tmp/device.raw".into(),
            device_id: "fork-1".to_string(),
            stripe_sector_count: 8,
            target_sector_count: 64,
            max_local_bytes: 1 << 20,
            low_water_bytes: 4096,
            hard_margin_bytes: 4096,
            min_free_bytes: 4096,
            clean_eviction: false,
            on_full: OnFull::Stall,
            max_concurrent_evictions: 2,
            sweep_batch: 4096,
            alignment: 4096,
        }
    }

    fn evictor() -> (Evictor, SharedMetadataState) {
        let target = TestBlockDevice::new(64 * 512);
        let metadata = UbiMetadata::new(3, 8, 8);
        let state = SharedMetadataState::new(&metadata);
        let evictor = Evictor::new(
            config(),
            target.create_channel().unwrap(),
            Some(Box::new(TestObjectStore::new())),
            SpillCodec::new(ArchiveCompressionAlgorithm::None, None, 8),
            Box::new(RecordingPuncher::default()),
            state.clone(),
        )
        .unwrap();
        (evictor, state)
    }

    #[test]
    fn stub_forwards_everything_and_is_never_busy() {
        let (mut evictor, _state) = evictor();
        assert!(!evictor.busy());
        assert_eq!(evictor.on_fetch_request(3), FetchDisposition::Forward);

        let (disposition, permit) =
            evictor.on_pushed_stripe(3, &[0u8; 512], PushPermit::unbounded());
        assert_eq!(disposition, PushDisposition::Forward);
        assert!(permit.is_some(), "a forwarded push keeps its permit");

        let (fetches, pushes) = evictor.take_released();
        assert!(fetches.is_empty() && pushes.is_empty());
    }

    #[test]
    fn stub_punches_nothing_at_startup() {
        let (mut evictor, _state) = evictor();
        let mut metadata = UbiMetadata::new(3, 8, 8);
        metadata.set_stripe_header(2, crate::block_device::metadata_flags::EVICTED);
        assert_eq!(evictor.punch_all_evicted(&metadata).unwrap(), 0);
    }

    #[test]
    fn stub_update_is_a_no_op() {
        let (mut evictor, state) = evictor();
        let metadata_dev = TestBlockDevice::new(8 * 1024);
        UbiMetadata::new(3, 8, 8)
            .save_to_bdev(&metadata_dev)
            .unwrap();
        let mut flusher = MetadataFlusher::new(&metadata_dev, 64, state.clone()).unwrap();
        evictor.update(&mut flusher, &[]);
        assert!(!flusher.busy());
        assert_eq!(state.evicted_stripes(), 0);
    }

    #[test]
    fn odd_tokens_belong_to_the_evictor() {
        assert!(Evictor::owns_token(1));
        assert!(Evictor::owns_token(7));
        assert!(!Evictor::owns_token(2));
        assert!(!Evictor::owns_token(0));
    }
}
