//! Reading spilled stripes back, and routing a fetch to the store or the base
//! source by what the metadata says about the stripe.
//!
//! This file holds the frozen types and signatures. The bodies are shells: the
//! spill source refuses every request and the composite passes everything
//! through to base. Routing by IN_S3 and `source_live` lands with the spill
//! stripe source work.

use std::{collections::HashMap, sync::atomic::Ordering, sync::Arc};

use log::error;

use super::StripeSource;
use crate::{
    archive::ArchiveStore,
    block_device::{spill::SpillCodec, SharedBuffer, SharedMetadataState, SpillCounters},
    Result,
};

/// Stripes from the spill store. One per fetcher, each with its own GET store
/// so a demand read never queues behind the evictor's uploads.
pub struct SpillStripeSource {
    // The GET path that uses these lands with the spill stripe source work.
    #[allow(dead_code)]
    store: Box<dyn ArchiveStore>,
    #[allow(dead_code)]
    codec: SpillCodec,
    #[allow(dead_code)]
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
}

impl StripeSource for SpillStripeSource {
    fn request(&mut self, stripe_id: usize, _buffer: SharedBuffer) -> Result<()> {
        // Shell: refuse rather than hang, and count it, so a misrouted request
        // shows up as a failed fetch and not a stuck guest.
        error!("Spill store reads are not available yet; refusing stripe {stripe_id}");
        self.counters.get_failures.fetch_add(1, Ordering::Relaxed);
        self.finished.push((stripe_id, false));
        Ok(())
    }

    fn poll(&mut self) -> Vec<(usize, bool)> {
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
    // Read by the routing that lands with the spill stripe source work.
    #[allow(dead_code)]
    state: SharedMetadataState,
    /// Immediate refusals.
    finished: Vec<(usize, bool)>,
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
        }
    }
}

impl StripeSource for SpillingStripeSource {
    fn request(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
        self.base.request(stripe_id, buffer)
    }

    fn request_demand(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
        self.base.request_demand(stripe_id, buffer)
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
        self.base.has_stripe(stripe_id)
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
    use super::*;
    use crate::{
        archive::{ArchiveCompressionAlgorithm, TestObjectStore},
        backends::SECTOR_SIZE,
        block_device::{bdev_test::TestBlockDevice, shared_buffer, BlockDevice, UbiMetadata},
        stripe_source::BlockDeviceStripeSource,
    };

    const STRIPE_SECTORS: u64 = 8;

    fn state() -> SharedMetadataState {
        SharedMetadataState::new(&UbiMetadata::new(3, 4, 4))
    }

    fn spill_source(state: &SharedMetadataState, connections: usize) -> SpillStripeSource {
        SpillStripeSource::new(
            Box::new(TestObjectStore::new()),
            SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS),
            "dev".to_string(),
            connections,
            state,
        )
    }

    #[test]
    fn spill_source_shell_refuses_requests() {
        let state = state();
        let mut source = spill_source(&state, 3);
        assert_eq!(source.sector_count(), 0);
        assert!(!source.has_stripe(0));
        assert_eq!(source.max_concurrent_requests(), 3);
        assert!(!source.busy());

        source
            .request(2, shared_buffer(STRIPE_SECTORS as usize * SECTOR_SIZE))
            .unwrap();
        assert!(source.busy());
        assert_eq!(source.poll(), vec![(2, false)]);
        assert!(!source.busy());
        assert_eq!(state.spill().get_failures.load(Ordering::Relaxed), 1);
    }

    fn base_over(device: &TestBlockDevice) -> Box<dyn StripeSource> {
        Box::new(BlockDeviceStripeSource::new(BlockDevice::clone(device), STRIPE_SECTORS).unwrap())
    }

    #[test]
    fn spilling_source_passes_through_to_base() {
        let state = state();
        let device = TestBlockDevice::new(4 * STRIPE_SECTORS * SECTOR_SIZE as u64);
        let pattern = vec![0x5Au8; SECTOR_SIZE];
        device.write(STRIPE_SECTORS as usize * SECTOR_SIZE, &pattern, SECTOR_SIZE);
        let mut source = SpillingStripeSource::new(
            base_over(&device),
            Some(spill_source(&state, 2)),
            state.clone(),
        );

        assert_eq!(source.sector_count(), 4 * STRIPE_SECTORS);
        assert!(source.has_stripe(1));
        assert!(!source.has_stripe(4));
        assert_eq!(source.max_concurrent_requests(), 1 + 2);
        assert!(!source.busy());

        let buffer = shared_buffer(STRIPE_SECTORS as usize * SECTOR_SIZE);
        source.request(1, buffer.clone()).unwrap();
        let mut results = source.poll();
        while results.is_empty() {
            results = source.poll();
        }
        assert_eq!(results, vec![(1, true)]);
        assert_eq!(&buffer.borrow().as_slice()[..SECTOR_SIZE], &pattern[..]);

        let buffer = shared_buffer(STRIPE_SECTORS as usize * SECTOR_SIZE);
        source.request_demand(0, buffer).unwrap();
        let mut results = source.poll();
        while results.is_empty() {
            results = source.poll();
        }
        assert_eq!(results, vec![(0, true)]);
        assert!(!source.busy());
    }

    #[test]
    fn spilling_source_without_spill_store_is_base_alone() {
        let state = state();
        let device = TestBlockDevice::new(4 * STRIPE_SECTORS * SECTOR_SIZE as u64);
        let source = SpillingStripeSource::new(base_over(&device), None, state);
        assert_eq!(source.max_concurrent_requests(), 1);
        assert!(!source.busy());
    }
}
