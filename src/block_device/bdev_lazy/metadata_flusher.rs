use crate::{
    backends::SECTOR_SIZE,
    block_device::{
        bdev_lazy::metadata::types::{write_sector_with_crc32, STRIPE_HEADERS_PER_SECTOR},
        metadata_flags, BlockDevice, IoChannel, SharedBuffer, SharedMetadataState, UbiMetadata,
    },
    utils::AlignedBufferPool,
    Result,
};
use log::{debug, error};
use std::collections::{HashMap, HashSet, VecDeque};

const MAX_CONCURRENT_CHANGES: usize = 16;

/// Header bits a request may set or clear. The rest of the byte is reserved.
const UPDATABLE_MASK: u8 =
    metadata_flags::FETCHED | metadata_flags::WRITTEN | metadata_flags::SPILL_MASK;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetadataFlusherRequest {
    stripe_id: usize,
    set: u8,
    clear: u8,
    /// 0: fire-and-forget (SetFetched, SetWritten), deduplicated when the byte
    /// already has the requested value. Non-zero: always written and flushed,
    /// outcome reported under this token via `take_persist_outcomes`.
    token: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestStage {
    Writing,
    Flushing,
}

/// One stripe's part of a sector write.
#[derive(Debug, Clone, Copy)]
struct HeaderUpdateStatus {
    stripe_id: usize,
    header: u8,
    /// The byte before this update. Restored on a write-stage failure. A bit
    /// mask of what was added could not undo a clear.
    previous: u8,
    token: u64,
}

/// A metadata sector write in flight and the updates it carries. Every
/// request for the sector that is queued when the write starts joins it, one
/// per stripe so a stripe's requests still apply in order, and they share the
/// one write and one fsync. The channel knows the write by its sector.
struct SectorWrite {
    buffer: SharedBuffer,
    stage: RequestStage,
    updates: Vec<HeaderUpdateStatus>,
}

/// How far a tokened header update got. The distinction matters because a
/// caller may only act on "the disk now says X" once it holds `Durable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistResult {
    /// Sector write and fsync both completed. The byte is on disk.
    Durable,
    /// The write failed. The disk still holds `previous`; the in-memory byte
    /// was restored to `previous`.
    NotWritten,
    /// The write completed but the fsync failed. The disk may hold either
    /// byte. The in-memory byte is left at the new value so a retry rewrites
    /// it. Callers must not act as if the old byte is on disk.
    Uncertain,
}

/// The completion of one tokened header update, handed back to whoever
/// issued it (odd tokens: the evictor; even tokens: the coordinator).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistOutcome {
    /// The stripe whose header was updated.
    pub stripe_id: usize,
    /// The token the caller passed to `update_stripe_header`.
    pub token: u64,
    /// What happened to the update.
    pub result: PersistResult,
}

pub struct MetadataFlusher {
    channel: Box<dyn IoChannel>,
    metadata: Box<UbiMetadata>,
    shared_state: SharedMetadataState,
    /// The write in flight for each sector; one at a time per sector, so the
    /// next starts from the image this one produced.
    header_updates: HashMap<u64, SectorWrite>,
    queued_requests: VecDeque<MetadataFlusherRequest>,
    buffer_pool: AlignedBufferPool,
    /// Outcomes of tokened requests, in completion order, until taken.
    persist_outcomes: Vec<PersistOutcome>,
    /// A submit failed with requests in the ring. io_uring keeps the SQEs a
    /// failed enter did not consume and enters them on the next submit
    /// (`bdev_uring::submit`), so their completions are still owed: the
    /// statuses, buffers and sectors stay as they are, and the channel is
    /// asked to submit again each update until one succeeds.
    resubmit_needed: bool,
}

impl MetadataFlusher {
    pub fn new(
        metadata_dev: &dyn BlockDevice,
        source_sector_count: u64,
        shared_state: SharedMetadataState,
    ) -> Result<Self> {
        let channel = metadata_dev.create_channel()?;
        let metadata = UbiMetadata::load_from_bdev(metadata_dev)?;

        // Validate stripe count
        let source_stripe_count = source_sector_count.div_ceil(metadata.stripe_sector_count());
        if source_stripe_count > metadata.stripe_count() {
            return Err(crate::ubiblk_error!(InvalidParameter {
                description: format!(
                    "Source stripe count {} exceeds metadata stripe count {}",
                    source_stripe_count,
                    metadata.stripe_count()
                ),
            }));
        }

        Ok(MetadataFlusher {
            channel,
            shared_state,
            metadata,
            queued_requests: VecDeque::new(),
            buffer_pool: AlignedBufferPool::new(4096, MAX_CONCURRENT_CHANGES, SECTOR_SIZE),
            header_updates: HashMap::new(),
            persist_outcomes: Vec::new(),
            resubmit_needed: false,
        })
    }

    pub fn busy(&self) -> bool {
        !self.header_updates.is_empty() || !self.queued_requests.is_empty()
    }

    /// Update{set: FETCHED, clear: EVICTED}, token 0. Clearing EVICTED here is
    /// belt and braces: FETCHED|EVICTED must never reach disk.
    pub fn set_stripe_fetched(&mut self, stripe_id: usize) {
        self.queued_requests.push_back(MetadataFlusherRequest {
            stripe_id,
            set: metadata_flags::FETCHED,
            clear: metadata_flags::EVICTED,
            token: 0,
        });
    }

    /// Update{set: WRITTEN, clear: 0}, token 0.
    pub fn set_stripe_written(&mut self, stripe_id: usize) {
        self.queued_requests.push_back(MetadataFlusherRequest {
            stripe_id,
            set: metadata_flags::WRITTEN,
            clear: 0,
            token: 0,
        });
    }

    /// Masked update. `set` and `clear` may only touch FETCHED | WRITTEN |
    /// SPILL_MASK (debug_assert). With a non-zero `token` the update is always
    /// written and flushed, and its outcome reported under that token by
    /// `take_persist_outcomes`. Token 0 is fire-and-forget like the setters
    /// above: skipped when the byte already reads as requested, no outcome.
    pub fn update_stripe_header(&mut self, stripe_id: usize, set: u8, clear: u8, token: u64) {
        debug_assert!(
            (set | clear) & !UPDATABLE_MASK == 0,
            "header update touches reserved bits: set {set:#010b} clear {clear:#010b}"
        );
        self.queued_requests.push_back(MetadataFlusherRequest {
            stripe_id,
            set,
            clear,
            token,
        });
    }

    /// Outcomes completed since the last call, in completion order.
    pub fn take_persist_outcomes(&mut self) -> Vec<PersistOutcome> {
        std::mem::take(&mut self.persist_outcomes)
    }

    /// The flusher's current in-memory byte (what the next write starts from).
    pub fn header(&self, stripe_id: usize) -> u8 {
        self.metadata
            .stripe_headers
            .get(stripe_id)
            .copied()
            .unwrap_or(0)
    }

    /// The metadata as loaded at construction plus every update applied since.
    pub fn metadata(&self) -> &UbiMetadata {
        &self.metadata
    }

    pub fn update(&mut self) {
        self.resubmit();
        self.start_writes();
        self.poll_channel();
    }

    /// Enter the SQEs of a failed submit, so their completions can arrive. A
    /// bare submit is enough: the ring still holds them.
    fn resubmit(&mut self) {
        if !self.resubmit_needed {
            return;
        }
        match self.channel.submit() {
            Ok(()) => self.resubmit_needed = false,
            Err(e) => debug!("Re-submitting metadata requests still fails: {e}"),
        }
    }

    /// The disk provably still holds the old byte: put memory back to match and
    /// tell a tokened caller so.
    fn fail_not_written(&mut self, status: &HeaderUpdateStatus) {
        self.metadata.stripe_headers[status.stripe_id] = status.previous;
        if status.token != 0 {
            self.persist_outcomes.push(PersistOutcome {
                stripe_id: status.stripe_id,
                token: status.token,
                result: PersistResult::NotWritten,
            });
        }
    }

    /// The sector was written but never made durable, so the disk may hold
    /// either byte. A tokened caller keeps the new byte in memory so its retry
    /// rewrites it; a fire-and-forget request reverts as it always has, since
    /// an unflushed SetFetched only costs a re-fetch.
    fn fail_uncertain(&mut self, status: &HeaderUpdateStatus) {
        if status.token != 0 {
            self.persist_outcomes.push(PersistOutcome {
                stripe_id: status.stripe_id,
                token: status.token,
                result: PersistResult::Uncertain,
            });
        } else {
            self.metadata.stripe_headers[status.stripe_id] = status.previous;
        }
    }

    fn poll_channel(&mut self) {
        let mut finished = Vec::new();
        let mut newly_flushing = false;

        for (id, success) in self.channel.poll() {
            let sector = id as u64;
            let Some(write) = self.header_updates.get_mut(&sector) else {
                error!("Received unexpected response for metadata sector {sector}");
                continue;
            };
            match (write.stage, success) {
                (RequestStage::Writing, false) => {
                    error!("Failed to write metadata sector {sector}");
                    let Some(write) = self.header_updates.remove(&sector) else {
                        continue;
                    };
                    for update in &write.updates {
                        self.fail_not_written(update);
                    }
                    // On write success the buffer is returned before
                    // transitioning to Flushing, so only a write failure
                    // still holds it.
                    self.buffer_pool.return_buffer(&write.buffer);
                }
                (RequestStage::Flushing, false) => {
                    error!("Failed to flush metadata sector {sector}");
                    let Some(write) = self.header_updates.remove(&sector) else {
                        continue;
                    };
                    for update in &write.updates {
                        self.fail_uncertain(update);
                    }
                }
                (RequestStage::Writing, true) => {
                    self.buffer_pool.return_buffer(&write.buffer);
                    self.channel.add_flush(id);
                    write.stage = RequestStage::Flushing;
                    newly_flushing = true;
                }
                (RequestStage::Flushing, true) => {
                    if let Some(write) = self.header_updates.remove(&sector) {
                        finished.push(write);
                    }
                }
            }
        }

        for write in finished {
            for update in write.updates {
                debug!(
                    "Stripe {} metadata updated with header {}",
                    update.stripe_id, update.header
                );
                self.shared_state
                    .set_stripe_header(update.stripe_id, update.header);
                if update.token != 0 {
                    self.persist_outcomes.push(PersistOutcome {
                        stripe_id: update.stripe_id,
                        token: update.token,
                        result: PersistResult::Durable,
                    });
                }
            }
        }

        if !newly_flushing {
            return;
        }

        // submit flushes, if any
        if let Err(e) = self.channel.submit() {
            // The flush SQEs are still in the ring and their completions
            // still owed, as for writes below: the statuses stay in Flushing
            // and hear the outcome when it lands.
            error!("Failed to submit metadata flushes: {e}");
            self.resubmit_needed = true;
        }
    }

    fn start_writes(&mut self) {
        let mut newly_added = false;

        while self.buffer_pool.has_available() {
            let Some(front) = self.queued_requests.front() else {
                break;
            };
            let group = UbiMetadata::stripe_id_to_group(front.stripe_id);
            let sector = UbiMetadata::stripe_id_to_sector(front.stripe_id);
            if self.header_updates.contains_key(&sector) {
                // One write per sector at a time: the next starts from the
                // image this one produces, which is what keeps `previous`
                // right for a stripe updated twice.
                break;
            }

            // Every queued request for this sector joins the write, one per
            // stripe so a stripe's requests still apply in order; the rest
            // stay queued for the sector's next write. With spill every
            // landing waits for a tokened write and fsync, and 508 stripes
            // share a sector, so neighbouring demand fetches that queued
            // while the sector was busy pay for one write and one fsync
            // between them rather than one each.
            let mut taken = Vec::new();
            let mut stripes = HashSet::new();
            self.queued_requests.retain(|req| {
                let joins = UbiMetadata::stripe_id_to_sector(req.stripe_id) == sector
                    && stripes.insert(req.stripe_id);
                if joins {
                    taken.push(*req);
                }
                !joins
            });
            let mut updates = Vec::with_capacity(taken.len());
            for req in taken {
                let previous = self.metadata.stripe_headers[req.stripe_id];
                let next = (previous | req.set) & !req.clear;
                if req.token == 0 && next == previous {
                    // Already as requested, skip. A tokened update is written
                    // regardless: its caller wants the byte on disk, not
                    // merely in memory.
                    continue;
                }
                self.metadata.stripe_headers[req.stripe_id] = next;
                updates.push(HeaderUpdateStatus {
                    stripe_id: req.stripe_id,
                    header: next,
                    previous,
                    token: req.token,
                });
            }
            if updates.is_empty() {
                continue;
            }

            let Some(buf) = self.buffer_pool.get_buffer() else {
                break;
            };
            let headers_start = group * STRIPE_HEADERS_PER_SECTOR;
            let headers_end =
                (headers_start + STRIPE_HEADERS_PER_SECTOR).min(self.metadata.stripe_headers.len());
            let headers = &self.metadata.stripe_headers[headers_start..headers_end];
            write_sector_with_crc32(buf.borrow_mut().as_mut_slice(), headers);

            self.channel
                .add_write(sector, 1, buf.clone(), sector as usize);
            self.header_updates.insert(
                sector,
                SectorWrite {
                    buffer: buf,
                    stage: RequestStage::Writing,
                    updates,
                },
            );
            newly_added = true;
        }

        if !newly_added {
            return;
        }

        // submit writes, if any
        if let Err(e) = self.channel.submit() {
            // The SQEs stay in the ring and a later submit enters them, so
            // the writes may yet happen and their completions are owed.
            // Told NotWritten now, a caller would act on a byte the disk may
            // still take; a buffer returned now could hold another sector's
            // image by the time the kernel reads it. Everything waits for
            // the completion, and `resubmit` keeps asking for it.
            error!("Failed to submit metadata writes: {e}");
            self.resubmit_needed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::block_device::bdev_test::TestBlockDevice;

    use super::*;

    fn init_metadata_device() -> TestBlockDevice {
        let metadata = UbiMetadata::new(11, 16, 16);
        let block_device = TestBlockDevice::new(8 * 1024);
        metadata.save_to_bdev(&block_device).unwrap();
        block_device
    }

    fn wait_for_completion(metadata_flusher: &mut MetadataFlusher) {
        let start = std::time::Instant::now();
        while start.elapsed().as_secs() < 1 && metadata_flusher.busy() {
            metadata_flusher.update();
        }
    }

    #[test]
    fn test_metadata_flusher() {
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let mut metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();

        metadata_flusher.set_stripe_fetched(5);
        metadata_flusher.set_stripe_fetched(6);

        for stripe_id in 5..=6 {
            assert!(!shared_state.stripe_fetched(stripe_id));
            assert!(!shared_state.stripe_written(stripe_id));
        }

        wait_for_completion(&mut metadata_flusher);

        for stripe_id in 5..=6 {
            assert!(shared_state.stripe_fetched(stripe_id));
            assert!(!shared_state.stripe_written(stripe_id));
        }

        metadata_flusher.set_stripe_written(7);
        assert!(!shared_state.stripe_written(7));
        assert!(!shared_state.stripe_fetched(7));

        wait_for_completion(&mut metadata_flusher);

        assert!(!shared_state.stripe_fetched(7));
        assert!(shared_state.stripe_written(7));
    }

    #[test]
    fn test_source_stripe_count_too_large() {
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 1024 * 1024 * 1024, shared_state);
        assert!(metadata_flusher.is_err());
    }

    #[test]
    fn test_request_serialization() {
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let mut metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();

        // Stripes 0-7 are in sector 1
        for stripe_id in 0..8 {
            metadata_flusher.set_stripe_fetched(stripe_id);
            if stripe_id % 3 == 0 {
                // Interleave some writes
                metadata_flusher.set_stripe_written(stripe_id);
            }
        }

        // add some duplicate requests
        metadata_flusher.set_stripe_fetched(2);
        metadata_flusher.set_stripe_written(3);

        wait_for_completion(&mut metadata_flusher);

        for stripe_id in 0..8 {
            assert!(shared_state.stripe_fetched(stripe_id));
            if stripe_id % 3 == 0 {
                assert!(shared_state.stripe_written(stripe_id));
            } else {
                assert!(!shared_state.stripe_written(stripe_id));
            }
        }
    }

    #[test]
    fn test_write_failure_allows_retry() {
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let mut metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();

        // Queue a fetched request and inject a write failure
        metadata_flusher.set_stripe_fetched(5);
        metadata_dev
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // First update: start_writes issues add_write (which fails), poll_channel sees failure
        metadata_flusher.update();

        // Stripe 5 should NOT be marked fetched in shared state
        assert!(!shared_state.stripe_fetched(5));
        // Flusher should not be busy (failure was handled, no pending requests)
        assert!(!metadata_flusher.busy());

        // Retry: queue the same request again - should NOT be skipped by dedup
        metadata_flusher.set_stripe_fetched(5);
        wait_for_completion(&mut metadata_flusher);

        // Now it should succeed
        assert!(shared_state.stripe_fetched(5));
    }

    #[test]
    fn test_write_failure_does_not_affect_other_stripes() {
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let mut metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();

        // First, successfully set stripe 5 as fetched
        metadata_flusher.set_stripe_fetched(5);
        wait_for_completion(&mut metadata_flusher);
        assert!(shared_state.stripe_fetched(5));

        // Now try to set stripe 5 as written, but inject a failure.
        // Stripe 5 is in sector group 0 (stripe 5 / 508 = 0), sector 1.
        metadata_flusher.set_stripe_written(5);
        metadata_dev
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        metadata_flusher.update();

        // The written flag should NOT be set, but fetched should still be set
        assert!(shared_state.stripe_fetched(5));
        assert!(!shared_state.stripe_written(5));

        // Retry written - should succeed
        metadata_flusher.set_stripe_written(5);
        wait_for_completion(&mut metadata_flusher);
        assert!(shared_state.stripe_written(5));
        assert!(shared_state.stripe_fetched(5));
    }

    #[test]
    fn test_metadata_flusher_coalesces_duplicate_requests() {
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let mut metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();
        let (start_writes, start_flushes) = {
            let metrics = metadata_dev.metrics.read().unwrap();
            (metrics.writes, metrics.flushes)
        };

        metadata_flusher.set_stripe_written(3);
        metadata_flusher.set_stripe_written(3);
        metadata_flusher.set_stripe_written(3);
        metadata_flusher.set_stripe_fetched(3);
        metadata_flusher.set_stripe_fetched(3);

        assert!(!shared_state.stripe_written(3));
        assert!(!shared_state.stripe_fetched(3));

        wait_for_completion(&mut metadata_flusher);

        assert!(shared_state.stripe_written(3));
        assert!(shared_state.stripe_fetched(3));

        let metrics = metadata_dev.metrics.read().unwrap();
        assert_eq!(metrics.writes - start_writes, 2);
        assert_eq!(metrics.flushes - start_flushes, 2);
    }

    /// A failed submit leaves the SQE in the ring for the next submit to
    /// enter, so the write may still happen. The request stays where it is
    /// and lands when its completion does.
    #[test]
    fn write_submit_failure_waits_for_the_completion() {
        use std::sync::atomic::Ordering::SeqCst;
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let mut metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();
        let (writes, flushes) = io_counts(&metadata_dev);

        // The request survives the failed submit, as io_uring's do, and its
        // completion is held back until the test lets it through.
        metadata_dev
            .keep_requests_on_failed_submit
            .store(true, SeqCst);
        metadata_dev.hold_completions.store(true, SeqCst);
        metadata_dev.fail_submit.store(true, SeqCst);
        metadata_flusher.set_stripe_fetched(5);
        metadata_flusher.update();

        assert!(!shared_state.stripe_fetched(5));
        assert!(metadata_flusher.busy(), "the write is still owed");
        assert_ne!(
            metadata_flusher.header(5) & metadata_flags::FETCHED,
            0,
            "memory keeps the byte the write carries"
        );
        // Ticks with the completion owed neither re-issue nor give up.
        metadata_flusher.update();
        assert_eq!(io_counts(&metadata_dev), (writes + 1, flushes));
        assert!(metadata_flusher.busy());

        metadata_dev.hold_completions.store(false, SeqCst);
        wait_for_completion(&mut metadata_flusher);
        assert!(shared_state.stripe_fetched(5));
        assert!(!metadata_flusher.busy());
        assert_eq!(io_counts(&metadata_dev), (writes + 1, flushes + 1));
    }

    #[test]
    fn test_flush_failure_no_double_buffer_return() {
        // Regression: when a write succeeds and the subsequent flush fails,
        // the error handler must NOT return the buffer a second time (it was
        // already returned when transitioning to Flushing).
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let mut metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();

        // Queue a request and let the write complete normally
        metadata_flusher.set_stripe_fetched(5);
        metadata_flusher.start_writes();

        // Set fail_next so that add_flush() (called when poll_channel
        // processes the write completion) will enqueue a flush failure.
        metadata_dev
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        metadata_flusher.poll_channel();

        // Now poll_channel again to process the flush failure.
        // Without the fix this panics due to double buffer return.
        metadata_flusher.poll_channel();

        // Stripe 5 should NOT be marked fetched (flush failed)
        assert!(!shared_state.stripe_fetched(5));
        // Flusher should not be busy (failure was cleaned up)
        assert!(!metadata_flusher.busy());
    }

    #[test]
    fn test_flush_failure_allows_retry() {
        // After a flush failure, the same stripe operation can be retried
        // and should succeed.
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let mut metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();

        // Queue a request and let the write complete normally
        metadata_flusher.set_stripe_fetched(5);
        metadata_flusher.start_writes();

        // Inject flush failure
        metadata_dev
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        metadata_flusher.poll_channel();
        metadata_flusher.poll_channel();

        // Verify the failure state
        assert!(!shared_state.stripe_fetched(5));
        assert!(!metadata_flusher.busy());

        // Retry: queue the same request again - should NOT be skipped by dedup
        metadata_flusher.set_stripe_fetched(5);
        wait_for_completion(&mut metadata_flusher);

        // Now it should succeed
        assert!(shared_state.stripe_fetched(5));
    }

    /// The same for a flush: its SQE stays in the ring, the request stays in
    /// Flushing, and the stripe lands when the fsync completes.
    #[test]
    fn flush_submit_failure_waits_for_the_completion() {
        use std::sync::atomic::Ordering::SeqCst;
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let mut metadata_flusher =
            MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();

        metadata_flusher.set_stripe_fetched(5);
        metadata_flusher.start_writes();
        // The write completes; the flush it triggers is added, and its
        // submit fails with the SQE left in the ring.
        metadata_dev
            .keep_requests_on_failed_submit
            .store(true, SeqCst);
        metadata_dev.fail_submit.store(true, SeqCst);
        metadata_flusher.poll_channel();
        assert!(!shared_state.stripe_fetched(5));
        assert!(metadata_flusher.busy(), "the flush is still owed");

        // Its completion held back, a tick changes nothing.
        metadata_dev.hold_completions.store(true, SeqCst);
        metadata_flusher.update();
        assert!(!shared_state.stripe_fetched(5));
        assert!(metadata_flusher.busy());

        metadata_dev.hold_completions.store(false, SeqCst);
        wait_for_completion(&mut metadata_flusher);
        assert!(shared_state.stripe_fetched(5));
        assert!(!metadata_flusher.busy());
    }

    fn init_flusher() -> (TestBlockDevice, SharedMetadataState, MetadataFlusher) {
        let metadata_dev = init_metadata_device();
        let shared_state = {
            let metadata = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&metadata)
        };
        let flusher = MetadataFlusher::new(&metadata_dev, 8 * 1024, shared_state.clone()).unwrap();
        (metadata_dev, shared_state, flusher)
    }

    fn on_disk_header(metadata_dev: &TestBlockDevice, stripe_id: usize) -> u8 {
        UbiMetadata::load_from_bdev(metadata_dev)
            .expect("load metadata")
            .stripe_header(stripe_id)
    }

    fn io_counts(metadata_dev: &TestBlockDevice) -> (usize, usize) {
        let metrics = metadata_dev.metrics.read().unwrap();
        (metrics.writes, metrics.flushes)
    }

    #[test]
    fn masked_update_clears_fetched_and_sets_evicted_in_one_write() {
        let (metadata_dev, shared_state, mut flusher) = init_flusher();
        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);
        let old = flusher.header(5);
        assert_eq!(old, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        let (writes, flushes) = io_counts(&metadata_dev);

        flusher.update_stripe_header(
            5,
            metadata_flags::EVICTED | metadata_flags::IN_S3,
            metadata_flags::FETCHED,
            7,
        );
        wait_for_completion(&mut flusher);

        let expected =
            (old | metadata_flags::EVICTED | metadata_flags::IN_S3) & !metadata_flags::FETCHED;
        assert_eq!(io_counts(&metadata_dev), (writes + 1, flushes + 1));
        assert_eq!(on_disk_header(&metadata_dev, 5), expected);
        assert_eq!(flusher.header(5), expected);
        assert_eq!(
            flusher.take_persist_outcomes(),
            vec![PersistOutcome {
                stripe_id: 5,
                token: 7,
                result: PersistResult::Durable,
            }]
        );
        assert!(flusher.take_persist_outcomes().is_empty());
        // The in-memory state is the evictor's to move; the completion only
        // carries the side bit.
        assert!(shared_state.stripe_fetched(5));
        assert!(shared_state.stripe_in_s3(5));
    }

    #[test]
    fn masked_update_outcome_is_durable_only_after_flush() {
        let (_metadata_dev, _shared_state, mut flusher) = init_flusher();

        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 1);
        flusher.start_writes();
        assert!(flusher.take_persist_outcomes().is_empty());

        // Write completion: the flush is issued, nothing is durable yet.
        flusher.poll_channel();
        assert!(flusher.take_persist_outcomes().is_empty());
        assert!(flusher.busy());

        // Flush completion.
        flusher.poll_channel();
        let outcomes = flusher.take_persist_outcomes();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].result, PersistResult::Durable);
        assert!(!flusher.busy());
    }

    #[test]
    fn masked_update_write_failure_reports_not_written_and_restores_previous_byte() {
        let (metadata_dev, _shared_state, mut flusher) = init_flusher();
        flusher.set_stripe_fetched(5);
        flusher.set_stripe_written(5);
        wait_for_completion(&mut flusher);
        let original =
            metadata_flags::FETCHED | metadata_flags::WRITTEN | metadata_flags::HAS_SOURCE;
        assert_eq!(flusher.header(5), original);

        metadata_dev
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 3);
        flusher.update();

        assert_eq!(
            flusher.take_persist_outcomes(),
            vec![PersistOutcome {
                stripe_id: 5,
                token: 3,
                result: PersistResult::NotWritten,
            }]
        );
        // The whole byte comes back, FETCHED included; `& !mask` could not
        // have restored a cleared bit.
        assert_eq!(flusher.header(5), original);
        assert_eq!(on_disk_header(&metadata_dev, 5), original);
        assert!(!flusher.busy());
    }

    #[test]
    fn masked_update_flush_failure_reports_uncertain_and_keeps_new_byte() {
        let (metadata_dev, _shared_state, mut flusher) = init_flusher();
        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);
        let old = flusher.header(5);

        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 3);
        flusher.start_writes();
        // The write has completed; arm the failure for the flush it triggers.
        metadata_dev
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        flusher.poll_channel();
        flusher.poll_channel();

        assert_eq!(
            flusher.take_persist_outcomes(),
            vec![PersistOutcome {
                stripe_id: 5,
                token: 3,
                result: PersistResult::Uncertain,
            }]
        );
        let new = (old | metadata_flags::EVICTED) & !metadata_flags::FETCHED;
        assert_eq!(
            flusher.header(5),
            new,
            "memory keeps the new byte for the retry"
        );
        assert_eq!(
            on_disk_header(&metadata_dev, 5),
            new,
            "the sector write did land"
        );
        assert!(!flusher.busy());

        // The retry rewrites the same byte and is durable.
        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 5);
        wait_for_completion(&mut flusher);
        assert_eq!(
            flusher.take_persist_outcomes()[0].result,
            PersistResult::Durable
        );
    }

    /// Every spare buffer in the pool, so a test can count what is on offer.
    fn spare_buffers(flusher: &mut MetadataFlusher) -> usize {
        let mut spare = Vec::new();
        while let Some(buf) = flusher.buffer_pool.get_buffer() {
            spare.push(buf);
        }
        for buf in &spare {
            flusher.buffer_pool.return_buffer(buf);
        }
        spare.len()
    }

    /// A tokened write whose submit failed may still land: the SQE is in the
    /// ring for the next submit. NotWritten now would have the evictor abort
    /// an eviction whose EVICTED header the disk may yet take, and a buffer
    /// returned now could hold another update's sector image by the time the
    /// kernel reads it. Status and buffer stay until the completion arrives,
    /// and the outcome is whatever it says.
    #[test]
    fn write_submit_failure_holds_status_and_buffer_until_the_completion_lands() {
        use std::sync::atomic::Ordering::SeqCst;
        let (metadata_dev, _shared_state, mut flusher) = init_flusher();
        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);
        let old = flusher.header(5);
        let (writes, flushes) = io_counts(&metadata_dev);
        assert_eq!(spare_buffers(&mut flusher), MAX_CONCURRENT_CHANGES);

        metadata_dev
            .keep_requests_on_failed_submit
            .store(true, SeqCst);
        metadata_dev.hold_completions.store(true, SeqCst);
        metadata_dev.fail_submit.store(true, SeqCst);
        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 3);
        flusher.update();

        let new = (old | metadata_flags::EVICTED) & !metadata_flags::FETCHED;
        assert!(
            flusher.take_persist_outcomes().is_empty(),
            "nothing reported early"
        );
        assert!(flusher.busy());
        assert_eq!(
            flusher.header(5),
            new,
            "memory keeps the byte the write carries"
        );
        assert_eq!(
            spare_buffers(&mut flusher),
            MAX_CONCURRENT_CHANGES - 1,
            "the write's buffer is not on offer"
        );

        // Ticks with the completion owed neither re-issue the write nor
        // give up on it.
        flusher.update();
        flusher.update();
        assert!(flusher.take_persist_outcomes().is_empty());
        assert_eq!(io_counts(&metadata_dev), (writes + 1, flushes));
        assert!(flusher.busy());

        // The late completion: the write landed, the flush follows, and the
        // outcome is Durable, once.
        metadata_dev.hold_completions.store(false, SeqCst);
        wait_for_completion(&mut flusher);
        assert_eq!(
            flusher.take_persist_outcomes(),
            vec![PersistOutcome {
                stripe_id: 5,
                token: 3,
                result: PersistResult::Durable,
            }]
        );
        assert_eq!(on_disk_header(&metadata_dev, 5), new);
        assert_eq!(io_counts(&metadata_dev), (writes + 1, flushes + 1));
        assert!(!flusher.busy());
        assert_eq!(
            spare_buffers(&mut flusher),
            MAX_CONCURRENT_CHANGES,
            "the buffer came back with the completion"
        );
    }

    /// The completion a failed submit still owes may itself be a failure;
    /// NotWritten is reported then, and the byte restored then, not at the
    /// failed submit.
    #[test]
    fn write_submit_failure_reports_not_written_only_when_the_failed_completion_lands() {
        use std::sync::atomic::Ordering::SeqCst;
        let (metadata_dev, _shared_state, mut flusher) = init_flusher();
        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);
        let original = flusher.header(5);

        metadata_dev
            .keep_requests_on_failed_submit
            .store(true, SeqCst);
        metadata_dev.hold_completions.store(true, SeqCst);
        metadata_dev.fail_next.store(true, SeqCst);
        metadata_dev.fail_submit.store(true, SeqCst);
        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 3);
        flusher.update();
        assert!(flusher.take_persist_outcomes().is_empty(), "not yet");
        assert!(flusher.busy());
        assert_ne!(
            flusher.header(5),
            original,
            "not restored before the disk has answered"
        );

        metadata_dev.hold_completions.store(false, SeqCst);
        flusher.update();
        assert_eq!(
            flusher.take_persist_outcomes(),
            vec![PersistOutcome {
                stripe_id: 5,
                token: 3,
                result: PersistResult::NotWritten,
            }]
        );
        assert_eq!(flusher.header(5), original);
        assert_eq!(on_disk_header(&metadata_dev, 5), original);
        assert!(!flusher.busy());
    }

    /// A flush whose submit failed is owed as well: the request stays in
    /// Flushing and is Durable, not Uncertain, once the fsync completes.
    #[test]
    fn flush_submit_failure_holds_the_request_until_the_completion_lands() {
        use std::sync::atomic::Ordering::SeqCst;
        let (metadata_dev, _shared_state, mut flusher) = init_flusher();
        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);
        let old = flusher.header(5);

        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 3);
        flusher.start_writes();
        // The sector write has completed; the flush it triggers is added and
        // its submit fails with the SQE left in the ring.
        metadata_dev
            .keep_requests_on_failed_submit
            .store(true, SeqCst);
        metadata_dev.fail_submit.store(true, SeqCst);
        flusher.poll_channel();
        assert!(
            flusher.take_persist_outcomes().is_empty(),
            "nothing reported early"
        );
        assert!(flusher.busy());
        let new = (old | metadata_flags::EVICTED) & !metadata_flags::FETCHED;
        assert_eq!(flusher.header(5), new);

        // Its completion held back, a tick changes nothing.
        metadata_dev.hold_completions.store(true, SeqCst);
        flusher.update();
        assert!(flusher.take_persist_outcomes().is_empty());
        assert!(flusher.busy());

        metadata_dev.hold_completions.store(false, SeqCst);
        wait_for_completion(&mut flusher);
        assert_eq!(
            flusher.take_persist_outcomes(),
            vec![PersistOutcome {
                stripe_id: 5,
                token: 3,
                result: PersistResult::Durable,
            }]
        );
        assert_eq!(on_disk_header(&metadata_dev, 5), new);
        assert!(!flusher.busy());
    }

    #[test]
    fn masked_update_is_never_deduplicated() {
        let (metadata_dev, _shared_state, mut flusher) = init_flusher();
        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);
        let (writes, flushes) = io_counts(&metadata_dev);

        // Fire-and-forget: already set, nothing written.
        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);
        assert_eq!(io_counts(&metadata_dev), (writes, flushes));

        // Tokened: the same no-op change is written and flushed anyway.
        flusher.update_stripe_header(5, metadata_flags::FETCHED, 0, 9);
        wait_for_completion(&mut flusher);
        assert_eq!(io_counts(&metadata_dev), (writes + 1, flushes + 1));
        assert_eq!(
            flusher.take_persist_outcomes()[0].result,
            PersistResult::Durable
        );
    }

    #[test]
    fn set_fetched_clears_evicted() {
        let (metadata_dev, shared_state, mut flusher) = init_flusher();
        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 1);
        wait_for_completion(&mut flusher);
        assert_ne!(flusher.header(5) & metadata_flags::EVICTED, 0);

        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);

        let header = flusher.header(5);
        assert_eq!(header & metadata_flags::EVICTED, 0);
        assert_ne!(header & metadata_flags::FETCHED, 0);
        assert_eq!(on_disk_header(&metadata_dev, 5), header);
        assert!(shared_state.stripe_fetched(5));
    }

    #[test]
    fn release_op_for_failed_was_evicted_stripe_lands_only_through_mark_stripe_resident() {
        use crate::block_device::{
            bdev_lazy::metadata::{Failed, Fetched},
            stripe_flags,
        };
        use std::sync::atomic::Ordering;
        let (metadata_dev, shared_state, mut flusher) = init_flusher();

        // Stripe 5 goes resident, gets evicted (header and state), and its
        // re-fetch then fails for good: Failed with WAS_EVICTED, header still
        // EVICTED.
        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);
        let previous = shared_state.try_begin_evicting(5).expect("claim stripe 5");
        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 1);
        wait_for_completion(&mut flusher);
        assert_eq!(
            flusher.take_persist_outcomes()[0].result,
            PersistResult::Durable
        );
        shared_state.finish_evicting(5, previous, false);
        shared_state.set_stripe_failed(5);
        assert_eq!(shared_state.stripe_fetch_state(5), Failed);
        assert!(shared_state.stripe_flags(5) & stripe_flags::WAS_EVICTED != 0);
        let degraded = shared_state
            .spill()
            .degraded_reasons
            .load(Ordering::Relaxed);
        let (fetched, resident) = (
            shared_state.fetched_stripes(),
            shared_state.resident_stripes(),
        );

        // The coordinator's even-token release op. Its completion runs
        // set_stripe_header, which must leave the landing to the coordinator
        // without recording an anomaly.
        flusher.update_stripe_header(5, metadata_flags::FETCHED, metadata_flags::EVICTED, 2);
        wait_for_completion(&mut flusher);
        assert_eq!(
            flusher.take_persist_outcomes(),
            vec![PersistOutcome {
                stripe_id: 5,
                token: 2,
                result: PersistResult::Durable,
            }]
        );
        let header = on_disk_header(&metadata_dev, 5);
        assert_ne!(header & metadata_flags::FETCHED, 0);
        assert_eq!(header & metadata_flags::EVICTED, 0);
        assert_eq!(shared_state.stripe_fetch_state(5), Failed);
        assert!(shared_state.stripe_flags(5) & stripe_flags::WAS_EVICTED != 0);
        assert_eq!(
            shared_state
                .spill()
                .degraded_reasons
                .load(Ordering::Relaxed),
            degraded
        );
        assert_eq!(shared_state.fetched_stripes(), fetched);
        assert_eq!(shared_state.resident_stripes(), resident);

        // The coordinator sees Durable and lands the stripe.
        shared_state.mark_stripe_resident(5);
        assert_eq!(shared_state.stripe_fetch_state(5), Fetched);
        assert_eq!(shared_state.stripe_flags(5) & stripe_flags::WAS_EVICTED, 0);
        assert_eq!(shared_state.fetched_stripes(), fetched + 1);
        assert_eq!(shared_state.resident_stripes(), resident + 1);
        assert_eq!(
            shared_state
                .spill()
                .degraded_reasons
                .load(Ordering::Relaxed),
            degraded
        );
    }

    /// Requests for different stripes of one sector that are queued together
    /// go out as one write and one fsync, and neither loses the other.
    #[test]
    fn same_sector_updates_share_one_write_and_one_flush() {
        let (metadata_dev, shared_state, mut flusher) = init_flusher();
        let (writes, flushes) = io_counts(&metadata_dev);

        // Stripes 5 and 6 share sector 1.
        flusher.set_stripe_written(5);
        flusher.update_stripe_header(6, metadata_flags::EVICTED, metadata_flags::FETCHED, 1);
        flusher.start_writes();
        assert_eq!(io_counts(&metadata_dev), (writes + 1, flushes));
        assert!(flusher.take_persist_outcomes().is_empty());
        assert!(!shared_state.stripe_written(5));

        wait_for_completion(&mut flusher);
        assert_eq!(io_counts(&metadata_dev), (writes + 1, flushes + 1));
        assert_eq!(
            flusher.take_persist_outcomes(),
            vec![PersistOutcome {
                stripe_id: 6,
                token: 1,
                result: PersistResult::Durable,
            }]
        );
        assert!(shared_state.stripe_written(5));
        assert_ne!(
            on_disk_header(&metadata_dev, 5) & metadata_flags::WRITTEN,
            0
        );
        assert_ne!(
            on_disk_header(&metadata_dev, 6) & metadata_flags::EVICTED,
            0
        );
    }

    /// Two requests for the same stripe never share a write: the second
    /// waits for the first, so it starts from the byte the first produced and
    /// `previous` is right for both.
    #[test]
    fn same_stripe_updates_still_apply_in_order() {
        let (metadata_dev, _shared_state, mut flusher) = init_flusher();
        flusher.set_stripe_fetched(5);
        wait_for_completion(&mut flusher);
        let old = flusher.header(5);
        let (writes, flushes) = io_counts(&metadata_dev);

        flusher.set_stripe_written(5);
        flusher.update_stripe_header(5, metadata_flags::EVICTED, metadata_flags::FETCHED, 1);
        flusher.start_writes();
        assert_eq!(
            io_counts(&metadata_dev),
            (writes + 1, flushes),
            "the tokened update waits behind SetWritten"
        );
        assert_eq!(flusher.header(5), old | metadata_flags::WRITTEN);

        wait_for_completion(&mut flusher);
        assert_eq!(io_counts(&metadata_dev), (writes + 2, flushes + 2));
        assert_eq!(
            flusher.take_persist_outcomes()[0].result,
            PersistResult::Durable
        );
        let expected =
            (old | metadata_flags::WRITTEN | metadata_flags::EVICTED) & !metadata_flags::FETCHED;
        assert_eq!(on_disk_header(&metadata_dev, 5), expected);
    }

    /// The case the coordinator produces under load: one landing's tokened
    /// header is in flight when the next ones queue for the same sector.
    /// They wait for that write, then share the next one, so four landings
    /// cost two writes and two fsyncs rather than four of each.
    #[test]
    fn tokened_updates_queued_behind_a_busy_sector_share_its_next_write() {
        let (metadata_dev, _shared_state, mut flusher) = init_flusher();
        let (writes, flushes) = io_counts(&metadata_dev);

        flusher.update_stripe_header(1, metadata_flags::FETCHED, metadata_flags::EVICTED, 2);
        flusher.start_writes();
        assert_eq!(io_counts(&metadata_dev), (writes + 1, flushes));

        for (stripe_id, token) in [(2, 4), (3, 6), (4, 8)] {
            flusher.update_stripe_header(
                stripe_id,
                metadata_flags::FETCHED,
                metadata_flags::EVICTED,
                token,
            );
        }
        flusher.start_writes();
        assert_eq!(
            io_counts(&metadata_dev),
            (writes + 1, flushes),
            "nothing starts while the sector's write is in flight"
        );
        assert_eq!(flusher.queued_requests.len(), 3);

        wait_for_completion(&mut flusher);
        assert_eq!(io_counts(&metadata_dev), (writes + 2, flushes + 2));
        let outcomes = flusher.take_persist_outcomes();
        assert_eq!(
            outcomes.iter().map(|o| o.token).collect::<Vec<_>>(),
            vec![2, 4, 6, 8]
        );
        assert!(outcomes.iter().all(|o| o.result == PersistResult::Durable));
        for stripe_id in 1..=4 {
            assert_ne!(
                on_disk_header(&metadata_dev, stripe_id) & metadata_flags::FETCHED,
                0
            );
        }
    }

    #[test]
    fn metadata_accessor_reflects_applied_updates() {
        let (_metadata_dev, _shared_state, mut flusher) = init_flusher();
        assert_eq!(
            flusher.metadata().stripe_header(5) & metadata_flags::FETCHED,
            0
        );

        flusher.set_stripe_fetched(5);
        assert_eq!(
            flusher.metadata().stripe_header(5) & metadata_flags::FETCHED,
            0,
            "queued is not applied"
        );

        flusher.start_writes();
        assert_ne!(
            flusher.metadata().stripe_header(5) & metadata_flags::FETCHED,
            0
        );
        assert_eq!(flusher.metadata().stripe_header(5), flusher.header(5));
        assert_eq!(flusher.metadata().evicted_stripe_ids(), Vec::<usize>::new());
        wait_for_completion(&mut flusher);

        flusher.update_stripe_header(6, metadata_flags::EVICTED, metadata_flags::FETCHED, 1);
        wait_for_completion(&mut flusher);
        assert_eq!(flusher.metadata().evicted_stripe_ids(), vec![6]);
    }
}
