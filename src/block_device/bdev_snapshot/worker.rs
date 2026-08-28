use std::{
    sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError},
    time::{Duration, Instant},
};

use log::{debug, error, info, warn};

use crate::{
    block_device::{shared_buffer, wait_for_completion, BlockDevice, IoChannel},
    Result,
};

use super::{
    destination::{DestinationId, SnapshotDestination},
    state::SharedSnapshotState,
};

/// How long a single stripe read from the device below may take before the
/// copy-out is treated as failed.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// The most destinations one snapshot may serve at a time.
pub const MAX_DESTINATIONS: usize = 8;

/// How long a fresh snapshot waits for its first subscriber before giving up.
///
/// A copy-out with nowhere to send has to either lose the stripe or hold the
/// write. Holding it briefly is what makes "snapshot, then start the fork" work
/// at all: prod would otherwise overwrite the snapshot in the seconds it takes
/// the fork to come up.
const SUBSCRIBER_GRACE: Duration = Duration::from_secs(60);

pub enum SnapshotRequest {
    /// A write is waiting on this stripe, or a destination asked for it.
    CopyOut {
        stripe_id: usize,
    },
    AddDestination(Box<dyn SnapshotDestination>),
    RemoveDestination(DestinationId),
    /// Freeze: lock every stripe and start serving the snapshot.
    Freeze,
    Shutdown,
}

/// Owns the snapshot destinations and does the copy-out reads.
///
/// It runs on its own thread so a slow or wedged destination costs the I/O
/// path nothing beyond the stripes it is actually holding.
pub struct SnapshotWorker {
    read_channel: Box<dyn IoChannel>,
    state: SharedSnapshotState,
    destinations: Vec<Box<dyn SnapshotDestination>>,
    requests: Receiver<SnapshotRequest>,
    next_request_id: usize,
    /// Stripes whose copy-out is waiting for the first subscriber. The writes
    /// that triggered them are blocked until then.
    deferred: Vec<usize>,
    frozen_at: Option<Instant>,
    subscriber_grace: Duration,
    done: bool,
}

impl SnapshotWorker {
    pub fn new(
        source_dev: &dyn BlockDevice,
        state: SharedSnapshotState,
        requests: Receiver<SnapshotRequest>,
    ) -> Result<Self> {
        Ok(Self {
            read_channel: source_dev.create_channel()?,
            state,
            destinations: Vec::new(),
            requests,
            next_request_id: 0,
            deferred: Vec::new(),
            frozen_at: None,
            subscriber_grace: SUBSCRIBER_GRACE,
            done: false,
        })
    }

    pub fn destination_count(&self) -> usize {
        self.destinations.len()
    }

    /// How long a fresh snapshot holds writes waiting for its first subscriber.
    pub fn set_subscriber_grace(&mut self, grace: Duration) {
        self.subscriber_grace = grace;
    }

    #[cfg(test)]
    pub fn deferred_stripes(&self) -> &[usize] {
        &self.deferred
    }

    fn add_destination(&mut self, destination: Box<dyn SnapshotDestination>) {
        if self.destinations.len() >= MAX_DESTINATIONS {
            warn!(
                "Refusing snapshot destination {}: already serving {} destinations",
                destination.id(),
                MAX_DESTINATIONS
            );
            return;
        }
        info!("Snapshot destination {} added", destination.id());
        self.destinations.push(destination);
        self.publish_destination_count();
        self.run_deferred_copy_outs();
    }

    /// Copy out the stripes that were waiting for someone to send them to.
    fn run_deferred_copy_outs(&mut self) {
        for stripe_id in std::mem::take(&mut self.deferred) {
            self.copy_out(stripe_id);
        }
    }

    /// Give up on a snapshot nobody ever subscribed to, releasing the writes it
    /// was holding.
    pub fn expire_grace_if_needed(&mut self) {
        if self.deferred.is_empty() || !self.destinations.is_empty() {
            return;
        }
        let Some(frozen_at) = self.frozen_at else {
            return;
        };
        if frozen_at.elapsed() < self.subscriber_grace {
            return;
        }

        warn!(
            "No snapshot subscriber after {}s; ending the snapshot and releasing {} held stripes",
            self.subscriber_grace.as_secs(),
            self.deferred.len()
        );
        self.deferred.clear();
        self.state.end_snapshot();
    }

    fn publish_destination_count(&self) {
        self.state.set_destination_count(self.destinations.len());
    }

    fn remove_destination(&mut self, id: DestinationId) {
        self.destinations.retain(|d| d.id() != id);
        self.publish_destination_count();
        self.end_snapshot_if_unwatched();
    }

    /// With no destination left there is nothing to protect, so every stripe
    /// goes back to Free and prod stops paying for the snapshot.
    fn end_snapshot_if_unwatched(&mut self) {
        if self.destinations.is_empty() && self.state.generation() > 0 {
            info!("Last snapshot destination is gone, releasing all stripes");
            self.state.release_all();
        }
    }

    /// A destination that wants the whole snapshot — an exporter writing it to
    /// an archive, rather than a fork pulling what it needs — gets every stripe
    /// copied out, not just the ones prod overwrites.
    ///
    /// The sweep runs inline after the freeze, one stripe at a time, so it
    /// interleaves with the copy-outs that prod writes are waiting on rather
    /// than blocking them behind a full pass over the device.
    fn sweep_for_exporters(&mut self) {
        if !self.destinations.iter().any(|d| d.wants_all_stripes()) {
            return;
        }

        info!("Sweeping every stripe for a snapshot exporter");
        for stripe_id in 0..self.state.stripe_count() {
            self.receive_requests(false);
            if self.done || self.destinations.is_empty() {
                return;
            }
            self.copy_out(stripe_id);
        }

        for destination in self.destinations.iter_mut() {
            destination.finish();
        }
    }

    fn read_stripe(&mut self, stripe_id: usize) -> Result<Vec<u8>> {
        let sector_count = self.state.stripe_sector_count();
        let len = sector_count as usize * crate::backends::SECTOR_SIZE;
        let buf = shared_buffer(len);
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);

        let sector_offset = stripe_id as u64 * sector_count;
        self.read_channel
            .add_read(sector_offset, sector_count as u32, buf.clone(), id);
        self.read_channel.submit()?;
        wait_for_completion(self.read_channel.as_mut(), id, READ_TIMEOUT)?;

        let data = buf.borrow().as_slice()[..len].to_vec();
        Ok(data)
    }

    /// Read the pre-write content of a stripe and hand it to every live
    /// destination. Destinations that fail are dropped, never retried: prod
    /// writes are waiting on this.
    fn copy_out(&mut self, stripe_id: usize) {
        if !self.state.begin_copy(stripe_id) {
            // Someone else already copied this stripe out, or it was never
            // locked in the first place.
            return;
        }

        if self.destinations.is_empty() {
            if self.state.generation() > 0
                && self
                    .frozen_at
                    .is_some_and(|frozen_at| frozen_at.elapsed() < self.subscriber_grace)
            {
                // The fork is still coming up. Put the stripe back and leave the
                // write waiting rather than losing the snapshot's copy of it.
                debug!("Deferring copy-out of stripe {stripe_id} until a fork subscribes");
                self.state.defer_copy(stripe_id);
                self.deferred.push(stripe_id);
                return;
            }

            // Nobody is listening and nobody is coming: this stripe's content is
            // about to be overwritten with no copy anywhere. Serving the rest of
            // the snapshot after that would hand a fork a torn image, so end it.
            warn!("Copy-out of stripe {stripe_id} with no destinations; ending the snapshot");
            self.state.finish_copy(stripe_id);
            self.state.end_snapshot();
            return;
        }

        let data = match self.read_stripe(stripe_id) {
            Ok(data) => data,
            Err(e) => {
                // The write cannot be held forever on a read that will not
                // succeed. Release the stripe and let prod carry on; the
                // destination will see a short read rather than a hang.
                error!("Failed to read stripe {stripe_id} for snapshot: {e}");
                self.state.finish_copy(stripe_id);
                return;
            }
        };

        let mut failed = Vec::new();
        for destination in self.destinations.iter_mut() {
            if !destination.is_alive() {
                failed.push(destination.id());
                continue;
            }
            if let Err(e) = destination.offer(stripe_id, &data) {
                warn!(
                    "Snapshot destination {} failed on stripe {stripe_id}: {e}",
                    destination.id()
                );
                failed.push(destination.id());
            }
        }

        for id in failed {
            info!("Dropping snapshot destination {id}");
            self.destinations.retain(|d| d.id() != id);
        }
        self.publish_destination_count();

        self.state.finish_copy(stripe_id);
        self.end_snapshot_if_unwatched();
    }

    pub fn process_request(&mut self, request: SnapshotRequest) {
        match request {
            SnapshotRequest::CopyOut { stripe_id } => self.copy_out(stripe_id),
            SnapshotRequest::AddDestination(destination) => self.add_destination(destination),
            SnapshotRequest::RemoveDestination(id) => self.remove_destination(id),
            SnapshotRequest::Freeze => {
                let generation = self.state.lock_all();
                self.frozen_at = Some(Instant::now());
                info!("Snapshot generation {generation} frozen");
                self.sweep_for_exporters();
            }
            SnapshotRequest::Shutdown => {
                info!("Snapshot worker shutting down");
                self.done = true;
            }
        }
    }

    pub fn receive_requests(&mut self, block: bool) {
        if block {
            // With stripes held for a subscriber, wake up regularly so the
            // grace period can expire even if no request arrives.
            let blocking_recv = if self.deferred.is_empty() {
                self.requests.recv().map_err(RecvTimeoutError::from)
            } else {
                self.requests.recv_timeout(Duration::from_secs(1))
            };

            match blocking_recv {
                Ok(request) => self.process_request(request),
                Err(RecvTimeoutError::Timeout) => self.expire_grace_if_needed(),
                Err(RecvTimeoutError::Disconnected) => {
                    error!("Snapshot worker request channel closed");
                    self.done = true;
                    return;
                }
            }
        }

        loop {
            match self.requests.try_recv() {
                Ok(request) => self.process_request(request),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    error!("Snapshot worker request channel disconnected");
                    self.done = true;
                    break;
                }
            }
        }
    }

    pub fn run(&mut self) {
        while !self.done {
            self.receive_requests(true);
        }
    }
}
