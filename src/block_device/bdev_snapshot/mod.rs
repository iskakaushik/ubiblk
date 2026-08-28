//! Snapshot layer.
//!
//! `bdev_snapshot` sits above `bdev_lazy` and lets a live device hand a
//! point-in-time copy of itself to snapshot destinations while it keeps taking
//! writes. See `docs/` in the fork workspace for the design.
//!
//! Snapshot state is deliberately in-memory only: a snapshot that a restart
//! interrupts is simply gone, and the fork is re-created. Nothing here touches
//! the on-disk metadata.

pub mod destination;
pub mod device;
pub mod state;
pub mod worker;

#[cfg(test)]
mod bdev_snapshot_tests;

pub use destination::{DestinationId, SnapshotDestination};
pub use device::SnapshotBlockDevice;
pub use state::SharedSnapshotState;
pub use worker::{SnapshotRequest, SnapshotWorker, MAX_DESTINATIONS};
