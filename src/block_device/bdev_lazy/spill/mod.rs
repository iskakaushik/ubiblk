//! Spilling a fork's overlay to an object store once the local disk is full.
//!
//! The local `device.raw` is a cache with a ceiling: clean stripes the live
//! snapshot can serve again are dropped, everything else is PUT to the store
//! and then punched out of the file, and reads of an evicted stripe come back
//! through the fetch path from the store or the replica.

pub mod codec;
pub mod evictor;
pub mod punch;

#[cfg(test)]
mod evictor_tests;

use std::sync::Arc;

use crate::{archive::ArchiveStore, Result};

pub use codec::{
    SpillCodec, SpillObjectHeader, SPILL_HEADER_LEN, SPILL_MAGIC, SPILL_OBJECT_VERSION,
};
pub use evictor::{Evictor, EvictorConfig, FetchDisposition, PushDisposition};
#[cfg(test)]
pub use punch::RecordingPuncher;
pub use punch::{FilePuncher, HolePuncher};

/// Builds a fresh store for one owner; the argument is the worker thread count.
pub type StoreFactory = dyn Fn(usize) -> Result<Box<dyn ArchiveStore>> + Send + Sync;

/// Builds the puncher the evictor runs against, in place of `FilePuncher`.
#[cfg(test)]
pub type PuncherFactory = dyn Fn() -> Box<dyn HolePuncher> + Send + Sync;

/// Everything the bgworker needs to run an evictor, built by the backend from
/// `SpillSection` and handed through `BgWorkerConfig`. Clone + Send + Sync so
/// the ingest pool's `StripeSourceBuilder` can carry it.
#[derive(Clone)]
pub struct SpillRuntime {
    /// The evictor's limits and paths, derived from `[spill]` and the device.
    pub cfg: EvictorConfig,
    /// Prefix of every object this device writes to the store.
    pub device_id: String,
    /// Builds a fresh `ArchiveStore` per caller: each fetcher owns a GET store,
    /// the evictor owns a PUT store, so demand GETs never queue behind PUTs.
    /// None means clean-only.
    pub store_factory: Option<Arc<StoreFactory>>,
    /// Transforms stripe bytes to object bytes and back; the fetchers and the
    /// evictor each clone it.
    pub codec: SpillCodec,
    /// Test seam: `build_bgworker` consults it before `FilePuncher::open`, so
    /// a test can hand the evictor a `RecordingPuncher` and observe the
    /// startup punch pass without a real file. None means open the data path.
    #[cfg(test)]
    pub puncher_factory: Option<Arc<PuncherFactory>>,
}
