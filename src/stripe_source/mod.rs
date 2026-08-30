use crate::{block_device::SharedBuffer, Result};

/// A source from which stripes can be fetched.
pub trait StripeSource {
    /// Request to fetch a stripe.
    fn request(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()>;
    /// Request a stripe a guest is waiting for. A source that can keep bulk
    /// work out of its way should; one that cannot treats it as any other
    /// request.
    fn request_demand(&mut self, stripe_id: usize, buffer: SharedBuffer) -> Result<()> {
        self.request(stripe_id, buffer)
    }
    /// Poll for completed stripe fetch requests.
    fn poll(&mut self) -> Vec<(usize, bool)>;
    /// Check if there are any pending requests.
    fn busy(&self) -> bool;
    /// Get the sector count of the stripe source.
    fn sector_count(&self) -> u64;
    /// Does the stripe source have the given stripe?
    fn has_stripe(&self, stripe_id: usize) -> bool;
    /// How many requests this source can usefully have outstanding. A remote
    /// source fetches over several connections at once and is idle unless the
    /// fetcher keeps them all fed.
    fn max_concurrent_requests(&self) -> usize {
        1
    }
}

mod archive;
mod bdev;
mod builder;
mod flaky;
mod remote;
pub mod wait;
pub use archive::ArchiveStripeSource;
pub use bdev::BlockDeviceStripeSource;
pub use builder::StripeSourceBuilder;
pub use flaky::FlakyStripeSource;
pub use remote::{ConnectFn, RemoteStripeSource};
