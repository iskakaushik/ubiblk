use crate::Result;

pub type DestinationId = u64;

/// Somewhere a snapshot's stripes are sent.
///
/// The only real implementation to start with is a remote fork over the stripe
/// protocol, but the layer never assumes that: a local file destination (which
/// would decouple prod write latency from the network) plugs in the same way.
///
/// A destination that fails or goes away is dropped from the set. A fork must
/// never be able to stall prod writes, so implementations should report
/// themselves dead rather than block.
pub trait SnapshotDestination: Send {
    fn id(&self) -> DestinationId;

    /// Hand over the pre-write content of a stripe. Returning an error drops
    /// this destination from the snapshot.
    fn offer(&mut self, stripe_id: usize, data: &[u8]) -> Result<()>;

    /// False once the peer is gone; checked before and after every offer.
    fn is_alive(&self) -> bool;

    /// True for a destination that wants the whole snapshot rather than only
    /// the stripes prod happens to overwrite. A remote fork pulls what it needs
    /// on demand and answers false; an exporter writing the snapshot somewhere
    /// (an S3 archive, say) answers true, and the worker sweeps every stripe
    /// through it.
    fn wants_all_stripes(&self) -> bool {
        false
    }

    /// The snapshot is over: no more stripes are coming. A destination that
    /// writes a container finalises it here.
    fn finish(&mut self) {}
}

#[cfg(test)]
pub mod test_destination {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };

    /// Stripes handed to a test destination, in offer order.
    pub type OfferLog = Arc<Mutex<Vec<(usize, Vec<u8>)>>>;

    /// An in-process destination that records what it was offered, so tests can
    /// drive the push path without a socket.
    #[derive(Clone)]
    pub struct TestDestination {
        id: DestinationId,
        pub offered: OfferLog,
        pub alive: Arc<AtomicBool>,
        pub fail_next_offer: Arc<AtomicBool>,
        pub wants_everything: Arc<AtomicBool>,
        pub finished: Arc<AtomicBool>,
    }

    impl TestDestination {
        pub fn new(id: DestinationId) -> Self {
            Self {
                id,
                offered: Arc::new(Mutex::new(Vec::new())),
                alive: Arc::new(AtomicBool::new(true)),
                fail_next_offer: Arc::new(AtomicBool::new(false)),
                wants_everything: Arc::new(AtomicBool::new(false)),
                finished: Arc::new(AtomicBool::new(false)),
            }
        }

        pub fn offered_stripes(&self) -> Vec<usize> {
            self.offered
                .lock()
                .unwrap()
                .iter()
                .map(|(stripe_id, _)| *stripe_id)
                .collect()
        }
    }

    impl SnapshotDestination for TestDestination {
        fn id(&self) -> DestinationId {
            self.id
        }

        fn offer(&mut self, stripe_id: usize, data: &[u8]) -> Result<()> {
            if self.fail_next_offer.swap(false, Ordering::SeqCst) {
                return Err(crate::ubiblk_error!(IoError {
                    source: std::io::Error::other("injected offer failure"),
                }));
            }
            self.offered
                .lock()
                .unwrap()
                .push((stripe_id, data.to_vec()));
            Ok(())
        }

        fn is_alive(&self) -> bool {
            self.alive.load(Ordering::SeqCst)
        }

        fn wants_all_stripes(&self) -> bool {
            self.wants_everything.load(Ordering::SeqCst)
        }

        fn finish(&mut self) {
            self.finished.store(true, Ordering::SeqCst);
        }
    }
}
