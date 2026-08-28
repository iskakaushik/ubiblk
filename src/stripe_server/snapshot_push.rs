//! The push half of snapshot serving.
//!
//! A fork opens a second session and sends `SUBSCRIBE_SNAPSHOT_CMD`. That
//! session stops being request/response and becomes a one-way stream of stripes
//! the prod side is about to overwrite: prod hands over the pre-write content
//! before the write is allowed through. Cold stripes the fork has not been
//! pushed are still pulled with `READ_STRIPE_CMD` on its other session, so an
//! idle fork cannot stall prod writes and a busy fork does not wait for prod to
//! overwrite something before it can read it.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use log::{info, warn};

use crate::{
    block_device::{DestinationId, SnapshotDestination},
    Result,
};

use super::{DynStream, PUSH_STRIPE_FRAME, SNAPSHOT_END_FRAME};

/// Server side: a fork that subscribed, seen as a snapshot destination.
pub struct RemoteDestination {
    id: DestinationId,
    stream: DynStream,
    alive: Arc<AtomicBool>,
}

impl RemoteDestination {
    pub fn new(id: DestinationId, stream: DynStream) -> Self {
        Self {
            id,
            stream,
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Shared with whoever wants to hang up on this destination from another
    /// thread, e.g. when the snapshot ends.
    pub fn alive_flag(&self) -> Arc<AtomicBool> {
        self.alive.clone()
    }

    fn mark_dead(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
    }

    /// Tell the fork the snapshot is over. Best effort: the fork may already be
    /// gone, which is not an error worth reporting.
    pub fn send_end(&mut self) {
        use std::io::Write;
        if self.stream.write_all(&[SNAPSHOT_END_FRAME]).is_err() || self.stream.flush().is_err() {
            info!(
                "Snapshot destination {} closed before the end frame",
                self.id
            );
        }
        self.mark_dead();
    }
}

impl SnapshotDestination for RemoteDestination {
    fn id(&self) -> DestinationId {
        self.id
    }

    fn offer(&mut self, stripe_id: usize, data: &[u8]) -> Result<()> {
        use std::io::Write;

        let result = (|| -> std::io::Result<()> {
            self.stream.write_all(&[PUSH_STRIPE_FRAME])?;
            self.stream.write_all(&(stripe_id as u64).to_le_bytes())?;
            self.stream.write_all(&(data.len() as u64).to_le_bytes())?;
            self.stream.write_all(data)?;
            self.stream.flush()
        })();

        if let Err(source) = result {
            // A write error means the fork is gone or wedged. Prod writes are
            // waiting on this offer, so the destination dies rather than the
            // write retrying.
            warn!("Snapshot destination {} failed: {source}", self.id);
            self.mark_dead();
            return Err(crate::ubiblk_error!(IoError { source: source }));
        }

        Ok(())
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }
}

/// What a subscribed fork reads off its push session.
#[derive(Debug, PartialEq, Eq)]
pub enum PushedFrame {
    /// Pre-write content of a stripe as of the snapshot.
    Stripe { stripe_id: u64, data: Vec<u8> },
    /// The snapshot ended: prod released it, or the last destination went away.
    End,
}

/// Client side: the fork's end of a push session.
pub struct SnapshotSubscriber {
    stream: DynStream,
    generation: u64,
}

impl SnapshotSubscriber {
    /// Send the subscribe command and read the acknowledgement. The caller
    /// supplies a stream that has already been connected (and PSK-wrapped, if
    /// the deployment uses one), exactly like the pull client.
    pub fn subscribe(mut stream: DynStream) -> Result<Self> {
        use std::io::{Read, Write};

        stream.write_all(&[super::SUBSCRIBE_SNAPSHOT_CMD])?;
        stream.flush()?;

        let mut status = [0u8; 1];
        stream.read_exact(&mut status)?;
        if status[0] != super::STATUS_OK {
            return Err(crate::ubiblk_error!(IoError {
                source: std::io::Error::other(format!(
                    "snapshot subscribe rejected with status {}",
                    status[0]
                )),
            }));
        }

        let mut generation_bytes = [0u8; 8];
        stream.read_exact(&mut generation_bytes)?;
        let generation = u64::from_le_bytes(generation_bytes);

        Ok(Self { stream, generation })
    }

    /// The snapshot generation this subscription is attached to. A fork that
    /// reconnects onto a different generation is looking at a different
    /// snapshot and has to start over.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Block until the next frame arrives. Returns `None` when the peer closes
    /// the connection.
    pub fn next_frame(&mut self) -> Result<Option<PushedFrame>> {
        use std::io::{ErrorKind, Read};

        let mut frame = [0u8; 1];
        match self.stream.read_exact(&mut frame) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(source) => return Err(crate::ubiblk_error!(IoError { source: source })),
        }

        match frame[0] {
            PUSH_STRIPE_FRAME => {
                let mut stripe_id_bytes = [0u8; 8];
                self.stream.read_exact(&mut stripe_id_bytes)?;
                let mut len_bytes = [0u8; 8];
                self.stream.read_exact(&mut len_bytes)?;

                let len = u64::from_le_bytes(len_bytes) as usize;
                let mut data = vec![0u8; len];
                self.stream.read_exact(&mut data)?;

                Ok(Some(PushedFrame::Stripe {
                    stripe_id: u64::from_le_bytes(stripe_id_bytes),
                    data,
                }))
            }
            SNAPSHOT_END_FRAME => Ok(Some(PushedFrame::End)),
            other => Err(crate::ubiblk_error!(IoError {
                source: std::io::Error::other(format!("unexpected snapshot frame {other}")),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    fn pair() -> (DynStream, DynStream) {
        let (a, b) = UnixStream::pair().unwrap();
        (Box::new(a), Box::new(b))
    }

    #[test]
    fn pushed_stripes_arrive_in_order() {
        let (server, client) = pair();
        let mut destination = RemoteDestination::new(1, server);

        destination.offer(7, &[0xAA; 16]).unwrap();
        destination.offer(9, &[0xBB; 16]).unwrap();
        destination.send_end();

        let mut subscriber = SnapshotSubscriber {
            stream: client,
            generation: 1,
        };

        assert_eq!(
            subscriber.next_frame().unwrap(),
            Some(PushedFrame::Stripe {
                stripe_id: 7,
                data: vec![0xAA; 16]
            })
        );
        assert_eq!(
            subscriber.next_frame().unwrap(),
            Some(PushedFrame::Stripe {
                stripe_id: 9,
                data: vec![0xBB; 16]
            })
        );
        assert_eq!(subscriber.next_frame().unwrap(), Some(PushedFrame::End));
    }

    #[test]
    fn a_hung_up_fork_kills_the_destination_instead_of_blocking() {
        let (server, client) = pair();
        let mut destination = RemoteDestination::new(2, server);
        drop(client);

        // The first offer may or may not fail depending on socket buffering,
        // but the destination must end up dead rather than looping.
        let mut died = false;
        for stripe_id in 0..64 {
            if destination.offer(stripe_id, &[0xCC; 4096]).is_err() {
                died = true;
                break;
            }
        }

        assert!(died, "writing to a closed peer must fail");
        assert!(!destination.is_alive());
    }

    #[test]
    fn the_end_of_the_stream_reads_as_no_frame() {
        let (server, client) = pair();
        drop(server);

        let mut subscriber = SnapshotSubscriber {
            stream: client,
            generation: 3,
        };
        assert_eq!(subscriber.next_frame().unwrap(), None);
    }

    #[test]
    fn an_unknown_frame_is_an_error_not_a_silent_skip() {
        let (mut server, client) = pair();
        {
            use std::io::Write;
            server.write_all(&[0x7F]).unwrap();
            server.flush().unwrap();
        }

        let mut subscriber = SnapshotSubscriber {
            stream: client,
            generation: 1,
        };
        assert!(subscriber.next_frame().is_err());
    }
}
