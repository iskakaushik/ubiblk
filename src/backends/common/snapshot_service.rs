//! The two threads that make forking work in a real deployment.
//!
//! On the prod side, `spawn_snapshot_server` listens for forks: a fork
//! subscribes to the current snapshot on one session and pulls cold stripes on
//! another. On the fork side, `spawn_snapshot_subscriber` keeps a subscription
//! open and hands every pushed stripe to the bgworker, which writes it to the
//! fork's disk the same way a fetched stripe is written.

use std::{
    net::{TcpListener, TcpStream},
    sync::{mpsc::Sender, Arc},
    thread,
    time::Duration,
};

use log::{error, info, warn};

use crate::{
    block_device::BlockDevice,
    block_device::{
        BgWorkerRequest, SharedMetadataState, SharedSnapshotState, SnapshotRequest, UbiMetadata,
    },
    stripe_server::{PushedFrame, SnapshotSubscriber, StripeServer},
    Result,
};

/// How long to wait before reconnecting a dropped subscription.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// How long a push may take to reach a fork before that fork is considered gone.
///
/// Without this a fork whose VM disappears leaves a half-open socket: writes to
/// it block until TCP gives up, minutes later, and the snapshot worker is stuck
/// in that write. Everything else then waits on the worker — the copy-out never
/// finishes, so prod's write stays blocked, and a new fork's subscription is not
/// even processed. A fork must never be able to do that to prod.
const PUSH_WRITE_TIMEOUT: Duration = Duration::from_secs(15);

/// Serve snapshots of this device to forks.
pub fn spawn_snapshot_server(
    address: &str,
    stripe_device: Box<dyn BlockDevice>,
    metadata: Arc<UbiMetadata>,
    live_state: SharedMetadataState,
    snapshot_ch: Sender<SnapshotRequest>,
    snapshot_state: SharedSnapshotState,
) -> Result<()> {
    let listener = TcpListener::bind(address).map_err(|e| {
        crate::ubiblk_error!(InvalidParameter {
            description: format!("Failed to listen for snapshot clients on {address}: {e}"),
        })
    })?;

    info!("Serving snapshots on {address}");

    let server = Arc::new(
        StripeServer::new(Arc::from(stripe_device), metadata, None)
            .with_live_state(live_state)
            .with_snapshot(snapshot_ch, snapshot_state),
    );

    thread::Builder::new()
        .name("snapshot-server".to_string())
        .spawn(move || {
            for stream in listener.incoming() {
                let stream = match stream {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Failed to accept a snapshot client: {e}");
                        continue;
                    }
                };

                if let Err(e) = stream.set_write_timeout(Some(PUSH_WRITE_TIMEOUT)) {
                    error!("Failed to set the push write timeout: {e}");
                    continue;
                }

                let server = server.clone();
                // One thread per fork. A session that subscribes hands its
                // stream to the snapshot worker and this thread ends.
                let spawned = thread::Builder::new()
                    .name("snapshot-session".to_string())
                    .spawn(move || match server.start_session(Box::new(stream)) {
                        Ok(mut session) => session.handle_requests(),
                        Err(e) => error!("Failed to start a snapshot session: {e}"),
                    });

                if let Err(e) = spawned {
                    error!("Failed to spawn a snapshot session thread: {e}");
                }
            }
        })
        .map_err(|e| {
            crate::ubiblk_error!(InvalidParameter {
                description: format!("Failed to spawn the snapshot server thread: {e}"),
            })
        })?;

    Ok(())
}

/// Subscribe to the prod device's snapshot and feed what it pushes into the
/// bgworker.
pub fn spawn_snapshot_subscriber(
    address: &str,
    bgworker_ch: Sender<BgWorkerRequest>,
) -> Result<()> {
    let address = address.to_string();

    thread::Builder::new()
        .name("snapshot-subscriber".to_string())
        .spawn(move || loop {
            match subscribe_once(&address, &bgworker_ch) {
                Ok(()) => info!("Snapshot ended; not resubscribing"),
                Err(e) => {
                    warn!("Snapshot subscription to {address} ended: {e}");
                    thread::sleep(RECONNECT_DELAY);
                    continue;
                }
            }
            return;
        })
        .map_err(|e| {
            crate::ubiblk_error!(InvalidParameter {
                description: format!("Failed to spawn the snapshot subscriber thread: {e}"),
            })
        })?;

    Ok(())
}

fn subscribe_once(address: &str, bgworker_ch: &Sender<BgWorkerRequest>) -> Result<()> {
    let stream = TcpStream::connect(address).map_err(|e| {
        crate::ubiblk_error!(InvalidParameter {
            description: format!("Failed to connect to the snapshot server {address}: {e}"),
        })
    })?;

    let mut subscriber = SnapshotSubscriber::subscribe(Box::new(stream))?;
    info!(
        "Subscribed to snapshot generation {} on {address}",
        subscriber.generation()
    );

    loop {
        match subscriber.next_frame()? {
            Some(PushedFrame::Stripe { stripe_id, data }) => {
                if bgworker_ch
                    .send(BgWorkerRequest::PushedStripe {
                        stripe_id: stripe_id as usize,
                        data,
                    })
                    .is_err()
                {
                    // The bgworker is gone, so this device is shutting down.
                    return Ok(());
                }
            }
            // Prod released the snapshot. Anything not pushed by now is
            // still available to pull, so there is nothing to wait for.
            Some(PushedFrame::End) => return Ok(()),
            None => {
                return Err(crate::ubiblk_error!(InvalidParameter {
                    description: "snapshot server closed the connection".to_string(),
                }))
            }
        }
    }
}
