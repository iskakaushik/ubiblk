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
        BgWorkerRequest, PushGate, SharedMetadataState, SharedSnapshotState, SnapshotRequest,
        UbiMetadata, MAX_QUEUED_PUSHES,
    },
    stripe_server::{PushedFrame, SnapshotSubscriber, StripeServer, WireCompression},
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
/// Generous, because it is the wrong tool for spotting a fork that has gone
/// away: a fork that is merely busy must not be dropped, and a fork whose VM
/// was destroyed is caught by keepalive below instead.
const PUSH_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Keepalive settings for a push connection: a peer that stops answering is
/// noticed in about ten seconds, so prod stops pushing to a fork whose VM was
/// destroyed without having to guess from how long a write is taking.
const KEEPALIVE_IDLE_SECS: libc::c_int = 5;
const KEEPALIVE_INTERVAL_SECS: libc::c_int = 2;
const KEEPALIVE_PROBES: libc::c_int = 3;

fn set_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let fd = stream.as_raw_fd();
    let options: [(libc::c_int, libc::c_int, libc::c_int); 4] = [
        (libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1),
        (libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, KEEPALIVE_IDLE_SECS),
        (
            libc::IPPROTO_TCP,
            libc::TCP_KEEPINTVL,
            KEEPALIVE_INTERVAL_SECS,
        ),
        (libc::IPPROTO_TCP, libc::TCP_KEEPCNT, KEEPALIVE_PROBES),
    ];

    for (level, name, value) in options {
        // SAFETY: fd is owned by `stream` and outlives this call, and value is a
        // c_int as every one of these options expects.
        let result = unsafe {
            libc::setsockopt(
                fd,
                level,
                name,
                &value as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }

    Ok(())
}

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

                if let Err(e) = set_keepalive(&stream) {
                    error!("Failed to set keepalive on a snapshot connection: {e}");
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
    compression: WireCompression,
    bgworker_ch: Sender<BgWorkerRequest>,
    liveness: SharedMetadataState,
) -> Result<()> {
    let address = address.to_string();
    let gate = PushGate::new(MAX_QUEUED_PUSHES);

    thread::Builder::new()
        .name("snapshot-subscriber".to_string())
        .spawn(move || {
            // The generation of the first subscription this process made, kept
            // across reconnects so `source_live` is set with it and never again.
            let mut first_generation = None;
            loop {
                match subscribe_once(
                    &address,
                    compression,
                    &gate,
                    &bgworker_ch,
                    &liveness,
                    &mut first_generation,
                ) {
                    Ok(()) => info!("Snapshot ended; not resubscribing"),
                    Err(e) => {
                        warn!("Snapshot subscription to {address} ended: {e}");
                        thread::sleep(RECONNECT_DELAY);
                        continue;
                    }
                }
                return;
            }
        })
        .map_err(|e| {
            crate::ubiblk_error!(InvalidParameter {
                description: format!("Failed to spawn the snapshot subscriber thread: {e}"),
            })
        })?;

    Ok(())
}

/// `liveness` is where `source_live` is kept. It goes true when the first
/// subscription this process makes is up, and false on every way out of here,
/// for good: a reconnect attaches to whatever generation is live by then, and
/// the pushes missed in the gap were copied out and are refused on pull, so a
/// clean stripe dropped while the subscription was up may have nowhere to
/// come back from. `first_generation` is what remembers that first
/// subscription across reconnects.
fn subscribe_once(
    address: &str,
    compression: WireCompression,
    gate: &Arc<PushGate>,
    bgworker_ch: &Sender<BgWorkerRequest>,
    liveness: &SharedMetadataState,
    first_generation: &mut Option<u64>,
) -> Result<()> {
    let stream = TcpStream::connect(address).map_err(|e| {
        crate::ubiblk_error!(InvalidParameter {
            description: format!("Failed to connect to the snapshot server {address}: {e}"),
        })
    })?;

    let mut subscriber = SnapshotSubscriber::subscribe(Box::new(stream), compression)?;
    let generation = subscriber.generation();
    info!("Subscribed to snapshot generation {generation} on {address}");
    match *first_generation {
        None => {
            *first_generation = Some(generation);
            liveness.set_source_live(true);
        }
        Some(first) if first != generation => warn!(
            "Reconnected to snapshot generation {generation} on {address}, not {first}: \
             this is a different snapshot"
        ),
        Some(_) => {}
    }

    let result = relay_pushes(&mut subscriber, gate, bgworker_ch);
    liveness.set_source_live(false);
    result
}

/// Hand every pushed stripe to the bgworker until the snapshot ends or the
/// bgworker is gone (`Ok`), or the connection fails (`Err`).
fn relay_pushes(
    subscriber: &mut SnapshotSubscriber,
    gate: &Arc<PushGate>,
    bgworker_ch: &Sender<BgWorkerRequest>,
) -> Result<()> {
    loop {
        match subscriber.next_frame()? {
            Some(PushedFrame::Stripe { stripe_id, data }) => {
                // Waits while the worker is behind, so a fork that cannot keep
                // up stops reading rather than holding prod's writes in memory.
                let permit = gate.acquire();
                if bgworker_ch
                    .send(BgWorkerRequest::PushedStripe {
                        stripe_id: stripe_id as usize,
                        data,
                        permit,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Write,
        net::{Shutdown, TcpListener},
        sync::{
            mpsc::{channel, Receiver},
            Mutex,
        },
        thread::JoinHandle,
        time::Instant,
    };

    use crate::{
        backends::SECTOR_SIZE,
        block_device::{
            metadata_flags, shared_buffer, IoChannel, SnapshotBlockDevice, SnapshotWorker,
            SyncBlockDevice,
        },
        stripe_server::RemoteDestination,
    };

    const STRIPE_SHIFT: u8 = 3;
    const STRIPE_SECTORS: usize = 1 << STRIPE_SHIFT;
    const STRIPE_BYTES: usize = STRIPE_SECTORS * SECTOR_SIZE;
    const STRIPES: usize = 4;

    fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn run_io(channel: &mut dyn IoChannel, id: usize) {
        channel.submit().expect("submit");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if channel.poll().iter().any(|(done, ok)| {
                assert!(*ok, "request {done} failed");
                *done == id
            }) {
                return;
            }
            assert!(Instant::now() < deadline, "timed out waiting for io {id}");
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Prod with a live snapshot, listening for forks. Every accepted session
    /// is kept as a second handle to its socket, so a test can end or cut a
    /// fork's subscription from prod's side.
    struct Prod {
        address: String,
        state: SharedSnapshotState,
        device: SnapshotBlockDevice,
        sessions: Arc<Mutex<Vec<TcpStream>>>,
        _file: tempfile::NamedTempFile,
    }

    fn prod() -> Prod {
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file()
            .write_all(&vec![0xA5; STRIPES * STRIPE_BYTES])
            .unwrap();
        file.as_file().sync_all().unwrap();
        let disk = SyncBlockDevice::new(file.path().to_path_buf(), false, false, false).unwrap();

        let (snapshot_ch, snapshot_requests) = channel();
        let device = SnapshotBlockDevice::new(
            BlockDevice::clone(disk.as_ref()),
            STRIPE_SHIFT,
            snapshot_ch.clone(),
        );
        let state = device.state();
        let mut worker = SnapshotWorker::new(
            BlockDevice::clone(disk.as_ref()).as_ref(),
            state.clone(),
            snapshot_requests,
        )
        .unwrap();
        thread::spawn(move || worker.run());

        let mut metadata = UbiMetadata::new(STRIPE_SHIFT, STRIPES, 0);
        for stripe_id in 0..STRIPES {
            metadata.stripe_headers[stripe_id] |= metadata_flags::WRITTEN;
        }
        let server = Arc::new(
            StripeServer::new(
                Arc::from(BlockDevice::clone(disk.as_ref())),
                Arc::from(metadata),
                None,
            )
            .with_snapshot(snapshot_ch, state.clone()),
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let sessions = Arc::new(Mutex::new(Vec::new()));
        let accepted = sessions.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                accepted.lock().unwrap().push(stream.try_clone().unwrap());
                let server = server.clone();
                thread::spawn(move || {
                    let mut session = server.start_session(Box::new(stream)).unwrap();
                    session.handle_requests();
                });
            }
        });

        let prod = Prod {
            address,
            state,
            device,
            sessions,
            _file: file,
        };
        assert_eq!(prod.freeze(), 1);
        prod
    }

    impl Prod {
        /// The freeze the `snapshot` RPC performs; returns the new generation.
        fn freeze(&self) -> u64 {
            self.state.begin_drain();
            wait_until("io to drain", || self.state.drained());
            let generation = self.state.lock_all();
            self.state.resume();
            generation
        }

        fn session(&self, index: usize) -> TcpStream {
            wait_until("the fork's session to be accepted", || {
                self.sessions.lock().unwrap().len() > index
            });
            self.sessions.lock().unwrap()[index].try_clone().unwrap()
        }

        /// What prod sends a fork when the snapshot is over.
        fn end(&self, index: usize) {
            RemoteDestination::new(0, Box::new(self.session(index)), WireCompression::None)
                .send_end();
        }

        /// The connection drops without a word.
        fn cut(&self, index: usize) {
            self.session(index).shutdown(Shutdown::Both).unwrap();
        }

        /// Overwrite stripe 0, which pushes its pre-write content to every
        /// fork before the write goes through.
        fn overwrite_stripe_0(&self) {
            let mut channel = self.device.create_channel().unwrap();
            let new_content = shared_buffer(STRIPE_BYTES);
            new_content.borrow_mut().as_mut_slice().fill(0xFF);
            channel.add_write(0, STRIPE_SECTORS as u32, new_content, 1);
            run_io(channel.as_mut(), 1);
        }
    }

    fn liveness() -> SharedMetadataState {
        SharedMetadataState::new(&UbiMetadata::new(STRIPE_SHIFT, STRIPES, 0))
    }

    /// One `subscribe_once`, as the subscriber thread would run it, returning
    /// what it returned and the first generation it now remembers.
    fn subscribe(
        prod: &Prod,
        liveness: SharedMetadataState,
        bgworker_ch: Sender<BgWorkerRequest>,
        first_generation: Option<u64>,
    ) -> JoinHandle<(Result<()>, Option<u64>)> {
        let address = prod.address.clone();
        thread::spawn(move || {
            let gate = PushGate::new(MAX_QUEUED_PUSHES);
            let mut first_generation = first_generation;
            let result = subscribe_once(
                &address,
                WireCompression::None,
                &gate,
                &bgworker_ch,
                &liveness,
                &mut first_generation,
            );
            (result, first_generation)
        })
    }

    fn pushed_stripe(pushes: &Receiver<BgWorkerRequest>) -> usize {
        match pushes.recv_timeout(Duration::from_secs(10)) {
            Ok(BgWorkerRequest::PushedStripe { stripe_id, .. }) => stripe_id,
            Ok(_) => panic!("the subscriber sent something other than a pushed stripe"),
            Err(e) => panic!("no pushed stripe reached the bgworker channel: {e}"),
        }
    }

    #[test]
    fn source_live_true_after_first_subscribe() {
        let prod = prod();
        let liveness = liveness();
        let (bgworker_ch, _pushes) = channel();
        assert!(
            !liveness.source_live(),
            "nothing is live before subscribing"
        );

        let fork = subscribe(&prod, liveness.clone(), bgworker_ch, None);
        wait_until("source_live", || liveness.source_live());
        wait_until("the fork to register as a destination", || {
            prod.state.destination_count() == 1
        });

        prod.end(0);
        let (result, first_generation) = fork.join().unwrap();
        assert!(result.is_ok());
        assert_eq!(
            first_generation,
            Some(1),
            "the first subscription is remembered"
        );
    }

    #[test]
    fn source_live_false_after_end() {
        let prod = prod();
        let liveness = liveness();
        let (bgworker_ch, pushes) = channel();
        let fork = subscribe(&prod, liveness.clone(), bgworker_ch, None);
        wait_until("source_live", || liveness.source_live());

        // A push flows while the subscription is up, and it stays live.
        prod.overwrite_stripe_0();
        assert_eq!(pushed_stripe(&pushes), 0);
        assert!(liveness.source_live());

        prod.end(0);
        let (result, _) = fork.join().unwrap();
        assert!(result.is_ok(), "End is a clean exit: {result:?}");
        assert!(!liveness.source_live());
    }

    /// A dropped connection is an error, so the subscriber reconnects; but the
    /// pushes it missed in between were copied out and cannot be pulled, and
    /// the reconnect attaches to whatever snapshot is live by then, so it must
    /// never bring `source_live` back.
    #[test]
    fn source_live_false_after_disconnect_and_stays_false_after_reconnect() {
        let prod = prod();
        let liveness = liveness();
        let (bgworker_ch, pushes) = channel();
        let fork = subscribe(&prod, liveness.clone(), bgworker_ch.clone(), None);
        wait_until("source_live", || liveness.source_live());

        prod.cut(0);
        let (result, first_generation) = fork.join().unwrap();
        assert!(
            result.is_err(),
            "a cut connection is what makes the thread reconnect"
        );
        assert!(!liveness.source_live());
        assert_eq!(first_generation, Some(1));

        // Prod has meanwhile released that snapshot and taken another, so the
        // reconnect lands on a different generation.
        prod.state.end_snapshot();
        assert_eq!(prod.freeze(), 2);

        let fork = subscribe(&prod, liveness.clone(), bgworker_ch, first_generation);
        wait_until("the fork to register again", || {
            prod.state.destination_count() == 2
        });
        // The relayed push proves the second subscription is up and reading,
        // and source_live has not come back with it.
        prod.overwrite_stripe_0();
        assert_eq!(pushed_stripe(&pushes), 0);
        assert!(
            !liveness.source_live(),
            "a reconnect never sets source_live"
        );

        prod.end(1);
        let (result, first_generation) = fork.join().unwrap();
        assert!(result.is_ok());
        assert!(!liveness.source_live());
        assert_eq!(
            first_generation,
            Some(1),
            "the first subscription stays the one remembered"
        );
    }
}
