//! End-to-end test of a fork: a live device is snapshotted, a fork subscribes
//! over TCP, prod overwrites a stripe, and the fork sees the pre-write content
//! pushed to it while cold stripes still come over the pull path.

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    sync::{mpsc::channel, Arc},
    thread,
    time::{Duration, Instant},
};

use crate::{
    backends::SECTOR_SIZE,
    block_device::{
        metadata_flags, shared_buffer, BlockDevice, IoChannel, SharedSnapshotState,
        SnapshotBlockDevice, SnapshotWorker, SyncBlockDevice, UbiMetadata,
    },
    stripe_server::{
        PushedFrame, RemoteStripeProvider, SnapshotSubscriber, StripeServer, StripeServerClient,
    },
};

const STRIPE_SHIFT: u8 = 3;
const STRIPE_SECTORS: usize = 1 << STRIPE_SHIFT;
const STRIPE_BYTES: usize = STRIPE_SECTORS * SECTOR_SIZE;
const STRIPES: usize = 4;

/// Byte every sector of a stripe is filled with before the snapshot.
fn pre_write_byte(stripe_id: usize) -> u8 {
    0xA0 + stripe_id as u8
}

fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

/// The freeze the `snapshot` RPC performs: hold I/O, wait for it to drain, mark
/// every stripe as owed to the snapshot, resume.
fn freeze(state: &SharedSnapshotState) -> u64 {
    state.begin_drain();
    wait_until("io to drain", || state.drained());
    let generation = state.lock_all();
    state.resume();
    generation
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

#[test]
fn a_fork_sees_the_snapshot_while_prod_keeps_writing() {
    let file = tempfile::NamedTempFile::new().unwrap();
    for stripe_id in 0..STRIPES {
        file.as_file()
            .write_all(&vec![pre_write_byte(stripe_id); STRIPE_BYTES])
            .unwrap();
    }
    file.as_file().sync_all().unwrap();

    let disk = SyncBlockDevice::new(file.path().to_path_buf(), false, false, false).unwrap();

    // Prod stack: the snapshot layer over the disk, with its worker.
    let (snapshot_ch, snapshot_requests) = channel();
    let snapshot_device = SnapshotBlockDevice::new(
        BlockDevice::clone(disk.as_ref()),
        STRIPE_SHIFT,
        snapshot_ch.clone(),
    );
    let state = snapshot_device.state();
    let mut worker = SnapshotWorker::new(
        BlockDevice::clone(disk.as_ref()).as_ref(),
        state.clone(),
        snapshot_requests,
    )
    .unwrap();
    thread::spawn(move || worker.run());

    // Prod's disk owns its content: no upstream source, every stripe written.
    // (Passing a non-zero image stripe count here sets HAS_SOURCE without
    // FETCHED, and the server then refuses to serve the stripe — the same trap
    // the remote-stripe runbook documents.)
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
        .with_snapshot(snapshot_ch.clone(), state.clone()),
    );

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let server = server.clone();
            thread::spawn(move || {
                let mut session = server.start_session(Box::new(stream)).unwrap();
                session.handle_requests();
            });
        }
    });

    let mut prod = snapshot_device.create_channel().unwrap();

    // --- the fork happens here ---
    let generation = freeze(&state);
    assert_eq!(generation, 1);

    let mut subscriber =
        SnapshotSubscriber::subscribe(Box::new(TcpStream::connect(address).unwrap())).unwrap();
    assert_eq!(subscriber.generation(), generation);
    wait_until("the fork to register as a destination", || {
        state.destination_count() == 1
    });

    // Prod overwrites stripe 0. The write is held until the pre-write content
    // has been pushed to the fork.
    let new_content = shared_buffer(STRIPE_BYTES);
    new_content.borrow_mut().as_mut_slice().fill(0xFF);
    prod.add_write(0, STRIPE_SECTORS as u32, new_content, 1);
    run_io(prod.as_mut(), 1);

    // The fork got the stripe as it was before that write.
    match subscriber.next_frame().unwrap() {
        Some(PushedFrame::Stripe { stripe_id, data }) => {
            assert_eq!(stripe_id, 0);
            assert_eq!(data.len(), STRIPE_BYTES);
            assert!(
                data.iter().all(|byte| *byte == pre_write_byte(0)),
                "the fork must see the pre-write content, not prod's new write"
            );
        }
        other => panic!("expected a pushed stripe, got {other:?}"),
    }

    // Prod's disk really did move on.
    let mut check = disk.create_channel().unwrap();
    let readback = shared_buffer(STRIPE_BYTES);
    check.add_read(0, STRIPE_SECTORS as u32, readback.clone(), 2);
    run_io(check.as_mut(), 2);
    assert!(readback.borrow().as_slice()[..STRIPE_BYTES]
        .iter()
        .all(|byte| *byte == 0xFF));

    // A cold stripe nobody has overwritten still comes over the pull path.
    let mut puller = StripeServerClient::new(Box::new(TcpStream::connect(address).unwrap()));
    puller.fetch_metadata().unwrap();
    let cold = puller.fetch_stripe(2).unwrap();
    assert_eq!(cold.len(), STRIPE_BYTES);
    assert!(cold.iter().all(|byte| *byte == pre_write_byte(2)));
}

#[test]
fn subscribing_without_a_snapshot_is_refused() {
    let file = tempfile::NamedTempFile::new().unwrap();
    file.as_file()
        .write_all(&vec![0u8; STRIPES * STRIPE_BYTES])
        .unwrap();
    file.as_file().sync_all().unwrap();

    let disk = SyncBlockDevice::new(file.path().to_path_buf(), false, false, false).unwrap();
    let (snapshot_ch, _requests) = channel();
    let snapshot_device = SnapshotBlockDevice::new(
        BlockDevice::clone(disk.as_ref()),
        STRIPE_SHIFT,
        snapshot_ch.clone(),
    );
    let state = snapshot_device.state();

    let metadata = UbiMetadata::new(STRIPE_SHIFT, STRIPES, 0);
    let server = Arc::new(
        StripeServer::new(
            Arc::from(BlockDevice::clone(disk.as_ref())),
            Arc::from(metadata),
            None,
        )
        .with_snapshot(snapshot_ch, state),
    );

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        if let Some(Ok(stream)) = listener.incoming().next() {
            let mut session = server.start_session(Box::new(stream)).unwrap();
            session.handle_requests();
        }
    });

    // No freeze has happened, so there is nothing to subscribe to.
    let result = SnapshotSubscriber::subscribe(Box::new(TcpStream::connect(address).unwrap()));
    assert!(result.is_err());
}
