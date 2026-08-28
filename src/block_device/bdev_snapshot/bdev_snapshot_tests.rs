use crate::{
    backends::SECTOR_SIZE,
    block_device::{bdev_test::TestBlockDevice, shared_buffer, BlockDevice},
};

use super::{
    device::SnapshotBlockDevice,
    state::{SharedSnapshotState, COPIED, FREE, LOCKED, RUNNING},
};

const STRIPE_SHIFT: u8 = 3; // 8 sectors per stripe
const STRIPE_SECTORS: u64 = 1 << STRIPE_SHIFT;

fn setup(stripes: u64) -> (SnapshotBlockDevice, SharedSnapshotState) {
    let size = stripes * STRIPE_SECTORS * SECTOR_SIZE as u64;
    let device = SnapshotBlockDevice::new(Box::new(TestBlockDevice::new(size)), STRIPE_SHIFT);
    let state = device.state();
    (device, state)
}

fn buffer_of(byte: u8, sectors: u32) -> crate::block_device::SharedBuffer {
    let buf = shared_buffer(sectors as usize * SECTOR_SIZE);
    buf.borrow_mut().as_mut_slice().fill(byte);
    buf
}

#[test]
fn passes_io_through_when_no_snapshot_is_live() {
    let (device, state) = setup(4);
    let mut channel = device.create_channel().unwrap();

    assert_eq!(state.stripe_state(0), FREE);

    channel.add_write(0, 1, buffer_of(0xAB, 1), 1);
    channel.submit().unwrap();
    assert_eq!(channel.poll(), vec![(1, true)]);

    let read_buf = buffer_of(0, 1);
    channel.add_read(0, 1, read_buf.clone(), 2);
    channel.submit().unwrap();
    assert_eq!(channel.poll(), vec![(2, true)]);
    assert_eq!(read_buf.borrow().as_slice()[0], 0xAB);
}

#[test]
fn write_to_locked_stripe_waits_for_the_copy_out() {
    let (device, state) = setup(4);
    let mut channel = device.create_channel().unwrap();

    state.lock_all();
    assert_eq!(state.stripe_state(0), LOCKED);

    channel.add_write(0, 1, buffer_of(0xCD, 1), 1);
    channel.submit().unwrap();
    // The write is held: the snapshot still needs this stripe's old content.
    assert!(channel.poll().is_empty());
    assert!(channel.busy());

    // A stripe the snapshot does not need any more lets writes through.
    assert!(state.begin_copy(0));
    state.finish_copy(0);
    assert_eq!(state.stripe_state(0), COPIED);

    assert_eq!(channel.poll(), vec![(1, true)]);
    assert!(!channel.busy());
}

#[test]
fn writes_to_other_stripes_are_not_blocked_by_a_locked_one() {
    let (device, state) = setup(4);
    let mut channel = device.create_channel().unwrap();

    state.lock_all();
    state.finish_copy(1);

    channel.add_write(STRIPE_SECTORS, 1, buffer_of(0x11, 1), 1);
    channel.submit().unwrap();
    assert_eq!(channel.poll(), vec![(1, true)]);
}

#[test]
fn reads_are_never_blocked_by_a_locked_stripe() {
    let (device, state) = setup(4);
    let mut channel = device.create_channel().unwrap();

    state.lock_all();

    channel.add_read(0, 1, buffer_of(0, 1), 1);
    channel.submit().unwrap();
    assert_eq!(channel.poll(), vec![(1, true)]);
}

#[test]
fn only_one_copy_out_is_started_per_stripe() {
    let (_device, state) = setup(2);
    state.lock_all();

    assert!(state.begin_copy(0), "first claim wins");
    assert!(
        !state.begin_copy(0),
        "second claim finds it already copying"
    );
}

#[test]
fn draining_holds_new_io_until_the_freeze_completes() {
    let (device, state) = setup(4);
    let mut channel = device.create_channel().unwrap();
    assert_eq!(state.channel_count(), 1);

    state.begin_drain();

    channel.add_read(0, 1, buffer_of(0, 1), 1);
    channel.add_write(0, 1, buffer_of(0x22, 1), 2);
    channel.submit().unwrap();

    // Nothing reaches the device below while draining, and with no requests in
    // flight the channel reports itself drained.
    assert!(channel.poll().is_empty());
    assert!(state.all_channels_drained());

    // The freeze happens here; afterwards the layer runs again and replays.
    state.lock_all();
    state.finish_copy(0);
    state.set_mode(RUNNING);

    let mut completed = channel.poll();
    completed.sort();
    assert_eq!(completed, vec![(1, true), (2, true)]);
}

#[test]
fn releasing_the_snapshot_unblocks_every_stripe() {
    let (device, state) = setup(4);
    let mut channel = device.create_channel().unwrap();

    state.lock_all();
    channel.add_write(0, 1, buffer_of(0x33, 1), 1);
    channel.submit().unwrap();
    assert!(channel.poll().is_empty());

    // Last destination went away: the snapshot ends and prod carries on.
    state.release_all();
    assert_eq!(state.stripe_state(0), FREE);
    assert_eq!(channel.poll(), vec![(1, true)]);
}

#[test]
fn a_write_spanning_stripes_waits_for_all_of_them() {
    let (device, state) = setup(4);
    let mut channel = device.create_channel().unwrap();

    state.lock_all();
    state.finish_copy(0);

    // Spans stripes 0 and 1; stripe 1 is still locked.
    channel.add_write(STRIPE_SECTORS - 1, 2, buffer_of(0x44, 2), 1);
    channel.submit().unwrap();
    assert!(channel.poll().is_empty());

    state.finish_copy(1);
    assert_eq!(channel.poll(), vec![(1, true)]);
}

#[test]
fn each_freeze_gets_its_own_generation() {
    let (_device, state) = setup(2);
    assert_eq!(state.generation(), 0);
    assert_eq!(state.lock_all(), 1);
    state.release_all();
    assert_eq!(state.lock_all(), 2);
}
