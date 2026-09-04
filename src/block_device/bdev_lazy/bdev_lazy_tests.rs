#[cfg(test)]
mod tests {
    use crate::backends::SECTOR_SIZE;
    use crate::block_device::{
        bdev_lazy::{
            metadata::Fetched, BgWorker, LazyBlockDevice, SharedMetadataState, UbiMetadata,
        },
        bdev_test::{TestBlockDevice, TestDeviceMetrics},
        metadata_flags, stripe_flags, BgWorkerRequest, BlockDevice, Evicted, Evicting, IoChannel,
        GATE_FAIL, GATE_HOLD, GATE_OPEN,
    };
    use crate::block_device::{shared_buffer, SharedBuffer};
    use std::cell::RefCell;
    use std::sync::atomic::Ordering;
    use std::sync::{
        mpsc::{channel, Receiver},
        Arc, RwLock,
    };
    use std::thread::sleep;
    use std::time::Duration;

    const STRIPE_SHIFT: u8 = 12;
    const STRIPE_COUNT: usize = 4;
    const STRIPE_SECTORS: u64 = 1u64 << STRIPE_SHIFT;
    const DEV_SIZE: u64 = STRIPE_SECTORS * SECTOR_SIZE as u64 * STRIPE_COUNT as u64;
    const METADATA_SIZE: u64 = 8 * 1024 * 1024;

    struct TestEnv {
        lazy: Box<LazyBlockDevice>,
        bgworker: RefCell<BgWorker>,
        metadata_state: SharedMetadataState,
        stripe_sectors: u64,
        target_mem: Arc<RwLock<Vec<u8>>>,
        target_metrics: Arc<RwLock<TestDeviceMetrics>>,
        image_metrics: Option<Arc<RwLock<TestDeviceMetrics>>>,
        /// The device the stripe source reads from (the image, when there is one).
        source_metrics: Arc<RwLock<TestDeviceMetrics>>,
    }

    fn setup_env(with_image: bool, track_written: bool, data: &[u8]) -> TestEnv {
        let target_dev = TestBlockDevice::new(DEV_SIZE);
        let target_mem = target_dev.mem.clone();
        let target_metrics = target_dev.metrics.clone();

        let metadata_dev = TestBlockDevice::new(METADATA_SIZE);
        let metadata = UbiMetadata::new(STRIPE_SHIFT, STRIPE_COUNT, STRIPE_COUNT);
        metadata.save_to_bdev(&metadata_dev).unwrap();

        let metadata_state = {
            let loaded = UbiMetadata::load_from_bdev(&metadata_dev).expect("load metadata");
            SharedMetadataState::new(&loaded)
        };

        let (bgworker_ch, bgworker_rx) = channel();

        if with_image {
            let image_dev = TestBlockDevice::new(DEV_SIZE);
            let stripe_source = Box::new(
                crate::stripe_source::BlockDeviceStripeSource::new(
                    image_dev.clone(),
                    STRIPE_SECTORS,
                )
                .unwrap(),
            );
            if !data.is_empty() {
                let mut tmp = vec![0u8; SECTOR_SIZE];
                tmp[..data.len()].copy_from_slice(data);
                image_dev.write(0, &tmp, SECTOR_SIZE);
            }
            let image_metrics = image_dev.metrics.clone();
            let bgworker = BgWorker::new(
                stripe_source,
                &target_dev,
                &metadata_dev,
                SECTOR_SIZE,
                false,
                false,
                metadata_state.clone(),
                bgworker_rx,
                None,
            )
            .unwrap();
            let lazy = LazyBlockDevice::new(
                Box::new(target_dev),
                Some(Box::new(image_dev)),
                bgworker_ch,
                metadata_state.clone(),
                track_written,
            )
            .unwrap();
            TestEnv {
                lazy,
                bgworker: RefCell::new(bgworker),
                metadata_state,
                stripe_sectors: STRIPE_SECTORS,
                target_mem,
                target_metrics,
                source_metrics: image_metrics.clone(),
                image_metrics: Some(image_metrics),
            }
        } else {
            let source_dev = TestBlockDevice::new(DEV_SIZE);
            let stripe_source = Box::new(
                crate::stripe_source::BlockDeviceStripeSource::new(
                    source_dev.clone(),
                    STRIPE_SECTORS,
                )
                .unwrap(),
            );
            if !data.is_empty() {
                let mut tmp = vec![0u8; SECTOR_SIZE];
                tmp[..data.len()].copy_from_slice(data);
                source_dev.write(0, &tmp, SECTOR_SIZE);
            }
            let source_metrics = source_dev.metrics.clone();
            let bgworker = BgWorker::new(
                stripe_source,
                &target_dev,
                &metadata_dev,
                SECTOR_SIZE,
                false,
                false,
                metadata_state.clone(),
                bgworker_rx,
                None,
            )
            .unwrap();
            let lazy = LazyBlockDevice::new(
                Box::new(target_dev),
                None,
                bgworker_ch,
                metadata_state.clone(),
                track_written,
            )
            .unwrap();
            TestEnv {
                lazy,
                bgworker: RefCell::new(bgworker),
                metadata_state,
                stripe_sectors: STRIPE_SECTORS,
                target_mem,
                target_metrics,
                image_metrics: None,
                source_metrics,
            }
        }
    }

    /// Poll the channel and the bgworker until all operations complete.
    fn drive(bgworker: &RefCell<BgWorker>, chan: &mut Box<dyn IoChannel>) -> Vec<(usize, bool)> {
        let mut results = Vec::new();
        loop {
            {
                let mut f = bgworker.borrow_mut();
                f.receive_requests(false);
                f.update();
            }
            results.extend(chan.poll());
            if !chan.busy() {
                break;
            }
            sleep(Duration::from_millis(1));
        }
        {
            let mut f = bgworker.borrow_mut();
            f.receive_requests(false);
            f.update();
        }
        results.extend(chan.poll());
        results
    }

    /// Ensure that reads trigger stripe fetches when copy-on-read is enabled
    /// and that queued writes are committed once the fetch completes.
    #[test]
    fn test_copy_on_read_true() {
        let env = setup_env(false, false, b"copy_on_read_data");
        let mut chan = env.lazy.create_channel().unwrap();

        assert_eq!(env.target_metrics.read().unwrap().reads, 0);
        assert_eq!(env.target_metrics.read().unwrap().writes, 0);

        let read_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        chan.add_read(0, 1, read_buf.clone(), 1);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(1, true)]);
        assert_eq!(
            &read_buf.borrow().as_slice()[.."copy_on_read_data".len()],
            b"copy_on_read_data"
        );
        assert_eq!(
            &env.target_mem.read().unwrap()[0.."copy_on_read_data".len()],
            b"copy_on_read_data"
        );

        assert_eq!(env.target_metrics.read().unwrap().reads, 1);
        assert_eq!(env.target_metrics.read().unwrap().writes, 1);

        let write_data = b"queued_write";
        let write_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        write_buf.borrow_mut().as_mut_slice()[..write_data.len()].copy_from_slice(write_data);
        chan.add_write(env.stripe_sectors, 1, write_buf.clone(), 2);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(2, true)]);
        let start = env.stripe_sectors as usize * SECTOR_SIZE;
        assert_eq!(
            &env.target_mem.read().unwrap()[start..start + write_data.len()],
            write_data
        );

        let flush_id = 3;
        chan.add_flush(flush_id);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(flush_id, true)]);
    }

    /// Verify that reads are served from the image when copy-on-read is
    /// disabled and that writes and flushes still operate on the target device.
    #[test]
    fn test_copy_on_read_false() {
        let env = setup_env(true, false, b"image_read");
        let mut chan = env.lazy.create_channel().unwrap();

        assert_eq!(env.image_metrics.as_ref().unwrap().read().unwrap().reads, 0);
        assert_eq!(env.target_metrics.read().unwrap().reads, 0);
        assert_eq!(env.target_metrics.read().unwrap().writes, 0);

        let read_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        chan.add_read(0, 1, read_buf.clone(), 1);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(1, true)]);
        assert_eq!(
            &read_buf.borrow().as_slice()[.."image_read".len()],
            b"image_read"
        );
        assert_ne!(
            &env.target_mem.read().unwrap()[0.."image_read".len()],
            b"image_read"
        );
        assert_eq!(env.image_metrics.as_ref().unwrap().read().unwrap().reads, 1);
        assert_eq!(env.target_metrics.read().unwrap().reads, 0);
        assert_eq!(env.target_metrics.read().unwrap().writes, 0);

        let write_data = b"write_after_fetch";
        let write_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        write_buf.borrow_mut().as_mut_slice()[..write_data.len()].copy_from_slice(write_data);
        chan.add_write(env.stripe_sectors, 1, write_buf.clone(), 2);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(2, true)]);
        let start = env.stripe_sectors as usize * SECTOR_SIZE;
        assert_eq!(
            &env.target_mem.read().unwrap()[start..start + write_data.len()],
            write_data
        );

        // write request on the same stripe
        let write_data = b"second_write";
        write_buf.borrow_mut().as_mut_slice()[..write_data.len()].copy_from_slice(write_data);
        chan.add_write(env.stripe_sectors, 1, write_buf.clone(), 3);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(3, true)]);
        let start = env.stripe_sectors as usize * SECTOR_SIZE;
        assert_eq!(
            &env.target_mem.read().unwrap()[start..start + write_data.len()],
            write_data
        );

        let flush_id = 3;
        chan.add_flush(flush_id);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(flush_id, true)]);
    }

    /// Verify that on multi-stripe reads, we fetch stripes regardless of
    /// whether copy-on-read is enabled or not.
    #[test]
    fn test_copy_on_read_false_multistripe() {
        let env = setup_env(true, false, b"image_read");
        let mut chan = env.lazy.create_channel().unwrap();

        let read_buf: SharedBuffer = shared_buffer(SECTOR_SIZE * 4);
        chan.add_read(STRIPE_SECTORS - 2, 4, read_buf.clone(), 1);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(1, true)]);

        {
            let image_metrics = env.image_metrics.as_ref().unwrap().read().unwrap();
            let target_metrics = env.target_metrics.read().unwrap();
            assert_eq!(image_metrics.reads, 2);
            assert_eq!(image_metrics.writes, 0);
            assert_eq!(target_metrics.reads, 1);
            assert_eq!(target_metrics.writes, 2);
        }

        // 2nd read should be served from target device
        chan.add_read(STRIPE_SECTORS - 2, 4, read_buf.clone(), 2);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(2, true)]);

        {
            let image_metrics = env.image_metrics.as_ref().unwrap().read().unwrap();
            let target_metrics = env.target_metrics.read().unwrap();
            assert_eq!(image_metrics.reads, 2);
            assert_eq!(image_metrics.writes, 0);
            assert_eq!(target_metrics.reads, 2);
            assert_eq!(target_metrics.writes, 2);
        }
    }

    /// Verify that stripes are marked written when tracking is enabled.
    #[test]
    fn test_track_written_true() {
        let env = setup_env(false, true, b"");
        let mut chan = env.lazy.create_channel().unwrap();

        let write_data = b"write_with_tracking";
        let write_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        write_buf.borrow_mut().as_mut_slice()[..write_data.len()].copy_from_slice(write_data);
        chan.add_write(env.stripe_sectors, 1, write_buf.clone(), 1);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(1, true)]);
        let start = env.stripe_sectors as usize * SECTOR_SIZE;
        assert_eq!(
            &env.target_mem.read().unwrap()[start..start + write_data.len()],
            write_data
        );

        let state = env.metadata_state.clone();
        assert!(state.stripe_written(1));

        let flush_id = 2;
        chan.add_flush(flush_id);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(flush_id, true)]);
    }

    /// A written stripe has to be visible in the shared state as soon as the
    /// write is submitted, without the worker having run.
    ///
    /// That state is what a fork is told its source holds, and a fork told a
    /// stripe holds nothing reads zeros there rather than fetching it — for
    /// good. Leaving it to the worker left a window where a stripe prod had
    /// just written looked empty, which is how a fork ended up with a
    /// zero-filled postgres file and refused to start.
    #[test]
    fn a_written_stripe_is_visible_before_the_worker_runs() {
        let env = setup_env(false, true, b"");
        let mut chan = env.lazy.create_channel().unwrap();

        let write_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        write_buf.borrow_mut().as_mut_slice()[..4].copy_from_slice(b"data");
        chan.add_write(env.stripe_sectors, 1, write_buf, 1);
        chan.submit().unwrap();

        assert!(
            env.metadata_state.stripe_written(1),
            "the stripe must count as written before the worker has processed anything"
        );
    }

    /// Verify tracking of written stripes when an image device is present.
    #[test]
    fn test_track_written_true_with_image() {
        let env = setup_env(true, true, b"image_data");
        let mut chan = env.lazy.create_channel().unwrap();

        let write_data = b"track_image_write";
        let write_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        write_buf.borrow_mut().as_mut_slice()[..write_data.len()].copy_from_slice(write_data);
        chan.add_write(env.stripe_sectors, 1, write_buf.clone(), 1);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(1, true)]);
        let start = env.stripe_sectors as usize * SECTOR_SIZE;
        assert_eq!(
            &env.target_mem.read().unwrap()[start..start + write_data.len()],
            write_data
        );

        let state = env.metadata_state.clone();
        assert!(state.stripe_written(1));

        let flush_id = 2;
        chan.add_flush(flush_id);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(flush_id, true)]);
    }

    #[test]
    fn test_failed_stripe_access() {
        let env = setup_env(true, false, b"image_read");
        let mut chan = env.lazy.create_channel().unwrap();

        env.metadata_state.set_stripe_failed(0);

        let read_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        chan.add_read(0, 1, read_buf.clone(), 1);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);

        assert_eq!(results, vec![(1, false)]);

        let write_data = b"write_to_failed_stripe";
        let write_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        write_buf.borrow_mut().as_mut_slice()[..write_data.len()].copy_from_slice(write_data);
        chan.add_write(0, 1, write_buf.clone(), 2);
        chan.submit().unwrap();
        let results = drive(&env.bgworker, &mut chan);

        assert_eq!(results, vec![(2, false)]);
    }

    #[test]
    fn test_clone() {
        let env = setup_env(true, false, b"image_read");
        let lazy_clone = env.lazy.clone();

        assert_eq!(lazy_clone.sector_count(), env.lazy.sector_count());

        let mut chan = lazy_clone.create_channel().unwrap();

        let read_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        chan.add_read(0, 1, read_buf.clone(), 1);
        chan.submit().unwrap();

        let results = drive(&env.bgworker, &mut chan);
        assert_eq!(results, vec![(1, true)]);
        assert_eq!(
            &read_buf.borrow().as_slice()[.."image_read".len()],
            b"image_read"
        );
    }

    #[test]
    fn test_start_stripe_fetches_channel_failure_read() {
        let TestEnv { lazy, bgworker, .. } = setup_env(false, false, b"");
        drop(bgworker);
        let mut chan = lazy.create_channel().unwrap();

        let read_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        chan.add_read(0, 1, read_buf, 1);
        chan.submit().unwrap();
        let results = chan.poll();

        assert_eq!(results, vec![(1, false)]);
    }

    #[test]
    fn test_start_stripe_fetches_channel_failure_write() {
        let TestEnv { lazy, bgworker, .. } = setup_env(false, false, b"");
        drop(bgworker);
        let mut chan = lazy.create_channel().unwrap();

        let write_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        chan.add_write(0, 1, write_buf, 1);
        chan.submit().unwrap();
        let results = chan.poll();
        assert_eq!(results, vec![(1, false)]);
    }

    #[test]
    fn test_start_stripe_set_written_channel_failure() {
        let TestEnv {
            lazy,
            bgworker,
            metadata_state,
            target_metrics,
            ..
        } = setup_env(false, true, b"");
        metadata_state.set_stripe_header(0, metadata_flags::FETCHED);
        drop(bgworker);
        let mut chan = lazy.create_channel().unwrap();

        let write_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        chan.add_write(0, 1, write_buf, 1);
        chan.submit().unwrap();
        let results = chan.poll();

        assert_eq!(results, vec![(1, false)]);
        assert_eq!(target_metrics.read().unwrap().writes, 0);
    }

    #[test]
    fn unknown_fetch_state_fails_the_request() {
        let TestEnv {
            lazy,
            metadata_state,
            target_metrics,
            ..
        } = setup_env(false, false, b"");
        metadata_state.set_stripe_fetch_state_for_test(0, 9);
        let mut chan = lazy.create_channel().unwrap();

        let buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        chan.add_read(0, 1, buf, 1);
        chan.submit().unwrap();
        let results = chan.poll();

        assert_eq!(results, vec![(1, false)]);
        assert_eq!(target_metrics.read().unwrap().reads, 0);
        assert_eq!(
            metadata_state
                .spill()
                .degraded_reasons
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    /// A lazy device over a test target with no coordinator behind it: the
    /// channel's requests land on `requests`, so a test can see exactly what
    /// the channel asked for and drive the shared state itself.
    struct ChannelEnv {
        lazy: Box<LazyBlockDevice>,
        metadata_state: SharedMetadataState,
        target: TestBlockDevice,
        requests: Receiver<BgWorkerRequest>,
    }

    fn setup_channel_env(track_written: bool) -> ChannelEnv {
        let target = TestBlockDevice::new(DEV_SIZE);
        let metadata = UbiMetadata::new(STRIPE_SHIFT, STRIPE_COUNT, STRIPE_COUNT);
        let metadata_state = SharedMetadataState::new(&metadata);
        let (bgworker_ch, requests) = channel();
        let lazy = LazyBlockDevice::new(
            target.clone(),
            None,
            bgworker_ch,
            metadata_state.clone(),
            track_written,
        )
        .unwrap();
        ChannelEnv {
            lazy,
            metadata_state,
            target,
            requests,
        }
    }

    impl ChannelEnv {
        fn reads(&self) -> usize {
            self.target.metrics.read().unwrap().reads
        }

        fn writes(&self) -> usize {
            self.target.metrics.read().unwrap().writes
        }

        fn referenced(&self, stripe_id: usize) -> bool {
            self.metadata_state.stripe_flags(stripe_id) & stripe_flags::REFERENCED != 0
        }

        /// Everything the channel sent so far: (Fetch stripe ids, SetWritten
        /// stripe ids), in order.
        fn drain_requests(&self) -> (Vec<usize>, Vec<usize>) {
            let (mut fetches, mut set_written) = (Vec::new(), Vec::new());
            while let Ok(req) = self.requests.try_recv() {
                match req {
                    BgWorkerRequest::Fetch { stripe_id } => fetches.push(stripe_id),
                    BgWorkerRequest::SetWritten { stripe_id } => set_written.push(stripe_id),
                    _ => {}
                }
            }
            (fetches, set_written)
        }

        fn fetch_ids(&self) -> Vec<usize> {
            self.drain_requests().0
        }
    }

    /// Take a resident stripe to Evicted through the real transitions, so the
    /// counters agree with the state (the raw setter leaves them alone).
    fn evict(state: &SharedMetadataState, stripe_id: usize) {
        let previous = state
            .try_begin_evicting(stripe_id)
            .expect("stripe is resident");
        state.finish_evicting(stripe_id, previous, false);
    }

    fn buf_with(data: &[u8]) -> SharedBuffer {
        let buf = shared_buffer(SECTOR_SIZE);
        buf.borrow_mut().as_mut_slice()[..data.len()].copy_from_slice(data);
        buf
    }

    fn sorted(mut results: Vec<(usize, bool)>) -> Vec<(usize, bool)> {
        results.sort_by_key(|r| r.0);
        results
    }

    #[test]
    fn read_of_evicting_stripe_queues_and_sends_fetch() {
        let env = setup_channel_env(false);
        env.metadata_state.mark_stripe_fetched(0);
        assert_eq!(env.metadata_state.try_begin_evicting(0), Some(Fetched));
        let mut chan = env.lazy.create_channel().unwrap();

        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        chan.submit().unwrap();
        assert_eq!(env.reads(), 0, "nothing reaches base while Evicting");
        assert!(chan.busy());
        assert_eq!(env.fetch_ids(), vec![0]);
        assert!(chan.poll().is_empty());
        assert_eq!(env.reads(), 0);

        // The coordinator aborts the eviction on that Fetch; the request goes.
        env.metadata_state.abort_evicting(0, Fetched);
        assert_eq!(chan.poll(), vec![(1, true)]);
        assert_eq!(env.reads(), 1);
        assert!(!chan.busy());
    }

    #[test]
    fn read_of_evicted_stripe_queues_and_sends_fetch() {
        let env = setup_channel_env(false);
        env.metadata_state.mark_stripe_fetched(0);
        evict(&env.metadata_state, 0);
        assert_eq!(env.metadata_state.stripe_fetch_state(0), Evicted);
        let mut chan = env.lazy.create_channel().unwrap();

        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        chan.submit().unwrap();
        assert_eq!(env.reads(), 0, "nothing reaches base while Evicted");
        assert_eq!(env.fetch_ids(), vec![0]);
        assert!(chan.poll().is_empty());

        // The coordinator re-materialises it (durable-first, item C).
        env.metadata_state.mark_stripe_resident(0);
        assert_eq!(chan.poll(), vec![(1, true)]);
        assert_eq!(env.reads(), 1);
    }

    /// The safety argument of section 4.2: the in-flight count is raised
    /// before the state is looked at, so by the time a request is inside base
    /// the evictor's one look at the counter after its CAS cannot see zero.
    /// The hook flips the stripe to Evicting from inside base's add_read,
    /// which is as late as a claim can race with this request.
    #[test]
    fn inflight_is_pinned_before_the_state_check() {
        let env = setup_channel_env(false);
        env.metadata_state.mark_stripe_fetched(0);
        let state = env.metadata_state.clone();
        env.target.set_on_add_read(Some(Box::new(move |sector| {
            let stripe_id = state.sector_to_stripe_id(sector);
            assert_eq!(
                state.stripe_inflight(stripe_id),
                1,
                "a request reached base without its stripe pinned"
            );
            state.set_stripe_fetch_state_for_test(stripe_id, Evicting);
        })));
        let mut chan = env.lazy.create_channel().unwrap();

        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        chan.submit().unwrap();

        // Either outcome is legal; dispatched unpinned is not.
        if env.reads() == 1 {
            assert_eq!(env.metadata_state.stripe_inflight(0), 1);
        } else {
            assert_eq!(env.metadata_state.stripe_inflight(0), 0);
            assert!(chan.busy());
        }
        assert_eq!(env.metadata_state.stripe_fetch_state(0), Evicting);

        env.target.set_on_add_read(None);
        let results = chan.poll();
        if env.reads() == 1 {
            assert_eq!(results, vec![(1, true)]);
        }
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);
    }

    #[test]
    fn inflight_returns_to_zero_after_completion() {
        let env = setup_channel_env(false);
        env.metadata_state.mark_stripe_fetched(0);
        env.target.hold_completions.store(true, Ordering::SeqCst);
        let mut chan = env.lazy.create_channel().unwrap();

        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        chan.submit().unwrap();
        assert_eq!(env.metadata_state.stripe_inflight(0), 1);
        assert!(chan.poll().is_empty());
        assert_eq!(
            env.metadata_state.stripe_inflight(0),
            1,
            "pinned until base completes it"
        );

        env.target.hold_completions.store(false, Ordering::SeqCst);
        assert_eq!(chan.poll(), vec![(1, true)]);
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);

        // A write pins and unpins the same way.
        env.target.hold_completions.store(true, Ordering::SeqCst);
        chan.add_write(0, 1, buf_with(b"w"), 2);
        chan.submit().unwrap();
        assert_eq!(env.metadata_state.stripe_inflight(0), 1);
        env.target.hold_completions.store(false, Ordering::SeqCst);
        assert_eq!(chan.poll(), vec![(2, true)]);
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);
    }

    #[test]
    fn inflight_is_released_when_queueing() {
        let env = setup_channel_env(true);
        let mut chan = env.lazy.create_channel().unwrap();

        // Not resident: pinned, seen Evicting, unpinned, queued.
        env.metadata_state.mark_stripe_fetched(0);
        assert_eq!(env.metadata_state.try_begin_evicting(0), Some(Fetched));
        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);

        // Resident but the write waits on WRITTEN under track_written.
        env.metadata_state.mark_stripe_fetched(1);
        chan.add_write(STRIPE_SECTORS, 1, buf_with(b"w"), 2);
        assert_eq!(env.metadata_state.stripe_inflight(1), 0);

        // Resident and written, but the gate holds writes.
        env.metadata_state.mark_stripe_fetched(2);
        env.metadata_state.mark_stripe_written(2);
        env.metadata_state.set_write_gate(GATE_HOLD);
        chan.add_write(2 * STRIPE_SECTORS, 1, buf_with(b"w"), 3);
        assert_eq!(env.metadata_state.stripe_inflight(2), 0);

        chan.submit().unwrap();
        assert!(chan.poll().is_empty());
        assert_eq!(env.writes(), 0);
        assert_eq!(env.reads(), 0);
        for stripe_id in 0..3 {
            assert_eq!(env.metadata_state.stripe_inflight(stripe_id), 0);
        }
    }

    #[test]
    fn inflight_is_released_on_submit_failure() {
        let env = setup_channel_env(false);
        env.metadata_state.mark_stripe_fetched(0);
        env.metadata_state.mark_stripe_fetched(1);
        let mut chan = env.lazy.create_channel().unwrap();

        // Direct path: the frontend's submit fails after the pass-through.
        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        assert_eq!(env.metadata_state.stripe_inflight(0), 1);
        env.target.fail_submit.store(true, Ordering::SeqCst);
        assert!(chan.submit().is_err());
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);
        assert!(chan.poll().is_empty());

        // The id comes round again: the map was cleared, so the old pin is
        // gone and the new one is released exactly once (no underflow).
        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        chan.submit().unwrap();
        assert_eq!(env.metadata_state.stripe_inflight(0), 1);
        assert_eq!(chan.poll(), vec![(1, true)]);
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);

        // Queued path: the submit inside poll fails; the request is failed
        // and its stripe unpinned.
        env.metadata_state
            .set_stripe_fetch_state_for_test(1, Evicting);
        chan.add_read(STRIPE_SECTORS, 1, shared_buffer(SECTOR_SIZE), 2);
        chan.submit().unwrap();
        assert!(chan.busy());
        env.metadata_state
            .set_stripe_fetch_state_for_test(1, Fetched);
        env.target.fail_submit.store(true, Ordering::SeqCst);
        assert_eq!(chan.poll(), vec![(2, false)]);
        assert_eq!(env.metadata_state.stripe_inflight(1), 0);
        assert!(!chan.busy());
    }

    #[test]
    fn flush_completions_do_not_touch_inflight() {
        let env = setup_channel_env(false);
        env.metadata_state.mark_stripe_fetched(0);
        let mut chan = env.lazy.create_channel().unwrap();

        // A flush by itself: no stripe is pinned, none is unpinned (an unpin
        // here would trip the underflow debug_assert).
        chan.add_flush(7);
        chan.submit().unwrap();
        assert_eq!(chan.poll(), vec![(7, true)]);
        for stripe_id in 0..STRIPE_COUNT {
            assert_eq!(env.metadata_state.stripe_inflight(stripe_id), 0);
        }

        // A flush completing next to a pinned read leaves the pin alone.
        env.target.hold_completions.store(true, Ordering::SeqCst);
        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        chan.add_flush(2);
        chan.submit().unwrap();
        assert_eq!(env.metadata_state.stripe_inflight(0), 1);
        env.target.hold_completions.store(false, Ordering::SeqCst);
        assert_eq!(sorted(chan.poll()), vec![(1, true), (2, true)]);
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);

        // A flush reusing a read's id after that read completed: the slot is
        // already None, so nothing is unpinned twice.
        chan.add_flush(1);
        chan.submit().unwrap();
        assert_eq!(chan.poll(), vec![(1, true)]);
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);
    }

    #[test]
    fn cross_stripe_request_pins_every_stripe_in_range() {
        let env = setup_channel_env(false);
        env.metadata_state.mark_stripe_fetched(0);
        env.metadata_state.mark_stripe_fetched(1);
        env.target.hold_completions.store(true, Ordering::SeqCst);
        let mut chan = env.lazy.create_channel().unwrap();

        let buf = shared_buffer(2 * SECTOR_SIZE);
        chan.add_read(STRIPE_SECTORS - 1, 2, buf, 1);
        chan.submit().unwrap();
        assert_eq!(env.metadata_state.stripe_inflight(0), 1);
        assert_eq!(env.metadata_state.stripe_inflight(1), 1);
        assert_eq!(env.metadata_state.stripe_inflight(2), 0);

        env.target.hold_completions.store(false, Ordering::SeqCst);
        assert_eq!(chan.poll(), vec![(1, true)]);
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);
        assert_eq!(env.metadata_state.stripe_inflight(1), 0);

        // If any stripe in the range is not resident, none is left pinned.
        evict(&env.metadata_state, 1);
        let buf = shared_buffer(2 * SECTOR_SIZE);
        chan.add_read(STRIPE_SECTORS - 1, 2, buf, 2);
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);
        assert_eq!(env.metadata_state.stripe_inflight(1), 0);
        assert_eq!(env.fetch_ids(), vec![1]);
    }

    #[test]
    fn clock_bit_set_only_on_pass_through() {
        let env = setup_channel_env(true);
        env.metadata_state.mark_stripe_fetched(0);
        env.metadata_state.mark_stripe_fetched(2);
        let mut chan = env.lazy.create_channel().unwrap();

        // Passes: referenced.
        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        assert!(env.referenced(0));
        assert!(env.metadata_state.take_reference(0));
        assert!(!env.referenced(0));

        // Queued on a stripe that is not here: not referenced.
        chan.add_read(STRIPE_SECTORS, 1, shared_buffer(SECTOR_SIZE), 2);
        assert!(!env.referenced(1));

        // Queued on a resident stripe, waiting on WRITTEN: not referenced
        // until the write actually passes on the next poll.
        chan.add_write(2 * STRIPE_SECTORS, 1, buf_with(b"w"), 3);
        assert!(!env.referenced(2));
        chan.submit().unwrap();
        // The front (stripe 1) is Pending, so the write waits behind it.
        assert!(chan.poll().len() == 1);
        assert!(!env.referenced(2));
        env.metadata_state.mark_stripe_fetched(1);
        assert_eq!(sorted(chan.poll()), vec![(2, true), (3, true)]);
        assert!(env.referenced(1));
        assert!(env.referenced(2));
    }

    #[test]
    fn pending_front_on_evicted_stripe_resends_fetch_every_100ms() {
        let env = setup_channel_env(true);
        env.metadata_state.mark_stripe_fetched(0);
        let mut chan = env.lazy.create_channel().unwrap();

        // A write on a Fetched but unwritten stripe queues without any Fetch:
        // the stripe is here, it only waits on WRITTEN.
        chan.add_write(0, 1, buf_with(b"w"), 1);
        chan.submit().unwrap();
        let (fetches, set_written) = env.drain_requests();
        assert!(fetches.is_empty());
        assert_eq!(set_written, vec![0]);
        assert!(chan.busy());

        // Before the write can go, the evictor takes the stripe away. Nothing
        // but this channel will ever ask for it back.
        evict(&env.metadata_state, 0);

        assert!(chan.poll().is_empty());
        assert_eq!(env.fetch_ids(), vec![0], "a Fetch is sent for the front");
        assert!(chan.poll().is_empty());
        assert!(env.fetch_ids().is_empty(), "not again straight away");

        sleep(Duration::from_millis(110));
        assert!(chan.poll().is_empty());
        assert_eq!(env.fetch_ids(), vec![0], "and again after the interval");

        sleep(Duration::from_millis(110));
        assert!(chan.poll().is_empty());
        assert_eq!(env.fetch_ids(), vec![0]);

        // Landed: the write passes on the next poll.
        env.metadata_state.mark_stripe_resident(0);
        assert_eq!(chan.poll(), vec![(1, true)]);
        assert_eq!(env.writes(), 1);
    }

    #[test]
    fn pending_front_on_not_fetched_stripe_does_not_resend() {
        let env = setup_channel_env(false);
        let mut chan = env.lazy.create_channel().unwrap();

        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 1);
        chan.submit().unwrap();
        assert_eq!(env.fetch_ids(), vec![0]);

        for _ in 0..3 {
            sleep(Duration::from_millis(110));
            assert!(chan.poll().is_empty());
            assert!(
                env.fetch_ids().is_empty(),
                "a NotFetched front is the fetcher's to finish"
            );
        }
        assert!(chan.busy());
    }

    #[test]
    fn hold_gate_queues_writes_and_passes_reads_then_drains_fifo() {
        let env = setup_channel_env(true);
        env.metadata_state.mark_stripe_fetched(0);
        env.metadata_state.mark_stripe_fetched(1);
        let mut chan = env.lazy.create_channel().unwrap();

        env.metadata_state.set_write_gate(GATE_HOLD);
        assert_eq!(env.metadata_state.spill().stalls.load(Ordering::Relaxed), 1);

        chan.add_write(0, 1, buf_with(b"first"), 1);
        chan.add_write(STRIPE_SECTORS, 1, buf_with(b"second"), 2);
        chan.submit().unwrap();
        assert_eq!(env.writes(), 0);
        assert!(chan.busy());
        // Today's contract with the snapshot server holds under the gate: the
        // stripe counts as written from the moment the write is queued.
        assert!(env.metadata_state.stripe_written(0));
        assert!(env.metadata_state.stripe_written(1));
        assert_eq!(env.drain_requests().1, vec![0, 1]);

        // Reads of resident stripes pass around the held writes.
        let read_buf = shared_buffer(SECTOR_SIZE);
        chan.add_read(0, 1, read_buf, 3);
        chan.submit().unwrap();
        assert_eq!(chan.poll(), vec![(3, true)]);
        assert_eq!(env.reads(), 1);
        assert_eq!(env.writes(), 0, "writes are still held");

        env.metadata_state.set_write_gate(GATE_OPEN);
        assert_eq!(chan.poll(), vec![(1, true), (2, true)]);
        assert_eq!(env.writes(), 2);
        assert!(!chan.busy());
        let mem = env.target.mem.read().unwrap();
        assert_eq!(&mem[..5], b"first");
        let start = STRIPE_SECTORS as usize * SECTOR_SIZE;
        assert_eq!(&mem[start..start + 6], b"second");
    }

    #[test]
    fn fail_gate_fails_new_writes_and_pending_fronts() {
        let env = setup_channel_env(false);
        env.metadata_state.mark_stripe_fetched(0);
        let mut chan = env.lazy.create_channel().unwrap();

        // A write held by the gate, and a read waiting on a fetch behind it.
        env.metadata_state.set_write_gate(GATE_HOLD);
        chan.add_write(0, 1, buf_with(b"held"), 1);
        chan.add_read(STRIPE_SECTORS, 1, shared_buffer(SECTOR_SIZE), 2);
        chan.submit().unwrap();
        assert_eq!(env.fetch_ids(), vec![1]);

        env.metadata_state.set_write_gate(GATE_FAIL);

        // New writes fail at once, resident stripe or not, and ask for nothing.
        chan.add_write(0, 1, buf_with(b"new"), 3);
        chan.add_write(2 * STRIPE_SECTORS, 1, buf_with(b"new"), 4);
        assert!(env.fetch_ids().is_empty());
        // Reads of resident stripes still pass.
        chan.add_read(0, 1, shared_buffer(SECTOR_SIZE), 5);
        chan.submit().unwrap();

        // The queued write and the Pending read are popped and failed.
        assert_eq!(
            sorted(chan.poll()),
            vec![(1, false), (2, false), (3, false), (4, false), (5, true)]
        );
        assert!(!chan.busy());
        assert_eq!(env.writes(), 0);
        assert_eq!(env.reads(), 1);
        for stripe_id in 0..STRIPE_COUNT {
            assert_eq!(env.metadata_state.stripe_inflight(stripe_id), 0);
        }
    }

    /// The whole loop with a real coordinator and fetcher: a read of an
    /// Evicted stripe sends Fetch, the fetcher pulls the stripe from the
    /// source once and writes it to the target, and the read is served with
    /// the right bytes once the stripe is resident again.
    ///
    /// Landing an Evicted stripe is the coordinator's durable-first release
    /// (item C); until it lands, this test stands in for it with
    /// `mark_stripe_resident` once the fetcher has written the data.
    #[test]
    fn evicted_stripe_is_refetched_before_read_end_to_end() {
        let data = b"back_from_the_source";
        let env = setup_env(false, false, data);
        env.metadata_state.mark_stripe_fetched(0);
        evict(&env.metadata_state, 0);
        assert_eq!(env.metadata_state.stripe_fetch_state(0), Evicted);
        assert_eq!(env.metadata_state.evicted_stripes(), 1);
        let mut chan = env.lazy.create_channel().unwrap();

        let read_buf: SharedBuffer = shared_buffer(SECTOR_SIZE);
        chan.add_read(0, 1, read_buf.clone(), 1);
        chan.submit().unwrap();
        assert_eq!(env.target_metrics.read().unwrap().reads, 0);

        let mut results = Vec::new();
        for _ in 0..5000 {
            {
                let mut f = env.bgworker.borrow_mut();
                f.receive_requests(false);
                f.update();
            }
            results.extend(chan.poll());
            if !results.is_empty() {
                break;
            }
            if env.metadata_state.stripe_fetch_state(0) == Evicted
                && env.target_metrics.read().unwrap().writes >= 1
            {
                assert_eq!(
                    env.target_metrics.read().unwrap().reads,
                    0,
                    "the read must not reach base while the stripe is Evicted"
                );
                env.metadata_state.mark_stripe_resident(0);
            }
            sleep(Duration::from_millis(1));
        }

        assert_eq!(results, vec![(1, true)]);
        assert_eq!(&read_buf.borrow().as_slice()[..data.len()], data);
        assert_eq!(env.source_metrics.read().unwrap().reads, 1);
        assert_eq!(env.target_metrics.read().unwrap().reads, 1);
        assert_eq!(env.metadata_state.stripe_fetch_state(0), Fetched);
        assert_eq!(env.metadata_state.evicted_stripes(), 0);
        assert_eq!(env.metadata_state.stripe_inflight(0), 0);
    }
}
