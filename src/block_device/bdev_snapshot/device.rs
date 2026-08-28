use std::collections::VecDeque;

use log::error;

use crate::{
    block_device::{BlockDevice, IoChannel, SharedBuffer},
    Result,
};

use super::state::{SharedSnapshotState, DRAINING, RUNNING};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestType {
    In,
    Out,
    Flush,
}

struct QueuedRequest {
    id: usize,
    kind: RequestType,
    sector_offset: u64,
    sector_count: u32,
    buf: Option<SharedBuffer>,
    stripe_id_first: usize,
    stripe_id_last: usize,
}

/// A device that can hand a point-in-time copy of itself to snapshot
/// destinations while the device below keeps taking writes.
///
/// It sits directly above `bdev_lazy`: reads and flushes always pass straight
/// through, and so do writes, except while a stripe still owes its pre-write
/// content to a snapshot.
pub struct SnapshotBlockDevice {
    inner: Box<dyn BlockDevice>,
    state: SharedSnapshotState,
}

impl SnapshotBlockDevice {
    pub fn new(inner: Box<dyn BlockDevice>, stripe_sector_count_shift: u8) -> Self {
        let stripe_count = inner.stripe_count(1u64 << stripe_sector_count_shift);
        let state = SharedSnapshotState::new(stripe_count, stripe_sector_count_shift);
        Self { inner, state }
    }

    pub fn state(&self) -> SharedSnapshotState {
        self.state.clone()
    }
}

impl BlockDevice for SnapshotBlockDevice {
    fn create_channel(&self) -> Result<Box<dyn IoChannel>> {
        self.state.register_channel();
        Ok(Box::new(SnapshotIoChannel::new(
            self.inner.create_channel()?,
            self.state.clone(),
        )))
    }

    fn sector_count(&self) -> u64 {
        self.inner.sector_count()
    }

    fn clone(&self) -> Box<dyn BlockDevice> {
        Box::new(SnapshotBlockDevice {
            inner: self.inner.clone(),
            state: self.state.clone(),
        })
    }
}

pub struct SnapshotIoChannel {
    inner: Box<dyn IoChannel>,
    state: SharedSnapshotState,
    queued: VecDeque<QueuedRequest>,
    finished: Vec<(usize, bool)>,
    /// Requests handed to the device below that have not completed yet. The
    /// freeze waits for this to reach zero on every channel.
    in_flight: usize,
    drain_reported: bool,
}

impl SnapshotIoChannel {
    fn new(inner: Box<dyn IoChannel>, state: SharedSnapshotState) -> Self {
        Self {
            inner,
            state,
            queued: VecDeque::new(),
            finished: Vec::new(),
            in_flight: 0,
            drain_reported: false,
        }
    }

    fn stripe_range(&self, sector_offset: u64, sector_count: u32) -> (usize, usize) {
        let first = self.state.sector_to_stripe_id(sector_offset);
        let last = self
            .state
            .sector_to_stripe_id(sector_offset + sector_count as u64 - 1);
        (first, last)
    }

    /// A write may start only when no stripe it touches still owes its
    /// pre-write content to a snapshot.
    fn write_allowed(&self, first: usize, last: usize) -> bool {
        (first..=last).all(|stripe_id| self.state.write_allowed(stripe_id))
    }

    fn queue(&mut self, request: QueuedRequest) {
        self.queued.push_back(request);
    }

    fn pass_through(&mut self, request: &QueuedRequest) {
        match request.kind {
            RequestType::In => self.inner.add_read(
                request.sector_offset,
                request.sector_count,
                request.buf.clone().expect("read has a buffer"),
                request.id,
            ),
            RequestType::Out => self.inner.add_write(
                request.sector_offset,
                request.sector_count,
                request.buf.clone().expect("write has a buffer"),
                request.id,
            ),
            RequestType::Flush => self.inner.add_flush(request.id),
        }
        self.in_flight += 1;
    }

    /// Replay whatever the layer was holding: everything while draining, and
    /// writes whose stripes have since been copied out.
    fn process_queued(&mut self) {
        if self.state.mode() != RUNNING {
            return;
        }

        let mut submit_needed = false;
        while let Some(front) = self.queued.front() {
            if front.kind == RequestType::Out
                && !self.write_allowed(front.stripe_id_first, front.stripe_id_last)
            {
                break;
            }

            let request = self.queued.pop_front().expect("front exists");
            self.pass_through(&request);
            submit_needed = true;
        }

        if submit_needed {
            if let Err(e) = self.inner.submit() {
                error!("Failed to submit queued snapshot requests: {e}");
            }
        }
    }
}

impl IoChannel for SnapshotIoChannel {
    fn add_read(&mut self, sector_offset: u64, sector_count: u32, buf: SharedBuffer, id: usize) {
        if self.state.mode() == RUNNING {
            self.inner.add_read(sector_offset, sector_count, buf, id);
            self.in_flight += 1;
            return;
        }

        let (first, last) = self.stripe_range(sector_offset, sector_count);
        self.queue(QueuedRequest {
            id,
            kind: RequestType::In,
            sector_offset,
            sector_count,
            buf: Some(buf),
            stripe_id_first: first,
            stripe_id_last: last,
        });
    }

    fn add_write(&mut self, sector_offset: u64, sector_count: u32, buf: SharedBuffer, id: usize) {
        let (first, last) = self.stripe_range(sector_offset, sector_count);

        if self.state.mode() == RUNNING && self.write_allowed(first, last) {
            self.inner.add_write(sector_offset, sector_count, buf, id);
            self.in_flight += 1;
            return;
        }

        self.queue(QueuedRequest {
            id,
            kind: RequestType::Out,
            sector_offset,
            sector_count,
            buf: Some(buf),
            stripe_id_first: first,
            stripe_id_last: last,
        });
    }

    fn add_flush(&mut self, id: usize) {
        if self.state.mode() == RUNNING {
            self.inner.add_flush(id);
            self.in_flight += 1;
            return;
        }

        self.queue(QueuedRequest {
            id,
            kind: RequestType::Flush,
            sector_offset: 0,
            sector_count: 0,
            buf: None,
            stripe_id_first: 0,
            stripe_id_last: 0,
        });
    }

    fn submit(&mut self) -> Result<()> {
        self.inner.submit()
    }

    fn poll(&mut self) -> Vec<(usize, bool)> {
        // Replay before polling, like `bdev_lazy` does, so a request released
        // this cycle can also complete this cycle.
        if self.state.mode() == RUNNING {
            self.drain_reported = false;
            self.process_queued();
        }

        let completed = self.inner.poll();
        self.in_flight = self.in_flight.saturating_sub(completed.len());
        self.finished.extend(completed);

        if self.state.mode() == DRAINING && self.in_flight == 0 && !self.drain_reported {
            self.state.report_drained();
            self.drain_reported = true;
        }

        std::mem::take(&mut self.finished)
    }

    fn busy(&self) -> bool {
        self.in_flight > 0 || !self.queued.is_empty() || !self.finished.is_empty()
    }
}
