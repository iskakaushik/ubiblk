//! Back-pressure for stripes pushed to a fork.
//!
//! The subscriber reads pushes as fast as the network delivers them, and the
//! worker that writes them to disk is also serving the guest. Without a limit
//! the difference lands in the fork's memory — a megabyte per stripe, unbounded
//! — which is prod's write rate turned into the fork's RSS.
//!
//! A fork that cannot keep up should stop reading instead. The pressure then
//! reaches prod as a push that takes longer to write, which is what a copy-out
//! is designed to wait for.

use std::sync::{Arc, Condvar, Mutex};

/// Stripes that may be waiting for the worker at once. Small: it exists to
/// smooth over the worker being busy, not to store prod's writes.
pub const MAX_QUEUED_PUSHES: usize = 64;

pub struct PushGate {
    queued: Mutex<usize>,
    room: Condvar,
    limit: usize,
}

impl PushGate {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            queued: Mutex::new(0),
            room: Condvar::new(),
            limit,
        })
    }

    /// Take a slot, waiting for one if the worker is behind.
    pub fn acquire(self: &Arc<Self>) -> PushPermit {
        let mut queued = self.queued.lock().unwrap();
        while *queued >= self.limit {
            queued = self.room.wait(queued).unwrap();
        }
        *queued += 1;
        PushPermit {
            gate: Some(self.clone()),
        }
    }

    #[cfg(test)]
    pub fn queued(&self) -> usize {
        *self.queued.lock().unwrap()
    }

    fn release(&self) {
        let mut queued = self.queued.lock().unwrap();
        *queued = queued.saturating_sub(1);
        self.room.notify_one();
    }
}

/// Held by a pushed stripe on its way to the worker, and dropped once the
/// worker has taken it — including when the request is dropped undelivered.
pub struct PushPermit {
    gate: Option<Arc<PushGate>>,
}

impl PushPermit {
    /// A permit that gates nothing, for callers with no subscriber behind them.
    pub fn unbounded() -> Self {
        Self { gate: None }
    }
}

impl Drop for PushPermit {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn a_permit_returns_its_slot_when_dropped() {
        let gate = PushGate::new(2);
        let first = gate.acquire();
        let _second = gate.acquire();
        assert_eq!(gate.queued(), 2);
        drop(first);
        assert_eq!(gate.queued(), 1);
    }

    #[test]
    fn acquiring_waits_while_the_worker_is_behind() {
        let gate = PushGate::new(1);
        let held = gate.acquire();

        let waiting = Arc::new(AtomicBool::new(true));
        let thread_gate = gate.clone();
        let thread_waiting = waiting.clone();
        let handle = thread::spawn(move || {
            let _permit = thread_gate.acquire();
            thread_waiting.store(false, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(
            waiting.load(Ordering::SeqCst),
            "the slot is taken, so the next push has to wait"
        );

        drop(held);
        handle.join().unwrap();
        assert!(!waiting.load(Ordering::SeqCst));
    }

    #[test]
    fn an_unbounded_permit_gates_nothing() {
        let gate = PushGate::new(1);
        let _held = gate.acquire();
        // Would block if it were the same gate.
        drop(PushPermit::unbounded());
    }
}
