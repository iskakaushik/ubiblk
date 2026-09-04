use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Instant,
};

use super::*;

/// An ArchiveStore for tests that need to hold, order or fail requests.
/// Objects live behind Arc<Mutex<..>> so a GET store and a PUT store in the
/// same test can share them.
pub struct TestObjectStore {
    pub objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    /// Puts wait in a queue until `release_puts`, so a test can observe what
    /// happens (or must not happen) while an upload is in flight.
    pub hold_puts: bool,
    pub hold_gets: bool,
    /// Names whose next PUT fails, consumed one per failure.
    pub fail_puts: VecDeque<String>,
    pub fail_gets: VecDeque<String>,
    /// Names in the order `start_put_object` was called.
    pub put_order: Vec<String>,
    pub put_started_at: Vec<(String, Instant)>,
    pending_puts: VecDeque<(String, Vec<u8>)>,
    pending_gets: VecDeque<String>,
    finished_puts: Vec<(String, Result<()>)>,
    finished_gets: Vec<(String, Result<Vec<u8>>)>,
}

impl Default for TestObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl TestObjectStore {
    pub fn new() -> Self {
        Self::shared(Arc::new(Mutex::new(HashMap::new())))
    }

    pub fn shared(objects: Arc<Mutex<HashMap<String, Vec<u8>>>>) -> Self {
        TestObjectStore {
            objects,
            hold_puts: false,
            hold_gets: false,
            fail_puts: VecDeque::new(),
            fail_gets: VecDeque::new(),
            put_order: Vec::new(),
            put_started_at: Vec::new(),
            pending_puts: VecDeque::new(),
            pending_gets: VecDeque::new(),
            finished_puts: Vec::new(),
            finished_gets: Vec::new(),
        }
    }

    /// Complete every held PUT, in the order it was started.
    pub fn release_puts(&mut self) {
        while let Some((name, data)) = self.pending_puts.pop_front() {
            self.complete_put(name, data);
        }
    }

    /// Complete every held GET, in the order it was started.
    pub fn release_gets(&mut self) {
        while let Some(name) = self.pending_gets.pop_front() {
            self.complete_get(name);
        }
    }

    /// Remove the first occurrence of `name` from `queue`, reporting whether
    /// there was one.
    fn take_failure(queue: &mut VecDeque<String>, name: &str) -> bool {
        match queue.iter().position(|failing| failing == name) {
            Some(index) => {
                queue.remove(index);
                true
            }
            None => false,
        }
    }

    fn complete_put(&mut self, name: String, data: Vec<u8>) {
        let result = if Self::take_failure(&mut self.fail_puts, &name) {
            Err(crate::ubiblk_error!(ArchiveError {
                description: format!("injected put failure for {name}"),
            }))
        } else {
            self.objects.lock().unwrap().insert(name.clone(), data);
            Ok(())
        };
        self.finished_puts.push((name, result));
    }

    fn complete_get(&mut self, name: String) {
        let result = if Self::take_failure(&mut self.fail_gets, &name) {
            Err(crate::ubiblk_error!(ArchiveError {
                description: format!("injected get failure for {name}"),
            }))
        } else {
            self.objects
                .lock()
                .unwrap()
                .get(&name)
                .cloned()
                .ok_or_else(|| {
                    crate::ubiblk_error!(ArchiveError {
                        description: format!("Object {name} not found"),
                    })
                })
        };
        self.finished_gets.push((name, result));
    }
}

impl ArchiveStore for TestObjectStore {
    fn start_put_object(&mut self, name: &str, data: Vec<u8>) {
        self.put_order.push(name.to_string());
        self.put_started_at.push((name.to_string(), Instant::now()));
        if self.hold_puts {
            self.pending_puts.push_back((name.to_string(), data));
        } else {
            self.complete_put(name.to_string(), data);
        }
    }

    fn start_get_object(&mut self, name: &str) {
        if self.hold_gets {
            self.pending_gets.push_back(name.to_string());
        } else {
            self.complete_get(name.to_string());
        }
    }

    fn poll_puts(&mut self) -> Vec<(String, Result<()>)> {
        std::mem::take(&mut self.finished_puts)
    }

    fn poll_gets(&mut self) -> Vec<(String, Result<Vec<u8>>)> {
        std::mem::take(&mut self.finished_gets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hold_release_puts() {
        let mut store = TestObjectStore::new();
        store.hold_puts = true;

        store.start_put_object("dev/1", b"one".to_vec());
        assert!(store.poll_puts().is_empty());
        assert!(store.objects.lock().unwrap().is_empty());

        store.release_puts();
        let finished = store.poll_puts();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].0, "dev/1");
        assert!(finished[0].1.is_ok());
        assert_eq!(
            store.objects.lock().unwrap().get("dev/1"),
            Some(&b"one".to_vec())
        );
    }

    #[test]
    fn hold_release_gets() {
        let objects = Arc::new(Mutex::new(HashMap::from([(
            "dev/1".to_string(),
            b"one".to_vec(),
        )])));
        let mut store = TestObjectStore::shared(objects);
        store.hold_gets = true;

        store.start_get_object("dev/1");
        store.start_get_object("dev/2");
        assert!(store.poll_gets().is_empty());

        store.release_gets();
        let finished = store.poll_gets();
        assert_eq!(finished.len(), 2);
        assert_eq!(finished[0].1.as_ref().unwrap(), b"one");
        assert!(finished[1].1.is_err(), "dev/2 does not exist");
    }

    #[test]
    fn fail_next_put() {
        let mut store = TestObjectStore::new();
        store.fail_puts.push_back("dev/1".to_string());

        store.start_put_object("dev/1", b"one".to_vec());
        let finished = store.poll_puts();
        assert!(finished[0].1.is_err());
        assert!(store.objects.lock().unwrap().is_empty());

        // The failure was consumed: the retry succeeds.
        store.start_put_object("dev/1", b"one".to_vec());
        assert!(store.poll_puts()[0].1.is_ok());
        assert_eq!(store.objects.lock().unwrap().len(), 1);

        // Other names are unaffected by a pending failure.
        store.fail_puts.push_back("dev/9".to_string());
        store.start_put_object("dev/2", b"two".to_vec());
        assert!(store.poll_puts()[0].1.is_ok());
    }

    #[test]
    fn fail_next_get() {
        let mut store = TestObjectStore::new();
        store.start_put_object("dev/1", b"one".to_vec());
        store.poll_puts();
        store.fail_gets.push_back("dev/1".to_string());

        store.start_get_object("dev/1");
        assert!(store.poll_gets()[0].1.is_err());
        store.start_get_object("dev/1");
        assert_eq!(store.poll_gets()[0].1.as_ref().unwrap(), b"one");
    }

    #[test]
    fn put_order_recorded() {
        let mut store = TestObjectStore::new();
        store.hold_puts = true;
        let before = Instant::now();
        store.start_put_object("dev/3", vec![]);
        store.start_put_object("dev/1", vec![]);
        store.start_put_object("dev/2", vec![]);

        assert_eq!(store.put_order, vec!["dev/3", "dev/1", "dev/2"]);
        assert_eq!(store.put_started_at.len(), 3);
        assert!(store.put_started_at.iter().all(|(_, at)| *at >= before));

        store.release_puts();
        let finished: Vec<String> = store.poll_puts().into_iter().map(|(name, _)| name).collect();
        assert_eq!(finished, vec!["dev/3", "dev/1", "dev/2"]);
    }

    #[test]
    fn shared_objects_are_visible_across_stores() {
        let mut put_store = TestObjectStore::new();
        let mut get_store = TestObjectStore::shared(put_store.objects.clone());
        put_store
            .put_object("dev/1", b"one", Duration::from_secs(1))
            .unwrap();
        assert_eq!(
            get_store.get_object("dev/1", Duration::from_secs(1)).unwrap(),
            b"one"
        );
    }
}
