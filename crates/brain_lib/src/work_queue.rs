//! Bounded work queue with file_id coalescing for the watch loop.
//!
//! Sits between the mpsc channel (from `BrainWatcher`) and the indexing pipeline.
//! Deduplicates events per-path (last-write-wins) and drops oldest entries when
//! the queue exceeds its configured capacity.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;

use tracing::warn;

use crate::watcher::{FileEvent, coalesce_events};

/// Default maximum number of entries in the work queue.
const DEFAULT_CAPACITY: usize = 1024;

/// Bounded work queue that deduplicates file events by path.
///
/// Internally maintains insertion-ordered entries via `VecDeque` + `HashMap`.
/// When capacity is exceeded, the oldest entries are evicted (they will be
/// picked up on the next full scan).
pub struct WorkQueue {
    /// Insertion-ordered paths (front = oldest).
    order: VecDeque<PathBuf>,
    /// Last event per path.
    events: HashMap<PathBuf, FileEvent>,
    /// Maximum number of distinct paths in the queue.
    capacity: usize,
}

impl WorkQueue {
    /// Create a new work queue with the default capacity (1024).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a new work queue with a custom capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "WorkQueue capacity must be > 0");
        Self {
            order: VecDeque::with_capacity(capacity.min(4096)),
            events: HashMap::with_capacity(capacity.min(4096)),
            capacity,
        }
    }

    /// Push a single event. Last-write-wins: if an event for the same path
    /// already exists, it is replaced without changing insertion order.
    pub fn push(&mut self, event: FileEvent) {
        let path = event_path(&event);

        if self.events.contains_key(&path) {
            // Last-write-wins: replace the event, keep position in order.
            self.events.insert(path, event);
        } else {
            // New path — check capacity before inserting.
            if self.order.len() >= self.capacity {
                self.evict_oldest();
            }
            self.order.push_back(path.clone());
            self.events.insert(path, event);
        }
    }

    /// Push all events from an iterator.
    pub fn push_batch(&mut self, events: impl IntoIterator<Item = FileEvent>) {
        for event in events {
            self.push(event);
        }
    }

    /// Drain all pending events, returning coalesced `(renames, index_paths, delete_paths)`.
    ///
    /// The queue is empty after this call.
    pub fn drain_batch(&mut self) -> (Vec<(PathBuf, PathBuf)>, Vec<PathBuf>, Vec<PathBuf>) {
        if self.events.is_empty() {
            return (vec![], vec![], vec![]);
        }

        // Drain in insertion order so coalesce_events sees the correct sequence.
        let events: Vec<FileEvent> = self
            .order
            .drain(..)
            .filter_map(|path| self.events.remove(&path))
            .collect();

        debug_assert!(self.events.is_empty());

        coalesce_events(events)
    }

    /// Number of distinct paths currently queued.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Evict the oldest entry to make room.
    fn evict_oldest(&mut self) {
        // Skip paths that were already removed (via last-write-wins replacement
        // of a Renamed event that removed the old path).
        while let Some(oldest) = self.order.pop_front() {
            if self.events.remove(&oldest).is_some() {
                warn!(
                    path = %oldest.display(),
                    queue_len = self.capacity,
                    "work queue full, dropping oldest entry"
                );
                return;
            }
        }
    }
}

impl Default for WorkQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract the primary path from a `FileEvent`.
///
/// For `Renamed` events, returns the destination path (`to`) since that is
/// what matters for subsequent indexing. The `from` path is handled by
/// `coalesce_events` when draining.
fn event_path(event: &FileEvent) -> PathBuf {
    match event {
        FileEvent::Changed(p) | FileEvent::Created(p) | FileEvent::Deleted(p) => p.clone(),
        FileEvent::Renamed { to, .. } => to.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_drain_basic() {
        let mut q = WorkQueue::new();
        q.push(FileEvent::Created("/a.md".into()));
        q.push(FileEvent::Changed("/b.md".into()));
        assert_eq!(q.len(), 2);

        let (renames, index, delete) = q.drain_batch();
        assert!(renames.is_empty());
        assert_eq!(index.len(), 2);
        assert!(delete.is_empty());
        assert!(q.is_empty());
    }

    #[test]
    fn last_write_wins_same_path() {
        let mut q = WorkQueue::new();
        q.push(FileEvent::Changed("/a.md".into()));
        q.push(FileEvent::Changed("/a.md".into()));
        q.push(FileEvent::Changed("/a.md".into()));
        assert_eq!(q.len(), 1, "dedup should keep only 1 entry");

        let (_, index, _) = q.drain_batch();
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn delete_overwrites_changed() {
        let mut q = WorkQueue::new();
        q.push(FileEvent::Changed("/a.md".into()));
        q.push(FileEvent::Deleted("/a.md".into()));
        assert_eq!(q.len(), 1);

        let (_, index, delete) = q.drain_batch();
        assert!(index.is_empty());
        assert_eq!(delete.len(), 1);
    }

    #[test]
    fn bounded_drops_oldest() {
        let mut q = WorkQueue::with_capacity(3);
        q.push(FileEvent::Created("/1.md".into()));
        q.push(FileEvent::Created("/2.md".into()));
        q.push(FileEvent::Created("/3.md".into()));
        assert_eq!(q.len(), 3);

        // This should evict /1.md
        q.push(FileEvent::Created("/4.md".into()));
        assert_eq!(q.len(), 3);

        let (_, index, _) = q.drain_batch();
        let paths: Vec<String> = index.iter().map(|p| p.display().to_string()).collect();
        assert!(!paths.contains(&"/1.md".to_string()), "oldest should be evicted");
        assert!(paths.contains(&"/4.md".to_string()));
    }

    #[test]
    fn drain_clears_queue() {
        let mut q = WorkQueue::new();
        q.push(FileEvent::Created("/a.md".into()));
        q.push(FileEvent::Created("/b.md".into()));

        let _ = q.drain_batch();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);

        // Second drain returns empty
        let (renames, index, delete) = q.drain_batch();
        assert!(renames.is_empty());
        assert!(index.is_empty());
        assert!(delete.is_empty());
    }

    #[test]
    fn push_batch_works() {
        let mut q = WorkQueue::new();
        q.push_batch(vec![
            FileEvent::Created("/a.md".into()),
            FileEvent::Changed("/b.md".into()),
            FileEvent::Deleted("/c.md".into()),
        ]);
        assert_eq!(q.len(), 3);

        let (_, index, delete) = q.drain_batch();
        assert_eq!(index.len(), 2);
        assert_eq!(delete.len(), 1);
    }

    #[test]
    fn rename_event_coalesced() {
        let mut q = WorkQueue::new();
        q.push(FileEvent::Renamed {
            from: "/old.md".into(),
            to: "/new.md".into(),
        });
        assert_eq!(q.len(), 1);

        let (renames, index, delete) = q.drain_batch();
        assert_eq!(renames.len(), 1);
        assert!(index.is_empty());
        assert!(delete.is_empty());
    }
}
