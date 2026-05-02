use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use notify_debouncer_full::FileIdCache;
use notify_debouncer_full::file_id::{FileId, get_file_id};
use notify_debouncer_full::notify::RecursiveMode;
use notify_debouncer_full::notify::event::EventKind;
use notify_debouncer_full::{
    DebounceEventResult, Debouncer, new_debouncer_opt,
};
use tracing::{info, warn};
use walkdir::WalkDir;

/// Expected number of file-ID cache entries per watched directory.
///
/// Used to pre-size the `FileIdMap` backing `HashMap` so that bulk registration
/// does not trigger repeated hashbrown rehash cycles.  The value is intentionally
/// generous: a brain root may contain thousands of notes and subdirectories.
/// Over-allocation is cheap (each entry is ~80 bytes); under-allocation causes
/// O(N) rehashes during startup.
///
/// Only relevant on macOS and Windows; Linux/Android/wasm use `NoCache` and
/// ignore this constant entirely.
pub const ESTIMATED_PATHS_PER_DIR: usize = 4096;

/// A file-ID cache backed by a pre-sized `HashMap`.
///
/// Functionally identical to `notify_debouncer_full::FileIdMap`, but constructed
/// with `HashMap::with_capacity` so that registering many paths in bulk does not
/// trigger repeated hashbrown rehash cycles.
#[cfg(not(any(target_os = "linux", target_os = "android", target_family = "wasm")))]
struct PreSizedFileIdMap {
    paths: HashMap<PathBuf, FileId>,
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_family = "wasm")))]
impl PreSizedFileIdMap {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            paths: HashMap::with_capacity(capacity),
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_family = "wasm")))]
impl FileIdCache for PreSizedFileIdMap {
    fn cached_file_id(&self, path: &Path) -> Option<impl AsRef<FileId>> {
        self.paths.get(path)
    }

    fn add_path(&mut self, path: &Path, recursive_mode: RecursiveMode) {
        let is_recursive = recursive_mode == RecursiveMode::Recursive;
        let depth = if is_recursive { usize::MAX } else { 1 };
        for (p, id) in WalkDir::new(path)
            .follow_links(true)
            .max_depth(depth)
            .into_iter()
            .filter_map(|e| {
                let p = e.ok()?.into_path();
                let id = get_file_id(&p).ok()?;
                Some((p, id))
            })
        {
            self.paths.insert(p, id);
        }
    }

    fn remove_path(&mut self, path: &Path) {
        self.paths.retain(|p, _| !p.starts_with(path));
    }
}

/// On Linux/Android/wasm `notify` uses inotify/epoll; file-ID tracking is not
/// required and `NoCache` is the platform recommendation.  We alias the cache
/// type so the rest of the code compiles uniformly.
#[cfg(any(target_os = "linux", target_os = "android", target_family = "wasm"))]
type PlatformCache = notify_debouncer_full::NoCache;
#[cfg(not(any(target_os = "linux", target_os = "android", target_family = "wasm")))]
type PlatformCache = PreSizedFileIdMap;

/// Events emitted by the file watcher.
#[derive(Debug)]
pub enum FileEvent {
    Changed(PathBuf),
    Created(PathBuf),
    Deleted(PathBuf),
    Renamed { from: PathBuf, to: PathBuf },
}

/// Watches directories for markdown file changes, debouncing and filtering events.
///
/// # Platform behaviour
///
/// The underlying file-ID cache differs by platform:
///
/// - **macOS / Windows**: uses `PreSizedFileIdMap`, a custom `FileIdCache` backed by a
///   pre-sized `HashMap<PathBuf, FileId>`.  File IDs (inode + device on macOS, file index
///   on Windows) enable `notify` to emit a single `Renamed { from, to }` event when a
///   file is moved within a watched tree.
///
/// - **Linux / Android / wasm**: uses `notify_debouncer_full::NoCache`.  The kernel
///   delivers inotify / epoll events directly; file-ID tracking is unnecessary and
///   `notify` does not attempt rename correlation.  A file move arrives as two separate
///   events: `Remove(old_path)` followed by `Create(new_path)`.  The event pipeline
///   handles this correctly — `coalesce_events` treats a `Created` that follows a
///   `Deleted` for the same path as a re-index, and paths are routed by longest-prefix
///   match so both events reach the correct brain's work queue regardless of rename
///   correlation.  No code path depends on a `Renamed` event being emitted on Linux.
pub struct BrainWatcher {
    debouncer: Debouncer<notify_debouncer_full::notify::RecommendedWatcher, PlatformCache>,
}

impl BrainWatcher {
    /// Create a new watcher that monitors the given directories for `.md` file changes.
    ///
    /// Events are sent over the provided `tokio::sync::mpsc::Sender`.
    /// The watcher uses a 250ms debounce window.
    pub fn new(
        dirs: &[PathBuf],
        tx: tokio::sync::mpsc::Sender<FileEvent>,
    ) -> crate::error::Result<Self> {
        let capacity = dirs.len().saturating_mul(ESTIMATED_PATHS_PER_DIR);
        let mut watcher = Self::new_empty_with_capacity(capacity, tx)?;
        for dir in dirs {
            watcher.watch_path(dir.as_path())?;
        }
        Ok(watcher)
    }

    /// Create a new watcher with no initial directories.
    ///
    /// Directories can be added later via [`watch_path`](Self::watch_path).
    ///
    /// The `capacity` hint is the expected total number of watched file paths across
    /// all directories that will be registered.  Passing a reasonable estimate avoids
    /// repeated HashMap rehash cycles when registering many paths in bulk (multi-brain mode).
    pub fn new_empty_with_capacity(
        capacity: usize,
        tx: tokio::sync::mpsc::Sender<FileEvent>,
    ) -> crate::error::Result<Self> {
        #[cfg(any(target_os = "linux", target_os = "android", target_family = "wasm"))]
        let cache = notify_debouncer_full::NoCache::new();
        #[cfg(not(any(target_os = "linux", target_os = "android", target_family = "wasm")))]
        let cache = PreSizedFileIdMap::with_capacity(capacity);

        let debouncer = new_debouncer_opt::<
            _,
            notify_debouncer_full::notify::RecommendedWatcher,
            PlatformCache,
        >(
            Duration::from_millis(250),
            None,
            move |result: DebounceEventResult| match result {
                Ok(events) => {
                    for event in events {
                        let file_events = map_event(&event);
                        for fe in file_events {
                            if tx.blocking_send(fe).is_err() {
                                warn!("watcher channel closed");
                                return;
                            }
                        }
                    }
                }
                Err(errors) => {
                    for e in errors {
                        warn!(error = %e, "watcher error");
                    }
                }
            },
            cache,
            notify_debouncer_full::notify::Config::default(),
        )
        .map_err(|e| {
            crate::error::BrainCoreError::Io(std::io::Error::other(format!("watcher init: {e}")))
        })?;

        Ok(Self { debouncer })
    }

    /// Create a new watcher with no initial directories.
    ///
    /// Equivalent to [`new_empty_with_capacity`](Self::new_empty_with_capacity) with a zero
    /// capacity hint.  Prefer `new_empty_with_capacity` when the total number of paths is
    /// known ahead of registration.
    pub fn new_empty(tx: tokio::sync::mpsc::Sender<FileEvent>) -> crate::error::Result<Self> {
        Self::new_empty_with_capacity(0, tx)
    }

    /// Start watching a directory recursively.
    ///
    /// Returns an error if the directory does not exist or cannot be watched.
    /// The caller decides whether to skip non-existent directories or fail.
    pub fn watch_path(&mut self, dir: &Path) -> crate::error::Result<()> {
        if !dir.exists() {
            return Err(crate::error::BrainCoreError::Io(std::io::Error::other(
                format!("watch dir does not exist: {}", dir.display()),
            )));
        }
        self.debouncer
            .watch(dir, RecursiveMode::Recursive)
            .map_err(|e| {
                crate::error::BrainCoreError::Io(std::io::Error::other(format!(
                    "watch dir {}: {e}",
                    dir.display()
                )))
            })?;
        info!(dir = %dir.display(), "watching directory");
        Ok(())
    }

    /// Stop watching a previously registered directory.
    ///
    /// Returns an error if the directory was not being watched or the unwatch fails.
    pub fn unwatch_path(&mut self, dir: &Path) -> crate::error::Result<()> {
        self.debouncer.unwatch(dir).map_err(|e| {
            crate::error::BrainCoreError::Io(std::io::Error::other(format!(
                "unwatch dir {}: {e}",
                dir.display()
            )))
        })
    }
}

/// Filter to only markdown files.
fn is_markdown(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("md"))
}

/// Map a debounced notify event to our FileEvent(s).
fn map_event(event: &notify_debouncer_full::DebouncedEvent) -> Vec<FileEvent> {
    let mut result = Vec::new();

    match &event.kind {
        EventKind::Create(_) => {
            for path in &event.paths {
                if is_markdown(path) {
                    result.push(FileEvent::Created(path.clone()));
                }
            }
        }
        EventKind::Modify(notify_debouncer_full::notify::event::ModifyKind::Name(_)) => {
            if event.paths.len() == 2 {
                let from = &event.paths[0];
                let to = &event.paths[1];
                if is_markdown(from) || is_markdown(to) {
                    result.push(FileEvent::Renamed {
                        from: from.clone(),
                        to: to.clone(),
                    });
                }
            }
            // Single-path Name events (From/To without Both) — treat as change
            // so the file gets re-indexed at its current path
            for path in &event.paths {
                if event.paths.len() != 2 && is_markdown(path) {
                    result.push(FileEvent::Changed(path.clone()));
                }
            }
        }
        EventKind::Modify(_) => {
            for path in &event.paths {
                if is_markdown(path) {
                    result.push(FileEvent::Changed(path.clone()));
                }
            }
        }
        EventKind::Remove(_) => {
            for path in &event.paths {
                if is_markdown(path) {
                    result.push(FileEvent::Deleted(path.clone()));
                }
            }
        }
        _ => {}
    }

    result
}

/// Coalesce a batch of file events, eliminating redundant operations.
///
/// Rules:
/// - `Changed`/`Created` for same path → keep one (deduplicate)
/// - `Deleted` cancels prior `Changed`/`Created` for same path
/// - `Created`/`Changed` after `Deleted` for same path → resolve to index (file recreated)
/// - `Renamed` events tracked separately, old path removed from pending actions
///
/// Returns `(renames, index_paths, delete_paths)`.
pub fn coalesce_events(
    events: Vec<FileEvent>,
) -> (Vec<(PathBuf, PathBuf)>, Vec<PathBuf>, Vec<PathBuf>) {
    use std::collections::{HashMap, HashSet};

    let mut renames: Vec<(PathBuf, PathBuf)> = Vec::new();
    let mut renamed_from: HashSet<PathBuf> = HashSet::new();

    // Track last action per path: true = index, false = delete
    let mut actions: HashMap<PathBuf, bool> = HashMap::new();

    for event in events {
        match event {
            FileEvent::Renamed { from, to } => {
                actions.remove(&from);
                renamed_from.insert(from.clone());
                renames.push((from, to));
            }
            FileEvent::Created(path) | FileEvent::Changed(path) => {
                actions.insert(path, true);
            }
            FileEvent::Deleted(path) => {
                actions.insert(path, false);
            }
        }
    }

    let mut index_paths: Vec<PathBuf> = Vec::new();
    let mut delete_paths: Vec<PathBuf> = Vec::new();

    for (path, should_index) in actions {
        if renamed_from.contains(&path) {
            continue;
        }
        if should_index {
            index_paths.push(path);
        } else {
            delete_paths.push(path);
        }
    }

    // Sort for deterministic output in tests
    index_paths.sort();
    delete_paths.sort();

    (renames, index_paths, delete_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify_debouncer_full::DebouncedEvent;
    use notify_debouncer_full::notify::Event;
    use notify_debouncer_full::notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
    use std::time::Instant;

    fn debounced(event: Event) -> DebouncedEvent {
        DebouncedEvent::new(event, Instant::now())
    }

    #[test]
    fn modify_name_both_two_paths_emits_renamed() {
        let event = debounced(Event {
            kind: EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            paths: vec!["/notes/old.md".into(), "/notes/new.md".into()],
            attrs: Default::default(),
        });
        let results = map_event(&event);
        assert_eq!(results.len(), 1);
        assert!(matches!(&results[0], FileEvent::Renamed { from, to }
            if from.to_str() == Some("/notes/old.md")
            && to.to_str() == Some("/notes/new.md")
        ));
    }

    #[test]
    fn modify_data_two_paths_emits_changed_not_renamed() {
        // This was the original bug: ModifyKind::Data with 2 paths was
        // misclassified as a rename.
        let event = debounced(Event {
            kind: EventKind::Modify(ModifyKind::Data(
                notify_debouncer_full::notify::event::DataChange::Any,
            )),
            paths: vec!["/notes/a.md".into(), "/notes/a.md".into()],
            attrs: Default::default(),
        });
        let results = map_event(&event);
        assert!(!results.is_empty());
        for r in &results {
            assert!(
                matches!(r, FileEvent::Changed(_)),
                "expected Changed, got {r:?}"
            );
        }
    }

    #[test]
    fn modify_name_single_path_emits_changed() {
        // A From or To event without its pair — treat as Changed so the
        // file gets re-indexed at its current path.
        let event = debounced(Event {
            kind: EventKind::Modify(ModifyKind::Name(RenameMode::From)),
            paths: vec!["/notes/moved.md".into()],
            attrs: Default::default(),
        });
        let results = map_event(&event);
        assert_eq!(results.len(), 1);
        assert!(
            matches!(&results[0], FileEvent::Changed(p) if p.to_str() == Some("/notes/moved.md"))
        );
    }

    #[test]
    fn modify_data_single_path_emits_changed() {
        let event = debounced(Event {
            kind: EventKind::Modify(ModifyKind::Data(
                notify_debouncer_full::notify::event::DataChange::Content,
            )),
            paths: vec!["/notes/edited.md".into()],
            attrs: Default::default(),
        });
        let results = map_event(&event);
        assert_eq!(results.len(), 1);
        assert!(
            matches!(&results[0], FileEvent::Changed(p) if p.to_str() == Some("/notes/edited.md"))
        );
    }

    #[test]
    fn non_markdown_files_are_filtered() {
        let event = debounced(Event {
            kind: EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            paths: vec!["/notes/old.txt".into(), "/notes/new.txt".into()],
            attrs: Default::default(),
        });
        assert!(map_event(&event).is_empty());
    }

    #[test]
    fn create_emits_created() {
        let event = debounced(Event {
            kind: EventKind::Create(CreateKind::File),
            paths: vec!["/notes/new.md".into()],
            attrs: Default::default(),
        });
        let results = map_event(&event);
        assert_eq!(results.len(), 1);
        assert!(matches!(&results[0], FileEvent::Created(_)));
    }

    #[test]
    fn remove_emits_deleted() {
        let event = debounced(Event {
            kind: EventKind::Remove(RemoveKind::File),
            paths: vec!["/notes/gone.md".into()],
            attrs: Default::default(),
        });
        let results = map_event(&event);
        assert_eq!(results.len(), 1);
        assert!(matches!(&results[0], FileEvent::Deleted(_)));
    }

    // ─── coalesce_events tests ───────────────────────────────────

    #[test]
    fn coalesce_create_then_delete() {
        let events = vec![
            FileEvent::Created("/notes/a.md".into()),
            FileEvent::Deleted("/notes/a.md".into()),
        ];
        let (renames, index, delete) = coalesce_events(events);
        assert!(renames.is_empty());
        assert!(index.is_empty());
        assert_eq!(delete.len(), 1);
    }

    #[test]
    fn coalesce_delete_then_create() {
        let events = vec![
            FileEvent::Deleted("/notes/a.md".into()),
            FileEvent::Created("/notes/a.md".into()),
        ];
        let (renames, index, delete) = coalesce_events(events);
        assert!(renames.is_empty());
        assert_eq!(index.len(), 1);
        assert!(delete.is_empty());
    }

    #[test]
    fn coalesce_duplicate_changes() {
        let events = vec![
            FileEvent::Changed("/notes/a.md".into()),
            FileEvent::Changed("/notes/a.md".into()),
            FileEvent::Changed("/notes/a.md".into()),
        ];
        let (renames, index, delete) = coalesce_events(events);
        assert!(renames.is_empty());
        assert_eq!(index.len(), 1);
        assert!(delete.is_empty());
    }

    #[test]
    fn coalesce_rename_clears_old_path() {
        let events = vec![
            FileEvent::Changed("/notes/old.md".into()),
            FileEvent::Renamed {
                from: "/notes/old.md".into(),
                to: "/notes/new.md".into(),
            },
        ];
        let (renames, index, delete) = coalesce_events(events);
        assert_eq!(renames.len(), 1);
        assert!(index.is_empty());
        assert!(delete.is_empty());
    }
}
