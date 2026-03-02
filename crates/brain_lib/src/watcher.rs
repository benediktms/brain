use std::path::PathBuf;
use std::time::Duration;

use notify_debouncer_full::notify::RecursiveMode;
use notify_debouncer_full::notify::event::EventKind;
use notify_debouncer_full::{DebounceEventResult, Debouncer, RecommendedCache, new_debouncer};
use tracing::{info, warn};

/// Events emitted by the file watcher.
#[derive(Debug)]
pub enum FileEvent {
    Changed(PathBuf),
    Created(PathBuf),
    Deleted(PathBuf),
    Renamed { from: PathBuf, to: PathBuf },
}

/// Watches directories for markdown file changes, debouncing and filtering events.
pub struct BrainWatcher {
    _debouncer: Debouncer<notify_debouncer_full::notify::RecommendedWatcher, RecommendedCache>,
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
        let mut debouncer = new_debouncer(
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
        )
        .map_err(|e| {
            crate::error::BrainCoreError::Io(std::io::Error::other(format!("watcher init: {e}")))
        })?;

        for dir in dirs {
            debouncer
                .watch(dir.as_path(), RecursiveMode::Recursive)
                .map_err(|e| {
                    crate::error::BrainCoreError::Io(std::io::Error::other(format!(
                        "watch dir: {e}"
                    )))
                })?;
            info!(dir = %dir.display(), "watching directory");
        }

        Ok(Self {
            _debouncer: debouncer,
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
