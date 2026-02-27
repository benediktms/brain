use std::path::PathBuf;
use std::time::Duration;

use notify_debouncer_full::notify::event::EventKind;
use notify_debouncer_full::notify::RecursiveMode;
use notify_debouncer_full::{new_debouncer, DebounceEventResult, Debouncer, RecommendedCache};
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
        EventKind::Modify(_) => {
            // Check for rename (2 paths: from, to)
            if event.paths.len() == 2 {
                let from = &event.paths[0];
                let to = &event.paths[1];
                if is_markdown(from) || is_markdown(to) {
                    result.push(FileEvent::Renamed {
                        from: from.clone(),
                        to: to.clone(),
                    });
                }
            } else {
                for path in &event.paths {
                    if is_markdown(path) {
                        result.push(FileEvent::Changed(path.clone()));
                    }
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
