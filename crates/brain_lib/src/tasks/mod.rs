pub mod capsule;
pub mod cycle;
pub mod enrichment;
pub mod events;
pub mod import_beads;
pub mod projections;
pub mod queries;

use std::collections::HashMap;

use crate::error::Result;
use brain_persistence::db::Db;

use events::TaskEvent;

/// The task store: SQLite is the sole source of truth.
pub struct TaskStore {
    db: Db,
    /// Brain ID that scopes this store's reads and writes.
    ///
    /// Empty string means "all brains" (legacy / single-brain mode).
    /// Non-empty means filter reads by this brain_id and stamp it on writes.
    pub brain_id: String,
}

impl TaskStore {
    /// Create a new TaskStore.
    ///
    /// `brain_id` scopes reads and writes to a specific brain. Pass `""` for
    /// legacy single-brain mode (no filter on reads, empty string on writes).
    pub fn new(db: Db) -> Self {
        Self {
            db,
            brain_id: String::new(),
        }
    }

    /// Create a new TaskStore with an explicit brain_id scope.
    pub fn with_brain_id(db: Db, brain_id: &str, brain_name: &str) -> Result<Self> {
        // Ensure the brain is registered — FK on brain_id requires it.
        if !brain_id.is_empty() {
            db.ensure_brain_registered(brain_id, brain_name)?;
        }
        Ok(Self {
            db,
            brain_id: brain_id.to_string(),
        })
    }

    /// Access the underlying Db handle.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Append a validated event to SQLite.
    ///
    /// Validation runs before any write:
    /// - `TaskCreated`: task_id must NOT already exist
    /// - `TaskUpdated`/`StatusChanged`: task must exist
    /// - `DependencyAdded`: both tasks must exist, no cycle
    /// - `DependencyRemoved`: task must exist
    /// - `NoteLinked`/`NoteUnlinked`: task must exist
    pub fn append(&self, event: &TaskEvent) -> Result<()> {
        let brain_id = self.brain_id.clone();
        self.db
            .with_write_conn(|conn| projections::validate_and_apply(conn, event, &brain_id))
    }

    /// List tasks that are ready to work on (no unresolved deps, not blocked).
    pub fn list_ready(&self) -> Result<Vec<queries::TaskRow>> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            let filter = if brain_id.is_empty() {
                None
            } else {
                Some(brain_id.as_str())
            };
            queries::list_ready(conn, filter)
        })
    }

    /// List tasks that are blocked (unresolved deps or explicit blocked_reason).
    pub fn list_blocked(&self) -> Result<Vec<queries::TaskRow>> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            let filter = if brain_id.is_empty() {
                None
            } else {
                Some(brain_id.as_str())
            };
            queries::list_blocked(conn, filter)
        })
    }

    /// List all tasks.
    pub fn list_all(&self) -> Result<Vec<queries::TaskRow>> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            let filter = if brain_id.is_empty() {
                None
            } else {
                Some(brain_id.as_str())
            };
            queries::list_all(conn, filter)
        })
    }

    /// List open tasks (excludes done/cancelled).
    pub fn list_open(&self) -> Result<Vec<queries::TaskRow>> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            let filter = if brain_id.is_empty() {
                None
            } else {
                Some(brain_id.as_str())
            };
            queries::list_open(conn, filter)
        })
    }

    /// List done/cancelled tasks.
    pub fn list_done(&self) -> Result<Vec<queries::TaskRow>> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            let filter = if brain_id.is_empty() {
                None
            } else {
                Some(brain_id.as_str())
            };
            queries::list_done(conn, filter)
        })
    }

    /// List tasks with status exactly 'in_progress'.
    pub fn list_in_progress(&self) -> Result<Vec<queries::TaskRow>> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            let filter = if brain_id.is_empty() {
                None
            } else {
                Some(brain_id.as_str())
            };
            queries::list_in_progress(conn, filter)
        })
    }

    /// List tasks with status exactly 'cancelled'.
    pub fn list_cancelled(&self) -> Result<Vec<queries::TaskRow>> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            let filter = if brain_id.is_empty() {
                None
            } else {
                Some(brain_id.as_str())
            };
            queries::list_cancelled(conn, filter)
        })
    }

    /// List ready tasks excluding epics (actionable work items only).
    pub fn list_ready_actionable(&self) -> Result<Vec<queries::TaskRow>> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            let filter = if brain_id.is_empty() {
                None
            } else {
                Some(brain_id.as_str())
            };
            queries::list_ready_actionable(conn, filter)
        })
    }

    /// Get a single task by ID.
    pub fn get_task(&self, task_id: &str) -> Result<Option<queries::TaskRow>> {
        self.db
            .with_read_conn(|conn| queries::get_task(conn, task_id))
    }

    /// List task IDs that became unblocked because `completed_task_id` was resolved.
    pub fn list_newly_unblocked(&self, completed_task_id: &str) -> Result<Vec<String>> {
        self.db
            .with_read_conn(|conn| queries::list_newly_unblocked(conn, completed_task_id))
    }

    /// Get the dependency summary for a task.
    pub fn get_dependency_summary(&self, task_id: &str) -> Result<queries::DependencySummary> {
        self.db
            .with_read_conn(|conn| queries::get_dependency_summary(conn, task_id))
    }

    /// Get note links for a task.
    pub fn get_task_note_links(&self, task_id: &str) -> Result<Vec<queries::TaskNoteLink>> {
        self.db
            .with_read_conn(|conn| queries::get_task_note_links(conn, task_id))
    }

    /// Get labels for a task.
    pub fn get_task_labels(&self, task_id: &str) -> Result<Vec<String>> {
        self.db
            .with_read_conn(|conn| queries::get_task_labels(conn, task_id))
    }

    /// Batch-fetch labels for a set of task IDs.
    pub fn get_labels_for_tasks(&self, task_ids: &[&str]) -> Result<HashMap<String, Vec<String>>> {
        self.db
            .with_read_conn(|conn| queries::get_labels_for_tasks(conn, task_ids))
    }

    /// Get comments for a task.
    pub fn get_task_comments(&self, task_id: &str) -> Result<Vec<queries::TaskComment>> {
        self.db
            .with_read_conn(|conn| queries::get_task_comments(conn, task_id))
    }

    /// Get child tasks of a parent.
    pub fn get_children(&self, parent_task_id: &str) -> Result<Vec<queries::TaskRow>> {
        self.db
            .with_read_conn(|conn| queries::get_children(conn, parent_task_id))
    }

    /// Get tasks that depend on the given task and are not yet resolved (reverse deps).
    pub fn get_tasks_blocking(&self, task_id: &str) -> Result<Vec<queries::TaskRow>> {
        self.db
            .with_read_conn(|conn| queries::get_tasks_blocking(conn, task_id))
    }

    /// Count of ready and blocked tasks.
    pub fn count_ready_blocked(&self) -> Result<(usize, usize)> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            let filter = if brain_id.is_empty() {
                None
            } else {
                Some(brain_id.as_str())
            };
            queries::count_ready_blocked(conn, filter)
        })
    }

    /// Count tasks grouped by status.
    pub fn count_by_status(&self) -> Result<queries::StatusCounts> {
        self.db.with_read_conn(queries::count_by_status)
    }

    /// Get external ID references for a task.
    pub fn get_external_ids(&self, task_id: &str) -> Result<Vec<queries::ExternalIdRow>> {
        self.db
            .with_read_conn(|conn| queries::get_external_ids(conn, task_id))
    }

    /// Resolve an external ID to a brain task_id.
    pub fn resolve_external_id(&self, source: &str, external_id: &str) -> Result<Option<String>> {
        self.db
            .with_read_conn(|conn| queries::resolve_external_id(conn, source, external_id))
    }

    /// List all dependency edges (bulk load for export).
    pub fn list_all_deps(&self) -> Result<Vec<queries::TaskDep>> {
        self.db.with_read_conn(queries::list_all_deps)
    }

    /// List all (task_id, label) pairs (bulk load for export).
    pub fn list_all_labels(&self) -> Result<Vec<(String, String)>> {
        self.db.with_read_conn(queries::list_all_labels)
    }

    /// Get all labels with counts and associated task IDs.
    pub fn label_summary(&self) -> Result<Vec<queries::LabelSummary>> {
        self.db.with_read_conn(queries::label_summary)
    }

    /// Append a batch of events, collecting individual results.
    ///
    /// Each event is individually validated/written/applied. Errors on one event
    /// do not abort subsequent events (partial-success semantics).
    ///
    /// Returns domain errors (`BrainCoreError`) so callers can match on specific
    /// variants (e.g. `TaskCycle`, `TaskEvent`). CLI callers can convert to
    /// `anyhow::Error` via `?` since `BrainCoreError` implements `std::error::Error`.
    pub fn append_batch(&self, events: &[TaskEvent]) -> Vec<Result<()>> {
        events.iter().map(|e| self.append(e)).collect()
    }

    /// Get all task IDs that have a given label.
    pub fn get_task_ids_with_label(&self, label: &str) -> Result<Vec<String>> {
        self.db
            .with_read_conn(|conn| queries::get_task_ids_with_label(conn, label))
    }

    /// Get all dependency targets for a task (what it depends on).
    pub fn get_deps_for_task(&self, task_id: &str) -> Result<Vec<String>> {
        self.db
            .with_read_conn(|conn| queries::get_deps_for_task(conn, task_id))
    }

    /// Full-text search on task title and description.
    pub fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<String>> {
        self.db
            .with_read_conn(|conn| queries::search_tasks_fts(conn, query, limit))
    }

    /// Resolve a task ID from an exact match or unique prefix.
    ///
    /// When the store is scoped to a brain, resolution is filtered to that
    /// brain's tasks — preventing cross-brain collisions on short hashes.
    ///
    /// If the input has a prefix (e.g. "ckt-ebd") that maps to a different
    /// brain, resolution automatically switches to that brain's scope. This
    /// ensures cross-brain task references work from any store context.
    pub fn resolve_task_id(&self, input: &str) -> Result<String> {
        let brain_id = self.brain_id.clone();
        self.db.with_read_conn(move |conn| {
            // If input has a prefix pointing to a different brain, use that brain's scope.
            let effective_brain_id = if !brain_id.is_empty() {
                queries::resolve_brain_from_prefix(conn, input).unwrap_or(Some(brain_id))
            } else {
                // Unscoped — let resolve_task_id_scoped handle prefix derivation
                None
            };
            let filter = effective_brain_id.as_deref();
            queries::resolve_task_id_scoped(conn, input, filter)
        })
    }

    /// Compute compact display IDs for all tasks (batch).
    pub fn compact_ids(&self) -> Result<HashMap<String, String>> {
        self.db.with_read_conn(queries::compact_ids)
    }

    /// Compute compact display ID for a single task.
    pub fn compact_id(&self, task_id: &str) -> Result<String> {
        self.db
            .with_read_conn(|conn| queries::compact_id(conn, task_id))
    }

    /// Get the project prefix for this brain.
    ///
    /// Reads from `brains.prefix` (per-brain column). If `brain_id` is not
    /// set or the prefix is missing/invalid, returns an error.
    pub fn get_project_prefix(&self) -> Result<String> {
        let brain_id = self.brain_id.clone();
        if !brain_id.is_empty() {
            let result = self.db.with_read_conn(|conn| {
                let prefix: Option<String> = conn
                    .query_row(
                        "SELECT prefix FROM brains WHERE brain_id = ?1",
                        [&brain_id],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    .ok()
                    .flatten();
                Ok(prefix)
            })?;
            if let Some(ref prefix) =
                result.filter(|p| p.len() == 3 && p.chars().all(|c| c.is_ascii_uppercase()))
            {
                return Ok(prefix.clone());
            }
            return Err(crate::error::BrainCoreError::Config(
                "brains.prefix not set for this brain".into(),
            ));
        }
        // Unscoped/legacy mode: fall back to brain_meta
        self.db.with_write_conn(|conn| {
            brain_persistence::db::meta::get_or_init_project_prefix(conn, std::path::Path::new("."))
        })
    }

    /// Import events from a JSONL file into the unified SQLite database.
    ///
    /// Reads the given path as a JSONL event log and replays all events into
    /// SQLite via `projections::apply_event`. Events that already exist (by
    /// `event_id`) or would violate constraints are silently skipped to make
    /// this safe to call multiple times (idempotent).
    ///
    /// This is intended for one-time migration when `brain init` is run on a
    /// cloned repo that already has a `.brain/tasks/events.jsonl` file.
    pub fn import_from_jsonl(&self, path: &std::path::Path) -> Result<usize> {
        if !path.exists() {
            return Ok(0);
        }
        let all_events = events::read_all_events(path)?;
        if all_events.is_empty() {
            return Ok(0);
        }
        let brain_id = self.brain_id.clone();
        let mut imported = 0usize;
        self.db.with_write_conn(|conn| {
            for event in &all_events {
                match projections::apply_event(conn, event, &brain_id) {
                    Ok(()) => imported += 1,
                    Err(e) => {
                        tracing::debug!(
                            event_id = %event.event_id,
                            "skipping event during import: {e}"
                        );
                    }
                }
            }
            Ok(())
        })?;
        Ok(imported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use events::*;

    fn setup() -> TaskStore {
        let db = Db::open_in_memory().unwrap();
        TaskStore::new(db)
    }

    fn created_event(task_id: &str, title: &str, priority: i32) -> TaskEvent {
        TaskEvent::from_payload(
            task_id,
            "user",
            TaskCreatedPayload {
                title: title.to_string(),
                description: None,
                priority,
                status: TaskStatus::Open,
                due_ts: None,
                task_type: None,
                assignee: None,
                defer_until: None,
                parent_task_id: None,
                display_id: None,
            },
        )
    }

    #[test]
    fn test_full_lifecycle() {
        let store = setup();

        // Create two tasks
        store.append(&created_event("t1", "Task 1", 2)).unwrap();
        store.append(&created_event("t2", "Task 2", 1)).unwrap();

        // Both should be ready
        let ready = store.list_ready().unwrap();
        assert_eq!(ready.len(), 2);

        // Add dependency: t2 depends on t1
        let dep_event = TaskEvent::new(
            "t2",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "t1".to_string(),
            },
        );
        store.append(&dep_event).unwrap();

        // Only t1 ready now
        let ready = store.list_ready().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, "t1");

        // t2 should be blocked
        let blocked = store.list_blocked().unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].task_id, "t2");

        // Complete t1
        let done_event = TaskEvent::from_payload(
            "t1",
            "user",
            StatusChangedPayload {
                new_status: TaskStatus::Done,
            },
        );
        store.append(&done_event).unwrap();

        // t2 now ready
        let ready = store.list_ready().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, "t2");
    }

    #[test]
    fn test_duplicate_task_rejected() {
        let store = setup();
        store.append(&created_event("t1", "Task", 2)).unwrap();

        let result = store.append(&created_event("t1", "Duplicate", 2));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn test_update_nonexistent_rejected() {
        let store = setup();

        let ev = TaskEvent::from_payload(
            "nonexistent",
            "user",
            StatusChangedPayload {
                new_status: TaskStatus::Done,
            },
        );
        let result = store.append(&ev);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_cycle_rejected() {
        let store = setup();
        store.append(&created_event("t1", "Task 1", 2)).unwrap();
        store.append(&created_event("t2", "Task 2", 2)).unwrap();

        // t1 depends on t2
        let dep1 = TaskEvent::new(
            "t1",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "t2".to_string(),
            },
        );
        store.append(&dep1).unwrap();

        // t2 depends on t1 — cycle!
        let dep2 = TaskEvent::new(
            "t2",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "t1".to_string(),
            },
        );
        let result = store.append(&dep2);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cycle"));
    }

    #[test]
    fn test_dep_on_nonexistent_rejected() {
        let store = setup();
        store.append(&created_event("t1", "Task 1", 2)).unwrap();

        let dep = TaskEvent::new(
            "t1",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "nonexistent".to_string(),
            },
        );
        let result = store.append(&dep);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("target not found"));
    }
}
