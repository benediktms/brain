pub mod capsule;
pub mod cycle;
pub mod enrichment;
pub mod events;
pub mod import_beads;
pub mod projections;
pub mod queries;

use std::path::PathBuf;

use std::collections::HashMap;

use crate::db::Db;
use crate::error::{BrainCoreError, Result};

use events::TaskEvent;

/// The task store: SQLite as source of truth, JSONL as audit trail.
pub struct TaskStore {
    events_path: PathBuf,
    db: Db,
    /// Brain ID that scopes this store's reads and writes.
    ///
    /// Empty string means "all brains" (legacy / single-brain mode).
    /// Non-empty means filter reads by this brain_id and stamp it on writes.
    pub brain_id: String,
    /// Optional path to the project-local audit trail JSONL file.
    ///
    /// When set, every successful `append()` also emits the event to this
    /// path (`.brain/tasks/events.jsonl` inside the project repo), making
    /// task history git-trackable. Emission is best-effort: failures are
    /// logged as warnings and do not affect the SQLite write.
    pub audit_path: Option<PathBuf>,
}

impl TaskStore {
    /// Create a new TaskStore.
    ///
    /// `tasks_dir` is the directory containing (or that will contain) `events.jsonl`.
    /// It will be created if it does not exist.
    /// `brain_id` scopes reads and writes to a specific brain. Pass `""` for
    /// legacy single-brain mode (no filter on reads, empty string on writes).
    pub fn new(tasks_dir: &std::path::Path, db: Db) -> Result<Self> {
        std::fs::create_dir_all(tasks_dir)?;
        Ok(Self {
            events_path: tasks_dir.join("events.jsonl"),
            db,
            brain_id: String::new(),
            audit_path: None,
        })
    }

    /// Create a new TaskStore with an explicit brain_id scope.
    pub fn with_brain_id(
        tasks_dir: &std::path::Path,
        db: Db,
        brain_id: &str,
        brain_name: &str,
    ) -> Result<Self> {
        std::fs::create_dir_all(tasks_dir)?;
        // Ensure the brain is registered — FK on brain_id requires it.
        if !brain_id.is_empty() {
            db.ensure_brain_registered(brain_id, brain_name)?;
        }
        Ok(Self {
            events_path: tasks_dir.join("events.jsonl"),
            db,
            brain_id: brain_id.to_string(),
            audit_path: None,
        })
    }

    /// Set the project-local audit trail path.
    ///
    /// When set, successful `append()` calls also emit the event to this path
    /// (typically `<project_root>/.brain/tasks/events.jsonl`) as a best-effort
    /// git-trackable audit trail. Parent directories are created on first write.
    pub fn with_audit_path(mut self, path: PathBuf) -> Self {
        self.audit_path = Some(path);
        self
    }

    /// Append a validated event: write to SQLite first, then emit to the JSONL audit log.
    ///
    /// Validation runs before any write:
    /// - `TaskCreated`: task_id must NOT already exist
    /// - `TaskUpdated`/`StatusChanged`: task must exist
    /// - `DependencyAdded`: both tasks must exist, no cycle
    /// - `DependencyRemoved`: task must exist
    /// - `NoteLinked`/`NoteUnlinked`: task must exist
    ///
    /// SQLite is the authoritative write — if it fails, the operation fails. The JSONL
    /// append is a best-effort audit trail; failure is logged as a warning but does not
    /// roll back the SQLite write.
    pub fn append(&self, event: &TaskEvent) -> Result<()> {
        let brain_id = self.brain_id.clone();
        self.db.with_write_conn(|conn| {
            // Validate + apply in one transaction (validation + projection inside brain_persistence)
            projections::validate_and_apply(conn, event, &brain_id)
        })?;
        // JSONL emit is best-effort audit trail — outside the write lock
        if let Err(e) = events::append_event(&self.events_path, event) {
            tracing::warn!("failed to append task event to audit log: {e}");
        }
        // Project-local audit trail (git-trackable) — also best-effort
        if let Some(ref audit) = self.audit_path
            && let Some(parent) = audit.parent()
        {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::warn!("failed to create project audit dir: {e}");
            } else if let Err(e) = events::append_event(audit, event) {
                tracing::warn!("failed to append task event to project audit log: {e}");
            }
        }
        Ok(())
    }

    /// Rebuild all SQLite projections from the event log.
    pub fn rebuild_projections(&self) -> Result<()> {
        let all_events = events::read_all_events(&self.events_path)?;
        self.db
            .with_write_conn(|conn| projections::rebuild(conn, &all_events))
    }

    /// Rewrite all task IDs in the event log from `old_prefix` to `new_prefix`.
    ///
    /// Backs up `events.jsonl` to `events.jsonl.bak`, rewrites all task ID
    /// references (top-level `task_id` and any task IDs in payloads), then
    /// rebuilds the SQLite projection.
    ///
    /// Returns the number of events rewritten.
    pub fn rewrite_prefix(&self, old_prefix: &str, new_prefix: &str) -> Result<usize> {
        let all_events = events::read_all_events(&self.events_path)?;
        if all_events.is_empty() {
            return Ok(0);
        }

        // Back up the original event log
        let backup_path = self.events_path.with_extension("jsonl.bak");
        std::fs::copy(&self.events_path, &backup_path)?;

        let old_pat = format!("{old_prefix}-");
        let new_pat = format!("{new_prefix}-");

        let mut rewritten = Vec::with_capacity(all_events.len());
        for mut event in all_events {
            // Replace in top-level task_id
            if event.task_id.starts_with(&old_pat) {
                event.task_id = format!("{new_pat}{}", &event.task_id[old_pat.len()..]);
            }
            // Replace task ID references in payload values
            rewrite_task_ids_in_value(&mut event.payload, &old_pat, &new_pat);
            rewritten.push(event);
        }

        // Write to temp file, then rename for atomicity
        let tmp_path = self.events_path.with_extension("jsonl.tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp_path)?;
            for event in &rewritten {
                let mut line = serde_json::to_string(event)
                    .map_err(|e| BrainCoreError::TaskEvent(format!("serialize: {e}")))?;
                line.push('\n');
                file.write_all(line.as_bytes())?;
            }
            file.flush()?;
            file.sync_data()?;
        }
        std::fs::rename(&tmp_path, &self.events_path)?;

        // Rebuild SQLite projection from rewritten events
        self.rebuild_projections()?;

        let count = rewritten.len();
        Ok(count)
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
    pub fn resolve_task_id(&self, input: &str) -> Result<String> {
        self.db
            .with_read_conn(|conn| queries::resolve_task_id(conn, input))
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
            let brain_dir = self
                .events_path
                .parent()
                .and_then(|p| p.parent())
                .unwrap_or(std::path::Path::new("."));
            crate::db::meta::get_or_init_project_prefix(conn, brain_dir)
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
                // Skip events that already exist in SQLite (idempotent import).
                // We detect duplicates by attempting the apply and swallowing
                // constraint violations.
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

/// Recursively replace task ID prefixes in a JSON value.
fn rewrite_task_ids_in_value(value: &mut serde_json::Value, old_pat: &str, new_pat: &str) {
    match value {
        serde_json::Value::String(s) => {
            if s.starts_with(old_pat) {
                *s = format!("{new_pat}{}", &s[old_pat.len()..]);
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                rewrite_task_ids_in_value(v, old_pat, new_pat);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                rewrite_task_ids_in_value(v, old_pat, new_pat);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use events::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, TaskStore) {
        let dir = TempDir::new().unwrap();
        let db = Db::open_in_memory().unwrap();
        let tasks_dir = dir.path().join("tasks");
        let store = TaskStore::new(&tasks_dir, db).unwrap();
        (dir, store)
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
            },
        )
    }

    #[test]
    fn test_full_lifecycle() {
        let (_dir, store) = setup();

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
        let (_dir, store) = setup();
        store.append(&created_event("t1", "Task", 2)).unwrap();

        let result = store.append(&created_event("t1", "Duplicate", 2));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn test_update_nonexistent_rejected() {
        let (_dir, store) = setup();

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
        let (_dir, store) = setup();
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
        let (_dir, store) = setup();
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

    #[test]
    fn test_rebuild_from_log() {
        let (_dir, store) = setup();

        store.append(&created_event("t1", "Task 1", 2)).unwrap();
        store.append(&created_event("t2", "Task 2", 1)).unwrap();

        // Verify state
        assert_eq!(store.list_all().unwrap().len(), 2);

        // Rebuild
        store.rebuild_projections().unwrap();

        // Same state after rebuild
        let all = store.list_all().unwrap();
        assert_eq!(all.len(), 2);

        let t1 = store.get_task("t1").unwrap().unwrap();
        assert_eq!(t1.title, "Task 1");
        assert_eq!(t1.priority, 2);
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = TempDir::new().unwrap();
        let tasks_dir = dir.path().join("tasks");

        // Write events with one store instance
        {
            let db = Db::open_in_memory().unwrap();
            let store = TaskStore::new(&tasks_dir, db).unwrap();
            store.append(&created_event("t1", "Persisted", 1)).unwrap();
        }

        // Reopen with a fresh DB and rebuild
        {
            let db = Db::open_in_memory().unwrap();
            let store = TaskStore::new(&tasks_dir, db).unwrap();
            store.rebuild_projections().unwrap();

            let task = store.get_task("t1").unwrap().unwrap();
            assert_eq!(task.title, "Persisted");
        }
    }

    #[test]
    fn test_rewrite_prefix() {
        let (dir, store) = setup();

        // Create tasks with OLD- prefix
        store
            .append(&created_event("OLD-001", "Task 1", 2))
            .unwrap();
        store
            .append(&created_event("OLD-002", "Task 2", 1))
            .unwrap();

        // Add a dependency (payload contains task ID reference)
        let dep_event = TaskEvent::new(
            "OLD-002",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "OLD-001".to_string(),
            },
        );
        store.append(&dep_event).unwrap();

        // Rewrite OLD -> NEW
        let count = store.rewrite_prefix("OLD", "NEW").unwrap();
        assert_eq!(count, 3);

        // Verify tasks are now under the new prefix
        let task1 = store.get_task("NEW-001").unwrap();
        assert!(task1.is_some(), "task should exist with NEW- prefix");
        assert_eq!(task1.unwrap().title, "Task 1");

        let task2 = store.get_task("NEW-002").unwrap();
        assert!(task2.is_some());

        // Old prefix should no longer exist
        assert!(store.get_task("OLD-001").unwrap().is_none());

        // Verify the dependency was rewritten too
        let blocked = store.list_blocked().unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].task_id, "NEW-002");

        // Verify backup was created
        let backup = dir.path().join("tasks").join("events.jsonl.bak");
        assert!(backup.exists());
    }

    #[test]
    fn test_rewrite_prefix_empty_log() {
        let (_dir, store) = setup();
        let count = store.rewrite_prefix("OLD", "NEW").unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_rewrite_prefix_no_match() {
        let (_dir, store) = setup();
        store.append(&created_event("AAA-001", "Task", 2)).unwrap();

        // Rewrite a prefix that doesn't match — events still rewritten (count=1) but IDs unchanged
        let count = store.rewrite_prefix("ZZZ", "NEW").unwrap();
        assert_eq!(count, 1);

        // Original task should still exist
        let task = store.get_task("AAA-001").unwrap();
        assert!(task.is_some());
    }

    // ─── B2: SQLite-first write semantics ────────────────────────────

    #[test]
    fn test_sqlite_write_succeeds_when_jsonl_dir_is_read_only() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let tasks_dir = dir.path().join("tasks");
        let db = Db::open_in_memory().unwrap();
        let store = TaskStore::new(&tasks_dir, db).unwrap();

        // Make the tasks directory read-only so JSONL append will fail
        let perms = fs::Permissions::from_mode(0o555);
        fs::set_permissions(&tasks_dir, perms).unwrap();

        // SQLite write must succeed even though JSONL is unavailable
        let result = store.append(&created_event("t1", "Read-only Test", 2));
        assert!(
            result.is_ok(),
            "append must succeed when JSONL dir is read-only"
        );

        // Task exists in SQLite
        let task = store.get_task("t1").unwrap();
        assert!(task.is_some(), "task must be in SQLite after write");
        assert_eq!(task.unwrap().title, "Read-only Test");

        // Restore permissions so TempDir cleanup works
        let perms = fs::Permissions::from_mode(0o755);
        fs::set_permissions(&tasks_dir, perms).unwrap();
    }

    #[test]
    fn test_jsonl_audit_trail_populated_on_success() {
        use std::fs;

        let dir = TempDir::new().unwrap();
        let tasks_dir = dir.path().join("tasks");
        let db = Db::open_in_memory().unwrap();
        let store = TaskStore::new(&tasks_dir, db).unwrap();

        store.append(&created_event("t1", "Audit Task", 3)).unwrap();

        // SQLite has the task
        assert!(store.get_task("t1").unwrap().is_some());

        // JSONL also has the event
        let jsonl_path = tasks_dir.join("events.jsonl");
        assert!(
            jsonl_path.exists(),
            "events.jsonl must exist after successful write"
        );
        let content = fs::read_to_string(&jsonl_path).unwrap();
        assert!(!content.is_empty(), "events.jsonl must contain the event");
        assert!(
            content.contains("t1"),
            "events.jsonl must reference the task id"
        );
    }

    #[test]
    fn test_rebuild_from_jsonl_recovers_projections() {
        let dir = TempDir::new().unwrap();
        let tasks_dir = dir.path().join("tasks");
        let db = Db::open_in_memory().unwrap();
        let store = TaskStore::new(&tasks_dir, db).unwrap();

        store.append(&created_event("t1", "Task One", 2)).unwrap();
        store.append(&created_event("t2", "Task Two", 1)).unwrap();

        // Verify both exist
        assert_eq!(store.list_all().unwrap().len(), 2);

        // Wipe SQLite projections manually
        store
            .db
            .with_write_conn(|conn| conn.execute_batch("DELETE FROM tasks").map_err(Into::into))
            .unwrap();
        assert_eq!(store.list_all().unwrap().len(), 0, "SQLite wiped");

        // Rebuild from JSONL
        store.rebuild_projections().unwrap();

        // All tasks restored
        let all = store.list_all().unwrap();
        assert_eq!(all.len(), 2);
        assert!(store.get_task("t1").unwrap().is_some());
        assert_eq!(store.get_task("t2").unwrap().unwrap().title, "Task Two");
    }
}
