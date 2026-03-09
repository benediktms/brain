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

use events::{EventType, TaskEvent};

/// The task store: event log (JSONL) as source of truth, SQLite as projection.
pub struct TaskStore {
    events_path: PathBuf,
    db: Db,
}

impl TaskStore {
    /// Create a new TaskStore.
    ///
    /// `tasks_dir` is the directory containing (or that will contain) `events.jsonl`.
    /// It will be created if it does not exist.
    pub fn new(tasks_dir: &std::path::Path, db: Db) -> Result<Self> {
        std::fs::create_dir_all(tasks_dir)?;
        Ok(Self {
            events_path: tasks_dir.join("events.jsonl"),
            db,
        })
    }

    /// Append a validated event to the log and apply it to the projection.
    ///
    /// Validation runs before the JSONL write to prevent log/projection divergence:
    /// - `TaskCreated`: task_id must NOT already exist
    /// - `TaskUpdated`/`StatusChanged`: task must exist
    /// - `DependencyAdded`: both tasks must exist, no cycle
    /// - `DependencyRemoved`: task must exist
    /// - `NoteLinked`/`NoteUnlinked`: task must exist
    pub fn append(&self, event: &TaskEvent) -> Result<()> {
        // Validate before writing to JSONL
        self.db.with_write_conn(|conn| {
            self.validate(conn, event)?;

            // Write to JSONL (source of truth)
            events::append_event(&self.events_path, event)?;

            // Apply to SQLite projection
            projections::apply_event(conn, event)?;

            Ok(())
        })
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
        self.db.with_read_conn(queries::list_ready)
    }

    /// List tasks that are blocked (unresolved deps or explicit blocked_reason).
    pub fn list_blocked(&self) -> Result<Vec<queries::TaskRow>> {
        self.db.with_read_conn(queries::list_blocked)
    }

    /// List all tasks.
    pub fn list_all(&self) -> Result<Vec<queries::TaskRow>> {
        self.db.with_read_conn(queries::list_all)
    }

    /// List open tasks (excludes done/cancelled).
    pub fn list_open(&self) -> Result<Vec<queries::TaskRow>> {
        self.db.with_read_conn(queries::list_open)
    }

    /// List done/cancelled tasks.
    pub fn list_done(&self) -> Result<Vec<queries::TaskRow>> {
        self.db.with_read_conn(queries::list_done)
    }

    /// List ready tasks excluding epics (actionable work items only).
    pub fn list_ready_actionable(&self) -> Result<Vec<queries::TaskRow>> {
        self.db.with_read_conn(queries::list_ready_actionable)
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
        self.db.with_read_conn(queries::count_ready_blocked)
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

    /// Compute shortest unique prefixes for all tasks.
    pub fn shortest_unique_prefixes(&self) -> Result<HashMap<String, String>> {
        self.db.with_read_conn(queries::shortest_unique_prefixes)
    }

    /// Compute shortest unique prefix for a single task.
    pub fn shortest_unique_prefix(&self, task_id: &str) -> Result<String> {
        self.db
            .with_read_conn(|conn| queries::shortest_unique_prefix(conn, task_id))
    }

    /// Get the project prefix, auto-generating from the brain directory name if needed.
    pub fn get_project_prefix(&self) -> Result<String> {
        self.db.with_write_conn(|conn| {
            // events_path = tasks_dir/events.jsonl, tasks_dir = brain_dir/tasks
            let brain_dir = self
                .events_path
                .parent()
                .and_then(|p| p.parent())
                .unwrap_or(std::path::Path::new("."));
            crate::db::meta::get_or_init_project_prefix(conn, brain_dir)
        })
    }

    /// Validate an event before writing it to the log.
    fn validate(&self, conn: &rusqlite::Connection, event: &TaskEvent) -> Result<()> {
        match event.event_type {
            EventType::TaskCreated => {
                if queries::task_exists(conn, &event.task_id)? {
                    return Err(BrainCoreError::TaskEvent(format!(
                        "task already exists: {}",
                        event.task_id
                    )));
                }
                // Validate parent_task_id if provided
                let payload: events::TaskCreatedPayload =
                    serde_json::from_value(event.payload.clone()).map_err(|e| {
                        BrainCoreError::TaskEvent(format!("bad TaskCreated payload: {e}"))
                    })?;
                if let Some(ref parent_id) = payload.parent_task_id {
                    if parent_id == &event.task_id {
                        return Err(BrainCoreError::TaskEvent(
                            "task cannot be its own parent".to_string(),
                        ));
                    }
                    if !queries::task_exists(conn, parent_id)? {
                        return Err(BrainCoreError::TaskEvent(format!(
                            "parent task not found: {parent_id}"
                        )));
                    }
                }
            }

            EventType::TaskUpdated | EventType::StatusChanged => {
                if !queries::task_exists(conn, &event.task_id)? {
                    return Err(BrainCoreError::TaskEvent(format!(
                        "task not found: {}",
                        event.task_id
                    )));
                }
            }

            EventType::DependencyAdded => {
                if !queries::task_exists(conn, &event.task_id)? {
                    return Err(BrainCoreError::TaskEvent(format!(
                        "task not found: {}",
                        event.task_id
                    )));
                }
                let payload: events::DependencyPayload =
                    serde_json::from_value(event.payload.clone()).map_err(|e| {
                        BrainCoreError::TaskEvent(format!("bad DependencyAdded payload: {e}"))
                    })?;
                if !queries::task_exists(conn, &payload.depends_on_task_id)? {
                    return Err(BrainCoreError::TaskEvent(format!(
                        "dependency target not found: {}",
                        payload.depends_on_task_id
                    )));
                }
                cycle::check_cycle(conn, &event.task_id, &payload.depends_on_task_id)?;
            }

            EventType::ParentSet => {
                if !queries::task_exists(conn, &event.task_id)? {
                    return Err(BrainCoreError::TaskEvent(format!(
                        "task not found: {}",
                        event.task_id
                    )));
                }
                let payload: events::ParentSetPayload =
                    serde_json::from_value(event.payload.clone()).map_err(|e| {
                        BrainCoreError::TaskEvent(format!("bad ParentSet payload: {e}"))
                    })?;
                if let Some(ref parent_id) = payload.parent_task_id {
                    if parent_id == &event.task_id {
                        return Err(BrainCoreError::TaskEvent(
                            "task cannot be its own parent".to_string(),
                        ));
                    }
                    if !queries::task_exists(conn, parent_id)? {
                        return Err(BrainCoreError::TaskEvent(format!(
                            "parent task not found: {parent_id}"
                        )));
                    }
                }
            }

            EventType::DependencyRemoved
            | EventType::NoteLinked
            | EventType::NoteUnlinked
            | EventType::LabelAdded
            | EventType::LabelRemoved
            | EventType::CommentAdded
            | EventType::ExternalIdAdded
            | EventType::ExternalIdRemoved => {
                if !queries::task_exists(conn, &event.task_id)? {
                    return Err(BrainCoreError::TaskEvent(format!(
                        "task not found: {}",
                        event.task_id
                    )));
                }
            }
        }
        Ok(())
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
        store
            .append(&created_event("AAA-001", "Task", 2))
            .unwrap();

        // Rewrite a prefix that doesn't match — events still rewritten (count=1) but IDs unchanged
        let count = store.rewrite_prefix("ZZZ", "NEW").unwrap();
        assert_eq!(count, 1);

        // Original task should still exist
        let task = store.get_task("AAA-001").unwrap();
        assert!(task.is_some());
    }
}
