pub mod cycle;
pub mod events;
pub mod projections;
pub mod queries;

use std::path::PathBuf;

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
        self.db.with_conn(|conn| {
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
            .with_conn(|conn| projections::rebuild(conn, &all_events))
    }

    /// List tasks that are ready to work on (no unresolved deps, not blocked).
    pub fn list_ready(&self) -> Result<Vec<queries::TaskRow>> {
        self.db.with_conn(queries::list_ready)
    }

    /// List tasks that are blocked (unresolved deps or explicit blocked_reason).
    pub fn list_blocked(&self) -> Result<Vec<queries::TaskRow>> {
        self.db.with_conn(queries::list_blocked)
    }

    /// List all tasks.
    pub fn list_all(&self) -> Result<Vec<queries::TaskRow>> {
        self.db.with_conn(queries::list_all)
    }

    /// Get a single task by ID.
    pub fn get_task(&self, task_id: &str) -> Result<Option<queries::TaskRow>> {
        self.db.with_conn(|conn| queries::get_task(conn, task_id))
    }

    /// List task IDs that became unblocked because `completed_task_id` was resolved.
    pub fn list_newly_unblocked(&self, completed_task_id: &str) -> Result<Vec<String>> {
        self.db
            .with_conn(|conn| queries::list_newly_unblocked(conn, completed_task_id))
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

            EventType::DependencyRemoved | EventType::NoteLinked | EventType::NoteUnlinked => {
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
        TaskEvent {
            event_id: new_event_id(),
            task_id: task_id.to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::TaskCreated,
            payload: serde_json::to_value(TaskCreatedPayload {
                title: title.to_string(),
                description: None,
                priority,
                status: "open".to_string(),
                due_ts: None,
            })
            .unwrap(),
        }
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
        let dep_event = TaskEvent {
            event_id: new_event_id(),
            task_id: "t2".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::DependencyAdded,
            payload: serde_json::to_value(DependencyPayload {
                depends_on_task_id: "t1".to_string(),
            })
            .unwrap(),
        };
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
        let done_event = TaskEvent {
            event_id: new_event_id(),
            task_id: "t1".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::StatusChanged,
            payload: serde_json::to_value(StatusChangedPayload {
                new_status: "done".to_string(),
            })
            .unwrap(),
        };
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

        let ev = TaskEvent {
            event_id: new_event_id(),
            task_id: "nonexistent".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::StatusChanged,
            payload: serde_json::to_value(StatusChangedPayload {
                new_status: "done".to_string(),
            })
            .unwrap(),
        };
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
        let dep1 = TaskEvent {
            event_id: new_event_id(),
            task_id: "t1".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::DependencyAdded,
            payload: serde_json::to_value(DependencyPayload {
                depends_on_task_id: "t2".to_string(),
            })
            .unwrap(),
        };
        store.append(&dep1).unwrap();

        // t2 depends on t1 — cycle!
        let dep2 = TaskEvent {
            event_id: new_event_id(),
            task_id: "t2".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::DependencyAdded,
            payload: serde_json::to_value(DependencyPayload {
                depends_on_task_id: "t1".to_string(),
            })
            .unwrap(),
        };
        let result = store.append(&dep2);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cycle"));
    }

    #[test]
    fn test_dep_on_nonexistent_rejected() {
        let (_dir, store) = setup();
        store.append(&created_event("t1", "Task 1", 2)).unwrap();

        let dep = TaskEvent {
            event_id: new_event_id(),
            task_id: "t1".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::DependencyAdded,
            payload: serde_json::to_value(DependencyPayload {
                depends_on_task_id: "nonexistent".to_string(),
            })
            .unwrap(),
        };
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
}
