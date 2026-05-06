pub mod status;
pub use status::SagaStatus;

use brain_persistence::db::Db;
use brain_persistence::db::sagas::SagaListFilter;
use brain_persistence::db::sagas::events::{
    SagaEvent, SagaEventType, SagaTaskPayload, SagaUpdatedPayload, new_saga_id,
};
use brain_persistence::db::sagas::queries::{self, SagaEventInsert, SagaRow};
use brain_persistence::db::tasks::queries::resolve_task_id_scoped;
use brain_persistence::error::BrainCoreError;

use crate::error::Result;

/// Store for saga lifecycle operations. Registry-level: not scoped to any brain.
pub struct SagaStore {
    db: Db,
}

impl SagaStore {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Create a new saga in `planning` status. Returns the resulting row.
    pub fn create(&self, title: &str, description: Option<&str>, actor: &str) -> Result<SagaRow> {
        if title.trim().is_empty() {
            return Err(brain_persistence::error::BrainCoreError::Parse(
                "saga title must not be empty".into(),
            ));
        }
        let saga_id = new_saga_id();
        let row = self.db.with_write_conn(|conn| {
            let row = queries::insert_saga(conn, &saga_id, title, description)?;

            let event = SagaEvent::new(
                &saga_id,
                actor,
                SagaEventType::SagaCreated,
                &serde_json::json!({ "title": title, "description": description }),
            );
            queries::insert_saga_event(
                conn,
                &SagaEventInsert {
                    event_id: &event.event_id,
                    saga_id: &event.saga_id,
                    event_type: &serde_json::to_string(&event.event_type)
                        .expect("SagaEventType serialization is infallible"),
                    timestamp: event.timestamp,
                    actor: &event.actor,
                    payload: &serde_json::to_string(&event.payload)?,
                },
            )?;

            Ok(row)
        })?;
        Ok(row)
    }

    /// Update title and/or description. At least one field required. Allowed in any status.
    ///
    /// `description` uses `Option<Option<&str>>`:
    /// - `None` = don't touch description
    /// - `Some(None)` = set description to NULL
    /// - `Some(Some("text"))` = set description to "text"
    pub fn update(
        &self,
        saga_id: &str,
        title: Option<&str>,
        description: Option<Option<&str>>,
        actor: &str,
    ) -> Result<SagaRow> {
        if title.is_none() && description.is_none() {
            return Err(crate::error::BrainCoreError::Parse(
                "update: at least one of title or description must be provided".into(),
            ));
        }
        let title = match title {
            Some(t) => {
                let trimmed = t.trim();
                if trimmed.is_empty() {
                    return Err(crate::error::BrainCoreError::Parse(
                        "update: title must not be empty".into(),
                    ));
                }
                Some(trimmed)
            }
            None => None,
        };
        // Canonicalize empty description to NULL so the store is consistent.
        let description = description.map(|d| match d {
            Some("") => None,
            other => other,
        });

        let row = self.db.with_write_conn(|conn| {
            let row = queries::update_saga(conn, saga_id, title, description)?;

            let payload = SagaUpdatedPayload {
                title: title.map(|t| t.to_string()),
                description: description.map(|d| d.map(|s| s.to_string())),
            };
            let event = SagaEvent::new(saga_id, actor, SagaEventType::SagaUpdated, &payload);
            queries::insert_saga_event(
                conn,
                &SagaEventInsert {
                    event_id: &event.event_id,
                    saga_id: &event.saga_id,
                    event_type: &serde_json::to_string(&event.event_type)
                        .expect("SagaEventType serialization is infallible"),
                    timestamp: event.timestamp,
                    actor: &event.actor,
                    payload: &event.payload.to_string(),
                },
            )?;

            Ok(row)
        })?;
        Ok(row)
    }


    /// Fetch a saga by ID. Returns None if not found.
    pub fn get(&self, saga_id: &str) -> Result<Option<SagaRow>> {
        self.db
            .with_read_conn(move |conn| queries::get_saga(conn, saga_id))
    }

    /// List sagas with optional filters.
    pub fn list(&self, filter: SagaListFilter) -> Result<Vec<SagaRow>> {
        self.db
            .with_read_conn(move |conn| queries::list_sagas(conn, &filter))
    }

    #[cfg(test)]
    pub(crate) fn db(&self) -> &Db {
        &self.db
    }

    /// Atomically add one or more tasks to a saga.
    ///
    /// All task IDs are resolved via `resolve_task_id_scoped` (cross-brain
    /// aware). The entire batch is all-or-nothing: if any task ID fails to
    /// resolve, is already a member, or the saga is closed/cancelled, the
    /// transaction is rolled back and an error is returned.
    ///
    /// Returns the number of tasks successfully added.
    pub fn add_tasks(&self, saga_id: &str, task_ids: &[String], actor: &str) -> Result<usize> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        self.db.with_write_conn(|conn| {
            let tx = conn.unchecked_transaction()?;

            // Verify the saga exists and is not in a terminal state.
            let row = queries::get_saga(&tx, saga_id)?
                .ok_or_else(|| BrainCoreError::TaskEvent(format!("saga not found: {saga_id}")))?;
            let status: SagaStatus = row.status.parse().map_err(|_| {
                BrainCoreError::TaskEvent(format!("saga '{saga_id}' has unrecognised status"))
            })?;
            match status {
                SagaStatus::Closed | SagaStatus::Cancelled => {
                    return Err(BrainCoreError::TaskEvent(format!(
                        "saga '{saga_id}' is {status}; reopen it before adding tasks"
                    )));
                }
                _ => {}
            }

            // Resolve all task IDs first — fail fast before any writes.
            let mut resolved: Vec<String> = Vec::with_capacity(task_ids.len());
            for raw_id in task_ids {
                let full_id = resolve_task_id_scoped(&tx, raw_id, None).map_err(|e| {
                    BrainCoreError::TaskEvent(format!("task '{raw_id}' could not be resolved: {e}"))
                })?;
                // Reject duplicates.
                if queries::saga_has_task(&tx, saga_id, &full_id)? {
                    return Err(BrainCoreError::TaskEvent(format!(
                        "task '{full_id}' is already a member of saga '{saga_id}'"
                    )));
                }
                resolved.push(full_id);
            }

            queries::insert_saga_tasks(&tx, saga_id, &resolved)?;

            // Emit one SagaTaskAdded event per task.
            for task_id in &resolved {
                let event = SagaEvent::new(
                    saga_id,
                    actor,
                    SagaEventType::SagaTaskAdded,
                    &SagaTaskPayload {
                        task_id: task_id.clone(),
                    },
                );
                queries::insert_saga_event(
                    &tx,
                    &SagaEventInsert {
                        event_id: &event.event_id,
                        saga_id: &event.saga_id,
                        event_type: &serde_json::to_string(&event.event_type)
                            .expect("SagaEventType serialization is infallible"),
                        timestamp: event.timestamp,
                        actor: &event.actor,
                        payload: &event.payload.to_string(),
                    },
                )?;
            }

            tx.commit()?;
            Ok(resolved.len())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brain_persistence::db::Db;

    fn in_memory_store() -> SagaStore {
        let db = Db::open_in_memory().unwrap();
        SagaStore::new(db)
    }

    #[test]
    fn create_returns_planning_status() {
        let store = in_memory_store();
        let row = store.create("My Saga", None, "test").unwrap();
        assert_eq!(row.status, "planning");
        assert_eq!(row.title, "My Saga");
        assert!(row.description.is_none());
        assert!(row.closed_at.is_none());
        assert_eq!(row.saga_id.len(), 26, "saga_id must be bare 26-char ULID");
        assert!(!row.saga_id.contains('-'), "saga_id must have no prefix");
    }

    #[test]
    fn create_with_description() {
        let store = in_memory_store();
        let row = store.create("Saga", Some("desc"), "test").unwrap();
        assert_eq!(row.description.as_deref(), Some("desc"));
    }

    #[test]
    fn get_returns_created_saga() {
        let store = in_memory_store();
        let created = store.create("Get Test", None, "test").unwrap();
        let fetched = store.get(&created.saga_id).unwrap().unwrap();
        assert_eq!(fetched.saga_id, created.saga_id);
        assert_eq!(fetched.title, "Get Test");
        assert_eq!(fetched.status, "planning");
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = in_memory_store();
        assert!(store.get("01NONEXISTENT000000000000").unwrap().is_none());
    }

    #[test]
    fn create_timestamps_populated() {
        let store = in_memory_store();
        let row = store.create("Timestamps", None, "test").unwrap();
        assert!(row.created_at > 0);
        assert!(row.updated_at > 0);
        assert_eq!(row.created_at, row.updated_at);
    }

    // T1: SagaCreated event row is written on create
    #[test]
    fn create_writes_saga_created_event() {
        let store = in_memory_store();
        let row = store.create("X", None, "actor").unwrap();
        let (event_type, actor): (String, String) = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT event_type, actor FROM saga_events WHERE saga_id = ?1",
                    [&row.saga_id],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert!(event_type.contains("saga_created"), "got: {event_type}");
        assert_eq!(actor, "actor");
    }

    // T2: empty title is rejected
    #[test]
    fn create_rejects_empty_title() {
        let store = in_memory_store();
        assert!(store.create("", None, "actor").is_err());
        assert!(store.create("   ", None, "actor").is_err());
    }

    // T3: saga_tasks allows cross-brain task_id (no FK on task_id)
    #[test]
    fn saga_tasks_allows_cross_brain_task_id() {
        let store = in_memory_store();
        let row = store.create("Cross-brain saga", None, "test").unwrap();
        // Insert a saga_tasks row with a task_id from a different brain —
        // saga_tasks has no FK on task_id by design so cross-brain links are allowed.
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, ?2, ?3)",
                    [row.saga_id.as_str(), "OTHER-BRAIN-TASK-01JXYZ", "1000000"],
                )?;
                Ok(())
            })
            .unwrap();
        let count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1",
                    [&row.saga_id],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(
            count, 1,
            "cross-brain task_id should be stored without error"
        );
    }

    #[test]
    fn list_default_excludes_closed_and_cancelled() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        let _b = store.create("Beta", None, "test").unwrap();

        // Manually force-close saga a by direct DB write.
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                Ok(())
            })
            .unwrap();

        let rows = store.list(SagaListFilter::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, "Beta");
    }

    #[test]
    fn list_include_closed() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        store.create("Beta", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                Ok(())
            })
            .unwrap();

        let rows = store
            .list(SagaListFilter {
                include_closed: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn list_all_includes_closed_and_cancelled() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        let b = store.create("Beta", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&b.saga_id],
                )?;
                Ok(())
            })
            .unwrap();

        let rows = store
            .list(SagaListFilter {
                include_closed: true,
                include_cancelled: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    // N4: default filter also excludes cancelled (not just closed)
    #[test]
    fn list_default_excludes_cancelled() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        store.create("Beta", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                Ok(())
            })
            .unwrap();

        let rows = store.list(SagaListFilter::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, "Beta");
    }

    // T4: include_cancelled alone (without --all)
    #[test]
    fn list_include_cancelled_only() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        store.create("Beta", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&a.saga_id],
                )?;
                Ok(())
            })
            .unwrap();

        let rows = store
            .list(SagaListFilter {
                include_cancelled: true,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    // Helper: insert a minimal task row with a given task_id and brain_id.
    // Ensures the brain row exists first (tasks.brain_id has a FK to brains).
    fn insert_task(store: &SagaStore, task_id: &str, brain_id: &str) {
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO brains (brain_id, name, created_at) VALUES (?1, ?1, 1000)",
                    [brain_id],
                )?;
                conn.execute(
                    "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
                     VALUES (?1, ?2, 'task', 'open', 4, 'task', 1000, 1000)",
                    [task_id, brain_id],
                )?;
                Ok(())
            })
            .unwrap();
    }

    // Helper: link a task to a saga.
    fn link_task(store: &SagaStore, saga_id: &str, task_id: &str) {
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "INSERT INTO saga_tasks (saga_id, task_id, added_at) VALUES (?1, ?2, 1000)",
                    [saga_id, task_id],
                )?;
                Ok(())
            })
            .unwrap();
    }

    // T1: containing_brain happy path — only returns sagas with a member-task in that brain.
    #[test]
    fn containing_brain_returns_only_matching_saga() {
        let store = in_memory_store();
        let a = store.create("Saga A", None, "test").unwrap();
        let b = store.create("Saga B", None, "test").unwrap();

        insert_task(&store, "task-x-brain", "brain-x");
        insert_task(&store, "task-y-brain", "brain-y");
        link_task(&store, &a.saga_id, "task-x-brain");
        link_task(&store, &b.saga_id, "task-y-brain");

        let rows = store
            .list(SagaListFilter {
                containing_brain: Some("brain-x".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].saga_id, a.saga_id);
    }

    // T2: cross-brain saga appears in both containing_brain queries.
    #[test]
    fn containing_brain_cross_brain_saga_appears_for_both() {
        let store = in_memory_store();
        let saga = store.create("Cross-Brain Saga", None, "test").unwrap();

        insert_task(&store, "task-in-x", "brain-x");
        insert_task(&store, "task-in-y", "brain-y");
        link_task(&store, &saga.saga_id, "task-in-x");
        link_task(&store, &saga.saga_id, "task-in-y");

        let rows_x = store
            .list(SagaListFilter {
                containing_brain: Some("brain-x".into()),
                ..Default::default()
            })
            .unwrap();
        let rows_y = store
            .list(SagaListFilter {
                containing_brain: Some("brain-y".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows_x.len(), 1, "should find saga via brain-x");
        assert_eq!(rows_y.len(), 1, "should find saga via brain-y");
        assert_eq!(rows_x[0].saga_id, saga.saga_id);
        assert_eq!(rows_y[0].saga_id, saga.saga_id);
    }

    // T3: containing_brain for non-existent brain returns empty list.
    #[test]
    fn containing_brain_nonexistent_brain_returns_empty() {
        let store = in_memory_store();
        store.create("Saga A", None, "test").unwrap();

        let rows = store
            .list(SagaListFilter {
                containing_brain: Some("no-such-brain".into()),
                ..Default::default()
            })
            .unwrap();
        assert!(rows.is_empty());
    }

    // T5: combined filters — include_closed=true + containing_brain.
    #[test]
    fn containing_brain_combined_with_include_closed() {
        let store = in_memory_store();
        let a = store.create("Open Saga", None, "test").unwrap();
        let b = store.create("Closed Saga", None, "test").unwrap();

        insert_task(&store, "task-open", "brain-x");
        insert_task(&store, "task-closed", "brain-x");
        link_task(&store, &a.saga_id, "task-open");
        link_task(&store, &b.saga_id, "task-closed");

        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&b.saga_id],
                )?;
                Ok(())
            })
            .unwrap();

        // Without include_closed: only open saga returned.
        let rows = store
            .list(SagaListFilter {
                containing_brain: Some("brain-x".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].saga_id, a.saga_id);

        // With include_closed: both returned.
        let rows_all = store
            .list(SagaListFilter {
                include_closed: true,
                containing_brain: Some("brain-x".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows_all.len(), 2);
    }

    // T1: update on non-existent saga returns error containing "not found"
    #[test]
    fn update_nonexistent_saga_returns_not_found() {
        let store = in_memory_store();
        let err = store
            .update("01NONEXISTENTSAGA0000000", None, Some(Some("x")), "actor")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not found"), "expected 'not found' in: {msg}");
    }

    // T2: update on closed/cancelled saga succeeds (metadata edit is lifecycle-independent)
    #[test]
    fn update_on_closed_saga_succeeds() {
        let store = in_memory_store();
        let row = store.create("Active", None, "test").unwrap();
        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&row.saga_id],
                )?;
                Ok(())
            })
            .unwrap();
        let updated = store
            .update(&row.saga_id, Some("Closed but renamed"), None, "actor")
            .unwrap();
        assert_eq!(updated.title, "Closed but renamed");
        assert_eq!(updated.status, "closed");
    }

    // T3: clear-description test — create with desc, update with Some(None), assert NULL
    #[test]
    fn update_clears_description_when_some_none() {
        let store = in_memory_store();
        let row = store.create("Has Desc", Some("original"), "test").unwrap();
        assert!(row.description.is_some());
        let updated = store
            .update(&row.saga_id, None, Some(None), "actor")
            .unwrap();
        assert!(
            updated.description.is_none(),
            "description should be NULL after clear"
        );
    }

    // T4: after update, saga_events has one row with event_type saga_updated and new title
    #[test]
    fn update_writes_saga_updated_event() {
        let store = in_memory_store();
        let row = store.create("Before", None, "test").unwrap();
        store
            .update(&row.saga_id, Some("After"), None, "actor")
            .unwrap();
        let (event_type, payload): (String, String) = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT event_type, payload FROM saga_events WHERE saga_id = ?1 AND event_type LIKE '%updated%'",
                    [&row.saga_id],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert!(event_type.contains("saga_updated"), "got: {event_type}");
        assert!(
            payload.contains("After"),
            "payload should contain new title, got: {payload}"
        );
    }

    // T5: whitespace-only title is rejected
    #[test]
    fn update_rejects_whitespace_only_title() {
        let store = in_memory_store();
        let row = store.create("Valid", None, "test").unwrap();
        assert!(
            store
                .update(&row.saga_id, Some("   "), None, "actor")
                .is_err()
        );
        assert!(
            store
                .update(&row.saga_id, Some("\t\n"), None, "actor")
                .is_err()
        );
    }

    // T6: updated_at is strictly greater than created_at after update.
    // now_ts() has second granularity, so we sleep 1100 ms to guarantee the next second.
    #[test]
    fn update_bumps_updated_at_strictly() {
        let store = in_memory_store();
        let row = store.create("Timing", None, "test").unwrap();
        let created_at = row.created_at;
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let updated = store
            .update(&row.saga_id, Some("Timing Updated"), None, "actor")
            .unwrap();
        assert!(
            updated.updated_at > created_at,
            "updated_at ({}) must be strictly greater than created_at ({})",
            updated.updated_at,
            created_at
        );
    }

    // ── add_tasks tests ────────────────────────────────────────────────────

    // T1: mid-batch atomicity — [good_id, bad_id] must leave good_id NOT a member.
    #[test]
    fn add_tasks_atomicity_bad_id_rolls_back_good_id() {
        let store = in_memory_store();
        let saga = store.create("Atomic Saga", None, "test").unwrap();
        insert_task(&store, "good-brain-task01", "brain-x");

        let result = store.add_tasks(
            &saga.saga_id,
            &[
                "good-brain-task01".to_string(),
                "NONEXISTENT-TASK-ID".to_string(),
            ],
            "test",
        );
        assert!(result.is_err(), "batch with bad ID should fail");

        // good-brain-task01 must NOT be a member because the transaction rolled back.
        let count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_tasks WHERE saga_id = ?1 AND task_id = ?2",
                    [saga.saga_id.as_str(), "good-brain-task01"],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(count, 0, "rolled-back task must not appear in saga_tasks");
    }

    // T2: closed saga is rejected with a message mentioning "reopen".
    #[test]
    fn add_tasks_closed_saga_rejected() {
        let store = in_memory_store();
        let saga = store.create("Closed Saga", None, "test").unwrap();
        insert_task(&store, "brain-z-task01", "brain-z");

        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                    [&saga.saga_id],
                )?;
                Ok(())
            })
            .unwrap();

        let err = store
            .add_tasks(&saga.saga_id, &["brain-z-task01".to_string()], "test")
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("reopen"),
            "error should mention 'reopen', got: {msg}"
        );
    }

    // T3: cancelled saga is rejected.
    #[test]
    fn add_tasks_cancelled_saga_rejected() {
        let store = in_memory_store();
        let saga = store.create("Cancelled Saga", None, "test").unwrap();
        insert_task(&store, "brain-w-task01", "brain-w");

        store
            .db
            .with_write_conn(|conn| {
                conn.execute(
                    "UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1",
                    [&saga.saga_id],
                )?;
                Ok(())
            })
            .unwrap();

        let err = store
            .add_tasks(&saga.saga_id, &["brain-w-task01".to_string()], "test")
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("reopen"),
            "error should mention 'reopen', got: {msg}"
        );
    }

    // T4: cross-brain mixed batch — tasks from two different brains added to one saga.
    #[test]
    fn add_tasks_cross_brain_mixed_batch() {
        let store = in_memory_store();
        let saga = store.create("Cross-Brain Saga", None, "test").unwrap();
        insert_task(&store, "brain-a-task01", "brain-a");
        insert_task(&store, "brain-b-task01", "brain-b");

        let count = store
            .add_tasks(
                &saga.saga_id,
                &["brain-a-task01".to_string(), "brain-b-task01".to_string()],
                "test",
            )
            .unwrap();
        assert_eq!(count, 2);

        let ids: Vec<String> = store
            .db
            .with_read_conn(|c| {
                let mut stmt = c
                    .prepare("SELECT task_id FROM saga_tasks WHERE saga_id = ?1 ORDER BY task_id")
                    .unwrap();
                let ids = stmt
                    .query_map([saga.saga_id.as_str()], |r| r.get(0))
                    .unwrap()
                    .collect::<std::result::Result<Vec<String>, _>>()
                    .unwrap();
                Ok(ids)
            })
            .unwrap();
        assert!(ids.contains(&"brain-a-task01".to_string()));
        assert!(ids.contains(&"brain-b-task01".to_string()));
    }

    // T5: after multi-add, saga_events has SagaTaskAdded count == tasks added.
    #[test]
    fn add_tasks_emits_one_event_per_task() {
        let store = in_memory_store();
        let saga = store.create("Event Saga", None, "test").unwrap();
        insert_task(&store, "ev-brain-task01", "brain-ev");
        insert_task(&store, "ev-brain-task02", "brain-ev");
        insert_task(&store, "ev-brain-task03", "brain-ev");

        let count = store
            .add_tasks(
                &saga.saga_id,
                &[
                    "ev-brain-task01".to_string(),
                    "ev-brain-task02".to_string(),
                    "ev-brain-task03".to_string(),
                ],
                "test",
            )
            .unwrap();
        assert_eq!(count, 3);

        let event_count: i64 = store
            .db
            .with_read_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM saga_events \
                     WHERE saga_id = ?1 AND event_type = '\"saga_task_added\"'",
                    [saga.saga_id.as_str()],
                    |r| r.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(
            event_count, 3,
            "expected 3 SagaTaskAdded events, got {event_count}"
        );
    }

    // M7: empty batch returns Ok(0) immediately.
    #[test]
    fn add_tasks_empty_batch_is_noop() {
        let store = in_memory_store();
        let saga = store.create("Noop Saga", None, "test").unwrap();
        let count = store.add_tasks(&saga.saga_id, &[], "test").unwrap();
        assert_eq!(count, 0);
    }
}
