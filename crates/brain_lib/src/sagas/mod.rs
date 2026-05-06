pub mod status;
pub use status::SagaStatus;

use brain_persistence::db::Db;
use brain_persistence::db::sagas::SagaListFilter;
use brain_persistence::db::sagas::events::{SagaEvent, SagaEventType, new_saga_id};
use brain_persistence::db::sagas::queries::{self, SagaEventInsert, SagaRow};

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
    fn insert_task(store: &SagaStore, task_id: &str, brain_id: &str) {
        store.db.with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at)
                 VALUES (?1, ?2, 'task', 'open', 4, 'task', 1000, 1000)",
                [task_id, brain_id],
            )?;
            Ok(())
        }).unwrap();
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
}
