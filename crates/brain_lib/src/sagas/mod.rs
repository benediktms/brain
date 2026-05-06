use brain_persistence::db::Db;
use brain_persistence::db::sagas::events::{SagaEvent, SagaEventType, new_saga_id};
use brain_persistence::db::sagas::queries::{self, SagaListFilter, SagaRow};

pub use brain_persistence::db::sagas::queries::SagaListFilter as ListFilter;

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
        let saga_id = new_saga_id();
        let row = self.db.with_write_conn(|conn| {
            let row = queries::insert_saga(conn, &saga_id, title, description)?;

            let event = SagaEvent::new(
                &saga_id,
                actor,
                SagaEventType::SagaCreated,
                &serde_json::json!({ "title": title, "description": description }),
            );
            conn.execute(
                "INSERT INTO saga_events (event_id, saga_id, event_type, timestamp, actor, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    event.event_id,
                    event.saga_id,
                    serde_json::to_string(&event.event_type).unwrap_or_default(),
                    event.timestamp,
                    event.actor,
                    event.payload.to_string(),
                ],
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

    #[test]
    fn list_default_excludes_closed_and_cancelled() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        let _b = store.create("Beta", None, "test").unwrap();

        // Manually force-close saga a by direct DB write.
        store.db.with_write_conn(|conn| {
            conn.execute(
                "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                [&a.saga_id],
            )?;
            Ok(())
        }).unwrap();

        let rows = store.list(SagaListFilter::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].title, "Beta");
    }

    #[test]
    fn list_include_closed() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        store.create("Beta", None, "test").unwrap();
        store.db.with_write_conn(|conn| {
            conn.execute(
                "UPDATE sagas SET status = 'closed' WHERE saga_id = ?1",
                [&a.saga_id],
            )?;
            Ok(())
        }).unwrap();

        let rows = store.list(SagaListFilter { include_closed: true, ..Default::default() }).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn list_all_includes_closed_and_cancelled() {
        let store = in_memory_store();
        let a = store.create("Alpha", None, "test").unwrap();
        let b = store.create("Beta", None, "test").unwrap();
        store.db.with_write_conn(|conn| {
            conn.execute("UPDATE sagas SET status = 'closed' WHERE saga_id = ?1", [&a.saga_id])?;
            conn.execute("UPDATE sagas SET status = 'cancelled' WHERE saga_id = ?1", [&b.saga_id])?;
            Ok(())
        }).unwrap();

        let rows = store.list(SagaListFilter { include_closed: true, include_cancelled: true, ..Default::default() }).unwrap();
        assert_eq!(rows.len(), 2);
    }
}
