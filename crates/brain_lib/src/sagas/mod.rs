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

    /// Update a saga's title and/or description. At least one must be provided.
    /// Allowed in any status. Emits SagaUpdated event.
    pub fn update(
        &self,
        saga_id: &str,
        title: Option<&str>,
        description: Option<&str>,
        actor: &str,
    ) -> Result<SagaRow> {
        if title.is_none() && description.is_none() {
            return Err(crate::error::BrainCoreError::Database(
                "update requires at least one of title or description".into(),
            ));
        }
        if let Some(t) = title {
            if t.trim().is_empty() {
                return Err(crate::error::BrainCoreError::Database(
                    "title must not be empty".into(),
                ));
            }
        }
        let row = self.db.with_write_conn(|conn| {
            let row = queries::update_saga(conn, saga_id, title, description)?;

            let payload = serde_json::json!({ "title": title, "description": description });
            let event = SagaEvent::new(
                saga_id,
                actor,
                SagaEventType::SagaUpdated,
                &payload,
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
    fn update_title_only() {
        let store = in_memory_store();
        let created = store.create("Original", None, "test").unwrap();
        let updated = store.update(&created.saga_id, Some("Renamed"), None, "test").unwrap();
        assert_eq!(updated.title, "Renamed");
        assert!(updated.updated_at >= created.updated_at);
    }

    #[test]
    fn update_description_only() {
        let store = in_memory_store();
        let created = store.create("Title", None, "test").unwrap();
        let updated = store.update(&created.saga_id, None, Some("new desc"), "test").unwrap();
        assert_eq!(updated.description.as_deref(), Some("new desc"));
        assert_eq!(updated.title, "Title");
    }

    #[test]
    fn update_both_fields() {
        let store = in_memory_store();
        let created = store.create("Old", Some("old desc"), "test").unwrap();
        let updated = store
            .update(&created.saga_id, Some("New"), Some("new desc"), "test")
            .unwrap();
        assert_eq!(updated.title, "New");
        assert_eq!(updated.description.as_deref(), Some("new desc"));
    }

    #[test]
    fn update_no_fields_errors() {
        let store = in_memory_store();
        let created = store.create("Saga", None, "test").unwrap();
        let result = store.update(&created.saga_id, None, None, "test");
        assert!(result.is_err());
    }

    #[test]
    fn update_empty_title_errors() {
        let store = in_memory_store();
        let created = store.create("Saga", None, "test").unwrap();
        let result = store.update(&created.saga_id, Some("  "), None, "test");
        assert!(result.is_err());
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
