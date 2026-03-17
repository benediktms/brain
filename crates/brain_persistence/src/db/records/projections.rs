use rusqlite::Connection;

use crate::error::{BrainCoreError, Result};

use super::events::{
    LinkPayload, PayloadEvictedPayload, RecordArchivedPayload, RecordCreatedPayload, RecordEvent,
    RecordEventType, RecordUpdatedPayload, RetentionClassSetPayload, TagPayload,
};

/// Apply a single event to the SQLite records projection tables.
///
/// `brain_id` is stamped on the `records` row for all `RecordCreated` events.
/// For all other event types the brain_id is not re-written (the row already
/// carries the brain_id set at creation time).
///
/// The event is applied inside an implicit transaction — callers operating in
/// bulk (e.g. `rebuild`) should use an outer transaction for performance.
pub fn apply_event(conn: &Connection, event: &RecordEvent, brain_id: &str) -> Result<()> {
    match event.event_type {
        RecordEventType::RecordCreated => {
            let p: RecordCreatedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::RecordEvent(format!("bad RecordCreated payload: {e}"))
                })?;

            let original_size = p.content_ref.original_size.unwrap_or(p.content_ref.size) as i64;

            conn.execute(
                "INSERT INTO records
                    (record_id, brain_id, title, kind, status, description,
                     content_hash, content_size, media_type,
                     task_id, actor, created_at, updated_at,
                     retention_class, pinned, payload_available, content_encoding, original_size)
                 VALUES (?1, ?2, ?3, ?4, 'active', ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                         ?13, 0, 1, ?14, ?15)",
                rusqlite::params![
                    event.record_id,
                    brain_id,
                    p.title,
                    p.kind,
                    p.description,
                    p.content_ref.hash,
                    p.content_ref.size as i64,
                    p.content_ref.media_type,
                    p.task_id,
                    event.actor,
                    event.timestamp,
                    event.timestamp,
                    p.retention_class,
                    p.content_ref.content_encoding,
                    original_size,
                ],
            )?;

            // Insert any inline tags from the creation payload
            for tag in &p.tags {
                conn.execute(
                    "INSERT OR IGNORE INTO record_tags (record_id, tag) VALUES (?1, ?2)",
                    rusqlite::params![event.record_id, tag],
                )?;
            }
        }

        RecordEventType::RecordUpdated => {
            use rusqlite::types::Value as SqlValue;

            let p: RecordUpdatedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::RecordEvent(format!("bad RecordUpdated payload: {e}"))
                })?;

            let mut set_cols: Vec<&str> = Vec::new();
            let mut params: Vec<SqlValue> = Vec::new();

            if let Some(ref title) = p.title {
                set_cols.push("title");
                params.push(SqlValue::Text(title.clone()));
            }
            if let Some(ref description) = p.description {
                set_cols.push("description");
                params.push(SqlValue::Text(description.clone()));
            }

            // Always bump updated_at
            set_cols.push("updated_at");
            params.push(SqlValue::Integer(event.timestamp));

            let set_clause: String = set_cols
                .iter()
                .enumerate()
                .map(|(i, col)| format!("{col} = ?{}", i + 1))
                .collect::<Vec<_>>()
                .join(", ");

            let record_id_idx = params.len() + 1;
            params.push(SqlValue::Text(event.record_id.clone()));

            let sql = format!("UPDATE records SET {set_clause} WHERE record_id = ?{record_id_idx}");
            conn.execute(&sql, rusqlite::params_from_iter(params))?;
        }

        RecordEventType::RecordArchived => {
            let _p: RecordArchivedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::RecordEvent(format!("bad RecordArchived payload: {e}"))
                })?;

            conn.execute(
                "UPDATE records SET status = 'archived', updated_at = ?1 WHERE record_id = ?2",
                rusqlite::params![event.timestamp, event.record_id],
            )?;
        }

        RecordEventType::TagAdded => {
            let p: TagPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::RecordEvent(format!("bad TagAdded payload: {e}")))?;

            conn.execute(
                "INSERT OR IGNORE INTO record_tags (record_id, tag) VALUES (?1, ?2)",
                rusqlite::params![event.record_id, p.tag],
            )?;
        }

        RecordEventType::TagRemoved => {
            let p: TagPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::RecordEvent(format!("bad TagRemoved payload: {e}")))?;

            conn.execute(
                "DELETE FROM record_tags WHERE record_id = ?1 AND tag = ?2",
                rusqlite::params![event.record_id, p.tag],
            )?;
        }

        RecordEventType::LinkAdded => {
            let p: LinkPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::RecordEvent(format!("bad LinkAdded payload: {e}")))?;

            conn.execute(
                "INSERT INTO record_links (record_id, task_id, chunk_id, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![event.record_id, p.task_id, p.chunk_id, event.timestamp],
            )?;
        }

        RecordEventType::LinkRemoved => {
            let p: LinkPayload = serde_json::from_value(event.payload.clone()).map_err(|e| {
                BrainCoreError::RecordEvent(format!("bad LinkRemoved payload: {e}"))
            })?;

            if p.task_id.is_none() && p.chunk_id.is_none() {
                return Err(BrainCoreError::RecordEvent(
                    "LinkRemoved payload must have at least one of task_id or chunk_id".into(),
                ));
            }

            conn.execute(
                "DELETE FROM record_links
                 WHERE record_id = ?1
                   AND (task_id IS ?2 OR task_id = ?2)
                   AND (chunk_id IS ?3 OR chunk_id = ?3)",
                rusqlite::params![event.record_id, p.task_id, p.chunk_id],
            )?;
        }

        RecordEventType::PayloadEvicted => {
            let _p: PayloadEvictedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::RecordEvent(format!("bad PayloadEvicted payload: {e}"))
                })?;

            conn.execute(
                "UPDATE records SET payload_available = 0, updated_at = ?1 WHERE record_id = ?2",
                rusqlite::params![event.timestamp, event.record_id],
            )?;
        }

        RecordEventType::RetentionClassSet => {
            let p: RetentionClassSetPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| {
                    BrainCoreError::RecordEvent(format!("bad RetentionClassSet payload: {e}"))
                })?;

            conn.execute(
                "UPDATE records SET retention_class = ?1, updated_at = ?2 WHERE record_id = ?3",
                rusqlite::params![p.retention_class, event.timestamp, event.record_id],
            )?;
        }

        RecordEventType::RecordPinned => {
            conn.execute(
                "UPDATE records SET pinned = 1, updated_at = ?1 WHERE record_id = ?2",
                rusqlite::params![event.timestamp, event.record_id],
            )?;
        }

        RecordEventType::RecordUnpinned => {
            conn.execute(
                "UPDATE records SET pinned = 0, updated_at = ?1 WHERE record_id = ?2",
                rusqlite::params![event.timestamp, event.record_id],
            )?;
        }
    }

    // Record the event itself in the queryable audit log
    let payload_json = serde_json::to_string(&event.payload).unwrap_or_else(|_| "{}".into());
    conn.execute(
        "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, payload, brain_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            event.event_id,
            event.record_id,
            serde_json::to_string(&event.event_type)
                .unwrap_or_default()
                .trim_matches('"'),
            event.timestamp,
            event.actor,
            payload_json,
            brain_id,
        ],
    )?;

    Ok(())
}

/// Rebuild all records projection tables from a pre-loaded event slice.
///
/// Clears `record_events`, `record_links`, `record_tags`, and `records`
/// (in FK-safe order), then replays every event.
///
/// Returns the number of events applied.
///
/// The rebuild is fully deterministic: the same event sequence always produces
/// the same projection state.
pub fn rebuild_from_events(conn: &Connection, events: &[RecordEvent]) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;

    // Clear in FK-safe order (child tables before parent)
    tx.execute_batch(
        "DELETE FROM record_events;
         DELETE FROM record_links;
         DELETE FROM record_tags;
         DELETE FROM records;",
    )?;

    for event in events {
        apply_event(&tx, event, "")?;
    }

    tx.commit()?;

    Ok(events.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::records::events::{
        ContentRefPayload, RecordArchivedPayload, RecordCreatedPayload, RecordEvent,
        RecordEventType, RecordUpdatedPayload, TagPayload,
    };
    use crate::db::schema::init_schema;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn make_created_event(record_id: &str, title: &str, kind: &str) -> RecordEvent {
        RecordEvent::from_payload(
            record_id,
            "test-agent",
            RecordCreatedPayload {
                title: title.to_string(),
                kind: kind.to_string(),
                content_ref: ContentRefPayload::new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
                    42,
                    Some("application/json".to_string()),
                ),
                description: None,
                task_id: None,
                tags: vec![],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        )
    }

    #[test]
    fn test_apply_record_created() {
        let conn = setup();
        let ev = make_created_event("r1", "My Artifact", "report");
        apply_event(&conn, &ev, "").unwrap();

        let (title, kind, status): (String, String, String) = conn
            .query_row(
                "SELECT title, kind, status FROM records WHERE record_id = 'r1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(title, "My Artifact");
        assert_eq!(kind, "report");
        assert_eq!(status, "active");
    }

    #[test]
    fn test_apply_tag_added_removed() {
        let conn = setup();
        apply_event(&conn, &make_created_event("r1", "T", "report"), "").unwrap();

        let tag_add = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: "performance".to_string(),
            },
        );
        apply_event(&conn, &tag_add, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_tags WHERE record_id = 'r1' AND tag = 'performance'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_event_recorded_in_record_events() {
        let conn = setup();
        let ev = make_created_event("r1", "T", "report");
        apply_event(&conn, &ev, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_events WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_rebuild_from_events_basic() {
        let conn = setup();

        let events = vec![
            make_created_event("r1", "First", "report"),
            make_created_event("r2", "Second", "diff"),
        ];

        let count = rebuild_from_events(&conn, &events).unwrap();
        assert_eq!(count, 2);

        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 2);
    }

    #[test]
    fn test_rebuild_from_events_idempotent() {
        let conn = setup();
        let events = vec![
            make_created_event("r1", "A", "report"),
            make_created_event("r2", "B", "diff"),
        ];

        rebuild_from_events(&conn, &events).unwrap();
        let count1: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();

        rebuild_from_events(&conn, &events).unwrap();
        let count2: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count1, 2);
        assert_eq!(count2, 2);
    }

    #[test]
    fn test_rebuild_from_events_empty() {
        let conn = setup();
        let count = rebuild_from_events(&conn, &[]).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_rebuild_from_events_full_lifecycle() {
        let conn = setup();

        let events = vec![
            make_created_event("r1", "Original", "report"),
            RecordEvent::from_payload(
                "r1",
                "agent",
                RecordUpdatedPayload {
                    title: Some("Updated".to_string()),
                    description: None,
                },
            ),
            RecordEvent::new(
                "r1",
                "agent",
                RecordEventType::TagAdded,
                &TagPayload {
                    tag: "important".to_string(),
                },
            ),
            RecordEvent::from_payload("r1", "agent", RecordArchivedPayload { reason: None }),
        ];

        let count = rebuild_from_events(&conn, &events).unwrap();
        assert_eq!(count, 4);

        let (title, status): (String, String) = conn
            .query_row(
                "SELECT title, status FROM records WHERE record_id = 'r1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(title, "Updated");
        assert_eq!(status, "archived");
    }
}
