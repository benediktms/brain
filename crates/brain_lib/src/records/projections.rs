use std::path::Path;

use rusqlite::Connection;

use crate::error::{BrainCoreError, Result};

use super::events::{
    LinkPayload, PayloadEvictedPayload, RecordArchivedPayload, RecordCreatedPayload, RecordEvent,
    RecordEventType, RecordUpdatedPayload, RetentionClassSetPayload, TagPayload, read_all_events,
};

/// Apply a single event to the SQLite records projection tables.
///
/// The event is applied inside an implicit transaction — callers operating in
/// bulk (e.g. `rebuild`) should use an outer transaction for performance.
pub fn apply_event(conn: &Connection, event: &RecordEvent) -> Result<()> {
    match event.event_type {
        RecordEventType::RecordCreated => {
            let p: RecordCreatedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::RecordEvent(format!("bad RecordCreated payload: {e}"))
                })?;

            let original_size = p.content_ref.original_size.unwrap_or(p.content_ref.size) as i64;

            conn.execute(
                "INSERT INTO records
                    (record_id, title, kind, status, description,
                     content_hash, content_size, media_type,
                     task_id, actor, created_at, updated_at,
                     retention_class, pinned, payload_available, content_encoding, original_size)
                 VALUES (?1, ?2, ?3, 'active', ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                         ?12, 0, 1, ?13, ?14)",
                rusqlite::params![
                    event.record_id,
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
        "INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            event.event_id,
            event.record_id,
            serde_json::to_string(&event.event_type)
                .unwrap_or_default()
                .trim_matches('"'),
            event.timestamp,
            event.actor,
            payload_json,
        ],
    )?;

    Ok(())
}

/// Rebuild all records projection tables from the JSONL event log.
///
/// Clears `record_events`, `record_links`, `record_tags`, and `records`
/// (in FK-safe order), then replays every event from `events_path`.
///
/// Returns the number of events applied.
///
/// The rebuild is fully deterministic: the same event log always produces
/// the same projection state.
pub fn rebuild(conn: &Connection, events_path: &Path) -> Result<usize> {
    let events = read_all_events(events_path)?;

    let tx = conn.unchecked_transaction()?;

    // Clear in FK-safe order (child tables before parent)
    tx.execute_batch(
        "DELETE FROM record_events;
         DELETE FROM record_links;
         DELETE FROM record_tags;
         DELETE FROM records;",
    )?;

    for event in &events {
        apply_event(&tx, event)?;
    }

    tx.commit()?;

    Ok(events.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;
    use crate::records::events::{
        ContentRefPayload, LinkPayload, RecordArchivedPayload, RecordCreatedPayload, RecordEvent,
        RecordEventType, RecordUpdatedPayload, TagPayload, append_event,
    };
    use tempfile::TempDir;

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
        apply_event(&conn, &ev).unwrap();

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
    fn test_apply_record_created_with_inline_tags() {
        let conn = setup();
        let ev = RecordEvent::from_payload(
            "r1",
            "agent",
            RecordCreatedPayload {
                title: "Tagged".to_string(),
                kind: "report".to_string(),
                content_ref: ContentRefPayload::new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
                    10,
                    None,
                ),
                description: None,
                task_id: None,
                tags: vec!["alpha".to_string(), "beta".to_string()],
                scope_type: None,
                scope_id: None,
                retention_class: None,
                producer: None,
            },
        );
        apply_event(&conn, &ev).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_tags WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_apply_record_updated() {
        let conn = setup();
        apply_event(&conn, &make_created_event("r1", "Original", "report")).unwrap();

        let update = RecordEvent::from_payload(
            "r1",
            "agent",
            RecordUpdatedPayload {
                title: Some("Updated Title".to_string()),
                description: Some("New description".to_string()),
            },
        );
        apply_event(&conn, &update).unwrap();

        let (title, desc): (String, Option<String>) = conn
            .query_row(
                "SELECT title, description FROM records WHERE record_id = 'r1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(title, "Updated Title");
        assert_eq!(desc.as_deref(), Some("New description"));
    }

    #[test]
    fn test_apply_record_archived() {
        let conn = setup();
        apply_event(&conn, &make_created_event("r1", "Some Record", "diff")).unwrap();

        let archive = RecordEvent::from_payload(
            "r1",
            "agent",
            RecordArchivedPayload {
                reason: Some("superseded".to_string()),
            },
        );
        apply_event(&conn, &archive).unwrap();

        let status: String = conn
            .query_row(
                "SELECT status FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "archived");
    }

    #[test]
    fn test_apply_tag_added_removed() {
        let conn = setup();
        apply_event(&conn, &make_created_event("r1", "T", "report")).unwrap();

        let tag_add = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: "performance".to_string(),
            },
        );
        apply_event(&conn, &tag_add).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_tags WHERE record_id = 'r1' AND tag = 'performance'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let tag_rm = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::TagRemoved,
            &TagPayload {
                tag: "performance".to_string(),
            },
        );
        apply_event(&conn, &tag_rm).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_tags WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_apply_link_added_removed() {
        let conn = setup();
        apply_event(&conn, &make_created_event("r1", "T", "report")).unwrap();

        let link_add = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: Some("BRN-01XXX".to_string()),
                chunk_id: None,
            },
        );
        apply_event(&conn, &link_add).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_links WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let link_rm = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::LinkRemoved,
            &LinkPayload {
                task_id: Some("BRN-01XXX".to_string()),
                chunk_id: None,
            },
        );
        apply_event(&conn, &link_rm).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_links WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_event_recorded_in_record_events() {
        let conn = setup();
        let ev = make_created_event("r1", "T", "report");
        apply_event(&conn, &ev).unwrap();

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
    fn test_rebuild_from_path() {
        let dir = TempDir::new().unwrap();
        let events_path = dir.path().join("events.jsonl");
        let conn = setup();

        let ev1 = make_created_event("r1", "First", "report");
        let ev2 = make_created_event("r2", "Second", "diff");
        append_event(&events_path, &ev1).unwrap();
        append_event(&events_path, &ev2).unwrap();

        let count = rebuild(&conn, &events_path).unwrap();
        assert_eq!(count, 2);

        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 2);
    }

    #[test]
    fn test_rebuild_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let events_path = dir.path().join("events.jsonl");
        let conn = setup();

        append_event(&events_path, &make_created_event("r1", "A", "report")).unwrap();
        append_event(&events_path, &make_created_event("r2", "B", "diff")).unwrap();

        rebuild(&conn, &events_path).unwrap();
        let count1: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();

        // Rebuild again — must produce identical state
        rebuild(&conn, &events_path).unwrap();
        let count2: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count1, 2);
        assert_eq!(count2, 2);
    }

    #[test]
    fn test_rebuild_empty_log() {
        let dir = TempDir::new().unwrap();
        let events_path = dir.path().join("events.jsonl");
        let conn = setup();

        // Non-existent file → 0 events, no error
        let count = rebuild(&conn, &events_path).unwrap();
        assert_eq!(count, 0);

        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn test_rebuild_full_lifecycle() {
        let dir = TempDir::new().unwrap();
        let events_path = dir.path().join("events.jsonl");
        let conn = setup();

        // Create
        append_event(
            &events_path,
            &make_created_event("r1", "Original", "report"),
        )
        .unwrap();

        // Update title
        append_event(
            &events_path,
            &RecordEvent::from_payload(
                "r1",
                "agent",
                RecordUpdatedPayload {
                    title: Some("Updated".to_string()),
                    description: None,
                },
            ),
        )
        .unwrap();

        // Add tag
        append_event(
            &events_path,
            &RecordEvent::new(
                "r1",
                "agent",
                RecordEventType::TagAdded,
                &TagPayload {
                    tag: "important".to_string(),
                },
            ),
        )
        .unwrap();

        // Archive
        append_event(
            &events_path,
            &RecordEvent::from_payload("r1", "agent", RecordArchivedPayload { reason: None }),
        )
        .unwrap();

        let count = rebuild(&conn, &events_path).unwrap();
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

        let tag_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_tags WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tag_count, 1);
    }

    #[test]
    fn test_apply_payload_evicted() {
        use crate::records::events::PayloadEvictedPayload;

        let conn = setup();
        apply_event(&conn, &make_created_event("r1", "T", "report")).unwrap();

        // Verify default: payload_available = 1
        let avail: i32 = conn
            .query_row(
                "SELECT payload_available FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(avail, 1);

        let evict = RecordEvent::from_payload(
            "r1",
            "gc-agent",
            PayloadEvictedPayload {
                content_hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .to_string(),
                reason: "gc".to_string(),
            },
        );
        apply_event(&conn, &evict).unwrap();

        let avail: i32 = conn
            .query_row(
                "SELECT payload_available FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(avail, 0);
    }

    #[test]
    fn test_apply_retention_class_set() {
        use crate::records::events::RetentionClassSetPayload;

        let conn = setup();
        apply_event(&conn, &make_created_event("r1", "T", "report")).unwrap();

        // Verify default: retention_class = NULL
        let rc: Option<String> = conn
            .query_row(
                "SELECT retention_class FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(rc.is_none());

        let set_rc = RecordEvent::from_payload(
            "r1",
            "agent",
            RetentionClassSetPayload {
                retention_class: Some("permanent".to_string()),
            },
        );
        apply_event(&conn, &set_rc).unwrap();

        let rc: Option<String> = conn
            .query_row(
                "SELECT retention_class FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rc.as_deref(), Some("permanent"));

        // Clear it back to None
        let clear_rc = RecordEvent::from_payload(
            "r1",
            "agent",
            RetentionClassSetPayload {
                retention_class: None,
            },
        );
        apply_event(&conn, &clear_rc).unwrap();

        let rc: Option<String> = conn
            .query_row(
                "SELECT retention_class FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(rc.is_none());
    }

    #[test]
    fn test_apply_record_pinned_unpinned() {
        use crate::records::events::PinPayload;

        let conn = setup();
        apply_event(&conn, &make_created_event("r1", "T", "report")).unwrap();

        // Default: pinned = 0
        let pinned: i32 = conn
            .query_row(
                "SELECT pinned FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pinned, 0);

        // Pin it
        apply_event(
            &conn,
            &RecordEvent::new("r1", "agent", RecordEventType::RecordPinned, &PinPayload {}),
        )
        .unwrap();

        let pinned: i32 = conn
            .query_row(
                "SELECT pinned FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pinned, 1);

        // Unpin it
        apply_event(
            &conn,
            &RecordEvent::new(
                "r1",
                "agent",
                RecordEventType::RecordUnpinned,
                &PinPayload {},
            ),
        )
        .unwrap();

        let pinned: i32 = conn
            .query_row(
                "SELECT pinned FROM records WHERE record_id = 'r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pinned, 0);
    }

    #[test]
    fn test_record_created_sets_new_columns() {
        use crate::records::events::RecordCreatedPayload;

        let conn = setup();

        let ev = RecordEvent::from_payload(
            "r1",
            "agent",
            RecordCreatedPayload {
                title: "T".to_string(),
                kind: "report".to_string(),
                content_ref: ContentRefPayload::new("abc123".to_string(), 512, None),
                description: None,
                task_id: None,
                tags: vec![],
                scope_type: None,
                scope_id: None,
                retention_class: Some("ephemeral".to_string()),
                producer: None,
            },
        );
        apply_event(&conn, &ev).unwrap();

        let (retention_class, pinned, payload_available, content_encoding, original_size): (
            Option<String>,
            i32,
            i32,
            String,
            Option<i64>,
        ) = conn
            .query_row(
                "SELECT retention_class, pinned, payload_available, content_encoding, original_size
                 FROM records WHERE record_id = 'r1'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(retention_class.as_deref(), Some("ephemeral"));
        assert_eq!(pinned, 0);
        assert_eq!(payload_available, 1);
        assert_eq!(content_encoding, "identity");
        assert_eq!(original_size, Some(512));
    }
}
