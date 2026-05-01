use rusqlite::Connection;

use crate::db::links::{
    EdgeKind, EntityRef, LinkCreatedPayload, LinkEvent, LinkRemovedPayload, apply_link_event,
};
use crate::error::{BrainCoreError, Result};

use super::events::{
    LinkPayload, PayloadEvictedPayload, RecordArchivedPayload, RecordCreatedPayload, RecordEvent,
    RecordEventType, RecordUpdatedPayload, RetentionClassSetPayload, TagPayload,
};

fn searchable_for_kind(kind: &str) -> bool {
    // Mirrors brain_lib::records::RecordKind::policy().searchable and must stay in sync.
    !matches!(kind, "snapshot")
}

/// Resolve a `LinkPayload` to a typed `EntityRef`.
///
/// Exactly one of `task_id` / `chunk_id` must be non-null. Both null or both
/// non-null are invalid; an error is returned.
fn link_payload_to_entity_ref(p: &LinkPayload) -> Result<EntityRef> {
    match (&p.task_id, &p.chunk_id) {
        (Some(tid), None) => EntityRef::task(tid).map_err(|_| {
            BrainCoreError::RecordEvent("link payload task_id must not be empty".into())
        }),
        (None, Some(cid)) => EntityRef::chunk(cid).map_err(|_| {
            BrainCoreError::RecordEvent("link payload chunk_id must not be empty".into())
        }),
        (None, None) => Err(BrainCoreError::RecordEvent(
            "link payload must have exactly one of task_id or chunk_id, got neither".into(),
        )),
        (Some(_), Some(_)) => Err(BrainCoreError::RecordEvent(
            "link payload must have exactly one of task_id or chunk_id, got both".into(),
        )),
    }
}

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
                     retention_class, pinned, payload_available, content_encoding, original_size,
                      searchable)
                  VALUES (?1, ?2, ?3, ?4, 'active', ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                          ?13, 0, 1, ?14, ?15,
                          ?16)",
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
                    searchable_for_kind(&p.kind),
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

            let to = link_payload_to_entity_ref(&p)?;
            let from = EntityRef::record(&event.record_id).map_err(|_| {
                BrainCoreError::RecordEvent("LinkAdded: record_id must not be empty".into())
            })?;
            apply_link_event(
                conn,
                &LinkEvent::Created(LinkCreatedPayload {
                    from,
                    to,
                    edge_kind: EdgeKind::Covers,
                }),
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

            let to = link_payload_to_entity_ref(&p)?;
            let from = EntityRef::record(&event.record_id).map_err(|_| {
                BrainCoreError::RecordEvent("LinkRemoved: record_id must not be empty".into())
            })?;
            apply_link_event(
                conn,
                &LinkEvent::Removed(LinkRemovedPayload {
                    from,
                    to,
                    edge_kind: EdgeKind::Covers,
                }),
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
        ContentRefPayload, LinkPayload, RecordArchivedPayload, RecordCreatedPayload, RecordEvent,
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

    #[test]
    fn test_searchable_default_by_kind() {
        let conn = setup();

        for (id, title, kind, expected) in [
            ("r-document", "Document", "document", 1),
            ("r-analysis", "Analysis", "analysis", 1),
            ("r-plan", "Plan", "plan", 1),
            ("r-summary", "Summary", "summary", 1),
            ("r-implementation", "Implementation", "implementation", 1),
            ("r-review", "Review", "review", 1),
            ("r-snapshot", "Snapshot", "snapshot", 0),
            ("r-custom", "Custom", "my-custom-kind", 1),
        ] {
            let ev = make_created_event(id, title, kind);
            apply_event(&conn, &ev, "").unwrap();

            let searchable: i32 = conn
                .query_row(
                    "SELECT searchable FROM records WHERE record_id = ?1",
                    [id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(searchable, expected, "{kind} searchable mismatch");
        }
    }

    fn make_link_added_event(
        record_id: &str,
        task_id: Option<&str>,
        chunk_id: Option<&str>,
    ) -> RecordEvent {
        RecordEvent::new(
            record_id,
            "test-agent",
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: task_id.map(str::to_string),
                chunk_id: chunk_id.map(str::to_string),
            },
        )
    }

    fn make_link_removed_event(
        record_id: &str,
        task_id: Option<&str>,
        chunk_id: Option<&str>,
    ) -> RecordEvent {
        RecordEvent::new(
            record_id,
            "test-agent",
            RecordEventType::LinkRemoved,
            &LinkPayload {
                task_id: task_id.map(str::to_string),
                chunk_id: chunk_id.map(str::to_string),
            },
        )
    }

    fn entity_links_count(conn: &Connection, from_id: &str, to_id: &str, to_type: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM entity_links
             WHERE from_type = 'RECORD' AND from_id = ?1
               AND to_type = ?2 AND to_id = ?3
               AND edge_kind = 'covers'",
            rusqlite::params![from_id, to_type, to_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn link_added_task_dual_writes_entity_links() {
        let conn = setup();
        apply_event(&conn, &make_created_event("rec-1", "R", "report"), "").unwrap();

        apply_event(
            &conn,
            &make_link_added_event("rec-1", Some("task-42"), None),
            "",
        )
        .unwrap();

        let rl: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_links WHERE record_id = 'rec-1' AND task_id = 'task-42'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rl, 1, "record_links row missing");

        assert_eq!(
            entity_links_count(&conn, "rec-1", "task-42", "TASK"),
            1,
            "entity_links row missing for task path"
        );
    }

    #[test]
    fn link_added_chunk_dual_writes_entity_links() {
        let conn = setup();
        apply_event(&conn, &make_created_event("rec-2", "R", "report"), "").unwrap();

        apply_event(
            &conn,
            &make_link_added_event("rec-2", None, Some("chunk-99")),
            "",
        )
        .unwrap();

        let rl: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_links WHERE record_id = 'rec-2' AND chunk_id = 'chunk-99'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rl, 1, "record_links row missing");

        assert_eq!(
            entity_links_count(&conn, "rec-2", "chunk-99", "CHUNK"),
            1,
            "entity_links row missing for chunk path"
        );
    }

    #[test]
    fn link_removed_task_removes_from_entity_links() {
        let conn = setup();
        apply_event(&conn, &make_created_event("rec-3", "R", "report"), "").unwrap();
        apply_event(
            &conn,
            &make_link_added_event("rec-3", Some("task-7"), None),
            "",
        )
        .unwrap();

        apply_event(
            &conn,
            &make_link_removed_event("rec-3", Some("task-7"), None),
            "",
        )
        .unwrap();

        let rl: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_links WHERE record_id = 'rec-3'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rl, 0, "record_links row should be deleted");

        assert_eq!(
            entity_links_count(&conn, "rec-3", "task-7", "TASK"),
            0,
            "entity_links row should be deleted for task path"
        );
    }

    #[test]
    fn link_removed_chunk_removes_from_entity_links() {
        let conn = setup();
        apply_event(&conn, &make_created_event("rec-4", "R", "report"), "").unwrap();
        apply_event(
            &conn,
            &make_link_added_event("rec-4", None, Some("chunk-55")),
            "",
        )
        .unwrap();

        apply_event(
            &conn,
            &make_link_removed_event("rec-4", None, Some("chunk-55")),
            "",
        )
        .unwrap();

        let rl: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM record_links WHERE record_id = 'rec-4'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rl, 0, "record_links row should be deleted");

        assert_eq!(
            entity_links_count(&conn, "rec-4", "chunk-55", "CHUNK"),
            0,
            "entity_links row should be deleted for chunk path"
        );
    }

    #[test]
    fn link_added_both_null_errors() {
        let conn = setup();
        apply_event(&conn, &make_created_event("rec-5", "R", "report"), "").unwrap();
        let ev = make_link_added_event("rec-5", None, None);
        let result = apply_event(&conn, &ev, "");
        assert!(result.is_err(), "both-null should return an error");
    }

    #[test]
    fn link_added_both_set_errors() {
        let conn = setup();
        apply_event(&conn, &make_created_event("rec-6", "R", "report"), "").unwrap();
        let ev = make_link_added_event("rec-6", Some("task-1"), Some("chunk-1"));
        let result = apply_event(&conn, &ev, "");
        assert!(result.is_err(), "both-set should return an error");
    }
}
