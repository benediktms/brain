use rusqlite::Connection;

use crate::error::{BrainCoreError, Result};

use super::events::{
    CommentPayload, DependencyPayload, EventType, ExternalIdPayload, LabelPayload, NoteLinkPayload,
    ParentSetPayload, StatusChangedPayload, TaskCreatedPayload, TaskEvent, TaskUpdatedPayload,
};
use super::queries::next_child_seq;

/// Apply a single event to SQLite projections (no transaction wrapper).
///
/// Called by `apply_event` (with transaction) and `rebuild` (inside existing tx).
fn apply_event_inner(conn: &Connection, event: &TaskEvent, brain_id: &str) -> Result<()> {
    match event.event_type {
        EventType::TaskCreated => {
            let p: TaskCreatedPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad TaskCreated payload: {e}")))?;

            let child_seq = p
                .parent_task_id
                .as_deref()
                .map(|pid| next_child_seq(conn, pid))
                .transpose()?;

            // Use id from event payload if present (rebuild/replay path).
            // Otherwise compute via BLAKE3. In both cases, optimistic INSERT
            // retries on UNIQUE(brain_id, id) collision by extending the hash.
            // Retry is needed even for payload-provided IDs during rebuild,
            // where brain_id="" collapses cross-brain hash namespaces.
            let full_hex = super::queries::blake3_short_hex(&event.task_id);
            let base_len = p
                .id
                .as_deref()
                .map(|id| id.len())
                .unwrap_or(super::queries::MIN_SHORT_HASH_LEN);
            let mut hash_len = base_len;

            loop {
                let id_value = &full_hex[..hash_len];
                let result = conn.execute(
                    "INSERT INTO tasks (task_id, brain_id, title, description, status, priority, due_ts,
                                        task_type, assignee, defer_until, parent_task_id, child_seq, created_at, updated_at, id)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                    rusqlite::params![
                        event.task_id,
                        brain_id,
                        p.title,
                        p.description,
                        p.status.as_ref(),
                        p.priority,
                        p.due_ts,
                        p.task_type.unwrap_or_default().as_str(),
                        p.assignee,
                        p.defer_until,
                        p.parent_task_id,
                        child_seq,
                        event.timestamp,
                        event.timestamp,
                        id_value,
                    ],
                );

                match result {
                    Ok(_) => break,
                    Err(rusqlite::Error::SqliteFailure(err, _)) if err.extended_code == 2067 => {
                        // UNIQUE constraint on (brain_id, id) — extend hash and retry
                        hash_len += 1;
                        if hash_len > full_hex.len() {
                            return Err(BrainCoreError::TaskEvent(
                                "short hash collision exhausted all 64 hex chars".into(),
                            ));
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }

        EventType::TaskUpdated => {
            use rusqlite::types::Value as SqlValue;

            let p: TaskUpdatedPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad TaskUpdated payload: {e}")))?;

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
            if let Some(priority) = p.priority {
                set_cols.push("priority");
                params.push(SqlValue::Integer(priority as i64));
            }
            if let Some(due_ts) = p.due_ts {
                set_cols.push("due_ts");
                params.push(SqlValue::Integer(due_ts));
            }
            if let Some(ref blocked_reason) = p.blocked_reason {
                set_cols.push("blocked_reason");
                params.push(SqlValue::Text(blocked_reason.clone()));
            }
            if let Some(task_type) = p.task_type {
                set_cols.push("task_type");
                params.push(SqlValue::Text(task_type.as_str().to_string()));
            }
            if let Some(ref assignee) = p.assignee {
                set_cols.push("assignee");
                params.push(SqlValue::Text(assignee.clone()));
            }
            if let Some(defer_until) = p.defer_until {
                set_cols.push("defer_until");
                params.push(SqlValue::Integer(defer_until));
            }

            // Always update the timestamp
            set_cols.push("updated_at");
            params.push(SqlValue::Integer(event.timestamp));

            let set_clause: String = set_cols
                .iter()
                .enumerate()
                .map(|(i, col)| format!("{col} = ?{}", i + 1))
                .collect::<Vec<_>>()
                .join(", ");

            let task_id_idx = params.len() + 1;
            params.push(SqlValue::Text(event.task_id.clone()));

            let sql = format!("UPDATE tasks SET {set_clause} WHERE task_id = ?{task_id_idx}");
            conn.execute(&sql, rusqlite::params_from_iter(params))?;
        }

        EventType::StatusChanged => {
            let p: StatusChangedPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::TaskEvent(format!("bad StatusChanged payload: {e}"))
                })?;

            conn.execute(
                "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE task_id = ?3",
                rusqlite::params![p.new_status.as_ref(), event.timestamp, event.task_id],
            )?;
        }

        EventType::DependencyAdded => {
            let p: DependencyPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::TaskEvent(format!("bad DependencyAdded payload: {e}"))
                })?;

            conn.execute(
                "INSERT OR IGNORE INTO task_deps (task_id, depends_on) VALUES (?1, ?2)",
                rusqlite::params![event.task_id, p.depends_on_task_id],
            )?;
        }

        EventType::DependencyRemoved => {
            let p: DependencyPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::TaskEvent(format!("bad DependencyRemoved payload: {e}"))
                })?;

            conn.execute(
                "DELETE FROM task_deps WHERE task_id = ?1 AND depends_on = ?2",
                rusqlite::params![event.task_id, p.depends_on_task_id],
            )?;
        }

        EventType::NoteLinked => {
            let p: NoteLinkPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad NoteLinked payload: {e}")))?;

            conn.execute(
                "INSERT OR IGNORE INTO task_note_links (task_id, chunk_id) VALUES (?1, ?2)",
                rusqlite::params![event.task_id, p.chunk_id],
            )?;
        }

        EventType::NoteUnlinked => {
            let p: NoteLinkPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad NoteUnlinked payload: {e}")))?;

            conn.execute(
                "DELETE FROM task_note_links WHERE task_id = ?1 AND chunk_id = ?2",
                rusqlite::params![event.task_id, p.chunk_id],
            )?;
        }

        EventType::LabelAdded => {
            let p: LabelPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad LabelAdded payload: {e}")))?;

            conn.execute(
                "INSERT OR IGNORE INTO task_labels (task_id, label) VALUES (?1, ?2)",
                rusqlite::params![event.task_id, p.label],
            )?;
        }

        EventType::LabelRemoved => {
            let p: LabelPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad LabelRemoved payload: {e}")))?;

            conn.execute(
                "DELETE FROM task_labels WHERE task_id = ?1 AND label = ?2",
                rusqlite::params![event.task_id, p.label],
            )?;
        }

        EventType::CommentAdded => {
            let p: CommentPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad CommentAdded payload: {e}")))?;

            conn.execute(
                "INSERT INTO task_comments (comment_id, task_id, author, body, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    event.event_id,
                    event.task_id,
                    event.actor,
                    p.body,
                    event.timestamp,
                ],
            )?;
        }

        EventType::ParentSet => {
            let p: ParentSetPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad ParentSet payload: {e}")))?;

            let child_seq = p
                .parent_task_id
                .as_deref()
                .map(|pid| next_child_seq(conn, pid))
                .transpose()?;

            conn.execute(
                "UPDATE tasks SET parent_task_id = ?1, child_seq = ?2, updated_at = ?3 WHERE task_id = ?4",
                rusqlite::params![p.parent_task_id, child_seq, event.timestamp, event.task_id],
            )?;
        }

        EventType::ExternalIdAdded => {
            let p: ExternalIdPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::TaskEvent(format!("bad ExternalIdAdded payload: {e}"))
                })?;

            conn.execute(
                "INSERT OR IGNORE INTO task_external_ids (task_id, source, external_id, external_url, imported_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![event.task_id, p.source, p.external_id, p.external_url, event.timestamp],
            )?;
        }

        EventType::ExternalIdRemoved => {
            let p: ExternalIdPayload =
                serde_json::from_value(event.payload.clone()).map_err(|e| {
                    BrainCoreError::TaskEvent(format!("bad ExternalIdRemoved payload: {e}"))
                })?;

            conn.execute(
                "DELETE FROM task_external_ids WHERE task_id = ?1 AND source = ?2 AND external_id = ?3",
                rusqlite::params![event.task_id, p.source, p.external_id],
            )?;
        }
    }

    // Record the event itself
    let payload_json = serde_json::to_string(&event.payload).unwrap_or_else(|_| "{}".into());
    conn.execute(
        "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            event.event_id,
            event.task_id,
            format!("{:?}", event.event_type),
            event.timestamp,
            event.actor,
            payload_json,
        ],
    )?;

    Ok(())
}

/// Apply a single event to the SQLite projection tables.
///
/// `brain_id` is stamped on the `tasks` row for all `TaskCreated` events.
/// For all other event types the brain_id is not re-written (the row already
/// carries the brain_id set at creation time).
///
/// The projection mutation and event INSERT are wrapped in an explicit
/// transaction for atomicity.
pub fn apply_event(conn: &Connection, event: &TaskEvent, brain_id: &str) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    apply_event_inner(&tx, event, brain_id)?;
    tx.commit()?;
    Ok(())
}

/// Validate an event and apply it in a single transaction.
///
/// Validation rules:
/// - `TaskCreated`: task_id must NOT already exist; parent must exist if set
/// - `TaskUpdated`/`StatusChanged`: task must exist
/// - `DependencyAdded`: both tasks must exist, no cycle
/// - `ParentSet`: task must exist; parent must exist and not be self
/// - Others: task must exist
pub fn validate_and_apply(conn: &Connection, event: &TaskEvent, brain_id: &str) -> Result<()> {
    use super::cycle;
    use super::queries::task_exists;

    let tx = conn.unchecked_transaction()?;

    // ── Validation ────────────────────────────────────────────────
    match event.event_type {
        EventType::TaskCreated => {
            if task_exists(&tx, &event.task_id)? {
                return Err(BrainCoreError::TaskEvent(format!(
                    "task already exists: {}",
                    event.task_id
                )));
            }
            let payload: TaskCreatedPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad TaskCreated payload: {e}")))?;
            if let Some(ref parent_id) = payload.parent_task_id {
                if parent_id == &event.task_id {
                    return Err(BrainCoreError::TaskEvent(
                        "task cannot be its own parent".to_string(),
                    ));
                }
                if !task_exists(&tx, parent_id)? {
                    return Err(BrainCoreError::TaskEvent(format!(
                        "parent task not found: {parent_id}"
                    )));
                }
            }
        }

        EventType::TaskUpdated | EventType::StatusChanged => {
            if !task_exists(&tx, &event.task_id)? {
                return Err(BrainCoreError::TaskEvent(format!(
                    "task not found: {}",
                    event.task_id
                )));
            }
        }

        EventType::DependencyAdded => {
            if !task_exists(&tx, &event.task_id)? {
                return Err(BrainCoreError::TaskEvent(format!(
                    "task not found: {}",
                    event.task_id
                )));
            }
            let payload: DependencyPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| {
                    BrainCoreError::TaskEvent(format!("bad DependencyAdded payload: {e}"))
                })?;
            if !task_exists(&tx, &payload.depends_on_task_id)? {
                return Err(BrainCoreError::TaskEvent(format!(
                    "dependency target not found: {}",
                    payload.depends_on_task_id
                )));
            }
            cycle::check_cycle(&tx, &event.task_id, &payload.depends_on_task_id)?;
        }

        EventType::ParentSet => {
            if !task_exists(&tx, &event.task_id)? {
                return Err(BrainCoreError::TaskEvent(format!(
                    "task not found: {}",
                    event.task_id
                )));
            }
            let payload: ParentSetPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad ParentSet payload: {e}")))?;
            if let Some(ref parent_id) = payload.parent_task_id {
                if parent_id == &event.task_id {
                    return Err(BrainCoreError::TaskEvent(
                        "task cannot be its own parent".to_string(),
                    ));
                }
                if !task_exists(&tx, parent_id)? {
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
            if !task_exists(&tx, &event.task_id)? {
                return Err(BrainCoreError::TaskEvent(format!(
                    "task not found: {}",
                    event.task_id
                )));
            }
        }
    }

    // ── Apply ─────────────────────────────────────────────────────
    apply_event_inner(&tx, event, brain_id)?;
    tx.commit()?;
    Ok(())
}

/// Rebuild all projection tables from a full event sequence.
///
/// Drops FTS task triggers before the bulk delete to avoid content-sync
/// deletes on a potentially corrupt index, then rebuilds the FTS index
/// and re-creates triggers after commit.
pub fn rebuild(conn: &Connection, events: &[TaskEvent]) -> Result<()> {
    // Drop FTS triggers to avoid content-sync deletes on potentially corrupt index
    conn.execute_batch(
        "DROP TRIGGER IF EXISTS tasks_fts_insert;
         DROP TRIGGER IF EXISTS tasks_fts_delete;
         DROP TRIGGER IF EXISTS tasks_fts_update;",
    )?;

    let tx = conn.unchecked_transaction()?;

    // Clear in FK-safe order (no FTS triggers fire now)
    tx.execute_batch(
        "DELETE FROM task_events;
         DELETE FROM task_comments;
         DELETE FROM task_labels;
         DELETE FROM task_note_links;
         DELETE FROM task_external_ids;
         DELETE FROM task_deps;
         DELETE FROM tasks;",
    )?;

    for event in events {
        // During rebuild, preserve the existing brain_id value from the event log
        // by reading it from the DB if available, or default to empty string.
        apply_event_inner(&tx, event, "")?;
    }

    tx.commit()?;

    // Rebuild FTS index from content table (must be outside transaction)
    conn.execute("INSERT INTO fts_tasks(fts_tasks) VALUES('rebuild')", [])?;

    // Re-create triggers
    crate::db::schema::ensure_fts5(conn)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;
    use crate::db::tasks::events::TaskStatus;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn make_created_event(task_id: &str, title: &str, priority: i32) -> TaskEvent {
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
                id: None,
            },
        )
    }

    #[test]
    fn test_apply_task_created() {
        let conn = setup();
        let ev = make_created_event("t1", "My Task", 2);
        apply_event(&conn, &ev, "").unwrap();

        let title: String = conn
            .query_row("SELECT title FROM tasks WHERE task_id = 't1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(title, "My Task");

        let priority: i32 = conn
            .query_row(
                "SELECT priority FROM tasks WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(priority, 2);
    }

    #[test]
    fn test_apply_task_updated() {
        let conn = setup();
        let ev = make_created_event("t1", "Original", 4);
        apply_event(&conn, &ev, "").unwrap();

        let update = TaskEvent::from_payload(
            "t1",
            "user",
            TaskUpdatedPayload {
                title: Some("Updated".to_string()),
                description: Some("A description".to_string()),
                priority: Some(1),
                due_ts: None,
                blocked_reason: None,
                task_type: None,
                assignee: None,
                defer_until: None,
            },
        );
        apply_event(&conn, &update, "").unwrap();

        let (title, desc, priority): (String, Option<String>, i32) = conn
            .query_row(
                "SELECT title, description, priority FROM tasks WHERE task_id = 't1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(title, "Updated");
        assert_eq!(desc.as_deref(), Some("A description"));
        assert_eq!(priority, 1);
    }

    #[test]
    fn test_apply_status_changed() {
        let conn = setup();
        let ev = make_created_event("t1", "Task", 2);
        apply_event(&conn, &ev, "").unwrap();

        let status_ev = TaskEvent::from_payload(
            "t1",
            "user",
            StatusChangedPayload {
                new_status: TaskStatus::Done,
            },
        );
        apply_event(&conn, &status_ev, "").unwrap();

        let status: String = conn
            .query_row("SELECT status FROM tasks WHERE task_id = 't1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(status, "done");
    }

    #[test]
    fn test_apply_dependency_added_removed() {
        let conn = setup();
        apply_event(&conn, &make_created_event("t1", "Task 1", 2), "").unwrap();
        apply_event(&conn, &make_created_event("t2", "Task 2", 2), "").unwrap();

        let dep_add = TaskEvent::new(
            "t1",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "t2".to_string(),
            },
        );
        apply_event(&conn, &dep_add, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_deps WHERE task_id = 't1' AND depends_on = 't2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Remove
        let dep_rm = TaskEvent::new(
            "t1",
            "user",
            EventType::DependencyRemoved,
            &DependencyPayload {
                depends_on_task_id: "t2".to_string(),
            },
        );
        apply_event(&conn, &dep_rm, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_deps WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_apply_note_linked_unlinked() {
        let conn = setup();
        apply_event(&conn, &make_created_event("t1", "Task", 2), "").unwrap();

        // Create a file and chunk to satisfy FK constraint on task_note_links
        conn.execute(
            "INSERT INTO files (file_id, path, indexing_state) VALUES ('f1', '/test.md', 'idle')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content) VALUES ('c1', 'f1', 0, 'h0', 'test')",
            [],
        )
        .unwrap();

        let link = TaskEvent::new(
            "t1",
            "user",
            EventType::NoteLinked,
            &NoteLinkPayload {
                chunk_id: "c1".to_string(),
            },
        );
        apply_event(&conn, &link, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_note_links WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let unlink = TaskEvent::new(
            "t1",
            "user",
            EventType::NoteUnlinked,
            &NoteLinkPayload {
                chunk_id: "c1".to_string(),
            },
        );
        apply_event(&conn, &unlink, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_note_links WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_event_recorded_in_task_events() {
        let conn = setup();
        let ev = make_created_event("t1", "Task", 2);
        apply_event(&conn, &ev, "").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM task_events WHERE task_id = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_rebuild_idempotent() {
        let conn = setup();

        let events = vec![
            make_created_event("t1", "Task 1", 1),
            make_created_event("t2", "Task 2", 3),
        ];

        rebuild(&conn, &events).unwrap();
        let count1: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count1, 2);

        // Rebuild again — should produce same state
        rebuild(&conn, &events).unwrap();
        let count2: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count2, 2);
    }

    #[test]
    fn test_rebuild_survives_corrupt_fts_index() {
        let conn = setup();

        // Insert tasks so FTS has content
        let events = vec![
            make_created_event("t1", "Alpha searchable task", 1),
            make_created_event("t2", "Beta findable task", 2),
        ];
        rebuild(&conn, &events).unwrap();

        // Corrupt the FTS index by deleting shadow table data
        conn.execute_batch("DELETE FROM fts_tasks_data;").unwrap();

        // Rebuild must succeed despite corrupt FTS index
        rebuild(&conn, &events).unwrap();

        // Verify FTS search works after rebuild
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_tasks WHERE fts_tasks MATCH 'searchable'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts_tasks WHERE fts_tasks MATCH 'findable'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }
}
