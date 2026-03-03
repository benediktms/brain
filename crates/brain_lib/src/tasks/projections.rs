use rusqlite::Connection;

use crate::error::{BrainCoreError, Result};

use super::events::{
    CommentPayload, DependencyPayload, EventType, LabelPayload, NoteLinkPayload, ParentSetPayload,
    StatusChangedPayload, TaskCreatedPayload, TaskEvent, TaskUpdatedPayload,
};

/// Apply a single event to the SQLite projection tables.
pub fn apply_event(conn: &Connection, event: &TaskEvent) -> Result<()> {
    match event.event_type {
        EventType::TaskCreated => {
            let p: TaskCreatedPayload = serde_json::from_value(event.payload.clone())
                .map_err(|e| BrainCoreError::TaskEvent(format!("bad TaskCreated payload: {e}")))?;

            conn.execute(
                "INSERT INTO tasks (task_id, title, description, status, priority, due_ts,
                                    task_type, assignee, defer_until, parent_task_id, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                rusqlite::params![
                    event.task_id,
                    p.title,
                    p.description,
                    p.status.as_ref(),
                    p.priority,
                    p.due_ts,
                    p.task_type.as_deref().unwrap_or("task"),
                    p.assignee,
                    p.defer_until,
                    p.parent_task_id,
                    event.timestamp,
                    event.timestamp,
                ],
            )?;
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
            if let Some(ref task_type) = p.task_type {
                set_cols.push("task_type");
                params.push(SqlValue::Text(task_type.clone()));
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

            let sql = format!(
                "UPDATE tasks SET {set_clause} WHERE task_id = ?{task_id_idx}"
            );
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

            conn.execute(
                "UPDATE tasks SET parent_task_id = ?1, updated_at = ?2 WHERE task_id = ?3",
                rusqlite::params![p.parent_task_id, event.timestamp, event.task_id],
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

/// Rebuild all projection tables from a full event sequence.
///
/// Wraps everything in a single transaction for atomicity.
pub fn rebuild(conn: &Connection, events: &[TaskEvent]) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    // Clear in FK-safe order
    tx.execute_batch(
        "DELETE FROM task_events;
         DELETE FROM task_comments;
         DELETE FROM task_labels;
         DELETE FROM task_note_links;
         DELETE FROM task_deps;
         DELETE FROM tasks;",
    )?;

    for event in events {
        apply_event(&tx, event)?;
    }

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;
    use crate::tasks::events::TaskStatus;

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
            },
        )
    }

    #[test]
    fn test_apply_task_created() {
        let conn = setup();
        let ev = make_created_event("t1", "My Task", 2);
        apply_event(&conn, &ev).unwrap();

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
        apply_event(&conn, &ev).unwrap();

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
        apply_event(&conn, &update).unwrap();

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
        apply_event(&conn, &ev).unwrap();

        let status_ev = TaskEvent::from_payload(
            "t1",
            "user",
            StatusChangedPayload {
                new_status: TaskStatus::Done,
            },
        );
        apply_event(&conn, &status_ev).unwrap();

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
        apply_event(&conn, &make_created_event("t1", "Task 1", 2)).unwrap();
        apply_event(&conn, &make_created_event("t2", "Task 2", 2)).unwrap();

        let dep_add = TaskEvent::new(
            "t1",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "t2".to_string(),
            },
        );
        apply_event(&conn, &dep_add).unwrap();

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
        apply_event(&conn, &dep_rm).unwrap();

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
        apply_event(&conn, &make_created_event("t1", "Task", 2)).unwrap();

        let link = TaskEvent::new(
            "t1",
            "user",
            EventType::NoteLinked,
            &NoteLinkPayload {
                chunk_id: "c1".to_string(),
            },
        );
        apply_event(&conn, &link).unwrap();

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
        apply_event(&conn, &unlink).unwrap();

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
        apply_event(&conn, &ev).unwrap();

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
}
