//! Writers for the `tasks` projection table.

use rusqlite::{Connection, OptionalExtension};

use crate::db::links::projections::{LinkEvent, apply_link_event};
use crate::db::links::{EdgeKind, EntityRef, LinkCreatedPayload, LinkRemovedPayload};
use crate::error::BrainCoreError;
use crate::sql::{SqlError, SqlResult};

/// Insert a new task row with optimistic hash-collision retry.
///
/// The `display_id_hint` is used as the base for the short display ID if
/// provided (replay path). If absent, the hash is computed from `task_id`.
/// Also dual-writes a `parent_of` edge into `entity_links` if
/// `parent_task_id` is Some.
#[allow(clippy::too_many_arguments)]
pub fn insert_task_row(
    conn: &Connection,
    task_id: &str,
    brain_id: &str,
    title: &str,
    description: Option<&str>,
    status: &str,
    priority: i32,
    due_ts: Option<i64>,
    task_type: &str,
    assignee: Option<&str>,
    defer_until: Option<i64>,
    parent_task_id: Option<&str>,
    child_seq: Option<i64>,
    ts: i64,
    display_id_hint: Option<&str>,
) -> SqlResult<()> {
    let full_hex = crate::db::short_id::blake3_short_hex(task_id);
    let base_len = display_id_hint
        .map(|id| id.len())
        .unwrap_or(crate::db::short_id::MIN_SHORT_HASH_LEN);
    let mut hash_len = base_len;

    loop {
        let id_value = &full_hex[..hash_len];
        let result = conn.execute(
            "INSERT INTO tasks (task_id, brain_id, title, description, status, priority, due_ts,
                                task_type, assignee, defer_until, parent_task_id, child_seq, created_at, updated_at, display_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            rusqlite::params![
                task_id,
                brain_id,
                title,
                description,
                status,
                priority,
                due_ts,
                task_type,
                assignee,
                defer_until,
                parent_task_id,
                child_seq,
                ts,
                ts,
                id_value,
            ],
        );

        match result {
            Ok(_) => break,
            Err(rusqlite::Error::SqliteFailure(err, _)) if err.extended_code == 2067 => {
                // UNIQUE constraint on (brain_id, display_id) — extend hash and retry
                hash_len += 1;
                if hash_len > full_hex.len() {
                    return Err(SqlError::Domain(BrainCoreError::TaskEvent(
                        "short hash collision exhausted all 64 hex chars".into(),
                    )));
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    // Dual-write the parent edge into entity_links if the task was
    // created with a parent already set (mirrors the ParentSet handler).
    if let Some(parent_id) = parent_task_id {
        apply_link_event(
            conn,
            &LinkEvent::Created(LinkCreatedPayload {
                from: EntityRef {
                    kind: crate::db::links::EntityType::Task,
                    id: parent_id.to_string(),
                },
                to: EntityRef {
                    kind: crate::db::links::EntityType::Task,
                    id: task_id.to_string(),
                },
                edge_kind: EdgeKind::ParentOf,
            }),
        )?;
    }

    Ok(())
}

/// UPDATE tasks SET status, updated_at.
pub fn set_task_status(
    conn: &Connection,
    task_id: &str,
    new_status: &str,
    ts: i64,
) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE task_id = ?3",
        rusqlite::params![new_status, ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET parent_task_id, child_seq, updated_at + dual-write LinkCreated parent_of.
pub fn set_task_parent(
    conn: &Connection,
    task_id: &str,
    parent_task_id: &str,
    child_seq: Option<i64>,
    ts: i64,
) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET parent_task_id = ?1, child_seq = ?2, updated_at = ?3 WHERE task_id = ?4",
        rusqlite::params![parent_task_id, child_seq, ts, task_id],
    )?;
    apply_link_event(
        conn,
        &LinkEvent::Created(LinkCreatedPayload {
            from: EntityRef {
                kind: crate::db::links::EntityType::Task,
                id: parent_task_id.to_string(),
            },
            to: EntityRef {
                kind: crate::db::links::EntityType::Task,
                id: task_id.to_string(),
            },
            edge_kind: EdgeKind::ParentOf,
        }),
    )?;
    Ok(())
}

/// UPDATE tasks SET parent_task_id=NULL + dual-write LinkRemoved if a prior parent exists.
///
/// Reads the previous parent from the DB before nulling it so the caller
/// (the projection dispatcher in `brain_tasks`) never needs to issue SQL reads.
pub fn clear_task_parent(conn: &Connection, task_id: &str, ts: i64) -> SqlResult<()> {
    let prev_parent: Option<String> = conn
        .query_row(
            "SELECT parent_task_id FROM tasks WHERE task_id = ?1",
            rusqlite::params![task_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();

    conn.execute(
        "UPDATE tasks SET parent_task_id = NULL, child_seq = NULL, updated_at = ?1 WHERE task_id = ?2",
        rusqlite::params![ts, task_id],
    )?;
    if let Some(old_parent) = prev_parent {
        apply_link_event(
            conn,
            &LinkEvent::Removed(LinkRemovedPayload {
                from: EntityRef {
                    kind: crate::db::links::EntityType::Task,
                    id: old_parent,
                },
                to: EntityRef {
                    kind: crate::db::links::EntityType::Task,
                    id: task_id.to_string(),
                },
                edge_kind: EdgeKind::ParentOf,
            }),
        )?;
    }
    Ok(())
}

/// UPDATE tasks SET assignee.
pub fn claim_task(conn: &Connection, task_id: &str, assignee: &str, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET assignee = ?1, updated_at = ?2 WHERE task_id = ?3",
        rusqlite::params![assignee, ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET assignee = NULL.
pub fn unclaim_task(conn: &Connection, task_id: &str, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET assignee = NULL, updated_at = ?1 WHERE task_id = ?2",
        rusqlite::params![ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET priority.
pub fn change_priority(conn: &Connection, task_id: &str, priority: i32, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET priority = ?1, updated_at = ?2 WHERE task_id = ?3",
        rusqlite::params![priority, ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET title.
pub fn change_title(conn: &Connection, task_id: &str, title: &str, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET title = ?1, updated_at = ?2 WHERE task_id = ?3",
        rusqlite::params![title, ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET description.
pub fn change_description(
    conn: &Connection,
    task_id: &str,
    description: &str,
    ts: i64,
) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET description = ?1, updated_at = ?2 WHERE task_id = ?3",
        rusqlite::params![description, ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET task_type.
pub fn change_task_type(
    conn: &Connection,
    task_id: &str,
    task_type: &str,
    ts: i64,
) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET task_type = ?1, updated_at = ?2 WHERE task_id = ?3",
        rusqlite::params![task_type, ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET due_ts.
pub fn set_due_date(conn: &Connection, task_id: &str, due_ts: i64, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET due_ts = ?1, updated_at = ?2 WHERE task_id = ?3",
        rusqlite::params![due_ts, ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET due_ts = NULL.
pub fn clear_due_date(conn: &Connection, task_id: &str, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET due_ts = NULL, updated_at = ?1 WHERE task_id = ?2",
        rusqlite::params![ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET defer_until.
pub fn defer_task(conn: &Connection, task_id: &str, defer_until: i64, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET defer_until = ?1, updated_at = ?2 WHERE task_id = ?3",
        rusqlite::params![defer_until, ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET defer_until = NULL.
pub fn undefer_task(conn: &Connection, task_id: &str, ts: i64) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET defer_until = NULL, updated_at = ?1 WHERE task_id = ?2",
        rusqlite::params![ts, task_id],
    )?;
    Ok(())
}

/// UPDATE tasks SET blocked_reason.
pub fn set_blocked_reason(
    conn: &Connection,
    task_id: &str,
    reason: &str,
    ts: i64,
) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET blocked_reason = ?1, updated_at = ?2 WHERE task_id = ?3",
        rusqlite::params![reason, ts, task_id],
    )?;
    Ok(())
}

/// Fields for the catch-all multi-field UPDATE used by the `TaskUpdated` arm.
///
/// All fields are `Option` — only `Some` fields are included in the SET clause.
pub struct TaskUpdateFields<'a> {
    pub title: Option<&'a str>,
    pub description: Option<&'a str>,
    pub priority: Option<i32>,
    pub due_ts: Option<i64>,
    pub blocked_reason: Option<&'a str>,
    pub task_type: Option<&'a str>,
    pub assignee: Option<&'a str>,
    pub defer_until: Option<i64>,
}

/// Catch-all multi-field UPDATE for `TaskUpdated` event replay.
///
/// Only fields that are `Some` in `fields` are included in the SET clause.
/// Intent-named writers (`change_title`, `set_due_date`, etc.) are preferred
/// for new code. This writer exists only to keep `TaskUpdated` payload replay
/// correct when a payload field has no matching intent writer.
#[deprecated(
    note = "Use intent-named writers; this exists only to keep TaskUpdated payload replay correct when a payload field has no matching intent writer."
)]
pub fn update_task(
    conn: &Connection,
    task_id: &str,
    fields: &TaskUpdateFields<'_>,
    ts: i64,
) -> SqlResult<()> {
    use rusqlite::types::Value as SqlValue;

    let mut set_cols: Vec<&str> = Vec::new();
    let mut params: Vec<SqlValue> = Vec::new();

    if let Some(title) = fields.title {
        set_cols.push("title");
        params.push(SqlValue::Text(title.to_string()));
    }
    if let Some(description) = fields.description {
        set_cols.push("description");
        params.push(SqlValue::Text(description.to_string()));
    }
    if let Some(priority) = fields.priority {
        set_cols.push("priority");
        params.push(SqlValue::Integer(priority as i64));
    }
    if let Some(due_ts) = fields.due_ts {
        set_cols.push("due_ts");
        params.push(SqlValue::Integer(due_ts));
    }
    if let Some(blocked_reason) = fields.blocked_reason {
        set_cols.push("blocked_reason");
        params.push(SqlValue::Text(blocked_reason.to_string()));
    }
    if let Some(task_type) = fields.task_type {
        set_cols.push("task_type");
        params.push(SqlValue::Text(task_type.to_string()));
    }
    if let Some(assignee) = fields.assignee {
        set_cols.push("assignee");
        params.push(SqlValue::Text(assignee.to_string()));
    }
    if let Some(defer_until) = fields.defer_until {
        set_cols.push("defer_until");
        params.push(SqlValue::Integer(defer_until));
    }

    // Always update the timestamp
    set_cols.push("updated_at");
    params.push(SqlValue::Integer(ts));

    let set_clause: String = set_cols
        .iter()
        .enumerate()
        .map(|(i, col)| format!("{col} = ?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");

    let task_id_idx = params.len() + 1;
    params.push(SqlValue::Text(task_id.to_string()));

    let sql = format!("UPDATE tasks SET {set_clause} WHERE task_id = ?{task_id_idx}");
    conn.execute(&sql, rusqlite::params_from_iter(params))?;

    Ok(())
}

/// UPDATE tasks SET brain_id, display_id (replay path for TaskTransferred).
pub fn transfer_task(
    conn: &Connection,
    task_id: &str,
    to_brain_id: &str,
    to_display_id: &str,
    ts: i64,
) -> SqlResult<()> {
    conn.execute(
        "UPDATE tasks SET brain_id = ?1, display_id = ?2, updated_at = ?3 WHERE task_id = ?4",
        rusqlite::params![to_brain_id, to_display_id, ts, task_id],
    )?;
    Ok(())
}
