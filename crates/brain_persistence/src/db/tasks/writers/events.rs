//! Writers for the `task_events` log table.

use rusqlite::Connection;
use ulid::Ulid;

use crate::sql::SqlResult;

/// INSERT into task_events (the event log append).
///
/// `event_type` should be formatted by the caller via `format!("{:?}", event.event_type)`.
/// `payload_json` should be serialized by the caller via
/// `serde_json::to_string(&event.payload).unwrap_or_else(|_| "{}".into())`.
pub fn append_task_event_log(
    conn: &Connection,
    event_id: &str,
    task_id: &str,
    event_type: &str,
    ts: i64,
    actor: &str,
    payload_json: &str,
) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO task_events (event_id, task_id, event_type, timestamp, actor, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![event_id, task_id, event_type, ts, actor, payload_json],
    )?;
    Ok(())
}

/// Append a `StatusChanged` row to the task_events log. Used by callers that
/// need to record a status transition without constructing a domain `TaskEvent`
/// (saga cascade flow).
///
/// Returns the generated event_id.
pub fn append_status_changed_event(
    conn: &Connection,
    task_id: &str,
    new_status: &str,
    actor: &str,
    ts: i64,
) -> SqlResult<String> {
    let event_id = Ulid::new().to_string();
    // Hand-roll the payload to match `serde_json::to_string(&StatusChangedPayload { new_status })`
    // byte-for-byte: `{"new_status":"<status>"}`. Status strings are tiny enum
    // members ("open", "in_progress", "done", "cancelled", "blocked") — no
    // injection risk.
    let payload = format!(r#"{{"new_status":"{new_status}"}}"#);
    append_task_event_log(
        conn,
        &event_id,
        task_id,
        "StatusChanged",
        ts,
        actor,
        &payload,
    )?;
    Ok(event_id)
}

/// Append a `TaskTransferred` row to the task_events log. Used by the transfer
/// transaction.
///
/// Returns the generated event_id.
#[allow(clippy::too_many_arguments)]
pub fn append_task_transferred_event(
    conn: &Connection,
    task_id: &str,
    from_brain_id: &str,
    to_brain_id: &str,
    from_display_id: &str,
    to_display_id: &str,
    actor: &str,
    ts: i64,
) -> SqlResult<String> {
    let event_id = Ulid::new().to_string();
    let payload = serde_json::json!({
        "from_brain_id": from_brain_id,
        "to_brain_id": to_brain_id,
        "from_display_id": from_display_id,
        "to_display_id": to_display_id,
    })
    .to_string();
    append_task_event_log(
        conn,
        &event_id,
        task_id,
        "TaskTransferred",
        ts,
        actor,
        &payload,
    )?;
    Ok(event_id)
}
