use serde_json::{Value, json};
use tracing::warn;

use crate::tasks::queries::TaskRow;

/// Current time as Unix seconds.
pub fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Convert a Unix-seconds timestamp to an ISO 8601 / RFC 3339 string.
pub fn ts_to_iso(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| ts.to_string())
}

/// Convert an `Option<i64>` timestamp to a JSON value (ISO string or null).
pub fn ts_to_json(ts: Option<i64>) -> Value {
    match ts {
        Some(t) => Value::String(ts_to_iso(t)),
        None => Value::Null,
    }
}

/// Serialize a `TaskRow` and its labels into a JSON object with ISO timestamps.
pub fn task_row_to_json(row: &TaskRow, labels: Vec<String>) -> Value {
    json!({
        "task_id": row.task_id,
        "title": row.title,
        "description": row.description,
        "status": row.status,
        "priority": row.priority,
        "blocked_reason": row.blocked_reason,
        "due_ts": ts_to_json(row.due_ts),
        "task_type": row.task_type,
        "assignee": row.assignee,
        "defer_until": ts_to_json(row.defer_until),
        "parent_task_id": row.parent_task_id,
        "labels": labels,
        "created_at": ts_to_json(Some(row.created_at)),
        "updated_at": ts_to_json(Some(row.updated_at)),
    })
}

/// Accept an ISO 8601 string or an integer from JSON and normalize to `i64` Unix seconds.
pub fn parse_timestamp(val: &Value) -> Option<i64> {
    match val {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                Some(dt.timestamp())
            } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                Some(dt.and_utc().timestamp())
            } else {
                warn!("unparseable timestamp '{s}'");
                None
            }
        }
        _ => None,
    }
}
