use serde_json::Value;
use tracing::warn;

/// Accept an ISO 8601 string or an integer from JSON and normalize to `i64` Unix seconds.
pub(super) fn parse_timestamp(val: &Value) -> Option<i64> {
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

/// Convert a Unix-seconds timestamp to an ISO 8601 / RFC 3339 string.
pub(super) fn ts_to_iso(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| ts.to_string())
}

/// Convert an `Option<i64>` timestamp to a JSON value (ISO string or null).
pub(super) fn ts_to_json(ts: Option<i64>) -> Value {
    match ts {
        Some(t) => Value::String(ts_to_iso(t)),
        None => Value::Null,
    }
}
