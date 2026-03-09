use serde_json::Value;
use tracing::warn;

pub use crate::tasks::enrichment::task_row_to_json;

/// Normalize content for deterministic hashing:
/// - Strip trailing whitespace per line
/// - Normalize line endings to LF
fn normalize_content(text: &str) -> String {
    text.lines()
        .map(|line| line.trim_end())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Compute BLAKE3 hash of normalized content, hex encoded.
pub fn content_hash(text: &str) -> String {
    let normalized = normalize_content(text);
    blake3::hash(normalized.as_bytes()).to_hex().to_string()
}

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

/// Accept an ISO 8601 string or an integer from JSON and normalize to `i64` Unix seconds.
///
/// Supports RFC 3339 strings, naive `%Y-%m-%dT%H:%M:%S` strings, and plain integers.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_strips_trailing_whitespace() {
        let input = "hello   \nworld\t\n";
        let normalized = normalize_content(input);
        assert_eq!(normalized, "hello\nworld");
    }

    #[test]
    fn test_normalize_crlf_to_lf() {
        let input = "hello\r\nworld\r\n";
        let normalized = normalize_content(input);
        assert_eq!(normalized, "hello\nworld");
    }

    #[test]
    fn test_hash_deterministic() {
        let h1 = content_hash("hello world");
        let h2 = content_hash("hello world");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_differs_for_different_content() {
        let h1 = content_hash("hello");
        let h2 = content_hash("world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_whitespace_normalization_same_hash() {
        let h1 = content_hash("hello   \nworld\n");
        let h2 = content_hash("hello\nworld\n");
        assert_eq!(h1, h2);
    }
}
