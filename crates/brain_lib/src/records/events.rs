// Type definitions and constructors re-exported from brain_persistence.
pub use brain_persistence::db::records::events::*;

// ---------------------------------------------------------------------------
// File I/O — stays in brain_lib
// ---------------------------------------------------------------------------

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use crate::error::{BrainCoreError, Result};

/// Append a single event to the JSONL file.
///
/// Creates the parent directory if it does not exist.
/// Uses `O_APPEND` for atomic writes (events are well under PIPE_BUF).
pub fn append_event(path: &Path, event: &RecordEvent) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| BrainCoreError::RecordEvent(format!("create parent dir: {e}")))?;
    }

    let mut line = serde_json::to_string(event)
        .map_err(|e| BrainCoreError::RecordEvent(format!("serialize event: {e}")))?;
    line.push('\n');

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| BrainCoreError::RecordEvent(format!("open events file: {e}")))?;

    file.write_all(line.as_bytes())?;
    file.flush()?;
    file.sync_data()?;

    Ok(())
}

/// Read all events from a JSONL file. Skips empty or malformed lines.
///
/// Returns events in append order (oldest first).
pub fn read_all_events(path: &Path) -> Result<Vec<RecordEvent>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(BrainCoreError::RecordEvent(format!(
                "open events file: {e}"
            )));
        }
    };

    let reader = BufReader::new(file);
    let mut events = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<RecordEvent>(trimmed) {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::warn!("skipping malformed record event line: {e}");
            }
        }
    }

    Ok(events)
}

/// Read all events for a specific record ID from a JSONL file.
///
/// Skips empty or malformed lines. Returns events in append order.
pub fn read_events_for_record(path: &Path, record_id: &str) -> Result<Vec<RecordEvent>> {
    let all = read_all_events(path)?;
    Ok(all
        .into_iter()
        .filter(|e| e.record_id == record_id)
        .collect())
}

/// Count the total number of valid events in a JSONL file.
///
/// Skips empty or malformed lines (same as `read_all_events`).
pub fn count_events(path: &Path) -> Result<usize> {
    let all = read_all_events(path)?;
    Ok(all.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_created_event(record_id: &str) -> RecordEvent {
        RecordEvent::from_payload(
            record_id,
            "test-agent",
            RecordCreatedPayload {
                title: "Test Artifact".to_string(),
                kind: "report".to_string(),
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
    fn test_jsonl_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        let ev1 = sample_created_event("r1");
        let ev2 = sample_created_event("r2");

        append_event(&path, &ev1).unwrap();
        append_event(&path, &ev2).unwrap();

        let events = read_all_events(&path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].record_id, "r1");
        assert_eq!(events[1].record_id, "r2");
    }

    #[test]
    fn test_append_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new_events.jsonl");

        assert!(!path.exists());
        let ev = sample_created_event("r1");
        append_event(&path, &ev).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_read_nonexistent_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("missing.jsonl");
        let events = read_all_events(&path).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_malformed_lines_skipped() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        let ev1 = sample_created_event("r1");
        append_event(&path, &ev1).unwrap();

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "{{not valid json").unwrap();
        writeln!(file).unwrap(); // empty line
        drop(file);

        let ev2 = sample_created_event("r2");
        append_event(&path, &ev2).unwrap();

        let events = read_all_events(&path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].record_id, "r1");
        assert_eq!(events[1].record_id, "r2");
    }

    #[test]
    fn test_event_serialization_format() {
        let ev = sample_created_event("r1");
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"event_type\":\"record_created\""));
        assert!(json.contains("\"record_id\":\"r1\""));
        assert!(json.contains("\"event_version\":1"));
    }

    #[test]
    fn test_tag_event_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        let ev = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: "performance".to_string(),
            },
        );
        append_event(&path, &ev).unwrap();

        let events = read_all_events(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, RecordEventType::TagAdded);

        let tag: TagPayload = serde_json::from_value(events[0].payload.clone()).unwrap();
        assert_eq!(tag.tag, "performance");
    }

    #[test]
    fn test_new_record_id_format() {
        let id = new_record_id("BRN");
        assert!(id.starts_with("BRN-"));
        assert_eq!(id.len(), 30); // "BRN-" (4) + ULID (26)
    }

    #[test]
    fn test_append_creates_parent_dir() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nested").join("dir").join("events.jsonl");
        assert!(!path.parent().unwrap().exists());

        let ev = sample_created_event("r1");
        append_event(&path, &ev).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_read_events_for_record_filters_correctly() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        append_event(&path, &sample_created_event("r1")).unwrap();
        append_event(&path, &sample_created_event("r2")).unwrap();
        append_event(
            &path,
            &RecordEvent::new(
                "r1",
                "agent",
                RecordEventType::TagAdded,
                &TagPayload {
                    tag: "x".to_string(),
                },
            ),
        )
        .unwrap();

        let r1_events = read_events_for_record(&path, "r1").unwrap();
        assert_eq!(r1_events.len(), 2);
        assert!(r1_events.iter().all(|e| e.record_id == "r1"));

        let r2_events = read_events_for_record(&path, "r2").unwrap();
        assert_eq!(r2_events.len(), 1);

        let missing = read_events_for_record(&path, "r999").unwrap();
        assert!(missing.is_empty());
    }

    #[test]
    fn test_count_events() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        assert_eq!(count_events(&path).unwrap(), 0);

        for i in 0..5u8 {
            append_event(&path, &sample_created_event(&format!("r{i}"))).unwrap();
        }
        assert_eq!(count_events(&path).unwrap(), 5);
    }
}
