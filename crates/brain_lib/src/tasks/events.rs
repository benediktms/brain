// Type definitions and constructors re-exported from brain_persistence.
pub use brain_persistence::db::tasks::events::*;

// ---------------------------------------------------------------------------
// File I/O — stays in brain_lib (uses local error + std::io)
// ---------------------------------------------------------------------------

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use crate::error::{BrainCoreError, Result};

/// Append a single event to the JSONL file.
///
/// Uses `O_APPEND` for atomic writes (events are well under PIPE_BUF).
pub fn append_event(path: &Path, event: &TaskEvent) -> Result<()> {
    let mut line = serde_json::to_string(event)
        .map_err(|e| BrainCoreError::TaskEvent(format!("serialize event: {e}")))?;
    line.push('\n');

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| BrainCoreError::TaskEvent(format!("open events file: {e}")))?;

    file.write_all(line.as_bytes())?;
    file.flush()?;
    file.sync_data()?;

    Ok(())
}

/// Read all events from a JSONL file. Skips empty or malformed lines.
pub fn read_all_events(path: &Path) -> Result<Vec<TaskEvent>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(BrainCoreError::TaskEvent(format!("open events file: {e}"))),
    };

    let reader = BufReader::new(file);
    let mut events = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<TaskEvent>(trimmed) {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::warn!("skipping malformed task event line: {e}");
            }
        }
    }

    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_event(task_id: &str, event_type: EventType) -> TaskEvent {
        TaskEvent::from_raw(
            task_id,
            "user",
            event_type,
            serde_json::json!({"title": "Test task", "priority": 2, "status": "open"}),
        )
    }

    #[test]
    fn test_jsonl_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        let ev1 = sample_event("t1", EventType::TaskCreated);
        let ev2 = sample_event("t2", EventType::TaskCreated);

        append_event(&path, &ev1).unwrap();
        append_event(&path, &ev2).unwrap();

        let events = read_all_events(&path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].task_id, "t1");
        assert_eq!(events[1].task_id, "t2");
    }

    #[test]
    fn test_append_creates_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new_events.jsonl");

        assert!(!path.exists());
        let ev = sample_event("t1", EventType::TaskCreated);
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

        // Write a valid event, then garbage, then another valid event
        let ev1 = sample_event("t1", EventType::TaskCreated);
        append_event(&path, &ev1).unwrap();

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "{{not valid json").unwrap();
        writeln!(file).unwrap(); // empty line
        drop(file);

        let ev2 = sample_event("t2", EventType::TaskCreated);
        append_event(&path, &ev2).unwrap();

        let events = read_all_events(&path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].task_id, "t1");
        assert_eq!(events[1].task_id, "t2");
    }

    #[test]
    fn test_event_serialization_format() {
        let ev = sample_event("t1", EventType::StatusChanged);
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"event_type\":\"status_changed\""));
        assert!(json.contains("\"task_id\":\"t1\""));
    }

    #[test]
    fn test_from_payload_sets_correct_event_type() {
        let ev = TaskEvent::from_payload(
            "t1",
            "user",
            StatusChangedPayload {
                new_status: TaskStatus::Done,
            },
        );
        assert_eq!(ev.event_type, EventType::StatusChanged);
        assert_eq!(ev.task_id, "t1");
        assert_eq!(ev.actor, "user");

        let ev = TaskEvent::from_payload(
            "t2",
            "user",
            TaskCreatedPayload {
                title: "Test".to_string(),
                description: None,
                priority: 2,
                status: TaskStatus::Open,
                due_ts: None,
                task_type: None,
                assignee: None,
                defer_until: None,
                parent_task_id: None,
            },
        );
        assert_eq!(ev.event_type, EventType::TaskCreated);

        let ev = TaskEvent::from_payload(
            "t3",
            "user",
            CommentPayload {
                body: "hello".to_string(),
            },
        );
        assert_eq!(ev.event_type, EventType::CommentAdded);

        let ev = TaskEvent::from_payload(
            "t4",
            "user",
            ParentSetPayload {
                parent_task_id: Some("p1".to_string()),
            },
        );
        assert_eq!(ev.event_type, EventType::ParentSet);

        let ev = TaskEvent::from_payload(
            "t5",
            "user",
            TaskUpdatedPayload {
                title: Some("New".to_string()),
                description: None,
                priority: None,
                due_ts: None,
                blocked_reason: None,
                task_type: None,
                assignee: None,
                defer_until: None,
            },
        );
        assert_eq!(ev.event_type, EventType::TaskUpdated);
    }

    #[test]
    fn test_new_sets_event_version() {
        let ev = TaskEvent::new(
            "t1",
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: "t2".to_string(),
            },
        );
        assert_eq!(ev.event_version, CURRENT_EVENT_VERSION);
        assert_eq!(ev.event_type, EventType::DependencyAdded);
    }

    #[test]
    fn test_with_timestamp_overrides() {
        let ev = TaskEvent::from_payload(
            "t1",
            "user",
            StatusChangedPayload {
                new_status: TaskStatus::Done,
            },
        )
        .with_timestamp(12345);
        assert_eq!(ev.timestamp, 12345);
    }

    #[test]
    fn test_from_raw_preserves_payload() {
        let payload = serde_json::json!({"new_status": "done"});
        let ev = TaskEvent::from_raw("t1", "user", EventType::StatusChanged, payload.clone());
        assert_eq!(ev.payload, payload);
        assert_eq!(ev.event_type, EventType::StatusChanged);
        assert_eq!(ev.event_version, CURRENT_EVENT_VERSION);
    }
}
