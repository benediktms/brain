use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{BrainCoreError, Result};

/// A single event in the task event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    pub event_id: String,
    pub task_id: String,
    pub timestamp: i64,
    pub actor: String,
    pub event_type: EventType,
    #[serde(default = "default_event_version")]
    pub event_version: u32,
    pub payload: serde_json::Value,
}

/// The current event schema version. Bump when event payload format changes.
pub const CURRENT_EVENT_VERSION: u32 = 1;

fn default_event_version() -> u32 {
    CURRENT_EVENT_VERSION
}

/// The set of event types for the task subsystem.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    TaskCreated,
    TaskUpdated,
    StatusChanged,
    DependencyAdded,
    DependencyRemoved,
    NoteLinked,
    NoteUnlinked,
    LabelAdded,
    LabelRemoved,
    CommentAdded,
    ParentSet,
}

/// Valid task statuses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Open,
    InProgress,
    Blocked,
    Done,
    Cancelled,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl AsRef<str> for TaskStatus {
    fn as_ref(&self) -> &str {
        match self {
            TaskStatus::Open => "open",
            TaskStatus::InProgress => "in_progress",
            TaskStatus::Blocked => "blocked",
            TaskStatus::Done => "done",
            TaskStatus::Cancelled => "cancelled",
        }
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "open" => Ok(TaskStatus::Open),
            "in_progress" => Ok(TaskStatus::InProgress),
            "blocked" => Ok(TaskStatus::Blocked),
            "done" => Ok(TaskStatus::Done),
            "cancelled" => Ok(TaskStatus::Cancelled),
            _ => Err(format!("invalid task status: '{s}'")),
        }
    }
}

// -- Typed payloads --

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCreatedPayload {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub priority: i32,
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defer_until: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskUpdatedPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_ts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defer_until: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusChangedPayload {
    pub new_status: TaskStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyPayload {
    pub depends_on_task_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteLinkPayload {
    pub chunk_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelPayload {
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentPayload {
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentSetPayload {
    pub parent_task_id: Option<String>,
}

/// Generate a new UUID v7 event ID.
pub fn new_event_id() -> String {
    Uuid::now_v7().to_string()
}

/// Current time as unix seconds.
pub fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

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
        TaskEvent {
            event_id: new_event_id(),
            task_id: task_id.to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type,
            event_version: CURRENT_EVENT_VERSION,
            payload: serde_json::json!({"title": "Test task", "priority": 2, "status": "open"}),
        }
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
}
