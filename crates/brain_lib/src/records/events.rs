use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::{BrainCoreError, Result};

/// The current event schema version. Bump when event payload format changes.
pub const CURRENT_EVENT_VERSION: u32 = 1;

fn default_event_version() -> u32 {
    CURRENT_EVENT_VERSION
}

/// A single event in the records event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordEvent {
    pub event_id: String,
    pub record_id: String,
    pub timestamp: i64,
    pub actor: String,
    pub event_type: RecordEventType,
    #[serde(default = "default_event_version")]
    pub event_version: u32,
    pub payload: serde_json::Value,
}

/// The set of event types for the records subsystem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordEventType {
    RecordCreated,
    RecordUpdated,
    RecordArchived,
    TagAdded,
    TagRemoved,
    LinkAdded,
    LinkRemoved,
}

// -- Typed payload structs --

/// Payload for `RecordCreated` events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordCreatedPayload {
    pub title: String,
    pub kind: String,
    pub content_ref: ContentRefPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Domain/scope type (e.g. "task", "brain", "global").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_type: Option<String>,
    /// ID of the scoped entity (e.g. a task ID).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_id: Option<String>,
    /// Retention class hint (e.g. "permanent", "ephemeral").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_class: Option<String>,
    /// Producer identifier (agent name, tool name, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
}

/// Inline `ContentRef` as it appears inside a `RecordCreatedPayload`.
///
/// Duplicated here (rather than reusing `super::ContentRef`) so that the
/// payload can be serialized to JSON without adding a dependency on the
/// domain types from within this events module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentRefPayload {
    /// BLAKE3 hex digest of the raw payload bytes (64 hex chars).
    pub hash: String,
    /// Byte length of the payload.
    pub size: u64,
    /// Optional MIME type hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

/// Payload for `RecordUpdated` events.
///
/// All fields are optional; only present fields are updated.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecordUpdatedPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Payload for `RecordArchived` events.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecordArchivedPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Payload for `TagAdded` / `TagRemoved` events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagPayload {
    pub tag: String,
}

/// Payload for `LinkAdded` / `LinkRemoved` events.
///
/// At least one of `task_id` or `chunk_id` must be non-null.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_id: Option<String>,
}

// -- EventPayload trait --

/// Maps a typed payload to its unambiguous `RecordEventType`.
pub trait RecordEventPayload: Serialize {
    fn event_type() -> RecordEventType;
}

impl RecordEventPayload for RecordCreatedPayload {
    fn event_type() -> RecordEventType {
        RecordEventType::RecordCreated
    }
}

impl RecordEventPayload for RecordUpdatedPayload {
    fn event_type() -> RecordEventType {
        RecordEventType::RecordUpdated
    }
}

impl RecordEventPayload for RecordArchivedPayload {
    fn event_type() -> RecordEventType {
        RecordEventType::RecordArchived
    }
}

// -- RecordEvent constructors --

impl RecordEvent {
    /// Create from a typed payload that implements `RecordEventPayload` (event_type inferred).
    pub fn from_payload<P: RecordEventPayload>(
        record_id: impl Into<String>,
        actor: impl Into<String>,
        payload: P,
    ) -> Self {
        Self {
            event_id: new_event_id(),
            record_id: record_id.into(),
            timestamp: crate::utils::now_ts(),
            actor: actor.into(),
            event_type: P::event_type(),
            event_version: CURRENT_EVENT_VERSION,
            payload: serde_json::to_value(payload).unwrap(),
        }
    }

    /// Create from any `Serialize` payload with an explicit `event_type`.
    ///
    /// Use this for `TagAdded`/`TagRemoved` and `LinkAdded`/`LinkRemoved`
    /// where the same payload struct maps to multiple event types.
    pub fn new(
        record_id: impl Into<String>,
        actor: impl Into<String>,
        event_type: RecordEventType,
        payload: &impl Serialize,
    ) -> Self {
        Self {
            event_id: new_event_id(),
            record_id: record_id.into(),
            timestamp: crate::utils::now_ts(),
            actor: actor.into(),
            event_type,
            event_version: CURRENT_EVENT_VERSION,
            payload: serde_json::to_value(payload).unwrap(),
        }
    }

    /// Create from pre-parsed raw JSON with an explicit `event_type`.
    pub fn from_raw(
        record_id: impl Into<String>,
        actor: impl Into<String>,
        event_type: RecordEventType,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            event_id: new_event_id(),
            record_id: record_id.into(),
            timestamp: crate::utils::now_ts(),
            actor: actor.into(),
            event_type,
            event_version: CURRENT_EVENT_VERSION,
            payload,
        }
    }

    /// Override the auto-generated timestamp.
    pub fn with_timestamp(mut self, ts: i64) -> Self {
        self.timestamp = ts;
        self
    }
}

/// Generate a new ULID event ID.
pub fn new_event_id() -> String {
    Ulid::new().to_string()
}

/// Generate a new record ID with project prefix.
/// Format: "{PREFIX}-{ULID}" e.g. "BRN-01JPHZS7VXQK4R3BGTHNED2P8M"
pub fn new_record_id(prefix: &str) -> String {
    format!("{}-{}", prefix, Ulid::new())
}

/// Append a single event to the JSONL file.
///
/// Uses `O_APPEND` for atomic writes (events are well under PIPE_BUF).
pub fn append_event(path: &Path, event: &RecordEvent) -> Result<()> {
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
pub fn read_all_events(path: &Path) -> Result<Vec<RecordEvent>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(BrainCoreError::RecordEvent(format!("open events file: {e}"))),
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
                content_ref: ContentRefPayload {
                    hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        .to_string(),
                    size: 42,
                    media_type: Some("application/json".to_string()),
                },
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
    fn test_link_event_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        let ev = RecordEvent::new(
            "r1",
            "agent",
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: Some("BRN-01XXXXX".to_string()),
                chunk_id: None,
            },
        );
        append_event(&path, &ev).unwrap();

        let events = read_all_events(&path).unwrap();
        assert_eq!(events[0].event_type, RecordEventType::LinkAdded);

        let link: LinkPayload = serde_json::from_value(events[0].payload.clone()).unwrap();
        assert_eq!(link.task_id.as_deref(), Some("BRN-01XXXXX"));
        assert!(link.chunk_id.is_none());
    }

    #[test]
    fn test_archived_payload_serde() {
        let payload = RecordArchivedPayload {
            reason: Some("superseded".to_string()),
        };
        let ev = RecordEvent::from_payload("r1", "agent", payload);
        assert_eq!(ev.event_type, RecordEventType::RecordArchived);

        let p: RecordArchivedPayload = serde_json::from_value(ev.payload).unwrap();
        assert_eq!(p.reason.as_deref(), Some("superseded"));
    }

    #[test]
    fn test_updated_payload_defaults() {
        let payload = RecordUpdatedPayload::default();
        let ev = RecordEvent::from_payload("r1", "agent", payload);
        assert_eq!(ev.event_type, RecordEventType::RecordUpdated);

        let p: RecordUpdatedPayload = serde_json::from_value(ev.payload).unwrap();
        assert!(p.title.is_none());
        assert!(p.description.is_none());
    }

    #[test]
    fn test_with_timestamp_overrides() {
        let ev = RecordEvent::from_payload(
            "r1",
            "agent",
            RecordUpdatedPayload {
                title: Some("Updated".to_string()),
                description: None,
            },
        )
        .with_timestamp(99999);
        assert_eq!(ev.timestamp, 99999);
    }

    #[test]
    fn test_new_record_id_format() {
        let id = new_record_id("BRN");
        assert!(id.starts_with("BRN-"));
        assert_eq!(id.len(), 30); // "BRN-" (4) + ULID (26)
    }

    #[test]
    fn test_content_ref_optional_media_type() {
        let cr = ContentRefPayload {
            hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            size: 0,
            media_type: None,
        };
        let json = serde_json::to_string(&cr).unwrap();
        // media_type should be absent when None (skip_serializing_if)
        assert!(!json.contains("media_type"));
    }
}
