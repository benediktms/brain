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
    PayloadEvicted,
    RetentionClassSet,
    RecordPinned,
    RecordUnpinned,
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
    /// Byte length of the stored blob (may be compressed).
    pub size: u64,
    /// Optional MIME type hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Content encoding: `"identity"` or `"zstd"`.  Defaults to `"identity"`
    /// for backward compatibility with events written before compression.
    #[serde(default = "default_encoding", skip_serializing_if = "is_identity")]
    pub content_encoding: String,
    /// Original (pre-compression) byte length.  Defaults to `size` for
    /// backward compatibility with events written before compression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_size: Option<u64>,
}

impl ContentRefPayload {
    /// Create a new `ContentRefPayload` for an uncompressed blob.
    pub fn new(hash: String, size: u64, media_type: Option<String>) -> Self {
        Self {
            hash,
            size,
            media_type,
            content_encoding: "identity".to_string(),
            original_size: None,
        }
    }

    /// Create a new `ContentRefPayload` for a (possibly) compressed blob.
    pub fn compressed(
        hash: String,
        stored_size: u64,
        media_type: Option<String>,
        encoding: String,
        original_size: u64,
    ) -> Self {
        Self {
            hash,
            size: stored_size,
            media_type,
            content_encoding: encoding,
            original_size: Some(original_size),
        }
    }
}

fn default_encoding() -> String {
    "identity".to_string()
}

fn is_identity(s: &str) -> bool {
    s == "identity"
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

/// Payload for `PayloadEvicted` events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadEvictedPayload {
    pub content_hash: String,
    pub reason: String,
}

/// Payload for `RetentionClassSet` events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionClassSetPayload {
    pub retention_class: Option<String>,
}

/// Payload for `RecordPinned` / `RecordUnpinned` events (empty payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinPayload {}

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

impl RecordEventPayload for PayloadEvictedPayload {
    fn event_type() -> RecordEventType {
        RecordEventType::PayloadEvicted
    }
}

impl RecordEventPayload for RetentionClassSetPayload {
    fn event_type() -> RecordEventType {
        RecordEventType::RetentionClassSet
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

/// Read all events for a specific record ID from a JSONL file.
///
/// Skips empty or malformed lines. Returns events in append order.
pub fn read_events_for_record(path: &Path, record_id: &str) -> Result<Vec<RecordEvent>> {
    let all = read_all_events(path)?;
    Ok(all.into_iter().filter(|e| e.record_id == record_id).collect())
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
        let cr = ContentRefPayload::new(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
            0,
            None,
        );
        let json = serde_json::to_string(&cr).unwrap();
        // media_type should be absent when None (skip_serializing_if)
        assert!(!json.contains("media_type"));
    }

    // -- Integration tests --

    #[test]
    fn test_append_creates_parent_dir() {
        let dir = TempDir::new().unwrap();
        // Nested path whose intermediate directory does not yet exist.
        let path = dir.path().join("nested").join("dir").join("events.jsonl");
        assert!(!path.parent().unwrap().exists());

        let ev = sample_created_event("r1");
        append_event(&path, &ev).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_multiple_events_order_preserved() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        let ids = ["r1", "r2", "r3", "r4", "r5"];
        for id in &ids {
            append_event(&path, &sample_created_event(id)).unwrap();
        }

        let events = read_all_events(&path).unwrap();
        assert_eq!(events.len(), ids.len());
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(events[i].record_id, *id);
        }
    }

    #[test]
    fn test_empty_log_handling() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        // File does not exist yet — should return empty vec.
        let events = read_all_events(&path).unwrap();
        assert!(events.is_empty());

        // Create an empty file — should still return empty vec.
        std::fs::File::create(&path).unwrap();
        let events = read_all_events(&path).unwrap();
        assert!(events.is_empty());
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
        append_event(&path, &sample_created_event("r3")).unwrap();

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

        // Non-existent file → 0
        assert_eq!(count_events(&path).unwrap(), 0);

        for i in 0..5u8 {
            append_event(&path, &sample_created_event(&format!("r{i}"))).unwrap();
        }
        assert_eq!(count_events(&path).unwrap(), 5);
    }

    #[test]
    fn test_count_events_skips_malformed() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        append_event(&path, &sample_created_event("r1")).unwrap();

        // Inject a corrupt line.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "{{corrupt}}").unwrap();
        drop(file);

        append_event(&path, &sample_created_event("r2")).unwrap();

        // Corrupt line is skipped, so count should be 2.
        assert_eq!(count_events(&path).unwrap(), 2);
    }

    #[test]
    fn test_all_event_types_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        // RecordCreated
        append_event(&path, &sample_created_event("r1")).unwrap();

        // RecordUpdated
        append_event(
            &path,
            &RecordEvent::from_payload(
                "r1",
                "agent",
                RecordUpdatedPayload {
                    title: Some("Updated".to_string()),
                    description: None,
                },
            ),
        )
        .unwrap();

        // RecordArchived
        append_event(
            &path,
            &RecordEvent::from_payload(
                "r1",
                "agent",
                RecordArchivedPayload {
                    reason: Some("superseded".to_string()),
                },
            ),
        )
        .unwrap();

        // TagAdded / TagRemoved
        for et in [RecordEventType::TagAdded, RecordEventType::TagRemoved] {
            append_event(
                &path,
                &RecordEvent::new("r1", "agent", et, &TagPayload { tag: "t".to_string() }),
            )
            .unwrap();
        }

        // LinkAdded / LinkRemoved
        for et in [RecordEventType::LinkAdded, RecordEventType::LinkRemoved] {
            append_event(
                &path,
                &RecordEvent::new(
                    "r1",
                    "agent",
                    et,
                    &LinkPayload {
                        task_id: Some("BRN-01XXX".to_string()),
                        chunk_id: None,
                    },
                ),
            )
            .unwrap();
        }

        let events = read_all_events(&path).unwrap();
        assert_eq!(events.len(), 7);

        let types: Vec<_> = events.iter().map(|e| &e.event_type).collect();
        assert_eq!(types[0], &RecordEventType::RecordCreated);
        assert_eq!(types[1], &RecordEventType::RecordUpdated);
        assert_eq!(types[2], &RecordEventType::RecordArchived);
        assert_eq!(types[3], &RecordEventType::TagAdded);
        assert_eq!(types[4], &RecordEventType::TagRemoved);
        assert_eq!(types[5], &RecordEventType::LinkAdded);
        assert_eq!(types[6], &RecordEventType::LinkRemoved);
    }

    #[test]
    fn test_concurrent_appends_no_interleaving() {
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let path = Arc::new(dir.path().join("events.jsonl"));

        let thread_count = 8usize;
        let events_per_thread = 10usize;

        let handles: Vec<_> = (0..thread_count)
            .map(|t| {
                let p = Arc::clone(&path);
                thread::spawn(move || {
                    for i in 0..events_per_thread {
                        let id = format!("t{t}-r{i}");
                        append_event(&p, &sample_created_event(&id)).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let events = read_all_events(&path).unwrap();
        // All events must be present — no data loss from concurrent appends.
        assert_eq!(events.len(), thread_count * events_per_thread);

        // Every line must deserialize cleanly (no interleaved JSON corruption).
        for ev in &events {
            assert!(!ev.record_id.is_empty());
        }
    }

    #[test]
    fn test_payload_evicted_serde_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        let payload = PayloadEvictedPayload {
            content_hash: "deadbeef".to_string(),
            reason: "gc".to_string(),
        };
        let ev = RecordEvent::from_payload("r1", "gc-agent", payload);
        assert_eq!(ev.event_type, RecordEventType::PayloadEvicted);

        append_event(&path, &ev).unwrap();
        let events = read_all_events(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, RecordEventType::PayloadEvicted);

        let p: PayloadEvictedPayload = serde_json::from_value(events[0].payload.clone()).unwrap();
        assert_eq!(p.content_hash, "deadbeef");
        assert_eq!(p.reason, "gc");
    }

    #[test]
    fn test_retention_class_set_serde_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        let payload = RetentionClassSetPayload {
            retention_class: Some("permanent".to_string()),
        };
        let ev = RecordEvent::from_payload("r1", "agent", payload);
        assert_eq!(ev.event_type, RecordEventType::RetentionClassSet);

        append_event(&path, &ev).unwrap();
        let events = read_all_events(&path).unwrap();
        let p: RetentionClassSetPayload =
            serde_json::from_value(events[0].payload.clone()).unwrap();
        assert_eq!(p.retention_class.as_deref(), Some("permanent"));
    }

    #[test]
    fn test_retention_class_set_null_serde() {
        let payload = RetentionClassSetPayload {
            retention_class: None,
        };
        let ev = RecordEvent::from_payload("r1", "agent", payload);
        let p: RetentionClassSetPayload = serde_json::from_value(ev.payload).unwrap();
        assert!(p.retention_class.is_none());
    }

    #[test]
    fn test_pin_events_serde_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");

        for event_type in [RecordEventType::RecordPinned, RecordEventType::RecordUnpinned] {
            append_event(
                &path,
                &RecordEvent::new("r1", "agent", event_type.clone(), &PinPayload {}),
            )
            .unwrap();
        }

        let events = read_all_events(&path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, RecordEventType::RecordPinned);
        assert_eq!(events[1].event_type, RecordEventType::RecordUnpinned);
    }

    #[test]
    fn test_new_event_type_serialization_format() {
        let ev = RecordEvent::from_payload(
            "r1",
            "agent",
            PayloadEvictedPayload {
                content_hash: "abc".to_string(),
                reason: "test".to_string(),
            },
        );
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"event_type\":\"payload_evicted\""));

        let ev2 = RecordEvent::from_payload(
            "r1",
            "agent",
            RetentionClassSetPayload {
                retention_class: None,
            },
        );
        let json2 = serde_json::to_string(&ev2).unwrap();
        assert!(json2.contains("\"event_type\":\"retention_class_set\""));

        let ev3 = RecordEvent::new("r1", "agent", RecordEventType::RecordPinned, &PinPayload {});
        let json3 = serde_json::to_string(&ev3).unwrap();
        assert!(json3.contains("\"event_type\":\"record_pinned\""));

        let ev4 =
            RecordEvent::new("r1", "agent", RecordEventType::RecordUnpinned, &PinPayload {});
        let json4 = serde_json::to_string(&ev4).unwrap();
        assert!(json4.contains("\"event_type\":\"record_unpinned\""));
    }
}
