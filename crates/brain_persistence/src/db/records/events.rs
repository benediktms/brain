use serde::{Deserialize, Serialize};
use ulid::Ulid;

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
