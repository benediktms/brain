use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

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
    ExternalIdAdded,
    ExternalIdRemoved,
}

/// Valid task statuses.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    #[default]
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

/// Valid task types.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    #[default]
    Task,
    Bug,
    Feature,
    Epic,
    Spike,
}

impl TaskType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskType::Task => "task",
            TaskType::Bug => "bug",
            TaskType::Feature => "feature",
            TaskType::Epic => "epic",
            TaskType::Spike => "spike",
        }
    }
}

impl AsRef<str> for TaskType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl FromStr for TaskType {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "task" => Ok(TaskType::Task),
            "bug" => Ok(TaskType::Bug),
            "feature" => Ok(TaskType::Feature),
            "epic" => Ok(TaskType::Epic),
            "spike" => Ok(TaskType::Spike),
            _ => Err(format!(
                "invalid task type: '{s}'. Valid types: task, bug, feature, epic, spike"
            )),
        }
    }
}

// -- Typed payloads --

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCreatedPayload {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default)]
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_type: Option<TaskType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defer_until: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
}

fn default_priority() -> i32 {
    4
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
    pub task_type: Option<TaskType>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalIdPayload {
    pub source: String,
    pub external_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_url: Option<String>,
}

// -- EventPayload trait --

/// Maps a typed payload to its unambiguous `EventType`.
///
/// Implemented for the 5 payload types that map to exactly one event type.
/// For ambiguous payloads (`DependencyPayload`, `NoteLinkPayload`, `LabelPayload`),
/// use `TaskEvent::new()` with an explicit `event_type` instead.
pub trait EventPayload: Serialize {
    fn event_type() -> EventType;
}

impl EventPayload for TaskCreatedPayload {
    fn event_type() -> EventType {
        EventType::TaskCreated
    }
}

impl EventPayload for TaskUpdatedPayload {
    fn event_type() -> EventType {
        EventType::TaskUpdated
    }
}

impl EventPayload for StatusChangedPayload {
    fn event_type() -> EventType {
        EventType::StatusChanged
    }
}

impl EventPayload for CommentPayload {
    fn event_type() -> EventType {
        EventType::CommentAdded
    }
}

impl EventPayload for ParentSetPayload {
    fn event_type() -> EventType {
        EventType::ParentSet
    }
}

// -- TaskEvent constructors --

impl TaskEvent {
    /// Create from a typed payload that implements `EventPayload` (event_type inferred).
    pub fn from_payload<P: EventPayload>(
        task_id: impl Into<String>,
        actor: impl Into<String>,
        payload: P,
    ) -> Self {
        Self {
            event_id: new_event_id(),
            task_id: task_id.into(),
            timestamp: now_ts(),
            actor: actor.into(),
            event_type: P::event_type(),
            event_version: CURRENT_EVENT_VERSION,
            payload: serde_json::to_value(payload).unwrap(),
        }
    }

    /// Create from any `Serialize` payload with an explicit `event_type`.
    ///
    /// For ambiguous payloads (`DependencyPayload`, `LabelPayload`, `NoteLinkPayload`)
    /// that map to multiple event types.
    pub fn new(
        task_id: impl Into<String>,
        actor: impl Into<String>,
        event_type: EventType,
        payload: &impl Serialize,
    ) -> Self {
        Self {
            event_id: new_event_id(),
            task_id: task_id.into(),
            timestamp: now_ts(),
            actor: actor.into(),
            event_type,
            event_version: CURRENT_EVENT_VERSION,
            payload: serde_json::to_value(payload).unwrap(),
        }
    }

    /// Create from pre-parsed raw JSON with an explicit `event_type`.
    ///
    /// Used by the MCP handler where the payload is already a `serde_json::Value`.
    pub fn from_raw(
        task_id: impl Into<String>,
        actor: impl Into<String>,
        event_type: EventType,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            event_id: new_event_id(),
            task_id: task_id.into(),
            timestamp: now_ts(),
            actor: actor.into(),
            event_type,
            event_version: CURRENT_EVENT_VERSION,
            payload,
        }
    }

    /// Override the auto-generated timestamp.
    ///
    /// Used by `import_beads` to preserve original timestamps.
    pub fn with_timestamp(mut self, ts: i64) -> Self {
        self.timestamp = ts;
        self
    }
}

/// Generate a new ULID event ID.
pub fn new_event_id() -> String {
    Ulid::new().to_string()
}

/// Generate a new task ID with project prefix.
/// Format: "{PREFIX}-{ULID}" e.g. "BRN-01JPHZS7VXQK4R3BGTHNED2P8M"
pub fn new_task_id(prefix: &str) -> String {
    format!("{}-{}", prefix, Ulid::new())
}

/// Current time as unix seconds.
pub fn now_ts() -> i64 {
    crate::utils::now_ts()
}

// NOTE: File I/O functions (append_event, read_all_events) live in
// brain_lib::tasks::events, not here. This module contains only type
// definitions and constructors.

#[cfg(test)]
mod tests {
    use super::*;

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
