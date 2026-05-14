use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use brain_persistence::db::tasks::queries::TaskRow;

use crate::events::{TaskStatus, TaskType};

/// Newtype wrapping the raw ULID-based task identifier (e.g. "BRN-01HXYZ...").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(String);

impl TaskId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for TaskId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for TaskId {
    fn from(s: String) -> Self {
        TaskId(s)
    }
}

impl From<&str> for TaskId {
    fn from(s: &str) -> Self {
        TaskId(s.to_string())
    }
}

/// Priority levels for tasks, mirroring the `0..=4` integer scale used in storage.
/// `Trivial` is the default (i32 4); `Critical` is `0`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Critical,
    High,
    Normal,
    Low,
    #[default]
    Trivial,
}

impl Priority {
    pub fn as_i32(self) -> i32 {
        match self {
            Priority::Critical => 0,
            Priority::High => 1,
            Priority::Normal => 2,
            Priority::Low => 3,
            Priority::Trivial => 4,
        }
    }
}

impl From<i32> for Priority {
    fn from(i: i32) -> Self {
        match i {
            0 => Priority::Critical,
            1 => Priority::High,
            2 => Priority::Normal,
            3 => Priority::Low,
            _ => Priority::Trivial,
        }
    }
}

impl From<Priority> for i32 {
    fn from(p: Priority) -> i32 {
        p.as_i32()
    }
}

/// A task in its domain shape — parsed types instead of raw SQL primitives.
///
/// Converted from `TaskRow` at the persistence boundary via `From<TaskRow>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub title: String,
    pub description: Option<String>,
    pub status: TaskStatus,
    pub priority: Priority,
    pub task_type: TaskType,
    pub assignee: Option<String>,
    pub blocked_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub due_at: Option<DateTime<Utc>>,
    pub defer_until: Option<DateTime<Utc>>,
    pub parent: Option<TaskId>,
    pub child_seq: Option<i64>,
    pub display_id: Option<String>,
}

impl From<TaskRow> for Task {
    fn from(row: TaskRow) -> Self {
        let status: TaskStatus = row.status.parse().unwrap_or(TaskStatus::Open);
        let task_type: TaskType = row.task_type.parse().unwrap_or(TaskType::Task);
        Task {
            id: TaskId::from(row.task_id),
            title: row.title,
            description: row.description,
            status,
            priority: Priority::from(row.priority),
            task_type,
            assignee: row.assignee,
            blocked_reason: row.blocked_reason,
            created_at: Utc.timestamp_opt(row.created_at, 0).unwrap(),
            updated_at: Utc.timestamp_opt(row.updated_at, 0).unwrap(),
            due_at: row.due_ts.and_then(|ts| Utc.timestamp_opt(ts, 0).single()),
            defer_until: row
                .defer_until
                .and_then(|ts| Utc.timestamp_opt(ts, 0).single()),
            parent: row.parent_task_id.map(TaskId::from),
            child_seq: row.child_seq,
            display_id: row.display_id,
        }
    }
}

impl From<&TaskRow> for Task {
    fn from(row: &TaskRow) -> Self {
        Task::from(row.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brain_persistence::db::tasks::queries::TaskRow;

    fn make_task_row(
        task_id: &str,
        status: &str,
        priority: i32,
        task_type: &str,
        created_at: i64,
        updated_at: i64,
    ) -> TaskRow {
        TaskRow {
            task_id: task_id.to_string(),
            title: "Test Task".to_string(),
            description: Some("desc".to_string()),
            status: status.to_string(),
            priority,
            blocked_reason: None,
            due_ts: None,
            task_type: task_type.to_string(),
            assignee: Some("alice".to_string()),
            defer_until: None,
            parent_task_id: Some("parent-1".to_string()),
            child_seq: Some(1),
            created_at,
            updated_at,
            display_id: Some("abc".to_string()),
        }
    }

    #[test]
    fn test_task_id_round_trip() {
        let id = TaskId::from("BRN-01HXYZ");
        assert_eq!(id.as_str(), "BRN-01HXYZ");
        assert_eq!(id.to_string(), "BRN-01HXYZ");

        let id2: TaskId = "BRN-01HXYZ".into();
        assert_eq!(id, id2);
    }

    #[test]
    fn test_priority_round_trip() {
        for (i, expected) in [
            (0, Priority::Critical),
            (1, Priority::High),
            (2, Priority::Normal),
            (3, Priority::Low),
            (4, Priority::Trivial),
        ] {
            let p = Priority::from(i);
            assert_eq!(p, expected);
            assert_eq!(p.as_i32(), i);
        }
        // Out-of-range clamps to Trivial
        assert_eq!(Priority::from(5), Priority::Trivial);
        assert_eq!(Priority::from(-1), Priority::Trivial);
    }

    #[test]
    fn test_from_task_row() {
        let row = make_task_row(
            "task-1",
            "in_progress",
            1,
            "bug",
            1_700_000_000,
            1_700_000_001,
        );
        let task = Task::from(row);

        assert_eq!(task.id.as_str(), "task-1");
        assert_eq!(task.title, "Test Task");
        assert_eq!(task.description, Some("desc".to_string()));
        assert_eq!(task.status, TaskStatus::InProgress);
        assert_eq!(task.priority, Priority::High);
        assert_eq!(task.task_type, TaskType::Bug);
        assert_eq!(task.assignee, Some("alice".to_string()));
        assert_eq!(task.created_at.timestamp(), 1_700_000_000);
        assert_eq!(task.updated_at.timestamp(), 1_700_000_001);
        assert_eq!(task.parent.as_ref().map(|p| p.as_str()), Some("parent-1"));
        assert_eq!(task.child_seq, Some(1));
        assert_eq!(task.display_id, Some("abc".to_string()));
    }

    #[test]
    fn test_from_task_row_invalid_status_defaults_to_open() {
        let row = make_task_row("t1", "unknown_status", 2, "task", 0, 0);
        let task = Task::from(row);
        assert_eq!(task.status, TaskStatus::Open);
    }

    #[test]
    fn test_from_task_row_invalid_type_defaults_to_task() {
        let row = make_task_row("t1", "open", 2, "unknown_type", 0, 0);
        let task = Task::from(row);
        assert_eq!(task.task_type, TaskType::Task);
    }
}
