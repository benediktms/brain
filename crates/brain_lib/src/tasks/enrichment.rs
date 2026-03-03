//! Shared task enrichment: JSON builders for comments, notes, deps, children.
//!
//! Eliminates duplication between MCP handlers and CLI commands.

use serde_json::{Value, json};

use super::TaskStore;
use super::queries::{DependencySummary, TaskComment, TaskNoteLink, TaskRow};
use crate::utils::{task_row_to_json, ts_to_iso};

/// Convert a slice of comments to a JSON array.
pub fn comments_to_json(comments: &[TaskComment]) -> Vec<Value> {
    comments
        .iter()
        .map(|c| {
            json!({
                "comment_id": c.comment_id,
                "author": c.author,
                "body": c.body,
                "created_at": ts_to_iso(c.created_at),
            })
        })
        .collect()
}

/// Convert a slice of note links to a JSON array.
pub fn note_links_to_json(links: &[TaskNoteLink]) -> Vec<Value> {
    links
        .iter()
        .map(|nl| {
            json!({
                "chunk_id": nl.chunk_id,
                "file_path": nl.file_path,
            })
        })
        .collect()
}

/// Convert a dependency summary to JSON (total_deps + done_deps only).
pub fn dep_summary_to_json(summary: &DependencySummary) -> Value {
    json!({
        "total_deps": summary.total_deps,
        "done_deps": summary.done_deps,
    })
}

/// Convert a dependency summary to JSON including the blocking_task_ids list.
pub fn dep_summary_to_json_with_blocking(summary: &DependencySummary) -> Value {
    json!({
        "total_deps": summary.total_deps,
        "done_deps": summary.done_deps,
        "blocking_task_ids": summary.blocking_task_ids,
    })
}

/// Convert child task rows to compact stubs (task_id, title, status, priority).
pub fn children_stubs_to_json(children: &[TaskRow]) -> Vec<Value> {
    children
        .iter()
        .map(|c| {
            json!({
                "task_id": c.task_id,
                "title": c.title,
                "status": c.status,
                "priority": c.priority,
            })
        })
        .collect()
}

/// Enrich a single task with dependency_summary and linked_notes (used by task_next).
pub fn enrich_task_summary(store: &TaskStore, task: &TaskRow) -> Value {
    let dep_summary = store
        .get_dependency_summary(&task.task_id)
        .unwrap_or_else(|_| DependencySummary {
            total_deps: 0,
            done_deps: 0,
            blocking_task_ids: vec![],
        });
    let note_links = store.get_task_note_links(&task.task_id).unwrap_or_default();
    let labels = store.get_task_labels(&task.task_id).unwrap_or_default();

    let mut task_json = task_row_to_json(task, labels);
    if let Some(obj) = task_json.as_object_mut() {
        obj.insert(
            "dependency_summary".into(),
            json!({
                "total_deps": dep_summary.total_deps,
                "done_deps": dep_summary.done_deps,
                "blocking_tasks": dep_summary.blocking_task_ids,
            }),
        );
        obj.insert(
            "linked_notes".into(),
            json!(note_links_to_json(&note_links)),
        );
    }
    task_json
}

/// Enrich a list of tasks with labels; also return ready/blocked aggregate counts.
///
/// Returns `(tasks_json, ready_count, blocked_count)`.
pub fn enrich_task_list(store: &TaskStore, tasks: &[TaskRow]) -> (Vec<Value>, usize, usize) {
    let tasks_json: Vec<Value> = tasks
        .iter()
        .map(|task| {
            let labels = store.get_task_labels(&task.task_id).unwrap_or_default();
            task_row_to_json(task, labels)
        })
        .collect();
    let (ready_count, blocked_count) = store.count_ready_blocked().unwrap_or((0, 0));
    (tasks_json, ready_count, blocked_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_comment(id: &str, author: &str, body: &str, ts: i64) -> TaskComment {
        TaskComment {
            comment_id: id.to_string(),
            author: author.to_string(),
            body: body.to_string(),
            created_at: ts,
        }
    }

    fn make_note_link(chunk_id: &str, file_path: &str) -> TaskNoteLink {
        TaskNoteLink {
            chunk_id: chunk_id.to_string(),
            file_path: file_path.to_string(),
        }
    }

    fn make_dep_summary(total: usize, done: usize, blocking: Vec<String>) -> DependencySummary {
        DependencySummary {
            total_deps: total,
            done_deps: done,
            blocking_task_ids: blocking,
        }
    }

    #[test]
    fn test_comments_to_json_shape() {
        let comments = vec![make_comment("c1", "alice", "hello", 1_700_000_000)];
        let result = comments_to_json(&comments);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["comment_id"], "c1");
        assert_eq!(result[0]["author"], "alice");
        assert_eq!(result[0]["body"], "hello");
        assert!(result[0]["created_at"].is_string());
    }

    #[test]
    fn test_comments_to_json_empty() {
        let result = comments_to_json(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_note_links_to_json_shape() {
        let links = vec![make_note_link("chunk-1", "/path/to/file.md")];
        let result = note_links_to_json(&links);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["chunk_id"], "chunk-1");
        assert_eq!(result[0]["file_path"], "/path/to/file.md");
    }

    #[test]
    fn test_dep_summary_to_json_no_blocking_field() {
        let summary = make_dep_summary(3, 2, vec!["t1".to_string()]);
        let result = dep_summary_to_json(&summary);
        assert_eq!(result["total_deps"], 3);
        assert_eq!(result["done_deps"], 2);
        // blocking_task_ids should NOT be present in the basic variant
        assert!(result.get("blocking_task_ids").is_none());
    }

    #[test]
    fn test_dep_summary_to_json_with_blocking_has_field() {
        let summary = make_dep_summary(3, 2, vec!["t1".to_string()]);
        let result = dep_summary_to_json_with_blocking(&summary);
        assert_eq!(result["total_deps"], 3);
        assert_eq!(result["done_deps"], 2);
        let blocking = result["blocking_task_ids"].as_array().unwrap();
        assert_eq!(blocking.len(), 1);
        assert_eq!(blocking[0], "t1");
    }

    #[test]
    fn test_children_stubs_to_json_shape() {
        use crate::tasks::queries::TaskRow;
        let child = TaskRow {
            task_id: "c1".to_string(),
            title: "Child Task".to_string(),
            description: None,
            status: "open".to_string(),
            priority: 2,
            blocked_reason: None,
            due_ts: None,
            task_type: "task".to_string(),
            assignee: None,
            defer_until: None,
            parent_task_id: None,
            created_at: 0,
            updated_at: 0,
        };
        let result = children_stubs_to_json(&[child]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["task_id"], "c1");
        assert_eq!(result[0]["title"], "Child Task");
        assert_eq!(result[0]["status"], "open");
        assert_eq!(result[0]["priority"], 2);
        // Stubs should NOT have description or other full fields
        assert!(result[0].get("description").is_none());
    }
}
