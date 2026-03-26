//! Shared task enrichment: JSON builders for comments, notes, deps, children.
//!
//! Eliminates duplication between MCP handlers and CLI commands.

use std::collections::HashMap;

use serde_json::{Value, json};

use super::TaskStore;
use super::queries::{DependencySummary, TaskComment, TaskNoteLink, TaskRow};
use crate::utils::{ts_to_iso, ts_to_json};

/// Serialize a `TaskRow` and its labels into a JSON object with ISO timestamps.
pub fn task_row_to_json(row: &TaskRow, labels: Vec<String>) -> Value {
    json!({
        "task_id": row.task_id,
        "title": row.title,
        "description": row.description,
        "status": row.status,
        "priority": row.priority,
        "blocked_reason": row.blocked_reason,
        "due_ts": ts_to_json(row.due_ts),
        "task_type": row.task_type.as_str(),
        "assignee": row.assignee,
        "defer_until": ts_to_json(row.defer_until),
        "parent_task_id": row.parent_task_id,
        "child_seq": row.child_seq,
        "id": row.display_id,
        "labels": labels,
        "created_at": ts_to_json(Some(row.created_at)),
        "updated_at": ts_to_json(Some(row.updated_at)),
    })
}

/// Serialize a `TaskRow` to JSON with compact `parent_task_id`.
///
/// Combines `task_row_to_json` + `apply_compact_parent_id` into one call.
/// Prefer this over the two-step pattern whenever a `TaskStore` is available.
pub fn task_row_to_compact_json(store: &TaskStore, row: &TaskRow, labels: Vec<String>) -> Value {
    let mut json = task_row_to_json(row, labels);
    apply_compact_task_id(store, &mut json);
    apply_compact_parent_id(store, &mut json);
    json
}

/// Convert the `task_id` field of a serialized task to its compact form.
pub fn apply_compact_task_id(store: &TaskStore, task_json: &mut Value) {
    let Some(obj) = task_json.as_object_mut() else {
        return;
    };
    let Some(task_id) = obj.get("task_id").and_then(|v| v.as_str()) else {
        return;
    };
    if task_id.is_empty() {
        return;
    }
    let compact = store
        .compact_id(task_id)
        .unwrap_or_else(|_| task_id.to_string());
    obj.insert("task_id".into(), json!(compact));
}

/// Convert the `parent_task_id` field of a serialized task to its compact form, if present.
pub fn apply_compact_parent_id(store: &TaskStore, task_json: &mut Value) {
    let Some(obj) = task_json.as_object_mut() else {
        return;
    };
    let Some(parent_val) = obj.get("parent_task_id") else {
        return;
    };
    let Some(parent_id) = parent_val.as_str() else {
        return;
    };
    if parent_id.is_empty() {
        return;
    }
    let compact = store
        .compact_id(parent_id)
        .unwrap_or_else(|_| parent_id.to_string());
    obj.insert("parent_task_id".into(), json!(compact));
}

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
pub fn dep_summary_to_json_with_blocking(store: &TaskStore, summary: &DependencySummary) -> Value {
    let compact_blocking: Vec<String> = summary
        .blocking_task_ids
        .iter()
        .map(|id| store.compact_id(id).unwrap_or_else(|_| id.clone()))
        .collect();
    json!({
        "total_deps": summary.total_deps,
        "done_deps": summary.done_deps,
        "blocking_task_ids": compact_blocking,
    })
}

/// Convert child task rows to compact stubs (task_id, title, status, priority).
pub fn children_stubs_to_json(store: &TaskStore, children: &[TaskRow]) -> Vec<Value> {
    children
        .iter()
        .map(|c| {
            let short = store
                .compact_id(&c.task_id)
                .unwrap_or_else(|_| c.task_id.clone());
            json!({
                "task_id": short,
                "title": c.title,
                "status": c.status,
                "priority": c.priority,
            })
        })
        .collect()
}

/// Attach `dependency_summary` and `linked_notes` fields onto an existing task JSON object.
///
/// Shared by `enrich_task_summary` and `enrich_task_summaries` to avoid duplicating
/// the JSON construction logic.
fn attach_summary_fields(
    store: &TaskStore,
    task_json: &mut Value,
    dep_summary: &DependencySummary,
    note_links: &[TaskNoteLink],
) {
    if let Some(obj) = task_json.as_object_mut() {
        obj.insert(
            "dependency_summary".into(),
            dep_summary_to_json_with_blocking(store, dep_summary),
        );
        obj.insert("linked_notes".into(), json!(note_links_to_json(note_links)));
    }
}

/// Enrich a single task with dependency_summary and linked_notes (used by task_next).
pub fn enrich_task_summary(store: &TaskStore, task: &TaskRow) -> Value {
    let dep_summary = match store.get_dependency_summary(&task.task_id) {
        Ok(dep_summary) => dep_summary,
        Err(err) => {
            tracing::warn!(
                "Failed to get_dependency_summary for {}: {}",
                task.task_id,
                err
            );
            DependencySummary {
                total_deps: 0,
                done_deps: 0,
                blocking_task_ids: vec![],
            }
        }
    };
    let note_links = match store.get_task_note_links(&task.task_id) {
        Ok(note_links) => note_links,
        Err(err) => {
            tracing::warn!(
                "Failed to get_task_note_links for {}: {}",
                task.task_id,
                err
            );
            Vec::default()
        }
    };
    let labels = match store.get_task_labels(&task.task_id) {
        Ok(labels) => labels,
        Err(err) => {
            tracing::warn!("Failed to get_task_labels for {}: {}", task.task_id, err);
            Vec::default()
        }
    };

    let mut task_json = task_row_to_compact_json(store, task, labels);
    attach_summary_fields(store, &mut task_json, &dep_summary, &note_links);
    task_json
}

/// Enrich a list of tasks with pre-fetched labels; also return ready/blocked aggregate counts.
///
/// Returns `(tasks_json, ready_count, blocked_count)`.
pub fn enrich_task_list(
    store: &TaskStore,
    tasks: &[TaskRow],
    labels_map: &HashMap<String, Vec<String>>,
) -> (Vec<Value>, usize, usize) {
    let tasks_json: Vec<Value> = tasks
        .iter()
        .map(|task| {
            let labels = labels_map.get(&task.task_id).cloned().unwrap_or_default();
            task_row_to_compact_json(store, task, labels)
        })
        .collect();
    let (ready_count, blocked_count) = match store.count_ready_blocked() {
        Ok(counts) => counts,
        Err(err) => {
            tracing::warn!("Failed to count_ready_blocked: {}", err);
            (0, 0)
        }
    };
    (tasks_json, ready_count, blocked_count)
}

/// Batch-enrich a list of tasks with labels, dependency summary, and note links.
///
/// Used by `task_next` for the selected top-k tasks. Batch-fetches labels in one
/// query instead of N per-task queries.
pub fn enrich_task_summaries(store: &TaskStore, tasks: &[TaskRow]) -> Vec<Value> {
    let task_ids: Vec<&str> = tasks.iter().map(|t| t.task_id.as_str()).collect();
    let labels_map = match store.get_labels_for_tasks(&task_ids) {
        Ok(labels_map) => labels_map,
        Err(err) => {
            tracing::warn!("Failed to get_labels_for_tasks: {}", err);
            HashMap::default()
        }
    };

    tasks
        .iter()
        .map(|task| {
            let dep_summary = match store.get_dependency_summary(&task.task_id) {
                Ok(dep_summary) => dep_summary,
                Err(err) => {
                    tracing::warn!(
                        "Failed to get_dependency_summary for {}: {}",
                        task.task_id,
                        err
                    );
                    DependencySummary {
                        total_deps: 0,
                        done_deps: 0,
                        blocking_task_ids: vec![],
                    }
                }
            };
            let note_links = match store.get_task_note_links(&task.task_id) {
                Ok(note_links) => note_links,
                Err(err) => {
                    tracing::warn!(
                        "Failed to get_task_note_links for {}: {}",
                        task.task_id,
                        err
                    );
                    Vec::default()
                }
            };
            let labels = labels_map.get(&task.task_id).cloned().unwrap_or_default();

            let mut task_json = task_row_to_compact_json(store, task, labels);
            attach_summary_fields(store, &mut task_json, &dep_summary, &note_links);
            task_json
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tasks::events::TaskType;

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
        use crate::db::Db;
        let db = Db::open_in_memory().unwrap();
        let store = TaskStore::new(db);
        let summary = make_dep_summary(3, 2, vec!["t1".to_string()]);
        let result = dep_summary_to_json_with_blocking(&store, &summary);
        assert_eq!(result["total_deps"], 3);
        assert_eq!(result["done_deps"], 2);
        let blocking = result["blocking_task_ids"].as_array().unwrap();
        assert_eq!(blocking.len(), 1);
        assert_eq!(blocking[0], "t1");
    }

    #[test]
    fn test_children_stubs_to_json_shape() {
        use crate::db::Db;
        use crate::tasks::queries::TaskRow;
        let db = Db::open_in_memory().unwrap();
        let store = TaskStore::new(db);
        let child = TaskRow {
            task_id: "c1".to_string(),
            title: "Child Task".to_string(),
            description: None,
            status: "open".to_string(),
            priority: 2,
            blocked_reason: None,
            due_ts: None,
            task_type: TaskType::Task,
            assignee: None,
            defer_until: None,
            parent_task_id: None,
            child_seq: None,
            created_at: 0,
            updated_at: 0,
            display_id: None,
        };
        let result = children_stubs_to_json(&store, &[child]);
        assert_eq!(result.len(), 1);
        // compact_id falls back to raw ID when task not in DB
        assert_eq!(result[0]["task_id"], "c1");
        assert_eq!(result[0]["title"], "Child Task");
        assert_eq!(result[0]["status"], "open");
        assert_eq!(result[0]["priority"], 2);
        assert!(result[0].get("description").is_none());
    }

    #[test]
    fn test_attach_summary_fields_key_names() {
        use crate::db::Db;
        use crate::tasks::queries::TaskRow;
        let row = TaskRow {
            task_id: "t1".to_string(),
            title: "Test".to_string(),
            description: None,
            status: "open".to_string(),
            priority: 1,
            blocked_reason: None,
            due_ts: None,
            task_type: TaskType::Task,
            assignee: None,
            defer_until: None,
            parent_task_id: None,
            child_seq: None,
            created_at: 0,
            updated_at: 0,
            display_id: None,
        };
        let db = Db::open_in_memory().unwrap();
        let store = TaskStore::new(db);
        let dep = make_dep_summary(2, 1, vec!["blocker".to_string()]);
        let links = vec![make_note_link("c1", "/file.md")];

        let mut json = task_row_to_json(&row, vec![]);
        attach_summary_fields(&store, &mut json, &dep, &links);

        // Verify key names are consistent
        let ds = &json["dependency_summary"];
        assert_eq!(ds["total_deps"], 2);
        assert_eq!(ds["done_deps"], 1);
        assert_eq!(ds["blocking_task_ids"][0], "blocker");

        let ln = json["linked_notes"].as_array().unwrap();
        assert_eq!(ln.len(), 1);
        assert_eq!(ln[0]["chunk_id"], "c1");
    }
}
