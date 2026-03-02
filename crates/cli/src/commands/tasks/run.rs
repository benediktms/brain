use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::json;

use brain_lib::db::Db;
use brain_lib::tasks::TaskStore;
use brain_lib::tasks::events::*;
use brain_lib::utils::{task_row_to_json, ts_to_iso};

fn open_store(sqlite_db: &Path) -> Result<TaskStore> {
    let db = Db::open(sqlite_db).context("Failed to open SQLite database")?;
    let tasks_dir = sqlite_db
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("tasks");
    TaskStore::new(&tasks_dir, db).context("Failed to open task store")
}

fn format_ts(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn format_ts_short(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn priority_label(p: i32) -> &'static str {
    match p {
        0 => "P0",
        1 => "P1",
        2 => "P2",
        3 => "P3",
        4 => "P4",
        _ => "P?",
    }
}

// ── create ──────────────────────────────────────────────────

pub fn create(
    sqlite_db: &Path,
    title: String,
    description: Option<String>,
    priority: i32,
    task_type: String,
    assignee: Option<String>,
    parent: Option<String>,
    json_output: bool,
) -> Result<()> {
    let store = open_store(sqlite_db)?;
    let task_id = new_event_id();

    let event = TaskEvent::from_payload(
        &task_id,
        "cli",
        TaskCreatedPayload {
            title: title.clone(),
            description: description.clone(),
            priority,
            status: TaskStatus::Open,
            due_ts: None,
            task_type: Some(task_type.clone()),
            assignee: assignee.clone(),
            defer_until: None,
            parent_task_id: parent.clone(),
        },
    );

    store.append(&event)?;

    if json_output {
        let task = store.get_task(&task_id)?.unwrap();
        let labels = store.get_task_labels(&task_id)?;
        let out = json!({
            "event_id": event.event_id,
            "task": task_row_to_json(&task, labels),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Created task {task_id}");
        println!("  Title: {title}");
        println!("  Priority: {}", priority_label(priority));
        println!("  Type: {task_type}");
        if let Some(ref a) = assignee {
            println!("  Assignee: {a}");
        }
        if let Some(ref p) = parent {
            println!("  Parent: {p}");
        }
    }

    Ok(())
}

// ── list ────────────────────────────────────────────────────

pub fn list(
    sqlite_db: &Path,
    status: Option<String>,
    priority: Option<i32>,
    task_type: Option<String>,
    assignee: Option<String>,
    ready: bool,
    blocked: bool,
    json_output: bool,
) -> Result<()> {
    if ready && blocked {
        bail!("--ready and --blocked are mutually exclusive");
    }

    let store = open_store(sqlite_db)?;

    let tasks = if ready {
        store.list_ready()?
    } else if blocked {
        store.list_blocked()?
    } else {
        store.list_all()?
    };

    // Apply client-side filters
    let tasks: Vec<_> = tasks
        .into_iter()
        .filter(|t| {
            if let Some(ref s) = status {
                if t.status != *s {
                    return false;
                }
            }
            if let Some(p) = priority {
                if t.priority != p {
                    return false;
                }
            }
            if let Some(ref tt) = task_type {
                if t.task_type != *tt {
                    return false;
                }
            }
            if let Some(ref a) = assignee {
                if t.assignee.as_deref() != Some(a.as_str()) {
                    return false;
                }
            }
            true
        })
        .collect();

    if json_output {
        let items: Vec<_> = tasks
            .iter()
            .map(|t| {
                let labels = store.get_task_labels(&t.task_id).unwrap_or_default();
                task_row_to_json(t, labels)
            })
            .collect();
        let (ready_count, blocked_count) = store.count_ready_blocked()?;
        let out = json!({
            "tasks": items,
            "count": tasks.len(),
            "ready_count": ready_count,
            "blocked_count": blocked_count,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if tasks.is_empty() {
            println!("No tasks found.");
            return Ok(());
        }

        println!(
            "{:<4} {:<12} {:<6} {:<10} {:<38} {}",
            "PRI", "STATUS", "TYPE", "ASSIGNEE", "ID", "TITLE"
        );
        println!("{}", "\u{2500}".repeat(100));

        for t in &tasks {
            println!(
                "{:<4} {:<12} {:<6} {:<10} {:<38} {}",
                priority_label(t.priority),
                t.status,
                &t.task_type,
                t.assignee.as_deref().unwrap_or("-"),
                &t.task_id,
                t.title,
            );
        }

        let (ready_count, blocked_count) = store.count_ready_blocked()?;
        println!(
            "\n{} task(s) shown ({ready_count} ready, {blocked_count} blocked)",
            tasks.len()
        );
    }

    Ok(())
}

// ── show ────────────────────────────────────────────────────

pub fn show(sqlite_db: &Path, id: &str, json_output: bool) -> Result<()> {
    let store = open_store(sqlite_db)?;

    let task = store
        .get_task(id)?
        .ok_or_else(|| anyhow::anyhow!("task not found: {id}"))?;

    let labels = store.get_task_labels(id)?;
    let comments = store.get_task_comments(id)?;
    let dep_summary = store.get_dependency_summary(id)?;
    let note_links = store.get_task_note_links(id)?;
    let children = store.get_children(id)?;

    if json_output {
        let comments_json: Vec<_> = comments
            .iter()
            .map(|c| {
                json!({
                    "comment_id": c.comment_id,
                    "author": c.author,
                    "body": c.body,
                    "created_at": ts_to_iso(c.created_at),
                })
            })
            .collect();

        let note_links_json: Vec<_> = note_links
            .iter()
            .map(|l| json!({"chunk_id": l.chunk_id, "file_path": l.file_path}))
            .collect();

        let children_json: Vec<_> = children
            .iter()
            .map(|c| {
                json!({
                    "task_id": c.task_id,
                    "title": c.title,
                    "status": c.status,
                    "priority": c.priority,
                })
            })
            .collect();

        let out = json!({
            "task": task_row_to_json(&task, labels),
            "dependency_summary": {
                "total_deps": dep_summary.total_deps,
                "done_deps": dep_summary.done_deps,
                "blocking_task_ids": dep_summary.blocking_task_ids,
            },
            "linked_notes": note_links_json,
            "comments": comments_json,
            "children": children_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Task: {}", task.task_id);
        println!("Title: {}", task.title);
        println!("Status: {}", task.status);
        println!("Priority: {}", priority_label(task.priority));
        println!("Type: {}", task.task_type);
        println!("Assignee: {}", task.assignee.as_deref().unwrap_or("-"));
        if let Some(ref parent) = task.parent_task_id {
            println!("Parent: {parent}");
        }
        println!("Created: {}", format_ts(task.created_at));
        println!("Updated: {}", format_ts(task.updated_at));
        if let Some(due) = task.due_ts {
            println!("Due: {}", format_ts(due));
        }
        if let Some(defer) = task.defer_until {
            println!("Deferred until: {}", format_ts(defer));
        }
        if let Some(ref reason) = task.blocked_reason {
            println!("Blocked: {reason}");
        }

        if let Some(ref desc) = task.description {
            println!("\nDescription:\n  {}", desc.replace('\n', "\n  "));
        }

        if !labels.is_empty() {
            println!("\nLabels: {}", labels.join(", "));
        }

        if dep_summary.total_deps > 0 {
            println!(
                "\nDependencies: ({}/{} done)",
                dep_summary.done_deps, dep_summary.total_deps
            );
            for dep_id in &dep_summary.blocking_task_ids {
                let dep_task = store.get_task(dep_id)?;
                let title = dep_task
                    .as_ref()
                    .map(|t| t.title.as_str())
                    .unwrap_or("(unknown)");
                println!("  [blocking] {dep_id}  {title}");
            }
        }

        if !note_links.is_empty() {
            println!("\nLinked Notes:");
            for link in &note_links {
                let path = if link.file_path.is_empty() {
                    "(unknown)"
                } else {
                    &link.file_path
                };
                println!("  {}  {path}", link.chunk_id);
            }
        }

        if !children.is_empty() {
            println!("\nChildren:");
            for child in &children {
                println!(
                    "  [{}] {} {}  {}",
                    child.status,
                    priority_label(child.priority),
                    child.task_id,
                    child.title
                );
            }
        }

        if !comments.is_empty() {
            println!("\nComments ({}):", comments.len());
            for c in &comments {
                println!(
                    "  [{}] {}: {}",
                    format_ts_short(c.created_at),
                    c.author,
                    c.body
                );
            }
        }
    }

    Ok(())
}

// ── update ──────────────────────────────────────────────────

pub fn update(
    sqlite_db: &Path,
    id: &str,
    title: Option<String>,
    description: Option<String>,
    status: Option<String>,
    priority: Option<i32>,
    task_type: Option<String>,
    assignee: Option<String>,
    blocked_reason: Option<String>,
    json_output: bool,
) -> Result<()> {
    let store = open_store(sqlite_db)?;

    let has_status = status.is_some();
    let has_field_updates = title.is_some()
        || description.is_some()
        || priority.is_some()
        || task_type.is_some()
        || assignee.is_some()
        || blocked_reason.is_some();

    if !has_status && !has_field_updates {
        bail!("no updates specified");
    }

    // Status change is a separate event type
    if let Some(ref s) = status {
        let new_status: TaskStatus = s.parse().map_err(|e: String| anyhow::anyhow!(e))?;
        let event = TaskEvent::from_payload(id, "cli", StatusChangedPayload { new_status });
        store.append(&event)?;
    }

    if has_field_updates {
        let event = TaskEvent::from_payload(
            id,
            "cli",
            TaskUpdatedPayload {
                title,
                description,
                priority,
                due_ts: None,
                blocked_reason,
                task_type,
                assignee,
                defer_until: None,
            },
        );
        store.append(&event)?;
    }

    let task = store
        .get_task(id)?
        .ok_or_else(|| anyhow::anyhow!("task not found after update: {id}"))?;

    if json_output {
        let labels = store.get_task_labels(id)?;
        let out = json!({ "task": task_row_to_json(&task, labels) });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Updated task {id}");
        println!("  Title: {}", task.title);
        println!("  Status: {}", task.status);
        println!("  Priority: {}", priority_label(task.priority));
    }

    Ok(())
}

// ── dep add / dep remove ────────────────────────────────────

pub fn dep_add(sqlite_db: &Path, task_id: &str, depends_on: &str, json_output: bool) -> Result<()> {
    let store = open_store(sqlite_db)?;
    let event = TaskEvent::new(
        task_id,
        "cli",
        EventType::DependencyAdded,
        &DependencyPayload {
            depends_on_task_id: depends_on.to_string(),
        },
    );
    store.append(&event)?;

    if json_output {
        let out = json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "depends_on": depends_on,
            "action": "added",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added dependency: {task_id} depends on {depends_on}");
    }

    Ok(())
}

pub fn dep_remove(
    sqlite_db: &Path,
    task_id: &str,
    depends_on: &str,
    json_output: bool,
) -> Result<()> {
    let store = open_store(sqlite_db)?;
    let event = TaskEvent::new(
        task_id,
        "cli",
        EventType::DependencyRemoved,
        &DependencyPayload {
            depends_on_task_id: depends_on.to_string(),
        },
    );
    store.append(&event)?;

    if json_output {
        let out = json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "depends_on": depends_on,
            "action": "removed",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Removed dependency: {task_id} no longer depends on {depends_on}");
    }

    Ok(())
}

// ── link / unlink ───────────────────────────────────────────

pub fn link(sqlite_db: &Path, task_id: &str, chunk_id: &str, json_output: bool) -> Result<()> {
    let store = open_store(sqlite_db)?;
    let event = TaskEvent::new(
        task_id,
        "cli",
        EventType::NoteLinked,
        &NoteLinkPayload {
            chunk_id: chunk_id.to_string(),
        },
    );
    store.append(&event)?;

    if json_output {
        let out = json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "chunk_id": chunk_id,
            "action": "linked",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Linked note {chunk_id} to task {task_id}");
    }

    Ok(())
}

pub fn unlink(sqlite_db: &Path, task_id: &str, chunk_id: &str, json_output: bool) -> Result<()> {
    let store = open_store(sqlite_db)?;
    let event = TaskEvent::new(
        task_id,
        "cli",
        EventType::NoteUnlinked,
        &NoteLinkPayload {
            chunk_id: chunk_id.to_string(),
        },
    );
    store.append(&event)?;

    if json_output {
        let out = json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "chunk_id": chunk_id,
            "action": "unlinked",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Unlinked note {chunk_id} from task {task_id}");
    }

    Ok(())
}

// ── comment ─────────────────────────────────────────────────

pub fn comment(sqlite_db: &Path, task_id: &str, body: &str, json_output: bool) -> Result<()> {
    let store = open_store(sqlite_db)?;
    let event = TaskEvent::from_payload(
        task_id,
        "cli",
        CommentPayload {
            body: body.to_string(),
        },
    );
    store.append(&event)?;

    if json_output {
        let out = json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "body": body,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added comment to task {task_id}");
    }

    Ok(())
}

// ── label add / label remove ────────────────────────────────

pub fn label_add(sqlite_db: &Path, task_id: &str, label: &str, json_output: bool) -> Result<()> {
    let store = open_store(sqlite_db)?;
    let event = TaskEvent::new(
        task_id,
        "cli",
        EventType::LabelAdded,
        &LabelPayload {
            label: label.to_string(),
        },
    );
    store.append(&event)?;

    if json_output {
        let out = json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "label": label,
            "action": "added",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added label \"{label}\" to task {task_id}");
    }

    Ok(())
}

pub fn label_remove(sqlite_db: &Path, task_id: &str, label: &str, json_output: bool) -> Result<()> {
    let store = open_store(sqlite_db)?;
    let event = TaskEvent::new(
        task_id,
        "cli",
        EventType::LabelRemoved,
        &LabelPayload {
            label: label.to_string(),
        },
    );
    store.append(&event)?;

    if json_output {
        let out = json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "label": label,
            "action": "removed",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Removed label \"{label}\" from task {task_id}");
    }

    Ok(())
}
