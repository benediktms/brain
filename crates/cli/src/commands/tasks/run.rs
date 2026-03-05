use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::json;

use brain_lib::db::Db;
use brain_lib::tasks::TaskStore;
use brain_lib::tasks::enrichment::{
    children_stubs_to_json, comments_to_json, dep_summary_to_json_with_blocking, enrich_task_list,
    note_links_to_json,
};
use brain_lib::tasks::events::{self, *};
use brain_lib::utils::task_row_to_json;

use crate::markdown_table::MarkdownTable;

// ── shared context ─────────────────────────────────────────

pub struct TaskCtx {
    store: TaskStore,
    json: bool,
}

impl TaskCtx {
    pub fn new(sqlite_db: &Path, json: bool) -> Result<Self> {
        let db = Db::open(sqlite_db).context("Failed to open SQLite database")?;
        let tasks_dir = sqlite_db
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("tasks");
        let store = TaskStore::new(&tasks_dir, db).context("Failed to open task store")?;
        Ok(Self { store, json })
    }
}

// ── param structs ──────────────────────────────────────────

pub struct CreateParams {
    pub title: String,
    pub description: Option<String>,
    pub priority: i32,
    pub task_type: String,
    pub assignee: Option<String>,
    pub parent: Option<String>,
}

pub struct ListParams {
    pub status: Option<String>,
    pub priority: Option<i32>,
    pub task_type: Option<String>,
    pub assignee: Option<String>,
    pub ready: bool,
    pub blocked: bool,
}

pub struct UpdateParams {
    pub id: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub status: Option<String>,
    pub priority: Option<i32>,
    pub task_type: Option<String>,
    pub assignee: Option<String>,
    pub blocked_reason: Option<String>,
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

pub fn create(ctx: &TaskCtx, params: CreateParams) -> Result<()> {
    let prefix = ctx.store.get_project_prefix()?;
    let task_id = events::new_task_id(&prefix);

    // Resolve parent task ID if provided
    let parent = match params.parent {
        Some(ref p) => Some(ctx.store.resolve_task_id(p)?),
        None => None,
    };

    let event = TaskEvent::from_payload(
        &task_id,
        "cli",
        TaskCreatedPayload {
            title: params.title.clone(),
            description: params.description.clone(),
            priority: params.priority,
            status: TaskStatus::Open,
            due_ts: None,
            task_type: Some(params.task_type.clone()),
            assignee: params.assignee.clone(),
            defer_until: None,
            parent_task_id: parent.clone(),
        },
    );

    ctx.store.append(&event)?;

    if ctx.json {
        let task = ctx.store.get_task(&task_id)?.unwrap();
        let labels = ctx.store.get_task_labels(&task_id)?;
        let out = json!({
            "event_id": event.event_id,
            "task": task_row_to_json(&task, labels),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Created task {task_id}");
        println!("  Title: {}", params.title);
        println!("  Priority: {}", priority_label(params.priority));
        println!("  Type: {}", params.task_type);
        if let Some(ref a) = params.assignee {
            println!("  Assignee: {a}");
        }
        if let Some(ref p) = params.parent {
            println!("  Parent: {p}");
        }
    }

    Ok(())
}

// ── list ────────────────────────────────────────────────────

pub fn list(ctx: &TaskCtx, params: &ListParams) -> Result<()> {
    if params.ready && params.blocked {
        bail!("--ready and --blocked are mutually exclusive");
    }

    let tasks = if params.ready {
        ctx.store.list_ready()?
    } else if params.blocked {
        ctx.store.list_blocked()?
    } else {
        ctx.store.list_all()?
    };

    // Apply client-side filters
    let tasks: Vec<_> = tasks
        .into_iter()
        .filter(|t| {
            if let Some(ref s) = params.status
                && t.status != *s
            {
                return false;
            }
            if let Some(p) = params.priority
                && t.priority != p
            {
                return false;
            }
            if let Some(ref tt) = params.task_type
                && t.task_type != *tt
            {
                return false;
            }
            if let Some(ref a) = params.assignee
                && t.assignee.as_deref() != Some(a.as_str())
            {
                return false;
            }
            true
        })
        .collect();

    if ctx.json {
        let (items, ready_count, blocked_count) = enrich_task_list(&ctx.store, &tasks);
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

        let short_ids = ctx.store.shortest_unique_prefixes()?;

        let mut table =
            MarkdownTable::new(vec!["PRI", "STATUS", "TYPE", "ASSIGNEE", "ID", "TITLE"]);

        for t in &tasks {
            let display_id = if let (Some(parent_id), Some(seq)) = (&t.parent_task_id, t.child_seq)
            {
                let parent_short = short_ids
                    .get(parent_id.as_str())
                    .map(|s| s.as_str())
                    .unwrap_or(parent_id);
                format!("{parent_short}.{seq}")
            } else {
                short_ids
                    .get(&t.task_id)
                    .cloned()
                    .unwrap_or_else(|| t.task_id.clone())
            };
            table.add_row(vec![
                priority_label(t.priority).to_string(),
                t.status.clone(),
                t.task_type.clone(),
                t.assignee.as_deref().unwrap_or("-").to_string(),
                display_id,
                t.title.clone(),
            ]);
        }

        print!("{table}");

        // Blank line separates the table from the summary so markdown renderers
        // (e.g. glow) don't treat the summary as a table row.
        println!();

        let (ready_count, blocked_count) = ctx.store.count_ready_blocked()?;
        println!(
            "{} task(s) shown ({ready_count} ready, {blocked_count} blocked)",
            tasks.len()
        );
    }

    Ok(())
}

// ── show ────────────────────────────────────────────────────

pub fn show(ctx: &TaskCtx, id: &str) -> Result<()> {
    let id = ctx.store.resolve_task_id(id)?;
    let task = ctx
        .store
        .get_task(&id)?
        .ok_or_else(|| anyhow::anyhow!("task not found: {id}"))?;

    let labels = ctx.store.get_task_labels(&id)?;
    let comments = ctx.store.get_task_comments(&id)?;
    let dep_summary = ctx.store.get_dependency_summary(&id)?;
    let note_links = ctx.store.get_task_note_links(&id)?;
    let children = ctx.store.get_children(&id)?;
    let external_ids = ctx.store.get_external_ids(&id)?;

    if ctx.json {
        let comments_json = comments_to_json(&comments);
        let note_links_json = note_links_to_json(&note_links);
        let children_json = children_stubs_to_json(&children);

        let ext_ids_json: Vec<serde_json::Value> = external_ids
            .iter()
            .map(|e| {
                json!({
                    "source": e.source,
                    "external_id": e.external_id,
                    "external_url": e.external_url,
                })
            })
            .collect();

        let out = json!({
            "task": task_row_to_json(&task, labels),
            "dependency_summary": dep_summary_to_json_with_blocking(&dep_summary),
            "linked_notes": note_links_json,
            "comments": comments_json,
            "children": children_json,
            "external_ids": ext_ids_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Task: {}", task.task_id);
        if let (Some(parent_id), Some(seq)) = (&task.parent_task_id, task.child_seq) {
            let parent_short = ctx
                .store
                .shortest_unique_prefix(parent_id)
                .unwrap_or_else(|_| parent_id.clone());
            println!("Display ID: {parent_short}.{seq}");
        }
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
                let dep_task = ctx.store.get_task(dep_id)?;
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

        if !external_ids.is_empty() {
            println!("\nExternal References:");
            for e in &external_ids {
                let url = e.external_url.as_deref().unwrap_or("");
                if url.is_empty() {
                    println!("  [{}] {}", e.source, e.external_id);
                } else {
                    println!("  [{}] {} ({})", e.source, e.external_id, url);
                }
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

pub fn update(ctx: &TaskCtx, mut params: UpdateParams) -> Result<()> {
    params.id = ctx.store.resolve_task_id(&params.id)?;
    let has_status = params.status.is_some();
    let has_field_updates = params.title.is_some()
        || params.description.is_some()
        || params.priority.is_some()
        || params.task_type.is_some()
        || params.assignee.is_some()
        || params.blocked_reason.is_some();

    if !has_status && !has_field_updates {
        bail!("no updates specified");
    }

    // Status change is a separate event type
    if let Some(ref s) = params.status {
        let new_status: TaskStatus = s.parse().map_err(|e: String| anyhow::anyhow!(e))?;
        let event = TaskEvent::from_payload(&params.id, "cli", StatusChangedPayload { new_status });
        ctx.store.append(&event)?;
    }

    if has_field_updates {
        let event = TaskEvent::from_payload(
            &params.id,
            "cli",
            TaskUpdatedPayload {
                title: params.title,
                description: params.description,
                priority: params.priority,
                due_ts: None,
                blocked_reason: params.blocked_reason,
                task_type: params.task_type,
                assignee: params.assignee,
                defer_until: None,
            },
        );
        ctx.store.append(&event)?;
    }

    let task = ctx
        .store
        .get_task(&params.id)?
        .ok_or_else(|| anyhow::anyhow!("task not found after update: {}", params.id))?;

    if ctx.json {
        let labels = ctx.store.get_task_labels(&params.id)?;
        let out = json!({ "task": task_row_to_json(&task, labels) });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Updated task {}", params.id);
        println!("  Title: {}", task.title);
        println!("  Status: {}", task.status);
        println!("  Priority: {}", priority_label(task.priority));
    }

    Ok(())
}

// ── close ────────────────────────────────────────────────────

pub fn close(ctx: &TaskCtx, ids: &[String]) -> Result<()> {
    let mut closed = Vec::new();
    let mut all_unblocked = Vec::new();

    for raw_id in ids {
        let id = ctx.store.resolve_task_id(raw_id)?;
        let event = TaskEvent::from_payload(
            &id,
            "cli",
            StatusChangedPayload {
                new_status: TaskStatus::Done,
            },
        );
        ctx.store.append(&event)?;

        let unblocked = ctx.store.list_newly_unblocked(&id).unwrap_or_default();
        all_unblocked.extend(unblocked.clone());

        if ctx.json {
            let task = ctx.store.get_task(&id)?.unwrap();
            let labels = ctx.store.get_task_labels(&id)?;
            closed.push(json!({
                "task": task_row_to_json(&task, labels),
                "unblocked": unblocked,
            }));
        } else {
            println!("Closed task {id}");
            for u in &unblocked {
                println!("  Unblocked: {u}");
            }
        }
    }

    if ctx.json {
        let out = json!({
            "closed": closed,
            "total_closed": ids.len(),
            "total_unblocked": all_unblocked.len(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    }

    Ok(())
}

// ── stats ────────────────────────────────────────────────────

pub fn stats(ctx: &TaskCtx) -> Result<()> {
    let counts = ctx.store.count_by_status()?;
    let (ready, blocked) = ctx.store.count_ready_blocked()?;

    if ctx.json {
        let out = json!({
            "open": counts.open,
            "in_progress": counts.in_progress,
            "blocked": counts.blocked,
            "done": counts.done,
            "cancelled": counts.cancelled,
            "total": counts.total(),
            "ready": ready,
            "blocked_by_deps": blocked,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Task Statistics");
        println!("{}", "\u{2500}".repeat(30));
        println!("Open:        {:>4}", counts.open);
        println!("In Progress: {:>4}", counts.in_progress);
        println!("Blocked:     {:>4}", counts.blocked);
        println!("Done:        {:>4}", counts.done);
        println!("Cancelled:   {:>4}", counts.cancelled);
        println!("{}", "\u{2500}".repeat(30));
        println!("Total:       {:>4}", counts.total());
        println!("Ready:       {:>4}", ready);
        println!("Blocked:     {:>4}", blocked);
    }

    Ok(())
}

// ── dep add / dep remove ────────────────────────────────────

pub fn dep_add(ctx: &TaskCtx, task_id: &str, depends_on: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let depends_on = &ctx.store.resolve_task_id(depends_on)?;
    let event = TaskEvent::new(
        task_id,
        "cli",
        EventType::DependencyAdded,
        &DependencyPayload {
            depends_on_task_id: depends_on.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.json {
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

pub fn dep_remove(ctx: &TaskCtx, task_id: &str, depends_on: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let depends_on = &ctx.store.resolve_task_id(depends_on)?;
    let event = TaskEvent::new(
        task_id,
        "cli",
        EventType::DependencyRemoved,
        &DependencyPayload {
            depends_on_task_id: depends_on.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.json {
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

pub fn link(ctx: &TaskCtx, task_id: &str, chunk_id: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let event = TaskEvent::new(
        task_id.as_str(),
        "cli",
        EventType::NoteLinked,
        &NoteLinkPayload {
            chunk_id: chunk_id.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.json {
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

pub fn unlink(ctx: &TaskCtx, task_id: &str, chunk_id: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let event = TaskEvent::new(
        task_id.as_str(),
        "cli",
        EventType::NoteUnlinked,
        &NoteLinkPayload {
            chunk_id: chunk_id.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.json {
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

pub fn comment(ctx: &TaskCtx, task_id: &str, body: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let event = TaskEvent::from_payload(
        task_id.as_str(),
        "cli",
        CommentPayload {
            body: body.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.json {
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

pub fn label_add(ctx: &TaskCtx, task_id: &str, label: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let event = TaskEvent::new(
        task_id.as_str(),
        "cli",
        EventType::LabelAdded,
        &LabelPayload {
            label: label.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.json {
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

pub fn label_remove(ctx: &TaskCtx, task_id: &str, label: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let event = TaskEvent::new(
        task_id.as_str(),
        "cli",
        EventType::LabelRemoved,
        &LabelPayload {
            label: label.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.json {
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
