use anyhow::bail;

mod deps;
mod labels;
mod list;
mod mutate;
mod next;
mod show;

pub use deps::*;
pub use labels::*;
pub use list::*;
pub use mutate::*;
pub use next::*;
pub use show::*;

use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::json;

use brain_lib::stores::BrainStores;
use brain_lib::tasks::TaskStore;
use brain_lib::tasks::events::{self, *};
use brain_lib::utils::task_row_to_json;

// ── shared context ─────────────────────────────────────────

pub struct TaskCtx {
    pub(crate) store: TaskStore,
    pub(crate) json: bool,
}

impl TaskCtx {
    pub fn new(sqlite_db: &Path, lance_db: Option<&Path>, json: bool) -> Result<Self> {
        let stores = BrainStores::from_path(sqlite_db, lance_db)?;
        Ok(Self {
            store: stores.tasks,
            json,
        })
    }
}

// ── param structs ──────────────────────────────────────────

pub struct CreateParams {
    pub title: String,
    pub description: Option<String>,
    pub priority: i32,
    pub task_type: TaskType,
    pub assignee: Option<String>,
    pub parent: Option<String>,
    pub brain: Option<String>,
}

pub struct ListParams {
    pub status: Option<String>,
    pub priority: Option<i32>,
    pub task_type: Option<TaskType>,
    pub assignee: Option<String>,
    pub label: Option<String>,
    pub search: Option<String>,
    pub ready: bool,
    pub blocked: bool,
    pub include_description: bool,
    pub group_by: Option<String>,
    pub brain: Option<String>,
}

pub struct UpdateParams {
    pub id: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub status: Option<String>,
    pub priority: Option<i32>,
    pub task_type: Option<TaskType>,
    pub assignee: Option<String>,
    pub blocked_reason: Option<String>,
}

pub(super) fn format_ts(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| ts.to_string())
}

pub(super) fn format_ts_short(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| ts.to_string())
}

pub(super) fn priority_label(p: i32) -> &'static str {
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
    if params.brain.is_some() {
        bail!("cross-brain creation removed — all brains share a unified DB");
    }

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
            task_type: Some(params.task_type),
            assignee: params.assignee.clone(),
            defer_until: None,
            parent_task_id: parent.clone(),
        },
    );

    ctx.store.append(&event)?;

    if ctx.json {
        let task = ctx
            .store
            .get_task(&task_id)?
            .ok_or_else(|| anyhow::anyhow!("Task not found after creation: {task_id}"))?;
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
        println!("  Type: {}", params.task_type.as_str());
        if let Some(ref a) = params.assignee {
            println!("  Assignee: {a}");
        }
        if let Some(ref p) = params.parent {
            println!("  Parent: {p}");
        }
    }

    Ok(())
}
