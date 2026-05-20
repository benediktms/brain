mod deps;
mod labels;
mod list;
mod mutate;
mod next;
mod show;
mod transfer;

pub use deps::*;
pub use labels::*;
pub use list::*;
pub use mutate::*;
pub use next::*;
pub use show::*;
pub use transfer::*;

use std::path::Path;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde_json::json;

use brain_lib::stores::BrainStores;
use brain_tasks::TaskStore;
use brain_tasks::enrichment::task_row_to_compact_json;
use brain_tasks::events::{self, *};

use crate::hooks::OutputFormat;

// ── shared context ─────────────────────────────────────────

pub struct TaskCtx {
    pub(crate) store: TaskStore,
    /// Output transport format.
    ///
    /// `Human` → human-readable text, `Json` → plain JSON,
    /// `HookEnvelope` → Claude Code hook envelope.
    pub(crate) output: OutputFormat,
}

impl TaskCtx {
    pub fn new(sqlite_db: &Path, lance_db: Option<&Path>, output: OutputFormat) -> Result<Self> {
        // Auto-detect cwd brain so `brain tasks <subcommand>` invoked inside
        // a project directory defaults to that project's brain instead of
        // the unscoped fallback. Falls through to unscoped on no match.
        let stores = BrainStores::from_path(sqlite_db, lance_db)?.scope_tasks_to_cwd()?;
        Ok(Self {
            store: stores.tasks,
            output,
        })
    }
}

// ── param structs ──────────────────────────────────────────

pub struct ShowParams {
    pub id: String,
    pub brain: Option<String>,
    pub remote: bool,
}

pub struct NextParams {
    pub k: usize,
    pub remote: bool,
}

pub struct CreateParams {
    pub title: String,
    pub description: Option<String>,
    pub priority: i32,
    pub task_type: TaskType,
    pub assignee: Option<String>,
    pub parent: Option<String>,
    pub brain: String,
    pub remote: bool,
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
    /// Experimental: route the request through brain-daemon over the
    /// local Unix socket. See `brain tasks list --remote` for usage.
    pub remote: bool,
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
    pub brain: String,
    pub remote: bool,
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

fn create_remote(ctx: &TaskCtx, params: &CreateParams) -> Result<()> {
    let mut client = crate::commands::rpc_client::connect_daemon()?;

    let wire_params = brain_rpc::TasksCreateParams {
        title: params.title.clone(),
        description: params.description.clone(),
        priority: params.priority.clamp(0, u8::MAX as i32) as u8,
        task_type: params.task_type.as_str().to_string(),
        assignee: params.assignee.clone(),
        parent: params.parent.clone(),
        brain: params.brain.clone(),
    };

    let (task, event_id) = client
        .tasks_create(wire_params)
        .map_err(|e| anyhow::anyhow!("TasksCreate rpc failed: {e}"))?;

    if ctx.output.is_json_mode() {
        let out = json!({
            "event_id": event_id,
            "task": {
                "task_id": task.task_id,
                "title": task.title,
                "status": task.status,
                "priority": task.priority,
                "brain_id": task.brain_id,
            },
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Created task {}", task.task_id);
        println!("  Title: {}", task.title);
        println!("  Priority: {}", priority_label(task.priority as i32));
        if let Some(ref a) = params.assignee {
            println!("  Assignee: {a}");
        }
        if let Some(ref p) = params.parent {
            println!("  Parent: {p}");
        }
    }

    Ok(())
}

pub fn create(ctx: &TaskCtx, params: CreateParams) -> Result<()> {
    if params.remote {
        return create_remote(ctx, &params);
    }
    // Cross-brain task creation: resolve target brain and write into its scope.
    let brain = &params.brain;
    let (bid, bname) = ctx.store.resolve_brain(brain)?;

    // Guard: reject writes to archived brains.
    if ctx.store.is_brain_archived(&bid)? {
        bail!("target brain '{bname}' is archived");
    }

    let remote_store = ctx.store.with_remote_brain_id(&bid, &bname)?;
    let prefix = remote_store.get_project_prefix()?;
    let task_id = events::new_task_id(&prefix);

    // Resolve parent task ID against the remote brain if provided.
    let parent = match params.parent {
        Some(ref p) => Some(remote_store.resolve_task_id(p)?.task_id),
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
            display_id: None,
        },
    );

    remote_store.append(&event)?;

    if ctx.output.is_json_mode() {
        let task = remote_store
            .get_task(&task_id)?
            .ok_or_else(|| anyhow::anyhow!("Task not found after creation: {task_id}"))?;
        let labels = remote_store.get_task_labels(&task_id)?;
        let out = json!({
            "event_id": event.event_id,
            "task": task_row_to_compact_json(&remote_store, &task, labels),
            "brain": bname,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        let display_id = remote_store.compact_id_or_raw(&task_id);
        println!("Created task {display_id} in brain '{bname}'");
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
