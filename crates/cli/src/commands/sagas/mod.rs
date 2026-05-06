use std::path::Path;

use anyhow::{Result, anyhow};
use serde_json::json;

use brain_lib::stores::BrainStores;
use brain_lib::tasks::events::{StatusChangedPayload, TaskEvent, TaskStatus};
use brain_persistence::db::sagas::SagaListFilter;

pub struct SagaCtx {
    pub(crate) stores: BrainStores,
    pub(crate) json: bool,
}

impl SagaCtx {
    pub fn new(sqlite_db: &Path, json: bool) -> Result<Self> {
        let stores = BrainStores::from_path(sqlite_db, None)?;
        Ok(Self { stores, json })
    }
}

pub fn create(ctx: &SagaCtx, title: &str, description: Option<&str>) -> Result<()> {
    let row = ctx.stores.sagas.create(title, description, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": row.saga_id,
            "saga": {
                "saga_id": row.saga_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
                "members": [],
            }
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Created saga {}", row.saga_id);
        println!("  Title:  {}", row.title);
        println!("  Status: {}", row.status);
        if let Some(desc) = &row.description {
            println!("  Desc:   {desc}");
        }
    }
    Ok(())
}

pub fn list(
    ctx: &SagaCtx,
    include_closed: bool,
    include_cancelled: bool,
    all: bool,
    containing_brain: Option<String>,
) -> Result<()> {
    let filter = SagaListFilter {
        include_closed: include_closed || all,
        include_cancelled: include_cancelled || all,
        containing_brain,
    };
    let rows = ctx.stores.sagas.list(filter)?;

    if ctx.json {
        let sagas: Vec<serde_json::Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "saga_id": r.saga_id,
                    "title": r.title,
                    "description": r.description,
                    "status": r.status,
                    "created_at": r.created_at,
                    "updated_at": r.updated_at,
                    "closed_at": r.closed_at,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({ "sagas": sagas, "total": sagas.len() }))?
        );
    } else if rows.is_empty() {
        println!("No sagas found.");
    } else {
        for r in &rows {
            println!("[{}] {} ({})", r.saga_id, r.title, r.status);
        }
    }
    Ok(())
}

pub fn update(
    ctx: &SagaCtx,
    saga_id: &str,
    title: Option<&str>,
    description: Option<Option<&str>>,
) -> Result<()> {
    if title.is_none() && description.is_none() {
        anyhow::bail!("at least one of --title or --description is required");
    }
    let row = ctx
        .stores
        .sagas
        .update(saga_id, title, description, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": row.saga_id,
            "saga": {
                "saga_id": row.saga_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
            }
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Updated saga {}", row.saga_id);
        println!("  Title:  {}", row.title);
        println!("  Status: {}", row.status);
        if let Some(desc) = &row.description {
            println!("  Desc:   {desc}");
        }
    }
    Ok(())
}

pub fn add_tasks(ctx: &SagaCtx, saga_id: &str, task_ids: &[String]) -> Result<()> {
    let count = ctx.stores.sagas.add_tasks(saga_id, task_ids, "cli")?;
    if ctx.json {
        let out = serde_json::json!({
            "saga_id": saga_id,
            "added": count,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added {count} task(s) to saga {saga_id}");
    }
    Ok(())
}

pub fn frontier(ctx: &SagaCtx, saga_id: &str) -> Result<()> {
    let f = ctx.stores.sagas.frontier(saga_id)?;
    if ctx.json {
        let tasks: Vec<serde_json::Value> = f
            .tasks
            .iter()
            .map(|t| {
                json!({
                    "task_id": t.task_id,
                    "title": t.title,
                    "status": t.status,
                    "priority": t.priority,
                })
            })
            .collect();
        let brains: Vec<serde_json::Value> = f
            .brains
            .iter()
            .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
            .collect();
        let total = tasks.len();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "saga_id": saga_id,
                "tasks": tasks,
                "brains": brains,
                "total": total,
            }))?
        );
    } else if f.tasks.is_empty() {
        println!("No ready tasks in saga {saga_id}.");
    } else {
        println!("Ready tasks in saga {saga_id}:");
        for t in &f.tasks {
            println!("  [{}] {} ({})", t.task_id, t.title, t.status);
        }
    }
    Ok(())
}

pub fn stats(ctx: &SagaCtx, saga_id: &str) -> Result<()> {
    let s = ctx.stores.sagas.stats(saga_id)?;
    if ctx.json {
        let label_histogram: Vec<serde_json::Value> = s
            .label_histogram
            .iter()
            .map(|l| json!({ "label": l.label, "count": l.count }))
            .collect();
        let brains: Vec<serde_json::Value> = s
            .brains
            .iter()
            .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
            .collect();
        let c = &s.counts;
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "saga_id": saga_id,
                "stats": {
                    "total": c.total,
                    "open": c.open,
                    "in_progress": c.in_progress,
                    "blocked": c.blocked,
                    "done": c.done,
                    "cancelled": c.cancelled,
                    "completion_pct": c.completion_pct,
                },
                "label_histogram": label_histogram,
                "brains": brains,
            }))?
        );
    } else {
        let c = &s.counts;
        println!("Saga {saga_id} stats:");
        println!("  Total:       {}", c.total);
        println!("  Open:        {}", c.open);
        println!("  In progress: {}", c.in_progress);
        println!("  Blocked:     {}", c.blocked);
        println!("  Done:        {}", c.done);
        println!("  Cancelled:   {}", c.cancelled);
        if let Some(pct) = c.completion_pct {
            println!("  Completion:  {:.1}%", pct * 100.0);
        } else {
            println!("  Completion:  n/a");
        }
        if !s.label_histogram.is_empty() {
            println!("  Labels:");
            for l in &s.label_histogram {
                println!("    {}: {}", l.label, l.count);
            }
        }
    }
    Ok(())
}

pub fn start(ctx: &SagaCtx, saga_id: &str) -> Result<()> {
    let row = ctx.stores.sagas.start(saga_id, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": row.saga_id,
            "saga": {
                "saga_id": row.saga_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
            }
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Started saga {} (status: {})", row.saga_id, row.status);
    }
    Ok(())
}

pub fn close(ctx: &SagaCtx, saga_id: &str, cascade: bool) -> Result<()> {
    let (row, member_ids) = ctx.stores.sagas.close(saga_id, cascade, "cli")?;

    if cascade {
        for task_id in &member_ids {
            let task = match ctx.stores.tasks.get_task(task_id) {
                Ok(Some(t)) => t,
                Ok(None) => continue,
                Err(_) => continue,
            };
            if task.status == "done" || task.status == "cancelled" {
                continue;
            }
            let event = TaskEvent::from_payload(
                task_id.as_str(),
                "cli",
                StatusChangedPayload {
                    new_status: TaskStatus::Done,
                },
            );
            let _ = ctx.stores.tasks.append(&event);
        }
    }

    if ctx.json {
        let out = json!({
            "saga_id": row.saga_id,
            "saga": {
                "saga_id": row.saga_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
                "members": [],
            },
            "cascade": cascade,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Closed saga {}", row.saga_id);
        println!("  Title:  {}", row.title);
        println!("  Status: {}", row.status);
        if cascade {
            println!("  Cascade: closed {} member task(s)", member_ids.len());
        }
    }
    Ok(())
}

pub fn remove(ctx: &SagaCtx, saga_id: &str, task_ids: Vec<String>) -> Result<()> {
    let removed = ctx.stores.sagas.remove_tasks(saga_id, task_ids, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": saga_id,
            "removed": removed,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Removed {removed} task(s) from saga {saga_id}");
    }
    Ok(())
}

pub fn reopen(ctx: &SagaCtx, saga_id: &str) -> Result<()> {
    let row = ctx.stores.sagas.reopen(saga_id, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": row.saga_id,
            "saga": {
                "saga_id": row.saga_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
                "members": [],
            }
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Reopened saga {}", row.saga_id);
        println!("  Status: {}", row.status);
    }
    Ok(())
}

pub fn show(ctx: &SagaCtx, saga_id: &str) -> Result<()> {
    let row = ctx
        .stores
        .sagas
        .get(saga_id)?
        .ok_or_else(|| anyhow!("saga not found: {saga_id}"))?;

    let brains = ctx.stores.sagas.brains_for_saga(saga_id)?;

    if ctx.json {
        let brains_json: Vec<serde_json::Value> = brains
            .iter()
            .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
            .collect();
        let out = json!({
            "saga_id": row.saga_id,
            "saga": {
                "saga_id": row.saga_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
                "members": [],
                "brains": brains_json,
            }
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Saga {}", row.saga_id);
        println!("  Title:  {}", row.title);
        println!("  Status: {}", row.status);
        if let Some(desc) = &row.description {
            println!("  Desc:   {desc}");
        }
        if let Some(ts) = row.closed_at {
            println!("  Closed: {ts}");
        }
        if !brains.is_empty() {
            let brain_names: Vec<&str> = brains.iter().map(|b| b.name.as_str()).collect();
            println!("  Brains: {}", brain_names.join(", "));
        }
    }
    Ok(())
}
