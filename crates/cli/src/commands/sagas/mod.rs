use std::path::Path;

use anyhow::{Result, anyhow};
use serde_json::json;

use brain_lib::sagas::SagaListFilter;
use brain_lib::stores::BrainStores;
use brain_persistence::db::sagas::compact_saga_id;
use brain_rpc::domain::{
    Request, Response, SagaDescriptionUpdate, SagaSummary, SagasCreateParams, SagasListParams,
    SagasUpdateParams,
};

use crate::commands::rpc_client;

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

// ── remote rendering helpers ─────────────────────────────────

fn render_saga_summary_json(s: &SagaSummary) -> serde_json::Value {
    json!({
        "saga_id": s.saga_id,
        "title": s.title,
        "description": s.description,
        "status": s.status,
        "created_at": s.created_at,
        "updated_at": s.updated_at,
        "closed_at": s.closed_at,
    })
}

// ── remote helpers ───────────────────────────────────────────

fn create_remote(title: &str, description: Option<&str>, json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasCreate {
            params: SagasCreateParams {
                title: title.to_string(),
                description: description.map(str::to_string),
            },
        })
        .map_err(|e| anyhow!("SagasCreate rpc failed: {e}"))?;
    let saga = match resp {
        Response::SagasCreate { saga } => saga,
        other => anyhow::bail!("unexpected response to SagasCreate: {other:?}"),
    };

    if json {
        let out = json!({
            "saga_id": saga.saga_id,
            "saga": render_saga_summary_json(&saga),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Created saga {}", saga.saga_id);
        println!("  Title:  {}", saga.title);
        println!("  Status: {}", saga.status);
        if let Some(ref desc) = saga.description {
            println!("  Desc:   {desc}");
        }
    }
    Ok(())
}

fn show_remote(saga_id: &str, json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasGet {
            saga_id: saga_id.to_string(),
        })
        .map_err(|e| anyhow!("SagasGet rpc failed: {e}"))?;
    let saga = match resp {
        Response::SagasGet { saga } => saga.ok_or_else(|| anyhow!("saga not found: {saga_id}"))?,
        other => anyhow::bail!("unexpected response to SagasGet: {other:?}"),
    };

    if json {
        let out = json!({
            "saga_id": saga.saga_id,
            "saga": render_saga_summary_json(&saga),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Saga {}", saga.saga_id);
        println!("  Title:  {}", saga.title);
        println!("  Status: {}", saga.status);
        if let Some(ref desc) = saga.description {
            println!("  Desc:   {desc}");
        }
        if let Some(ref ts) = saga.closed_at {
            println!("  Closed: {ts}");
        }
    }
    Ok(())
}

fn list_remote(
    include_closed: bool,
    include_cancelled: bool,
    all: bool,
    containing_brain: Option<String>,
    json: bool,
) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasList {
            params: SagasListParams {
                include_closed: include_closed || all,
                include_cancelled: include_cancelled || all,
                containing_brain,
            },
        })
        .map_err(|e| anyhow!("SagasList rpc failed: {e}"))?;
    let sagas = match resp {
        Response::SagasList { sagas } => sagas,
        other => anyhow::bail!("unexpected response to SagasList: {other:?}"),
    };

    if json {
        let items: Vec<serde_json::Value> = sagas.iter().map(render_saga_summary_json).collect();
        let total = items.len();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({ "sagas": items, "total": total }))?
        );
    } else if sagas.is_empty() {
        println!("No sagas found.");
    } else {
        for s in &sagas {
            println!("[{}] {} ({})", s.saga_id, s.title, s.status);
        }
    }
    Ok(())
}

fn update_remote(
    saga_id: &str,
    title: Option<&str>,
    description: Option<Option<&str>>,
    json: bool,
) -> Result<()> {
    let wire_desc = description.map(|opt| match opt {
        None => SagaDescriptionUpdate::Clear,
        Some(v) => SagaDescriptionUpdate::Set {
            value: v.to_string(),
        },
    });
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasUpdate {
            params: SagasUpdateParams {
                saga_id: saga_id.to_string(),
                title: title.map(str::to_string),
                description: wire_desc,
            },
        })
        .map_err(|e| anyhow!("SagasUpdate rpc failed: {e}"))?;
    let saga = match resp {
        Response::SagasUpdate { saga } => saga,
        other => anyhow::bail!("unexpected response to SagasUpdate: {other:?}"),
    };

    if json {
        let out = json!({
            "saga_id": saga.saga_id,
            "saga": render_saga_summary_json(&saga),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Updated saga {}", saga.saga_id);
        println!("  Title:  {}", saga.title);
        println!("  Status: {}", saga.status);
        if let Some(ref desc) = saga.description {
            println!("  Desc:   {desc}");
        }
    }
    Ok(())
}

fn add_tasks_remote(saga_id: &str, task_ids: &[String], cascade: bool, json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasAddTasks {
            saga_id: saga_id.to_string(),
            task_ids: task_ids.to_vec(),
            cascade,
        })
        .map_err(|e| anyhow!("SagasAddTasks rpc failed: {e}"))?;
    let (added, added_task_ids) = match resp {
        Response::SagasAddTasks {
            added,
            added_task_ids,
            ..
        } => (added as usize, added_task_ids),
        other => anyhow::bail!("unexpected response to SagasAddTasks: {other:?}"),
    };

    if json {
        let out = json!({
            "saga_id": saga_id,
            "added": added,
            "added_task_ids": added_task_ids,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if cascade && added > task_ids.len() {
        println!(
            "Added {added} task(s) to saga {saga_id} ({} input + {} cascaded)",
            task_ids.len(),
            added.saturating_sub(task_ids.len())
        );
    } else {
        println!("Added {added} task(s) to saga {saga_id}");
    }
    Ok(())
}

fn remove_tasks_remote(
    saga_id: &str,
    task_ids: Vec<String>,
    cascade: bool,
    json: bool,
) -> Result<()> {
    let input_count = task_ids.len();
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasRemoveTasks {
            saga_id: saga_id.to_string(),
            task_ids,
            cascade,
        })
        .map_err(|e| anyhow!("SagasRemoveTasks rpc failed: {e}"))?;
    let (removed, removed_task_ids) = match resp {
        Response::SagasRemoveTasks {
            removed,
            removed_task_ids,
            ..
        } => (removed as usize, removed_task_ids),
        other => anyhow::bail!("unexpected response to SagasRemoveTasks: {other:?}"),
    };

    if json {
        let out = json!({
            "saga_id": saga_id,
            "removed": removed,
            "removed_task_ids": removed_task_ids,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if cascade && removed > input_count {
        println!(
            "Removed {removed} task(s) from saga {saga_id} ({input_count} input + {} cascaded)",
            removed.saturating_sub(input_count)
        );
    } else {
        println!("Removed {removed} task(s) from saga {saga_id}");
    }
    Ok(())
}

fn frontier_remote(saga_id: &str, json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasFrontier {
            saga_id: saga_id.to_string(),
        })
        .map_err(|e| anyhow!("SagasFrontier rpc failed: {e}"))?;
    let (saga_status, tasks, brains) = match resp {
        Response::SagasFrontier {
            saga_status,
            tasks,
            brains,
            ..
        } => (saga_status, tasks, brains),
        other => anyhow::bail!("unexpected response to SagasFrontier: {other:?}"),
    };

    if json {
        let tasks_json: Vec<serde_json::Value> = tasks
            .iter()
            .map(|t| {
                json!({
                    "task_id": t.task_id,
                    "title": t.title,
                    "status": t.status,
                    "priority": t.priority,
                    "task_type": t.task_type,
                })
            })
            .collect();
        let brains_json: Vec<serde_json::Value> = brains
            .iter()
            .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
            .collect();
        let total = tasks_json.len();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "saga_id": saga_id,
                "saga_status": saga_status,
                "tasks": tasks_json,
                "brains": brains_json,
                "total": total,
            }))?
        );
    } else {
        if tasks.is_empty() {
            println!("No ready tasks in saga {saga_id}.");
        } else {
            println!("Ready tasks in saga {saga_id}:");
            for t in &tasks {
                println!("  [{}] {} ({})", t.task_id, t.title, t.status);
            }
        }
        if !brains.is_empty() {
            let names: Vec<&str> = brains.iter().map(|b| b.name.as_str()).collect();
            println!("Brains: {}", names.join(", "));
        }
    }
    Ok(())
}

fn stats_remote(saga_id: &str, json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasStats {
            saga_id: saga_id.to_string(),
        })
        .map_err(|e| anyhow!("SagasStats rpc failed: {e}"))?;
    let (stats, label_histogram, brains) = match resp {
        Response::SagasStats {
            stats,
            label_histogram,
            brains,
            ..
        } => (stats, label_histogram, brains),
        other => anyhow::bail!("unexpected response to SagasStats: {other:?}"),
    };

    if json {
        let label_histogram_json: Vec<serde_json::Value> = label_histogram
            .iter()
            .map(|l| json!({ "label": l.label, "count": l.count }))
            .collect();
        let brains_json: Vec<serde_json::Value> = brains
            .iter()
            .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "saga_id": saga_id,
                "stats": {
                    "total": stats.total,
                    "open": stats.open,
                    "in_progress": stats.in_progress,
                    "blocked": stats.blocked,
                    "done": stats.done,
                    "cancelled": stats.cancelled,
                    "completion_pct": stats.completion_pct,
                },
                "label_histogram": label_histogram_json,
                "brains": brains_json,
            }))?
        );
    } else {
        println!("Saga {saga_id} stats:");
        println!("  Total:       {}", stats.total);
        println!("  Open:        {}", stats.open);
        println!("  In progress: {}", stats.in_progress);
        println!("  Blocked:     {}", stats.blocked);
        println!("  Done:        {}", stats.done);
        println!("  Cancelled:   {}", stats.cancelled);
        if let Some(pct) = stats.completion_pct {
            println!("  Completion:  {pct:.1}%");
        } else {
            println!("  Completion:  n/a");
        }
        if !label_histogram.is_empty() {
            println!("  Labels:");
            for l in &label_histogram {
                println!("    {}: {}", l.label, l.count);
            }
        }
        if !brains.is_empty() {
            let names: Vec<&str> = brains.iter().map(|b| b.name.as_str()).collect();
            println!("  Brains: {}", names.join(", "));
        }
    }
    Ok(())
}

fn start_remote(saga_id: &str, json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasStart {
            saga_id: saga_id.to_string(),
        })
        .map_err(|e| anyhow!("SagasStart rpc failed: {e}"))?;
    let saga = match resp {
        Response::SagasStart { saga } => saga,
        other => anyhow::bail!("unexpected response to SagasStart: {other:?}"),
    };

    if json {
        let out = json!({
            "saga_id": saga.saga_id,
            "saga": render_saga_summary_json(&saga),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Started saga {} (status: {})", saga.saga_id, saga.status);
    }
    Ok(())
}

fn close_remote(saga_id: &str, cascade: bool, json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasClose {
            saga_id: saga_id.to_string(),
            cascade,
        })
        .map_err(|e| anyhow!("SagasClose rpc failed: {e}"))?;
    let (saga, cascade_results) = match resp {
        Response::SagasClose {
            saga,
            cascade_results,
            ..
        } => (saga, cascade_results),
        other => anyhow::bail!("unexpected response to SagasClose: {other:?}"),
    };

    if json {
        let cascade_json = render_wire_cascade_json(&cascade_results);
        let out = json!({
            "saga_id": saga.saga_id,
            "saga": render_saga_summary_json(&saga),
            "cascade": cascade,
            "cascade_results": cascade_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Closed saga {}", saga.saga_id);
        println!("  Title:  {}", saga.title);
        println!("  Status: {}", saga.status);
        if cascade {
            print_wire_cascade_summary(&cascade_results, "closed");
        }
    }

    if cascade {
        let failures = cascade_results
            .iter()
            .filter(|r| {
                matches!(
                    r.outcome,
                    brain_rpc::domain::SagaCascadeOutcome::Failed { .. }
                )
            })
            .count();
        if failures > 0 {
            anyhow::bail!(
                "cascade had {} failed member(s) for saga {}",
                failures,
                saga_id,
            );
        }
    }

    Ok(())
}

fn reopen_remote(saga_id: &str, json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasReopen {
            saga_id: saga_id.to_string(),
        })
        .map_err(|e| anyhow!("SagasReopen rpc failed: {e}"))?;
    let saga = match resp {
        Response::SagasReopen { saga } => saga,
        other => anyhow::bail!("unexpected response to SagasReopen: {other:?}"),
    };

    if json {
        let out = json!({
            "saga_id": saga.saga_id,
            "saga": render_saga_summary_json(&saga),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Reopened saga {}", saga.saga_id);
        println!("  Status: {}", saga.status);
    }
    Ok(())
}

fn cancel_remote(saga_id: &str, cascade: bool, json: bool) -> Result<()> {
    let mut client = rpc_client::connect_daemon()?;
    let resp = client
        .call(Request::SagasCancel {
            saga_id: saga_id.to_string(),
            cascade,
        })
        .map_err(|e| anyhow!("SagasCancel rpc failed: {e}"))?;
    let (saga, cascade_results) = match resp {
        Response::SagasCancel {
            saga,
            cascade_results,
            ..
        } => (saga, cascade_results),
        other => anyhow::bail!("unexpected response to SagasCancel: {other:?}"),
    };

    if json {
        let cascade_json = render_wire_cascade_json(&cascade_results);
        let out = json!({
            "saga_id": saga.saga_id,
            "saga": render_saga_summary_json(&saga),
            "cascade": cascade,
            "cascade_results": cascade_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Cancelled saga {}", saga.saga_id);
        if cascade {
            print_wire_cascade_summary(&cascade_results, "cancelled");
        }
    }

    if cascade {
        let failures = cascade_results
            .iter()
            .filter(|r| {
                matches!(
                    r.outcome,
                    brain_rpc::domain::SagaCascadeOutcome::Failed { .. }
                )
            })
            .count();
        if failures > 0 {
            anyhow::bail!(
                "cascade had {} failed member(s) for saga {}",
                failures,
                saga_id,
            );
        }
    }

    Ok(())
}

fn render_wire_cascade_json(
    results: &[brain_rpc::domain::SagaCascadeResult],
) -> Vec<serde_json::Value> {
    use brain_rpc::domain::SagaCascadeOutcome;
    results
        .iter()
        .map(|r| match &r.outcome {
            SagaCascadeOutcome::Closed => json!({ "task_id": r.task_id, "closed": true }),
            SagaCascadeOutcome::Cancelled => json!({ "task_id": r.task_id, "cancelled": true }),
            SagaCascadeOutcome::Skipped { reason } => {
                json!({ "task_id": r.task_id, "skipped": true, "reason": reason })
            }
            SagaCascadeOutcome::Failed { error } => {
                json!({ "task_id": r.task_id, "failed": true, "error": error })
            }
        })
        .collect()
}

fn print_wire_cascade_summary(
    results: &[brain_rpc::domain::SagaCascadeResult],
    success_verb: &str,
) {
    use brain_rpc::domain::SagaCascadeOutcome;
    let mut closed = 0usize;
    let mut cancelled = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    for r in results {
        match &r.outcome {
            SagaCascadeOutcome::Closed => closed += 1,
            SagaCascadeOutcome::Cancelled => cancelled += 1,
            SagaCascadeOutcome::Skipped { .. } => skipped += 1,
            SagaCascadeOutcome::Failed { .. } => failed += 1,
        }
    }
    let success_count = closed + cancelled;
    println!(
        "  Cascade: {success_count} member task(s) {success_verb}, {skipped} skipped, {failed} failed"
    );
    for r in results {
        if let SagaCascadeOutcome::Failed { error } = &r.outcome {
            println!("    failed: {} ({error})", r.task_id);
        }
    }
}

// ── public local + remote dispatchers ───────────────────────

pub fn create(ctx: &SagaCtx, title: &str, description: Option<&str>, remote: bool) -> Result<()> {
    if remote {
        return create_remote(title, description, ctx.json);
    }
    let row = ctx.stores.sagas.create(title, description, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": compact_saga_id(&row.display_id),
            "saga": {
                "saga_id": compact_saga_id(&row.display_id),
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
        println!("Created saga {}", compact_saga_id(&row.display_id));
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
    remote: bool,
) -> Result<()> {
    if remote {
        return list_remote(
            include_closed,
            include_cancelled,
            all,
            containing_brain,
            ctx.json,
        );
    }
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
                    "saga_id": compact_saga_id(&r.display_id),
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
            println!(
                "[{}] {} ({})",
                compact_saga_id(&r.display_id),
                r.title,
                r.status
            );
        }
    }
    Ok(())
}

pub fn update(
    ctx: &SagaCtx,
    saga_id: &str,
    title: Option<&str>,
    description: Option<Option<&str>>,
    remote: bool,
) -> Result<()> {
    if remote {
        return update_remote(saga_id, title, description, ctx.json);
    }
    if title.is_none() && description.is_none() {
        anyhow::bail!("at least one of --title, --description, or --clear-description is required");
    }
    let row = ctx
        .stores
        .sagas
        .update(saga_id, title, description, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": compact_saga_id(&row.display_id),
            "saga": {
                "saga_id": compact_saga_id(&row.display_id),
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
        println!("Updated saga {}", compact_saga_id(&row.display_id));
        println!("  Title:  {}", row.title);
        println!("  Status: {}", row.status);
        if let Some(desc) = &row.description {
            println!("  Desc:   {desc}");
        }
    }
    Ok(())
}

pub fn add_tasks(
    ctx: &SagaCtx,
    saga_id: &str,
    task_ids: &[String],
    cascade: bool,
    remote: bool,
) -> Result<()> {
    if remote {
        return add_tasks_remote(saga_id, task_ids, cascade, ctx.json);
    }
    let (canonical, saga_id_short) = ctx.stores.sagas.resolve_short(saga_id)?;
    let added = ctx
        .stores
        .sagas
        .add_tasks(&canonical, task_ids, cascade, "cli")?;
    let added_task_ids: Vec<String> = added
        .iter()
        .map(|id| ctx.stores.tasks.compact_id_or_raw(id))
        .collect();
    let count = added_task_ids.len();
    if ctx.json {
        let out = serde_json::json!({
            "saga_id": saga_id_short,
            "added": count,
            "added_task_ids": added_task_ids,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if cascade && count > task_ids.len() {
        println!(
            "Added {count} task(s) to saga {saga_id_short} ({} input + {} cascaded)",
            task_ids.len(),
            count.saturating_sub(task_ids.len())
        );
    } else {
        println!("Added {count} task(s) to saga {saga_id_short}");
    }
    Ok(())
}

pub fn frontier(ctx: &SagaCtx, saga_id: &str, remote: bool) -> Result<()> {
    if remote {
        return frontier_remote(saga_id, ctx.json);
    }
    let (canonical, saga_id_short) = ctx.stores.sagas.resolve_short(saga_id)?;
    let saga_id = saga_id_short.as_str();
    let f = ctx.stores.sagas.frontier(&canonical)?;
    let compact = |canonical: &str| -> String { ctx.stores.tasks.compact_id_or_raw(canonical) };
    if ctx.json {
        let tasks: Vec<serde_json::Value> = f
            .tasks
            .iter()
            .map(|t| {
                json!({
                    "task_id": compact(t.id.as_str()),
                    "title": t.title,
                    "status": t.status,
                    "priority": t.priority,
                    "task_type": t.task_type,
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
                "saga_status": f.status.to_string(),
                "tasks": tasks,
                "brains": brains,
                "total": total,
            }))?
        );
    } else {
        if f.tasks.is_empty() {
            println!("No ready tasks in saga {saga_id}.");
        } else {
            println!("Ready tasks in saga {saga_id}:");
            for t in &f.tasks {
                println!("  [{}] {} ({})", compact(t.id.as_str()), t.title, t.status);
            }
        }
        // Mirror the `show` command's brains line.
        if !f.brains.is_empty() {
            let names: Vec<&str> = f.brains.iter().map(|b| b.name.as_str()).collect();
            println!("Brains: {}", names.join(", "));
        }
    }
    Ok(())
}

pub fn stats(ctx: &SagaCtx, saga_id: &str, remote: bool) -> Result<()> {
    if remote {
        return stats_remote(saga_id, ctx.json);
    }
    let (canonical, saga_id_short) = ctx.stores.sagas.resolve_short(saga_id)?;
    let saga_id = saga_id_short.as_str();
    let s = ctx.stores.sagas.stats(&canonical)?;
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
            // pct is already a 0–100 percentage from saga_stats; do not multiply.
            println!("  Completion:  {pct:.1}%");
        } else {
            println!("  Completion:  n/a");
        }
        if !s.label_histogram.is_empty() {
            println!("  Labels:");
            for l in &s.label_histogram {
                println!("    {}: {}", l.label, l.count);
            }
        }
        if !s.brains.is_empty() {
            let names: Vec<&str> = s.brains.iter().map(|b| b.name.as_str()).collect();
            println!("  Brains: {}", names.join(", "));
        }
    }
    Ok(())
}

pub fn start(ctx: &SagaCtx, saga_id: &str, remote: bool) -> Result<()> {
    if remote {
        return start_remote(saga_id, ctx.json);
    }
    let row = ctx.stores.sagas.start(saga_id, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": compact_saga_id(&row.display_id),
            "saga": {
                "saga_id": compact_saga_id(&row.display_id),
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
        println!(
            "Started saga {} (status: {})",
            compact_saga_id(&row.display_id),
            row.status
        );
    }
    Ok(())
}

pub fn close(ctx: &SagaCtx, saga_id: &str, cascade: bool, remote: bool) -> Result<()> {
    if remote {
        return close_remote(saga_id, cascade, ctx.json);
    }
    // H2: cascade now happens inside SagaStore::close, atomically with the
    // saga's status change. We just consume the per-task results here.
    let (row, cascade_results) = ctx.stores.sagas.close(saga_id, cascade, "cli")?;

    let any_failed = cascade_results.iter().any(|r| r.is_failure());

    if ctx.json {
        let cascade_json = render_cascade_json(&cascade_results);
        let out = json!({
            "saga_id": compact_saga_id(&row.display_id),
            "saga": {
                "saga_id": compact_saga_id(&row.display_id),
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
            },
            "cascade": cascade,
            "cascade_results": cascade_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Closed saga {}", compact_saga_id(&row.display_id));
        println!("  Title:  {}", row.title);
        println!("  Status: {}", row.status);
        if cascade {
            print_cascade_summary(&cascade_results, "closed");
        }
    }

    // H3: surface cascade failures as a non-zero exit so users notice them.
    if any_failed {
        anyhow::bail!(
            "{} member task(s) failed to transition during cascade",
            cascade_results.iter().filter(|r| r.is_failure()).count()
        );
    }
    Ok(())
}

pub fn remove(
    ctx: &SagaCtx,
    saga_id: &str,
    task_ids: Vec<String>,
    cascade: bool,
    remote: bool,
) -> Result<()> {
    if remote {
        return remove_tasks_remote(saga_id, task_ids, cascade, ctx.json);
    }
    let (canonical, saga_id_short) = ctx.stores.sagas.resolve_short(saga_id)?;
    let input_count = task_ids.len();
    let removed = ctx
        .stores
        .sagas
        .remove_tasks(&canonical, task_ids, cascade, "cli")?;
    let removed_task_ids: Vec<String> = removed
        .iter()
        .map(|id| ctx.stores.tasks.compact_id_or_raw(id))
        .collect();
    let count = removed_task_ids.len();
    if ctx.json {
        let out = json!({
            "saga_id": saga_id_short,
            "removed": count,
            "removed_task_ids": removed_task_ids,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if cascade && count > input_count {
        println!(
            "Removed {count} task(s) from saga {saga_id_short} ({input_count} input + {} cascaded)",
            count.saturating_sub(input_count)
        );
    } else {
        println!("Removed {count} task(s) from saga {saga_id_short}");
    }
    Ok(())
}

pub fn reopen(ctx: &SagaCtx, saga_id: &str, remote: bool) -> Result<()> {
    if remote {
        return reopen_remote(saga_id, ctx.json);
    }
    let row = ctx.stores.sagas.reopen(saga_id, "cli")?;
    if ctx.json {
        let out = json!({
            "saga_id": compact_saga_id(&row.display_id),
            "saga": {
                "saga_id": compact_saga_id(&row.display_id),
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
        println!("Reopened saga {}", compact_saga_id(&row.display_id));
        println!("  Status: {}", row.status);
    }
    Ok(())
}

pub fn cancel(ctx: &SagaCtx, saga_id: &str, cascade: bool, remote: bool) -> Result<()> {
    if remote {
        return cancel_remote(saga_id, cascade, ctx.json);
    }
    let (row, cascade_results) = ctx.stores.sagas.cancel(saga_id, cascade, "cli")?;
    let any_failed = cascade_results.iter().any(|r| r.is_failure());

    if ctx.json {
        let cascade_json = render_cascade_json(&cascade_results);
        let out = json!({
            "saga_id": compact_saga_id(&row.display_id),
            "saga": {
                "saga_id": compact_saga_id(&row.display_id),
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
            },
            "cascade": cascade,
            "cascade_results": cascade_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Cancelled saga {}", compact_saga_id(&row.display_id));
        if cascade {
            print_cascade_summary(&cascade_results, "cancelled");
        }
    }

    if any_failed {
        anyhow::bail!(
            "{} member task(s) failed to transition during cascade",
            cascade_results.iter().filter(|r| r.is_failure()).count()
        );
    }
    Ok(())
}

fn render_cascade_json(results: &[brain_lib::sagas::CascadeResult]) -> Vec<serde_json::Value> {
    use brain_lib::sagas::CascadeOutcome;
    results
        .iter()
        .map(|r| match &r.outcome {
            CascadeOutcome::Closed => json!({ "task_id": r.task_id, "closed": true }),
            CascadeOutcome::Cancelled => json!({ "task_id": r.task_id, "cancelled": true }),
            CascadeOutcome::Skipped { reason } => {
                json!({ "task_id": r.task_id, "skipped": true, "reason": reason })
            }
            CascadeOutcome::Failed { error } => {
                json!({ "task_id": r.task_id, "failed": true, "error": error })
            }
        })
        .collect()
}

fn print_cascade_summary(results: &[brain_lib::sagas::CascadeResult], success_verb: &str) {
    use brain_lib::sagas::CascadeOutcome;
    let mut closed = 0usize;
    let mut cancelled = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    for r in results {
        match &r.outcome {
            CascadeOutcome::Closed => closed += 1,
            CascadeOutcome::Cancelled => cancelled += 1,
            CascadeOutcome::Skipped { .. } => skipped += 1,
            CascadeOutcome::Failed { .. } => failed += 1,
        }
    }
    let success_count = closed + cancelled;
    println!(
        "  Cascade: {success_count} member task(s) {success_verb}, {skipped} skipped, {failed} failed"
    );
    // List failures explicitly so users can see what didn't transition.
    for r in results {
        if let CascadeOutcome::Failed { error } = &r.outcome {
            println!("    failed: {} ({error})", r.task_id);
        }
    }
}

pub fn show(ctx: &SagaCtx, saga_id: &str, remote: bool) -> Result<()> {
    if remote {
        return show_remote(saga_id, ctx.json);
    }
    let row = ctx
        .stores
        .sagas
        .get(saga_id)?
        .ok_or_else(|| anyhow!("saga not found: {saga_id}"))?;

    let brains = ctx.stores.sagas.brains_for_saga(saga_id)?;
    let members = ctx.stores.sagas.list_member_stubs(saga_id)?;

    if ctx.json {
        let brains_json: Vec<serde_json::Value> = brains
            .iter()
            .map(|b| json!({ "brain_id": b.brain_id, "name": b.name, "prefix": b.prefix }))
            .collect();
        let members_json: Vec<serde_json::Value> = members
            .iter()
            .map(|m| {
                json!({
                    "task_id": ctx.stores.tasks.compact_id_or_raw(m.task_id.as_str()),
                    "brain_id": m.brain_id,
                    "title": m.title,
                    "status": m.status,
                    "task_type": m.task_type,
                })
            })
            .collect();
        let out = json!({
            "saga_id": compact_saga_id(&row.display_id),
            "saga": {
                "saga_id": compact_saga_id(&row.display_id),
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "closed_at": row.closed_at,
                "members": members_json,
                "brains": brains_json,
            }
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Saga {}", compact_saga_id(&row.display_id));
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
