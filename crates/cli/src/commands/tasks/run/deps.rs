use anyhow::{Result, bail};
use serde_json::json;

use brain_lib::tasks::events::*;

use super::TaskCtx;

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

// ── batch dep operations ────────────────────────────────────

pub fn dep_add_chain(ctx: &TaskCtx, task_ids: &[String]) -> Result<()> {
    if task_ids.len() < 2 {
        bail!("chain requires at least 2 task IDs");
    }

    // Resolve all IDs first
    let resolved: Vec<String> = task_ids
        .iter()
        .map(|id| ctx.store.resolve_task_id(id))
        .collect::<brain_lib::error::Result<Vec<_>>>()?;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for i in 1..resolved.len() {
        let task_id = &resolved[i];
        let depends_on = &resolved[i - 1];

        let event = TaskEvent::new(
            task_id,
            "cli",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: depends_on.clone(),
            },
        );

        match ctx.store.append(&event) {
            Ok(()) => {
                succeeded.push((task_id.clone(), depends_on.clone()));
                if !ctx.json {
                    println!("{task_id} depends on {depends_on}");
                }
            }
            Err(e) => {
                failed.push((task_id.clone(), depends_on.clone(), format!("{e}")));
                if !ctx.json {
                    println!("Failed: {task_id} -> {depends_on}: {e}");
                }
            }
        }
    }

    if ctx.json {
        let out = json!({
            "succeeded": succeeded.iter().map(|(t, d)| json!({"task_id": t, "depends_on": d})).collect::<Vec<_>>(),
            "failed": failed.iter().map(|(t, d, e)| json!({"task_id": t, "depends_on": d, "error": e})).collect::<Vec<_>>(),
            "summary": { "succeeded": succeeded.len(), "failed": failed.len() },
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Chain: {} edges added, {} failed",
            succeeded.len(),
            failed.len()
        );
    }

    Ok(())
}

pub fn dep_add_fan(ctx: &TaskCtx, source: &str, dependents: &[String]) -> Result<()> {
    let source_resolved = ctx.store.resolve_task_id(source)?;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for raw_id in dependents {
        let dep_id = match ctx.store.resolve_task_id(raw_id) {
            Ok(id) => id,
            Err(e) => {
                failed.push((raw_id.clone(), format!("{e}")));
                if !ctx.json {
                    println!("Failed to resolve {raw_id}: {e}");
                }
                continue;
            }
        };

        let event = TaskEvent::new(
            &dep_id,
            "cli",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: source_resolved.clone(),
            },
        );

        match ctx.store.append(&event) {
            Ok(()) => {
                succeeded.push(dep_id.clone());
                if !ctx.json {
                    println!("{dep_id} depends on {source_resolved}");
                }
            }
            Err(e) => {
                failed.push((dep_id, format!("{e}")));
                if !ctx.json {
                    println!("Failed: {raw_id} -> {source_resolved}: {e}");
                }
            }
        }
    }

    if ctx.json {
        let out = json!({
            "source": source_resolved,
            "succeeded": succeeded,
            "failed": failed.iter().map(|(id, e)| json!({"task_id": id, "error": e})).collect::<Vec<_>>(),
            "summary": { "succeeded": succeeded.len(), "failed": failed.len() },
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Fan: {} dependents added to {source_resolved}, {} failed",
            succeeded.len(),
            failed.len()
        );
    }

    Ok(())
}

pub fn dep_clear(ctx: &TaskCtx, task_id: &str) -> Result<()> {
    let resolved = ctx.store.resolve_task_id(task_id)?;
    let deps = ctx.store.get_deps_for_task(&resolved)?;

    if deps.is_empty() {
        if ctx.json {
            let out = json!({
                "task_id": resolved,
                "succeeded": [],
                "failed": [],
                "summary": { "succeeded": 0, "failed": 0 },
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("No dependencies found for task {resolved}");
        }
        return Ok(());
    }

    let events: Vec<TaskEvent> = deps
        .iter()
        .map(|dep| {
            TaskEvent::new(
                &resolved,
                "cli",
                EventType::DependencyRemoved,
                &DependencyPayload {
                    depends_on_task_id: dep.clone(),
                },
            )
        })
        .collect();

    let results = ctx.store.append_batch(&events);
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(()) => succeeded.push(&deps[i]),
            Err(e) => failed.push((&deps[i], e)),
        }
    }

    if ctx.json {
        let out = json!({
            "task_id": resolved,
            "succeeded": succeeded,
            "failed": failed.iter().map(|(id, e)| json!({"depends_on": id, "error": format!("{e}")})).collect::<Vec<_>>(),
            "summary": { "succeeded": succeeded.len(), "failed": failed.len() },
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Cleared {} dependencies from task {resolved} ({} failed)",
            succeeded.len(),
            failed.len()
        );
    }

    Ok(())
}
