use anyhow::Result;
use serde_json::json;

use brain_lib::tasks::events::*;

use crate::markdown_table::MarkdownTable;

use super::TaskCtx;

// ── labels ──────────────────────────────────────────────────

pub fn labels(ctx: &TaskCtx) -> Result<()> {
    let summaries = ctx.store.label_summary()?;

    if ctx.json {
        let labels_json: Vec<serde_json::Value> = summaries
            .iter()
            .map(|s| {
                json!({
                    "label": s.label,
                    "count": s.count,
                    "task_ids": s.task_ids,
                })
            })
            .collect();
        let out = json!({ "labels": labels_json });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if summaries.is_empty() {
            println!("No labels found.");
            return Ok(());
        }

        let mut table = MarkdownTable::new(vec!["LABEL", "COUNT"]);
        for s in &summaries {
            table.add_row(vec![s.label.clone(), s.count.to_string()]);
        }
        print!("{table}");
        println!();
        println!("{} label(s)", summaries.len());
    }

    Ok(())
}

// ── label add / label remove ────────────────────────────────

pub fn label_add(ctx: &TaskCtx, task_id: &str, label: &str, brain: Option<&str>) -> Result<()> {
    if let Some(target_brain) = brain {
        let (_name, _id, tasks, _records, _objects) =
            brain_lib::config::open_brain_stores(target_brain)?;
        let remote_ctx = TaskCtx {
            store: tasks,
            json: ctx.json,
        };
        return label_add(&remote_ctx, task_id, label, None);
    }
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

pub fn label_remove(ctx: &TaskCtx, task_id: &str, label: &str, brain: Option<&str>) -> Result<()> {
    if let Some(target_brain) = brain {
        let (_name, _id, tasks, _records, _objects) =
            brain_lib::config::open_brain_stores(target_brain)?;
        let remote_ctx = TaskCtx {
            store: tasks,
            json: ctx.json,
        };
        return label_remove(&remote_ctx, task_id, label, None);
    }
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

// ── batch label operations ──────────────────────────────────

fn batch_label_op(
    ctx: &TaskCtx,
    task_ids: &[String],
    label: &str,
    event_type: EventType,
    action_name: &str,
) -> Result<()> {
    let events: Vec<TaskEvent> = task_ids
        .iter()
        .map(|raw_id| {
            let resolved = ctx.store.resolve_task_id(raw_id)?;
            Ok(TaskEvent::new(
                &resolved,
                "cli",
                event_type.clone(),
                &LabelPayload {
                    label: label.to_string(),
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let results = ctx.store.append_batch(&events);
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(()) => succeeded.push(&events[i].task_id),
            Err(e) => failed.push((&events[i].task_id, e)),
        }
    }

    if ctx.json {
        let out = json!({
            "succeeded": succeeded,
            "failed": failed.iter().map(|(id, e)| json!({"task_id": id, "error": format!("{e}")})).collect::<Vec<_>>(),
            "summary": { "succeeded": succeeded.len(), "failed": failed.len() },
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        for tid in &succeeded {
            println!("{action_name} label \"{label}\" on task {tid}");
        }
        for (tid, e) in &failed {
            println!("Failed on task {tid}: {e}");
        }
        println!("{} succeeded, {} failed", succeeded.len(), failed.len());
    }

    Ok(())
}

pub fn label_batch_add(
    ctx: &TaskCtx,
    task_ids: &[String],
    label: &str,
    brain: Option<&str>,
) -> Result<()> {
    if let Some(target_brain) = brain {
        let (_name, _id, tasks, _records, _objects) =
            brain_lib::config::open_brain_stores(target_brain)?;
        let remote_ctx = TaskCtx {
            store: tasks,
            json: ctx.json,
        };
        return label_batch_add(&remote_ctx, task_ids, label, None);
    }
    batch_label_op(ctx, task_ids, label, EventType::LabelAdded, "Added")
}

pub fn label_batch_remove(
    ctx: &TaskCtx,
    task_ids: &[String],
    label: &str,
    brain: Option<&str>,
) -> Result<()> {
    if let Some(target_brain) = brain {
        let (_name, _id, tasks, _records, _objects) =
            brain_lib::config::open_brain_stores(target_brain)?;
        let remote_ctx = TaskCtx {
            store: tasks,
            json: ctx.json,
        };
        return label_batch_remove(&remote_ctx, task_ids, label, None);
    }
    batch_label_op(ctx, task_ids, label, EventType::LabelRemoved, "Removed")
}

pub fn label_rename(ctx: &TaskCtx, old_label: &str, new_label: &str) -> Result<()> {
    let task_ids = ctx.store.get_task_ids_with_label(old_label)?;

    if task_ids.is_empty() {
        if ctx.json {
            let out = json!({
                "succeeded": [],
                "failed": [],
                "summary": { "succeeded": 0, "failed": 0 },
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("No tasks found with label \"{old_label}\"");
        }
        return Ok(());
    }

    let mut events = Vec::new();
    for tid in &task_ids {
        events.push(TaskEvent::new(
            tid,
            "cli",
            EventType::LabelRemoved,
            &LabelPayload {
                label: old_label.to_string(),
            },
        ));
        events.push(TaskEvent::new(
            tid,
            "cli",
            EventType::LabelAdded,
            &LabelPayload {
                label: new_label.to_string(),
            },
        ));
    }

    let results = ctx.store.append_batch(&events);
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (i, tid) in task_ids.iter().enumerate() {
        let remove_ok = results[i * 2].is_ok();
        let add_ok = results[i * 2 + 1].is_ok();
        if remove_ok && add_ok {
            succeeded.push(tid);
        } else {
            failed.push(tid);
        }
    }

    if ctx.json {
        let out = json!({
            "succeeded": succeeded,
            "failed": failed,
            "summary": { "succeeded": succeeded.len(), "failed": failed.len() },
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Renamed label \"{old_label}\" to \"{new_label}\" on {} task(s) ({} failed)",
            succeeded.len(),
            failed.len()
        );
    }

    Ok(())
}

pub fn label_purge(ctx: &TaskCtx, label: &str) -> Result<()> {
    let task_ids = ctx.store.get_task_ids_with_label(label)?;

    if task_ids.is_empty() {
        if ctx.json {
            let out = json!({
                "succeeded": [],
                "failed": [],
                "summary": { "succeeded": 0, "failed": 0 },
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("No tasks found with label \"{label}\"");
        }
        return Ok(());
    }

    let events: Vec<TaskEvent> = task_ids
        .iter()
        .map(|tid| {
            TaskEvent::new(
                tid,
                "cli",
                EventType::LabelRemoved,
                &LabelPayload {
                    label: label.to_string(),
                },
            )
        })
        .collect();

    let results = ctx.store.append_batch(&events);
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(()) => succeeded.push(&task_ids[i]),
            Err(e) => failed.push((&task_ids[i], e)),
        }
    }

    if ctx.json {
        let out = json!({
            "succeeded": succeeded,
            "failed": failed.iter().map(|(id, e)| json!({"task_id": id, "error": format!("{e}")})).collect::<Vec<_>>(),
            "summary": { "succeeded": succeeded.len(), "failed": failed.len() },
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Purged label \"{label}\" from {} task(s) ({} failed)",
            succeeded.len(),
            failed.len()
        );
    }

    Ok(())
}
