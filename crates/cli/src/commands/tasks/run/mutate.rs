use anyhow::{Result, bail};
use serde_json::json;

use brain_lib::tasks::events::*;
use brain_lib::utils::task_row_to_json;

use super::{TaskCtx, UpdateParams, priority_label};

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

pub fn close(ctx: &TaskCtx, ids: &[String], _brain: Option<&str>) -> Result<()> {
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

        let unblocked = match ctx.store.list_newly_unblocked(&id) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!("Failed to list newly unblocked tasks: {e}");
                Default::default()
            }
        };
        all_unblocked.extend(unblocked.clone());

        if ctx.json {
            let task = ctx
                .store
                .get_task(&id)?
                .ok_or_else(|| anyhow::anyhow!("Task not found after close: {id}"))?;
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

// ── ext-link ─────────────────────────────────────────────────

pub fn ext_link_add(
    ctx: &TaskCtx,
    task_id: &str,
    source: &str,
    id: &str,
    url: Option<&str>,
) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let event = TaskEvent::new(
        task_id.as_str(),
        "cli",
        EventType::ExternalIdAdded,
        &ExternalIdPayload {
            source: source.to_string(),
            external_id: id.to_string(),
            external_url: url.map(|u| u.to_string()),
        },
    );
    ctx.store.append(&event)?;

    if ctx.json {
        let out = serde_json::json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "source": source,
            "external_id": id,
            "external_url": url,
            "action": "ext_link_added",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added external reference [{source}:{id}] to task {task_id}");
    }

    Ok(())
}

pub fn ext_link_remove(ctx: &TaskCtx, task_id: &str, source: &str, id: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let event = TaskEvent::new(
        task_id.as_str(),
        "cli",
        EventType::ExternalIdRemoved,
        &ExternalIdPayload {
            source: source.to_string(),
            external_id: id.to_string(),
            external_url: None,
        },
    );
    ctx.store.append(&event)?;

    if ctx.json {
        let out = serde_json::json!({
            "event_id": event.event_id,
            "task_id": task_id,
            "source": source,
            "external_id": id,
            "action": "ext_link_removed",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Removed external reference [{source}:{id}] from task {task_id}");
    }

    Ok(())
}

pub fn ext_link_list(ctx: &TaskCtx, task_id: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let refs = ctx.store.get_external_ids(task_id)?;

    if ctx.json {
        let out: Vec<serde_json::Value> = refs
            .iter()
            .map(|e| {
                serde_json::json!({
                    "source": e.source,
                    "external_id": e.external_id,
                    "external_url": e.external_url,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if refs.is_empty() {
        println!("No external references for task {task_id}");
    } else {
        for r in &refs {
            if let Some(ref u) = r.external_url {
                println!("[{}:{}] {}", r.source, r.external_id, u);
            } else {
                println!("[{}:{}]", r.source, r.external_id);
            }
        }
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
