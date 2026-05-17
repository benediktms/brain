use anyhow::{Result, bail};
use serde_json::json;

use brain_tasks::enrichment::task_row_to_compact_json;
use brain_tasks::events::*;

use crate::hooks::{OutputFormat, build_hook_envelope};

use super::{TaskCtx, UpdateParams, priority_label};

// ── update ──────────────────────────────────────────────────

fn update_remote(ctx: &TaskCtx, params: &UpdateParams) -> Result<()> {
    // Status-only changes go through TasksMutate on the remote path.
    // Field updates go through TasksUpdate. When both are present we run
    // mutate first, then update — matching the local path's ordering.
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

    // blocked_reason is not on the wire UpdateParams today; bail clearly.
    if params.blocked_reason.is_some() {
        bail!("--blocked-reason is not yet supported on the --remote path");
    }
    // task_type is not on the wire UpdateParams today; bail clearly.
    if params.task_type.is_some() {
        bail!("--task-type is not yet supported on the --remote path");
    }

    let mut client = crate::commands::rpc_client::connect_daemon()?;

    // Status change via TasksMutate
    let mut last_task = None;
    let mut last_event_id = String::new();

    if let Some(ref status) = params.status {
        let action = match status.as_str() {
            "done" => "close",
            "open" => "open",
            "blocked" => "block",
            "in_progress" => "in_progress",
            "cancelled" => "cancel",
            other => bail!("unknown status for --remote path: {other}"),
        };
        let (task, event_id) = client
            .tasks_mutate(brain_rpc::TasksMutateParams {
                id: params.id.clone(),
                action: action.to_string(),
            })
            .map_err(|e| anyhow::anyhow!("TasksMutate rpc failed: {e}"))?;
        last_event_id = event_id;
        last_task = Some(task);
    }

    if has_field_updates {
        let (task, event_id) = client
            .tasks_update(brain_rpc::TasksUpdateParams {
                id: params.id.clone(),
                title: params.title.clone(),
                description: params.description.clone(),
                priority: params.priority.map(|p| p.clamp(0, u8::MAX as i32) as u8),
                assignee: params.assignee.clone(),
            })
            .map_err(|e| anyhow::anyhow!("TasksUpdate rpc failed: {e}"))?;
        last_event_id = event_id;
        last_task = Some(task);
    }

    let task = last_task.expect("at least one RPC was called");

    if ctx.output.is_json_mode() {
        let out = json!({
            "event_id": last_event_id,
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
        println!("Updated task {}", task.task_id);
        println!("  Title: {}", task.title);
        println!("  Status: {}", task.status);
        println!("  Priority: {}", priority_label(task.priority as i32));
    }

    Ok(())
}

pub fn update(ctx: &TaskCtx, mut params: UpdateParams) -> Result<()> {
    if params.remote {
        return update_remote(ctx, &params);
    }
    params.id = ctx.store.resolve_task_id(&params.id)?;
    let display_id = ctx.store.compact_id_or_raw(&params.id);
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

    if ctx.output.is_json_mode() {
        let labels = ctx.store.get_task_labels(&params.id)?;
        let out = json!({ "task": task_row_to_compact_json(&ctx.store, &task, labels) });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Updated task {display_id}");
        println!("  Title: {}", task.title);
        println!("  Status: {}", task.status.as_ref());
        println!("  Priority: {}", priority_label(task.priority.as_i32()));
    }

    Ok(())
}

// ── close ────────────────────────────────────────────────────

pub fn close(ctx: &TaskCtx, ids: &[String], _brain: Option<&str>) -> Result<()> {
    let mut closed = Vec::new();
    let mut all_unblocked = Vec::new();

    for raw_id in ids {
        let id = ctx.store.resolve_task_id(raw_id)?;
        let display_id = ctx.store.compact_id_or_raw(&id);
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
        let display_unblocked: Vec<String> = unblocked
            .iter()
            .map(|u| ctx.store.compact_id_or_raw(u))
            .collect();
        all_unblocked.extend(display_unblocked.clone());

        if ctx.output.is_json_mode() {
            let task = ctx
                .store
                .get_task(&id)?
                .ok_or_else(|| anyhow::anyhow!("Task not found after close: {id}"))?;
            let labels = ctx.store.get_task_labels(&id)?;
            closed.push(json!({
                "task": task_row_to_compact_json(&ctx.store, &task, labels),
                "unblocked": display_unblocked,
            }));
        } else {
            println!("Closed task {display_id}");
            for u in &display_unblocked {
                println!("  Unblocked: {u}");
            }
        }
    }

    if ctx.output.is_json_mode() {
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

    if ctx.output.is_json_mode() {
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
        if ctx.output == OutputFormat::HookEnvelope {
            let payload = serde_json::to_string_pretty(&out)?;
            println!("{}", build_hook_envelope("SessionStart", &payload));
        } else {
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
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
    let display_id = ctx.store.compact_id_or_raw(task_id);
    let event = TaskEvent::new(
        task_id.as_str(),
        "cli",
        EventType::NoteLinked,
        &NoteLinkPayload {
            chunk_id: chunk_id.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.output.is_json_mode() {
        let out = json!({
            "event_id": event.event_id,
            "task_id": display_id,
            "chunk_id": chunk_id,
            "action": "linked",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Linked note {chunk_id} to task {display_id}");
    }

    Ok(())
}

pub fn unlink(ctx: &TaskCtx, task_id: &str, chunk_id: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let display_id = ctx.store.compact_id_or_raw(task_id);
    let event = TaskEvent::new(
        task_id.as_str(),
        "cli",
        EventType::NoteUnlinked,
        &NoteLinkPayload {
            chunk_id: chunk_id.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.output.is_json_mode() {
        let out = json!({
            "event_id": event.event_id,
            "task_id": display_id,
            "chunk_id": chunk_id,
            "action": "unlinked",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Unlinked note {chunk_id} from task {display_id}");
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
    let display_id = ctx.store.compact_id_or_raw(task_id);
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

    if ctx.output.is_json_mode() {
        let out = serde_json::json!({
            "event_id": event.event_id,
            "task_id": display_id,
            "source": source,
            "external_id": id,
            "external_url": url,
            "action": "ext_link_added",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added external reference [{source}:{id}] to task {display_id}");
    }

    Ok(())
}

pub fn ext_link_remove(ctx: &TaskCtx, task_id: &str, source: &str, id: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let display_id = ctx.store.compact_id_or_raw(task_id);
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

    if ctx.output.is_json_mode() {
        let out = serde_json::json!({
            "event_id": event.event_id,
            "task_id": display_id,
            "source": source,
            "external_id": id,
            "action": "ext_link_removed",
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Removed external reference [{source}:{id}] from task {display_id}");
    }

    Ok(())
}

pub fn ext_link_list(ctx: &TaskCtx, task_id: &str) -> Result<()> {
    let task_id = &ctx.store.resolve_task_id(task_id)?;
    let display_id = ctx.store.compact_id_or_raw(task_id);
    let refs = ctx.store.get_external_ids(task_id)?;

    if ctx.output.is_json_mode() {
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
        println!("No external references for task {display_id}");
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
    let display_id = ctx.store.compact_id_or_raw(task_id);
    let event = TaskEvent::from_payload(
        task_id.as_str(),
        "cli",
        CommentPayload {
            body: body.to_string(),
        },
    );
    ctx.store.append(&event)?;

    if ctx.output.is_json_mode() {
        let out = json!({
            "event_id": event.event_id,
            "task_id": display_id,
            "body": body,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Added comment to task {display_id}");
    }

    Ok(())
}
