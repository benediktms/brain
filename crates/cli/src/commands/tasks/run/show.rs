use anyhow::Result;
use serde_json::json;

use brain_lib::tasks::cross_brain::cross_brain_fetch;
use brain_lib::tasks::enrichment::{
    children_stubs_to_json, comments_to_json, dep_summary_to_json_with_blocking, note_links_to_json,
};
use brain_lib::utils::task_row_to_json;

use super::{TaskCtx, format_ts, format_ts_short, priority_label};

// ── show ────────────────────────────────────────────────────

pub fn show(ctx: &TaskCtx, id: &str, brain: Option<&str>) -> Result<()> {
    if let Some(target_brain) = brain {
        return show_remote(ctx, id, target_brain);
    }
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
        let display_id = ctx.store.compact_id(&id).unwrap_or_else(|_| id.clone());
        println!("Task: {display_id}");
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

fn show_remote(ctx: &TaskCtx, id: &str, target_brain: &str) -> Result<()> {
    let r = cross_brain_fetch(target_brain, id)?;

    if ctx.json {
        let comments_json = comments_to_json(&r.comments);
        let note_links_json = note_links_to_json(&r.note_links);
        let children_json = children_stubs_to_json(&r.children);
        let out = json!({
            "remote_brain_name": r.remote_brain_name,
            "remote_brain_id": r.remote_brain_id,
            "task": task_row_to_json(&r.task, r.labels),
            "dependency_summary": dep_summary_to_json_with_blocking(&r.dependency_summary),
            "linked_notes": note_links_json,
            "comments": comments_json,
            "children": children_json,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Brain: {} ({})", r.remote_brain_name, r.remote_brain_id);
        println!("Task: {}", r.task.task_id);
        println!("Title: {}", r.task.title);
        println!("Status: {}", r.task.status);
        println!("Priority: {}", priority_label(r.task.priority));
        println!("Type: {}", r.task.task_type);
        println!("Assignee: {}", r.task.assignee.as_deref().unwrap_or("-"));
        if let Some(ref parent) = r.task.parent_task_id {
            println!("Parent: {parent}");
        }
        println!("Created: {}", format_ts(r.task.created_at));
        println!("Updated: {}", format_ts(r.task.updated_at));
        if let Some(ref desc) = r.task.description {
            println!("\nDescription:\n  {}", desc.replace('\n', "\n  "));
        }
        if !r.labels.is_empty() {
            println!("\nLabels: {}", r.labels.join(", "));
        }
        if !r.comments.is_empty() {
            println!("\nComments ({}):", r.comments.len());
            for c in &r.comments {
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
