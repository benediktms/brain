use anyhow::{Result, bail};
use serde_json::json;

use brain_lib::tasks::enrichment::enrich_task_list;
use brain_lib::tasks::queries::{TaskFilter, apply_filters};
use brain_lib::utils::task_row_to_json;

use crate::markdown_table::MarkdownTable;

use super::{ListParams, TaskCtx, priority_label};

// ── list ────────────────────────────────────────────────────

pub fn list(ctx: &TaskCtx, params: &ListParams) -> Result<()> {
    if let Some(ref group) = params.group_by {
        if group == "label" {
            return list_grouped_by_label(ctx, params);
        }
        bail!("Unknown --group-by value: \"{group}\". Supported: label");
    }

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

    // FTS pre-filter
    let fts_ids = if let Some(ref query) = params.search {
        let ids = ctx.store.search_fts(query, 1000)?;
        Some(
            ids.into_iter()
                .collect::<std::collections::HashSet<String>>(),
        )
    } else {
        None
    };

    // Build filter (status is handled separately via list_ready/list_blocked/list_all)
    let filter = TaskFilter {
        priority: params.priority,
        task_type: params.task_type,
        assignee: params.assignee.clone(),
        label: params.label.clone(),
        search: params.search.clone(),
    };

    // Pre-filter by status (not part of TaskFilter since it's handled by the base query)
    let tasks: Vec<_> = tasks
        .into_iter()
        .filter(|t| {
            if let Some(ref s) = params.status {
                t.status == *s
            } else {
                true
            }
        })
        .collect();

    // Batch-fetch labels if label filter is active
    let labels_map = if filter.label.is_some() {
        let task_ids: Vec<&str> = tasks.iter().map(|t| t.task_id.as_str()).collect();
        ctx.store.get_labels_for_tasks(&task_ids).ok()
    } else {
        None
    };

    let tasks = apply_filters(tasks, &filter, fts_ids.as_ref(), labels_map.as_ref());

    if ctx.json {
        let task_ids: Vec<&str> = tasks.iter().map(|t| t.task_id.as_str()).collect();
        let labels_map = match ctx.store.get_labels_for_tasks(&task_ids) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("Failed to get labels for tasks: {e}");
                Default::default()
            }
        };
        let (mut items, ready_count, blocked_count) =
            enrich_task_list(&ctx.store, &tasks, &labels_map);
        if !params.include_description {
            for item in &mut items {
                if let Some(obj) = item.as_object_mut() {
                    obj.remove("description");
                }
            }
        }
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

        let short_ids = ctx.store.compact_ids()?;

        let mut table =
            MarkdownTable::new(vec!["PRI", "STATUS", "TYPE", "ASSIGNEE", "ID", "TITLE"]);

        for t in &tasks {
            let display_id = short_ids
                .get(&t.task_id)
                .cloned()
                .unwrap_or_else(|| t.task_id.clone());
            table.add_row(vec![
                priority_label(t.priority).to_string(),
                t.status.clone(),
                t.task_type.as_str().to_string(),
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

// ── list grouped by label ───────────────────────────────────

fn list_grouped_by_label(ctx: &TaskCtx, params: &ListParams) -> Result<()> {
    use std::collections::BTreeMap;

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

    // FTS pre-filter
    let fts_ids = if let Some(ref query) = params.search {
        let ids = ctx.store.search_fts(query, 1000)?;
        Some(
            ids.into_iter()
                .collect::<std::collections::HashSet<String>>(),
        )
    } else {
        None
    };

    let filter = brain_lib::tasks::queries::TaskFilter {
        priority: params.priority,
        task_type: params.task_type,
        assignee: params.assignee.clone(),
        label: params.label.clone(),
        search: params.search.clone(),
    };

    let tasks: Vec<_> = tasks
        .into_iter()
        .filter(|t| {
            if let Some(ref s) = params.status {
                t.status == *s
            } else {
                true
            }
        })
        .collect();

    // Batch-fetch labels for all tasks
    let task_ids: Vec<&str> = tasks.iter().map(|t| t.task_id.as_str()).collect();
    let labels_map = match ctx.store.get_labels_for_tasks(&task_ids) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!("Failed to get labels for tasks: {e}");
            Default::default()
        }
    };

    let tasks = apply_filters(tasks, &filter, fts_ids.as_ref(), Some(&labels_map));

    // Group tasks by label
    let mut groups: BTreeMap<String, Vec<&brain_lib::tasks::queries::TaskRow>> = BTreeMap::new();
    let mut unlabeled = Vec::new();

    for task in &tasks {
        let task_labels = labels_map.get(&task.task_id);
        if let Some(lbls) = task_labels {
            if lbls.is_empty() {
                unlabeled.push(task);
            } else {
                for lbl in lbls {
                    groups.entry(lbl.clone()).or_default().push(task);
                }
            }
        } else {
            unlabeled.push(task);
        }
    }

    if ctx.json {
        let short_ids = ctx.store.compact_ids()?;
        let mut group_list: Vec<serde_json::Value> = groups
            .iter()
            .map(|(label, group_tasks)| {
                let task_jsons: Vec<serde_json::Value> = group_tasks
                    .iter()
                    .map(|t| {
                        let labels = labels_map.get(&t.task_id).cloned().unwrap_or_default();
                        let mut j = task_row_to_json(t, labels);
                        if let Some(obj) = j.as_object_mut() {
                            let short = short_ids
                                .get(&t.task_id)
                                .cloned()
                                .unwrap_or_else(|| t.task_id.clone());
                            obj.insert("short_id".into(), json!(short));
                            if !params.include_description {
                                obj.remove("description");
                            }
                        }
                        j
                    })
                    .collect();
                json!({ "label": label, "tasks": task_jsons })
            })
            .collect();

        if !unlabeled.is_empty() {
            let task_jsons: Vec<serde_json::Value> = unlabeled
                .iter()
                .map(|t| {
                    let labels = labels_map.get(&t.task_id).cloned().unwrap_or_default();
                    let mut j = task_row_to_json(t, labels);
                    if let Some(obj) = j.as_object_mut() {
                        let short = short_ids
                            .get(&t.task_id)
                            .cloned()
                            .unwrap_or_else(|| t.task_id.clone());
                        obj.insert("short_id".into(), json!(short));
                        if !params.include_description {
                            obj.remove("description");
                        }
                    }
                    j
                })
                .collect();
            group_list.push(json!({ "label": "(unlabeled)", "tasks": task_jsons }));
        }

        let out = json!({ "groups": group_list, "count": tasks.len() });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if tasks.is_empty() {
            println!("No tasks found.");
            return Ok(());
        }

        let short_ids = ctx.store.compact_ids()?;

        for (label, group_tasks) in &groups {
            println!("## {label}");
            println!();
            let mut table =
                MarkdownTable::new(vec!["PRI", "STATUS", "TYPE", "ASSIGNEE", "ID", "TITLE"]);
            for t in group_tasks {
                let display_id = short_ids
                    .get(&t.task_id)
                    .cloned()
                    .unwrap_or_else(|| t.task_id.clone());
                table.add_row(vec![
                    priority_label(t.priority).to_string(),
                    t.status.clone(),
                    t.task_type.as_str().to_string(),
                    t.assignee.as_deref().unwrap_or("-").to_string(),
                    display_id,
                    t.title.clone(),
                ]);
            }
            print!("{table}");
            println!();
        }

        if !unlabeled.is_empty() {
            println!("## (unlabeled)");
            println!();
            let mut table =
                MarkdownTable::new(vec!["PRI", "STATUS", "TYPE", "ASSIGNEE", "ID", "TITLE"]);
            for t in &unlabeled {
                let display_id = short_ids
                    .get(&t.task_id)
                    .cloned()
                    .unwrap_or_else(|| t.task_id.clone());
                table.add_row(vec![
                    priority_label(t.priority).to_string(),
                    t.status.clone(),
                    t.task_type.as_str().to_string(),
                    t.assignee.as_deref().unwrap_or("-").to_string(),
                    display_id,
                    t.title.clone(),
                ]);
            }
            print!("{table}");
            println!();
        }

        let (ready_count, blocked_count) = ctx.store.count_ready_blocked()?;
        println!(
            "{} task(s) shown ({ready_count} ready, {blocked_count} blocked)",
            tasks.len()
        );
    }

    Ok(())
}
