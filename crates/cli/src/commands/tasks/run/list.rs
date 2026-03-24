use anyhow::{Result, bail};
use serde_json::json;
use std::collections::{HashMap, HashSet};

use brain_lib::tasks::enrichment::enrich_task_list;
use brain_lib::tasks::queries::{TaskFilter, TaskRow, apply_filters};
use brain_lib::utils::task_row_to_compact_json;

use crate::markdown_table::MarkdownTable;

use super::{ListParams, TaskCtx, priority_label};

enum LabelFetchMode {
    OnlyWhenFiltering,
    AlwaysWarnDefault,
}

struct FilteredTasks {
    tasks: Vec<TaskRow>,
    labels_map: Option<HashMap<String, Vec<String>>>,
}

fn display_id(task_id: &str, short_ids: &HashMap<String, String>) -> String {
    short_ids
        .get(task_id)
        .cloned()
        .unwrap_or_else(|| task_id.to_string())
}

fn render_task_table<'a, I>(tasks: I, short_ids: &HashMap<String, String>)
where
    I: IntoIterator<Item = &'a TaskRow>,
{
    let mut table = MarkdownTable::new(vec!["PRI", "STATUS", "TYPE", "ASSIGNEE", "ID", "TITLE"]);
    for t in tasks {
        table.add_row(vec![
            priority_label(t.priority).to_string(),
            t.status.clone(),
            t.task_type.as_str().to_string(),
            t.assignee.as_deref().unwrap_or("-").to_string(),
            display_id(&t.task_id, short_ids),
            t.title.clone(),
        ]);
    }
    print!("{table}");
}

fn fetch_filtered_tasks(
    ctx: &TaskCtx,
    params: &ListParams,
    label_fetch_mode: LabelFetchMode,
) -> Result<FilteredTasks> {
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

    let fts_ids = if let Some(ref query) = params.search {
        let ids = ctx.store.search_fts(query, 1000)?;
        Some(ids.into_iter().collect::<HashSet<String>>())
    } else {
        None
    };

    let filter = TaskFilter {
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

    let labels_map = match label_fetch_mode {
        LabelFetchMode::OnlyWhenFiltering => {
            if filter.label.is_some() {
                let task_ids: Vec<&str> = tasks.iter().map(|t| t.task_id.as_str()).collect();
                ctx.store.get_labels_for_tasks(&task_ids).ok()
            } else {
                None
            }
        }
        LabelFetchMode::AlwaysWarnDefault => {
            let task_ids: Vec<&str> = tasks.iter().map(|t| t.task_id.as_str()).collect();
            let labels_map = match ctx.store.get_labels_for_tasks(&task_ids) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("Failed to get labels for tasks: {e}");
                    Default::default()
                }
            };
            Some(labels_map)
        }
    };

    let tasks = apply_filters(tasks, &filter, fts_ids.as_ref(), labels_map.as_ref());

    Ok(FilteredTasks { tasks, labels_map })
}

// ── list ────────────────────────────────────────────────────

pub fn list(ctx: &TaskCtx, params: &ListParams) -> Result<()> {
    if let Some(ref brain) = params.brain {
        let (_name, _id, tasks, _records, _objects) = brain_lib::config::open_brain_stores(brain)?;
        let remote_ctx = TaskCtx {
            store: tasks,
            json: ctx.json,
        };
        return list_inner(&remote_ctx, params);
    }
    list_inner(ctx, params)
}

fn list_inner(ctx: &TaskCtx, params: &ListParams) -> Result<()> {
    if let Some(ref group) = params.group_by {
        if group == "label" {
            return list_grouped_by_label(ctx, params);
        }
        bail!("Unknown --group-by value: \"{group}\". Supported: label");
    }

    let FilteredTasks { tasks, .. } =
        fetch_filtered_tasks(ctx, params, LabelFetchMode::OnlyWhenFiltering)?;

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

        render_task_table(&tasks, &short_ids);

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

    let FilteredTasks { tasks, labels_map } =
        fetch_filtered_tasks(ctx, params, LabelFetchMode::AlwaysWarnDefault)?;
    let labels_map = labels_map.unwrap_or_default();

    // Group tasks by label
    let mut groups: BTreeMap<String, Vec<&TaskRow>> = BTreeMap::new();
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
                        let mut j = task_row_to_compact_json(&ctx.store, t, labels);
                        if let Some(obj) = j.as_object_mut() {
                            let short = display_id(&t.task_id, &short_ids);
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
                    let mut j = task_row_to_compact_json(&ctx.store, t, labels);
                    if let Some(obj) = j.as_object_mut() {
                        let short = display_id(&t.task_id, &short_ids);
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
            render_task_table(group_tasks.iter().copied(), &short_ids);
            println!();
        }

        if !unlabeled.is_empty() {
            println!("## (unlabeled)");
            println!();
            render_task_table(unlabeled.iter().copied(), &short_ids);
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
