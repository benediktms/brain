use std::collections::HashMap;

use anyhow::Result;
use serde_json::json;

use brain_lib::tasks::enrichment::enrich_task_summaries;
use brain_lib::tasks::events::TaskType;
use brain_lib::tasks::queries::TaskRow;

use crate::markdown_table::MarkdownTable;

use super::{TaskCtx, priority_label};

pub fn next(ctx: &TaskCtx, k: usize) -> Result<()> {
    // Fetch ready actionable tasks (epics excluded by the store query)
    let mut tasks = ctx.store.list_ready_actionable()?;

    // Sort: in-progress first, then priority ascending (0=critical), then due_date
    let status_ord = |status: &str| -> u8 {
        if status == "in_progress" { 0 } else { 1 }
    };
    tasks.sort_by(|a, b| {
        status_ord(&a.status)
            .cmp(&status_ord(&b.status))
            .then(a.priority.cmp(&b.priority))
            .then_with(|| match (a.due_ts, b.due_ts) {
                (Some(a_ts), Some(b_ts)) => a_ts.cmp(&b_ts),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
            .then(a.task_id.cmp(&b.task_id))
    });

    // Take top-k
    let selected: Vec<TaskRow> = tasks.into_iter().take(k).collect();

    // Aggregate counts
    let (ready_count, blocked_count) = ctx.store.count_ready_blocked()?;

    if ctx.json {
        // Enrich with labels and dependency summaries
        let mut results_json = enrich_task_summaries(&ctx.store, &selected);

        // Replace task_id with short form; strip description
        for task_val in &mut results_json {
            if let Some(obj) = task_val.as_object_mut() {
                if let Some(tid) = obj
                    .get("task_id")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                {
                    let short = ctx
                        .store
                        .compact_id(&tid)
                        .unwrap_or_else(|_| tid.clone());
                    obj.insert("task_id".into(), json!(short));
                }
                obj.remove("description");
            }
        }

        // Build epic cache for grouping
        let mut epic_cache: HashMap<String, Option<serde_json::Value>> = HashMap::new();
        for task in &selected {
            if let Some(ref parent_id) = task.parent_task_id {
                if epic_cache.contains_key(parent_id) {
                    continue;
                }
                let epic_val = ctx
                    .store
                    .get_task(parent_id)
                    .ok()
                    .flatten()
                    .filter(|t| t.task_type == TaskType::Epic)
                    .map(|t| {
                        let short_id = ctx
                            .store
                            .compact_id(&t.task_id)
                            .unwrap_or_else(|_| t.task_id.clone());
                        json!({ "task_id": short_id, "title": t.title })
                    });
                epic_cache.insert(parent_id.clone(), epic_val);
            }
        }

        // Group by parent epic preserving selection order
        let mut groups: Vec<(Option<serde_json::Value>, Vec<serde_json::Value>)> = Vec::new();
        let mut group_index: HashMap<Option<String>, usize> = HashMap::new();

        for (task, task_json) in selected.iter().zip(results_json) {
            let epic_key: Option<String> = task
                .parent_task_id
                .as_ref()
                .and_then(|pid| epic_cache.get(pid))
                .and_then(|v| v.as_ref())
                .map(|_| task.parent_task_id.clone())
                .unwrap_or(None);

            if let Some(&idx) = group_index.get(&epic_key) {
                groups[idx].1.push(task_json);
            } else {
                let epic_val: Option<serde_json::Value> = epic_key
                    .as_ref()
                    .and_then(|pid| epic_cache.get(pid))
                    .and_then(|v| v.clone());
                let idx = groups.len();
                group_index.insert(epic_key, idx);
                groups.push((epic_val, vec![task_json]));
            }
        }

        let groups_json: Vec<serde_json::Value> = groups
            .into_iter()
            .map(|(epic, group_tasks)| json!({ "epic": epic, "tasks": group_tasks }))
            .collect();

        let out = json!({
            "tasks": groups_json,
            "ready_count": ready_count,
            "blocked_count": blocked_count,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        if selected.is_empty() {
            println!("No actionable tasks found.");
            println!();
            println!("{ready_count} ready, {blocked_count} blocked");
            return Ok(());
        }

        let short_ids = ctx.store.compact_ids()?;

        // Build a per-task epic title map for the EPIC column
        let mut epic_titles: HashMap<String, String> = HashMap::new();
        for task in &selected {
            if let Some(ref parent_id) = task.parent_task_id {
                if epic_titles.contains_key(&task.task_id) {
                    continue;
                }
                if let Ok(Some(parent)) = ctx.store.get_task(parent_id) {
                    if parent.task_type == TaskType::Epic {
                        let short = short_ids
                            .get(parent_id)
                            .cloned()
                            .unwrap_or_else(|| parent_id.clone());
                        epic_titles.insert(task.task_id.clone(), format!("{short} {}", parent.title));
                    }
                }
            }
        }

        let mut table = MarkdownTable::new(vec!["ID", "TITLE", "PRIORITY", "STATUS", "EPIC"]);
        for t in &selected {
            let short = short_ids
                .get(&t.task_id)
                .cloned()
                .unwrap_or_else(|| t.task_id.clone());
            let epic_col = epic_titles.get(&t.task_id).cloned().unwrap_or_default();
            table.add_row(vec![
                short,
                t.title.clone(),
                priority_label(t.priority).to_string(),
                t.status.clone(),
                epic_col,
            ]);
        }
        print!("{table}");
        println!();
        println!(
            "{} task(s) shown ({ready_count} ready, {blocked_count} blocked)",
            selected.len()
        );
    }

    Ok(())
}
