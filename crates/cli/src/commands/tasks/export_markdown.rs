use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use brain_lib::db::Db;
use brain_lib::tasks::TaskStore;
use brain_lib::tasks::queries::TaskRow;

fn format_ts(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn render_task_markdown(
    task: &TaskRow,
    labels: &[&str],
    blocks: &[&str],
    blocked_by: &[&str],
) -> String {
    let mut out = String::new();

    // YAML frontmatter
    out.push_str("---\n");
    out.push_str(&format!("id: {}\n", task.task_id));
    // Quote title to handle special YAML characters
    out.push_str(&format!(
        "title: \"{}\"\n",
        task.title.replace('\\', "\\\\").replace('"', "\\\"")
    ));
    out.push_str(&format!("status: {}\n", task.status));
    out.push_str(&format!("priority: {}\n", task.priority));
    out.push_str(&format!("type: {}\n", task.task_type));
    out.push_str(&format!("created: {}\n", format_ts(task.created_at)));
    out.push_str(&format!("updated: {}\n", format_ts(task.updated_at)));
    if let Some(assignee) = &task.assignee {
        out.push_str(&format!("assignee: {assignee}\n"));
    }
    if !labels.is_empty() {
        out.push_str(&format!("tags: [{}]\n", labels.join(", ")));
    }
    out.push_str("---\n\n");

    // Title
    out.push_str(&format!("# {}\n", task.title));

    // Description
    if let Some(desc) = &task.description
        && !desc.is_empty()
    {
        out.push_str(&format!("\n## Description\n\n{desc}\n"));
    }

    // Dependencies
    if !blocks.is_empty() || !blocked_by.is_empty() {
        out.push_str("\n## Dependencies\n\n");
        if !blocks.is_empty() {
            out.push_str(&format!("- Blocks: {}\n", blocks.join(", ")));
        }
        if !blocked_by.is_empty() {
            out.push_str(&format!("- Blocked by: {}\n", blocked_by.join(", ")));
        }
    }

    out
}

pub fn run(dir: PathBuf, sqlite_db: PathBuf) -> Result<()> {
    let db = Db::open(&sqlite_db).context("Failed to open SQLite database")?;
    let tasks_dir = sqlite_db
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("tasks");
    let store = TaskStore::new(&tasks_dir, db).context("Failed to open task store")?;

    let tasks = store.list_all()?;
    if tasks.is_empty() {
        println!("No tasks to export.");
        return Ok(());
    }

    // Load all deps and labels in bulk
    let all_deps = store.list_all_deps()?;
    let all_labels = store.list_all_labels()?;

    // Build dependency maps: blocked_by[task] = [deps...], blocks[dep] = [tasks...]
    let mut blocked_by: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut blocks: HashMap<&str, Vec<&str>> = HashMap::new();
    for dep in &all_deps {
        blocked_by
            .entry(&dep.task_id)
            .or_default()
            .push(&dep.depends_on);
        blocks
            .entry(&dep.depends_on)
            .or_default()
            .push(&dep.task_id);
    }

    // Build label map
    let mut labels_map: HashMap<&str, Vec<&str>> = HashMap::new();
    for (task_id, label) in &all_labels {
        labels_map.entry(task_id).or_default().push(label);
    }

    // Create output directory
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create output directory: {}", dir.display()))?;

    let mut count = 0;
    for task in &tasks {
        let task_labels = labels_map
            .get(task.task_id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let task_blocks = blocks
            .get(task.task_id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let task_blocked_by = blocked_by
            .get(task.task_id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let md = render_task_markdown(task, task_labels, task_blocks, task_blocked_by);

        let file_path = dir.join(format!("{}.md", task.task_id));
        std::fs::write(&file_path, md)
            .with_context(|| format!("Failed to write {}", file_path.display()))?;
        count += 1;
    }

    println!("Exported {count} tasks to {}", dir.display());
    Ok(())
}
