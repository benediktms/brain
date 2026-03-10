use std::collections::HashMap;

use rusqlite::Connection;

use crate::error::Result;

/// Get labels for a task.
pub fn get_task_labels(conn: &Connection, task_id: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT label FROM task_labels WHERE task_id = ?1 ORDER BY label")?;
    let rows = stmt.query_map([task_id], |row| row.get::<_, String>(0))?;
    crate::db::collect_rows(rows)
}

/// Batch-fetch labels for a set of task IDs. Returns a map from task_id to sorted labels.
pub fn get_labels_for_tasks(
    conn: &Connection,
    task_ids: &[&str],
) -> Result<HashMap<String, Vec<String>>> {
    if task_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders: Vec<&str> = task_ids.iter().map(|_| "?").collect();
    let sql = format!(
        "SELECT task_id, label FROM task_labels WHERE task_id IN ({}) ORDER BY task_id, label",
        placeholders.join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::types::ToSql> = task_ids
        .iter()
        .map(|id| id as &dyn rusqlite::types::ToSql)
        .collect();
    let rows = stmt.query_map(params.as_slice(), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let (tid, label) = row?;
        map.entry(tid).or_default().push(label);
    }
    Ok(map)
}

/// Get all task IDs that have a given label.
pub fn get_task_ids_with_label(conn: &Connection, label: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT task_id FROM task_labels WHERE label = ?1")?;
    let rows = stmt.query_map([label], |row| row.get::<_, String>(0))?;
    crate::db::collect_rows(rows)
}

/// List all (task_id, label) pairs (bulk load for export).
pub fn list_all_labels(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt =
        conn.prepare("SELECT task_id, label FROM task_labels ORDER BY task_id, label")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    crate::db::collect_rows(rows)
}

/// Summary of a label: name, count, and associated task IDs.
#[derive(Debug, Clone)]
pub struct LabelSummary {
    pub label: String,
    pub count: usize,
    pub task_ids: Vec<String>,
}

/// Get all labels with counts and associated task IDs, sorted by count descending.
pub fn label_summary(conn: &Connection) -> Result<Vec<LabelSummary>> {
    let mut stmt = conn.prepare(
        "SELECT label, COUNT(*) as cnt, GROUP_CONCAT(task_id) as task_ids \
         FROM task_labels GROUP BY label ORDER BY cnt DESC, label ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        let label: String = row.get(0)?;
        let count: usize = row.get::<_, i64>(1)? as usize;
        let task_ids_str: String = row.get(2)?;
        let task_ids: Vec<String> = task_ids_str.split(',').map(|s| s.to_string()).collect();
        Ok(LabelSummary {
            label,
            count,
            task_ids,
        })
    })?;
    crate::db::collect_rows(rows)
}
