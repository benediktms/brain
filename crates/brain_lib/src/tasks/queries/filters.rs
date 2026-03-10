use std::collections::{HashMap, HashSet};

use rusqlite::Connection;

use crate::error::Result;
use crate::tasks::events::TaskType;

use super::TaskRow;

// ── Per-field filtering + FTS ─────────────────────────────────────────

/// Criteria for filtering tasks beyond the base status query.
#[derive(Debug, Default)]
pub struct TaskFilter {
    pub priority: Option<i32>,
    pub task_type: Option<TaskType>,
    pub assignee: Option<String>,
    pub label: Option<String>,
    pub search: Option<String>,
}

impl TaskFilter {
    pub fn is_empty(&self) -> bool {
        self.priority.is_none()
            && self.task_type.is_none()
            && self.assignee.is_none()
            && self.label.is_none()
            && self.search.is_none()
    }
}

/// Apply in-memory filters to a list of tasks.
///
/// - `fts_ids`: if a search was performed, the set of matching task_ids from FTS.
/// - `labels_map`: pre-fetched label map for label filtering.
pub fn apply_filters(
    tasks: Vec<TaskRow>,
    filter: &TaskFilter,
    fts_ids: Option<&HashSet<String>>,
    labels_map: Option<&HashMap<String, Vec<String>>>,
) -> Vec<TaskRow> {
    tasks
        .into_iter()
        .filter(|t| {
            if let Some(fts) = fts_ids
                && !fts.contains(&t.task_id)
            {
                return false;
            }
            if let Some(p) = filter.priority
                && t.priority != p
            {
                return false;
            }
            if let Some(tt) = filter.task_type
                && t.task_type != tt
            {
                return false;
            }
            if let Some(ref a) = filter.assignee
                && t.assignee.as_deref() != Some(a.as_str())
            {
                return false;
            }
            if let Some(ref label) = filter.label {
                let has_label = labels_map
                    .and_then(|m| m.get(&t.task_id))
                    .is_some_and(|labels| labels.iter().any(|l| l == label));
                if !has_label {
                    return false;
                }
            }
            true
        })
        .collect()
}

/// Full-text search on task title and description via FTS5.
/// Returns matching task_ids ordered by relevance.
pub fn search_tasks_fts(conn: &Connection, query: &str, limit: usize) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT t.task_id
         FROM fts_tasks
         JOIN tasks t ON t.rowid = fts_tasks.rowid
         WHERE fts_tasks MATCH ?1
         ORDER BY rank
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![query, limit as i64], |row| row.get(0))?;
    crate::db::collect_rows(rows)
}
