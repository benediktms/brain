use rusqlite::{Connection, OptionalExtension};

use crate::error::Result;
use crate::tasks::events::TaskStatus;

use super::ANCESTOR_BLOCKED_CTE;

/// Summary of a task's dependency state.
#[derive(Debug, Clone, Default)]
pub struct DependencySummary {
    pub total_deps: usize,
    pub done_deps: usize,
    pub blocking_task_ids: Vec<String>,
}

/// Get the dependency summary for a task.
pub fn get_dependency_summary(conn: &Connection, task_id: &str) -> Result<DependencySummary> {
    let mut stmt = conn.prepare(
        "SELECT d.depends_on, t.status
         FROM task_deps d
         JOIN tasks t ON t.task_id = d.depends_on
         WHERE d.task_id = ?1",
    )?;

    let mut total_deps = 0;
    let mut done_deps = 0;
    let mut blocking_task_ids = Vec::new();

    let rows = stmt.query_map([task_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;

    for row in rows {
        let (dep_id, status) = row?;
        total_deps += 1;
        if status == TaskStatus::Done.as_ref() || status == TaskStatus::Cancelled.as_ref() {
            done_deps += 1;
        } else {
            blocking_task_ids.push(dep_id);
        }
    }

    Ok(DependencySummary {
        total_deps,
        done_deps,
        blocking_task_ids,
    })
}

/// A linked note for a task.
#[derive(Debug, Clone)]
pub struct TaskNoteLink {
    pub chunk_id: String,
    pub file_path: String,
}

/// Get note links for a task, resolving chunk_id to file_path.
pub fn get_task_note_links(conn: &Connection, task_id: &str) -> Result<Vec<TaskNoteLink>> {
    let mut stmt = conn.prepare(
        "SELECT l.chunk_id, COALESCE(f.path, '') as file_path
         FROM task_note_links l
         LEFT JOIN chunks c ON c.chunk_id = l.chunk_id
         LEFT JOIN files f ON f.file_id = c.file_id
         WHERE l.task_id = ?1",
    )?;

    let rows = stmt.query_map([task_id], |row| {
        Ok(TaskNoteLink {
            chunk_id: row.get(0)?,
            file_path: row.get(1)?,
        })
    })?;

    crate::db::collect_rows(rows)
}

/// Per-status task counts for project health overview.
#[derive(Debug, Clone, Default)]
pub struct StatusCounts {
    pub open: usize,
    pub in_progress: usize,
    pub blocked: usize,
    pub done: usize,
    pub cancelled: usize,
}

impl StatusCounts {
    pub fn total(&self) -> usize {
        self.open + self.in_progress + self.blocked + self.done + self.cancelled
    }
}

/// Count tasks grouped by status.
pub fn count_by_status(conn: &Connection) -> Result<StatusCounts> {
    let mut stmt = conn.prepare("SELECT status, COUNT(*) FROM tasks GROUP BY status")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;

    let mut counts = StatusCounts::default();
    for row in rows {
        let (status, count) = row?;
        let count = count as usize;
        match status.as_str() {
            "open" => counts.open = count,
            "in_progress" => counts.in_progress = count,
            "blocked" => counts.blocked = count,
            "done" => counts.done = count,
            "cancelled" => counts.cancelled = count,
            _ => {}
        }
    }
    Ok(counts)
}

/// Count of ready and blocked tasks (for response metadata).
pub fn count_ready_blocked(conn: &Connection, brain_id: Option<&str>) -> Result<(usize, usize)> {
    let (brain_clause, brain_params) = super::listing::brain_id_filter(brain_id);
    let ready_sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT COUNT(*) FROM tasks t
         WHERE t.status IN ('open', 'in_progress')
           AND t.blocked_reason IS NULL
           AND (t.defer_until IS NULL OR t.defer_until <= strftime('%s', 'now'))
           AND NOT EXISTS (
               SELECT 1 FROM task_deps d
               JOIN tasks dep ON dep.task_id = d.depends_on
               WHERE d.task_id = t.task_id
                 AND dep.status NOT IN ('done', 'cancelled')
           )
           AND t.task_id NOT IN (SELECT tid FROM has_blocked_ancestor)
           {brain_clause}"
    );
    let ready: i64 = conn.query_row(
        &ready_sql,
        rusqlite::params_from_iter(brain_params.iter()),
        |row| row.get(0),
    )?;

    let (brain_clause, brain_params) = super::listing::brain_id_filter(brain_id);
    let blocked_sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT COUNT(*) FROM tasks t
         WHERE t.status IN ('open', 'in_progress', 'blocked')
           AND (
               t.blocked_reason IS NOT NULL
               OR (t.defer_until IS NOT NULL AND t.defer_until > strftime('%s', 'now'))
               OR EXISTS (
                   SELECT 1 FROM task_deps d
                   JOIN tasks dep ON dep.task_id = d.depends_on
                   WHERE d.task_id = t.task_id
                     AND dep.status NOT IN ('done', 'cancelled')
               )
               OR t.task_id IN (SELECT tid FROM has_blocked_ancestor)
           )
           {brain_clause}"
    );
    let blocked: i64 = conn.query_row(
        &blocked_sql,
        rusqlite::params_from_iter(brain_params.iter()),
        |row| row.get(0),
    )?;

    Ok((ready as usize, blocked as usize))
}

/// An external ID reference for a task.
#[derive(Debug, Clone)]
pub struct ExternalIdRow {
    pub task_id: String,
    pub source: String,
    pub external_id: String,
    pub external_url: Option<String>,
    pub imported_at: i64,
}

/// Get external ID references for a task.
pub fn get_external_ids(conn: &Connection, task_id: &str) -> Result<Vec<ExternalIdRow>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, source, external_id, external_url, imported_at
         FROM task_external_ids WHERE task_id = ?1 ORDER BY source, external_id",
    )?;
    let rows = stmt.query_map([task_id], |row| {
        Ok(ExternalIdRow {
            task_id: row.get(0)?,
            source: row.get(1)?,
            external_id: row.get(2)?,
            external_url: row.get(3)?,
            imported_at: row.get(4)?,
        })
    })?;
    crate::db::collect_rows(rows)
}

/// Resolve an external ID to a brain task_id.
pub fn resolve_external_id(
    conn: &Connection,
    source: &str,
    external_id: &str,
) -> Result<Option<String>> {
    let result = conn
        .query_row(
            "SELECT task_id FROM task_external_ids WHERE source = ?1 AND external_id = ?2",
            rusqlite::params![source, external_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(result)
}

/// A comment on a task.
#[derive(Debug, Clone)]
pub struct TaskComment {
    pub comment_id: String,
    pub author: String,
    pub body: String,
    pub created_at: i64,
}

/// Get comments for a task, ordered by creation time.
pub fn get_task_comments(conn: &Connection, task_id: &str) -> Result<Vec<TaskComment>> {
    let mut stmt = conn.prepare(
        "SELECT comment_id, author, body, created_at
         FROM task_comments WHERE task_id = ?1 ORDER BY created_at ASC",
    )?;
    let rows = stmt.query_map([task_id], |row| {
        Ok(TaskComment {
            comment_id: row.get(0)?,
            author: row.get(1)?,
            body: row.get(2)?,
            created_at: row.get(3)?,
        })
    })?;
    crate::db::collect_rows(rows)
}

/// A dependency edge between two tasks.
#[derive(Debug, Clone)]
pub struct TaskDep {
    pub task_id: String,
    pub depends_on: String,
}

/// List all dependency edges (bulk load for export).
pub fn list_all_deps(conn: &Connection) -> Result<Vec<TaskDep>> {
    let mut stmt = conn.prepare("SELECT task_id, depends_on FROM task_deps")?;
    let rows = stmt.query_map([], |row| {
        Ok(TaskDep {
            task_id: row.get(0)?,
            depends_on: row.get(1)?,
        })
    })?;
    crate::db::collect_rows(rows)
}

/// Get all dependency targets for a task (what it depends on).
pub fn get_deps_for_task(conn: &Connection, task_id: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT depends_on FROM task_deps WHERE task_id = ?1")?;
    let rows = stmt.query_map([task_id], |row| row.get::<_, String>(0))?;
    crate::db::collect_rows(rows)
}
