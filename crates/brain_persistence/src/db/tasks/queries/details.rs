use rusqlite::{Connection, OptionalExtension};

use crate::db::tasks::events::TaskStatus;
use crate::error::Result;

use super::ANCESTOR_BLOCKED_CTE;

/// Summary of a task's dependency state. The two domains are kept disjoint
/// so each field has one source of truth:
/// - `total_deps` / `done_deps` / `blocking_task_ids` cover `entity_links` blocks edges.
/// - `external_blocker_unresolved_count` covers `task_external_ids` rows
///   with `blocking = 1 AND resolved_at IS NULL`.
///
/// Callers that want a unified "X of Y blocking" string compute
/// `(total_deps - done_deps) + external_blocker_unresolved_count`.
#[derive(Debug, Clone, Default)]
pub struct DependencySummary {
    pub total_deps: usize,
    pub done_deps: usize,
    pub blocking_task_ids: Vec<String>,
    pub external_blocker_unresolved_count: usize,
}

/// Get the dependency summary for a task.
///
/// `total_deps` / `done_deps` / `blocking_task_ids` count `entity_links` blocks
/// edges only. The LEFT JOIN keeps orphaned references (target task absent from
/// the DB — e.g. cross-brain task in an unregistered brain, or a deleted
/// dep target) blocking rather than silently dropping them.
///
/// External blockers (`task_external_ids` rows with `blocking = 1`) are
/// counted separately in `external_blocker_unresolved_count`. Resolved
/// blockers don't appear here at all — fetch `get_external_blockers` for
/// the resolved-history view.
pub fn get_dependency_summary(conn: &Connection, task_id: &str) -> Result<DependencySummary> {
    let mut stmt = conn.prepare(
        "SELECT el.to_id, t.status
         FROM entity_links el
         LEFT JOIN tasks t ON t.task_id = el.to_id
         WHERE el.from_type='TASK' AND el.from_id = ?1
           AND el.to_type='TASK' AND el.edge_kind='blocks'",
    )?;

    let mut total_deps = 0;
    let mut done_deps = 0;
    let mut blocking_task_ids = Vec::new();

    let rows = stmt.query_map([task_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;

    for row in rows {
        let (dep_id, status_opt) = row?;
        total_deps += 1;
        match status_opt.as_deref() {
            Some(s) if s == TaskStatus::Done.as_ref() || s == TaskStatus::Cancelled.as_ref() => {
                done_deps += 1;
            }
            _ => {
                // None means orphaned dep (target not in DB) — counts as blocking.
                blocking_task_ids.push(dep_id);
            }
        }
    }

    let ext_unresolved: i64 = conn.query_row(
        "SELECT COUNT(*) FROM task_external_ids
         WHERE task_id = ?1 AND blocking = 1 AND resolved_at IS NULL",
        [task_id],
        |row| row.get(0),
    )?;

    Ok(DependencySummary {
        total_deps,
        done_deps,
        blocking_task_ids,
        external_blocker_unresolved_count: ext_unresolved as usize,
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
///
/// Mirrors the LEFT JOIN semantics from `list_ready` / `list_blocked`: orphaned
/// dep entries (depends_on references a missing task) count as blocking. The
/// external-blocker NOT EXISTS clause matches the listing queries so the
/// counts agree with `list_ready` / `list_blocked` row counts.
pub fn count_ready_blocked(conn: &Connection, brain_id: Option<&str>) -> Result<(usize, usize)> {
    let (brain_clause, brain_params) = super::listing::brain_id_filter(brain_id);
    let ready_sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT COUNT(*) FROM tasks t
         WHERE t.status IN ('open', 'in_progress')
           AND t.blocked_reason IS NULL
           AND (t.defer_until IS NULL OR t.defer_until <= strftime('%s', 'now'))
           AND NOT EXISTS (
               SELECT 1 FROM entity_links el
               LEFT JOIN tasks dep ON dep.task_id = el.to_id
               WHERE el.from_type = 'TASK' AND el.to_type = 'TASK'
                 AND el.edge_kind = 'blocks'
                 AND el.from_id = t.task_id
                 AND (dep.task_id IS NULL OR dep.status NOT IN ('done', 'cancelled'))
           )
           AND NOT EXISTS (
               SELECT 1 FROM task_external_ids x
               WHERE x.task_id = t.task_id
                 AND x.blocking = 1
                 AND x.resolved_at IS NULL
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
                   SELECT 1 FROM entity_links el
                   LEFT JOIN tasks dep ON dep.task_id = el.to_id
                   WHERE el.from_type = 'TASK' AND el.to_type = 'TASK'
                     AND el.edge_kind = 'blocks'
                     AND el.from_id = t.task_id
                     AND (dep.task_id IS NULL OR dep.status NOT IN ('done', 'cancelled'))
               )
               OR EXISTS (
                   SELECT 1 FROM task_external_ids x
                   WHERE x.task_id = t.task_id
                     AND x.blocking = 1
                     AND x.resolved_at IS NULL
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
///
/// `blocking = true` rows act as first-class blockers; `false` rows are pure
/// metadata (the historical pre-v45 behavior). `resolved_at` is `Some(ts)`
/// once the external system reports completion; `None` means still active.
#[derive(Debug, Clone)]
pub struct ExternalIdRow {
    pub task_id: String,
    pub source: String,
    pub external_id: String,
    pub external_url: Option<String>,
    pub imported_at: i64,
    pub blocking: bool,
    pub resolved_at: Option<i64>,
}

/// Get external ID references for a task.
///
/// Returns ALL `task_external_ids` rows (both metadata-only and blocking).
/// Callers wanting only the blocker subset should use `get_external_blockers`.
pub fn get_external_ids(conn: &Connection, task_id: &str) -> Result<Vec<ExternalIdRow>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, source, external_id, external_url, imported_at, blocking, resolved_at
         FROM task_external_ids WHERE task_id = ?1 ORDER BY source, external_id",
    )?;
    let rows = stmt.query_map([task_id], |row| {
        Ok(ExternalIdRow {
            task_id: row.get(0)?,
            source: row.get(1)?,
            external_id: row.get(2)?,
            external_url: row.get(3)?,
            imported_at: row.get(4)?,
            blocking: row.get::<_, i64>(5)? != 0,
            resolved_at: row.get(6)?,
        })
    })?;
    crate::db::collect_rows(rows)
}

/// Get external blocker rows for a task (`blocking = 1` only).
///
/// Returns both unresolved (`resolved_at IS NULL`) and resolved (timestamp)
/// blockers so callers can render history. The `tasks.get` MCP response
/// surfaces this list as `external_blockers`, distinct from `external_ids`
/// which still includes pure-metadata rows.
pub fn get_external_blockers(conn: &Connection, task_id: &str) -> Result<Vec<ExternalIdRow>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, source, external_id, external_url, imported_at, blocking, resolved_at
         FROM task_external_ids
         WHERE task_id = ?1 AND blocking = 1
         ORDER BY resolved_at IS NOT NULL ASC, source, external_id",
    )?;
    let rows = stmt.query_map([task_id], |row| {
        Ok(ExternalIdRow {
            task_id: row.get(0)?,
            source: row.get(1)?,
            external_id: row.get(2)?,
            external_url: row.get(3)?,
            imported_at: row.get(4)?,
            blocking: row.get::<_, i64>(5)? != 0,
            resolved_at: row.get(6)?,
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
    let mut stmt = conn.prepare(
        "SELECT from_id, to_id FROM entity_links
         WHERE from_type='TASK' AND to_type='TASK' AND edge_kind='blocks'",
    )?;
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
    let mut stmt = conn.prepare(
        "SELECT to_id FROM entity_links
         WHERE from_type='TASK' AND from_id=?1 AND to_type='TASK' AND edge_kind='blocks'",
    )?;
    let rows = stmt.query_map([task_id], |row| row.get::<_, String>(0))?;
    crate::db::collect_rows(rows)
}
