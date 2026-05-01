use rusqlite::{Connection, OptionalExtension};

use crate::error::Result;

use super::{ANCESTOR_BLOCKED_CTE, TASK_COLUMNS, TASK_COLUMNS_T, TaskRow, row_to_task};

/// Build a brain_id filter clause and push the param value.
///
/// Returns `(clause, params)` where `clause` is either `""` (all brains)
/// or `" AND t.brain_id = ?"` / `" WHERE brain_id = ?"` depending on the
/// `prefix` argument, and `params` is a `Vec<String>` with 0 or 1 element.
pub(super) fn brain_id_filter(brain_id: Option<&str>) -> (String, Vec<String>) {
    match brain_id {
        Some(id) if !id.is_empty() => (" AND t.brain_id = ?".to_string(), vec![id.to_string()]),
        _ => (String::new(), vec![]),
    }
}

/// Build a brain_id filter for queries that use bare table alias (no `t.`).
fn brain_id_filter_bare(brain_id: Option<&str>) -> (String, Vec<String>) {
    match brain_id {
        Some(id) if !id.is_empty() => (" AND brain_id = ?".to_string(), vec![id.to_string()]),
        _ => (String::new(), vec![]),
    }
}

/// List tasks that are ready to work on: open/in_progress, no blocked_reason,
/// and all dependencies are done or cancelled.
///
/// Ordered by priority ASC, epics first within tier, due_ts ASC NULLS LAST, updated_at DESC.
///
/// Cross-brain model: all brains share one `tasks` table (single-DB). A dependency
/// on a task from another brain is satisfied by the same JOIN — no per-brain routing
/// is needed. If `depends_on` references a task ID that does not exist at all (orphaned
/// dep, unregistered brain), the LEFT JOIN produces a NULL row which is treated as
/// still-blocking so the depending task is never mis-classified as ready.
///
/// External-blocker model: a row in `task_external_ids` with `blocking = 1` and
/// `resolved_at IS NULL` keeps the task out of the ready list. The clause sits
/// alongside the dep clause, not inside it — readiness requires both no
/// unresolved internal deps AND no unresolved external blockers.
pub fn list_ready(conn: &Connection, brain_id: Option<&str>) -> Result<Vec<TaskRow>> {
    let (brain_clause, brain_params) = brain_id_filter(brain_id);
    let sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT {TASK_COLUMNS}
         FROM tasks t
         WHERE t.status IN ('open', 'in_progress')
           AND t.blocked_reason IS NULL
           AND (t.defer_until IS NULL OR t.defer_until <= strftime('%s', 'now'))
           AND NOT EXISTS (
               SELECT 1 FROM entity_links el
               LEFT JOIN tasks dep ON dep.task_id = el.to_id
               WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='blocks'
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
           {brain_clause}
         ORDER BY t.priority ASC,
                  CASE WHEN t.task_type = 'epic' THEN 0 ELSE 1 END ASC,
                  t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map(rusqlite::params_from_iter(brain_params.iter()), row_to_task)?;
    crate::db::collect_rows(rows)
}

/// Like `list_ready` but excludes epics — returns only actionable work items.
/// Used by `tasks.next` so epics don't occupy top-k slots.
///
/// External-blocker semantics match `list_ready`: tasks with an unresolved
/// blocking external_id are excluded.
pub fn list_ready_actionable(conn: &Connection, brain_id: Option<&str>) -> Result<Vec<TaskRow>> {
    let (brain_clause, brain_params) = brain_id_filter(brain_id);
    let sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT {TASK_COLUMNS}
         FROM tasks t
         WHERE t.status IN ('open', 'in_progress')
           AND t.blocked_reason IS NULL
           AND t.task_type != 'epic'
           AND (t.defer_until IS NULL OR t.defer_until <= strftime('%s', 'now'))
           AND NOT EXISTS (
               SELECT 1 FROM entity_links el
               LEFT JOIN tasks dep ON dep.task_id = el.to_id
               WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='blocks'
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
           {brain_clause}
         ORDER BY CASE WHEN t.status = 'in_progress' THEN 0 ELSE 1 END ASC,
                  t.priority ASC,
                  t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map(rusqlite::params_from_iter(brain_params.iter()), row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List tasks that are blocked: have unresolved deps, an unresolved external
/// blocker, an explicit blocked_reason, a future defer_until, or a blocked
/// ancestor.
///
/// An orphaned dep (depends_on references a non-existent task) counts as blocking;
/// the LEFT JOIN + NULL check ensures such tasks appear here rather than in the
/// ready list.
///
/// A task whose only blocker is an unresolved external blocker is included
/// here via the parallel `EXISTS` clause on `task_external_ids`.
pub fn list_blocked(conn: &Connection, brain_id: Option<&str>) -> Result<Vec<TaskRow>> {
    let (brain_clause, brain_params) = brain_id_filter(brain_id);
    let sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT {TASK_COLUMNS}
         FROM tasks t
         WHERE t.status IN ('open', 'in_progress', 'blocked')
           AND (
               t.blocked_reason IS NOT NULL
               OR (t.defer_until IS NOT NULL AND t.defer_until > strftime('%s', 'now'))
               OR EXISTS (
                   SELECT 1 FROM entity_links el
                   LEFT JOIN tasks dep ON dep.task_id = el.to_id
                   WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='blocks'
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
           {brain_clause}
         ORDER BY t.priority ASC,
                  CASE WHEN t.task_type = 'epic' THEN 0 ELSE 1 END ASC,
                  t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map(rusqlite::params_from_iter(brain_params.iter()), row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List all tasks.
pub fn list_all(conn: &Connection, brain_id: Option<&str>) -> Result<Vec<TaskRow>> {
    let (brain_clause, brain_params) = brain_id_filter_bare(brain_id);
    let where_clause = if brain_clause.is_empty() {
        String::new()
    } else {
        format!("WHERE 1=1{brain_clause}")
    };
    let sql = format!(
        "SELECT {TASK_COLUMNS}
         FROM tasks
         {where_clause}
         ORDER BY priority ASC,
                  CASE WHEN task_type = 'epic' THEN 0 ELSE 1 END ASC,
                  due_ts ASC NULLS LAST, updated_at DESC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map(rusqlite::params_from_iter(brain_params.iter()), row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List open tasks (excludes done/cancelled).
pub fn list_open(conn: &Connection, brain_id: Option<&str>) -> Result<Vec<TaskRow>> {
    let (brain_clause, brain_params) = brain_id_filter_bare(brain_id);
    let sql = format!(
        "SELECT {TASK_COLUMNS}
         FROM tasks
         WHERE status IN ('open', 'in_progress', 'blocked'){brain_clause}
         ORDER BY priority ASC,
                  CASE WHEN task_type = 'epic' THEN 0 ELSE 1 END ASC,
                  due_ts ASC NULLS LAST, updated_at DESC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map(rusqlite::params_from_iter(brain_params.iter()), row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List done/cancelled tasks, most recently updated first.
pub fn list_done(conn: &Connection, brain_id: Option<&str>) -> Result<Vec<TaskRow>> {
    let (brain_clause, brain_params) = brain_id_filter_bare(brain_id);
    let sql = format!(
        "SELECT {TASK_COLUMNS}
         FROM tasks
         WHERE status IN ('done', 'cancelled'){brain_clause}
         ORDER BY updated_at DESC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map(rusqlite::params_from_iter(brain_params.iter()), row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List tasks with status exactly 'in_progress'.
pub fn list_in_progress(conn: &Connection, brain_id: Option<&str>) -> Result<Vec<TaskRow>> {
    let (brain_clause, brain_params) = brain_id_filter_bare(brain_id);
    let sql = format!(
        "SELECT {TASK_COLUMNS}
         FROM tasks
         WHERE status = 'in_progress'{brain_clause}
         ORDER BY priority ASC,
                  CASE WHEN task_type = 'epic' THEN 0 ELSE 1 END ASC,
                  due_ts ASC NULLS LAST, updated_at DESC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(brain_params.iter()), row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List tasks with status exactly 'cancelled'.
pub fn list_cancelled(conn: &Connection, brain_id: Option<&str>) -> Result<Vec<TaskRow>> {
    let (brain_clause, brain_params) = brain_id_filter_bare(brain_id);
    let sql = format!(
        "SELECT {TASK_COLUMNS}
         FROM tasks
         WHERE status = 'cancelled'{brain_clause}
         ORDER BY updated_at DESC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(brain_params.iter()), row_to_task)?;
    crate::db::collect_rows(rows)
}

/// Get a single task by ID.
pub fn get_task(conn: &Connection, task_id: &str) -> Result<Option<TaskRow>> {
    let sql = format!("SELECT {TASK_COLUMNS} FROM tasks WHERE task_id = ?1");
    let result = conn.query_row(&sql, [task_id], row_to_task).optional()?;
    Ok(result)
}

/// List task IDs that became unblocked because `completed_task_id` was just
/// marked done or cancelled. Returns IDs of tasks that depended on the
/// completed task and now have all dependencies resolved.
///
/// Uses LEFT JOIN so that orphaned dep entries (depends_on references a missing
/// task) are treated as still-blocking — such tasks are excluded from the
/// unblocked set until the orphan is cleaned up.
///
/// A task whose only remaining blocker is an unresolved external blocker
/// is also excluded — the same NOT EXISTS clause used by `list_ready` is
/// applied here so cross-system blockers correctly suppress unblock events.
pub fn list_newly_unblocked(conn: &Connection, completed_task_id: &str) -> Result<Vec<String>> {
    let sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT el.from_id
         FROM entity_links el
         JOIN tasks t ON t.task_id = el.from_id
         WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='blocks' AND el.to_id = ?1
           AND t.status IN ('open', 'in_progress')
           AND t.blocked_reason IS NULL
           AND NOT EXISTS (
               SELECT 1 FROM entity_links el2
               LEFT JOIN tasks dep ON dep.task_id = el2.to_id
               WHERE el2.from_type='TASK' AND el2.to_type='TASK' AND el2.edge_kind='blocks'
                 AND el2.from_id = el.from_id
                 AND (dep.task_id IS NULL OR dep.status NOT IN ('done', 'cancelled'))
           )
           AND NOT EXISTS (
               SELECT 1 FROM task_external_ids x
               WHERE x.task_id = el.from_id
                 AND x.blocking = 1
                 AND x.resolved_at IS NULL
           )
           AND t.task_id NOT IN (SELECT tid FROM has_blocked_ancestor)"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map([completed_task_id], |row| row.get::<_, String>(0))?;
    crate::db::collect_rows(rows)
}

/// Get child tasks of a parent.
pub fn get_children(conn: &Connection, parent_task_id: &str) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS_T} FROM tasks t
         JOIN entity_links el ON el.to_id = t.task_id
         WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='parent_of'
           AND el.from_id = ?1
         ORDER BY t.child_seq ASC, t.priority ASC, t.created_at ASC, t.task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([parent_task_id], row_to_task)?;
    crate::db::collect_rows(rows)
}

/// Get tasks that depend on the given task and are not yet resolved (reverse deps).
pub fn get_tasks_blocking(conn: &Connection, task_id: &str) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS_T} FROM tasks t
         JOIN entity_links el ON el.from_id = t.task_id
         WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='blocks'
           AND el.to_id = ?1
           AND t.status NOT IN ('done', 'cancelled')
         ORDER BY t.priority ASC, t.task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([task_id], row_to_task)?;
    crate::db::collect_rows(rows)
}

/// Check if a task exists in the projection.
pub fn task_exists(conn: &Connection, task_id: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tasks WHERE task_id = ?1",
        [task_id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

// ---------------------------------------------------------------------------
// Embed-poll helpers
// ---------------------------------------------------------------------------

/// A task row for the embedding poll (minimal fields for capsule generation).
#[derive(Debug, Clone)]
pub struct TaskPollRow {
    pub task_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: i32,
    pub blocked_reason: Option<String>,
}

/// Find tasks that need (re-)embedding into LanceDB (limit 256).
pub fn find_stale_tasks_for_embedding(
    conn: &Connection,
    brain_id: &str,
) -> Result<Vec<TaskPollRow>> {
    let (sql, has_brain_filter) = if brain_id.is_empty() {
        (
            "SELECT task_id, title, description, status, priority, blocked_reason
             FROM tasks
             WHERE (updated_at > COALESCE(embedded_at, 0) OR embedded_at IS NULL)
             LIMIT 256"
                .to_string(),
            false,
        )
    } else {
        (
            "SELECT task_id, title, description, status, priority, blocked_reason
             FROM tasks
             WHERE (updated_at > COALESCE(embedded_at, 0) OR embedded_at IS NULL)
               AND brain_id = ?1
             LIMIT 256"
                .to_string(),
            true,
        )
    };

    let mut stmt = conn.prepare(&sql)?;

    let map_row = |row: &rusqlite::Row<'_>| {
        Ok(TaskPollRow {
            task_id: row.get(0)?,
            title: row.get(1)?,
            description: row.get(2)?,
            status: row.get(3)?,
            priority: row.get(4)?,
            blocked_reason: row.get(5)?,
        })
    };

    let rows = if has_brain_filter {
        stmt.query_map([brain_id], map_row)?
    } else {
        stmt.query_map([], map_row)?
    };
    crate::db::collect_rows(rows)
}
