use rusqlite::{Connection, OptionalExtension};

use crate::error::Result;

use super::{ANCESTOR_BLOCKED_CTE, TASK_COLUMNS, TaskRow, row_to_task};

/// List tasks that are ready to work on: open/in_progress, no blocked_reason,
/// and all dependencies are done or cancelled.
///
/// Ordered by priority ASC, epics first within tier, due_ts ASC NULLS LAST, updated_at DESC.
pub fn list_ready(conn: &Connection) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT {TASK_COLUMNS}
         FROM tasks t
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
         ORDER BY t.priority ASC,
                  CASE WHEN t.task_type = 'epic' THEN 0 ELSE 1 END ASC,
                  t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map([], row_to_task)?;
    crate::db::collect_rows(rows)
}

/// Like `list_ready` but excludes epics — returns only actionable work items.
/// Used by `tasks.next` so epics don't occupy top-k slots.
pub fn list_ready_actionable(conn: &Connection) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT {TASK_COLUMNS}
         FROM tasks t
         WHERE t.status IN ('open', 'in_progress')
           AND t.blocked_reason IS NULL
           AND t.task_type != 'epic'
           AND (t.defer_until IS NULL OR t.defer_until <= strftime('%s', 'now'))
           AND NOT EXISTS (
               SELECT 1 FROM task_deps d
               JOIN tasks dep ON dep.task_id = d.depends_on
               WHERE d.task_id = t.task_id
                 AND dep.status NOT IN ('done', 'cancelled')
           )
           AND t.task_id NOT IN (SELECT tid FROM has_blocked_ancestor)
         ORDER BY t.priority ASC, t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map([], row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List tasks that are blocked: have unresolved deps or an explicit blocked_reason.
pub fn list_blocked(conn: &Connection) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT {TASK_COLUMNS}
         FROM tasks t
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
         ORDER BY t.priority ASC,
                  CASE WHEN t.task_type = 'epic' THEN 0 ELSE 1 END ASC,
                  t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map([], row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List all tasks.
pub fn list_all(conn: &Connection) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS}
         FROM tasks
         ORDER BY priority ASC,
                  CASE WHEN task_type = 'epic' THEN 0 ELSE 1 END ASC,
                  due_ts ASC NULLS LAST, updated_at DESC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map([], row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List open tasks (excludes done/cancelled).
pub fn list_open(conn: &Connection) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS}
         FROM tasks
         WHERE status IN ('open', 'in_progress', 'blocked')
         ORDER BY priority ASC,
                  CASE WHEN task_type = 'epic' THEN 0 ELSE 1 END ASC,
                  due_ts ASC NULLS LAST, updated_at DESC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map([], row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List done/cancelled tasks, most recently updated first.
pub fn list_done(conn: &Connection) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS}
         FROM tasks
         WHERE status IN ('done', 'cancelled')
         ORDER BY updated_at DESC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map([], row_to_task)?;
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
pub fn list_newly_unblocked(conn: &Connection, completed_task_id: &str) -> Result<Vec<String>> {
    let sql = format!(
        "{ANCESTOR_BLOCKED_CTE}
         SELECT d.task_id
         FROM task_deps d
         JOIN tasks t ON t.task_id = d.task_id
         WHERE d.depends_on = ?1
           AND t.status IN ('open', 'in_progress')
           AND t.blocked_reason IS NULL
           AND NOT EXISTS (
               SELECT 1 FROM task_deps d2
               JOIN tasks dep ON dep.task_id = d2.depends_on
               WHERE d2.task_id = d.task_id
                 AND dep.status NOT IN ('done', 'cancelled')
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
        "SELECT {TASK_COLUMNS} FROM tasks WHERE parent_task_id = ?1
         ORDER BY priority ASC, created_at ASC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([parent_task_id], row_to_task)?;
    crate::db::collect_rows(rows)
}

/// Get tasks that depend on the given task and are not yet resolved (reverse deps).
pub fn get_tasks_blocking(conn: &Connection, task_id: &str) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS} FROM tasks
         WHERE task_id IN (
             SELECT d.task_id FROM task_deps d WHERE d.depends_on = ?1
         )
         AND status NOT IN ('done', 'cancelled')
         ORDER BY priority ASC, task_id ASC"
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
