use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use crate::db::meta;
use crate::error::{BrainCoreError, Result};

use super::events::TaskStatus;

/// A row from the tasks projection table.
#[derive(Debug, Clone)]
pub struct TaskRow {
    pub task_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: i32,
    pub blocked_reason: Option<String>,
    pub due_ts: Option<i64>,
    pub task_type: String,
    pub assignee: Option<String>,
    pub defer_until: Option<i64>,
    pub parent_task_id: Option<String>,
    pub child_seq: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

const TASK_COLUMNS: &str = "task_id, title, description, status, priority, blocked_reason, due_ts, \
     task_type, assignee, defer_until, parent_task_id, child_seq, created_at, updated_at";

fn row_to_task(row: &rusqlite::Row) -> rusqlite::Result<TaskRow> {
    Ok(TaskRow {
        task_id: row.get(0)?,
        title: row.get(1)?,
        description: row.get(2)?,
        status: row.get(3)?,
        priority: row.get(4)?,
        blocked_reason: row.get(5)?,
        due_ts: row.get(6)?,
        task_type: row.get(7)?,
        assignee: row.get(8)?,
        defer_until: row.get(9)?,
        parent_task_id: row.get(10)?,
        child_seq: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

/// List tasks that are ready to work on: open/in_progress, no blocked_reason,
/// and all dependencies are done or cancelled.
///
/// Ordered by priority ASC, due_ts ASC NULLS LAST, updated_at DESC.
pub fn list_ready(conn: &Connection) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS}
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
         ORDER BY t.priority ASC, t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map([], row_to_task)?;
    crate::db::collect_rows(rows)
}

/// List tasks that are blocked: have unresolved deps or an explicit blocked_reason.
pub fn list_blocked(conn: &Connection) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS}
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
           )
         ORDER BY t.priority ASC, t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC"
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
         ORDER BY priority ASC, due_ts ASC NULLS LAST, updated_at DESC, task_id ASC"
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
    // Find tasks that depend on the completed task, are open/in_progress,
    // have no blocked_reason, and all of their deps are now done/cancelled.
    let mut stmt = conn.prepare(
        "SELECT d.task_id
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
           )",
    )?;

    let rows = stmt.query_map([completed_task_id], |row| row.get::<_, String>(0))?;
    crate::db::collect_rows(rows)
}

/// Summary of a task's dependency state.
#[derive(Debug, Clone)]
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
pub fn count_ready_blocked(conn: &Connection) -> Result<(usize, usize)> {
    let ready: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tasks t
         WHERE t.status IN ('open', 'in_progress')
           AND t.blocked_reason IS NULL
           AND (t.defer_until IS NULL OR t.defer_until <= strftime('%s', 'now'))
           AND NOT EXISTS (
               SELECT 1 FROM task_deps d
               JOIN tasks dep ON dep.task_id = d.depends_on
               WHERE d.task_id = t.task_id
                 AND dep.status NOT IN ('done', 'cancelled')
           )",
        [],
        |row| row.get(0),
    )?;

    let blocked: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tasks t
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
           )",
        [],
        |row| row.get(0),
    )?;

    Ok((ready as usize, blocked as usize))
}

/// Get labels for a task.
pub fn get_task_labels(conn: &Connection, task_id: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT label FROM task_labels WHERE task_id = ?1 ORDER BY label")?;
    let rows = stmt.query_map([task_id], |row| row.get::<_, String>(0))?;
    crate::db::collect_rows(rows)
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

/// List all (task_id, label) pairs (bulk load for export).
pub fn list_all_labels(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt =
        conn.prepare("SELECT task_id, label FROM task_labels ORDER BY task_id, label")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
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

// -- Prefix resolution -------------------------------------------------------

/// Minimum ULID prefix length (after project prefix + separator).
const MIN_ULID_PREFIX_LEN: usize = 4;

/// Minimum display prefix: "BRN-" (4) + 4 ULID chars = 8.
const MIN_DISPLAY_PREFIX_LEN: usize = 8;

/// Resolve a task ID from an exact match or unique prefix.
///
/// Accepts: full ID, prefix with project prefix ("BRN-01JPH"),
/// or bare ULID prefix ("01JPH") — auto-prepends project prefix.
/// Min prefix length: 4 chars (after any project prefix).
/// Get the next child_seq for a parent task (max existing + 1, or 1 if no children).
pub fn next_child_seq(conn: &Connection, parent_task_id: &str) -> Result<i64> {
    let max: Option<i64> = conn
        .query_row(
            "SELECT MAX(child_seq) FROM tasks WHERE parent_task_id = ?1",
            [parent_task_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    Ok(max.unwrap_or(0) + 1)
}

pub fn resolve_task_id(conn: &Connection, input: &str) -> Result<String> {
    // Fast path: exact match
    if task_exists(conn, input)? {
        return Ok(input.to_string());
    }

    // Check for hierarchical display ID: "PREFIX.N" where N is child_seq
    if let Some(dot_pos) = input.rfind('.') {
        let parent_part = &input[..dot_pos];
        let seq_part = &input[dot_pos + 1..];
        if let Ok(seq) = seq_part.parse::<i64>() {
            // Resolve the parent prefix first (recursive)
            if let Ok(parent_id) = resolve_task_id(conn, parent_part) {
                let child: Option<String> = conn
                    .query_row(
                        "SELECT task_id FROM tasks WHERE parent_task_id = ?1 AND child_seq = ?2",
                        rusqlite::params![parent_id, seq],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(child_id) = child {
                    return Ok(child_id);
                }
            }
        }
    }

    let normalized = input.to_ascii_uppercase();

    // Determine if this looks like a prefixed ID (has a dash after position 0)
    // or a bare ULID prefix. Legacy UUIDs also have dashes but at position 8.
    let search_prefix = match normalized.find('-') {
        Some(dash_pos) if dash_pos <= 4 => {
            // Looks like a project prefix (1-4 chars before dash), e.g. "BRN-01JPH..."
            let ulid_part = &normalized[dash_pos + 1..];
            if ulid_part.len() < MIN_ULID_PREFIX_LEN {
                return Err(BrainCoreError::TaskEvent(format!(
                    "prefix too short: need at least {MIN_ULID_PREFIX_LEN} characters after '{}'",
                    &normalized[..=dash_pos]
                )));
            }
            normalized
        }
        Some(_) => {
            // Legacy UUID format (dash at position 8, e.g. "019571A8-...") — search as-is
            normalized
        }
        None => {
            // No dash — bare ULID prefix, auto-prepend project prefix
            if normalized.len() < MIN_ULID_PREFIX_LEN {
                return Err(BrainCoreError::TaskEvent(format!(
                    "prefix too short: need at least {MIN_ULID_PREFIX_LEN} characters, got {}",
                    normalized.len()
                )));
            }
            let prefix =
                meta::get_meta(conn, "project_prefix")?.unwrap_or_else(|| "BRN".to_string());
            format!("{prefix}-{normalized}")
        }
    };

    // Range scan on PRIMARY KEY B-tree
    let upper_bound = increment_string(&search_prefix);
    let mut stmt =
        conn.prepare("SELECT task_id FROM tasks WHERE task_id >= ?1 AND task_id < ?2")?;
    let matches: Vec<String> = stmt
        .query_map(rusqlite::params![search_prefix, upper_bound], |row| {
            row.get(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    match matches.len() {
        0 => Err(BrainCoreError::TaskEvent(format!(
            "no task found matching prefix: {input}"
        ))),
        1 => Ok(matches.into_iter().next().unwrap()),
        n => Err(BrainCoreError::TaskEvent(format!(
            "ambiguous prefix '{input}': matches {n} tasks"
        ))),
    }
}

/// Compute the shortest unique prefix for a single task ID.
///
/// Uses two O(log n) index seeks (predecessor + successor) instead of loading
/// all task IDs. Preferred over `shortest_unique_prefixes()` when displaying
/// a single task (e.g. `tasks.get` responses).
pub fn shortest_unique_prefix(conn: &Connection, task_id: &str) -> Result<String> {
    let prev: Option<String> = conn
        .query_row(
            "SELECT task_id FROM tasks WHERE task_id < ?1 ORDER BY task_id DESC LIMIT 1",
            [task_id],
            |row| row.get(0),
        )
        .optional()?;
    let next: Option<String> = conn
        .query_row(
            "SELECT task_id FROM tasks WHERE task_id > ?1 ORDER BY task_id ASC LIMIT 1",
            [task_id],
            |row| row.get(0),
        )
        .optional()?;

    let min_prev = prev
        .as_deref()
        .map(|p| common_prefix_len(task_id, p) + 1)
        .unwrap_or(1);
    let min_next = next
        .as_deref()
        .map(|n| common_prefix_len(task_id, n) + 1)
        .unwrap_or(1);

    let min_len = min_prev
        .max(min_next)
        .max(MIN_DISPLAY_PREFIX_LEN)
        .min(task_id.len());

    Ok(task_id[..min_len].to_string())
}

/// Compute shortest unique prefixes for all tasks (batch, for list display).
///
/// Loads all IDs sorted, compares neighbors. O(n log n).
/// The prefix portion (e.g. "BRN-") is always shown in full; only the ULID
/// portion gets truncated.
pub fn shortest_unique_prefixes(conn: &Connection) -> Result<HashMap<String, String>> {
    let mut stmt = conn.prepare("SELECT task_id FROM tasks ORDER BY task_id")?;
    let ids: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut result = HashMap::new();
    let n = ids.len();

    for i in 0..n {
        let id = &ids[i];
        let prev = if i > 0 {
            Some(ids[i - 1].as_str())
        } else {
            None
        };
        let next = if i + 1 < n {
            Some(ids[i + 1].as_str())
        } else {
            None
        };

        // Find the minimum length to distinguish from both neighbors
        let min_len_prev = prev.map(|p| common_prefix_len(id, p) + 1).unwrap_or(1);
        let min_len_next = next.map(|nx| common_prefix_len(id, nx) + 1).unwrap_or(1);

        let min_len = min_len_prev.max(min_len_next).max(MIN_DISPLAY_PREFIX_LEN);
        let prefix_len = min_len.min(id.len());

        result.insert(id.clone(), id[..prefix_len].to_string());
    }

    Ok(result)
}

/// Increment the last byte of a string for exclusive upper bounds in range scans.
///
/// Example: `"BRN-01JP"` → `"BRN-01JQ"`
///
/// Precondition: `s` must be ASCII (ULID chars are Crockford Base32 `0-9A-Z`,
/// project prefixes are `A-Z`, and legacy UUIDs are `0-9a-f-`). All bytes are
/// in the `0x00..0x7E` range so incrementing always produces valid UTF-8.
/// If a non-ASCII byte is encountered, the fallback appends `\u{FFFF}`.
fn increment_string(s: &str) -> String {
    debug_assert!(s.is_ascii(), "increment_string expects ASCII input");
    let mut bytes = s.as_bytes().to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xFF {
            bytes[i] += 1;
            return String::from_utf8(bytes).unwrap_or_else(|_| format!("{s}\u{FFFF}"));
        }
        bytes[i] = 0;
    }
    // All 0xFF — append a high character as upper bound
    format!("{s}\u{FFFF}")
}

/// Length of the common byte prefix between two strings.
///
/// Uses byte comparison, which is correct and safe for ASCII strings (ULIDs,
/// project prefixes, UUIDs). For non-ASCII task IDs, this still returns a
/// valid byte offset since it only counts matching bytes.
fn common_prefix_len(a: &str, b: &str) -> usize {
    a.bytes()
        .zip(b.bytes())
        .take_while(|(ba, bb)| ba == bb)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;
    use crate::tasks::events::*;
    use crate::tasks::projections::apply_event;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn create_task(conn: &Connection, task_id: &str, title: &str, priority: i32) {
        let ev = TaskEvent::from_payload(
            task_id,
            "user",
            TaskCreatedPayload {
                title: title.to_string(),
                description: None,
                priority,
                status: TaskStatus::Open,
                due_ts: None,
                task_type: None,
                assignee: None,
                defer_until: None,
                parent_task_id: None,
            },
        );
        apply_event(conn, &ev).unwrap();
    }

    fn set_status(conn: &Connection, task_id: &str, status: &str) {
        let ev = TaskEvent::from_payload(
            task_id,
            "user",
            StatusChangedPayload {
                new_status: status.parse().unwrap(),
            },
        );
        apply_event(conn, &ev).unwrap();
    }

    fn add_dep(conn: &Connection, task_id: &str, depends_on: &str) {
        let ev = TaskEvent::new(
            task_id,
            "user",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: depends_on.to_string(),
            },
        );
        apply_event(conn, &ev).unwrap();
    }

    #[test]
    fn test_list_ready_no_deps() {
        let conn = setup();
        create_task(&conn, "t1", "Task 1", 2);
        create_task(&conn, "t2", "Task 2", 1);

        let ready = list_ready(&conn).unwrap();
        assert_eq!(ready.len(), 2);
        // Lower priority number first
        assert_eq!(ready[0].task_id, "t2");
        assert_eq!(ready[1].task_id, "t1");
    }

    #[test]
    fn test_list_ready_excludes_blocked_by_dep() {
        let conn = setup();
        create_task(&conn, "t1", "Blocker", 2);
        create_task(&conn, "t2", "Blocked", 1);
        add_dep(&conn, "t2", "t1");

        let ready = list_ready(&conn).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, "t1");
    }

    #[test]
    fn test_list_ready_unblocks_when_dep_done() {
        let conn = setup();
        create_task(&conn, "t1", "Blocker", 2);
        create_task(&conn, "t2", "Was Blocked", 1);
        add_dep(&conn, "t2", "t1");

        // Complete the blocker
        set_status(&conn, "t1", "done");

        let ready = list_ready(&conn).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, "t2");
    }

    #[test]
    fn test_list_ready_unblocks_when_dep_cancelled() {
        let conn = setup();
        create_task(&conn, "t1", "Blocker", 2);
        create_task(&conn, "t2", "Was Blocked", 1);
        add_dep(&conn, "t2", "t1");

        set_status(&conn, "t1", "cancelled");

        let ready = list_ready(&conn).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, "t2");
    }

    #[test]
    fn test_list_blocked() {
        let conn = setup();
        create_task(&conn, "t1", "Blocker", 2);
        create_task(&conn, "t2", "Blocked by dep", 1);
        add_dep(&conn, "t2", "t1");

        let blocked = list_blocked(&conn).unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].task_id, "t2");
    }

    #[test]
    fn test_list_blocked_explicit_reason() {
        let conn = setup();
        create_task(&conn, "t1", "Task", 2);

        let ev = TaskEvent::from_payload(
            "t1",
            "user",
            TaskUpdatedPayload {
                title: None,
                description: None,
                priority: None,
                due_ts: None,
                blocked_reason: Some("waiting on external API".to_string()),
                task_type: None,
                assignee: None,
                defer_until: None,
            },
        );
        apply_event(&conn, &ev).unwrap();

        let blocked = list_blocked(&conn).unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(
            blocked[0].blocked_reason.as_deref(),
            Some("waiting on external API")
        );
    }

    #[test]
    fn test_list_all() {
        let conn = setup();
        create_task(&conn, "t1", "Task 1", 2);
        create_task(&conn, "t2", "Task 2", 1);
        set_status(&conn, "t1", "done");

        let all = list_all(&conn).unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_get_task() {
        let conn = setup();
        create_task(&conn, "t1", "My Task", 3);

        let task = get_task(&conn, "t1").unwrap().unwrap();
        assert_eq!(task.title, "My Task");
        assert_eq!(task.priority, 3);
        assert_eq!(task.status, "open");

        assert!(get_task(&conn, "nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_task_exists() {
        let conn = setup();
        create_task(&conn, "t1", "Task", 2);

        assert!(task_exists(&conn, "t1").unwrap());
        assert!(!task_exists(&conn, "t2").unwrap());
    }

    #[test]
    fn test_priority_ordering_with_due_dates() {
        let conn = setup();

        // Same priority, different due dates
        let ev1 = TaskEvent::from_payload(
            "t1",
            "user",
            TaskCreatedPayload {
                title: "Later due".to_string(),
                description: None,
                priority: 2,
                status: TaskStatus::Open,
                due_ts: Some(2000),
                task_type: None,
                assignee: None,
                defer_until: None,
                parent_task_id: None,
            },
        );
        let ev2 = TaskEvent::from_payload(
            "t2",
            "user",
            TaskCreatedPayload {
                title: "Earlier due".to_string(),
                description: None,
                priority: 2,
                status: TaskStatus::Open,
                due_ts: Some(1000),
                task_type: None,
                assignee: None,
                defer_until: None,
                parent_task_id: None,
            },
        );
        let ev3 = TaskEvent::from_payload(
            "t3",
            "user",
            TaskCreatedPayload {
                title: "No due date".to_string(),
                description: None,
                priority: 2,
                status: TaskStatus::Open,
                due_ts: None,
                task_type: None,
                assignee: None,
                defer_until: None,
                parent_task_id: None,
            },
        );

        apply_event(&conn, &ev1).unwrap();
        apply_event(&conn, &ev2).unwrap();
        apply_event(&conn, &ev3).unwrap();

        let ready = list_ready(&conn).unwrap();
        assert_eq!(ready.len(), 3);
        // Earlier due first, later due second, null due last
        assert_eq!(ready[0].task_id, "t2");
        assert_eq!(ready[1].task_id, "t1");
        assert_eq!(ready[2].task_id, "t3");
    }

    #[test]
    fn test_list_newly_unblocked_basic() {
        let conn = setup();
        create_task(&conn, "t1", "Blocker", 2);
        create_task(&conn, "t2", "Blocked", 1);
        add_dep(&conn, "t2", "t1");

        // Before completing t1, nothing is unblocked
        let unblocked = list_newly_unblocked(&conn, "t1").unwrap();
        assert!(unblocked.is_empty());

        // Complete t1
        set_status(&conn, "t1", "done");

        // Now t2 should be unblocked
        let unblocked = list_newly_unblocked(&conn, "t1").unwrap();
        assert_eq!(unblocked, vec!["t2"]);
    }

    #[test]
    fn test_list_newly_unblocked_partial_deps() {
        let conn = setup();
        create_task(&conn, "t1", "Blocker 1", 2);
        create_task(&conn, "t2", "Blocker 2", 2);
        create_task(&conn, "t3", "Blocked by both", 1);
        add_dep(&conn, "t3", "t1");
        add_dep(&conn, "t3", "t2");

        // Complete only t1 — t3 still blocked by t2
        set_status(&conn, "t1", "done");
        let unblocked = list_newly_unblocked(&conn, "t1").unwrap();
        assert!(unblocked.is_empty());

        // Complete t2 — t3 now fully unblocked
        set_status(&conn, "t2", "done");
        let unblocked = list_newly_unblocked(&conn, "t2").unwrap();
        assert_eq!(unblocked, vec!["t3"]);
    }

    #[test]
    fn test_list_newly_unblocked_cancelled_counts() {
        let conn = setup();
        create_task(&conn, "t1", "Blocker", 2);
        create_task(&conn, "t2", "Blocked", 1);
        add_dep(&conn, "t2", "t1");

        // Cancel t1 — should also unblock t2
        set_status(&conn, "t1", "cancelled");
        let unblocked = list_newly_unblocked(&conn, "t1").unwrap();
        assert_eq!(unblocked, vec!["t2"]);
    }

    #[test]
    fn test_get_dependency_summary() {
        let conn = setup();
        create_task(&conn, "t1", "Dep 1", 2);
        create_task(&conn, "t2", "Dep 2", 2);
        create_task(&conn, "t3", "Has deps", 1);
        add_dep(&conn, "t3", "t1");
        add_dep(&conn, "t3", "t2");

        let summary = get_dependency_summary(&conn, "t3").unwrap();
        assert_eq!(summary.total_deps, 2);
        assert_eq!(summary.done_deps, 0);
        assert_eq!(summary.blocking_task_ids.len(), 2);

        // Complete one dep
        set_status(&conn, "t1", "done");
        let summary = get_dependency_summary(&conn, "t3").unwrap();
        assert_eq!(summary.total_deps, 2);
        assert_eq!(summary.done_deps, 1);
        assert_eq!(summary.blocking_task_ids, vec!["t2"]);
    }

    #[test]
    fn test_count_ready_blocked() {
        let conn = setup();
        create_task(&conn, "t1", "Ready 1", 2);
        create_task(&conn, "t2", "Ready 2", 1);
        create_task(&conn, "t3", "Blocked", 1);
        add_dep(&conn, "t3", "t1");

        let (ready, blocked) = count_ready_blocked(&conn).unwrap();
        assert_eq!(ready, 2);
        assert_eq!(blocked, 1);
    }

    // -- Prefix resolution tests --

    #[test]
    fn test_resolve_exact_match() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Full ID task", 2);
        let resolved = resolve_task_id(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M").unwrap();
        assert_eq!(resolved, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M");
    }

    #[test]
    fn test_resolve_prefix_match() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Task", 2);
        let resolved = resolve_task_id(&conn, "BRN-01JPHZ").unwrap();
        assert_eq!(resolved, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M");
    }

    #[test]
    fn test_resolve_case_insensitive() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Task", 2);
        let resolved = resolve_task_id(&conn, "brn-01jphz").unwrap();
        assert_eq!(resolved, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M");
    }

    #[test]
    fn test_resolve_bare_ulid_prefix() {
        let conn = setup();
        crate::db::meta::set_meta(&conn, "project_prefix", "BRN").unwrap();
        create_task(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Task", 2);
        let resolved = resolve_task_id(&conn, "01JPHZ").unwrap();
        assert_eq!(resolved, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M");
    }

    #[test]
    fn test_resolve_too_short() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Task", 2);
        let result = resolve_task_id(&conn, "BRN-01J");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_resolve_not_found() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Task", 2);
        let result = resolve_task_id(&conn, "BRN-99ZZZZ");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no task found"));
    }

    #[test]
    fn test_resolve_ambiguous() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZAAAA", "Task A", 2);
        create_task(&conn, "BRN-01JPHZAAAB", "Task B", 2);
        let result = resolve_task_id(&conn, "BRN-01JPHZAAA");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ambiguous"));
    }

    #[test]
    fn test_resolve_legacy_uuid() {
        let conn = setup();
        create_task(&conn, "019571a8-7c4e-7d3a-beef-deadbeef0001", "Legacy", 2);
        let resolved = resolve_task_id(&conn, "019571a8-7c4e-7d3a-beef-deadbeef0001").unwrap();
        assert_eq!(resolved, "019571a8-7c4e-7d3a-beef-deadbeef0001");
    }

    #[test]
    fn test_resolve_simple_test_ids() {
        // Existing tests use "t1", "t2" etc — exact match fast path
        let conn = setup();
        create_task(&conn, "t1", "Task 1", 2);
        let resolved = resolve_task_id(&conn, "t1").unwrap();
        assert_eq!(resolved, "t1");
    }

    // -- Shortest unique prefix tests --

    #[test]
    fn test_shortest_unique_single_task() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Only task", 2);
        let prefixes = shortest_unique_prefixes(&conn).unwrap();
        let short = &prefixes["BRN-01JPHZS7VXQK4R3BGTHNED2P8M"];
        assert_eq!(short.len(), MIN_DISPLAY_PREFIX_LEN);
        assert_eq!(short, "BRN-01JP");
    }

    #[test]
    fn test_shortest_unique_shared_prefix() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZAAAA", "Task A", 2);
        create_task(&conn, "BRN-01JPHZAAAB", "Task B", 2);
        let prefixes = shortest_unique_prefixes(&conn).unwrap();
        // Must distinguish the last char
        assert_eq!(prefixes["BRN-01JPHZAAAA"], "BRN-01JPHZAAAA");
        assert_eq!(prefixes["BRN-01JPHZAAAB"], "BRN-01JPHZAAAB");
    }

    #[test]
    fn test_shortest_unique_mixed_formats() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZ0001", "New format", 2);
        create_task(&conn, "t1", "Simple ID", 2);
        let prefixes = shortest_unique_prefixes(&conn).unwrap();
        assert_eq!(prefixes.len(), 2);
        // Both should be present; "t1" is too short for MIN_DISPLAY_PREFIX_LEN
        // so it stays as "t1"
        assert_eq!(prefixes["t1"], "t1");
    }

    #[test]
    fn test_shortest_unique_prefix_singular_matches_batch() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZAAAA", "Task A", 2);
        create_task(&conn, "BRN-01JPHZAAAB", "Task B", 2);
        create_task(&conn, "BRN-01JPHZ9999", "Task C", 2);

        // The O(log n) singular version should produce the same results as the
        // O(n log n) batch version for each task.
        let batch = shortest_unique_prefixes(&conn).unwrap();
        for (id, expected) in &batch {
            let single = shortest_unique_prefix(&conn, id).unwrap();
            assert_eq!(&single, expected, "mismatch for {id}");
        }
    }

    // -- Helper function tests --

    #[test]
    fn test_increment_string_basic() {
        assert_eq!(increment_string("BRN-01JP"), "BRN-01JQ");
        assert_eq!(increment_string("A"), "B");
        assert_eq!(increment_string("Z"), "["); // Z (0x5A) + 1 = [ (0x5B)
    }

    #[test]
    fn test_increment_string_carry() {
        // 0xFF bytes carry over to the next position
        let result = increment_string("\u{7f}"); // DEL (0x7F) → 0x80 (invalid UTF-8)
        // Falls back to appending \u{FFFF} since 0x80 is invalid UTF-8
        assert!(result.starts_with('\u{7f}'));
    }

    #[test]
    fn test_common_prefix_len_basic() {
        assert_eq!(common_prefix_len("BRN-01JPHA", "BRN-01JPHB"), 9);
        assert_eq!(common_prefix_len("abc", "abd"), 2);
        assert_eq!(common_prefix_len("abc", "xyz"), 0);
        assert_eq!(common_prefix_len("abc", "abc"), 3);
        assert_eq!(common_prefix_len("", "abc"), 0);
    }
}
