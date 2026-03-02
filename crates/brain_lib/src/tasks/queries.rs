use rusqlite::Connection;

use crate::error::Result;

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
    pub created_at: i64,
    pub updated_at: i64,
}

const TASK_COLUMNS: &str = "task_id, title, description, status, priority, blocked_reason, due_ts, \
     task_type, assignee, defer_until, parent_task_id, created_at, updated_at";

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
        created_at: row.get(11)?,
        updated_at: row.get(12)?,
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
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
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
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
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
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// Get a single task by ID.
pub fn get_task(conn: &Connection, task_id: &str) -> Result<Option<TaskRow>> {
    let sql = format!("SELECT {TASK_COLUMNS} FROM tasks WHERE task_id = ?1");
    let result = conn.query_row(&sql, [task_id], row_to_task).ok();
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
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
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

    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
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
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
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
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// Get child tasks of a parent.
pub fn get_children(conn: &Connection, parent_task_id: &str) -> Result<Vec<TaskRow>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS} FROM tasks WHERE parent_task_id = ?1
         ORDER BY priority ASC, created_at ASC, task_id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([parent_task_id], row_to_task)?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
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
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// List all (task_id, label) pairs (bulk load for export).
pub fn list_all_labels(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt =
        conn.prepare("SELECT task_id, label FROM task_labels ORDER BY task_id, label")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
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
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
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
}
