use rusqlite::Connection;

use crate::error::Result;

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
    pub created_at: i64,
    pub updated_at: i64,
}

fn row_to_task(row: &rusqlite::Row) -> rusqlite::Result<TaskRow> {
    Ok(TaskRow {
        task_id: row.get(0)?,
        title: row.get(1)?,
        description: row.get(2)?,
        status: row.get(3)?,
        priority: row.get(4)?,
        blocked_reason: row.get(5)?,
        due_ts: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

/// List tasks that are ready to work on: open/in_progress, no blocked_reason,
/// and all dependencies are done or cancelled.
///
/// Ordered by priority ASC, due_ts ASC NULLS LAST, updated_at DESC.
pub fn list_ready(conn: &Connection) -> Result<Vec<TaskRow>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, title, description, status, priority, blocked_reason,
                due_ts, created_at, updated_at
         FROM tasks t
         WHERE t.status IN ('open', 'in_progress')
           AND t.blocked_reason IS NULL
           AND NOT EXISTS (
               SELECT 1 FROM task_deps d
               JOIN tasks dep ON dep.task_id = d.depends_on
               WHERE d.task_id = t.task_id
                 AND dep.status NOT IN ('done', 'cancelled')
           )
         ORDER BY t.priority ASC, t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC",
    )?;

    let rows = stmt.query_map([], row_to_task)?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// List tasks that are blocked: have unresolved deps or an explicit blocked_reason.
pub fn list_blocked(conn: &Connection) -> Result<Vec<TaskRow>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, title, description, status, priority, blocked_reason,
                due_ts, created_at, updated_at
         FROM tasks t
         WHERE t.status IN ('open', 'in_progress', 'blocked')
           AND (
               t.blocked_reason IS NOT NULL
               OR EXISTS (
                   SELECT 1 FROM task_deps d
                   JOIN tasks dep ON dep.task_id = d.depends_on
                   WHERE d.task_id = t.task_id
                     AND dep.status NOT IN ('done', 'cancelled')
               )
           )
         ORDER BY t.priority ASC, t.due_ts ASC NULLS LAST, t.updated_at DESC, t.task_id ASC",
    )?;

    let rows = stmt.query_map([], row_to_task)?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// List all tasks.
pub fn list_all(conn: &Connection) -> Result<Vec<TaskRow>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, title, description, status, priority, blocked_reason,
                due_ts, created_at, updated_at
         FROM tasks
         ORDER BY priority ASC, due_ts ASC NULLS LAST, updated_at DESC, task_id ASC",
    )?;

    let rows = stmt.query_map([], row_to_task)?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// Get a single task by ID.
pub fn get_task(conn: &Connection, task_id: &str) -> Result<Option<TaskRow>> {
    let result = conn
        .query_row(
            "SELECT task_id, title, description, status, priority, blocked_reason,
                    due_ts, created_at, updated_at
             FROM tasks WHERE task_id = ?1",
            [task_id],
            row_to_task,
        )
        .ok();

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
        let ev = TaskEvent {
            event_id: new_event_id(),
            task_id: task_id.to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::TaskCreated,
            payload: serde_json::to_value(TaskCreatedPayload {
                title: title.to_string(),
                description: None,
                priority,
                status: "open".to_string(),
                due_ts: None,
            })
            .unwrap(),
        };
        apply_event(conn, &ev).unwrap();
    }

    fn set_status(conn: &Connection, task_id: &str, status: &str) {
        let ev = TaskEvent {
            event_id: new_event_id(),
            task_id: task_id.to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::StatusChanged,
            payload: serde_json::to_value(StatusChangedPayload {
                new_status: status.to_string(),
            })
            .unwrap(),
        };
        apply_event(conn, &ev).unwrap();
    }

    fn add_dep(conn: &Connection, task_id: &str, depends_on: &str) {
        let ev = TaskEvent {
            event_id: new_event_id(),
            task_id: task_id.to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::DependencyAdded,
            payload: serde_json::to_value(DependencyPayload {
                depends_on_task_id: depends_on.to_string(),
            })
            .unwrap(),
        };
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

        let ev = TaskEvent {
            event_id: new_event_id(),
            task_id: "t1".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::TaskUpdated,
            payload: serde_json::to_value(TaskUpdatedPayload {
                title: None,
                description: None,
                priority: None,
                due_ts: None,
                blocked_reason: Some("waiting on external API".to_string()),
            })
            .unwrap(),
        };
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
        let ev1 = TaskEvent {
            event_id: new_event_id(),
            task_id: "t1".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::TaskCreated,
            payload: serde_json::to_value(TaskCreatedPayload {
                title: "Later due".to_string(),
                description: None,
                priority: 2,
                status: "open".to_string(),
                due_ts: Some(2000),
            })
            .unwrap(),
        };
        let ev2 = TaskEvent {
            event_id: new_event_id(),
            task_id: "t2".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::TaskCreated,
            payload: serde_json::to_value(TaskCreatedPayload {
                title: "Earlier due".to_string(),
                description: None,
                priority: 2,
                status: "open".to_string(),
                due_ts: Some(1000),
            })
            .unwrap(),
        };
        let ev3 = TaskEvent {
            event_id: new_event_id(),
            task_id: "t3".to_string(),
            timestamp: now_ts(),
            actor: "user".to_string(),
            event_type: EventType::TaskCreated,
            payload: serde_json::to_value(TaskCreatedPayload {
                title: "No due date".to_string(),
                description: None,
                priority: 2,
                status: "open".to_string(),
                due_ts: None,
            })
            .unwrap(),
        };

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
}
