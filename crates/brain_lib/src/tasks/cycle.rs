use rusqlite::Connection;

use crate::error::{BrainCoreError, Result};

/// Check whether adding a dependency `task_id → depends_on` would create a cycle.
///
/// Returns `Ok(())` if safe, or `Err(TaskCycle(...))` if it would create a cycle.
///
/// Algorithm: recursive CTE traversal from `depends_on` following existing "depends on"
/// edges in the database. If `task_id` is reachable from `depends_on`, a cycle exists.
/// This avoids loading the full edge set into memory.
pub fn check_cycle(conn: &Connection, task_id: &str, depends_on: &str) -> Result<()> {
    // Self-loop is always a cycle
    if task_id == depends_on {
        return Err(BrainCoreError::TaskCycle(format!(
            "self-dependency: {task_id} -> {task_id}"
        )));
    }

    // Use a recursive CTE to walk from `depends_on` through all reachable nodes,
    // then check if `task_id` is among them.
    let found: bool = conn.query_row(
        "WITH RECURSIVE reachable(tid) AS (
             SELECT ?1
             UNION
             SELECT d.depends_on
             FROM task_deps d
             JOIN reachable r ON d.task_id = r.tid
         )
         SELECT EXISTS(SELECT 1 FROM reachable WHERE tid = ?2)",
        rusqlite::params![depends_on, task_id],
        |row| row.get(0),
    )?;

    if found {
        return Err(BrainCoreError::TaskCycle(format!(
            "adding {task_id} -> {depends_on} would create a cycle"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn insert_task(conn: &Connection, task_id: &str) {
        conn.execute(
            "INSERT INTO tasks (task_id, title, status, priority, created_at, updated_at)
             VALUES (?1, ?1, 'open', 2, 1000, 1000)",
            [task_id],
        )
        .unwrap();
    }

    fn insert_dep(conn: &Connection, task_id: &str, depends_on: &str) {
        conn.execute(
            "INSERT INTO task_deps (task_id, depends_on) VALUES (?1, ?2)",
            rusqlite::params![task_id, depends_on],
        )
        .unwrap();
    }

    #[test]
    fn test_self_loop() {
        let conn = setup();
        insert_task(&conn, "t1");

        let result = check_cycle(&conn, "t1", "t1");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("self-dependency"));
    }

    #[test]
    fn test_direct_cycle() {
        let conn = setup();
        insert_task(&conn, "t1");
        insert_task(&conn, "t2");

        // t1 already depends on t2
        insert_dep(&conn, "t1", "t2");

        // Adding t2 -> t1 would create a cycle
        let result = check_cycle(&conn, "t2", "t1");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cycle"));
    }

    #[test]
    fn test_transitive_cycle() {
        let conn = setup();
        insert_task(&conn, "t1");
        insert_task(&conn, "t2");
        insert_task(&conn, "t3");

        // t1 -> t2 -> t3 (t1 depends on t2, t2 depends on t3)
        insert_dep(&conn, "t1", "t2");
        insert_dep(&conn, "t2", "t3");

        // Adding t3 -> t1 would create t1 -> t2 -> t3 -> t1
        let result = check_cycle(&conn, "t3", "t1");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cycle"));
    }

    #[test]
    fn test_no_cycle_valid_dep() {
        let conn = setup();
        insert_task(&conn, "t1");
        insert_task(&conn, "t2");
        insert_task(&conn, "t3");

        // t1 -> t2 (t1 depends on t2)
        insert_dep(&conn, "t1", "t2");

        // Adding t1 -> t3 is fine (no cycle)
        check_cycle(&conn, "t1", "t3").unwrap();

        // Adding t3 -> t2 is fine (no cycle)
        check_cycle(&conn, "t3", "t2").unwrap();
    }

    #[test]
    fn test_no_false_positive_diamond() {
        let conn = setup();
        insert_task(&conn, "t1");
        insert_task(&conn, "t2");
        insert_task(&conn, "t3");
        insert_task(&conn, "t4");

        // Diamond: t1 -> t2, t1 -> t3, t2 -> t4, t3 -> t4
        insert_dep(&conn, "t1", "t2");
        insert_dep(&conn, "t1", "t3");
        insert_dep(&conn, "t2", "t4");
        insert_dep(&conn, "t3", "t4");

        // Adding t4 -> t1 would be a cycle
        assert!(check_cycle(&conn, "t4", "t1").is_err());

        // But adding another path t1 -> t4 is fine (already reachable, no cycle)
        check_cycle(&conn, "t1", "t4").unwrap();
    }
}
