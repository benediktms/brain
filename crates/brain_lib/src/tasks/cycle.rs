use std::collections::{HashMap, HashSet};

use rusqlite::Connection;

use crate::error::{BrainCoreError, Result};

/// Check whether adding a dependency `task_id → depends_on` would create a cycle.
///
/// Returns `Ok(())` if safe, or `Err(TaskCycle(...))` if it would create a cycle.
///
/// Algorithm: iterative DFS from `depends_on` following existing "depends on" edges.
/// If we reach `task_id`, a cycle exists.
pub fn check_cycle(conn: &Connection, task_id: &str, depends_on: &str) -> Result<()> {
    // Self-loop is always a cycle
    if task_id == depends_on {
        return Err(BrainCoreError::TaskCycle(format!(
            "self-dependency: {task_id} -> {task_id}"
        )));
    }

    // Load the adjacency list: for each task, what does it depend on?
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    let mut stmt = conn.prepare("SELECT task_id, depends_on FROM task_deps")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (tid, dep) = row?;
        adj.entry(tid).or_default().push(dep);
    }

    // Iterative DFS from `depends_on` following "depends on" edges
    let mut visited = HashSet::new();
    let mut stack = vec![depends_on.to_string()];

    while let Some(current) = stack.pop() {
        if current == task_id {
            return Err(BrainCoreError::TaskCycle(format!(
                "adding {task_id} -> {depends_on} would create a cycle"
            )));
        }
        if !visited.insert(current.clone()) {
            continue;
        }
        if let Some(deps) = adj.get(&current) {
            for dep in deps {
                if !visited.contains(dep) {
                    stack.push(dep.clone());
                }
            }
        }
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
