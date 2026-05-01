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

    // Use a recursive CTE to walk from `depends_on` through all reachable nodes
    // via entity_links 'blocks' edges (from_id=dependent, to_id=blocker).
    // Starting from `depends_on`, follow outgoing blocks edges (what it depends on)
    // and check if `task_id` is reachable — which would indicate a cycle.
    let found: bool = conn.query_row(
        "WITH RECURSIVE reachable(tid) AS (
             SELECT ?1
             UNION
             SELECT el.to_id
             FROM entity_links el
             JOIN reachable r ON el.from_id = r.tid
             WHERE el.from_type='TASK' AND el.to_type='TASK' AND el.edge_kind='blocks'
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
    use crate::db::tasks::events::{
        DependencyPayload, EventType, TaskCreatedPayload, TaskEvent, TaskStatus,
    };
    use crate::db::tasks::projections::apply_event;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn create_task(conn: &Connection, task_id: &str) {
        let ev = TaskEvent::from_payload(
            task_id,
            "user",
            TaskCreatedPayload {
                title: task_id.to_string(),
                description: None,
                priority: 2,
                status: TaskStatus::Open,
                due_ts: None,
                task_type: None,
                assignee: None,
                defer_until: None,
                parent_task_id: None,
                display_id: None,
            },
        );
        apply_event(conn, &ev, "").unwrap();
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
        apply_event(conn, &ev, "").unwrap();
    }

    #[test]
    fn test_self_loop() {
        let conn = setup();
        create_task(&conn, "t1");

        let result = check_cycle(&conn, "t1", "t1");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("self-dependency"));
    }

    #[test]
    fn test_direct_cycle() {
        let conn = setup();
        create_task(&conn, "t1");
        create_task(&conn, "t2");

        // t1 already depends on t2 (entity_links: blocks from=t1 to=t2)
        add_dep(&conn, "t1", "t2");

        // Adding t2 -> t1 would create a cycle
        let result = check_cycle(&conn, "t2", "t1");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cycle"));
    }

    #[test]
    fn test_transitive_cycle() {
        let conn = setup();
        create_task(&conn, "t1");
        create_task(&conn, "t2");
        create_task(&conn, "t3");

        // t1 depends on t2, t2 depends on t3 (entity_links blocks edges)
        add_dep(&conn, "t1", "t2");
        add_dep(&conn, "t2", "t3");

        // Adding t3 -> t1 would create t1 -> t2 -> t3 -> t1
        let result = check_cycle(&conn, "t3", "t1");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cycle"));
    }

    /// 3-deep cycle test (A→B→C→D and attempt D→A).
    #[test]
    fn test_three_deep_cycle_via_entity_links() {
        let conn = setup();
        create_task(&conn, "a");
        create_task(&conn, "b");
        create_task(&conn, "c");
        create_task(&conn, "d");

        // a depends on b, b depends on c, c depends on d
        add_dep(&conn, "a", "b");
        add_dep(&conn, "b", "c");
        add_dep(&conn, "c", "d");

        // Adding d→a would close the cycle a→b→c→d→a
        let result = check_cycle(&conn, "d", "a");
        assert!(result.is_err(), "3-deep cycle must be detected");
        assert!(result.unwrap_err().to_string().contains("cycle"));

        // But d→x (a new independent task) is safe
        create_task(&conn, "x");
        check_cycle(&conn, "d", "x").unwrap();
    }

    /// Orphan-edge test: an entity_links blocks edge pointing to a nonexistent task
    /// (FK off) must not cause a crash or false-positive cycle.
    #[test]
    fn test_orphan_entity_links_edge_no_panic() {
        let conn = setup();
        create_task(&conn, "t1");
        create_task(&conn, "t2");

        // Insert an orphan entity_links edge — blocks from t2 to a ghost task.
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO entity_links \
             (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope) \
             VALUES \
             (lower(hex(randomblob(16))), 'TASK', 't2', 'TASK', 'ghost-9999', 'blocks', \
              strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), NULL)",
            [],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();

        // t1 depends on t2 (real edge)
        add_dep(&conn, "t1", "t2");

        // Adding t2→t1 would be a cycle (t1 depends on t2 already)
        assert!(check_cycle(&conn, "t2", "t1").is_err());

        // Adding t1→ghost-9999 is not a cycle (no cycle exists)
        // The orphan edge from t2→ghost should not cause a crash.
        let result = check_cycle(&conn, "t1", "ghost-9999");
        // ghost-9999 is the start node, t1 would need to be reachable from ghost-9999 —
        // no entity_links edges originate from ghost-9999, so no cycle.
        assert!(
            result.is_ok(),
            "orphan edge must not cause false-positive cycle: {result:?}"
        );
    }

    #[test]
    fn test_no_cycle_valid_dep() {
        let conn = setup();
        create_task(&conn, "t1");
        create_task(&conn, "t2");
        create_task(&conn, "t3");

        // t1 depends on t2
        add_dep(&conn, "t1", "t2");

        // Adding t1 -> t3 is fine (no cycle)
        check_cycle(&conn, "t1", "t3").unwrap();

        // Adding t3 -> t2 is fine (no cycle)
        check_cycle(&conn, "t3", "t2").unwrap();
    }

    #[test]
    fn test_no_false_positive_diamond() {
        let conn = setup();
        create_task(&conn, "t1");
        create_task(&conn, "t2");
        create_task(&conn, "t3");
        create_task(&conn, "t4");

        // Diamond: t1 -> t2, t1 -> t3, t2 -> t4, t3 -> t4
        add_dep(&conn, "t1", "t2");
        add_dep(&conn, "t1", "t3");
        add_dep(&conn, "t2", "t4");
        add_dep(&conn, "t3", "t4");

        // Adding t4 -> t1 would be a cycle
        assert!(check_cycle(&conn, "t4", "t1").is_err());

        // But adding another path t1 -> t4 is fine (already reachable, no cycle)
        check_cycle(&conn, "t1", "t4").unwrap();
    }
}
