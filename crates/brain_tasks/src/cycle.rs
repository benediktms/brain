//! Dependency-cycle detection for the task DAG.
//!
//! The rule: `task_deps` (and the corresponding `entity_links` 'blocks' edges)
//! must form a directed acyclic graph. Adding an edge that would close a cycle
//! is rejected at the validator stage of the projection pipeline.
//!
//! The algorithm walks the existing graph from the proposed edge's target,
//! returning early if it reaches the proposed edge's source. brain_persistence
//! contributes only the per-node neighbor lookup; the traversal lives here.

use std::collections::{HashSet, VecDeque};

use rusqlite::Connection;

use brain_core::error::BrainCoreError;
use brain_persistence::db::tasks::queries::outgoing_blocks;
use brain_persistence::sql::{SqlError, SqlResult};

/// Check whether adding a dependency `task_id → depends_on` would create a cycle.
///
/// Returns `Ok(())` if safe, or `Err(TaskCycle(...))` if it would create a cycle.
///
/// Algorithm: BFS from `depends_on` following existing TASK→TASK 'blocks' edges.
/// If `task_id` is reachable from `depends_on`, a cycle exists. The traversal
/// is on-demand: each visited node fetches its own outgoing edges, so only the
/// reachable subgraph is loaded.
pub fn check_cycle(conn: &Connection, task_id: &str, depends_on: &str) -> SqlResult<()> {
    if task_id == depends_on {
        return Err(SqlError::Domain(BrainCoreError::TaskCycle(format!(
            "self-dependency: {task_id} -> {task_id}"
        ))));
    }

    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(depends_on.to_string());
    visited.insert(depends_on.to_string());

    while let Some(node) = queue.pop_front() {
        for next in outgoing_blocks(conn, &node)? {
            if next == task_id {
                return Err(SqlError::Domain(BrainCoreError::TaskCycle(format!(
                    "adding {task_id} -> {depends_on} would create a cycle"
                ))));
            }
            if visited.insert(next.clone()) {
                queue.push_back(next);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use brain_persistence::db::schema::init_schema;
    use brain_persistence::db::tasks::writers::{add_dependency, insert_task_row};

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn create_task(conn: &Connection, task_id: &str) {
        insert_task_row(
            conn, task_id, "", task_id, None, "open", 2, None, "task", None, None, None, None, 0,
            None,
        )
        .unwrap();
    }

    fn add_dep(conn: &Connection, task_id: &str, depends_on: &str) {
        add_dependency(conn, task_id, depends_on).unwrap();
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

    /// Recovers the BFS-level orphan-edge coverage that was lost when the
    /// persistence-side `cycle` module was deleted. The persistence test
    /// `outgoing_blocks_orphan` pins the primitive's contract; this test pins
    /// the algorithm's behavior when that contract is exercised at runtime.
    #[test]
    fn test_orphan_neighbor_does_not_break_cycle_detection() {
        use brain_persistence::db::tasks::writers::add_orphan_blocks_edge;

        let conn = setup();
        create_task(&conn, "t1");
        create_task(&conn, "t2");
        create_task(&conn, "t3");

        // Real edges: t3 -> t2 -> t1.
        add_dep(&conn, "t3", "t2");
        add_dep(&conn, "t2", "t1");

        // t2 also has an outgoing 'blocks' edge to a task that has no row in
        // `tasks`. The BFS must tolerate this when traversal reaches t2.
        add_orphan_blocks_edge(&conn, "t2", "ghost-9999").unwrap();

        // (a) Real cycle still caught despite encountering the ghost mid-traversal.
        //     Proposing t1 -> t3 would close the cycle t3 -> t2 -> t1 -> t3.
        let result = check_cycle(&conn, "t1", "t3");
        assert!(
            result.is_err(),
            "cycle must still be detected with orphan neighbor present"
        );
        assert!(result.unwrap_err().to_string().contains("cycle"));

        // (b) No false-positive when BFS starts from a ghost id (proposed
        //     depends_on has no row). Traversal terminates with no edges to follow.
        check_cycle(&conn, "t1", "ghost-9999")
            .expect("ghost-rooted traversal must not yield a false cycle");
    }
}
