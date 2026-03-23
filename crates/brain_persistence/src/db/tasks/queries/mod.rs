mod details;
mod filters;
mod labels;
mod listing;
mod resolve;

pub use details::*;
pub use filters::*;
pub use labels::*;
pub use listing::*;
pub use resolve::*;

use super::events::TaskType;

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
    pub task_type: TaskType,
    pub assignee: Option<String>,
    pub defer_until: Option<i64>,
    pub parent_task_id: Option<String>,
    pub child_seq: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    /// Stable hash-based short ID (hex only). `None` for pre-migration tasks.
    pub id: Option<String>,
}

pub(super) const TASK_COLUMNS: &str = "task_id, title, description, status, priority, blocked_reason, due_ts, \
     task_type, assignee, defer_until, parent_task_id, child_seq, created_at, updated_at, id";

/// Reusable `WITH RECURSIVE` CTE that produces `has_blocked_ancestor(tid)` — the set
/// of task IDs whose parent chain contains at least one blocked ancestor.
/// An ancestor is blocking if it has unresolved deps, a `blocked_reason`, or a future `defer_until`.
pub(super) const ANCESTOR_BLOCKED_CTE: &str = "\
WITH RECURSIVE ancestor_chain(tid, ancestor_id) AS (
    SELECT task_id, parent_task_id FROM tasks WHERE parent_task_id IS NOT NULL
    UNION ALL
    SELECT ac.tid, t.parent_task_id
    FROM ancestor_chain ac
    JOIN tasks t ON t.task_id = ac.ancestor_id
    WHERE t.parent_task_id IS NOT NULL
),
has_blocked_ancestor(tid) AS (
    SELECT DISTINCT ac.tid
    FROM ancestor_chain ac
    JOIN tasks a ON a.task_id = ac.ancestor_id
    WHERE a.status NOT IN ('done', 'cancelled')
      AND (
          a.blocked_reason IS NOT NULL
          OR (a.defer_until IS NOT NULL AND a.defer_until > strftime('%s', 'now'))
          OR EXISTS (
              SELECT 1 FROM task_deps d
              JOIN tasks dep ON dep.task_id = d.depends_on
              WHERE d.task_id = a.task_id
                AND dep.status NOT IN ('done', 'cancelled')
          )
      )
) ";

pub(super) fn row_to_task(row: &rusqlite::Row) -> rusqlite::Result<TaskRow> {
    let task_type_str: String = row.get(7)?;
    let task_id: String = row.get(0)?;
    let task_type = task_type_str.parse().unwrap_or_else(|e| {
        tracing::warn!(
            task_id = %task_id,
            raw_value = %task_type_str,
            error = %e,
            "invalid task_type in database; defaulting to Task — possible schema or migration issue"
        );
        TaskType::Task
    });
    Ok(TaskRow {
        task_id,
        title: row.get(1)?,
        description: row.get(2)?,
        status: row.get(3)?,
        priority: row.get(4)?,
        blocked_reason: row.get(5)?,
        due_ts: row.get(6)?,
        task_type,
        assignee: row.get(8)?,
        defer_until: row.get(9)?,
        parent_task_id: row.get(10)?,
        child_seq: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
        id: row.get(14)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;
    use crate::db::tasks::events::*;
    use crate::db::tasks::projections::apply_event;
    use rusqlite::Connection;

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
                id: None,
            },
        );
        apply_event(conn, &ev, "").unwrap();
    }

    fn set_status(conn: &Connection, task_id: &str, status: &str) {
        let ev = TaskEvent::from_payload(
            task_id,
            "user",
            StatusChangedPayload {
                new_status: status.parse().unwrap(),
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
    fn test_list_ready_no_deps() {
        let conn = setup();
        create_task(&conn, "t1", "Task 1", 2);
        create_task(&conn, "t2", "Task 2", 1);

        let ready = list_ready(&conn, None).unwrap();
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

        let ready = list_ready(&conn, None).unwrap();
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

        let ready = list_ready(&conn, None).unwrap();
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

        let ready = list_ready(&conn, None).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, "t2");
    }

    #[test]
    fn test_list_blocked() {
        let conn = setup();
        create_task(&conn, "t1", "Blocker", 2);
        create_task(&conn, "t2", "Blocked by dep", 1);
        add_dep(&conn, "t2", "t1");

        let blocked = list_blocked(&conn, None).unwrap();
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
        apply_event(&conn, &ev, "").unwrap();

        let blocked = list_blocked(&conn, None).unwrap();
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

        let all = list_all(&conn, None).unwrap();
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
                id: None,
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
                id: None,
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
                id: None,
            },
        );

        apply_event(&conn, &ev1, "").unwrap();
        apply_event(&conn, &ev2, "").unwrap();
        apply_event(&conn, &ev3, "").unwrap();

        let ready = list_ready(&conn, None).unwrap();
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

        let (ready, blocked) = count_ready_blocked(&conn, None).unwrap();
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

    // -- Compact ID tests --

    #[test]
    fn test_compact_id_single_task() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZS7VXQK4R3BGTHNED2P8M", "Only task", 2);
        let prefixes = compact_ids(&conn).unwrap();
        let short = &prefixes["BRN-01JPHZS7VXQK4R3BGTHNED2P8M"];
        // Hash-based: {prefix_lower}-{hex[:3+]}
        assert!(short.contains('-'), "should have prefix-hash format: {short}");
        assert!(short.len() >= 4 + resolve::MIN_SHORT_HASH_LEN); // "xxx-" + 3+ hex
    }

    #[test]
    fn test_compact_id_shared_prefix() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZAAAA", "Task A", 2);
        create_task(&conn, "BRN-01JPHZAAAB", "Task B", 2);
        let prefixes = compact_ids(&conn).unwrap();
        // Different tasks should get different display IDs
        assert_ne!(prefixes["BRN-01JPHZAAAA"], prefixes["BRN-01JPHZAAAB"]);
    }

    #[test]
    fn test_compact_id_mixed_formats() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZ0001", "New format", 2);
        create_task(&conn, "t1", "Simple ID", 2);
        let prefixes = compact_ids(&conn).unwrap();
        assert_eq!(prefixes.len(), 2);
        // Both should have hash-based IDs
        assert!(prefixes["BRN-01JPHZ0001"].contains('-'));
        assert!(prefixes["t1"].contains('-'));
    }

    #[test]
    fn test_compact_id_singular_matches_batch() {
        let conn = setup();
        create_task(&conn, "BRN-01JPHZAAAA", "Task A", 2);
        create_task(&conn, "BRN-01JPHZAAAB", "Task B", 2);
        create_task(&conn, "BRN-01JPHZ9999", "Task C", 2);

        // The O(log n) singular version should produce the same results as the
        // O(n log n) batch version for each task.
        let batch = compact_ids(&conn).unwrap();
        for (id, expected) in &batch {
            let single = compact_id(&conn, id).unwrap();
            assert_eq!(&single, expected, "mismatch for {id}");
        }
    }

    // -- Ancestor-blocked propagation tests --

    fn create_child_task(
        conn: &Connection,
        task_id: &str,
        parent_id: &str,
        title: &str,
        priority: i32,
    ) {
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
                parent_task_id: Some(parent_id.to_string()),
                id: None,
            },
        );
        apply_event(conn, &ev, "").unwrap();
    }

    fn create_epic(conn: &Connection, task_id: &str, title: &str, priority: i32) {
        let ev = TaskEvent::from_payload(
            task_id,
            "user",
            TaskCreatedPayload {
                title: title.to_string(),
                description: None,
                priority,
                status: TaskStatus::Open,
                due_ts: None,
                task_type: Some(TaskType::Epic),
                assignee: None,
                defer_until: None,
                parent_task_id: None,
                id: None,
            },
        );
        apply_event(conn, &ev, "").unwrap();
    }

    fn set_blocked_reason(conn: &Connection, task_id: &str, reason: Option<&str>) {
        let ev = TaskEvent::from_payload(
            task_id,
            "user",
            TaskUpdatedPayload {
                title: None,
                description: None,
                priority: None,
                due_ts: None,
                blocked_reason: reason.map(|s| s.to_string()),
                task_type: None,
                assignee: None,
                defer_until: None,
            },
        );
        apply_event(conn, &ev, "").unwrap();
    }

    fn set_defer_until(conn: &Connection, task_id: &str, ts: Option<i64>) {
        let ev = TaskEvent::from_payload(
            task_id,
            "user",
            TaskUpdatedPayload {
                title: None,
                description: None,
                priority: None,
                due_ts: None,
                blocked_reason: None,
                task_type: None,
                assignee: None,
                defer_until: ts,
            },
        );
        apply_event(conn, &ev, "").unwrap();
    }

    #[test]
    fn test_list_ready_excludes_child_of_blocked_epic() {
        let conn = setup();
        create_epic(&conn, "epic1", "Epic", 1);
        create_task(&conn, "blocker", "Blocker", 2);
        add_dep(&conn, "epic1", "blocker"); // epic blocked by dep
        create_child_task(&conn, "child1", "epic1", "Child 1", 2);

        let ready = list_ready(&conn, None).unwrap();
        let ready_ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            !ready_ids.contains(&"child1"),
            "child of blocked epic should NOT be ready"
        );
        assert!(ready_ids.contains(&"blocker"));
    }

    #[test]
    fn test_list_blocked_includes_child_of_blocked_epic() {
        let conn = setup();
        create_epic(&conn, "epic1", "Epic", 1);
        create_task(&conn, "blocker", "Blocker", 2);
        add_dep(&conn, "epic1", "blocker");
        create_child_task(&conn, "child1", "epic1", "Child 1", 2);

        let blocked = list_blocked(&conn, None).unwrap();
        let blocked_ids: Vec<&str> = blocked.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            blocked_ids.contains(&"child1"),
            "child of blocked epic should be in blocked list"
        );
        assert!(
            blocked_ids.contains(&"epic1"),
            "epic itself should be blocked"
        );
    }

    #[test]
    fn test_child_becomes_ready_when_epic_dep_resolved() {
        let conn = setup();
        create_epic(&conn, "epic1", "Epic", 1);
        create_task(&conn, "blocker", "Blocker", 2);
        add_dep(&conn, "epic1", "blocker");
        create_child_task(&conn, "child1", "epic1", "Child 1", 2);

        // Child should NOT be ready while epic is blocked
        let ready = list_ready(&conn, None).unwrap();
        assert!(!ready.iter().any(|t| t.task_id == "child1"));

        // Complete the blocker
        set_status(&conn, "blocker", "done");

        // Now child should be ready
        let ready = list_ready(&conn, None).unwrap();
        assert!(
            ready.iter().any(|t| t.task_id == "child1"),
            "child should be ready after epic's dep is resolved"
        );
    }

    #[test]
    fn test_grandchild_blocked_by_grandparent_dep() {
        let conn = setup();
        create_epic(&conn, "grandparent", "GP Epic", 1);
        create_task(&conn, "blocker", "Blocker", 2);
        add_dep(&conn, "grandparent", "blocker");
        create_child_task(&conn, "parent", "grandparent", "Parent", 2);
        create_child_task(&conn, "grandchild", "parent", "Grandchild", 2);

        let ready = list_ready(&conn, None).unwrap();
        assert!(
            !ready.iter().any(|t| t.task_id == "grandchild"),
            "grandchild should NOT be ready when grandparent is blocked"
        );

        let blocked = list_blocked(&conn, None).unwrap();
        assert!(
            blocked.iter().any(|t| t.task_id == "grandchild"),
            "grandchild should be in blocked list"
        );

        // Resolve grandparent's dep
        set_status(&conn, "blocker", "done");
        let ready = list_ready(&conn, None).unwrap();
        assert!(
            ready.iter().any(|t| t.task_id == "grandchild"),
            "grandchild should be ready after grandparent unblocked"
        );
    }

    #[test]
    fn test_child_blocked_by_parent_blocked_reason() {
        let conn = setup();
        create_task(&conn, "parent", "Parent", 1);
        create_child_task(&conn, "child1", "parent", "Child", 2);

        // Parent gets an explicit blocked_reason
        set_blocked_reason(&conn, "parent", Some("waiting on external"));

        let ready = list_ready(&conn, None).unwrap();
        assert!(
            !ready.iter().any(|t| t.task_id == "child1"),
            "child should NOT be ready when parent has blocked_reason"
        );

        let blocked = list_blocked(&conn, None).unwrap();
        assert!(blocked.iter().any(|t| t.task_id == "child1"));
    }

    #[test]
    fn test_child_blocked_by_parent_defer_until() {
        let conn = setup();
        create_task(&conn, "parent", "Parent", 1);
        create_child_task(&conn, "child1", "parent", "Child", 2);

        // Set defer_until far in the future
        set_defer_until(&conn, "parent", Some(i64::MAX));

        let ready = list_ready(&conn, None).unwrap();
        assert!(
            !ready.iter().any(|t| t.task_id == "child1"),
            "child should NOT be ready when parent has future defer_until"
        );

        let blocked = list_blocked(&conn, None).unwrap();
        assert!(blocked.iter().any(|t| t.task_id == "child1"));
    }

    #[test]
    fn test_count_ready_blocked_reflects_ancestor_deps() {
        let conn = setup();
        create_epic(&conn, "epic1", "Epic", 1);
        create_task(&conn, "blocker", "Blocker", 2);
        add_dep(&conn, "epic1", "blocker");
        create_child_task(&conn, "child1", "epic1", "Child 1", 2);
        create_child_task(&conn, "child2", "epic1", "Child 2", 2);

        let (ready, blocked) = count_ready_blocked(&conn, None).unwrap();
        // Only "blocker" is ready; epic1 + child1 + child2 are blocked
        assert_eq!(ready, 1, "only blocker should be ready");
        assert_eq!(blocked, 3, "epic + 2 children should be blocked");
    }

    #[test]
    fn test_child_without_blocked_parent_still_ready() {
        let conn = setup();
        create_task(&conn, "parent", "Parent", 1);
        create_child_task(&conn, "child1", "parent", "Child", 2);

        // Parent has no deps, no blocked_reason, no defer_until — child should be ready
        let ready = list_ready(&conn, None).unwrap();
        let ready_ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            ready_ids.contains(&"child1"),
            "child of unblocked parent should be ready"
        );
        assert!(ready_ids.contains(&"parent"));
    }

    // -- Invalid task_type handling tests --

    /// Directly corrupt a task_type value in the DB (bypassing the CHECK constraint via
    /// PRAGMA writable_schema / direct column update after disabling constraint checks)
    /// and verify that row_to_task gracefully defaults to TaskType::Task instead of panicking.
    ///
    /// This scenario models external DB manipulation or future schema migrations where an
    /// unknown task_type value could appear in the database.
    #[test]
    fn test_row_to_task_invalid_type_defaults_to_task() {
        let conn = setup();
        create_task(&conn, "t1", "Task 1", 2);

        // Disable integrity enforcement, update the value, then re-enable.
        // This simulates data corruption or a future migration that doesn't
        // yet know about the CHECK constraint.
        conn.execute_batch(
            "PRAGMA ignore_check_constraints = ON;
             UPDATE tasks SET task_type = 'corrupted_type' WHERE task_id = 't1';
             PRAGMA ignore_check_constraints = OFF;",
        )
        .unwrap();

        let task = get_task(&conn, "t1").unwrap().unwrap();
        assert_eq!(
            task.task_type,
            TaskType::Task,
            "invalid task_type should default to TaskType::Task"
        );
    }

    /// Verify that the TaskType FromStr implementation correctly rejects unknown values.
    /// This is the underlying mechanism that row_to_task relies on for its warn+default path.
    #[test]
    fn test_task_type_parse_rejects_unknown_values() {
        let invalid_values = ["", "unknown", "corrupted_type", "TASK", "Task", "BUG"];
        for s in &invalid_values {
            let result: Result<TaskType, _> = s.parse();
            assert!(result.is_err(), "'{s}' should fail to parse as TaskType");
        }
    }

    /// Verify that all valid task_type values round-trip correctly through the DB.
    #[test]
    fn test_row_to_task_all_valid_types_roundtrip() {
        let conn = setup();

        let cases = [
            ("t_task", "task", TaskType::Task),
            ("t_bug", "bug", TaskType::Bug),
            ("t_feature", "feature", TaskType::Feature),
            ("t_epic", "epic", TaskType::Epic),
            ("t_spike", "spike", TaskType::Spike),
        ];

        for (task_id, type_str, expected) in &cases {
            create_task(&conn, task_id, "title", 2);
            conn.execute(
                "UPDATE tasks SET task_type = ?1 WHERE task_id = ?2",
                rusqlite::params![type_str, task_id],
            )
            .unwrap();

            let task = get_task(&conn, task_id).unwrap().unwrap();
            assert_eq!(
                &task.task_type, expected,
                "task_type '{type_str}' should round-trip correctly"
            );
        }
    }
}
