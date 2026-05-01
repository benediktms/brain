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
    /// Stable hash-based short display ID (hex only). `None` for pre-migration tasks.
    pub display_id: Option<String>,
}

pub(super) const TASK_COLUMNS: &str = "task_id, title, description, status, priority, blocked_reason, due_ts, \
     task_type, assignee, defer_until, parent_task_id, child_seq, created_at, updated_at, display_id";

/// Reusable `WITH RECURSIVE` CTE that produces `has_blocked_ancestor(tid)` — the set
/// of task IDs whose parent chain contains at least one blocked ancestor.
/// An ancestor is blocking if it has unresolved deps, an unresolved external blocker,
/// a `blocked_reason`, or a future `defer_until`.
///
/// The dep check uses a LEFT JOIN so that an orphaned `depends_on` reference (target
/// task not present in the DB) is treated as still-blocking rather than silently dropped.
///
/// The external-blocker clause sits ALONGSIDE the dep clause: a task with any
/// unresolved external blocker is treated as blocked regardless of whether its
/// dependency set is empty.
///
/// Parent chain and blocking deps are both read from `entity_links` (Wave 5 cutover).
/// `entity_links(from_id=parent, to_id=child, edge_kind='parent_of')` — from line 297
/// of `tasks/projections.rs` (ParentSet handler). `entity_links(from_id=dependent,
/// to_id=blocker, edge_kind='blocks')` — from line 167 of `tasks/projections.rs`
/// (DependencyAdded handler).
pub(super) const ANCESTOR_BLOCKED_CTE: &str = "\
WITH RECURSIVE ancestor_chain(tid, ancestor_id) AS (
    SELECT to_id AS tid, from_id AS ancestor_id
      FROM entity_links
     WHERE from_type='TASK' AND to_type='TASK' AND edge_kind='parent_of'
    UNION ALL
    SELECT ac.tid, el.from_id
      FROM ancestor_chain ac
      JOIN entity_links el
        ON el.to_type='TASK' AND el.to_id = ac.ancestor_id
       AND el.from_type='TASK' AND el.edge_kind='parent_of'
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
              SELECT 1 FROM entity_links el2
              LEFT JOIN tasks dep ON dep.task_id = el2.to_id
              WHERE el2.from_type='TASK' AND el2.to_type='TASK' AND el2.edge_kind='blocks'
                AND el2.from_id = a.task_id
                AND (dep.task_id IS NULL OR dep.status NOT IN ('done', 'cancelled'))
          )
          OR EXISTS (
              SELECT 1 FROM task_external_ids x
              WHERE x.task_id = a.task_id
                AND x.blocking = 1
                AND x.resolved_at IS NULL
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
        display_id: row.get(14)?,
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
                display_id: None,
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
                display_id: None,
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
                display_id: None,
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
                display_id: None,
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
        assert!(
            short.contains('-'),
            "should have prefix-hash format: {short}"
        );
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
                display_id: None,
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
                display_id: None,
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

    // -- Wave 5 reader cutover: entity_links tests --
    //
    // These tests validate that the CTE and listing queries correctly use
    // entity_links edges rather than task_deps / tasks.parent_task_id.
    // All edges are wired via the dual-write path (apply_event), not direct INSERTs.

    fn set_parent(conn: &Connection, task_id: &str, parent_id: &str) {
        let ev = TaskEvent::new(
            task_id,
            "user",
            EventType::ParentSet,
            &ParentSetPayload {
                parent_task_id: Some(parent_id.to_string()),
            },
        );
        apply_event(conn, &ev, "").unwrap();
    }

    /// Three-deep parent chain via entity_links parent_of edges.
    /// Grandparent is blocked by a blocks edge. Grandchild must appear in has_blocked_ancestor.
    #[test]
    fn test_ancestor_blocked_via_entity_links() {
        let conn = setup();
        create_task(&conn, "gp", "Grandparent", 1);
        create_task(&conn, "blocker", "Blocker", 2);
        create_task(&conn, "parent", "Parent", 2);
        create_task(&conn, "child", "Child", 3);

        // Wire parent chain via ParentSet (emits entity_links parent_of edges).
        set_parent(&conn, "parent", "gp");
        set_parent(&conn, "child", "parent");

        // Block grandparent via DependencyAdded (emits entity_links blocks edge).
        add_dep(&conn, "gp", "blocker");

        let ready = list_ready(&conn, None).unwrap();
        let ready_ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            !ready_ids.contains(&"child"),
            "grandchild should NOT be ready when grandparent is blocked via entity_links: {ready_ids:?}"
        );

        let blocked = list_blocked(&conn, None).unwrap();
        let blocked_ids: Vec<&str> = blocked.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            blocked_ids.contains(&"child"),
            "grandchild should appear in blocked list via entity_links ancestor CTE: {blocked_ids:?}"
        );

        // Resolve grandparent's blocker — child becomes ready.
        set_status(&conn, "blocker", "done");
        let ready = list_ready(&conn, None).unwrap();
        assert!(
            ready.iter().any(|t| t.task_id == "child"),
            "grandchild should be ready after grandparent's blocker is done"
        );
    }

    /// get_children reads entity_links parent_of edges, returns all children in child_seq order.
    #[test]
    fn test_get_children_via_entity_links() {
        let conn = setup();
        create_task(&conn, "parent", "Parent", 1);
        create_task(&conn, "c1", "Child 1", 2);
        create_task(&conn, "c2", "Child 2", 2);
        create_task(&conn, "c3", "Child 3", 2);

        // Wire via ParentSet to populate entity_links parent_of edges.
        set_parent(&conn, "c1", "parent");
        set_parent(&conn, "c2", "parent");
        set_parent(&conn, "c3", "parent");

        let children = get_children(&conn, "parent").unwrap();
        assert_eq!(children.len(), 3, "expected 3 children via entity_links");
        // Verify child_seq ordering: c1 gets seq=1, c2 seq=2, c3 seq=3.
        assert_eq!(children[0].task_id, "c1");
        assert_eq!(children[1].task_id, "c2");
        assert_eq!(children[2].task_id, "c3");
    }

    /// Task A blocked by task B via entity_links blocks edge — A is excluded from ready set.
    #[test]
    fn test_blocking_dep_via_entity_links() {
        let conn = setup();
        create_task(&conn, "blocker", "Blocker", 2);
        create_task(&conn, "dependent", "Dependent", 1);

        // Wire via DependencyAdded (emits entity_links blocks edge).
        add_dep(&conn, "dependent", "blocker");

        let ready = list_ready(&conn, None).unwrap();
        let ready_ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            !ready_ids.contains(&"dependent"),
            "dependent should NOT be in ready set while blocker is open: {ready_ids:?}"
        );
        assert!(
            ready_ids.contains(&"blocker"),
            "blocker itself should be ready: {ready_ids:?}"
        );

        // Resolve blocker — dependent becomes ready.
        set_status(&conn, "blocker", "done");
        let ready = list_ready(&conn, None).unwrap();
        assert!(
            ready.iter().any(|t| t.task_id == "dependent"),
            "dependent should be ready after blocker is done"
        );
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

    // -- Cross-brain dependency and orphan-dep tests --
    //
    // All brains share one `tasks` table (single-DB model). Cross-brain deps
    // are just rows where `t.brain_id` differs from `dep.brain_id` — the
    // queries work without federation. Orphan deps (depends_on references a
    // nonexistent task_id) must keep the depending task out of the ready list.

    /// Insert a row directly into `task_deps` with FK checks disabled so we
    /// can create orphaned deps (depends_on has no matching tasks row).
    fn insert_orphan_dep(conn: &Connection, task_id: &str, depends_on: &str) {
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO task_deps (task_id, depends_on) VALUES (?1, ?2)",
            rusqlite::params![task_id, depends_on],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    }

    /// Create a task scoped to a specific brain_id. Registers the brain in the
    /// `brains` table first (required by the FK constraint on `tasks.brain_id`).
    fn create_task_in_brain(
        conn: &Connection,
        task_id: &str,
        title: &str,
        priority: i32,
        brain_id: &str,
    ) {
        // Register the brain so the FK on tasks.brain_id is satisfied.
        crate::db::schema::ensure_brain_registered(conn, brain_id, brain_id).unwrap();

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
                display_id: None,
            },
        );
        // Apply with the target brain_id so the task lands in that partition.
        apply_event(conn, &ev, brain_id).unwrap();
    }

    /// A task in brain-a depending on an open task in brain-b is NOT ready.
    #[test]
    fn test_cross_brain_dep_blocks_ready() {
        let conn = setup();
        create_task_in_brain(&conn, "b-blocker", "Blocker in B", 2, "brain-b");
        create_task_in_brain(&conn, "a-dependent", "Dependent in A", 1, "brain-a");

        // Add dep: a-dependent depends on b-blocker.
        add_dep(&conn, "a-dependent", "b-blocker");

        // When querying brain-a without brain filter, a-dependent is NOT ready.
        let ready = list_ready(&conn, None).unwrap();
        let ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            !ids.contains(&"a-dependent"),
            "a-dependent should NOT be ready while cross-brain blocker is open: {ids:?}"
        );
        assert!(
            ids.contains(&"b-blocker"),
            "b-blocker itself should be ready: {ids:?}"
        );
    }

    /// Once the cross-brain blocker is done, the dependent task becomes ready.
    #[test]
    fn test_cross_brain_dep_resolves_on_done() {
        let conn = setup();
        create_task_in_brain(&conn, "b-blocker", "Blocker in B", 2, "brain-b");
        create_task_in_brain(&conn, "a-dependent", "Dependent in A", 1, "brain-a");
        add_dep(&conn, "a-dependent", "b-blocker");

        set_status(&conn, "b-blocker", "done");

        let ready = list_ready(&conn, None).unwrap();
        let ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            ids.contains(&"a-dependent"),
            "a-dependent should be ready after cross-brain blocker done: {ids:?}"
        );
    }

    /// Same resolution path via cancelled.
    #[test]
    fn test_cross_brain_dep_resolves_on_cancelled() {
        let conn = setup();
        create_task_in_brain(&conn, "b-blocker", "Blocker in B", 2, "brain-b");
        create_task_in_brain(&conn, "a-dependent", "Dependent in A", 1, "brain-a");
        add_dep(&conn, "a-dependent", "b-blocker");

        set_status(&conn, "b-blocker", "cancelled");

        let ready = list_ready(&conn, None).unwrap();
        let ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            ids.contains(&"a-dependent"),
            "a-dependent should be ready after cross-brain blocker cancelled: {ids:?}"
        );
    }

    /// An orphaned dep (depends_on references a nonexistent task) keeps the
    /// depending task out of the ready list and in the blocked list.
    #[test]
    fn test_orphan_dep_stays_blocked() {
        let conn = setup();
        create_task(&conn, "dependent", "Has orphan dep", 1);

        // Insert a dep on a nonexistent task — bypasses FK check.
        insert_orphan_dep(&conn, "dependent", "ghost-task");

        // dependent must NOT appear in ready.
        let ready = list_ready(&conn, None).unwrap();
        let ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            !ids.contains(&"dependent"),
            "task with orphan dep should NOT be ready: {ids:?}"
        );

        // dependent MUST appear in blocked.
        let blocked = list_blocked(&conn, None).unwrap();
        let blocked_ids: Vec<&str> = blocked.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            blocked_ids.contains(&"dependent"),
            "task with orphan dep should appear in blocked list: {blocked_ids:?}"
        );
    }

    /// get_dependency_summary counts orphan deps in total_deps and blocking_task_ids.
    #[test]
    fn test_orphan_dep_counted_in_dependency_summary() {
        let conn = setup();
        create_task(&conn, "dependent", "Has orphan dep", 1);
        insert_orphan_dep(&conn, "dependent", "ghost-task");

        let summary = get_dependency_summary(&conn, "dependent").unwrap();
        assert_eq!(summary.total_deps, 1, "orphan dep counts toward total_deps");
        assert_eq!(summary.done_deps, 0, "orphan dep is not done");
        assert!(
            summary
                .blocking_task_ids
                .contains(&"ghost-task".to_string()),
            "orphan dep ID appears in blocking_task_ids: {:?}",
            summary.blocking_task_ids
        );
    }

    /// list_ready_actionable also excludes tasks with orphan deps.
    #[test]
    fn test_orphan_dep_excluded_from_actionable() {
        let conn = setup();
        create_task(&conn, "dependent", "Has orphan dep", 1);
        insert_orphan_dep(&conn, "dependent", "ghost-task");

        let actionable = list_ready_actionable(&conn, None).unwrap();
        let ids: Vec<&str> = actionable.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            !ids.contains(&"dependent"),
            "task with orphan dep should NOT appear in list_ready_actionable: {ids:?}"
        );
    }

    /// Exercises the ANCESTOR_BLOCKED_CTE's own `LEFT JOIN tasks dep ... AND
    /// (dep.task_id IS NULL OR ...)` clause. Without the NULL-check inside
    /// the CTE, a child of a parent whose only blocker is an orphan dep
    /// would slip into the ready list (the outer WHERE catches the parent
    /// itself, but only the CTE catches descendants). A mutation that
    /// removed the NULL-check from the CTE alone (not from the outer
    /// listing queries) would not be caught by `test_orphan_dep_stays_blocked`.
    #[test]
    fn test_child_of_task_with_orphan_dep_not_ready() {
        let conn = setup();
        create_epic(&conn, "parent", "Parent epic", 1);
        insert_orphan_dep(&conn, "parent", "ghost-blocker");
        create_child_task(
            &conn,
            "child",
            "parent",
            "Child of orphan-blocked parent",
            2,
        );

        let ready = list_ready(&conn, None).unwrap();
        assert!(
            !ready.iter().any(|t| t.task_id == "child"),
            "child of a parent blocked by an orphan dep must not be ready"
        );

        let blocked = list_blocked(&conn, None).unwrap();
        assert!(
            blocked.iter().any(|t| t.task_id == "child"),
            "child of a parent blocked by an orphan dep must appear in blocked list"
        );
    }

    // -- External-blocker tests (brn-3a93) --
    //
    // External blockers are rows in `task_external_ids` with `blocking = 1`.
    // An unresolved blocker (`resolved_at IS NULL`) keeps the task out of
    // ready/actionable lists and pulls it into the blocked list. Resolving
    // the blocker (stamping `resolved_at`) moves it back to ready (if no
    // other blockers remain).

    /// Helper: insert a `task_external_ids` row with FK off so we don't have
    /// to register a brain. Used by tests that don't care about brain scoping.
    fn insert_external_id(
        conn: &Connection,
        task_id: &str,
        source: &str,
        external_id: &str,
        blocking: bool,
        resolved_at: Option<i64>,
    ) {
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        conn.execute(
            "INSERT INTO task_external_ids
                 (task_id, source, external_id, imported_at, blocking, resolved_at)
             VALUES (?1, ?2, ?3, 1000, ?4, ?5)
             ON CONFLICT(task_id, source, external_id) DO UPDATE SET
                 blocking = excluded.blocking,
                 resolved_at = excluded.resolved_at",
            rusqlite::params![
                task_id,
                source,
                external_id,
                if blocking { 1 } else { 0 },
                resolved_at,
            ],
        )
        .unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();
    }

    /// A task with an unresolved blocking external_id is NOT ready / NOT actionable.
    #[test]
    fn test_unresolved_external_blocker_excludes_from_ready() {
        let conn = setup();
        create_task(&conn, "t1", "Has external blocker", 1);
        insert_external_id(&conn, "t1", "github", "GH-99", true, None);

        let ready = list_ready(&conn, None).unwrap();
        let ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            !ids.contains(&"t1"),
            "task with unresolved external blocker must NOT be ready: {ids:?}"
        );

        let actionable = list_ready_actionable(&conn, None).unwrap();
        let act_ids: Vec<&str> = actionable.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            !act_ids.contains(&"t1"),
            "task with unresolved external blocker must NOT be actionable: {act_ids:?}"
        );

        let blocked = list_blocked(&conn, None).unwrap();
        let blocked_ids: Vec<&str> = blocked.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            blocked_ids.contains(&"t1"),
            "task with unresolved external blocker must be in blocked: {blocked_ids:?}"
        );
    }

    /// After stamping resolved_at, the task becomes ready.
    #[test]
    fn test_resolved_external_blocker_unblocks_task() {
        let conn = setup();
        create_task(&conn, "t1", "Has external blocker", 1);
        insert_external_id(&conn, "t1", "github", "GH-99", true, None);

        // Initially blocked.
        let ready = list_ready(&conn, None).unwrap();
        assert!(!ready.iter().any(|t| t.task_id == "t1"));

        // Resolve it.
        insert_external_id(&conn, "t1", "github", "GH-99", true, Some(2000));

        let ready = list_ready(&conn, None).unwrap();
        let ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            ids.contains(&"t1"),
            "task should be ready after external blocker resolved: {ids:?}"
        );
    }

    /// Non-blocking external_id rows (the legacy / metadata-only shape) do
    /// NOT gate readiness. This ensures the migration's default of
    /// `blocking = 0` preserves the historical behavior.
    #[test]
    fn test_non_blocking_external_id_does_not_gate_ready() {
        let conn = setup();
        create_task(&conn, "t1", "Has metadata external_id", 1);
        insert_external_id(&conn, "t1", "github", "GH-1", false, None);

        let ready = list_ready(&conn, None).unwrap();
        let ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
        assert!(
            ids.contains(&"t1"),
            "non-blocking external_id must NOT gate readiness: {ids:?}"
        );
    }

    /// Dependency summary keeps the two domains disjoint: `total_deps` /
    /// `done_deps` / `blocking_task_ids` are about `task_deps` only;
    /// external blockers are surfaced solely via
    /// `external_blocker_unresolved_count`. Resolved blockers and metadata
    /// rows are not counted here at all.
    #[test]
    fn test_external_blocker_in_dependency_summary() {
        let conn = setup();
        create_task(&conn, "t1", "Has blockers", 1);
        insert_external_id(&conn, "t1", "github", "GH-A", true, None); // unresolved
        insert_external_id(&conn, "t1", "github", "GH-B", true, Some(2000)); // resolved
        insert_external_id(&conn, "t1", "jira", "PROJ-1", false, None); // metadata only

        let summary = get_dependency_summary(&conn, "t1").unwrap();
        assert_eq!(
            summary.total_deps, 0,
            "total_deps must NOT include external_ids — task_deps only: {summary:?}"
        );
        assert_eq!(
            summary.done_deps, 0,
            "done_deps must NOT include resolved external blockers: {summary:?}"
        );
        assert!(
            summary.blocking_task_ids.is_empty(),
            "external blocker IDs are NOT pushed to blocking_task_ids: {:?}",
            summary.blocking_task_ids
        );
        assert_eq!(
            summary.external_blocker_unresolved_count, 1,
            "only the unresolved blocking external_id should count: {summary:?}"
        );
    }

    /// `count_ready_blocked` must agree with `list_ready` / `list_blocked`
    /// under external blockers.
    #[test]
    fn test_count_ready_blocked_with_external_blockers() {
        let conn = setup();
        create_task(&conn, "t1", "Ready", 2);
        create_task(&conn, "t2", "Has external blocker", 1);
        insert_external_id(&conn, "t2", "github", "GH-99", true, None);

        let (ready, blocked) = count_ready_blocked(&conn, None).unwrap();
        assert_eq!(ready, 1, "only t1 ready");
        assert_eq!(blocked, 1, "t2 blocked by external");
    }

    /// `list_newly_unblocked` must NOT report a task whose only remaining
    /// blocker is an unresolved external blocker, even if an internal dep
    /// was just completed.
    #[test]
    fn test_newly_unblocked_excluded_when_external_blocker_remains() {
        let conn = setup();
        create_task(&conn, "blocker", "Internal blocker", 2);
        create_task(&conn, "dependent", "Has both blockers", 1);
        add_dep(&conn, "dependent", "blocker");
        insert_external_id(&conn, "dependent", "github", "GH-X", true, None);

        // Complete the internal dep.
        set_status(&conn, "blocker", "done");

        let unblocked = list_newly_unblocked(&conn, "blocker").unwrap();
        assert!(
            !unblocked.contains(&"dependent".to_string()),
            "dependent should NOT be newly unblocked while external blocker is unresolved: {unblocked:?}"
        );
    }

    /// `get_external_blockers` returns the blocking subset including history.
    #[test]
    fn test_get_external_blockers_includes_resolved_history() {
        let conn = setup();
        create_task(&conn, "t1", "Has blockers", 1);
        insert_external_id(&conn, "t1", "github", "GH-A", true, None);
        insert_external_id(&conn, "t1", "github", "GH-B", true, Some(5000));
        insert_external_id(&conn, "t1", "jira", "PROJ-1", false, None); // not a blocker

        let blockers = get_external_blockers(&conn, "t1").unwrap();
        assert_eq!(
            blockers.len(),
            2,
            "should return only blocking=1 rows: {blockers:?}"
        );
        assert!(blockers.iter().all(|b| b.blocking));
        // Unresolved should sort first (resolved_at IS NOT NULL ASC -> false (0) before true (1)).
        assert_eq!(blockers[0].external_id, "GH-A");
        assert!(blockers[0].resolved_at.is_none());
        assert_eq!(blockers[1].external_id, "GH-B");
        assert_eq!(blockers[1].resolved_at, Some(5000));
    }

    /// Regression: an internal dep alone still works when no external blockers exist.
    /// (Sanity check that the new clause doesn't break the dep-only path.)
    #[test]
    fn test_external_blocker_clause_does_not_break_dep_only_blocking() {
        let conn = setup();
        create_task(&conn, "blocker", "Blocker", 2);
        create_task(&conn, "dep_only", "Dep-only", 1);
        add_dep(&conn, "dep_only", "blocker");

        let ready = list_ready(&conn, None).unwrap();
        assert!(!ready.iter().any(|t| t.task_id == "dep_only"));

        set_status(&conn, "blocker", "done");
        let unblocked = list_newly_unblocked(&conn, "blocker").unwrap();
        assert_eq!(unblocked, vec!["dep_only"]);
    }

    /// count_ready_blocked is consistent with list_ready/list_blocked under orphan deps.
    #[test]
    fn test_orphan_dep_count_ready_blocked_consistent() {
        let conn = setup();
        create_task(&conn, "t1", "Normal ready", 2);
        create_task(&conn, "t2", "Orphan dep", 1);
        insert_orphan_dep(&conn, "t2", "ghost-task");

        let (ready, blocked) = count_ready_blocked(&conn, None).unwrap();
        assert_eq!(ready, 1, "only t1 should be ready");
        assert_eq!(blocked, 1, "t2 with orphan dep should be blocked");
    }
}
