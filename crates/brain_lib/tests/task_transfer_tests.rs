#![allow(clippy::disallowed_macros, clippy::disallowed_types)]

/// brn-3d7.6 — Task transfer integration tests.
///
/// Covers: happy path, display_id collision, same-brain no-op, concurrent
/// transfers (CAS), replay via task_transferred event, records.brain_id,
/// MCP path.
mod mcp_test_harness;

use brain_lib::tasks::TaskStore;
use brain_lib::tasks::events::{TaskCreatedPayload, TaskEvent, TaskStatus, TaskTransferredPayload};
use brain_persistence::db::Db;
use brain_persistence::db::tasks::projections::apply_event;
use serde_json::json;

// ── helpers ──────────────────────────────────────────────────────────────────

fn open_two_brain_store() -> (TaskStore, String, String) {
    let db = Db::open_in_memory().expect("open in-memory db");
    // Register two brains.
    db.ensure_brain_registered("brain-src", "source-brain")
        .unwrap();
    db.ensure_brain_registered("brain-dst", "dest-brain")
        .unwrap();

    let store = TaskStore::with_brain_id(db, "brain-src", "source-brain").unwrap();
    (store, "brain-src".to_string(), "brain-dst".to_string())
}

fn make_task_in_store(store: &TaskStore, task_id: &str, title: &str) {
    let ev = TaskEvent::from_payload(
        task_id,
        "test",
        TaskCreatedPayload {
            title: title.to_string(),
            description: None,
            priority: 4,
            status: TaskStatus::Open,
            due_ts: None,
            task_type: None,
            assignee: None,
            defer_until: None,
            parent_task_id: None,
            display_id: None,
        },
    );
    store.append(&ev).unwrap();
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn transfer_happy_path_all_tables_updated() {
    let (store, src_brain, dst_brain) = open_two_brain_store();
    make_task_in_store(&store, "task-hp-1", "Happy Path Task");

    // Verify task is in source brain before transfer.
    let row = store.get_task("task-hp-1").unwrap().unwrap();
    assert_eq!(row.task_id, "task-hp-1");

    let result = store
        .transfer_task("task-hp-1", &dst_brain, None)
        .await
        .unwrap();
    assert!(!result.was_no_op);
    assert_eq!(result.from_brain_id, src_brain);
    assert_eq!(result.to_brain_id, dst_brain);

    // tasks.brain_id must be updated.
    let updated_brain_id: String = store
        .db_for_tests()
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT brain_id FROM tasks WHERE task_id = 'task-hp-1'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(updated_brain_id, dst_brain);

    // task_events must have a task_transferred row.
    let event_count: i64 = store
        .db_for_tests()
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM task_events WHERE task_id = 'task-hp-1' AND event_type = 'TaskTransferred'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(event_count, 1, "task_transferred event must be recorded");
}

#[tokio::test]
async fn transfer_same_brain_is_no_op() {
    let (store, src_brain, _dst) = open_two_brain_store();
    make_task_in_store(&store, "task-noop-1", "No-op Task");

    let result = store
        .transfer_task("task-noop-1", &src_brain, None)
        .await
        .unwrap();
    assert!(result.was_no_op, "transfer to same brain must be a no-op");

    // No task_transferred event should exist.
    let event_count: i64 = store
        .db_for_tests()
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM task_events WHERE task_id = 'task-noop-1' AND event_type = 'TaskTransferred'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(event_count, 0);
}

#[tokio::test]
async fn transfer_nonexistent_task_returns_error() {
    let (store, _src, dst_brain) = open_two_brain_store();
    let result = store
        .transfer_task("task-does-not-exist", &dst_brain, None)
        .await;
    assert!(result.is_err(), "transfer of nonexistent task must fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("task not found") || err.contains("not found"),
        "error message: {err}"
    );
}

#[tokio::test]
async fn transfer_display_id_collision_resolved() {
    let (store, _src_brain, dst_brain) = open_two_brain_store();

    // Create two tasks whose blake3 hash collides at length 3 — very unlikely
    // in practice, but we can force a collision by pre-inserting a display_id
    // in the target brain that matches the hash of task-col-1.
    make_task_in_store(&store, "task-col-1", "Collision Source Task");

    // Compute the expected display_id for task-col-1.
    use brain_persistence::db::tasks::queries::blake3_short_hex;
    let full_hex = blake3_short_hex("task-col-1");
    let natural_id = &full_hex[..3];

    // Force a collision by inserting a task in the target brain with that display_id.
    store
        .db_for_tests()
        .with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO tasks (task_id, brain_id, title, status, priority, task_type, created_at, updated_at, display_id) \
                 VALUES ('collision-blocker', ?1, 'blocker', 'open', 4, 'task', 0, 0, ?2)",
                rusqlite::params![dst_brain, natural_id],
            )
            .map_err(brain_persistence::error::BrainCoreError::from)?;
            Ok(())
        })
        .unwrap();

    let result = store
        .transfer_task("task-col-1", &dst_brain, None)
        .await
        .unwrap();

    // The new display_id must differ from the colliding blocker.
    assert_ne!(
        result.to_display_id, natural_id,
        "collision must be resolved — display_id must be extended"
    );
    assert!(
        result.to_display_id.len() > 3,
        "extended display_id must be longer than 3 chars"
    );
}

/// Renamed from `transfer_concurrent_cas_second_call_fails` — the original test
/// verified idempotence (sequential second transfer picks up the new brain_id),
/// not a true concurrent CAS race. The real race is covered by
/// `transfer_concurrent_cas_truly_concurrent` below.
#[tokio::test]
async fn transfer_second_call_picks_up_new_brain_id() {
    let (store, _src_brain, dst_brain) = open_two_brain_store();
    make_task_in_store(&store, "task-cas-1", "CAS Task");

    // First transfer: succeeds.
    let r1 = store
        .transfer_task("task-cas-1", &dst_brain, None)
        .await
        .unwrap();
    assert!(!r1.was_no_op);

    // Register a third brain for the second transfer attempt.
    store
        .db_for_tests()
        .ensure_brain_registered("brain-third", "third-brain")
        .unwrap();

    // Second transfer from the original source brain — the task is now in
    // dst_brain. transfer_task reads current brain_id fresh, so it picks up
    // dst_brain as the current owner and proceeds to brain-third.
    let r2 = store
        .transfer_task("task-cas-1", "brain-third", None)
        .await
        .unwrap();
    assert_eq!(
        r2.from_brain_id, dst_brain,
        "second transfer must read updated brain_id from dst"
    );
}

/// Verifies the CAS clause rejects a concurrent double-transfer.
///
/// Two OS threads both attempt to transfer the same task. A `Barrier` aligns
/// them so the `BEGIN IMMEDIATE` contend on the same write lock. The loser's
/// CAS UPDATE sees 0 rows affected and returns `TaskTransferCasFailed`.
#[test]
fn transfer_concurrent_cas_truly_concurrent() {
    use std::sync::{Arc, Barrier};

    let db = Db::open_in_memory().expect("open in-memory db");
    db.ensure_brain_registered("brain-src", "source-brain")
        .unwrap();
    db.ensure_brain_registered("brain-dst-a", "dest-a")
        .unwrap();
    db.ensure_brain_registered("brain-dst-b", "dest-b")
        .unwrap();

    let store = Arc::new(TaskStore::with_brain_id(db, "brain-src", "source-brain").unwrap());

    // Create the task once.
    make_task_in_store(&store, "task-concurrent-1", "Concurrent CAS Task");

    // Barrier ensures both threads reach transfer_task before either commits.
    let barrier = Arc::new(Barrier::new(2));

    let store_a = Arc::clone(&store);
    let barrier_a = Arc::clone(&barrier);
    let h_a = std::thread::spawn(move || {
        barrier_a.wait();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store_a.transfer_task("task-concurrent-1", "brain-dst-a", None))
    });

    let store_b = Arc::clone(&store);
    let barrier_b = Arc::clone(&barrier);
    let h_b = std::thread::spawn(move || {
        barrier_b.wait();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store_b.transfer_task("task-concurrent-1", "brain-dst-b", None))
    });

    let result_a = h_a.join().expect("thread A panicked");
    let result_b = h_b.join().expect("thread B panicked");

    // Exactly one must succeed; the other must get a CAS failure.
    let successes = [result_a.is_ok(), result_b.is_ok()]
        .iter()
        .filter(|&&ok| ok)
        .count();
    let failures = [result_a.is_err(), result_b.is_err()]
        .iter()
        .filter(|&&err| err)
        .count();

    // The Db write mutex serialises the two BEGIN IMMEDIATE calls — so one
    // acquires the lock first and commits, then the second reads the updated
    // brain_id and its CAS WHERE clause matches 0 rows.
    assert_eq!(successes, 1, "exactly one transfer must succeed");
    assert_eq!(failures, 1, "exactly one transfer must fail with CAS error");

    // Verify the failure is specifically a CAS failure.
    let err_result = if result_a.is_err() { result_a } else { result_b };
    let err = err_result.unwrap_err();
    assert!(
        matches!(err, brain_persistence::error::BrainCoreError::TaskTransferCasFailed(_)),
        "loser must return TaskTransferCasFailed, got: {err}"
    );
}

#[tokio::test]
async fn transfer_records_brain_id_moves_with_task() {
    let (store, _src, dst_brain) = open_two_brain_store();
    make_task_in_store(&store, "task-rec-1", "Records Task");

    // Insert a record linked to the task.
    store
        .db_for_tests()
        .with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO records (record_id, title, kind, status, content_hash, content_size, media_type, task_id, actor, created_at, updated_at, retention_class, pinned, payload_available, content_encoding, brain_id, searchable) \
                 VALUES ('rec-1', 'Test Record', 'snapshot', 'active', 'hash', 4, 'text/plain', 'task-rec-1', 'test', 0, 0, NULL, 0, 1, 'identity', 'brain-src', 1)",
                [],
            )
            .map_err(Into::into)
        })
        .unwrap();

    store
        .transfer_task("task-rec-1", &dst_brain, None)
        .await
        .unwrap();

    let record_brain_id: String = store
        .db_for_tests()
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT brain_id FROM records WHERE record_id = 'rec-1'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(
        record_brain_id, dst_brain,
        "record.brain_id must follow the task"
    );
}

#[tokio::test]
async fn transfer_event_payload_self_contained_for_replay() {
    let (store, src_brain, dst_brain) = open_two_brain_store();
    make_task_in_store(&store, "task-replay-1", "Replay Task");

    let result = store
        .transfer_task("task-replay-1", &dst_brain, None)
        .await
        .unwrap();

    // Fetch the task_transferred event payload from the DB.
    let payload_json: String = store
        .db_for_tests()
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT payload FROM task_events WHERE task_id = 'task-replay-1' AND event_type = 'TaskTransferred'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();

    let payload: TaskTransferredPayload =
        serde_json::from_str(&payload_json).expect("payload must deserialize");

    assert_eq!(payload.from_brain_id, src_brain);
    assert_eq!(payload.to_brain_id, dst_brain);
    assert_eq!(payload.from_display_id, result.from_display_id);
    assert_eq!(payload.to_display_id, result.to_display_id);

    // Verify replay: construct a TaskEvent from the payload and apply it to a fresh DB.
    let fresh_db = Db::open_in_memory().expect("open fresh in-memory db");
    fresh_db
        .ensure_brain_registered(&src_brain, "source-brain")
        .unwrap();
    fresh_db
        .ensure_brain_registered(&dst_brain, "dest-brain")
        .unwrap();

    // First create the task in src brain.
    let create_ev = TaskEvent::from_payload(
        "task-replay-1",
        "test",
        TaskCreatedPayload {
            title: "Replay Task".into(),
            description: None,
            priority: 4,
            status: TaskStatus::Open,
            due_ts: None,
            task_type: None,
            assignee: None,
            defer_until: None,
            parent_task_id: None,
            display_id: Some(result.from_display_id.clone()),
        },
    );
    fresh_db
        .with_write_conn(|conn| apply_event(conn, &create_ev, &src_brain))
        .unwrap();

    // Now replay the transfer event.
    let transfer_ev = TaskEvent::from_payload(
        "task-replay-1",
        "system",
        TaskTransferredPayload {
            from_brain_id: src_brain.clone(),
            to_brain_id: dst_brain.clone(),
            from_display_id: result.from_display_id.clone(),
            to_display_id: result.to_display_id.clone(),
        },
    );
    fresh_db
        .with_write_conn(|conn| apply_event(conn, &transfer_ev, &src_brain))
        .unwrap();

    // After replay the task should be in dst_brain.
    let replayed_brain_id: String = fresh_db
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT brain_id FROM tasks WHERE task_id = 'task-replay-1'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(
        replayed_brain_id, dst_brain,
        "replay must move task to dst brain"
    );

    let replayed_display_id: String = fresh_db
        .with_read_conn(|conn| {
            conn.query_row(
                "SELECT display_id FROM tasks WHERE task_id = 'task-replay-1'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    assert_eq!(replayed_display_id, result.to_display_id);
}

// ── MCP path ──────────────────────────────────────────────────────────────────

use crate::mcp_test_harness::{call_tool, make_test_context};

#[tokio::test]
async fn mcp_tasks_transfer_happy_path() {
    let harness = make_test_context().await;

    // Register a second brain in the test context.
    harness
        .ctx
        .stores
        .tasks
        .db_for_tests()
        .ensure_brain_registered("brain-target", "target-brain")
        .unwrap();

    // Create a task in the current (test) brain.
    let created = call_tool(
        &harness,
        "tasks.apply_event",
        json!({
            "event_type": "task_created",
            "task_id": "mcp-transfer-t1",
            "payload": { "title": "MCP transfer task" }
        }),
    )
    .await;
    assert_ne!(created.is_error, Some(true), "create failed: {:?}", created);

    let transferred = call_tool(
        &harness,
        "tasks.transfer",
        json!({
            "task_id": "mcp-transfer-t1",
            "target_brain": "brain-target"
        }),
    )
    .await;
    assert_ne!(
        transferred.is_error,
        Some(true),
        "transfer failed: {:?}",
        transferred
    );

    let resp: serde_json::Value =
        serde_json::from_str(&transferred.content[0].text).expect("response must be JSON");
    assert_eq!(resp["task_id"], "mcp-transfer-t1");
    assert_eq!(resp["to_brain_id"], "brain-target");
    assert_eq!(resp["was_no_op"], false);
}

#[tokio::test]
async fn mcp_tasks_transfer_unknown_task_returns_error() {
    let harness = make_test_context().await;

    harness
        .ctx
        .stores
        .tasks
        .db_for_tests()
        .ensure_brain_registered("brain-target2", "target-brain-2")
        .unwrap();

    let result = call_tool(
        &harness,
        "tasks.transfer",
        json!({
            "task_id": "nonexistent-task",
            "target_brain": "brain-target2"
        }),
    )
    .await;
    assert_eq!(
        result.is_error,
        Some(true),
        "expected error for unknown task"
    );
}
