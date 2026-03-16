//! Integration tests for the workspace model (unified single-DB storage).
//!
//! Validates:
//! 1. Multi-brain task creation and query — brain_id isolation + unfiltered listing.
//! 2. Cross-brain dependencies — tasks in different brain partitions can depend on each other.
//! 3. Unified object dedup — identical content from two brains produces one blob.
//! 4. Brain filtering in stores — `with_brain_id` scopes reads and writes correctly.

use brain_lib::db::Db;
use brain_lib::records::RecordStore;
use brain_lib::records::events::{ContentRefPayload, RecordCreatedPayload, RecordEvent};
use brain_lib::records::objects::ObjectStore;
use brain_lib::records::queries::RecordFilter;
use brain_lib::tasks::TaskStore;
use brain_lib::tasks::events::{
    DependencyPayload, EventType, TaskCreatedPayload, TaskEvent, TaskStatus,
};
use tempfile::TempDir;

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// A minimal content hash used as a placeholder in record creation events.
const PLACEHOLDER_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Create two `TaskStore`s that share a single `Db` — simulating the unified
/// storage model where both brains write to the same SQLite database.
/// Returns the stores and the shared `Db` so callers can create additional stores.
fn make_shared_db_stores(dir: &TempDir) -> (TaskStore, TaskStore, Db) {
    let db = Db::open_in_memory().unwrap();
    let tasks_dir_a = dir.path().join("tasks_a");
    let tasks_dir_b = dir.path().join("tasks_b");
    let store_a = TaskStore::with_brain_id(&tasks_dir_a, db.clone(), "brain-a").unwrap();
    let store_b = TaskStore::with_brain_id(&tasks_dir_b, db.clone(), "brain-b").unwrap();
    (store_a, store_b, db)
}

/// Append a `TaskCreated` event to a store and return the task_id.
fn create_task(store: &TaskStore, task_id: &str, title: &str) {
    let ev = TaskEvent::from_payload(
        task_id,
        "agent",
        TaskCreatedPayload {
            title: title.to_string(),
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
    store.append(&ev).unwrap();
}

/// Create a `RecordStore` and `ObjectStore` pair from the given `Db`,
/// scoped to `brain_id`.
fn make_record_store(dir: &TempDir, db: Db, brain_id: &str) -> (RecordStore, ObjectStore) {
    let records_dir = dir.path().join(format!("records_{brain_id}"));
    // Object store is shared — same path for both brains (unified dedup).
    let objects_dir = dir.path().join("objects");
    let store = RecordStore::with_brain_id(&records_dir, db, brain_id).unwrap();
    let objects = ObjectStore::new(objects_dir).unwrap();
    (store, objects)
}

/// Append a `RecordCreated` event to a `RecordStore`.
fn create_record(store: &RecordStore, record_id: &str, content_hash: &str, size: u64) {
    let ev = RecordEvent::from_payload(
        record_id,
        "agent",
        RecordCreatedPayload {
            title: "Test record".to_string(),
            kind: "report".to_string(),
            content_ref: ContentRefPayload::new(content_hash.to_string(), size, None),
            description: None,
            task_id: None,
            tags: vec![],
            scope_type: None,
            scope_id: None,
            retention_class: None,
            producer: None,
        },
    );
    store.apply_and_append(&ev).unwrap();
}

// ─── 1. Multi-brain task creation and query ──────────────────────────────────

/// Tasks created in separate brain-scoped stores remain isolated when listed
/// through a scoped store, but both are visible when listed through an
/// unscoped store backed by the same DB.
#[test]
fn test_multi_brain_task_isolation() {
    let dir = TempDir::new().unwrap();
    let (store_a, store_b, db_all) = make_shared_db_stores(&dir);

    create_task(&store_a, "task-a1", "Task A1");
    create_task(&store_a, "task-a2", "Task A2");
    create_task(&store_b, "task-b1", "Task B1");

    // Each scoped store sees only its own tasks.
    let tasks_a = store_a.list_all().unwrap();
    assert_eq!(tasks_a.len(), 2, "brain-a should see exactly 2 tasks");
    assert!(tasks_a.iter().all(|t| t.task_id.starts_with("task-a")));

    let tasks_b = store_b.list_all().unwrap();
    assert_eq!(tasks_b.len(), 1, "brain-b should see exactly 1 task");
    assert_eq!(tasks_b[0].task_id, "task-b1");

    // An unscoped store on the same DB sees all three tasks.
    let tasks_dir_all = dir.path().join("tasks_all");
    let store_all = TaskStore::new(&tasks_dir_all, db_all).unwrap();
    let all_tasks = store_all.list_all().unwrap();
    assert_eq!(all_tasks.len(), 3, "unscoped store should see all 3 tasks");
}

/// Tasks listed through a scoped store only return the correct brain's tasks
/// even after interleaved writes from multiple brains.
#[test]
fn test_multi_brain_task_interleaved_writes() {
    let dir = TempDir::new().unwrap();
    let (store_a, store_b, _db) = make_shared_db_stores(&dir);

    // Interleave writes from both brains.
    create_task(&store_a, "a1", "Alpha");
    create_task(&store_b, "b1", "Beta");
    create_task(&store_a, "a2", "Gamma");
    create_task(&store_b, "b2", "Delta");

    let tasks_a = store_a.list_all().unwrap();
    let ids_a: Vec<&str> = tasks_a.iter().map(|t| t.task_id.as_str()).collect();
    assert!(ids_a.contains(&"a1"));
    assert!(ids_a.contains(&"a2"));
    assert!(!ids_a.contains(&"b1"));
    assert!(!ids_a.contains(&"b2"));

    let tasks_b = store_b.list_all().unwrap();
    let ids_b: Vec<&str> = tasks_b.iter().map(|t| t.task_id.as_str()).collect();
    assert!(ids_b.contains(&"b1"));
    assert!(ids_b.contains(&"b2"));
    assert!(!ids_b.contains(&"a1"));
    assert!(!ids_b.contains(&"a2"));
}

// ─── 2. Cross-brain dependencies ─────────────────────────────────────────────

/// A task in brain-b can declare a dependency on a task in brain-a when both
/// share the same underlying DB (unified storage model).
/// The dependency resolves correctly once the blocking task is completed.
#[test]
fn test_cross_brain_dependency_resolves() {
    let dir = TempDir::new().unwrap();
    let (store_a, store_b, _db) = make_shared_db_stores(&dir);

    // brain-a has a prerequisite task.
    create_task(&store_a, "prereq", "Prerequisite in A");

    // brain-b has a task that depends on the brain-a task.
    create_task(&store_b, "dependent", "Dependent in B");

    let dep_ev = TaskEvent::new(
        "dependent",
        "agent",
        EventType::DependencyAdded,
        &DependencyPayload {
            depends_on_task_id: "prereq".to_string(),
        },
    );
    // Cross-brain dep works because both tasks reside in the same DB.
    store_b.append(&dep_ev).unwrap();

    // "dependent" is blocked until "prereq" completes.
    let summary = store_b.get_dependency_summary("dependent").unwrap();
    assert_eq!(summary.total_deps, 1);
    assert_eq!(summary.done_deps, 0);
    assert!(summary.blocking_task_ids.contains(&"prereq".to_string()));

    // Complete the prereq via store_a (same DB).
    let done_ev = TaskEvent::from_payload(
        "prereq",
        "agent",
        brain_lib::tasks::events::StatusChangedPayload {
            new_status: brain_lib::tasks::events::TaskStatus::Done,
        },
    );
    store_a.append(&done_ev).unwrap();

    // Dependency summary should now show it resolved.
    let summary_after = store_b.get_dependency_summary("dependent").unwrap();
    assert_eq!(summary_after.done_deps, 1);
    assert!(summary_after.blocking_task_ids.is_empty());

    // "dependent" should now appear as ready from brain-b's perspective.
    let ready = store_b.list_ready().unwrap();
    let ready_ids: Vec<&str> = ready.iter().map(|t| t.task_id.as_str()).collect();
    assert!(
        ready_ids.contains(&"dependent"),
        "dependent should be ready after prereq done; got: {ready_ids:?}"
    );
}

// ─── 3. Unified object dedup ──────────────────────────────────────────────────

/// Identical content written from brain-a and brain-b shares a single blob in
/// the unified object store. Both records reference the same `content_hash`.
#[test]
fn test_unified_object_dedup() {
    let dir = TempDir::new().unwrap();
    let db = Db::open_in_memory().unwrap();

    let (store_a, objects_a) = make_record_store(&dir, db.clone(), "brain-a");
    let (store_b, _objects_b) = make_record_store(&dir, db, "brain-b");

    // Both stores share the same on-disk object store root.
    let shared_objects = ObjectStore::new(dir.path().join("objects")).unwrap();

    let content = b"shared payload data across brains";

    // brain-a writes the blob.
    let ref_a = objects_a.write(content).unwrap();

    // brain-b writes the same content — must be a no-op (blob already exists).
    let ref_b = shared_objects.write(content).unwrap();

    assert_eq!(
        ref_a.hash, ref_b.hash,
        "same content must produce the same hash"
    );

    // Create records in each brain referencing the shared hash.
    create_record(&store_a, "rec-a1", &ref_a.hash, ref_a.size);
    create_record(&store_b, "rec-b1", &ref_b.hash, ref_b.size);

    // Verify both records point to the same content_hash.
    let row_a = store_a.get_record("rec-a1").unwrap().unwrap();
    let row_b = store_b.get_record("rec-b1").unwrap().unwrap();
    assert_eq!(
        row_a.content_hash, row_b.content_hash,
        "both records must reference the same content hash"
    );

    // Only one blob file exists in the object store.
    let all_hashes = shared_objects.list_all_hashes().unwrap();
    assert_eq!(
        all_hashes.len(),
        1,
        "exactly one blob should exist in the unified object store; got: {all_hashes:?}"
    );
}

// ─── 4. Brain filtering in stores ────────────────────────────────────────────

/// `TaskStore::with_brain_id` scopes `list_all`, `list_ready`, and `list_open`
/// to the configured brain. Tasks belonging to other brains are not visible.
#[test]
fn test_task_store_brain_id_scoping() {
    let dir = TempDir::new().unwrap();

    // Three brains sharing one DB.
    let db = Db::open_in_memory().unwrap();
    let store_x = TaskStore::with_brain_id(&dir.path().join("tx"), db.clone(), "x").unwrap();
    let store_y = TaskStore::with_brain_id(&dir.path().join("ty"), db.clone(), "y").unwrap();
    let store_z = TaskStore::with_brain_id(&dir.path().join("tz"), db, "z").unwrap();

    create_task(&store_x, "x1", "X task 1");
    create_task(&store_x, "x2", "X task 2");
    create_task(&store_y, "y1", "Y task 1");
    create_task(&store_z, "z1", "Z task 1");
    create_task(&store_z, "z2", "Z task 2");
    create_task(&store_z, "z3", "Z task 3");

    assert_eq!(store_x.list_all().unwrap().len(), 2);
    assert_eq!(store_y.list_all().unwrap().len(), 1);
    assert_eq!(store_z.list_all().unwrap().len(), 3);

    // Ready and open also respect the brain_id scope.
    assert_eq!(store_x.list_ready().unwrap().len(), 2);
    assert_eq!(store_y.list_open().unwrap().len(), 1);
    assert_eq!(store_z.list_ready().unwrap().len(), 3);
}

/// `RecordStore::with_brain_id` scopes `list_records` to the configured brain.
/// Records belonging to other brains are not returned.
#[test]
fn test_record_store_brain_id_scoping() {
    let dir = TempDir::new().unwrap();
    let db = Db::open_in_memory().unwrap();

    let (store_a, _) = make_record_store(&dir, db.clone(), "brain-a");
    let (store_b, _) = make_record_store(&dir, db.clone(), "brain-b");

    create_record(&store_a, "ra1", PLACEHOLDER_HASH, 42);
    create_record(&store_a, "ra2", PLACEHOLDER_HASH, 42);
    create_record(&store_b, "rb1", PLACEHOLDER_HASH, 42);

    let filter = RecordFilter {
        kind: None,
        status: None,
        tag: None,
        task_id: None,
        limit: None,
        brain_id: None, // store's own brain_id takes precedence
    };

    let records_a = store_a.list_records(&filter).unwrap();
    assert_eq!(records_a.len(), 2, "brain-a should see 2 records");
    assert!(records_a.iter().all(|r| r.record_id.starts_with("ra")));

    let records_b = store_b.list_records(&filter).unwrap();
    assert_eq!(records_b.len(), 1, "brain-b should see 1 record");
    assert_eq!(records_b[0].record_id, "rb1");

    // Unscoped store sees all records.
    let records_dir_all = dir.path().join("records_all");
    let store_all = RecordStore::new(&records_dir_all, db).unwrap();
    let all_records = store_all.list_records(&filter).unwrap();
    assert_eq!(
        all_records.len(),
        3,
        "unscoped store should see all 3 records"
    );
}

// ─── 5. Project prefix isolation ──────────────────────────────────────────────

/// Two brains sharing a unified DB produce distinct project prefixes via the
/// `brains.prefix` column (set during `ensure_brain_registered`).
///
/// This is the regression test for the prefix collision bug: the old approach
/// used a global `brain_meta.project_prefix` key. The fix stores the prefix
/// per-brain in the `brains` table.
#[test]
fn test_task_store_prefix_isolation_via_brains_table() {
    let dir = TempDir::new().unwrap();
    let unified_db = Db::open_in_memory().unwrap();

    // Register two brains with distinct names — `ensure_brain_registered`
    // computes and stores `brains.prefix` from the name.
    unified_db
        .ensure_brain_registered("brain-a", "auth-service")
        .unwrap();
    unified_db
        .ensure_brain_registered("brain-b", "my-cool-project")
        .unwrap();

    let tasks_dir_a = dir.path().join("tasks_a");
    let tasks_dir_b = dir.path().join("tasks_b");
    let store_a = TaskStore::with_brain_id(&tasks_dir_a, unified_db.clone(), "brain-a").unwrap();
    let store_b = TaskStore::with_brain_id(&tasks_dir_b, unified_db.clone(), "brain-b").unwrap();

    let prefix_a = store_a.get_project_prefix().unwrap();
    let prefix_b = store_b.get_project_prefix().unwrap();

    assert_eq!(prefix_a, "ASR", "auth-service → ASR");
    assert_eq!(prefix_b, "MCP", "my-cool-project → MCP");
    assert_ne!(
        prefix_a, prefix_b,
        "two brains must have distinct prefixes; both got: {prefix_a}"
    );
}

/// Same prefix isolation test for RecordStore.
#[test]
fn test_record_store_prefix_isolation_via_brains_table() {
    let dir = TempDir::new().unwrap();
    let unified_db = Db::open_in_memory().unwrap();

    // Register two brains with distinct names.
    unified_db
        .ensure_brain_registered("brain-a", "auth-service")
        .unwrap();
    unified_db
        .ensure_brain_registered("brain-b", "my-cool-project")
        .unwrap();

    let records_dir_a = dir.path().join("records_a");
    let records_dir_b = dir.path().join("records_b");
    let store_a =
        RecordStore::with_brain_id(&records_dir_a, unified_db.clone(), "brain-a").unwrap();
    let store_b =
        RecordStore::with_brain_id(&records_dir_b, unified_db.clone(), "brain-b").unwrap();

    let prefix_a = store_a.get_project_prefix().unwrap();
    let prefix_b = store_b.get_project_prefix().unwrap();

    assert_eq!(prefix_a, "ASR", "auth-service → ASR");
    assert_eq!(prefix_b, "MCP", "my-cool-project → MCP");
    assert_ne!(
        prefix_a, prefix_b,
        "two brains must have distinct record prefixes; both got: {prefix_a}"
    );
}

/// With `brains.prefix`, each brain gets its own prefix from the `brains` table.
/// No `meta_db` is needed — the unified DB stores per-brain prefixes.
#[test]
fn test_prefix_no_collision_without_meta_db() {
    let dir = TempDir::new().unwrap();
    let unified_db = Db::open_in_memory().unwrap();

    let tasks_dir_a = dir.path().join("tasks_a");
    let tasks_dir_b = dir.path().join("tasks_b");

    // `with_brain_id` calls `ensure_brain_registered(brain_id, brain_id)`,
    // so brains "brain-a" and "brain-b" get distinct prefixes.
    let store_a = TaskStore::with_brain_id(&tasks_dir_a, unified_db.clone(), "brain-a").unwrap();
    let store_b = TaskStore::with_brain_id(&tasks_dir_b, unified_db.clone(), "brain-b").unwrap();

    let prefix_a = store_a.get_project_prefix().unwrap();
    let prefix_b = store_b.get_project_prefix().unwrap();

    // With brains.prefix, each brain gets its own prefix — no collision.
    assert_ne!(
        prefix_a, prefix_b,
        "two brains should have distinct prefixes; a={prefix_a}, b={prefix_b}"
    );
}

// ─── 6. McpContext with unified DB ────────────────────────────────────────────

/// `McpContext` uses a single `db` handle for all tables.
///
/// Verifies that tasks written via the TaskStore are correctly scoped to the
/// brain_id and are not visible to a different brain.
#[tokio::test]
async fn test_mcp_context_unified_db_task_scoping() {
    use std::sync::Arc;

    use brain_lib::db::Db;
    use brain_lib::mcp::McpContext;
    use brain_lib::mcp::tools::ToolRegistry;
    use brain_lib::metrics::Metrics;
    use brain_lib::records::RecordStore;
    use brain_lib::records::objects::ObjectStore;
    use brain_lib::tasks::TaskStore;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();

    // Single shared DB — tasks and records share the same handle.
    let db = Db::open_in_memory().unwrap();

    let brain_id_a = "brain-id-alpha";
    let brain_id_b = "brain-id-beta";

    let tasks_dir_a = tmp.path().join("tasks_a");
    let tasks_dir_b = tmp.path().join("tasks_b");
    let records_dir_a = tmp.path().join("records_a");
    let objects_dir = tmp.path().join("objects");

    // Both TaskStores use the same db but different brain_ids.
    let tasks_a = TaskStore::with_brain_id(&tasks_dir_a, db.clone(), brain_id_a).unwrap();
    let tasks_b = TaskStore::with_brain_id(&tasks_dir_b, db.clone(), brain_id_b).unwrap();
    let records_a = RecordStore::with_brain_id(&records_dir_a, db.clone(), brain_id_a).unwrap();
    let objects = ObjectStore::new(&objects_dir).unwrap();

    // Build context for brain-a.
    let ctx_a = McpContext {
        db: db.clone(),
        store: None,
        writable_store: None,
        embedder: None,
        tasks: tasks_a,
        records: records_a,
        objects,
        metrics: Arc::new(Metrics::new()),
        brain_home: tmp.path().to_path_buf(),
        brain_name: "alpha".to_string(),
        brain_id: brain_id_a.to_string(),
    };

    // Create a task for brain-a via MCP tool.
    let registry = ToolRegistry::new();
    let result = registry
        .dispatch(
            "tasks.apply_event",
            serde_json::json!({
                "event_type": "task_created",
                "task_id": "alpha-task-1",
                "payload": { "title": "Alpha brain task", "priority": 2 }
            }),
            &ctx_a,
        )
        .await;
    assert!(
        result.is_error.is_none(),
        "task creation should succeed; got: {}",
        result.content[0].text
    );

    // brain-b's TaskStore (same db, different brain_id) must not see alpha's task.
    let records_dir_b2 = tmp.path().join("records_b2");
    let records_b = RecordStore::with_brain_id(&records_dir_b2, db.clone(), brain_id_b).unwrap();
    let objects_b = ObjectStore::new(tmp.path().join("objects_b")).unwrap();
    let ctx_b = McpContext {
        db: db.clone(),
        store: None,
        writable_store: None,
        embedder: None,
        tasks: tasks_b,
        records: records_b,
        objects: objects_b,
        metrics: Arc::new(Metrics::new()),
        brain_home: tmp.path().to_path_buf(),
        brain_name: "beta".to_string(),
        brain_id: brain_id_b.to_string(),
    };

    let list_result = registry
        .dispatch(
            "tasks.list",
            serde_json::json!({ "status": "open" }),
            &ctx_b,
        )
        .await;
    assert!(
        list_result.is_error.is_none(),
        "task list for beta should succeed; got: {}",
        list_result.content[0].text
    );
    let parsed: serde_json::Value = serde_json::from_str(&list_result.content[0].text).unwrap();
    let tasks_array = parsed["tasks"].as_array().unwrap();
    assert!(
        tasks_array.is_empty(),
        "beta brain must not see alpha's tasks; got: {tasks_array:?}"
    );
}
