#![allow(clippy::disallowed_macros, clippy::disallowed_types)]

mod mcp_test_harness;

use crate::mcp_test_harness::*;
use brain_lib::mcp::protocol::ToolCallResult;
use serde_json::{Value, json};

fn success_json(result: &ToolCallResult) -> Value {
    assert_ne!(
        result.is_error,
        Some(true),
        "tool returned error: {}",
        result.content[0].text
    );
    serde_json::from_str(&result.content[0].text).expect("tool output should be JSON")
}

#[tokio::test]
async fn mcp_handler_task_apply_event_create_update_dependency_comment() {
    let ctx = make_test_context().await;

    let created = call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "task_created",
            "task_id": "mcp-handler-t1",
            "payload": { "title": "handler apply event task" }
        }),
    )
    .await;
    let created_json = success_json(&created);
    assert_eq!(created_json["task"]["title"], "handler apply event task");

    let status_changed = call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "status_changed",
            "task_id": "mcp-handler-t1",
            "payload": { "new_status": "in_progress" }
        }),
    )
    .await;
    let status_json = success_json(&status_changed);
    assert_eq!(status_json["task"]["status"], "in_progress");

    let second = call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "task_created",
            "task_id": "mcp-handler-t2",
            "payload": { "title": "dependent task" }
        }),
    )
    .await;
    success_json(&second);

    let dep_added = call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "dependency_added",
            "task_id": "mcp-handler-t2",
            "payload": { "depends_on_task_id": "mcp-handler-t1" }
        }),
    )
    .await;
    success_json(&dep_added);

    let comment_added = call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "comment_added",
            "task_id": "mcp-handler-t2",
            "payload": { "body": "dependency added and verified" }
        }),
    )
    .await;
    success_json(&comment_added);

    let fetched = call_tool(&ctx, "tasks.get", json!({ "task_id": "mcp-handler-t2" })).await;
    let fetched_json = success_json(&fetched);
    let comments = fetched_json["comments"].as_array().expect("comments array");
    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0]["body"], "dependency added and verified");

    let blocked_by = fetched_json["blocked_by"]
        .as_array()
        .expect("blocked_by array");
    assert_eq!(blocked_by.len(), 1);
}

#[tokio::test]
async fn mcp_handler_task_create_validation_default_priority_cross_brain() {
    let ctx = make_test_context().await;

    let invalid = call_tool(&ctx, "tasks.create", json!({ "priority": 1 })).await;
    assert_eq!(invalid.is_error, Some(true));
    assert!(invalid.content[0].text.contains("title"));

    let local = call_tool(
        &ctx,
        "tasks.create",
        json!({ "title": "default priority task" }),
    )
    .await;
    let local_json = success_json(&local);
    assert_eq!(local_json["task"]["priority"], 4);

    ctx.ctx
        .stores
        .db_for_tests()
        .ensure_brain_registered("brain-remote-1", "remote-brain")
        .expect("register remote brain");

    let remote = call_tool(
        &ctx,
        "tasks.create",
        json!({ "title": "remote task", "brain": "remote-brain" }),
    )
    .await;
    let remote_json = success_json(&remote);
    assert_eq!(remote_json["remote_brain_name"], "remote-brain");
    assert_eq!(remote_json["remote_brain_id"], "brain-remote-1");
    assert!(remote_json["remote_task_id"].is_string());
}

#[tokio::test]
async fn mcp_handler_task_list_filters_status_priority_label_search() {
    let ctx = make_test_context().await;

    success_json(
        &call_tool(
            &ctx,
            "tasks.apply_event",
            json!({
                "event_type": "task_created",
                "task_id": "list-open-labeled",
                "payload": {
                    "title": "alpha filtering task",
                    "priority": 1,
                    "description": "search token alpha"
                }
            }),
        )
        .await,
    );

    success_json(
        &call_tool(
            &ctx,
            "tasks.apply_event",
            json!({
                "event_type": "label_added",
                "task_id": "list-open-labeled",
                "payload": { "label": "urgent" }
            }),
        )
        .await,
    );

    success_json(
        &call_tool(
            &ctx,
            "tasks.apply_event",
            json!({
                "event_type": "task_created",
                "task_id": "list-done",
                "payload": { "title": "beta task", "priority": 2 }
            }),
        )
        .await,
    );

    success_json(
        &call_tool(
            &ctx,
            "tasks.apply_event",
            json!({
                "event_type": "status_changed",
                "task_id": "list-done",
                "payload": { "new_status": "done" }
            }),
        )
        .await,
    );

    let by_status = success_json(&call_tool(&ctx, "tasks.list", json!({ "status": "done" })).await);
    assert_eq!(by_status["count"], 1);
    assert_eq!(by_status["tasks"][0]["title"], "beta task");

    let by_priority = success_json(&call_tool(&ctx, "tasks.list", json!({ "priority": 1 })).await);
    assert_eq!(by_priority["count"], 1);
    assert_eq!(by_priority["tasks"][0]["title"], "alpha filtering task");

    let by_label = success_json(&call_tool(&ctx, "tasks.list", json!({ "label": "urgent" })).await);
    assert_eq!(by_label["count"], 1);
    assert_eq!(by_label["tasks"][0]["title"], "alpha filtering task");

    let by_search =
        success_json(&call_tool(&ctx, "tasks.list", json!({ "search": "alpha" })).await);
    assert_eq!(by_search["count"], 1);
    assert_eq!(by_search["tasks"][0]["title"], "alpha filtering task");
}

#[tokio::test]
async fn mcp_handler_mem_retrieve_returns_results() {
    let ctx = make_test_context().await;
    let indexed = seed_test_chunks(&ctx).await;
    assert!(indexed > 0, "seeding should index at least one chunk");

    let result = call_tool(
        &ctx,
        "memory.retrieve",
        json!({
            "query": "seeds chunks for MCP handler tests",
            "lod": "L0",
            "count": 5
        }),
    )
    .await;

    let parsed = success_json(&result);
    assert!(
        parsed["result_count"].as_u64().unwrap_or(0) > 0,
        "seeded note should be searchable: {parsed}"
    );

    let results = parsed["results"].as_array().expect("results array");
    assert!(!results.is_empty(), "seeded note should produce results");
    for item in results {
        assert!(item["uri"].is_string(), "result missing uri: {item}");
        assert!(item["title"].is_string(), "result missing title: {item}");
        assert!(
            item["content"].is_string(),
            "result missing content: {item}"
        );
        assert!(item["kind"].is_string(), "result missing kind: {item}");
        assert!(item["lod"].is_string(), "result missing lod: {item}");
    }
}

#[tokio::test]
async fn mcp_handler_mem_write_episode_writes_and_is_queryable() {
    let ctx = make_test_context().await;

    let phrase = "handler episode phrase q9x";
    let write = call_tool(
        &ctx,
        "memory.write_episode",
        json!({
            "goal": format!("goal {phrase}"),
            "actions": "performed handler-level test action",
            "outcome": "episode persisted",
            "tags": ["mcp", "handler-tests"],
            "importance": 0.9
        }),
    )
    .await;
    let write_json = success_json(&write);
    assert_eq!(write_json["status"], "stored");
    let summary_id = write_json["summary_id"]
        .as_str()
        .expect("summary_id should be string");

    let reflect = call_tool(
        &ctx,
        "memory.reflect",
        json!({ "mode": "prepare", "topic": phrase, "budget_tokens": 600 }),
    )
    .await;
    let reflect_json = success_json(&reflect);
    let episodes = reflect_json["episodes"].as_array().expect("episodes array");
    assert!(
        episodes.iter().any(|ep| ep["summary_id"] == summary_id),
        "new episode should be queryable in memory.reflect prepare"
    );
}

#[tokio::test]
async fn mcp_handler_task_get_returns_expected_fields_by_id() {
    let ctx = make_test_context().await;

    let created = call_tool(
        &ctx,
        "tasks.create",
        json!({
            "title": "get task fixture",
            "description": "full task get verification",
            "priority": 1,
            "task_type": "bug",
            "assignee": "alice"
        }),
    )
    .await;
    let created_json = success_json(&created);
    let task_id = created_json["task_id"].as_str().expect("task_id string");

    success_json(
        &call_tool(
            &ctx,
            "tasks.apply_event",
            json!({
                "event_type": "label_added",
                "task_id": task_id,
                "payload": { "label": "urgent" }
            }),
        )
        .await,
    );

    success_json(
        &call_tool(
            &ctx,
            "tasks.apply_event",
            json!({
                "event_type": "comment_added",
                "task_id": task_id,
                "payload": { "body": "task get comment" }
            }),
        )
        .await,
    );

    let fetched = call_tool(&ctx, "tasks.get", json!({ "task_id": task_id })).await;
    let task = success_json(&fetched);

    assert_eq!(task["task_id"], task_id);
    assert_eq!(task["title"], "get task fixture");
    assert_eq!(task["description"], "full task get verification");
    assert_eq!(task["priority"], 1);
    assert_eq!(task["task_type"], "bug");
    assert_eq!(task["assignee"], "alice");
    assert!(
        task["labels"]
            .as_array()
            .expect("labels array")
            .contains(&json!("urgent"))
    );
    assert_eq!(
        task["comments"].as_array().expect("comments array").len(),
        1
    );
    assert!(task["dependency_summary"].is_object());
    assert!(task["linked_notes"].is_array());
    assert!(task["uri"].is_string());
}

#[tokio::test]
async fn mcp_handler_record_create_document_and_retrieve() {
    let ctx = make_test_context().await;

    let create = call_tool(
        &ctx,
        "records.create_document",
        json!({
            "title": "handler document",
            "text": "document payload from handler test",
            "tags": ["mcp", "artifact-test"]
        }),
    )
    .await;
    let create_json = success_json(&create);
    let record_id = create_json["record_id"].as_str().expect("record_id string");

    let fetched = call_tool(&ctx, "records.get", json!({ "record_id": record_id })).await;
    let fetched_json = success_json(&fetched);
    assert_eq!(fetched_json["title"], "handler document");
    assert_eq!(fetched_json["kind"], "document");

    let content = call_tool(
        &ctx,
        "records.fetch_content",
        json!({ "record_id": record_id }),
    )
    .await;
    let content_json = success_json(&content);
    assert_eq!(content_json["encoding"], "utf-8");
    assert_eq!(content_json["text"], "document payload from handler test");
}

#[tokio::test]
async fn mcp_handler_status_returns_expected_json_fields() {
    let ctx = make_test_context().await;

    let result = call_tool(&ctx, "status", json!({})).await;
    let parsed = success_json(&result);

    assert!(parsed["uptime_seconds"].is_u64());
    assert!(parsed["indexing_latency"]["p50_us"].is_u64());
    assert!(parsed["indexing_latency"]["p95_us"].is_u64());
    assert!(parsed["query_latency"]["p50_us"].is_u64());
    assert!(parsed["query_latency"]["p95_us"].is_u64());
    assert!(parsed["stale_hashes_prevented"].is_u64());
    assert!(parsed["queue_depth"].is_u64());
    assert!(parsed["lancedb_unoptimized_rows"].is_u64());
    assert!(parsed["lancedb_optimize_failures"].is_u64());
    assert!(parsed["dual_store_stuck_files"].is_u64());
    // indexing_errors and query_errors added in brn-83a.8 status refactor
    assert!(parsed["indexing_errors"].is_u64());
    assert!(parsed["query_errors"].is_u64());
}

// ---------------------------------------------------------------------------
// Cross-brain dep + orphan-dep readiness tests (brn-6f4)
// ---------------------------------------------------------------------------
//
// The persistence layer uses a LEFT JOIN so that:
//   (a) Cross-brain deps (dep in different brain partition, same DB) block correctly.
//   (b) Orphan deps (depends_on references a nonexistent task_id) stay blocked.
//
// These tests exercise the MCP surface — tasks.next, tasks.list, tasks.get —
// and confirm the fix propagates end-to-end.

/// Inject an orphan dep row directly (bypassing the event layer which enforces
/// task_exists). FK checks are disabled for the insert.
///
/// Mirrors what dual-write would produce by writing into both `task_deps`
/// (legacy) and `entity_links` (Wave 5 reader source). Both tables must stay
/// consistent for hot-path readers to behave correctly during the dual-write
/// window.
fn inject_orphan_dep(db: &brain_persistence::db::Db, task_id: &str, depends_on: &str) {
    db.with_write_conn(|conn| {
        // Restore FK enforcement unconditionally — `?` on the INSERT would
        // otherwise return the pooled connection with FK disabled, polluting
        // every subsequent caller of the pool.
        conn.execute_batch("PRAGMA foreign_keys = OFF")?;
        let task_deps_result = conn.execute(
            "INSERT OR IGNORE INTO task_deps (task_id, depends_on) VALUES (?1, ?2)",
            rusqlite::params![task_id, depends_on],
        );
        let entity_links_result = conn.execute(
            "INSERT OR IGNORE INTO entity_links \
                 (id, from_type, from_id, to_type, to_id, edge_kind, created_at, brain_scope) \
             VALUES \
                 (lower(hex(randomblob(16))), 'TASK', ?1, 'TASK', ?2, 'blocks', \
                  strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), NULL)",
            rusqlite::params![task_id, depends_on],
        );
        conn.execute_batch("PRAGMA foreign_keys = ON")?;
        task_deps_result?;
        entity_links_result?;
        Ok(())
    })
    .unwrap();
}

/// `tasks.next` must exclude a task that has a dep on a nonexistent (orphan) task.
#[tokio::test]
async fn mcp_tasks_next_excludes_task_with_orphan_dep() {
    let ctx = make_test_context().await;

    // Create a task with no deps — should be ready.
    call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "task_created",
            "task_id": "orphan-t1",
            "payload": { "title": "Ready task", "priority": 2 }
        }),
    )
    .await;

    // Create a task that will get an orphan dep injected.
    call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "task_created",
            "task_id": "orphan-t2",
            "payload": { "title": "Has orphan dep", "priority": 1 }
        }),
    )
    .await;

    // Inject orphan dep (ghost-task doesn't exist).
    inject_orphan_dep(ctx.ctx.stores.db_for_tests(), "orphan-t2", "ghost-task");

    let result = call_tool(&ctx, "tasks.next", json!({ "k": 10 })).await;
    let parsed = success_json(&result);

    let tasks: Vec<&Value> = parsed["results"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|g| g["tasks"].as_array().unwrap().iter())
        .collect();

    let titles: Vec<&str> = tasks.iter().filter_map(|t| t["title"].as_str()).collect();

    assert!(
        !titles.contains(&"Has orphan dep"),
        "task with orphan dep must NOT appear in tasks.next: {titles:?}"
    );
    assert!(
        titles.contains(&"Ready task"),
        "ready task must appear in tasks.next: {titles:?}"
    );

    // Counts should reflect one blocked, one ready.
    assert_eq!(
        parsed["ready_count"], 1,
        "ready_count should be 1: {parsed}"
    );
    assert_eq!(
        parsed["blocked_count"], 1,
        "blocked_count should be 1: {parsed}"
    );
}

/// `tasks.list` with `status: "blocked"` includes tasks with orphan deps;
/// `status: "ready"` excludes them.
#[tokio::test]
async fn mcp_tasks_list_orphan_dep_in_blocked_not_in_ready() {
    let ctx = make_test_context().await;

    call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "task_created",
            "task_id": "list-t1",
            "payload": { "title": "Orphan dep task", "priority": 1 }
        }),
    )
    .await;

    inject_orphan_dep(ctx.ctx.stores.db_for_tests(), "list-t1", "nonexistent-task");

    // Check "ready" list — must not include list-t1.
    let ready_result = call_tool(
        &ctx,
        "tasks.list",
        json!({ "status": "ready", "limit": 100 }),
    )
    .await;
    let ready_json = success_json(&ready_result);
    let ready_tasks = ready_json["tasks"].as_array().unwrap();
    let ready_ids: Vec<&str> = ready_tasks
        .iter()
        .filter_map(|t| t["task_id"].as_str())
        .collect();
    assert!(
        !ready_ids.iter().any(|id| id.contains("list-t1")),
        "task with orphan dep must NOT appear in ready list: {ready_ids:?}"
    );

    // Check "blocked" list — must include list-t1.
    let blocked_result = call_tool(
        &ctx,
        "tasks.list",
        json!({ "status": "blocked", "limit": 100 }),
    )
    .await;
    let blocked_json = success_json(&blocked_result);
    let blocked_tasks = blocked_json["tasks"].as_array().unwrap();
    let blocked_titles: Vec<&str> = blocked_tasks
        .iter()
        .filter_map(|t| t["title"].as_str())
        .collect();
    assert!(
        blocked_titles.contains(&"Orphan dep task"),
        "task with orphan dep must appear in blocked list: {blocked_titles:?}"
    );
}

/// `tasks.get` dependency_summary includes orphan dep in total_deps and blocking_task_ids.
#[tokio::test]
async fn mcp_tasks_get_orphan_dep_in_dependency_summary() {
    let ctx = make_test_context().await;

    call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "task_created",
            "task_id": "get-t1",
            "payload": { "title": "Task with orphan dep", "priority": 1 }
        }),
    )
    .await;

    inject_orphan_dep(ctx.ctx.stores.db_for_tests(), "get-t1", "phantom-task");

    let result = call_tool(&ctx, "tasks.get", json!({ "task_id": "get-t1" })).await;
    let parsed = success_json(&result);

    let dep_summary = &parsed["dependency_summary"];
    assert_eq!(
        dep_summary["total_deps"], 1,
        "orphan dep must count in total_deps: {dep_summary}"
    );
    assert_eq!(
        dep_summary["done_deps"], 0,
        "orphan dep is not done: {dep_summary}"
    );
    let blocking = dep_summary["blocking_task_ids"]
        .as_array()
        .expect("blocking_task_ids is array");
    assert_eq!(
        blocking.len(),
        1,
        "orphan dep appears in blocking_task_ids: {dep_summary}"
    );
    // The blocking ID is the raw orphan task ID (compact_id falls back to raw when not in DB).
    let blocking_id = blocking[0].as_str().unwrap();
    assert!(
        blocking_id.contains("phantom-task") || blocking_id == "phantom-task",
        "blocking_task_ids contains orphan dep ID: {blocking_id}"
    );
}

/// `tasks.apply_event` accepts `external_blocker_added`; the task is then
/// excluded from `tasks.list({status: "ready"})` and `tasks.get` returns the
/// blocker in its `external_blockers` field. After `external_blocker_resolved`
/// the task is ready again.
#[tokio::test]
async fn mcp_external_blocker_gates_readiness() {
    let ctx = make_test_context().await;

    call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "task_created",
            "task_id": "blk-t1",
            "payload": { "title": "Awaits external sign-off", "priority": 1 }
        }),
    )
    .await;

    // Add an unresolved external blocker.
    call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "external_blocker_added",
            "task_id": "blk-t1",
            "payload": {
                "source": "jira",
                "external_id": "PLAT-42",
                "external_url": "https://example.atlassian.net/browse/PLAT-42"
            }
        }),
    )
    .await;

    // Task must not be in ready list.
    let ready_result = call_tool(
        &ctx,
        "tasks.list",
        json!({ "status": "ready", "limit": 100 }),
    )
    .await;
    let parsed_ready = success_json(&ready_result);
    let ready_titles: Vec<&str> = parsed_ready["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["title"].as_str())
        .collect();
    assert!(
        !ready_titles.contains(&"Awaits external sign-off"),
        "task with unresolved external blocker must not be ready: {ready_titles:?}"
    );

    // tasks.get exposes the external blocker.
    let get_result = call_tool(&ctx, "tasks.get", json!({ "task_id": "blk-t1" })).await;
    let parsed_get = success_json(&get_result);
    let blockers = parsed_get["external_blockers"]
        .as_array()
        .expect("external_blockers is array");
    assert_eq!(
        blockers.len(),
        1,
        "external_blockers should have one entry: {parsed_get}"
    );
    assert_eq!(blockers[0]["source"], "jira");
    assert_eq!(blockers[0]["external_id"], "PLAT-42");
    assert!(
        blockers[0]["resolved_at"].is_null(),
        "unresolved blocker has null resolved_at: {}",
        blockers[0]
    );

    // Resolve the blocker.
    call_tool(
        &ctx,
        "tasks.apply_event",
        json!({
            "event_type": "external_blocker_resolved",
            "task_id": "blk-t1",
            "payload": { "source": "jira", "external_id": "PLAT-42" }
        }),
    )
    .await;

    // Task is now ready.
    let ready_after = call_tool(
        &ctx,
        "tasks.list",
        json!({ "status": "ready", "limit": 100 }),
    )
    .await;
    let parsed_after = success_json(&ready_after);
    let after_titles: Vec<&str> = parsed_after["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["title"].as_str())
        .collect();
    assert!(
        after_titles.contains(&"Awaits external sign-off"),
        "task should be ready after blocker resolved: {after_titles:?}"
    );
}
