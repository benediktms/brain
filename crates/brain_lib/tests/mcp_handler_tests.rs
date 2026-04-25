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
async fn mcp_handler_mem_search_minimal_returns_stubs_within_budget() {
    let ctx = make_test_context().await;
    let indexed = seed_test_chunks(&ctx).await;
    assert!(indexed > 0, "seeding should index at least one chunk");

    let budget = 80;
    let result = call_tool(
        &ctx,
        "memory.search_minimal",
        json!({
            "query": "seeds chunks for MCP handler tests",
            "budget_tokens": budget,
            "k": 5
        }),
    )
    .await;

    let parsed = success_json(&result);
    assert_eq!(parsed["budget_tokens"], budget);
    let used = parsed["used_tokens_est"]
        .as_u64()
        .expect("used_tokens_est u64");
    assert!(used <= budget);

    let results = parsed["results"].as_array().expect("results array");
    assert!(!results.is_empty(), "seeded note should be searchable");
    for stub in results {
        assert!(stub["memory_id"].is_string());
        assert!(stub["title"].is_string());
        assert!(stub["summary"].is_string());
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
async fn mcp_handler_record_create_artifact_and_retrieve() {
    let ctx = make_test_context().await;

    let create = call_tool(
        &ctx,
        "records.create_artifact",
        json!({
            "title": "handler artifact",
            "kind": "document",
            "text": "artifact payload from handler test",
            "tags": ["mcp", "artifact-test"]
        }),
    )
    .await;
    let create_json = success_json(&create);
    let record_id = create_json["record_id"].as_str().expect("record_id string");

    let fetched = call_tool(&ctx, "records.get", json!({ "record_id": record_id })).await;
    let fetched_json = success_json(&fetched);
    assert_eq!(fetched_json["title"], "handler artifact");
    assert_eq!(fetched_json["kind"], "document");

    let content = call_tool(
        &ctx,
        "records.fetch_content",
        json!({ "record_id": record_id }),
    )
    .await;
    let content_json = success_json(&content);
    assert_eq!(content_json["encoding"], "utf-8");
    assert_eq!(content_json["text"], "artifact payload from handler test");
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
    assert!(parsed["tokens"].is_object());
    assert!(parsed["queue_depth"].is_u64());
    assert!(parsed["lancedb_unoptimized_rows"].is_u64());
    assert!(parsed["dual_store_stuck_files"].is_u64());
}
