//! TDD integration tests — `brain://` URI contract for MCP tools.
//!
//! RED PHASE: These tests document expected behavior that is not yet
//! implemented. They verify:
//!
//! 1. Output: `tasks.create` response includes a `uri` field of the form
//!    `brain://<brain>/task/<id>`.
//! 2. Output: `records.create_artifact` response includes a `uri` field of
//!    the form `brain://<brain>/record/<id>`.
//! 3. Input: `tasks.get` accepts a `brain://` URI as `task_id` and resolves
//!    the correct task.
//! 4. Input: `records.get` accepts a `brain://` URI as `record_id` and
//!    resolves the correct record.

use std::sync::Arc;

use brain_lib::mcp::McpContext;
use brain_lib::mcp::tools::ToolRegistry;
use brain_lib::metrics::Metrics;
use brain_lib::stores::BrainStores;
use serde_json::{Value, json};

// ─── Helpers ──────────────────────────────────────────────────────────────────

async fn make_ctx() -> (tempfile::TempDir, McpContext) {
    let (tmp, stores) = BrainStores::in_memory().unwrap();
    let ctx = McpContext {
        stores,
        search: None,
        writable_store: None,
        metrics: Arc::new(Metrics::new()),
    };
    (tmp, ctx)
}

fn parse_response(result: &brain_lib::mcp::protocol::ToolCallResult) -> Value {
    assert!(
        result.is_error.is_none(),
        "tool returned error: {}",
        result.content[0].text
    );
    serde_json::from_str(&result.content[0].text).expect("response must be valid JSON")
}

// ─── Output tests: uri field present in creation responses ───────────────────

/// `tasks.create` must include a `uri` field shaped `brain://<brain>/task/<id>`.
#[tokio::test]
async fn test_tasks_create_returns_uri_field() {
    let (_tmp, ctx) = make_ctx().await;
    let registry = ToolRegistry::new();

    let result = registry
        .dispatch("tasks.create", json!({ "title": "URI test task" }), &ctx)
        .await;

    let parsed = parse_response(&result);

    // The uri field must be present.
    assert!(
        parsed["uri"].is_string(),
        "expected `uri` string field in tasks.create response, got: {parsed}"
    );

    let uri = parsed["uri"].as_str().unwrap();
    let brain_name = ctx.brain_name();
    let task_id = parsed["task_id"].as_str().expect("task_id must be present");

    // URI must follow brain://<brain>/task/<id> format.
    let expected = format!("brain://{brain_name}/task/{task_id}");
    assert_eq!(
        uri, expected,
        "uri field does not match expected format: got {uri:?}, want {expected:?}"
    );
}

/// `records.create_artifact` must include a `uri` field shaped
/// `brain://<brain>/record/<id>`.
#[tokio::test]
async fn test_records_create_artifact_returns_uri_field() {
    let (_tmp, ctx) = make_ctx().await;
    let registry = ToolRegistry::new();

    let result = registry
        .dispatch(
            "records.create_artifact",
            json!({
                "title": "URI test artifact",
                "kind": "document",
                "text": "probe data"
            }),
            &ctx,
        )
        .await;

    let parsed = parse_response(&result);

    assert!(
        parsed["uri"].is_string(),
        "expected `uri` string field in records.create_artifact response, got: {parsed}"
    );

    let uri = parsed["uri"].as_str().unwrap();
    let brain_name = ctx.brain_name();
    let record_id = parsed["record_id"].as_str().expect("record_id must be present");

    let expected = format!("brain://{brain_name}/record/{record_id}");
    assert_eq!(
        uri, expected,
        "uri field does not match expected format: got {uri:?}, want {expected:?}"
    );
}

// ─── Input tests: brain:// accepted as task_id / record_id ───────────────────

/// `tasks.get` must accept a `brain://` URI as the `task_id` parameter and
/// return the correct task.
#[tokio::test]
async fn test_tasks_get_accepts_brain_uri_as_task_id() {
    let (_tmp, ctx) = make_ctx().await;
    let registry = ToolRegistry::new();

    // Create a task first.
    let create_result = registry
        .dispatch("tasks.create", json!({ "title": "URI input task" }), &ctx)
        .await;
    let created = parse_response(&create_result);
    let task_id = created["task_id"].as_str().expect("task_id must be present");
    let brain_name = ctx.brain_name();

    // Construct a brain:// URI for the task.
    let uri = format!("brain://{brain_name}/task/{task_id}");

    // tasks.get should resolve the URI and return the task.
    let get_result = registry
        .dispatch("tasks.get", json!({ "task_id": uri }), &ctx)
        .await;

    let parsed = parse_response(&get_result);
    assert_eq!(
        parsed["task"]["title"], "URI input task",
        "tasks.get via brain:// URI must return the correct task; got: {parsed}"
    );
}

/// `records.get` must accept a `brain://` URI as `record_id` and return the
/// correct record.
#[tokio::test]
async fn test_records_get_accepts_brain_uri_as_record_id() {
    let (_tmp, ctx) = make_ctx().await;
    let registry = ToolRegistry::new();

    // Create a record first.
    let create_result = registry
        .dispatch(
            "records.create_artifact",
            json!({
                "title": "URI input record",
                "kind": "document",
                "text": "assimilation data"
            }),
            &ctx,
        )
        .await;
    let created = parse_response(&create_result);
    let record_id = created["record_id"]
        .as_str()
        .expect("record_id must be present");
    let brain_name = ctx.brain_name();

    // Construct a brain:// URI for the record.
    let uri = format!("brain://{brain_name}/record/{record_id}");

    // records.get should resolve the URI and return the record.
    let get_result = registry
        .dispatch("records.get", json!({ "record_id": uri }), &ctx)
        .await;

    let parsed = parse_response(&get_result);
    assert_eq!(
        parsed["record"]["title"], "URI input record",
        "records.get via brain:// URI must return the correct record; got: {parsed}"
    );
}
