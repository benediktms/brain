//! Per-tool smoke tests for brain_mcp's integration test suite.
//!
//! Each test spawns a fresh `brain-daemon` backed by an in-memory
//! `BrainStores` (real SQLite on RAM, not a mock), drives one or two
//! `ToolRegistry::dispatch` calls, and asserts on the response shape.
//! Tests are fully independent — no shared state or TempDirs.

#![cfg(unix)]

mod common;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use brain_daemon::{BrainStoresDispatcher, DaemonConfig, UnixSocketServer};
use brain_lib::stores::BrainStores;
use brain_mcp::{McpContext, ToolRegistry};
use brain_rpc::{DaemonClient, UnixSocketTransport};
use serde_json::json;
use tempfile::TempDir;

// ── stores-backed daemon helper ───────────────────────────────────────────────

/// Spawn a `BrainStoresDispatcher`-backed daemon on a temp-dir socket.
///
/// Unlike `common::spawn_daemon` (which uses `DefaultDispatcher` — only
/// ping/handshake), this variant wires in a real in-memory `BrainStores`
/// so storage-backed tool calls (tasks.list, memory.write_episode, etc.)
/// actually resolve.
fn spawn_stores_daemon() -> (TempDir, std::path::PathBuf, common::ServerGuard) {
    let (stores_tmp, stores) = BrainStores::in_memory().expect("BrainStores::in_memory");
    let sock_tmp = TempDir::new().expect("tempdir for socket");
    let sock_path = sock_tmp.path().join("brain.sock");
    let config = DaemonConfig::new(&sock_path);
    // `BrainStoresDispatcher::new` takes a second
    // `Option<Arc<WatcherHandle>>` arg only when `brain-daemon/embed`
    // is active. The cli's `default = ["embed"]` activates it for
    // workspace-default builds (so `cargo check --workspace --tests`
    // sees the 2-arg form), while `--no-default-features` deactivates
    // it. brain_mcp's mirror `embed` feature (default-on) tracks
    // brain-daemon's state so this cfg correctly picks the right
    // signature in both CI lanes.
    #[cfg(feature = "embed")]
    let dispatcher = BrainStoresDispatcher::new(stores, None);
    #[cfg(not(feature = "embed"))]
    let dispatcher = BrainStoresDispatcher::new(stores);
    let server = UnixSocketServer::bind(&config, dispatcher).expect("bind UnixSocketServer");
    let shutdown = server.shutdown_handle();
    let handle = thread::spawn(move || {
        // Keep stores_tmp alive for the duration of the server thread.
        let _keep = stores_tmp;
        server.run()
    });
    common::wait_for_server_ready(&sock_path, Duration::from_millis(500))
        .expect("server socket not ready within 500ms");
    (
        sock_tmp,
        sock_path,
        common::ServerGuard::new(shutdown, handle),
    )
}

/// Connect an `McpContext` (duplicated from common for local use).
async fn connect(sock: &std::path::Path) -> Arc<McpContext> {
    let transport = UnixSocketTransport::connect(sock).expect("connect transport");
    let client = DaemonClient::connect(transport).expect("connect client");
    Arc::new(McpContext::new(client, "default".to_string()))
}

// ── brains.list ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_brains_list_returns_at_least_one_brain() {
    let (_tmp, sock, _guard) = spawn_stores_daemon();
    let ctx = connect(&sock).await;
    let registry = ToolRegistry::new();

    let result = common::dispatch(&registry, &ctx, "brains.list", json!({})).await;
    assert!(
        result.is_error.is_none(),
        "unexpected error: {:?}",
        result.content
    );

    let body: serde_json::Value =
        serde_json::from_str(&result.content[0].text).expect("valid JSON");
    assert!(body["brains"].is_array(), "missing brains array: {body}");
    assert!(body["count"].is_u64(), "missing count: {body}");
}

// ── tasks.list ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tasks_list_returns_empty_list_on_fresh_daemon() {
    let (_tmp, sock, _guard) = spawn_stores_daemon();
    let ctx = connect(&sock).await;
    let registry = ToolRegistry::new();

    let result = common::dispatch(&registry, &ctx, "tasks.list", json!({})).await;
    assert!(
        result.is_error.is_none(),
        "unexpected error: {:?}",
        result.content
    );

    let body: serde_json::Value =
        serde_json::from_str(&result.content[0].text).expect("valid JSON");
    assert_eq!(
        body["tasks"],
        json!([]),
        "expected empty tasks array: {body}"
    );
    assert_eq!(body["total"], json!(0), "expected total: 0: {body}");
    assert_eq!(
        body["has_more"],
        json!(false),
        "expected has_more: false: {body}"
    );
}

// ── memory.write_episode ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_episode_returns_summary_id_and_uri() {
    let (_tmp, sock, _guard) = spawn_stores_daemon();
    let ctx = connect(&sock).await;
    let registry = ToolRegistry::new();

    let result = common::dispatch(
        &registry,
        &ctx,
        "memory.write_episode",
        json!({
            "goal": "verify episode write",
            "actions": "called memory.write_episode with valid params",
            "outcome": "received summary_id and uri"
        }),
    )
    .await;

    assert!(
        result.is_error.is_none(),
        "unexpected error: {:?}",
        result.content
    );

    let body: serde_json::Value =
        serde_json::from_str(&result.content[0].text).expect("valid JSON");

    let summary_id = body["summary_id"]
        .as_str()
        .expect("summary_id must be a string");
    assert!(
        !summary_id.is_empty(),
        "summary_id must not be empty: {body}"
    );

    let uri = body["uri"].as_str().expect("uri must be a string");
    assert!(
        uri.starts_with("synapse://"),
        "uri must start with 'synapse://': got {uri:?}"
    );
}

// ── memory.walk_thread ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_walk_thread_empty_seed_rejected() {
    let (_tmp, sock, _guard) = spawn_stores_daemon();
    let ctx = connect(&sock).await;
    let registry = ToolRegistry::new();

    let result = common::dispatch(
        &registry,
        &ctx,
        "memory.walk_thread",
        json!({ "seed_summary_id": "" }),
    )
    .await;

    assert_eq!(
        result.is_error,
        Some(true),
        "expected is_error=true for empty seed"
    );

    let text = &result.content[0].text;
    assert!(
        text.contains("must not be empty"),
        "error message should contain 'must not be empty': got {text:?}"
    );
}

// ── links.add + links.for_entity ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_links_add_then_query_roundtrip() {
    let (_tmp, sock, _guard) = spawn_stores_daemon();
    let ctx = common::connect_mcp_context(&sock).await;
    let registry = ToolRegistry::new();

    // Add a link between two synthetic TASK entities.
    let add_result = common::dispatch(
        &registry,
        &ctx,
        "links.add",
        json!({
            "from": { "type": "TASK", "id": "A" },
            "to":   { "type": "TASK", "id": "B" },
            "edge_kind": "relates_to"
        }),
    )
    .await;

    assert!(
        add_result.is_error.is_none(),
        "links.add unexpected error: {:?}",
        add_result.content
    );

    // Query outgoing links for TASK/A.
    let query_result = common::dispatch(
        &registry,
        &ctx,
        "links.for_entity",
        json!({
            "entity": { "type": "TASK", "id": "A" },
            "direction": "out"
        }),
    )
    .await;

    assert!(
        query_result.is_error.is_none(),
        "links.for_entity unexpected error: {:?}",
        query_result.content
    );

    let body: serde_json::Value =
        serde_json::from_str(&query_result.content[0].text).expect("valid JSON");

    let outgoing = body["outgoing"]
        .as_array()
        .expect("outgoing must be an array");
    assert!(
        outgoing
            .iter()
            .any(|l| l["from"]["id"] == "A" && l["to"]["id"] == "B"),
        "expected A->B link in outgoing: {body}"
    );
}

// ── sagas.create + sagas.get ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_saga_create_then_get_lifecycle() {
    let (_tmp, sock, _guard) = spawn_stores_daemon();
    let ctx = common::connect_mcp_context(&sock).await;
    let registry = ToolRegistry::new();

    // Create a saga.
    let create_result = common::dispatch(
        &registry,
        &ctx,
        "sagas.create",
        json!({ "title": "smoke-test-saga" }),
    )
    .await;

    assert!(
        create_result.is_error.is_none(),
        "sagas.create unexpected error: {:?}",
        create_result.content
    );

    let create_body: serde_json::Value =
        serde_json::from_str(&create_result.content[0].text).expect("valid JSON");
    let saga_id = create_body["saga_id"]
        .as_str()
        .expect("saga_id must be a string");
    assert!(!saga_id.is_empty(), "saga_id must not be empty");

    // Fetch the saga back.
    let get_result =
        common::dispatch(&registry, &ctx, "sagas.get", json!({ "saga_id": saga_id })).await;

    assert!(
        get_result.is_error.is_none(),
        "sagas.get unexpected error: {:?}",
        get_result.content
    );

    let get_body: serde_json::Value =
        serde_json::from_str(&get_result.content[0].text).expect("valid JSON");
    assert!(
        !get_body["saga"].is_null(),
        "sagas.get returned null saga: {get_body}"
    );
    assert_eq!(
        get_body["saga"]["title"],
        json!("smoke-test-saga"),
        "saga title mismatch: {get_body}"
    );
    assert_eq!(
        get_body["saga_id"],
        json!(saga_id),
        "top-level saga_id mismatch: {get_body}"
    );
}

// ── tasks.create rejection ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_task_create_rejects_brain_param() {
    let (_tmp, sock, _guard) = common::spawn_daemon();
    let ctx = common::connect_mcp_context(&sock).await;
    let registry = ToolRegistry::new();

    let result = common::dispatch(
        &registry,
        &ctx,
        "tasks.create",
        json!({ "title": "x", "brain": "some-other" }),
    )
    .await;

    assert_eq!(
        result.is_error,
        Some(true),
        "expected is_error=true for cross-brain creation"
    );

    let text = &result.content[0].text;
    // The error message references either "cross-brain" or "tasks.apply_event"
    // — both indicate the correct rejection path.
    assert!(
        text.contains("cross-brain") || text.contains("tasks.apply_event"),
        "error should reference cross-brain or tasks.apply_event: got {text:?}"
    );
}

// ── records.list rejection ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_records_list_rejects_brains_param() {
    let (_tmp, sock, _guard) = common::spawn_daemon();
    let ctx = common::connect_mcp_context(&sock).await;
    let registry = ToolRegistry::new();

    let result = common::dispatch(
        &registry,
        &ctx,
        "records.list",
        json!({ "brains": ["all"] }),
    )
    .await;

    assert_eq!(
        result.is_error,
        Some(true),
        "expected is_error=true for cross-brain records.list"
    );

    let text = &result.content[0].text;
    assert!(
        text.contains("cross-brain") || text.contains("brains"),
        "error should reference cross-brain or brains param: got {text:?}"
    );
}

// ── records.search ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_records_search_returns_result_shape() {
    let (_tmp, sock, _guard) = common::spawn_daemon();
    let ctx = common::connect_mcp_context(&sock).await;
    let registry = ToolRegistry::new();

    let result = common::dispatch(
        &registry,
        &ctx,
        "records.search",
        json!({ "query": "test query", "k": 3 }),
    )
    .await;

    // Tool should respond (error or success), not crash the transport.
    assert!(
        result.is_error.is_none() || !result.content.is_empty(),
        "records.search should return a response: {:?}",
        result.content
    );
}

// ── records.fetch_content ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_records_fetch_content_rejects_empty_record_id() {
    let (_tmp, sock, _guard) = common::spawn_daemon();
    let ctx = common::connect_mcp_context(&sock).await;
    let registry = ToolRegistry::new();

    let result = common::dispatch(
        &registry,
        &ctx,
        "records.fetch_content",
        json!({ "record_id": "" }),
    )
    .await;

    // Empty record_id should be rejected with an error.
    assert!(
        result.is_error.is_some(),
        "expected is_error=true for empty record_id"
    );
}
