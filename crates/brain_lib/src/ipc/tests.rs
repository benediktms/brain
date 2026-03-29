//! Integration tests for the IPC layer.
//!
//! Each test is self-contained: it creates a temp directory for the socket,
//! spins up an IpcServer, connects via IpcClient, and verifies round-trip
//! behaviour. All tests use tokio multi-thread runtime to support concurrent
//! connection tests.

use std::sync::Arc;

use serde_json::json;
use tokio::time::{Duration, sleep};

use crate::ipc::client::IpcClient;
use crate::ipc::router::BrainRouter;
use crate::ipc::server::IpcServer;
use crate::mcp::tools::tests::create_test_context;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Spin up a test server with one brain registered as "test-brain".
/// Returns the cancellation token and the TempDir (keep alive for socket lifetime).
async fn start_server(
    sock: &std::path::Path,
) -> (tempfile::TempDir, tokio_util::sync::CancellationToken) {
    let (dir, ctx) = create_test_context().await;
    ctx.stores
        .db()
        .ensure_brain_registered("test-brain", "test-brain")
        .unwrap();
    let router = BrainRouter::new(Arc::new(ctx), "test-brain".to_string());
    let server = IpcServer::bind(sock, router).expect("bind failed");
    let token = server.cancellation_token();
    let token2 = token.clone();
    tokio::spawn(async move { server.run().await });
    // Brief yield so accept loop is ready.
    sleep(Duration::from_millis(10)).await;
    (dir, token2)
}

/// Extract the content text from a ToolCallResult-shaped JSON value.
///
/// tools_call returns `Value` = serialised `ToolCallResult`:
/// `{ "content": [{ "type": "text", "text": "<JSON string>" }], "isError": ... }`
///
/// Returns the inner text string.
fn content_text(result: &serde_json::Value) -> &str {
    result
        .get("content")
        .and_then(|c| c.as_array())
        .and_then(|a| a.first())
        .and_then(|item| item.get("text"))
        .and_then(|t| t.as_str())
        .expect("expected content[0].text in ToolCallResult")
}

/// Parse the content text as JSON.
fn content_json(result: &serde_json::Value) -> serde_json::Value {
    let text = content_text(result);
    serde_json::from_str(text).unwrap_or_else(|_| serde_json::Value::String(text.to_string()))
}

// ---------------------------------------------------------------------------
// Test 1: Server bind + client connect — ping succeeds
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ipc_server_bind_and_client_ping() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sock = tmp.path().join("ping.sock");

    let (_dir, token) = start_server(&sock).await;

    let mut client = IpcClient::connect(&sock).await.expect("connect failed");
    let result = client.ping("test-brain").await.expect("ping failed");

    assert!(result.is_object(), "ping should return an object");
    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 2: Task CRUD through IPC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ipc_task_crud() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sock = tmp.path().join("task_crud.sock");

    let (_dir, token) = start_server(&sock).await;
    let mut client = IpcClient::connect(&sock).await.expect("connect failed");

    // Create a task.
    // tools_call returns ToolCallResult; content[0].text contains JSON.
    let create_raw = client
        .task_create(
            "test-brain",
            json!({ "title": "IPC test task", "priority": 3 }),
        )
        .await
        .expect("task_create failed");

    assert!(
        create_raw.get("isError").is_none(),
        "task_create should not return error: {create_raw}"
    );

    let create_result = content_json(&create_raw);

    // Extract the task_id.
    let task_id = create_result
        .get("task_id")
        .and_then(|v| v.as_str())
        .expect("task_id missing from create result");

    // List tasks — should contain at least one.
    let list_raw = client
        .task_list("test-brain", json!({}))
        .await
        .expect("task_list failed");

    assert!(
        list_raw.get("isError").is_none(),
        "task_list should not return error"
    );

    let list_result = content_json(&list_raw);
    let tasks = list_result
        .get("tasks")
        .and_then(|t| t.as_array())
        .expect("expected 'tasks' array in task_list result");
    assert!(!tasks.is_empty(), "expected non-empty task list");

    // Fetch the task.
    let fetch_raw = client
        .task_fetch("test-brain", json!({ "task_id": task_id }))
        .await
        .expect("task_fetch failed");

    assert!(
        fetch_raw.get("isError").is_none(),
        "task_fetch should not return error"
    );

    let fetch_result = content_json(&fetch_raw);
    let fetched_title = fetch_result
        .get("title")
        .and_then(|v| v.as_str())
        .expect("title missing from fetched task");
    assert_eq!(fetched_title, "IPC test task");

    // Close the task. task_close expects `task_ids` (plural).
    let close_raw = client
        .task_close("test-brain", json!({ "task_ids": [task_id] }))
        .await
        .expect("task_close failed");

    assert!(
        close_raw.get("isError").is_none(),
        "task_close should not return error"
    );

    let close_result = content_json(&close_raw);
    let status = close_result
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("done");
    assert!(
        status == "done" || close_result.is_object(),
        "expected done status, got: {status}"
    );

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 2b: Close task by prefix through IPC (regression test for bug where
// tasks_close failed with "no task found matching prefix" for a task that
// tasks_apply_event with status_changed could resolve)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ipc_task_close_by_prefix() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sock = tmp.path().join("task_close_prefix.sock");

    let (_dir, token) = start_server(&sock).await;
    let mut client = IpcClient::connect(&sock).await.expect("connect failed");

    // Create a task with a known title.
    let create_raw = client
        .task_create(
            "test-brain",
            json!({ "title": "Prefix close regression test", "priority": 2 }),
        )
        .await
        .expect("task_create failed");

    assert!(
        create_raw.get("isError").is_none(),
        "task_create should not error: {create_raw}"
    );

    let create_result = content_json(&create_raw);
    let compact_id = create_result
        .get("task_id")
        .and_then(|v| v.as_str())
        .expect("task_id missing from create result");

    // The compact_id is "{prefix}-{3-char-hash}" (e.g. "nsx-a3f").
    // Use only the 3-char hash as a prefix — should still resolve uniquely.
    // Format: the prefix part before the hyphen contains the brain prefix (e.g. "nsx").
    // We test closing by the full compact_id (unique) and then by the hash-only prefix.
    let parts: Vec<&str> = compact_id.split('-').collect();
    assert_eq!(
        parts.len(),
        2,
        "compact_id '{compact_id}' should have format 'brain-hash'"
    );
    let _hash = parts[1];

    // --- Test 1: close by full compact_id (should always work) ---
    let close_full_raw = client
        .task_close("test-brain", json!({ "task_ids": compact_id }))
        .await
        .expect("task_close by full compact_id failed");

    assert!(
        close_full_raw.get("isError").is_none(),
        "task_close by full compact_id should not error: {}",
        content_text(&close_full_raw)
    );

    let close_full_result = content_json(&close_full_raw);
    let closed_count = close_full_result["summary"]["closed"]
        .as_i64()
        .expect("summary.closed should be a number");
    assert_eq!(
        closed_count, 1,
        "should have closed exactly 1 task, got {closed_count}"
    );
    let failed_count = close_full_result["summary"]["failed"]
        .as_i64()
        .expect("summary.failed should be a number");
    assert_eq!(
        failed_count, 0,
        "should have 0 failures, got {failed_count}"
    );

    // --- Test 2: create another task and close by hash-only prefix ---
    let create2_raw = client
        .task_create(
            "test-brain",
            json!({ "title": "Second task for prefix test", "priority": 3 }),
        )
        .await
        .expect("task_create 2 failed");

    let create2_result = content_json(&create2_raw);
    let compact_id2 = create2_result
        .get("task_id")
        .and_then(|v| v.as_str())
        .expect("task_id missing from create2 result");

    // Close using only the hash prefix (3 chars). Each hash is project-wide unique
    // since it's derived from the task_id via BLAKE3.
    let hash_prefix = compact_id2.split('-').nth(1).expect("brain-hash format");
    let close_prefix_raw = client
        .task_close("test-brain", json!({ "task_ids": hash_prefix }))
        .await
        .expect("task_close by hash prefix failed");

    assert!(
        close_prefix_raw.get("isError").is_none(),
        "task_close by hash prefix '{hash_prefix}' should not error: {}",
        content_text(&close_prefix_raw)
    );

    let close_prefix_result = content_json(&close_prefix_raw);
    let prefix_closed = close_prefix_result["summary"]["closed"]
        .as_i64()
        .expect("summary.closed should be a number");
    assert_eq!(
        prefix_closed, 1,
        "should have closed exactly 1 task by hash prefix, got {prefix_closed}"
    );
    let prefix_failed = close_prefix_result["summary"]["failed"]
        .as_i64()
        .expect("summary.failed should be a number");
    assert_eq!(
        prefix_failed, 0,
        "hash prefix close should have 0 failures, got {prefix_failed}"
    );

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 3: Record operations through IPC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ipc_record_operations() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sock = tmp.path().join("records.sock");

    let (_dir, token) = start_server(&sock).await;
    let mut client = IpcClient::connect(&sock).await.expect("connect failed");

    // List records — should succeed (may be empty).
    let list_raw = client
        .record_list("test-brain", json!({}))
        .await
        .expect("record_list failed");

    assert!(
        list_raw.get("isError").is_none(),
        "record_list should not return error"
    );

    // Save a snapshot so we have a record to query.
    // content[0].text contains the JSON payload.
    let snapshot_raw = client
        .tools_call(
            "records_save_snapshot",
            "test-brain",
            json!({
                "title": "IPC test snapshot",
                "text": "hello from IPC test",
            }),
        )
        .await
        .expect("records_save_snapshot failed");

    assert!(
        snapshot_raw.get("isError").is_none(),
        "records_save_snapshot should not return error: {snapshot_raw}"
    );

    let snapshot_result = content_json(&snapshot_raw);
    let record_id = snapshot_result
        .get("record_id")
        .and_then(|v| v.as_str())
        .expect("record_id missing from snapshot result");

    // Get record metadata.
    let get_raw = client
        .record_get("test-brain", json!({ "record_id": record_id }))
        .await
        .expect("record_get failed");

    assert!(
        get_raw.get("isError").is_none(),
        "record_get should not return error"
    );

    let get_result = content_json(&get_raw);
    let title = get_result
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(title, "IPC test snapshot");

    // Fetch record content.
    let content_raw = client
        .record_fetch_content("test-brain", json!({ "record_id": record_id }))
        .await
        .expect("record_fetch_content failed");

    assert!(
        content_raw.get("isError").is_none(),
        "record_fetch_content should not return error"
    );

    let content_result = content_json(&content_raw);
    let text = content_result
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(text, "hello from IPC test");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 4: Unknown brain error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ipc_unknown_brain_returns_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sock = tmp.path().join("unknown_brain.sock");

    let (_dir, token) = start_server(&sock).await;
    let mut client = IpcClient::connect(&sock).await.expect("connect failed");

    // tools_call returns Value (not Err) even on tool-level errors; check content.
    let result = client
        .tools_call("status", "no-such-brain", json!({}))
        .await
        .expect("tools_call itself should not fail at transport level");

    // The router returns a ToolCallResult with isError=true; content[0].text
    // contains "Brain not found: no-such-brain".
    let text = content_text(&result);
    assert!(
        text.contains("no-such-brain") || text.contains("Brain not found"),
        "expected 'Brain not found' error, got: {text}"
    );

    let is_error = result
        .get("isError")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(
        is_error,
        "expected isError=true for unknown brain, got: {result}"
    );

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 5: Stale socket cleanup — IpcServer::bind removes stale file
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ipc_stale_socket_cleanup() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sock = tmp.path().join("stale.sock");

    // Write a fake (stale) socket file — not a real listening socket.
    std::fs::write(&sock, b"stale").unwrap();
    assert!(sock.exists(), "stale file should exist before bind");

    // Bind should detect the stale file, remove it, and succeed.
    let (dir, ctx) = create_test_context().await;
    ctx.stores
        .db()
        .ensure_brain_registered("test-brain", "test-brain")
        .unwrap();
    let router = BrainRouter::new(Arc::new(ctx), "test-brain".to_string());

    let server = IpcServer::bind(&sock, router).expect("bind should succeed after stale removal");
    let token = server.cancellation_token();
    tokio::spawn(async move {
        let _dir = dir; // keep alive
        server.run().await;
    });
    sleep(Duration::from_millis(10)).await;

    // Socket file should now be a real UDS — client can connect.
    let mut client = IpcClient::connect(&sock)
        .await
        .expect("connect after stale cleanup failed");
    let result = client
        .ping("test-brain")
        .await
        .expect("ping after stale cleanup failed");
    assert!(result.is_object());

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 6: Concurrent connections — multiple clients in parallel
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ipc_concurrent_connections() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sock = tmp.path().join("concurrent.sock");

    let (_dir, token) = start_server(&sock).await;

    let n = 5;
    let mut handles = Vec::new();

    for i in 0..n {
        let sock_path = sock.clone();
        handles.push(tokio::spawn(async move {
            let mut client = IpcClient::connect(&sock_path)
                .await
                .expect("concurrent connect failed");
            let result = client
                .ping("test-brain")
                .await
                .unwrap_or_else(|e| panic!("concurrent ping {i} failed: {e}"));
            assert!(
                result.is_object(),
                "concurrent ping {i} should return object"
            );
        }));
    }

    for handle in handles {
        handle.await.expect("concurrent task panicked");
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 7: Graceful shutdown — server stops accepting after cancel
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ipc_graceful_shutdown() {
    let tmp = tempfile::TempDir::new().unwrap();
    let sock = tmp.path().join("shutdown.sock");

    let (dir, ctx) = create_test_context().await;
    ctx.stores
        .db()
        .ensure_brain_registered("test-brain", "test-brain")
        .unwrap();
    let router = BrainRouter::new(Arc::new(ctx), "test-brain".to_string());

    let server = IpcServer::bind(&sock, router).expect("bind failed");
    let token = server.cancellation_token();
    let token2 = token.clone();
    let sock_path = sock.clone();

    let server_handle = tokio::spawn(async move {
        let _dir = dir;
        server.run().await;
    });

    sleep(Duration::from_millis(10)).await;

    // Verify server is up.
    assert!(
        IpcClient::is_daemon_available(&sock_path).await,
        "server should be available before shutdown"
    );

    // Cancel the server.
    token2.cancel();

    // Wait for server task to complete.
    server_handle.await.expect("server task panicked");

    // After shutdown the socket file may or may not be removed by the server
    // itself — but new connections should fail (ECONNREFUSED or ENOENT).
    let connect_result = IpcClient::connect(&sock_path).await;
    assert!(
        connect_result.is_err(),
        "connect after shutdown should fail, got: {:?}",
        connect_result.ok()
    );
}
