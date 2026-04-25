use std::path::PathBuf;
use std::sync::Arc;

use brain_lib::embedder::MockEmbedder;
use brain_lib::mcp::McpContext;
use brain_lib::mcp::protocol::ToolCallResult;
use brain_lib::mcp::tools::ToolRegistry;
use brain_lib::metrics::Metrics;
use brain_lib::pipeline::IndexPipeline;
use brain_lib::search_service::SearchService;
use brain_lib::stores::BrainStores;
use brain_persistence::store::{Store, StoreReader};
use serde_json::{Value, json};
use tempfile::TempDir;

pub struct TestHarnessContext {
    _tmp: TempDir,
    pub ctx: McpContext,
    pub registry: ToolRegistry,
    notes_dir: PathBuf,
    lance_path: PathBuf,
}

pub async fn make_test_context() -> TestHarnessContext {
    let (tmp, stores) = BrainStores::in_memory().expect("create in-memory stores");

    let lance_path = tmp.path().join("test_lance");
    let writable_store = Store::open_or_create(&lance_path)
        .await
        .expect("open writable LanceDB");

    let store_reader = StoreReader::from_store(&writable_store);
    let search = SearchService {
        store: store_reader,
        embedder: Arc::new(MockEmbedder),
    };

    let notes_dir = tmp.path().join("notes");
    std::fs::create_dir_all(&notes_dir).expect("create notes dir");

    let ctx = McpContext {
        stores,
        search: Some(search),
        writable_store: Some(writable_store),
        metrics: Arc::new(Metrics::new()),
    };

    TestHarnessContext {
        _tmp: tmp,
        ctx,
        registry: ToolRegistry::new(),
        notes_dir,
        lance_path,
    }
}

pub async fn call_tool(ctx: &TestHarnessContext, tool_name: &str, params: Value) -> ToolCallResult {
    ctx.registry.dispatch(tool_name, params, &ctx.ctx).await
}

fn parse_tool_json(result: &ToolCallResult) -> Value {
    assert_ne!(
        result.is_error,
        Some(true),
        "tool error: {}",
        result.content[0].text
    );
    serde_json::from_str(&result.content[0].text).expect("tool result should be JSON")
}

pub async fn seed_test_task(ctx: &TestHarnessContext) -> String {
    let result = call_tool(ctx, "tasks.create", json!({ "title": "seed test task" })).await;
    let parsed = parse_tool_json(&result);
    parsed["task_id"]
        .as_str()
        .expect("tasks.create should return task_id")
        .to_string()
}

pub async fn seed_test_record(ctx: &TestHarnessContext) -> String {
    let result = call_tool(
        ctx,
        "records.create_artifact",
        json!({
            "title": "seed test record",
            "kind": "document",
            "text": "seed payload"
        }),
    )
    .await;

    let parsed = parse_tool_json(&result);
    parsed["record_id"]
        .as_str()
        .expect("records.create_artifact should return record_id")
        .to_string()
}

pub async fn seed_test_chunks(ctx: &TestHarnessContext) -> usize {
    let note_path = ctx.notes_dir.join("seed_note.md");
    std::fs::write(
        &note_path,
        "# Seed note\n\nThis note seeds chunks for MCP handler tests.",
    )
    .expect("write seed markdown");

    let db = ctx.ctx.stores.db_for_tests().clone();
    let store = Store::open_or_create(&ctx.lance_path)
        .await
        .expect("open LanceDB for indexing");
    let pipeline = IndexPipeline::with_embedder(db, store, Arc::new(MockEmbedder))
        .await
        .expect("build index pipeline");

    let stats = pipeline
        .full_scan(std::slice::from_ref(&ctx.notes_dir))
        .await
        .expect("index seed note");
    assert_eq!(stats.errors, 0, "seed indexing should have no errors");
    stats.indexed
}

#[tokio::test]
async fn mcp_test_harness_status_smoke() {
    let ctx = make_test_context().await;
    let result = call_tool(&ctx, "status", json!({})).await;

    assert_ne!(result.is_error, Some(true), "status should not error");

    let parsed = parse_tool_json(&result);
    assert!(parsed.is_object(), "status should return JSON object");
    assert!(parsed["uptime_seconds"].is_u64());
    assert!(parsed["indexing_latency"]["p50_us"].is_u64());
    assert!(parsed["query_latency"]["p95_us"].is_u64());
    assert!(parsed["tokens"].is_object());
    assert!(parsed["queue_depth"].is_u64());
}
