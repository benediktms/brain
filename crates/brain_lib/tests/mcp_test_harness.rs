use std::path::PathBuf;
use std::sync::Arc;

use brain_lib::embedder::{Embed, MockEmbedder};
use brain_lib::error::Result as BrainResult;
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
    // Register a brain up front so the strict ambient-resolution check in
    // `mcp::tools::scope::resolve_scope` succeeds. Mirror the unit-test
    // scaffolding in `create_test_context` (`crates/brain_lib/src/mcp/tools/mod.rs`):
    // brain_id `test-brain-id` with prefix `TST` so compact IDs render `tst-...`.
    let (tmp, stores) = BrainStores::in_memory_with_brain("test-brain-id", "test-brain", "TST")
        .expect("create in-memory stores");

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
    let mut pipeline = IndexPipeline::with_embedder(db, store, Arc::new(MockEmbedder))
        .await
        .expect("build index pipeline");
    // Match the brain_id `make_test_context` registers so seeded chunks live
    // in the same brain the MCP handlers query under.
    pipeline.set_brain_id(ctx.ctx.brain_id().to_string());

    let stats = pipeline
        .full_scan(std::slice::from_ref(&ctx.notes_dir))
        .await
        .expect("index seed note");
    assert_eq!(stats.errors, 0, "seed indexing should have no errors");
    stats.indexed
}

/// Embedder that maps each known tag string to one of four near-orthogonal
/// 384-dim one-hot vectors so the clustering algorithm produces exactly four
/// connected components (`bug/bugs/defect`, `performance/perf`,
/// `docs/documentation`, `chore`) regardless of cosine threshold within
/// (0, 1).
pub struct ControlledEmbedder;

impl ControlledEmbedder {
    fn cluster_index(text: &str) -> usize {
        match text {
            "bug" | "bugs" | "defect" => 0,
            "performance" | "perf" => 1,
            "docs" | "documentation" => 2,
            "chore" => 3,
            other => panic!("ControlledEmbedder: unexpected tag {other:?}"),
        }
    }

    fn vector_for(text: &str) -> Vec<f32> {
        let mut v = vec![0f32; 384];
        v[Self::cluster_index(text)] = 1.0;
        v
    }
}

impl Embed for ControlledEmbedder {
    fn embed_batch(&self, texts: &[&str]) -> BrainResult<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|t| Self::vector_for(t)).collect())
    }

    fn hidden_size(&self) -> usize {
        384
    }

    fn version(&self) -> &str {
        "controlled-v0"
    }
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
