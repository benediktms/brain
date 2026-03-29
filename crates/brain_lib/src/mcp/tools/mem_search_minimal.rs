use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::query_pipeline::{FederatedPipeline, QueryPipeline, SearchParams};
use crate::store::VectorSearchMode;

use crate::uri::SynapseUri;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    query: String,
    #[serde(default = "default_intent")]
    intent: String,
    #[serde(default = "default_budget")]
    budget_tokens: u64,
    #[serde(default = "default_k")]
    k: u64,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    brains: Vec<String>,
    /// Vector search strategy: "exact", "ann_refined" (default), or "ann_fast".
    #[serde(default)]
    vector_search_mode: Option<String>,
    /// When true, include per-result signal score breakdowns in the response.
    #[serde(default)]
    explain: bool,
}

fn default_intent() -> String {
    "auto".into()
}
fn default_budget() -> u64 {
    800
}
fn default_k() -> u64 {
    10
}

pub(super) struct MemSearchMinimal;

impl McpTool for MemSearchMinimal {
    fn name(&self) -> &'static str {
        "memory.search_minimal"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Search the knowledge base and return compact memory stubs within a token budget. Results include note chunks, task capsules, episodes, and reflections. Each result carries a `kind` field: \"note\", \"task\", \"task-outcome\", \"episode\", or \"reflection\". Use this first to find relevant memories, then expand specific ones.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query"
                    },
                    "intent": {
                        "type": "string",
                        "enum": ["lookup", "planning", "reflection", "synthesis", "auto"],
                        "description": "Retrieval intent — controls ranking weight profile. Default: auto",
                        "default": "auto"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 800",
                        "default": 800
                    },
                    "k": {
                        "type": "integer",
                        "description": "Maximum number of results. Default: 10",
                        "default": 10
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional tags to boost results matching these tags via Jaccard similarity. Pass as a JSON array, e.g. [\"rust\", \"memory\"]"
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional list of brain names or IDs to search across multiple brains. Use [\"all\"] to search all registered brains. When omitted, searches only the current brain. Call brains.list first to discover available brain names."
                    },
                    "vector_search_mode": {
                        "type": "string",
                        "enum": ["exact", "ann_refined", "ann_fast"],
                        "description": "Vector search strategy controlling the ANN (Approximate Nearest Neighbor) tradeoff. exact: brute-force scan against all vectors — fully deterministic, slowest. ann_refined (default): ANN index finds candidates, then rescores against full uncompressed vectors for accurate ordering. ann_fast: pure ANN with compressed vectors only — fastest, but distances are approximate."
                    },
                    "explain": {
                        "type": "boolean",
                        "description": "When true, return per-signal scores (vector, bm25, recency, links, tag_match, importance) for each result. Default: false",
                        "default": false
                    }
                },
                "required": ["query"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let Some(store) = ctx.store() else {
                return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
            };
            let Some(embedder) = ctx.embedder() else {
                return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
            };

            let params: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let mode = match params.vector_search_mode.as_deref() {
                Some(s) => match s.parse::<VectorSearchMode>() {
                    Ok(m) => m,
                    Err(e) => return ToolCallResult::error(e),
                },
                None => VectorSearchMode::default(),
            };

            let search_params = SearchParams::new(
                &params.query,
                &params.intent,
                params.budget_tokens as usize,
                params.k as usize,
                &params.tags,
            )
            .with_mode(mode);

            let search_result = if params.brains.is_empty() {
                // Single-brain path.
                let pipeline = QueryPipeline::new(ctx.stores.db(), store, embedder, &ctx.metrics);
                if params.explain {
                    match pipeline.search_with_scores(&search_params).await {
                        Ok(r) => r,
                        Err(e) => return ToolCallResult::error(format!("Search failed: {e}")),
                    }
                } else {
                    match pipeline.search(&search_params).await {
                        Ok(r) => r,
                        Err(e) => return ToolCallResult::error(format!("Search failed: {e}")),
                    }
                }
            } else {
                // Federated path — delegate setup to shared helper.
                // Build the brain list: local brain first, then each remote.
                // All share the same unified `ctx.db` — no separate Db per brain.
                let brains = match super::build_federated_brains(
                    ctx,
                    store.clone(),
                    embedder,
                    &params.brains,
                )
                .await
                {
                    Ok(b) => b,
                    Err(e) => return ToolCallResult::error(e),
                };

                let federated = FederatedPipeline {
                    db: ctx.stores.db(),
                    brains,
                    embedder,
                    metrics: &ctx.metrics,
                };

                // TODO(W1-IMPL-EXPLAIN): FederatedPipeline has no search_with_scores.
                // explain=true is silently ignored for federated searches.
                match federated.search(&search_params).await {
                    Ok(r) => r,
                    Err(e) => {
                        return ToolCallResult::error(format!("Federated search failed: {e}"));
                    }
                }
            };

            ctx.metrics
                .record_search_minimal_tokens(search_result.used_tokens_est);

            let results_json: Vec<Value> = search_result
                .results
                .iter()
                .map(|stub| {
                    let mut stub_json = json!({
                        "memory_id": stub.memory_id,
                        "title": stub.title,
                        "summary": stub.summary_2sent,
                        "score": stub.hybrid_score,
                        "file_path": stub.file_path,
                        "heading_path": stub.heading_path,
                        "kind": stub.kind,
                    });
                    let uri_brain = stub.brain_name.as_deref().unwrap_or(ctx.brain_name());
                    let uri = match stub.kind.as_str() {
                        "episode" => SynapseUri::for_episode(uri_brain, &stub.memory_id),
                        "reflection" => SynapseUri::for_reflection(uri_brain, &stub.memory_id),
                        "procedure" => SynapseUri::for_procedure(uri_brain, &stub.memory_id),
                        "record" => SynapseUri::for_record(uri_brain, &stub.memory_id),
                        "task" | "task-outcome" => SynapseUri::for_task(uri_brain, &stub.memory_id),
                        _ => SynapseUri::for_memory(uri_brain, &stub.memory_id),
                    };
                    stub_json["uri"] = json!(uri.to_string());
                    if let Some(ref bn) = stub.brain_name {
                        stub_json["brain_name"] = json!(bn);
                    }
                    if let Some(ref scores) = stub.signal_scores {
                        stub_json["signals"] = json!({
                            "sim_vector": scores.vector,
                            "bm25": scores.keyword,
                            "recency": scores.recency,
                            "links": scores.links,
                            "tag_match": scores.tag_match,
                            "importance": scores.importance,
                        });
                    }
                    stub_json
                })
                .collect();

            let response = json!({
                "budget_tokens": search_result.budget_tokens,
                "used_tokens_est": search_result.used_tokens_est,
                "intent_resolved": format!("{:?}", crate::ranking::resolve_intent(&params.intent)),
                "result_count": search_result.num_results,
                "total_available": search_result.total_available,
                "results": results_json
            });

            json_response(&response)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};

    use crate::mcp::McpContext;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    /// Build a context with store and embedder both absent (tasks-only mode).
    async fn create_tasks_only_context() -> (tempfile::TempDir, McpContext) {
        let (tmp, stores) =
            crate::stores::BrainStores::in_memory().expect("checked in test assertions");

        (
            tmp,
            McpContext {
                stores,
                search: None,
                writable_store: None,
                metrics: Arc::new(crate::metrics::Metrics::new()),
            },
        )
    }

    /// Build a context with store present but embedder absent.
    ///
    /// After the McpContext refactor, store and embedder are bundled together
    /// in `SearchService`. To simulate "no embedder", we set `search: None`.
    /// The test still validates that the tool returns MEMORY_UNAVAILABLE.
    async fn create_no_embedder_context() -> (tempfile::TempDir, McpContext) {
        let (tmp, stores) =
            crate::stores::BrainStores::in_memory().expect("checked in test assertions");

        (
            tmp,
            McpContext {
                stores,
                search: None,
                writable_store: None,
                metrics: Arc::new(crate::metrics::Metrics::new()),
            },
        )
    }

    #[tokio::test]
    async fn test_missing_query() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.search_minimal", json!({}), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_memory_unavailable_no_store() {
        let (_dir, ctx) = create_tasks_only_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "rust memory" }),
                &ctx,
            )
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0]
                .text
                .contains("Memory tools are unavailable")
        );
    }

    #[tokio::test]
    async fn test_memory_unavailable_no_embedder() {
        let (_dir, ctx) = create_no_embedder_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "rust memory" }),
                &ctx,
            )
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.content[0]
                .text
                .contains("Memory tools are unavailable")
        );
    }

    #[tokio::test]
    async fn test_empty_query_succeeds() {
        // An empty string is a valid query string — serde accepts it.
        // With an empty index the search returns 0 results.
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.search_minimal", json!({ "query": "" }), &ctx)
            .await;
        assert!(
            result.is_error.is_none(),
            "empty query should not be a validation error; got: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_valid_minimal_uses_defaults() {
        // Only query supplied — budget_tokens, k, intent, tags take their defaults.
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.search_minimal", json!({ "query": "hello" }), &ctx)
            .await;
        assert!(result.is_error.is_none(), "should succeed with defaults");

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["budget_tokens"], 800);
        assert_eq!(parsed["result_count"], 0);
        assert!(
            parsed["results"]
                .as_array()
                .expect("checked in test assertions")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_budget_tokens_parameter() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "budget_tokens": 200 }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["budget_tokens"], 200);
    }

    #[tokio::test]
    async fn test_k_parameter() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        // k=1 is the tightest valid limit; with empty index still 0 results.
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "k": 1 }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["result_count"], 0);
    }

    #[tokio::test]
    async fn test_tags_parameter() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "tags": ["rust", "memory"] }),
                &ctx,
            )
            .await;
        assert!(
            result.is_error.is_none(),
            "tags array should be accepted; got: {}",
            result.content[0].text
        );
    }

    #[tokio::test]
    async fn test_empty_tags_array() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "tags": [] }),
                &ctx,
            )
            .await;
        assert!(
            result.is_error.is_none(),
            "empty tags array should be valid"
        );
    }

    #[tokio::test]
    async fn test_intent_lookup() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "intent": "lookup" }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["intent_resolved"], "Lookup");
    }

    #[tokio::test]
    async fn test_intent_planning() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "intent": "planning" }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["intent_resolved"], "Planning");
    }

    #[tokio::test]
    async fn test_intent_reflection() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "intent": "reflection" }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["intent_resolved"], "Reflection");
    }

    #[tokio::test]
    async fn test_intent_synthesis() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "intent": "synthesis" }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["intent_resolved"], "Synthesis");
    }

    #[tokio::test]
    async fn test_intent_auto_maps_to_default() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "intent": "auto" }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        // "auto" does not match any named profile — falls through to Default.
        assert_eq!(parsed["intent_resolved"], "Default");
    }

    #[tokio::test]
    async fn test_response_shape() {
        // Verify every expected top-level field is present in the response.
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "anything" }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert!(
            parsed.get("budget_tokens").is_some(),
            "missing budget_tokens"
        );
        assert!(
            parsed.get("used_tokens_est").is_some(),
            "missing used_tokens_est"
        );
        assert!(
            parsed.get("intent_resolved").is_some(),
            "missing intent_resolved"
        );
        assert!(parsed.get("result_count").is_some(), "missing result_count");
        assert!(
            parsed.get("total_available").is_some(),
            "missing total_available"
        );
        assert!(parsed.get("results").is_some(), "missing results");
        assert!(parsed["results"].is_array(), "results should be an array");
    }

    #[tokio::test]
    async fn test_large_budget_tokens() {
        // Very large budget_tokens should not cause an error.
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({ "query": "hello", "budget_tokens": 1_000_000 }),
                &ctx,
            )
            .await;
        assert!(
            result.is_error.is_none(),
            "large budget should be accepted; got: {}",
            result.content[0].text
        );
        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        assert_eq!(parsed["budget_tokens"], 1_000_000);
    }

    /// TDD: explain=true must be accepted without error and — once implemented —
    /// must include per-result signal score breakdowns in the response.
    ///
    /// Phase 1 (red): verifies param acceptance; signal fields asserted below will
    /// fail until W1-IMPL-EXPLAIN wires explain=true through the pipeline.
    #[tokio::test]
    async fn test_explain_true_returns_signal_scores() {
        use std::sync::Arc;

        use tempfile::TempDir;

        use crate::db::Db;
        use crate::embedder::{Embed, MockEmbedder};
        use crate::mcp::McpContext;
        use crate::pipeline::IndexPipeline;
        use crate::store::Store;

        // Build a fully-indexed context so we get actual results back.
        let tmp = TempDir::new().expect("checked in test assertions");
        let sqlite_path = tmp.path().join("brain.db");
        let lance_path = tmp.path().join("brain_lancedb");
        let notes_dir = tmp.path().join("notes");
        std::fs::create_dir_all(&notes_dir).expect("checked in test assertions");

        // Write a note and index it.
        let note_path = notes_dir.join("signals.md");
        std::fs::write(
            &note_path,
            "## Signal Scores\n\nThis chunk exists to produce ranked results with signal breakdown.",
        ).expect("checked in test assertions");

        let db = Db::open(&sqlite_path).expect("checked in test assertions");
        let store = Store::open_or_create(&lance_path)
            .await
            .expect("checked in test assertions");
        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let pipeline = IndexPipeline::with_embedder(db, store, embedder)
            .await
            .expect("checked in test assertions");
        pipeline
            .full_scan(&[notes_dir])
            .await
            .expect("checked in test assertions");
        drop(pipeline);

        // Create a fresh McpContext over the indexed data.
        let store2 = Store::open_or_create(&lance_path)
            .await
            .expect("checked in test assertions");
        let store2_reader = crate::store::StoreReader::from_store(&store2);
        let ctx_db = Db::open(&sqlite_path).expect("checked in test assertions");
        let stores2 = crate::stores::BrainStores::from_dbs(ctx_db, "", tmp.path(), tmp.path())
            .expect("checked in test assertions");
        let ctx = McpContext {
            stores: stores2,
            search: Some(crate::search_service::SearchService {
                store: store2_reader,
                embedder: Arc::new(MockEmbedder),
            }),
            writable_store: Some(store2),
            metrics: Arc::new(crate::metrics::Metrics::new()),
        };

        let registry = ToolRegistry::new();

        // Step 1: explain=true must not be rejected as an invalid param.
        let result = registry
            .dispatch(
                "memory.search_minimal",
                json!({
                    "query": "Signal Scores chunk ranked results",
                    "explain": true,
                    "intent": "lookup",
                    "k": 5
                }),
                &ctx,
            )
            .await;
        assert!(
            result.is_error.is_none(),
            "explain=true should be accepted without error; got: {}",
            result.content[0].text
        );

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");

        // Step 2: verify we have at least one result.
        let count = parsed["result_count"].as_u64().unwrap_or(0);
        assert!(
            count > 0,
            "expected at least one ranked result for explain test; response: {}",
            result.content[0].text
        );

        // Step 3 (TDD red): each result must expose a "signals" object with
        // sim_vector and bm25 fields. This assertion fails until W1-IMPL-EXPLAIN
        // populates signals from SignalScores when explain=true.
        let results = parsed["results"].as_array().expect("results must be array");
        for (i, r) in results.iter().enumerate() {
            let signals = match r.get("signals") {
                Some(s) => s,
                None => {
                    panic!(
                        "result[{i}] missing 'signals' key (explain=true not yet wired through pipeline)"
                    )
                }
            };
            assert!(
                signals.get("sim_vector").is_some(),
                "result[{i}].signals missing 'sim_vector'"
            );
            assert!(
                signals.get("bm25").is_some(),
                "result[{i}].signals missing 'bm25'"
            );
        }
    }
}
