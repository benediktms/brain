use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::query_pipeline::{FederatedPipeline, QueryPipeline};

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
            let Some(store) = ctx.store.as_ref() else {
                return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
            };
            let Some(embedder) = ctx.embedder.as_ref() else {
                return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
            };

            let params: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let search_result = if params.brains.is_empty() {
                // Single-brain path — unchanged behaviour.
                let pipeline = QueryPipeline::new(&ctx.db, store, embedder, &ctx.metrics);
                match pipeline
                    .search(
                        &params.query,
                        &params.intent,
                        params.budget_tokens as usize,
                        params.k as usize,
                        &params.tags,
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(e) => return ToolCallResult::error(format!("Search failed: {e}")),
                }
            } else {
                // Federated path — open remote brain contexts.
                let brain_keys: Vec<String> = if params.brains.iter().any(|b| b == "all") {
                    match crate::config::list_brain_keys(&ctx.brain_home) {
                        Ok(pairs) => pairs.into_iter().map(|(name, _id)| name).collect(),
                        Err(e) => {
                            return ToolCallResult::error(format!("Failed to list brains: {e}"));
                        }
                    }
                } else {
                    params.brains.clone()
                };

                // Build the brain list: local brain first, then each remote.
                // All share the same unified `ctx.db` — no separate Db per brain.
                let mut brains: Vec<(String, Option<crate::store::StoreReader>)> = Vec::new();
                brains.push((ctx.brain_name.clone(), Some(store.clone())));

                for key in &brain_keys {
                    if key == &ctx.brain_name {
                        // Local brain already added above.
                        continue;
                    }
                    match crate::config::open_remote_search_context(
                        &ctx.brain_home,
                        key,
                        std::path::Path::new(""),
                        embedder,
                    )
                    .await
                    {
                        Ok(Some(remote)) => {
                            brains.push((remote.brain_name, remote.store));
                        }
                        Ok(None) => {
                            tracing::warn!(brain = %key, "brain not found in registry, skipping");
                        }
                        Err(e) => {
                            tracing::warn!(brain = %key, error = %e, "failed to open remote brain, skipping");
                        }
                    }
                }

                let federated = FederatedPipeline {
                    db: &ctx.db,
                    brains,
                    embedder,
                    metrics: &ctx.metrics,
                };

                match federated
                    .search(
                        &params.query,
                        &params.intent,
                        params.budget_tokens as usize,
                        params.k as usize,
                        &params.tags,
                    )
                    .await
                {
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
                    if let Some(ref bn) = stub.brain_name {
                        stub_json["brain_name"] = json!(bn);
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
        let tmp = tempfile::TempDir::new().unwrap();
        let brain_home = tmp.path().to_path_buf();
        let sqlite_path = tmp.path().join("test.db");
        let tasks_dir = tmp.path().join("tasks");

        let db = crate::db::Db::open(&sqlite_path).unwrap();
        let tasks_db = crate::db::Db::open(&sqlite_path).unwrap();
        let tasks = crate::tasks::TaskStore::new(&tasks_dir, tasks_db).unwrap();
        let records_dir = tmp.path().join("records");
        let records_db = crate::db::Db::open(&sqlite_path).unwrap();
        let records = crate::records::RecordStore::new(&records_dir, records_db).unwrap();
        let objects_dir = tmp.path().join("objects");
        let objects = crate::records::objects::ObjectStore::new(&objects_dir).unwrap();

        (
            tmp,
            McpContext {
                db,
                store: None,
                writable_store: None,
                embedder: None,
                tasks,
                records,
                objects,
                metrics: Arc::new(crate::metrics::Metrics::new()),
                brain_home,
                brain_name: "test-brain".to_string(),
                brain_id: String::new(),
            },
        )
    }

    /// Build a context with store present but embedder absent.
    async fn create_no_embedder_context() -> (tempfile::TempDir, McpContext) {
        let tmp = tempfile::TempDir::new().unwrap();
        let brain_home = tmp.path().to_path_buf();
        let sqlite_path = tmp.path().join("test.db");
        let lance_path = tmp.path().join("test_lance");
        let tasks_dir = tmp.path().join("tasks");

        let db = crate::db::Db::open(&sqlite_path).unwrap();
        let store = crate::store::Store::open_or_create(&lance_path)
            .await
            .unwrap();
        let store_reader = crate::store::StoreReader::from_store(&store);
        let tasks_db = crate::db::Db::open(&sqlite_path).unwrap();
        let tasks = crate::tasks::TaskStore::new(&tasks_dir, tasks_db).unwrap();
        let records_dir = tmp.path().join("records");
        let records_db = crate::db::Db::open(&sqlite_path).unwrap();
        let records = crate::records::RecordStore::new(&records_dir, records_db).unwrap();
        let objects_dir = tmp.path().join("objects");
        let objects = crate::records::objects::ObjectStore::new(&objects_dir).unwrap();

        (
            tmp,
            McpContext {
                db,
                store: Some(store_reader),
                writable_store: Some(store),
                embedder: None,
                tasks,
                records,
                objects,
                metrics: Arc::new(crate::metrics::Metrics::new()),
                brain_home,
                brain_name: "test-brain".to_string(),
                brain_id: String::new(),
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

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["budget_tokens"], 800);
        assert_eq!(parsed["result_count"], 0);
        assert!(parsed["results"].as_array().unwrap().is_empty());
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

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
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

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
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
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
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
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
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
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
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
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
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
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
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

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
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
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["budget_tokens"], 1_000_000);
    }
}
