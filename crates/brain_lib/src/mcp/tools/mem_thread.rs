use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::links::collect_thread_episodes;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::uri::SynapseUri;

use super::{McpTool, json_response};

/// Default BFS depth bound when the caller does not specify `max_depth`.
/// Threads are linear via DAG-validated `continues` edges, so 32 hops
/// covers the long tail of typical agent saga lengths without risking
/// pathological neighbourhoods.
const DEFAULT_MAX_DEPTH: u32 = 32;

#[derive(Deserialize)]
struct Params {
    seed_memory_id: String,
    #[serde(default)]
    max_depth: Option<u32>,
}

pub(super) struct MemThread;

impl MemThread {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if params.seed_memory_id.is_empty() {
            return ToolCallResult::error("seed_memory_id must not be empty");
        }

        let max_depth = params.max_depth.unwrap_or(DEFAULT_MAX_DEPTH);

        let thread_ids =
            match ctx.stores.inner_db().with_read_conn(|conn| {
                collect_thread_episodes(conn, &params.seed_memory_id, max_depth)
            }) {
                Ok(ids) => ids,
                Err(e) => return ToolCallResult::error(format!("Failed to walk thread: {e}")),
            };

        // The helper returns IDs only; hydrate to full episode rows so the
        // agent does not have to round-trip through `memory.retrieve`.
        let rows = match ctx.stores.get_summaries_by_ids(&thread_ids) {
            Ok(rows) => rows,
            Err(e) => return ToolCallResult::error(format!("Failed to hydrate thread: {e}")),
        };

        // Re-sort after hydration to preserve the helper's contract
        // (created_at ASC, ID ASC for ties). `get_summaries_by_ids` does
        // not promise any particular order.
        let mut rows = rows;
        rows.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.summary_id.cmp(&b.summary_id))
        });

        let episodes: Vec<Value> = rows
            .into_iter()
            .map(|row| {
                let uri = SynapseUri::for_episode(ctx.brain_name(), &row.summary_id).to_string();
                json!({
                    "summary_id": row.summary_id,
                    "uri": uri,
                    "title": row.title,
                    "content": row.content,
                    "tags": row.tags,
                    "importance": row.importance,
                    "created_at": row.created_at,
                })
            })
            .collect();

        let count = episodes.len();
        json_response(&json!({
            "seed_memory_id": params.seed_memory_id,
            "count": count,
            "thread": episodes,
        }))
    }
}

impl McpTool for MemThread {
    fn name(&self) -> &'static str {
        "memory.thread"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Walk an episode thread by following only `continues` edges from a seed. Returns all episodes reachable via `continues` (both predecessors of the seed and successors), ordered by `created_at` ASC with `summary_id` as tiebreaker. Companion to the `continues` parameter on `memory.write_episode`. Walks bidirectionally — returns the full thread including any forks.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "seed_memory_id": {
                        "type": "string",
                        "description": "The `summary_id` of any episode in the thread. The walk recovers the full thread regardless of which member is passed."
                    },
                    "max_depth": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "BFS depth bound. Default: 32. Clamped internally to MAX_VISITED (1024)."
                    }
                },
                "required": ["seed_memory_id"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(std::future::ready(self.execute(params, ctx)))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;
    use super::{McpTool, MemThread};

    /// Build a thread of three episodes and return their summary_ids in chronological order.
    async fn seed_thread(ctx: &crate::mcp::McpContext) -> (String, String, String) {
        let registry = ToolRegistry::new();

        let head = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Thread head",
                    "actions": "First step",
                    "outcome": "Stored"
                }),
                ctx,
            )
            .await;
        let head_id =
            serde_json::from_str::<serde_json::Value>(&head.content[0].text).unwrap()["summary_id"]
                .as_str()
                .unwrap()
                .to_string();

        let mid = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Thread mid",
                    "actions": "Second step",
                    "outcome": "Stored",
                    "continues": &head_id,
                }),
                ctx,
            )
            .await;
        let mid_id =
            serde_json::from_str::<serde_json::Value>(&mid.content[0].text).unwrap()["summary_id"]
                .as_str()
                .unwrap()
                .to_string();

        let tail = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Thread tail",
                    "actions": "Third step",
                    "outcome": "Stored",
                    "continues": &mid_id,
                }),
                ctx,
            )
            .await;
        let tail_id =
            serde_json::from_str::<serde_json::Value>(&tail.content[0].text).unwrap()["summary_id"]
                .as_str()
                .unwrap()
                .to_string();

        (head_id, mid_id, tail_id)
    }

    #[tokio::test]
    async fn test_thread_returns_full_chain_in_order() {
        let (_dir, ctx) = create_test_context().await;
        let (head_id, mid_id, tail_id) = seed_thread(&ctx).await;

        let result = MemThread.execute(json!({ "seed_memory_id": &mid_id }), &ctx);
        assert_ne!(result.is_error, Some(true), "got: {:?}", result);

        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 3);
        assert_eq!(parsed["seed_memory_id"], mid_id);

        let thread = parsed["thread"].as_array().unwrap();
        assert_eq!(thread.len(), 3);
        // Ordered by created_at — head first, tail last.
        assert_eq!(thread[0]["summary_id"], head_id);
        assert_eq!(thread[1]["summary_id"], mid_id);
        assert_eq!(thread[2]["summary_id"], tail_id);
        // Each entry carries the resolved episode shape.
        assert!(thread[0]["uri"].is_string());
        assert!(thread[0]["content"].is_string());
        assert!(thread[0]["created_at"].is_i64());
    }

    #[tokio::test]
    async fn test_thread_walks_from_any_member() {
        let (_dir, ctx) = create_test_context().await;
        let (head_id, _mid_id, tail_id) = seed_thread(&ctx).await;

        // From head — full thread reachable forward.
        let from_head = MemThread.execute(json!({ "seed_memory_id": &head_id }), &ctx);
        let parsed: serde_json::Value = serde_json::from_str(&from_head.content[0].text).unwrap();
        assert_eq!(parsed["count"], 3);

        // From tail — full thread reachable backward.
        let from_tail = MemThread.execute(json!({ "seed_memory_id": &tail_id }), &ctx);
        let parsed: serde_json::Value = serde_json::from_str(&from_tail.content[0].text).unwrap();
        assert_eq!(parsed["count"], 3);
    }

    #[tokio::test]
    async fn test_thread_missing_seed_returns_empty() {
        let (_dir, ctx) = create_test_context().await;
        let result = MemThread.execute(json!({ "seed_memory_id": "01KQNONEXISTENTSEED000" }), &ctx);
        assert_ne!(result.is_error, Some(true));
        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 0);
        assert_eq!(parsed["thread"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_thread_empty_seed_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let result = MemThread.execute(json!({ "seed_memory_id": "" }), &ctx);
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("must not be empty"), "got: {text}");
    }

    #[tokio::test]
    async fn test_thread_isolated_episode_returns_self_only() {
        // An episode written with no `continues` parameter is its own thread.
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let isolated = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Standalone",
                    "actions": "No predecessor",
                    "outcome": "Stored"
                }),
                &ctx,
            )
            .await;
        let id = serde_json::from_str::<serde_json::Value>(&isolated.content[0].text).unwrap()
            ["summary_id"]
            .as_str()
            .unwrap()
            .to_string();

        let result = MemThread.execute(json!({ "seed_memory_id": &id }), &ctx);
        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["thread"][0]["summary_id"], id);
    }

    #[test]
    fn test_thread_schema_stable() {
        let tool = MemThread;
        let schema = tool.definition().input_schema;
        let expected = json!({
            "type": "object",
            "properties": {
                "seed_memory_id": {
                    "type": "string",
                    "description": "The `summary_id` of any episode in the thread. The walk recovers the full thread regardless of which member is passed."
                },
                "max_depth": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "BFS depth bound. Default: 32. Clamped internally to MAX_VISITED (1024)."
                }
            },
            "required": ["seed_memory_id"]
        });
        assert_eq!(
            schema, expected,
            "memory.thread schema changed — update golden or revert"
        );
    }
}
