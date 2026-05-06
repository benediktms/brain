use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use brain_persistence::db::links::collect_thread_episode_rows;

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
    seed_summary_id: String,
    #[serde(default)]
    max_depth: Option<u32>,
}

pub(super) struct MemWalkThread;

impl MemWalkThread {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if params.seed_summary_id.is_empty() {
            return ToolCallResult::error("seed_summary_id must not be empty");
        }

        let max_depth = params.max_depth.unwrap_or(DEFAULT_MAX_DEPTH);

        // Single hydration: the helper returns sorted rows + a truncation
        // flag. No re-query, no re-sort.
        let result = match ctx.stores.inner_db().with_read_conn(|conn| {
            collect_thread_episode_rows(conn, &params.seed_summary_id, max_depth)
        }) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Failed to walk thread: {e}")),
        };

        // Defense in depth: filter cross-brain rows even though the typed
        // `continues` parameter on `memory.write_episode` rejects cross-brain
        // predecessors. Direct DB writes or future writers could produce
        // edges crossing brain boundaries; we never surface them here.
        let current_brain_id = ctx.brain_id();
        let total_before_filter = result.rows.len();
        let rows: Vec<_> = result
            .rows
            .into_iter()
            .filter(|row| row.brain_id == current_brain_id)
            .collect();
        if rows.len() < total_before_filter {
            warn!(
                seed = %params.seed_summary_id,
                brain_id = %current_brain_id,
                dropped = total_before_filter - rows.len(),
                "memory.walk_thread: dropped cross-brain rows from thread"
            );
        }

        let episodes: Vec<Value> = rows
            .into_iter()
            .map(|row| {
                let uri = SynapseUri::for_episode(ctx.brain_name(), &row.summary_id).to_string();
                json!({
                    "summary_id": row.summary_id,
                    "uri": uri,
                    "kind": "episode",
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
            "seed_summary_id": params.seed_summary_id,
            "count": count,
            "truncated": result.truncated,
            "thread": episodes,
        }))
    }
}

impl McpTool for MemWalkThread {
    fn name(&self) -> &'static str {
        "memory.walk_thread"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Walk an episode thread by following only `continues` edges from a seed. Returns all episodes reachable via `continues` (both predecessors of the seed and successors), ordered by `created_at` ASC with `summary_id` as tiebreaker. Companion to the `continues` parameter on `memory.write_episode`. Walks bidirectionally — returns the full thread including any forks. Response shape: `{ seed_summary_id, count, truncated, thread: [{ summary_id, uri, kind, title, content, tags, importance, created_at }] }`. The `truncated` flag is `true` when the BFS halted at the visited cap (1024 episodes) before exhausting the neighbourhood. Cross-brain rows are filtered out defensively.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "seed_summary_id": {
                        "type": "string",
                        "description": "The `summary_id` of any episode in the thread. The walk recovers the full thread regardless of which member is passed."
                    },
                    "max_depth": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "BFS depth bound. Default: 32. The visited set is also capped at MAX_VISITED (1024) episodes; if the cap is hit, `truncated: true` is set in the response."
                    }
                },
                "required": ["seed_summary_id"]
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
    use super::{McpTool, MemWalkThread};

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
    async fn test_walk_thread_returns_full_chain_in_order() {
        let (_dir, ctx) = create_test_context().await;
        let (head_id, mid_id, tail_id) = seed_thread(&ctx).await;

        let result = MemWalkThread.execute(json!({ "seed_summary_id": &mid_id }), &ctx);
        assert_ne!(result.is_error, Some(true), "got: {:?}", result);

        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 3);
        assert_eq!(parsed["seed_summary_id"], mid_id);
        assert_eq!(parsed["truncated"], false, "small thread must not truncate");

        let thread = parsed["thread"].as_array().unwrap();
        assert_eq!(thread.len(), 3);
        // Ordered by created_at — head first, tail last.
        assert_eq!(thread[0]["summary_id"], head_id);
        assert_eq!(thread[1]["summary_id"], mid_id);
        assert_eq!(thread[2]["summary_id"], tail_id);
        // Each entry carries the resolved episode shape.
        assert_eq!(thread[0]["kind"], "episode");
        assert!(thread[0]["uri"].is_string());
        assert!(thread[0]["content"].is_string());
        assert!(thread[0]["created_at"].is_i64());
    }

    #[tokio::test]
    async fn test_walk_thread_walks_from_any_member() {
        let (_dir, ctx) = create_test_context().await;
        let (head_id, _mid_id, tail_id) = seed_thread(&ctx).await;

        // From head — full thread reachable forward.
        let from_head = MemWalkThread.execute(json!({ "seed_summary_id": &head_id }), &ctx);
        let parsed: serde_json::Value = serde_json::from_str(&from_head.content[0].text).unwrap();
        assert_eq!(parsed["count"], 3);

        // From tail — full thread reachable backward.
        let from_tail = MemWalkThread.execute(json!({ "seed_summary_id": &tail_id }), &ctx);
        let parsed: serde_json::Value = serde_json::from_str(&from_tail.content[0].text).unwrap();
        assert_eq!(parsed["count"], 3);
    }

    #[tokio::test]
    async fn test_walk_thread_missing_seed_returns_empty() {
        let (_dir, ctx) = create_test_context().await;
        let result =
            MemWalkThread.execute(json!({ "seed_summary_id": "01KQNONEXISTENTSEED000" }), &ctx);
        assert_ne!(result.is_error, Some(true));
        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 0);
        assert_eq!(parsed["thread"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_walk_thread_empty_seed_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let result = MemWalkThread.execute(json!({ "seed_summary_id": "" }), &ctx);
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("must not be empty"), "got: {text}");
    }

    #[tokio::test]
    async fn test_walk_thread_isolated_episode_returns_self_only() {
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

        let result = MemWalkThread.execute(json!({ "seed_summary_id": &id }), &ctx);
        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1);
        assert_eq!(parsed["thread"][0]["summary_id"], id);
    }

    #[tokio::test]
    async fn test_walk_thread_max_depth_zero_returns_seed_only() {
        let (_dir, ctx) = create_test_context().await;
        let (_head_id, mid_id, _tail_id) = seed_thread(&ctx).await;

        let result =
            MemWalkThread.execute(json!({ "seed_summary_id": &mid_id, "max_depth": 0 }), &ctx);
        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["count"], 1, "max_depth=0 must return only the seed");
        assert_eq!(parsed["thread"][0]["summary_id"], mid_id);
    }

    #[test]
    fn test_walk_thread_schema_stable() {
        let tool = MemWalkThread;
        let schema = tool.definition().input_schema;
        let expected = json!({
            "type": "object",
            "properties": {
                "seed_summary_id": {
                    "type": "string",
                    "description": "The `summary_id` of any episode in the thread. The walk recovers the full thread regardless of which member is passed."
                },
                "max_depth": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "BFS depth bound. Default: 32. The visited set is also capped at MAX_VISITED (1024) episodes; if the cap is hit, `truncated: true` is set in the response."
                }
            },
            "required": ["seed_summary_id"]
        });
        assert_eq!(
            schema, expected,
            "memory.walk_thread schema changed — update golden or revert"
        );
    }
}
