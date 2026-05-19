//! `memory.walk_thread` MCP tool — thin wrapper over `DaemonClient::memory_walk_thread`.
//!
//! Daemon owns the BFS walk. The MCP tool body validates the seed
//! up-front (matches the legacy "must not be empty" error wording),
//! wraps the input as opaque JSON for the wire, and echoes the daemon's
//! result JSON.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::MemoryWalkThreadParams;

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[derive(Deserialize)]
struct Params {
    seed_summary_id: String,
    #[serde(default)]
    max_depth: Option<u64>,
}

pub(super) struct MemoryWalkThread;

impl McpTool for MemoryWalkThread {
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
        Box::pin(async move {
            let parsed: Params = match serde_json::from_value(params.clone()) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };
            if parsed.seed_summary_id.trim().is_empty() {
                return ToolCallResult::error("seed_summary_id must not be empty");
            }

            let mut body = json!({ "seed_summary_id": parsed.seed_summary_id });
            if let Some(depth) = parsed.max_depth {
                body["max_depth"] = json!(depth);
            }

            let wire_params = MemoryWalkThreadParams { params_json: body };

            match ctx.with_client(|c| c.memory_walk_thread(wire_params)).await {
                Ok(result_json) => ToolCallResult::text(result_json),
                Err(e) => ToolCallResult::error(format!("Failed to walk thread: {e}")),
            }
        })
    }
}
