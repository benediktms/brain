//! `memory.consolidate` MCP tool — thin wrapper over `DaemonClient::memory_consolidate`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::MemoryConsolidateParams;

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[derive(Deserialize, Default)]
struct Params {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default = "default_gap")]
    gap_seconds: i64,
    #[serde(default)]
    auto_summarize: bool,
}

fn default_limit() -> usize {
    50
}
fn default_gap() -> i64 {
    3600
}

pub(super) struct MemoryConsolidate;

impl McpTool for MemoryConsolidate {
    fn name(&self) -> &'static str {
        "memory.consolidate"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: concat!(
                "Group recent episodes by temporal proximity into consolidation clusters.\n\n",
                "Returns clusters of temporally proximate episodes with suggested titles ",
                "and summaries. Clusters are ordered newest-first. Use the output to decide ",
                "which episodes to synthesize into a reflection via `memory.reflect`."
            )
            .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "limit": { "type": "integer", "description": "Maximum number of recent episodes to consider. Default: 50", "default": 50 },
                    "gap_seconds": { "type": "integer", "description": "Gap in seconds that separates two clusters. Default: 3600 (1 hour)", "default": 3600 },
                    "auto_summarize": { "type": "boolean", "description": "Enqueue async LLM synthesis jobs for each cluster. Default: false", "default": false }
                },
                "required": []
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let wire_params = MemoryConsolidateParams {
                limit: parsed.limit,
                gap_seconds: parsed.gap_seconds,
                auto_summarize: parsed.auto_summarize,
            };

            match ctx.with_client(|c| c.memory_consolidate(wire_params)).await {
                Ok(result_json) => ToolCallResult::text(result_json),
                Err(e) => ToolCallResult::error(format!("Failed to consolidate: {e}")),
            }
        })
    }
}
