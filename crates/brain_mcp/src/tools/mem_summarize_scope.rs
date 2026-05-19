//! `memory.summarize_scope` MCP tool — thin wrapper over `DaemonClient::memory_summarize_scope`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::MemorySummarizeScopeParams;

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[derive(Deserialize)]
struct Params {
    scope_type: String,
    scope_value: String,
    #[serde(default)]
    regenerate: bool,
    #[serde(default = "default_async_llm")]
    async_llm: bool,
}

fn default_async_llm() -> bool {
    true
}

pub(super) struct MemorySummarizeScope;

impl McpTool for MemorySummarizeScope {
    fn name(&self) -> &'static str {
        "memory.summarize_scope"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: concat!(
                "Generate or retrieve an extractive summary of memory chunks ",
                "scoped to a directory path or tag.\n\n",
                "When `regenerate` is false (default), returns any cached summary. ",
                "Set `regenerate: true` to force a fresh extraction. ",
                "When `async_llm` is true (default), the tool enqueues an async LLM job ",
                "and returns the extractive placeholder immediately. ",
                "The response includes a `stale` flag when the cached summary is ",
                "out of date and should be regenerated on the next call."
            )
            .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "scope_type": { "type": "string", "enum": ["directory", "tag"], "description": "Whether to scope the summary by directory path or by tag." },
                    "scope_value": { "type": "string", "description": "The directory path (e.g. \"src/auth/\") or tag name (e.g. \"rust\")." },
                    "regenerate": { "type": "boolean", "description": "Force regeneration of the summary even if one already exists. Default: false.", "default": false },
                    "async_llm": { "type": "boolean", "description": "Enqueue an async LLM refresh after generating the placeholder summary. Default: true.", "default": true }
                },
                "required": ["scope_type", "scope_value"]
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

            let wire_params = MemorySummarizeScopeParams {
                scope_type: parsed.scope_type,
                scope_value: parsed.scope_value,
                regenerate: parsed.regenerate,
                async_llm: parsed.async_llm,
            };

            match ctx
                .with_client(|c| c.memory_summarize_scope(wire_params))
                .await
            {
                Ok(result_json) => ToolCallResult::text(result_json),
                Err(e) => ToolCallResult::error(format!("Failed to summarize scope: {e}")),
            }
        })
    }
}
