//! `memory.reflect` MCP tool — thin wrapper over `DaemonClient::memory_reflect`.
//!
//! Maps the legacy `mode: "prepare" | "commit"` shape onto the wire's
//! `commit: bool` flag, then echoes the daemon's opaque JSON result.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::MemoryReflectParams;

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[derive(Deserialize, Default)]
struct Params {
    #[serde(default = "default_mode")]
    mode: String,
    #[serde(default)]
    topic: Option<String>,
    #[serde(default = "default_budget")]
    budget_tokens: usize,
    #[serde(default)]
    brains: Vec<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    source_ids: Vec<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    importance: Option<f64>,
}

fn default_mode() -> String {
    "prepare".into()
}
fn default_budget() -> usize {
    2000
}

pub(super) struct MemoryReflect;

impl McpTool for MemoryReflect {
    fn name(&self) -> &'static str {
        "memory.reflect"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: concat!(
                "Two-phase episodic reflection.\n\n",
                "**prepare** (default): Retrieve source material — recent episodes and related chunks — ",
                "that the LLM can synthesize into a reflection. Returns structured source material.\n\n",
                "**commit**: Store a completed reflection linked to its source episodes. ",
                "Requires title, content, and source_ids from a prior prepare call."
            ).into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "mode": { "type": "string", "enum": ["prepare", "commit"], "description": "Operation mode. Default: 'prepare'", "default": "prepare" },
                    "topic": { "type": "string", "description": "(prepare) Topic to reflect on" },
                    "budget_tokens": { "type": "integer", "description": "(prepare) Maximum tokens for source material. Default: 2000", "default": 2000 },
                    "brains": { "type": "array", "items": { "type": "string" }, "description": "(prepare) Brain names/IDs to include. Empty = current brain. 'all' = all brains." },
                    "title": { "type": "string", "description": "(commit) Title of the reflection" },
                    "content": { "type": "string", "description": "(commit) Synthesized reflection content" },
                    "source_ids": { "type": "array", "items": { "type": "string" }, "description": "(commit) summary_ids of source episodes used" },
                    "tags": { "type": "array", "items": { "type": "string" }, "description": "(commit) Tags for the reflection" },
                    "importance": { "type": "number", "description": "(commit) Importance score (0.0–1.0). Default: 1.0", "default": 1.0 }
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

            let commit = match parsed.mode.as_str() {
                "commit" => true,
                "prepare" => false,
                other => {
                    return ToolCallResult::error(format!(
                        "Invalid mode '{other}': expected 'prepare' or 'commit'"
                    ));
                }
            };

            let importance_millis = parsed
                .importance
                .map(|v| (v.clamp(0.0, 1.0) * 1000.0) as u32);

            let wire_params = MemoryReflectParams {
                commit,
                topic: parsed.topic,
                budget: parsed.budget_tokens,
                brains: parsed.brains,
                title: parsed.title,
                content: parsed.content,
                source_ids: parsed.source_ids,
                tags: parsed.tags,
                importance_millis,
            };

            match ctx.with_client(|c| c.memory_reflect(wire_params)).await {
                Ok(result_json) => ToolCallResult::text(result_json),
                Err(e) => ToolCallResult::error(format!("Reflect failed: {e}")),
            }
        })
    }
}
