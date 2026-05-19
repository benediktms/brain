//! `memory.write_episode` MCP tool — thin wrapper over `DaemonClient::memory_write_episode`.
//!
//! The wire variant carries the core episode fields plus the optional
//! `continues` predecessor pointer. The daemon validates the
//! predecessor (exists / same brain / kind=episode) pre-write, so a
//! missing predecessor rejects the episode write entirely — the same
//! semantics as the legacy MCP surface.
//!
//! Inline `links` remain MCP-layer framing: they apply post-write via
//! per-link `links_add` round-trips. The episode is persisted before
//! any link attempt, mirroring the legacy partial-failure tolerance.
//! When `continues` was set, the synthesized `{to: EPISODE/<prev>,
//! edge_kind: continues}` entry is prepended to the inline-link batch
//! so it appears first in the response's `links` block.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::MemoryWriteEpisodeParams;

use super::helpers::{InlineEntityInput, InlineLinkInput, apply_inline_links, inline_links_schema};
use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[derive(Deserialize)]
struct Params {
    goal: String,
    actions: String,
    outcome: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default = "default_importance")]
    importance: f64,
    #[serde(default)]
    links: Vec<InlineLinkInput>,
    #[serde(default)]
    continues: Option<String>,
}

fn default_importance() -> f64 {
    1.0
}

pub(super) struct MemoryWriteEpisode;

impl McpTool for MemoryWriteEpisode {
    fn name(&self) -> &'static str {
        "memory.write_episode"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Record an episode (goal, actions, outcome) to memory. Returns `{summary_id, uri, ...}`. Optionally pass `continues` (a prior episode's `summary_id`) to extend a thread — equivalent to a `links` entry of `{to: {type: EPISODE, id: <prev>}, edge_kind: continues}`, but ergonomic for the common case. Pass `links` to add edges from the new episode (type EPISODE) to existing TASK/RECORD/PROCEDURE/EPISODE/CHUNK/NOTE entities in one round-trip — the episode persists even if every link fails. When either `continues` or `links` is provided the response carries `links: {succeeded:[{to, edge_kind}], failed:[{to, edge_kind, error}], summary:{succeeded, failed}}` (the `continues` entry appears first). Use `links_add` for any links discovered after the write.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "goal": { "type": "string", "description": "What was the goal" },
                    "actions": { "type": "string", "description": "What actions were taken" },
                    "outcome": { "type": "string", "description": "What was the outcome" },
                    "tags": { "type": "array", "items": { "type": "string" }, "description": "Tags for categorization. Pass as a JSON array, e.g. [\"debugging\", \"auth\"]" },
                    "importance": { "type": "number", "description": "Importance score (0.0 to 1.0). Default: 1.0", "default": 1.0 },
                    "continues": { "type": "string", "description": "Optional. The `summary_id` of a prior episode this episode continues. Internally lowered to a `links` entry of edge_kind `continues` (DAG-validated). The synthesized entry is reported in the response's `links` block, prepended before any explicit entries from `links`." },
                    "links": inline_links_schema("Optional. After the episode is stored, create polymorphic edges from it (as EPISODE) to the listed entities. Partial failures are reported per-link without aborting the write. Prefer the top-level `continues` parameter for thread-extension edges; use `links` for non-thread relationships (covers, relates_to, see_also, etc.).")
                },
                "required": ["goal", "actions", "outcome"]
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

            let importance_millis = (parsed.importance.clamp(0.0, 1.0) * 1000.0) as u32;

            // Pass `continues` to the wire so the daemon validates the
            // predecessor (exists / same brain / kind=episode) pre-
            // write and rejects with a Protocol error if validation
            // fails — preserving the legacy MCP semantics that a
            // missing predecessor aborts the episode write.
            let wire_params = MemoryWriteEpisodeParams {
                goal: parsed.goal.clone(),
                actions: parsed.actions,
                outcome: parsed.outcome,
                tags: parsed.tags.clone(),
                importance_millis,
                continues: parsed.continues.clone(),
            };

            let (summary_id, uri) = match ctx
                .with_client(|c| c.memory_write_episode(wire_params))
                .await
            {
                Ok(pair) => pair,
                Err(e) => return ToolCallResult::error(format!("Failed to store episode: {e}")),
            };

            let mut response = json!({
                "status": "stored",
                "summary_id": summary_id,
                "uri": uri,
                "goal": parsed.goal,
                "tags": parsed.tags,
                "importance": (importance_millis as f64) / 1000.0,
            });

            let mut effective_links = parsed.links;
            if let Some(prev_id) = parsed.continues {
                effective_links.insert(
                    0,
                    InlineLinkInput {
                        to: InlineEntityInput {
                            kind: "EPISODE".into(),
                            id: prev_id,
                        },
                        edge_kind: Some("continues".into()),
                    },
                );
            }

            if !effective_links.is_empty() {
                let links_block =
                    apply_inline_links("EPISODE", &summary_id, effective_links, ctx).await;
                response["links"] = links_block;
            }

            json_response(&response)
        })
    }
}
