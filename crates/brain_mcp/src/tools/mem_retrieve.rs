//! `memory.retrieve` MCP tool — thin wrapper over `DaemonClient::memory_retrieve`.
//!
//! The daemon owns retrieval orchestration (federated brains, URI vs
//! query mode, LOD fallback, search ranking). The MCP tool body
//! parses the surface params into the wire shape and echoes the
//! daemon's opaque JSON result.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::MemoryRetrieveParams;

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[derive(Deserialize, Default)]
struct Params {
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    uri: Option<String>,
    #[serde(default = "default_lod")]
    lod: String,
    #[serde(default = "default_count")]
    count: u64,
    #[serde(default = "default_strategy")]
    strategy: String,
    #[serde(default)]
    brain: Option<String>,
    #[serde(default)]
    brains: Vec<String>,
    #[serde(default)]
    time_scope: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    kinds: Vec<String>,
    #[serde(default)]
    time_after: Option<i64>,
    #[serde(default)]
    time_before: Option<i64>,
    #[serde(default)]
    tags_require: Vec<String>,
    #[serde(default)]
    tags_exclude: Vec<String>,
    #[serde(default)]
    explain: bool,
}

fn default_lod() -> String {
    "L0".into()
}
fn default_count() -> u64 {
    10
}
fn default_strategy() -> String {
    "auto".into()
}

pub(super) struct MemoryRetrieve;

impl McpTool for MemoryRetrieve {
    fn name(&self) -> &'static str {
        "memory.retrieve"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Retrieve memory chunks at a requested level of detail (LOD). Supports two modes: query (semantic search) and URI (direct access by synapse:// address). L0 returns extractive summaries (~100 tokens each), L1 returns LLM-summarized content (~2000 tokens each), L2 returns full source content. Falls back to the next available level when the requested LOD is not yet generated. Provide `query` for semantic search or `uri` for direct access.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Natural language search query. Provide either query or uri." },
                    "uri": { "type": "string", "description": "Direct access by synapse:// URI (e.g. synapse://brain-name/memory/chunk-id). Provide either query or uri." },
                    "lod": { "type": "string", "enum": ["L0", "L1", "L2"], "description": "Level of detail for returned content. L0: extractive abstract (~100 tokens). L1: LLM summary (~2000 tokens). L2: full source passthrough. Default: L0", "default": "L0" },
                    "count": { "type": "integer", "description": "Maximum number of results. Default: 10", "default": 10 },
                    "strategy": { "type": "string", "enum": ["lookup", "planning", "reflection", "synthesis", "auto"], "description": "Retrieval strategy — controls ranking weight profile. Default: auto", "default": "auto" },
                    "brain": { "type": "string", "description": "Optional brain name or ID to scope search to a single brain" },
                    "brains": { "type": "array", "items": { "type": "string" }, "description": "Optional list of brain names or IDs to search across multiple brains. Use [\"all\"] to search all registered brains." },
                    "time_scope": { "type": "string", "description": "Relative time window, e.g. \"7d\" or \"24h\". Sets time_after to now minus the duration." },
                    "tags": { "type": "array", "items": { "type": "string" }, "description": "Tags to boost results via Jaccard similarity" },
                    "kinds": { "type": "array", "items": { "type": "string", "enum": ["note", "episode", "reflection", "procedure", "task", "task-outcome", "record"] }, "description": "Filter by result kind. Empty = all kinds." },
                    "time_after": { "type": "integer", "description": "Only results modified/created after this Unix timestamp (seconds)" },
                    "time_before": { "type": "integer", "description": "Only results modified/created before this Unix timestamp (seconds)" },
                    "tags_require": { "type": "array", "items": { "type": "string" }, "description": "Require ALL of these tags (AND logic, case-insensitive)" },
                    "tags_exclude": { "type": "array", "items": { "type": "string" }, "description": "Exclude results matching ANY of these tags (NOR logic, case-insensitive)" },
                    "explain": { "type": "boolean", "description": "When true, include per-signal score breakdowns in the response. Default: false", "default": false },
                    "vector_search_mode": { "type": "string", "enum": ["exact", "ann_refined", "ann_fast"], "description": "Vector search strategy. Default: ann_refined" }
                }
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

            let has_query = parsed.query.as_ref().is_some_and(|q| !q.trim().is_empty());
            let has_uri = parsed.uri.is_some();
            if has_query && has_uri {
                return ToolCallResult::error("Provide 'query' or 'uri', not both");
            }
            if !has_query && !has_uri {
                return ToolCallResult::error("Either 'query' or 'uri' is required");
            }

            // Pre-validate LOD wording so the error matches legacy
            // byte-for-byte; the daemon also validates but emits a
            // slightly different envelope.
            if !matches!(parsed.lod.to_uppercase().as_str(), "L0" | "L1" | "L2") {
                return ToolCallResult::error(format!(
                    "Invalid lod value {:?}: must be one of L0, L1, L2",
                    parsed.lod
                ));
            }

            // `brain` (single) folds into `brains` (list) so the wire
            // sees one source of truth.
            let mut brains = parsed.brains;
            if let Some(b) = parsed.brain
                && brains.is_empty()
            {
                brains.push(b);
            }

            let wire_params = MemoryRetrieveParams {
                query: parsed.query,
                uri: parsed.uri,
                lod: parsed.lod,
                count: parsed.count,
                strategy: parsed.strategy,
                brains,
                time_scope: parsed.time_scope,
                time_after: parsed.time_after,
                time_before: parsed.time_before,
                tags: parsed.tags,
                tags_require: parsed.tags_require,
                tags_exclude: parsed.tags_exclude,
                kinds: parsed.kinds,
                explain: parsed.explain,
            };

            match ctx.with_client(|c| c.memory_retrieve(wire_params)).await {
                Ok(result_json) => ToolCallResult::text(result_json),
                Err(e) => ToolCallResult::error(format!("Retrieve failed: {e}")),
            }
        })
    }
}
