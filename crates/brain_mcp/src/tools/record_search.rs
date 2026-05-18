//! `records.search` MCP tool — thin wrapper over
//! [`brain_rpc::DaemonClient::records_search`].
//!
//! The daemon owns the hybrid retrieval pipeline (FTS + embedding +
//! optional federation), filters results to `kind == "record"`, packs
//! within the token budget, and returns a typed
//! [`brain_rpc::RecordsSearchReport`]. Serialising that report directly
//! yields the legacy MCP envelope byte-for-byte
//! (`{budget_tokens, used_tokens_est, result_count, total_available,
//! results: [{record_id, memory_id, title, summary, score, kind, uri,
//! brain_name?}]}`), so this tool body has no JSON-shape logic of its
//! own.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::RecordsSearchParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct RecordSearch;

#[derive(Deserialize)]
struct Params {
    query: String,
    #[serde(default = "default_k")]
    k: u64,
    #[serde(default = "default_budget")]
    budget_tokens: u64,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    brains: Vec<String>,
}

fn default_k() -> u64 {
    10
}

fn default_budget() -> u64 {
    800
}

impl McpTool for RecordSearch {
    fn name(&self) -> &'static str {
        "records.search"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Search records (artifacts, snapshots, documents) using semantic + \
                keyword hybrid retrieval. Returns only record-kind results, filtered from the \
                full hybrid pipeline. Use this to find previously saved artifacts, snapshots, \
                reports, and documents by content."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query"
                    },
                    "k": {
                        "type": "integer",
                        "description": "Maximum number of results to return. Default: 10",
                        "default": 10
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 800",
                        "default": 800
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional tags to filter results. Pass as a JSON array, e.g. [\"drone-checkpoint\", \"wave:1\"]"
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional list of brain names or IDs to search across multiple brains. Use [\"all\"] to search all registered brains. When omitted, searches only the current brain."
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
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let wire_params = RecordsSearchParams {
                query: parsed.query,
                k: parsed.k,
                budget_tokens: parsed.budget_tokens,
                tags: parsed.tags,
                brains: parsed.brains,
            };

            let report = match ctx.with_client(|c| c.records_search(wire_params)).await {
                Ok(r) => r,
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to search records: {err}"));
                }
            };

            json_response(&report)
        })
    }
}
