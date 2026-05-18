//! `records.search` MCP tool.
//!
//! # Wire gap
//!
//! There is no typed `records_search` / semantic-search wire method on
//! `DaemonClient`. The legacy tool routed through the in-process
//! `query_pipeline` (FTS + embedding) which is owned by `brain_lib` and
//! requires direct store access — neither of which is available in
//! `brain_mcp` by architectural ratchet.
//!
//! This stub preserves the byte-identical JSON Schema so clients do not need
//! a schema update, and returns a structured error until a `RecordsSearch`
//! wire variant is added to `brain_rpc`.
//!
//! When the wire method lands, replace the `call` body with:
//! `ctx.with_client(|c| c.records_search(wire_params))`.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct RecordSearch;

#[derive(Deserialize)]
struct Params {
    #[allow(dead_code)]
    query: String,
    #[serde(default = "default_k")]
    #[allow(dead_code)]
    k: u64,
    #[serde(default = "default_budget")]
    #[allow(dead_code)]
    budget_tokens: u64,
    #[serde(default)]
    #[allow(dead_code)]
    tags: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
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
        _ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            // Validate params eagerly so missing `query` returns the expected
            // "Invalid parameters" error (matching legacy behaviour).
            let _parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            ToolCallResult::error(
                "records.search is not yet wired in brain_mcp: \
                 no DaemonClient::records_search wire method exists. \
                 The semantic/FTS query pipeline is server-side in brain_lib \
                 and requires a dedicated RPC variant to cross the daemon boundary. \
                 Use memory.retrieve with kind filter as an interim alternative."
                    .to_string(),
            )
        })
    }
}
