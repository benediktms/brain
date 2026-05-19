//! `tags.aliases_list` MCP tool — thin wrapper over
//! `DaemonClient::tags_aliases_list`.
//!
//! Filters by optional `canonical` or `cluster_id` and paginates via
//! `limit`/`offset`. The wire-side `TagAliasSummary` carries
//! raw_tag / canonical_tag / cluster_id / updated_at; legacy MCP also
//! surfaced last_run_id and embedder_version per row but those fields
//! were dropped from the wire variant on purpose. Migration preserves
//! the legacy `{filters, aliases}` envelope.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::TagsAliasesListParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

const DEFAULT_LIMIT: i64 = 50;

fn default_limit() -> i64 {
    DEFAULT_LIMIT
}

#[derive(Deserialize, Default)]
struct Params {
    canonical: Option<String>,
    cluster_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

pub(super) struct TagsAliasesList;

impl McpTool for TagsAliasesList {
    fn name(&self) -> &'static str {
        "tags.aliases_list"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "List tag_aliases rows for the current brain with optional filtering. \
                Filter by `canonical` (exact match on canonical_tag) and/or `cluster_id`; \
                paginate via `limit` and `offset`. Returns one row per raw_tag with its \
                canonical_tag, cluster_id, and updated_at."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "canonical": {
                        "type": "string",
                        "description": "Filter to rows whose canonical_tag equals this value."
                    },
                    "cluster_id": {
                        "type": "string",
                        "description": "Filter to rows in the given cluster_id."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return. Default: 50.",
                        "default": 50,
                        "minimum": 1
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Row offset for pagination. Default: 0.",
                        "default": 0,
                        "minimum": 0
                    }
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
            if parsed.limit <= 0 {
                return ToolCallResult::error("limit must be positive".to_string());
            }
            if parsed.offset < 0 {
                return ToolCallResult::error("offset must be non-negative".to_string());
            }

            let wire_params = TagsAliasesListParams {
                canonical: parsed.canonical.clone(),
                cluster_id: parsed.cluster_id.clone(),
                limit: parsed.limit,
                offset: parsed.offset,
            };

            let rows = match ctx.with_client(|c| c.tags_aliases_list(wire_params)).await {
                Ok(rows) => rows,
                Err(e) => return ToolCallResult::error(format!("list_tag_aliases: {e}")),
            };

            json_response(&json!({
                "filters": {
                    "canonical": parsed.canonical,
                    "cluster_id": parsed.cluster_id,
                    "limit": parsed.limit,
                    "offset": parsed.offset,
                },
                "aliases": rows,
            }))
        })
    }
}
