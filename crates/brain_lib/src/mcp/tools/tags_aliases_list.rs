//! `tags.aliases_list` — read-only listing of `tag_aliases` rows.
//!
//! Filters by optional `canonical` or `cluster_id` and paginates via
//! `limit`/`offset`. Returns inspection-shaped rows (no embedding BLOB).

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

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
                canonical_tag, cluster_id, last_run_id, embedder_version, and updated_at."
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

            let rows = match ctx.stores.list_tag_aliases(
                parsed.canonical.as_deref(),
                parsed.cluster_id.as_deref(),
                parsed.limit,
                parsed.offset,
            ) {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn aliases_list_returns_empty_on_fresh_brain() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("tags.aliases_list", json!({}), &ctx)
            .await;
        assert!(result.is_error.is_none(), "{}", result.content[0].text);

        let parsed: serde_json::Value =
            serde_json::from_str(&result.content[0].text).expect("valid json");
        assert_eq!(parsed["aliases"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["filters"]["limit"], 50);
        assert_eq!(parsed["filters"]["offset"], 0);
    }

    #[tokio::test]
    async fn aliases_list_rejects_negative_offset() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("tags.aliases_list", json!({ "offset": -1 }), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
    }
}
