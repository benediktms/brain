use std::future::Future;
use std::pin::Pin;

use serde::Serialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, Warning, inject_warnings, json_response};

pub(super) struct BrainsList;

#[derive(Serialize)]
struct BrainInfo {
    name: String,
    id: Option<String>,
    root: String,
    aliases: Vec<String>,
    extra_roots: Vec<String>,
    prefix: Option<String>,
    archived: bool,
}

#[derive(Serialize)]
struct BrainsListResponse {
    brains: Vec<BrainInfo>,
    count: usize,
}

impl McpTool for BrainsList {
    fn name(&self) -> &'static str {
        "brains.list"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "List all registered brain projects from the global registry. Returns name, ID, root path, aliases, extra_roots, task prefix, and archived status for each brain. By default, archived brains are excluded. Pass include_archived: true to include them. Use this to discover available brains before cross-brain operations (federated search via memory.search_minimal with brains parameter, or cross-brain task creation).".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "include_archived": {
                        "type": "boolean",
                        "description": "When true, include archived brains in the results. Defaults to false."
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
            let include_archived = params
                .get("include_archived")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let active_only = !include_archived;
            let rows = match ctx.stores.list_brains(active_only) {
                Ok(r) => r,
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to list brains: {err}"));
                }
            };

            let mut warnings: Vec<Warning> = Vec::new();
            let brains: Vec<BrainInfo> = rows
                .into_iter()
                .filter_map(|row| {
                    let roots: Vec<String> = match row.roots_json.as_deref() {
                        Some(raw) => match serde_json::from_str(raw) {
                            Ok(parsed) => parsed,
                            Err(e) => {
                                warn!(
                                    brain = %row.name,
                                    error = %e,
                                    "skipping brain with malformed roots_json"
                                );
                                warnings.push(Warning {
                                    source: format!("roots_json({})", row.name),
                                    error: e.to_string(),
                                });
                                return None;
                            }
                        },
                        None => Vec::new(),
                    };
                    let root = match roots.first() {
                        Some(first) => first.clone(),
                        None => String::new(),
                    };
                    let extra_roots = roots.into_iter().skip(1).collect();

                    let aliases: Vec<String> = match row.aliases_json.as_deref() {
                        Some(raw) => match serde_json::from_str(raw) {
                            Ok(parsed) => parsed,
                            Err(e) => {
                                warn!(
                                    brain = %row.name,
                                    error = %e,
                                    "skipping brain with malformed aliases_json"
                                );
                                warnings.push(Warning {
                                    source: format!("aliases_json({})", row.name),
                                    error: e.to_string(),
                                });
                                return None;
                            }
                        },
                        None => Vec::new(),
                    };

                    Some(BrainInfo {
                        name: row.name,
                        id: Some(row.brain_id),
                        root,
                        aliases,
                        extra_roots,
                        prefix: row.prefix,
                        archived: row.archived,
                    })
                })
                .collect();

            let count = brains.len();
            let mut result = match serde_json::to_value(BrainsListResponse { brains, count }) {
                Ok(v) => v,
                Err(e) => {
                    return ToolCallResult::error(format!("Internal serialization error: {e}"));
                }
            };
            inject_warnings(&mut result, warnings);
            json_response(&result)
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_brains_list_returns_valid_json() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry.dispatch("brains.list", json!({}), &ctx).await;
        assert!(result.is_error.is_none(), "brains.list should not error");

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");

        assert!(parsed.get("brains").is_some());
        assert!(parsed.get("count").is_some());
        assert!(parsed["brains"].is_array());
    }

    // Note: integration tests verifying archived brain filtering (default excludes, include_archived
    // includes) would require extending create_test_context to produce a config with an archived
    // brain entry. The filtering logic is exercised by the retain() call in the tool handler.

    #[tokio::test]
    async fn test_brains_list_via_underscore_alias() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry.dispatch("brains_list", json!({}), &ctx).await;
        assert!(
            result.is_error.is_none(),
            "brains_list alias should not error"
        );
    }
}
