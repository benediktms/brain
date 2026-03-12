use std::future::Future;
use std::pin::Pin;

use serde::Serialize;
use serde_json::{Value, json};

use crate::config::{load_global_config, open_remote_task_store};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

pub(super) struct BrainsList;

#[derive(Serialize)]
struct BrainInfo {
    name: String,
    id: Option<String>,
    root: String,
    aliases: Vec<String>,
    extra_roots: Vec<String>,
    prefix: Option<String>,
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
            description: "List all registered brain projects from the global registry (~/.brain/config.toml). Returns name, ID, root path, aliases, extra_roots, and task prefix for each brain. Use this to discover available brains before cross-brain operations (federated search via memory.search_minimal with brains parameter, or cross-brain task creation).".into(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn call<'a>(
        &'a self,
        _params: Value,
        _ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let config = match load_global_config() {
                Ok(c) => c,
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to load global config: {err}"));
                }
            };

            let mut brains: Vec<BrainInfo> = config
                .brains
                .into_iter()
                .map(|(name, entry)| {
                    // Try to read the prefix from the brain's SQLite store.
                    let prefix = open_remote_task_store(&name, &entry)
                        .ok()
                        .and_then(|store| store.get_project_prefix().ok());

                    let extra_roots: Vec<String> = entry
                        .roots
                        .iter()
                        .skip(1)
                        .map(|p| p.display().to_string())
                        .collect();
                    BrainInfo {
                        name,
                        root: entry.primary_root().display().to_string(),
                        aliases: entry.aliases,
                        extra_roots,
                        id: entry.id,
                        prefix,
                    }
                })
                .collect();

            // Sort by name for deterministic output.
            brains.sort_by(|a, b| a.name.cmp(&b.name));

            let count = brains.len();
            json_response(&BrainsListResponse { brains, count })
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
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();

        assert!(parsed.get("brains").is_some());
        assert!(parsed.get("count").is_some());
        assert!(parsed["brains"].is_array());
    }

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
