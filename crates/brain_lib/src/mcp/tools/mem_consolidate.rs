use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::consolidation::consolidate_episodes;
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::ports::EpisodeReader;

use super::{McpTool, json_response};

fn default_limit() -> usize {
    50
}

#[derive(Deserialize)]
struct Params {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    brain_id: Option<String>,
    /// Gap in seconds between episodes that triggers a cluster boundary.
    /// Default: 3600 (1 hour).
    #[serde(default = "default_gap_seconds")]
    gap_seconds: i64,
}

fn default_gap_seconds() -> i64 {
    3600
}

pub(super) struct MemConsolidate;

impl McpTool for MemConsolidate {
    fn name(&self) -> &'static str {
        "memory.consolidate"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: concat!(
                "Group recent episodes by temporal proximity into consolidation clusters.\n\n",
                "Returns clusters of temporally proximate episodes with suggested titles ",
                "and summaries. Clusters are ordered newest-first. Use the output to decide ",
                "which episodes to synthesize into a reflection via `memory.reflect`."
            )
            .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of recent episodes to consider. Default: 50",
                        "default": 50
                    },
                    "brain_id": {
                        "type": "string",
                        "description": "Brain ID to scope episodes to. Empty or omitted = current brain."
                    },
                    "gap_seconds": {
                        "type": "integer",
                        "description": "Gap in seconds that separates two clusters. Default: 3600 (1 hour)",
                        "default": 3600
                    }
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
            let params: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let effective_brain_id = params
                .brain_id
                .as_deref()
                .unwrap_or_else(|| ctx.brain_id());

            let episodes = ctx
                .db()
                .list_episodes(params.limit, effective_brain_id)
                .unwrap_or_default();

            let result = consolidate_episodes(episodes, params.gap_seconds);

            let clusters_json: Vec<Value> = result
                .clusters
                .iter()
                .map(|c| {
                    json!({
                        "episode_ids": c.episode_ids,
                        "episode_count": c.episodes.len(),
                        "suggested_title": c.suggested_title,
                        "summary": c.summary,
                        "oldest_ts": c.episodes.iter().map(|e| e.created_at).min(),
                        "newest_ts": c.episodes.iter().map(|e| e.created_at).max(),
                    })
                })
                .collect();

            let response = json!({
                "cluster_count": clusters_json.len(),
                "clusters": clusters_json,
            });

            json_response(&response)
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_consolidate_empty_returns_ok() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry.dispatch("memory.consolidate", json!({}), &ctx).await;
        assert!(result.is_error.is_none(), "unexpected error: {:?}", result.content);
        let text = &result.content[0].text;
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["cluster_count"], 0);
    }

    #[tokio::test]
    async fn test_consolidate_with_limit_param() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.consolidate", json!({"limit": 10}), &ctx)
            .await;
        assert!(result.is_error.is_none());
    }

    #[tokio::test]
    async fn test_consolidate_invalid_params_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        // limit must be an integer — passing a string triggers a deserialization error.
        let result = registry
            .dispatch("memory.consolidate", json!({"limit": "not-a-number"}), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
    }
}
