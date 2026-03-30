use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::consolidation::{consolidate_episodes, enqueue_cluster_summarization};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

fn default_limit() -> usize {
    50
}

fn default_auto_summarize() -> bool {
    false
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
    #[serde(default = "default_auto_summarize")]
    auto_summarize: bool,
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
                    },
                    "auto_summarize": {
                        "type": "boolean",
                        "description": "Enqueue async LLM synthesis jobs for each cluster. Default: false",
                        "default": false
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

            let effective_brain_id = match params.brain_id.as_deref() {
                Some(brain_id) => brain_id,
                None => ctx.brain_id(),
            };

            let limit = params.limit.min(500);
            let episodes = match ctx.stores.list_episodes(limit, effective_brain_id) {
                Ok(rows) => rows,
                Err(e) => return ToolCallResult::error(format!("Failed to list episodes: {e}")),
            };

            let result = consolidate_episodes(episodes, params.gap_seconds);
            let jobs_enqueued = if params.auto_summarize {
                match enqueue_cluster_summarization(
                    ctx.stores.db(),
                    &result.clusters,
                    effective_brain_id,
                ) {
                    Ok(count) => count,
                    Err(e) => {
                        return ToolCallResult::error(format!(
                            "Failed to enqueue consolidation jobs: {e}"
                        ));
                    }
                }
            } else {
                0
            };

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
                "jobs_enqueued": jobs_enqueued,
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
        let result = registry
            .dispatch("memory.consolidate", json!({}), &ctx)
            .await;
        assert!(
            result.is_error.is_none(),
            "unexpected error: {:?}",
            result.content
        );
        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["cluster_count"], 0);
        assert_eq!(parsed["jobs_enqueued"], 0);
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
    async fn test_consolidate_auto_summarize_empty_enqueues_no_jobs() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.consolidate", json!({"auto_summarize": true}), &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["jobs_enqueued"], 0);
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
