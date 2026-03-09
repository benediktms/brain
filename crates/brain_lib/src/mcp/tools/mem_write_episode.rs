use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::db::summaries::{Episode, store_episode};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::McpTool;

#[derive(Deserialize)]
struct Params {
    goal: String,
    actions: String,
    outcome: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default = "default_importance")]
    importance: f64,
}

fn default_importance() -> f64 {
    1.0
}

pub(super) struct MemWriteEpisode;

impl MemWriteEpisode {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let episode = Episode {
            goal: params.goal.clone(),
            actions: params.actions,
            outcome: params.outcome,
            tags: params.tags.clone(),
            importance: params.importance,
        };

        match ctx.db.with_write_conn(|conn| store_episode(conn, &episode)) {
            Ok(summary_id) => {
                let response = json!({
                    "status": "stored",
                    "summary_id": summary_id,
                    "goal": params.goal,
                    "tags": params.tags,
                    "importance": params.importance
                });
                ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
            }
            Err(e) => {
                error!(error = %e, "failed to store episode");
                ToolCallResult::error(format!("Failed to store episode: {e}"))
            }
        }
    }
}

impl McpTool for MemWriteEpisode {
    fn name(&self) -> &'static str {
        "memory.write_episode"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Record an episode (goal, actions, outcome) to the knowledge base.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "goal": {
                        "type": "string",
                        "description": "What was the goal"
                    },
                    "actions": {
                        "type": "string",
                        "description": "What actions were taken"
                    },
                    "outcome": {
                        "type": "string",
                        "description": "What was the outcome"
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Tags for categorization. Pass as a JSON array, e.g. [\"debugging\", \"auth\"]"
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance score (0.0 to 1.0). Default: 1.0",
                        "default": 1.0
                    }
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
        Box::pin(std::future::ready(self.execute(params, ctx)))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_write_episode() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "goal": "Fix the bug",
            "actions": "Debugged and patched",
            "outcome": "Bug fixed",
            "tags": ["debugging"],
            "importance": 0.8
        });

        let result = registry
            .dispatch("memory.write_episode", params, &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["status"], "stored");
        assert!(parsed["summary_id"].is_string());
    }
}
