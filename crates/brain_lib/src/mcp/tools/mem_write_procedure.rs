use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::ports::ProcedureWriter;

use crate::uri::SynapseUri;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    title: String,
    steps: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default = "default_importance")]
    importance: f64,
}

fn default_importance() -> f64 {
    0.9
}

pub(super) struct MemWriteProcedure;

impl McpTool for MemWriteProcedure {
    fn name(&self) -> &'static str {
        "memory.write_procedure"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Record a reusable procedure (title + steps) to the knowledge base."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Procedure title"
                    },
                    "steps": {
                        "type": "string",
                        "description": "Procedure steps as markdown"
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Tags for categorization. Pass as a JSON array, e.g. [\"ci\", \"workflow\"]"
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance score (0.0 to 1.0). Default: 0.9",
                        "default": 0.9
                    }
                },
                "required": ["title", "steps"]
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

            let importance = params.importance.clamp(0.0, 1.0);

            let summary_id = match ctx.stores.db().store_procedure(
                &params.title,
                &params.steps,
                &params.tags,
                importance,
                ctx.brain_id(),
            ) {
                Ok(id) => id,
                Err(e) => {
                    error!(error = %e, "failed to store procedure");
                    return ToolCallResult::error(format!("Failed to store procedure: {e}"));
                }
            };

            let uri = SynapseUri::for_procedure(ctx.brain_name(), &summary_id).to_string();
            let response = json!({
                "status": "stored",
                "summary_id": summary_id,
                "uri": uri,
                "title": params.title,
                "tags": params.tags,
                "importance": params.importance
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
    async fn test_write_procedure() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "title": "Standard Deploy Procedure",
            "steps": "Step 1: Build.\nStep 2: Test.\nStep 3: Deploy.",
            "tags": ["deploy", "ci"],
            "importance": 0.9
        });

        let result = registry
            .dispatch("memory.write_procedure", params, &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["status"], "stored");
        assert!(parsed["summary_id"].is_string());
        assert_eq!(parsed["title"], "Standard Deploy Procedure");
    }
}
