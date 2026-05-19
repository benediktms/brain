//! `tags.recluster` MCP tool — thin wrapper over
//! `DaemonClient::tags_recluster`.
//!
//! Wraps the per-brain reclustering job. The wire variant carries an
//! opaque `params_json` body so the wire surface does not need to
//! mirror the MCP tool's input schema; the daemon parses it and runs
//! the cluster pass over the brain's raw tags using its embedder.
//! Returns the report serialised as JSON; the tool echoes that string
//! through `ToolCallResult::text` to preserve byte-shape.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::TagsReclusterParams;

use super::McpTool;
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

#[derive(Deserialize, Default)]
struct Params {
    threshold: Option<f32>,
}

pub(super) struct TagsRecluster;

impl McpTool for TagsRecluster {
    fn name(&self) -> &'static str {
        "tags.recluster"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Run synonym clustering over the calling brain's raw tags. \
                Folds raw tags into canonical clusters via the embedder + cosine threshold, \
                writes the result to tag_aliases, and returns a ReclusterReport. \
                Mutates tag_aliases — the only tags.* tool that writes."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "threshold": {
                        "type": "number",
                        "description": "Cosine similarity threshold for cluster edges. Default: 0.85.",
                        "default": 0.85,
                        "minimum": 0.0,
                        "maximum": 1.0
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
            let parsed: Params = match serde_json::from_value(params.clone()) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            // Pre-validate threshold so the error message matches the
            // legacy MCP wording verbatim. The daemon also validates,
            // but its error path returns a different envelope.
            if let Some(threshold) = parsed.threshold
                && !(0.0..=1.0).contains(&threshold)
            {
                return ToolCallResult::error(format!(
                    "threshold must be between 0.0 and 1.0 (got {threshold}); \
                     values outside this range produce all-singleton clusters \
                     and write a misleading 'successful' run row"
                ));
            }

            let body = json!({ "threshold": parsed.threshold });
            let wire_params = TagsReclusterParams { params_json: body };

            match ctx.with_client(|c| c.tags_recluster(wire_params)).await {
                Ok(report_json) => ToolCallResult::text(report_json),
                Err(e) => ToolCallResult::error(format!("recluster failed: {e}")),
            }
        })
    }
}
