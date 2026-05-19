//! `tasks.labels_summary` MCP tool — thin wrapper over
//! `DaemonClient::tasks_labels_summary`.
//!
//! Returns all unique labels with counts and associated task IDs (compact
//! short prefixes). The daemon resolves canonical IDs to compact form at
//! the boundary. The `WireTaskLabelSummary` wire type serializes directly
//! to the legacy `{label, count, task_ids}` shape, so it is wrapped in
//! the legacy `{labels: [...]}` envelope.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct TaskLabelsSummary;

impl McpTool for TaskLabelsSummary {
    fn name(&self) -> &'static str {
        "tasks.labels_summary"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get all unique labels with counts and associated task IDs (short prefixes). Returns labels sorted by count descending. Use for label discovery and taxonomy overview.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn call<'a>(
        &'a self,
        _params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            match ctx.with_client(|c| c.tasks_labels_summary()).await {
                Ok(labels) => {
                    // WireTaskLabelSummary serializes as {label, count, task_ids}
                    // which matches the legacy shape byte-for-byte.
                    let response = json!({ "labels": labels });
                    json_response(&response)
                }
                Err(e) => ToolCallResult::error(format!("Failed to get labels summary: {e}")),
            }
        })
    }
}
