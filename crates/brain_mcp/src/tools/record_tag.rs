//! `records.tag_add` and `records.tag_remove` MCP tools — thin wrappers
//! over `DaemonClient::records_tag_add` / `records_tag_remove`.
//!
//! Both tools accept `{record_id, tag}`. Response shape:
//! - `tag_add`:    `{record_id, tag, action: "added"}`
//! - `tag_remove`: `{record_id, tag, action: "removed"}`
//!
//! Byte-identical to the legacy tool responses.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

// ── Shared params ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct TagParams {
    record_id: String,
    tag: String,
}

// ── RecordTagAdd ───────────────────────────────────────────────────────────

pub(super) struct RecordTagAdd;

impl McpTool for RecordTagAdd {
    fn name(&self) -> &'static str {
        "records.tag_add"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Add a tag to a record (artifact or snapshot). Idempotent — adding a tag that already exists has no effect on the projection.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to tag (full ID or unique prefix)"
                    },
                    "tag": {
                        "type": "string",
                        "description": "Tag to add"
                    }
                },
                "required": ["record_id", "tag"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let parsed: TagParams = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let tag = match ctx
                .with_client(|c| c.records_tag_add(parsed.record_id.clone(), parsed.tag.clone()))
                .await
            {
                Ok(t) => t,
                Err(err) => return ToolCallResult::error(format!("Failed to add tag: {err}")),
            };

            json_response(&json!({
                "record_id": parsed.record_id,
                "tag": tag,
                "action": "added",
            }))
        })
    }
}

// ── RecordTagRemove ────────────────────────────────────────────────────────

pub(super) struct RecordTagRemove;

impl McpTool for RecordTagRemove {
    fn name(&self) -> &'static str {
        "records.tag_remove"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Remove a tag from a record (artifact or snapshot). Idempotent — removing a tag that doesn't exist has no effect on the projection.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to untag (full ID or unique prefix)"
                    },
                    "tag": {
                        "type": "string",
                        "description": "Tag to remove"
                    }
                },
                "required": ["record_id", "tag"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let parsed: TagParams = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            match ctx
                .with_client(|c| c.records_tag_remove(parsed.record_id.clone(), parsed.tag.clone()))
                .await
            {
                Ok(_removed) => {}
                Err(err) => return ToolCallResult::error(format!("Failed to remove tag: {err}")),
            }

            json_response(&json!({
                "record_id": parsed.record_id,
                "tag": parsed.tag,
                "action": "removed",
            }))
        })
    }
}
