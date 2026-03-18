use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::records::events::{RecordEvent, RecordEventType, TagPayload};

use super::{McpTool, json_response};

// -- TagAdd --

#[derive(Deserialize)]
struct TagParams {
    record_id: String,
    tag: String,
}

pub(super) struct RecordTagAdd;

impl RecordTagAdd {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: TagParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let record_id = match ctx.records.resolve_record_id(&params.record_id) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve record_id: {e}")),
        };

        match ctx.records.get_record(&record_id) {
            Ok(Some(_)) => {}
            Ok(None) => return ToolCallResult::error(format!("Record not found: {record_id}")),
            Err(e) => return ToolCallResult::error(format!("Failed to get record: {e}")),
        }

        let event = RecordEvent::new(
            &record_id,
            "mcp",
            RecordEventType::TagAdded,
            &TagPayload {
                tag: params.tag.clone(),
            },
        );

        if let Err(e) = ctx.records.apply_event(&event) {
            return ToolCallResult::error(format!("Failed to add tag: {e}"));
        }

        let compact_id = ctx
            .records
            .compact_record_id(&record_id)
            .unwrap_or_else(|_| record_id.clone());

        json_response(&json!({
            "record_id": compact_id,
            "tag": params.tag,
            "action": "added",
        }))
    }
}

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
        Box::pin(std::future::ready(self.execute(params, ctx)))
    }
}

// -- TagRemove --

pub(super) struct RecordTagRemove;

impl RecordTagRemove {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: TagParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let record_id = match ctx.records.resolve_record_id(&params.record_id) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve record_id: {e}")),
        };

        match ctx.records.get_record(&record_id) {
            Ok(Some(_)) => {}
            Ok(None) => return ToolCallResult::error(format!("Record not found: {record_id}")),
            Err(e) => return ToolCallResult::error(format!("Failed to get record: {e}")),
        }

        let event = RecordEvent::new(
            &record_id,
            "mcp",
            RecordEventType::TagRemoved,
            &TagPayload {
                tag: params.tag.clone(),
            },
        );

        if let Err(e) = ctx.records.apply_event(&event) {
            return ToolCallResult::error(format!("Failed to remove tag: {e}"));
        }

        let compact_id = ctx
            .records
            .compact_record_id(&record_id)
            .unwrap_or_else(|_| record_id.clone());

        json_response(&json!({
            "record_id": compact_id,
            "tag": params.tag,
            "action": "removed",
        }))
    }
}

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
        Box::pin(std::future::ready(self.execute(params, ctx)))
    }
}
