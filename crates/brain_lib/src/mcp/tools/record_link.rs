use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::records::events::{LinkPayload, RecordEvent, RecordEventType};

use super::{McpTool, json_response};

// -- LinkAdd --

#[derive(Deserialize)]
struct LinkParams {
    record_id: String,
    task_id: Option<String>,
    chunk_id: Option<String>,
}

pub(super) struct RecordLinkAdd;

impl RecordLinkAdd {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: LinkParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if params.task_id.is_none() && params.chunk_id.is_none() {
            return ToolCallResult::error(
                "At least one of task_id or chunk_id must be provided".to_string(),
            );
        }

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
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: params.task_id.clone(),
                chunk_id: params.chunk_id.clone(),
            },
        );

        if let Err(e) = ctx.records.apply_and_append(&event) {
            return ToolCallResult::error(format!("Failed to add link: {e}"));
        }

        let compact_id = ctx
            .records
            .compact_record_id(&record_id)
            .unwrap_or_else(|_| record_id.clone());

        json_response(&json!({
            "record_id": compact_id,
            "task_id": params.task_id,
            "chunk_id": params.chunk_id,
            "action": "linked",
        }))
    }
}

impl McpTool for RecordLinkAdd {
    fn name(&self) -> &'static str {
        "records.link_add"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Add a link from a record to a task or note chunk. At least one of task_id or chunk_id must be provided. Idempotent — duplicate links are deduplicated by the projection.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to link from (full ID or unique prefix)"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Task ID to link to (optional if chunk_id is provided)"
                    },
                    "chunk_id": {
                        "type": "string",
                        "description": "Note chunk ID to link to (optional if task_id is provided)"
                    }
                },
                "required": ["record_id"]
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

// -- LinkRemove --

pub(super) struct RecordLinkRemove;

impl RecordLinkRemove {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: LinkParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if params.task_id.is_none() && params.chunk_id.is_none() {
            return ToolCallResult::error(
                "At least one of task_id or chunk_id must be provided".to_string(),
            );
        }

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
            RecordEventType::LinkRemoved,
            &LinkPayload {
                task_id: params.task_id.clone(),
                chunk_id: params.chunk_id.clone(),
            },
        );

        if let Err(e) = ctx.records.apply_and_append(&event) {
            return ToolCallResult::error(format!("Failed to remove link: {e}"));
        }

        let compact_id = ctx
            .records
            .compact_record_id(&record_id)
            .unwrap_or_else(|_| record_id.clone());

        json_response(&json!({
            "record_id": compact_id,
            "task_id": params.task_id,
            "chunk_id": params.chunk_id,
            "action": "unlinked",
        }))
    }
}

impl McpTool for RecordLinkRemove {
    fn name(&self) -> &'static str {
        "records.link_remove"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Remove a link from a record to a task or note chunk. At least one of task_id or chunk_id must be provided. Idempotent — removing a link that doesn't exist has no effect.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to unlink from (full ID or unique prefix)"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Task ID to unlink (optional if chunk_id is provided)"
                    },
                    "chunk_id": {
                        "type": "string",
                        "description": "Note chunk ID to unlink (optional if task_id is provided)"
                    }
                },
                "required": ["record_id"]
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
