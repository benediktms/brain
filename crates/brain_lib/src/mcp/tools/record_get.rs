use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    record_id: String,
}

pub(super) struct RecordGet;

impl RecordGet {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let record_id = match ctx.records.resolve_record_id(&params.record_id) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve record_id: {e}")),
        };

        let record = match ctx.records.get_record(&record_id) {
            Ok(Some(r)) => r,
            Ok(None) => return ToolCallResult::error(format!("Record not found: {record_id}")),
            Err(e) => return ToolCallResult::error(format!("Failed to get record: {e}")),
        };

        let tags = ctx.records.get_record_tags(&record_id).unwrap_or_default();
        let links = ctx.records.get_record_links(&record_id).unwrap_or_default();

        let compact_id = ctx
            .records
            .compact_record_id(&record_id)
            .unwrap_or_else(|_| record_id.clone());

        let links_json: Vec<Value> = links
            .iter()
            .map(|l| {
                json!({
                    "task_id": l.task_id,
                    "chunk_id": l.chunk_id,
                    "created_at": l.created_at,
                })
            })
            .collect();

        let result = json!({
            "record_id": compact_id,
            "title": record.title,
            "kind": record.kind,
            "status": record.status,
            "description": record.description,
            "content_hash": record.content_hash,
            "content_size": record.content_size,
            "media_type": record.media_type,
            "task_id": record.task_id,
            "actor": record.actor,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "tags": tags,
            "links": links_json,
        });

        json_response(&result)
    }
}

impl McpTool for RecordGet {
    fn name(&self) -> &'static str {
        "records.get"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get a record by ID with full details including tags and links. Supports prefix resolution.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to retrieve (full ID or unique prefix)"
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
