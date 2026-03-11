use std::future::Future;
use std::pin::Pin;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    record_id: String,
}

pub(super) struct RecordFetchContent;

impl RecordFetchContent {
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

        let raw_bytes = match ctx.objects.read_auto(&record.content_hash) {
            Ok(b) => b,
            Err(e) => return ToolCallResult::error(format!("Failed to read content: {e}")),
        };

        let compact_id = ctx
            .records
            .compact_record_id(&record_id)
            .unwrap_or_else(|_| record_id.clone());

        // Detect text-like content by media_type
        let is_text = record
            .media_type
            .as_deref()
            .map(|mt| {
                mt.starts_with("text/")
                    || mt == "application/json"
                    || mt == "application/toml"
                    || mt == "application/yaml"
            })
            .unwrap_or(false);

        let result = if is_text {
            match std::str::from_utf8(&raw_bytes) {
                Ok(text) => json!({
                    "record_id": compact_id,
                    "title": record.title,
                    "kind": record.kind,
                    "content_hash": record.content_hash,
                    "size": record.content_size,
                    "media_type": record.media_type,
                    "encoding": "utf-8",
                    "text": text,
                }),
                Err(_) => {
                    // Not valid UTF-8 despite text media_type — fall back to base64
                    let data_b64 = BASE64.encode(&raw_bytes);
                    json!({
                        "record_id": compact_id,
                        "title": record.title,
                        "kind": record.kind,
                        "content_hash": record.content_hash,
                        "size": record.content_size,
                        "media_type": record.media_type,
                        "encoding": "base64",
                        "data": data_b64,
                    })
                }
            }
        } else {
            let data_b64 = BASE64.encode(&raw_bytes);
            json!({
                "record_id": compact_id,
                "title": record.title,
                "kind": record.kind,
                "content_hash": record.content_hash,
                "size": record.content_size,
                "media_type": record.media_type,
                "encoding": "base64",
                "data": data_b64,
            })
        };

        json_response(&result)
    }
}

impl McpTool for RecordFetchContent {
    fn name(&self) -> &'static str {
        "records.fetch_content"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Fetch the content of a record. For text content (media_type starting with 'text/' or 'application/json'), returns decoded UTF-8 text directly in the 'text' field. For binary content, returns base64-encoded data in the 'data' field. The 'encoding' field indicates how to interpret the content ('utf-8' or 'base64'). Includes title and kind metadata.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID (full ID or unique prefix)"
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
