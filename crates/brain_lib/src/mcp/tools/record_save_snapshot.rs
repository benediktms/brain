use std::future::Future;
use std::pin::Pin;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::records::events::{ContentRefPayload, RecordCreatedPayload, RecordEvent, new_record_id};
use crate::records::objects::COMPRESSION_THRESHOLD;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    title: String,
    data: Option<String>,
    text: Option<String>,
    description: Option<String>,
    task_id: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    media_type: Option<String>,
    brain: Option<String>,
}

pub(super) struct RecordSaveSnapshot;

impl RecordSaveSnapshot {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        // Resolve target brain (local or remote)
        let records = if let Some(ref brain_param) = params.brain {
            let (brain_name, bid) = match ctx.resolve_brain_id(brain_param) {
                Ok(r) => r,
                Err(e) => return ToolCallResult::error(format!("Failed to resolve brain: {e}")),
            };
            // Guard: reject writes to archived brains
            match super::is_brain_archived(ctx.db(), &bid) {
                Ok(true) => {
                    return ToolCallResult::error(format!(
                        "Target brain '{brain_name}' is archived"
                    ));
                }
                Ok(false) => {}
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Failed to check archived status: {e}"
                    ));
                }
            }
            // Short-circuit if same brain
            if bid == ctx.brain_id() {
                ctx.stores.records.clone()
            } else {
                match crate::records::RecordStore::with_brain_id(
                    ctx.db().clone(), &bid, &brain_name,
                ) {
                    Ok(s) => s,
                    Err(e) => return ToolCallResult::error(format!("Failed to open remote brain: {e}")),
                }
            }
        } else {
            ctx.stores.records.clone()
        };

        let (raw_bytes, media_type) = match (&params.data, &params.text) {
            (Some(_), Some(_)) => {
                return ToolCallResult::error(
                    "Provide either 'data' (base64) or 'text' (plain), not both".to_string(),
                );
            }
            (Some(b64), None) => {
                let bytes = match BASE64.decode(b64) {
                    Ok(b) => b,
                    Err(e) => return ToolCallResult::error(format!("Invalid base64 data: {e}")),
                };
                let mt = params
                    .media_type
                    .clone()
                    .unwrap_or_else(|| "application/octet-stream".to_string());
                (bytes, mt)
            }
            (None, Some(t)) => {
                let mt = params
                    .media_type
                    .clone()
                    .unwrap_or_else(|| "text/plain".to_string());
                (t.as_bytes().to_vec(), mt)
            }
            (None, None) => {
                return ToolCallResult::error(
                    "Either 'data' (base64) or 'text' (plain) is required".to_string(),
                );
            }
        };

        let (content_ref, encoding, original_size) = match ctx.stores.objects.write_compressed(
            &raw_bytes,
            Some(media_type.clone()),
            COMPRESSION_THRESHOLD,
        ) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Failed to write object: {e}")),
        };

        let prefix = match records.get_project_prefix() {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Failed to get project prefix: {e}")),
        };
        let record_id = new_record_id(&prefix);

        let payload = RecordCreatedPayload {
            title: params.title,
            kind: "snapshot".to_string(),
            content_ref: ContentRefPayload::compressed(
                content_ref.hash.clone(),
                content_ref.size,
                Some(media_type),
                encoding,
                original_size,
            ),
            description: params.description,
            task_id: params.task_id,
            tags: params.tags,
            scope_type: None,
            scope_id: None,
            retention_class: None,
            producer: None,
        };

        let event = RecordEvent::from_payload(&record_id, "mcp", payload);

        if let Err(e) = records.apply_event(&event) {
            return ToolCallResult::error(format!("Failed to save record: {e}"));
        }

        let mut result = json!({
            "record_id": record_id,
            "content_hash": content_ref.hash,
            "size": content_ref.size,
        });

        // If remote brain, include brain info in response
        if let Some(ref brain_param) = params.brain {
            if let Ok((brain_name, bid)) = ctx.resolve_brain_id(brain_param) {
                result["brain_name"] = json!(brain_name);
                result["brain_id"] = json!(bid);
            }
        }

        json_response(&result)
    }
}

impl McpTool for RecordSaveSnapshot {
    fn name(&self) -> &'static str {
        "records.save_snapshot"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description:
                "Save a new snapshot record. Writes content to the object store with kind='snapshot'."
                    .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Human-readable title for the snapshot"
                    },
                    "data": {
                        "type": "string",
                        "description": "Base64-encoded snapshot bytes. Provide either 'data' or 'text', not both."
                    },
                    "text": {
                        "type": "string",
                        "description": "Plain-text content (server encodes internally). Provide either 'text' or 'data', not both."
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional description"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Optional task ID this snapshot is associated with"
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional list of tags"
                    },
                    "media_type": {
                        "type": "string",
                        "description": "MIME type hint. Defaults to 'text/plain' for text, 'application/octet-stream' for data."
                    },
                    "brain": {
                        "type": "string",
                        "description": "Target brain name or ID. Writes to current brain if omitted."
                    }
                },
                "required": ["title"]
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
