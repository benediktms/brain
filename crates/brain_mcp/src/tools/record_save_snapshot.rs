//! `records.save_snapshot` MCP tool — thin wrapper over
//! `DaemonClient::snapshots_create`.
//!
//! Accepts `title` plus either `text` (plain UTF-8) or `data` (base64).
//! Response: `{record_id, uri, content_hash, size}` — byte-identical to
//! the legacy shape; `brain_name`/`brain_id` included when `brain` param
//! is set.

use std::future::Future;
use std::pin::Pin;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use serde_json::{Value, json};

use brain_rpc::RecordsCreateParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct RecordSaveSnapshot;

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

impl McpTool for RecordSaveSnapshot {
    fn name(&self) -> &'static str {
        "records.save_snapshot"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Save a new snapshot record. Writes content to the object store with kind='snapshot'.".into(),
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
        Box::pin(async move {
            let parsed: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let (body, media_type) = match (&parsed.data, &parsed.text) {
                (Some(_), Some(_)) => {
                    return ToolCallResult::error(
                        "Provide either 'data' (base64) or 'text' (plain), not both".to_string(),
                    );
                }
                (Some(b64), None) => {
                    let bytes = match BASE64.decode(b64) {
                        Ok(b) => b,
                        Err(e) => {
                            return ToolCallResult::error(format!("Invalid base64 data: {e}"));
                        }
                    };
                    let mt = parsed
                        .media_type
                        .clone()
                        .unwrap_or_else(|| "application/octet-stream".to_string());
                    (bytes, mt)
                }
                (None, Some(t)) => {
                    let mt = parsed
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

            let wire_params = RecordsCreateParams {
                title: parsed.title,
                description: parsed.description,
                body,
                media_type: Some(media_type),
                task_id: parsed.task_id,
                tags: parsed.tags,
                brain: parsed.brain.clone(),
            };

            let (record, content_hash, size) =
                match ctx.with_client(|c| c.snapshots_create(wire_params)).await {
                    Ok(r) => r,
                    Err(err) => {
                        return ToolCallResult::error(format!("Failed to save record: {err}"));
                    }
                };

            let mut result = json!({
                "record_id": record.record_id,
                "uri": format!("synapse://{}/record/{}", record.brain_id, record.record_id),
                "content_hash": content_hash,
                "size": size,
            });

            if let Some(brain_param) = parsed.brain {
                result["brain_name"] = json!(brain_param);
                result["brain_id"] = json!(record.brain_id);
            }

            json_response(&result)
        })
    }
}
