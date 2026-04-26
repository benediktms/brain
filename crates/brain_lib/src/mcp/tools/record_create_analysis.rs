use std::future::Future;
use std::pin::Pin;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::l0_abstract::generate_l0_abstract;
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::pipeline::embed_poll::upsert_domain_lod_l0;
use crate::records::events::{ContentRefPayload, RecordCreatedPayload, RecordEvent, new_record_id};
use crate::records::objects::COMPRESSION_THRESHOLD;

use crate::uri::SynapseUri;

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

pub(super) struct RecordCreateAnalysis;

impl RecordCreateAnalysis {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: Params = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let records = if let Some(ref brain_param) = params.brain {
            let (bid, brain_name) = match ctx.resolve_brain_id(brain_param) {
                Ok(r) => r,
                Err(e) => return ToolCallResult::error(format!("Failed to resolve brain: {e}")),
            };
            match ctx
                .stores
                .is_brain_archived(&bid)
                .map_err(|e| e.to_string())
            {
                Ok(true) => {
                    return ToolCallResult::error(format!(
                        "Target brain '{brain_name}' is archived"
                    ));
                }
                Ok(false) => {}
                Err(e) => {
                    return ToolCallResult::error(format!("Failed to check archived status: {e}"));
                }
            }
            if bid == ctx.brain_id() {
                ctx.stores.records.clone()
            } else {
                match ctx.stores.with_brain_id(&bid, &brain_name) {
                    Ok(s) => s.records,
                    Err(e) => {
                        return ToolCallResult::error(format!("Failed to open remote brain: {e}"));
                    }
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
                    .unwrap_or("application/octet-stream".to_string());
                (bytes, mt)
            }
            (None, Some(t)) => {
                let mt = params
                    .media_type
                    .clone()
                    .unwrap_or("text/plain".to_string());
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

        let title_for_capsule = params.title.clone();
        let tags_for_capsule = params.tags.clone();

        let payload = RecordCreatedPayload {
            title: params.title,
            kind: "analysis".to_string(),
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

        let content = String::from_utf8_lossy(&raw_bytes);
        let tags_refs: Vec<&str> = tags_for_capsule.iter().map(|s| s.as_str()).collect();
        let abstract_text = generate_l0_abstract(&title_for_capsule, &content, &tags_refs);
        let record_file_id = format!("record:{record_id}");
        if let Err(e) = ctx
            .stores
            .upsert_record_chunk(&record_file_id, &abstract_text)
        {
            tracing::warn!(
                record_id = %record_id,
                error = %e,
                "record_create_analysis: failed to write L0 abstract to FTS"
            );
        }
        upsert_domain_lod_l0(
            &ctx.stores,
            &record_file_id,
            &abstract_text,
            ctx.brain_id(),
            "record",
        );

        let uri = SynapseUri::for_record(ctx.brain_name(), &record_id).to_string();

        let mut result = json!({
            "record_id": record_id,
            "uri": uri,
            "content_hash": content_ref.hash,
            "size": content_ref.size,
        });

        if let Some(ref brain_param) = params.brain
            && let Ok((bid, brain_name)) = ctx.resolve_brain_id(brain_param)
        {
            result["brain_name"] = json!(brain_name);
            result["brain_id"] = json!(bid);
        }

        json_response(&result)
    }
}

impl McpTool for RecordCreateAnalysis {
    fn name(&self) -> &'static str {
        "records.create_analysis"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description:
                "Create a new analysis record. Writes content to the object store with kind='analysis'."
                    .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Human-readable title for the analysis"
                    },
                    "data": {
                        "type": "string",
                        "description": "Base64-encoded analysis bytes. Provide either 'data' or 'text', not both."
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
                        "description": "Optional task ID this analysis is associated with"
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
