#![allow(clippy::items_after_test_module)]

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
use crate::records::RecordKind;
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

pub(super) struct RecordCreatePlan;

impl RecordCreatePlan {
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
            kind: "plan".to_string(),
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
        let policy = RecordKind::from("plan").policy();
        if policy.searchable
            && let Err(e) = ctx
                .stores
                .upsert_record_chunk(&record_file_id, &abstract_text)
        {
            tracing::warn!(
                record_id = %record_id,
                error = %e,
                "record_create_plan: failed to write L0 abstract to FTS"
            );
        }
        if policy.embed {
            upsert_domain_lod_l0(
                &ctx.stores,
                &record_file_id,
                &abstract_text,
                ctx.brain_id(),
                "record",
            );
        }

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

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_create_plan_writes_fts_and_lod() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry
            .dispatch(
                "records.create_plan",
                json!({
                    "title": "Plan Title",
                    "text": "Plan body for FTS and LOD"
                }),
                &ctx,
            )
            .await;

        assert_ne!(
            result.is_error,
            Some(true),
            "tool returned error: {result:?}"
        );

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("result should be valid JSON");
        let record_id = parsed["record_id"]
            .as_str()
            .expect("record_id should be present");
        let record_file_id = format!("record:{record_id}");

        let chunk_count: i64 = ctx
            .stores
            .db_for_tests()
            .with_read_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM chunks WHERE file_id = ?1",
                    [&record_file_id],
                    |row| row.get(0),
                )
                .map_err(|e| brain_persistence::error::BrainCoreError::Database(e.to_string()))
            })
            .expect("chunk query should succeed");
        assert_eq!(chunk_count, 1, "plan record should write one FTS chunk");

        let lod_uri = format!("synapse://{}/record/{}:0", ctx.brain_id(), record_file_id);
        let lod = ctx
            .stores
            .db_for_tests()
            .get_lod_chunk(&lod_uri, "L0")
            .expect("LOD query should succeed");
        assert!(lod.is_some(), "plan record should write L0 LOD chunk");
    }
}

impl McpTool for RecordCreatePlan {
    fn name(&self) -> &'static str {
        "records.create_plan"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description:
                "Create a new plan record. Writes content to the object store with kind='plan'."
                    .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Human-readable title for the plan"
                    },
                    "data": {
                        "type": "string",
                        "description": "Base64-encoded plan bytes. Provide either 'data' or 'text', not both."
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
                        "description": "Optional task ID this plan is associated with"
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
