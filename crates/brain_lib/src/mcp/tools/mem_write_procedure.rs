use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use brain_persistence::db::links::{EntityRef, EntityType};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use crate::uri::SynapseUri;

use super::links_add::{InlineLinkInput, apply_inline_links, inline_links_schema};
use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    title: String,
    steps: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default = "default_importance")]
    importance: f64,
    #[serde(default)]
    links: Vec<InlineLinkInput>,
}

fn default_importance() -> f64 {
    0.9
}

pub(super) struct MemWriteProcedure;

impl McpTool for MemWriteProcedure {
    fn name(&self) -> &'static str {
        "memory.write_procedure"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Record a reusable procedure (title, markdown steps) to memory. Returns `{summary_id, uri, ...}`. Optionally pass `links` to add edges in the entity graph from the new procedure (type PROCEDURE) to the EPISODE it was distilled from and any TASK/RECORD/PROCEDURE/EPISODE/CHUNK/NOTE entities — the procedure persists even if every link fails. When `links` is provided the response carries `links: {succeeded:[{to, edge_kind}], failed:[{to, edge_kind, error}], summary:{succeeded, failed}}`. Use `links_add` for any links discovered after the write."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Procedure title"
                    },
                    "steps": {
                        "type": "string",
                        "description": "Procedure steps as markdown"
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Tags for categorization. Pass as a JSON array, e.g. [\"ci\", \"workflow\"]"
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance score (0.0 to 1.0). Default: 0.9",
                        "default": 0.9
                    },
                    "links": inline_links_schema("Optional. After the procedure is stored, create polymorphic edges from it (as PROCEDURE) to the listed entities. Partial failures are reported per-link without aborting the write.")
                },
                "required": ["title", "steps"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let params: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let importance = params.importance.clamp(0.0, 1.0);

            let summary_id = match ctx.stores.store_procedure(
                &params.title,
                &params.steps,
                &params.tags,
                importance,
                ctx.brain_id(),
            ) {
                Ok(id) => id,
                Err(e) => {
                    error!(error = %e, "failed to store procedure");
                    return ToolCallResult::error(format!("Failed to store procedure: {e}"));
                }
            };

            let uri = SynapseUri::for_procedure(ctx.brain_name(), &summary_id).to_string();
            let mut response = json!({
                "status": "stored",
                "summary_id": summary_id,
                "uri": uri,
                "title": params.title,
                "tags": params.tags,
                "importance": params.importance
            });

            if !params.links.is_empty() {
                let from_ref = EntityRef::new(EntityType::Procedure, summary_id.clone())
                    .expect("summary_id is non-empty");
                let links_block = apply_inline_links(from_ref, params.links.clone(), ctx);
                response["links"] = links_block;
            }

            json_response(&response)
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;
    use super::{McpTool, MemWriteProcedure};

    #[tokio::test]
    async fn test_write_procedure() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "title": "Standard Deploy Procedure",
            "steps": "Step 1: Build.\nStep 2: Test.\nStep 3: Deploy.",
            "tags": ["deploy", "ci"],
            "importance": 0.9
        });

        let result = registry
            .dispatch("memory.write_procedure", params, &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["status"], "stored");
        assert!(parsed["summary_id"].is_string());
        assert_eq!(parsed["title"], "Standard Deploy Procedure");
    }

    #[tokio::test]
    async fn test_write_procedure_with_links_happy_path() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "title": "Deploy Checklist",
            "steps": "Step 1: Verify staging.\nStep 2: Tag release.",
            "links": [
                {
                    "to": { "type": "TASK", "id": "brn-task-abc123" },
                    "edge_kind": "covers"
                }
            ]
        });

        let result = registry
            .dispatch("memory.write_procedure", params, &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["status"], "stored");
        assert_eq!(parsed["links"]["summary"]["succeeded"], 1);
        assert_eq!(parsed["links"]["summary"]["failed"], 0);
    }

    #[tokio::test]
    async fn test_write_procedure_with_links_partial_failure() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "title": "Incident Response",
            "steps": "Step 1: Alert on-call.\nStep 2: Assess blast radius.",
            "links": [
                {
                    "to": { "type": "TASK", "id": "brn-task-def456" },
                    "edge_kind": "relates_to"
                },
                {
                    "to": { "type": "TASK", "id": "brn-task-ghi789" },
                    "edge_kind": "not_a_real_edge_kind"
                }
            ]
        });

        let result = registry
            .dispatch("memory.write_procedure", params, &ctx)
            .await;
        // Procedure must still be stored even with a partial link failure
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["status"], "stored");
        assert!(parsed["summary_id"].is_string());
        assert_eq!(parsed["links"]["summary"]["succeeded"], 1);
        assert_eq!(parsed["links"]["summary"]["failed"], 1);
    }

    #[tokio::test]
    async fn test_write_procedure_no_links_omits_block() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "title": "Simple Procedure",
            "steps": "Step 1: Do the thing."
        });

        let result = registry
            .dispatch("memory.write_procedure", params, &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["status"], "stored");
        assert!(
            parsed.get("links").is_none(),
            "links key must be absent when no links passed"
        );
    }

    #[tokio::test]
    async fn test_write_procedure_links_null_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({
            "title": "x", "steps": "y",
            "links": null
        });
        let result = registry
            .dispatch("memory.write_procedure", params, &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("Invalid parameters"), "got: {text}");
    }

    #[test]
    fn test_write_procedure_schema_stable() {
        let tool = MemWriteProcedure;
        let schema = tool.definition().input_schema;
        let expected = json!({
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Procedure title"
                },
                "steps": {
                    "type": "string",
                    "description": "Procedure steps as markdown"
                },
                "tags": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Tags for categorization. Pass as a JSON array, e.g. [\"ci\", \"workflow\"]"
                },
                "importance": {
                    "type": "number",
                    "description": "Importance score (0.0 to 1.0). Default: 0.9",
                    "default": 0.9
                },
                "links": {
                    "type": "array",
                    "description": "Optional. After the procedure is stored, create polymorphic edges from it (as PROCEDURE) to the listed entities. Partial failures are reported per-link without aborting the write.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "to": {
                                "type": "object",
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "enum": ["TASK", "RECORD", "EPISODE", "PROCEDURE", "CHUNK", "NOTE"],
                                        "description": "The entity type. TASK/RECORD/EPISODE/PROCEDURE are agent-writable; CHUNK and NOTE are read-only entities created by the file-watcher pipeline — only link to them when you have a specific chunk_id or note_id from prior retrieval."
                                    },
                                    "id": {
                                        "type": "string",
                                        "description": "The entity ID"
                                    }
                                },
                                "required": ["type", "id"]
                            },
                            "edge_kind": {
                                "type": "string",
                                "enum": ["parent_of", "blocks", "covers", "relates_to", "see_also", "supersedes", "contradicts", "continues"],
                                "description": "Default: relates_to. Semantics: parent_of (DAG-validated; hierarchical containment), blocks (DAG-validated; dependency), supersedes (DAG-validated; replacement), covers (this entity documents/explains the target), relates_to (default; generic association), see_also (cross-reference), contradicts (conflicts with target), continues (DAG-validated; episode-thread continuation — new episode continues the named predecessor). DAG-validated kinds reject cycles per-link without aborting the batch."
                            }
                        },
                        "required": ["to"]
                    }
                }
            },
            "required": ["title", "steps"]
        });
        assert_eq!(
            schema, expected,
            "memory.write_procedure schema changed — update golden or revert"
        );
    }
}
