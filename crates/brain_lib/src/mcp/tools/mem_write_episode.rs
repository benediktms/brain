use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use brain_persistence::db::links::{EntityRef, EntityType};
use brain_persistence::db::summaries::Episode;

use crate::uri::SynapseUri;

use super::links_add::{InlineLinkInput, apply_inline_links, inline_links_schema};
use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    goal: String,
    actions: String,
    outcome: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default = "default_importance")]
    importance: f64,
    #[serde(default)]
    links: Vec<InlineLinkInput>,
}

fn default_importance() -> f64 {
    1.0
}

pub(super) struct MemWriteEpisode;

impl McpTool for MemWriteEpisode {
    fn name(&self) -> &'static str {
        "memory.write_episode"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Record an episode (goal, actions, outcome) to memory. Returns `{summary_id, uri, ...}`. Optionally pass `links` to add edges in the entity graph from the new episode (type EPISODE) to existing TASK/RECORD/PROCEDURE/EPISODE/CHUNK/NOTE entities in one round-trip — the episode persists even if every link fails. When `links` is provided the response carries `links: {succeeded:[{to, edge_kind}], failed:[{to, edge_kind, error}], summary:{succeeded, failed}}`. Use `links_add` for any links discovered after the write.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "goal": {
                        "type": "string",
                        "description": "What was the goal"
                    },
                    "actions": {
                        "type": "string",
                        "description": "What actions were taken"
                    },
                    "outcome": {
                        "type": "string",
                        "description": "What was the outcome"
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Tags for categorization. Pass as a JSON array, e.g. [\"debugging\", \"auth\"]"
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance score (0.0 to 1.0). Default: 1.0",
                        "default": 1.0
                    },
                    "links": inline_links_schema("Optional. After the episode is stored, create polymorphic edges from it (as EPISODE) to the listed entities. Partial failures are reported per-link without aborting the write.")
                },
                "required": ["goal", "actions", "outcome"]
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

            let episode = Episode {
                brain_id: ctx.brain_id().to_string(),
                goal: params.goal.clone(),
                actions: params.actions,
                outcome: params.outcome,
                tags: params.tags.clone(),
                importance: params.importance,
            };

            let summary_id = match ctx.stores.store_episode(&episode) {
                Ok(id) => id,
                Err(e) => {
                    error!(error = %e, "failed to store episode");
                    return ToolCallResult::error(format!("Failed to store episode: {e}"));
                }
            };

            let uri = SynapseUri::for_episode(ctx.brain_name(), &summary_id).to_string();
            let mut response = json!({
                "status": "stored",
                "summary_id": summary_id,
                "uri": uri,
                "goal": params.goal,
                "tags": params.tags,
                "importance": params.importance
            });

            if !params.links.is_empty() {
                let from_ref = EntityRef::new(EntityType::Episode, summary_id.clone())
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
    use super::{McpTool, MemWriteEpisode};

    #[tokio::test]
    async fn test_write_episode() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "goal": "Fix the bug",
            "actions": "Debugged and patched",
            "outcome": "Bug fixed",
            "tags": ["debugging"],
            "importance": 0.8
        });

        let result = registry
            .dispatch("memory.write_episode", params, &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["status"], "stored");
        assert!(parsed["summary_id"].is_string());
    }

    #[tokio::test]
    async fn test_write_episode_with_links_happy_path() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "goal": "Integrate the link parameter",
            "actions": "Added links field to Params and handler",
            "outcome": "Episode stored with link",
            "links": [
                {
                    "to": { "type": "TASK", "id": "TST-01ABCDEFGHIJKLMNO" }
                }
            ]
        });

        let result = registry
            .dispatch("memory.write_episode", params, &ctx)
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
    async fn test_write_episode_with_links_partial_failure() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "goal": "Test partial link failure",
            "actions": "Passed one valid link and one with bogus edge_kind",
            "outcome": "Episode stored; one link succeeded, one failed",
            "links": [
                {
                    "to": { "type": "TASK", "id": "TST-01VALIDTASKIDHERE1" }
                },
                {
                    "to": { "type": "TASK", "id": "TST-01VALIDTASKIDHERE2" },
                    "edge_kind": "frobnicate"
                }
            ]
        });

        let result = registry
            .dispatch("memory.write_episode", params, &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["status"], "stored");
        assert_eq!(parsed["links"]["summary"]["succeeded"], 1);
        assert_eq!(parsed["links"]["summary"]["failed"], 1);
    }

    #[tokio::test]
    async fn test_write_episode_no_links_omits_block() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let params = json!({
            "goal": "No links provided",
            "actions": "Called without links field",
            "outcome": "Episode stored without links block"
        });

        let result = registry
            .dispatch("memory.write_episode", params, &ctx)
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["status"], "stored");
        assert!(parsed.get("links").is_none());
    }

    #[tokio::test]
    async fn test_write_episode_links_null_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({
            "goal": "x", "actions": "y", "outcome": "z",
            "links": null
        });
        let result = registry
            .dispatch("memory.write_episode", params, &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("Invalid parameters"), "got: {text}");
    }

    #[tokio::test]
    async fn test_write_episode_inline_link_round_trips_via_links_for_entity() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let task_id = "TST-01ROUNDTRIPTASK001";
        let params = json!({
            "goal": "Round-trip test",
            "actions": "Write episode with inline TASK link",
            "outcome": "Episode stored and link visible via links_for_entity",
            "links": [{ "to": { "type": "TASK", "id": task_id } }]
        });
        let result = registry
            .dispatch("memory.write_episode", params, &ctx)
            .await;
        assert!(result.is_error.is_none(), "write failed: {:?}", result);

        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        let summary_id = parsed["summary_id"].as_str().unwrap().to_string();

        let lookup = registry
            .dispatch(
                "links.for_entity",
                json!({ "entity": { "type": "TASK", "id": task_id }, "direction": "in" }),
                &ctx,
            )
            .await;
        assert!(lookup.is_error.is_none(), "lookup failed: {:?}", lookup);

        let lookup_parsed: serde_json::Value =
            serde_json::from_str(&lookup.content[0].text).unwrap();
        let incoming = lookup_parsed["incoming"]
            .as_array()
            .expect("incoming array");
        assert_eq!(incoming.len(), 1, "expected one incoming edge");
        assert_eq!(incoming[0]["from"]["type"], "EPISODE");
        assert_eq!(incoming[0]["from"]["id"], summary_id);
    }

    #[test]
    fn test_write_episode_schema_stable() {
        let tool = MemWriteEpisode;
        let schema = tool.definition().input_schema;
        let expected = json!({
            "type": "object",
            "properties": {
                "goal": {
                    "type": "string",
                    "description": "What was the goal"
                },
                "actions": {
                    "type": "string",
                    "description": "What actions were taken"
                },
                "outcome": {
                    "type": "string",
                    "description": "What was the outcome"
                },
                "tags": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Tags for categorization. Pass as a JSON array, e.g. [\"debugging\", \"auth\"]"
                },
                "importance": {
                    "type": "number",
                    "description": "Importance score (0.0 to 1.0). Default: 1.0",
                    "default": 1.0
                },
                "links": {
                    "type": "array",
                    "description": "Optional. After the episode is stored, create polymorphic edges from it (as EPISODE) to the listed entities. Partial failures are reported per-link without aborting the write.",
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
                                "enum": ["parent_of", "blocks", "covers", "relates_to", "see_also", "supersedes", "contradicts"],
                                "description": "Default: relates_to. Semantics: parent_of (DAG-validated; hierarchical containment), blocks (DAG-validated; dependency), supersedes (DAG-validated; replacement), covers (this entity documents/explains the target), relates_to (default; generic association), see_also (cross-reference), contradicts (conflicts with target). DAG-validated kinds reject cycles per-link without aborting the batch."
                            }
                        },
                        "required": ["to"]
                    }
                }
            },
            "required": ["goal", "actions", "outcome"]
        });
        assert_eq!(
            schema, expected,
            "memory.write_episode schema changed — update golden or revert"
        );
    }
}
