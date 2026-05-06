use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::error;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use brain_persistence::db::links::{
    EdgeKind, EntityRef, EntityType, edge_kind_str, entity_type_str,
};
use brain_persistence::db::summaries::Episode;

use crate::uri::SynapseUri;

use super::links_add::{EntityRefInput, InlineLinkInput, apply_inline_links, inline_links_schema};
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
    #[serde(default)]
    continues: Option<String>,
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
            description: "Record an episode (goal, actions, outcome) to memory. Returns `{summary_id, uri, ...}`. Optionally pass `continues` (a prior episode's `summary_id`) to extend a thread — equivalent to a `links` entry of `{to: {type: EPISODE, id: <prev>}, edge_kind: continues}`, but ergonomic for the common case. Pass `links` to add edges from the new episode (type EPISODE) to existing TASK/RECORD/PROCEDURE/EPISODE/CHUNK/NOTE entities in one round-trip — the episode persists even if every link fails. When either `continues` or `links` is provided the response carries `links: {succeeded:[{to, edge_kind}], failed:[{to, edge_kind, error}], summary:{succeeded, failed}}` (the `continues` entry appears first). Use `links_add` for any links discovered after the write.".into(),
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
                    "continues": {
                        "type": "string",
                        "description": "Optional. The `summary_id` of a prior episode this episode continues. Internally lowered to a `links` entry of edge_kind `continues` (DAG-validated). The synthesized entry is reported in the response's `links` block, prepended before any explicit entries from `links`."
                    },
                    "links": inline_links_schema("Optional. After the episode is stored, create polymorphic edges from it (as EPISODE) to the listed entities. Partial failures are reported per-link without aborting the write. Prefer the top-level `continues` parameter for thread-extension edges; use `links` for non-thread relationships (covers, relates_to, see_also, etc.).")
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

            // Validate `continues` predecessor before the episode is stored.
            // Thread extension is atomic — either the new episode lands AND is
            // linked, or nothing happens. The typed parameter exists to be
            // stricter than a generic `links` entry.
            if let Some(prev_id) = &params.continues {
                if prev_id.is_empty() {
                    return ToolCallResult::error(
                        "continues: predecessor summary_id must not be empty",
                    );
                }
                match ctx.stores.get_summary_by_id(prev_id) {
                    Ok(None) => {
                        return ToolCallResult::error(format!(
                            "continues: predecessor episode not found: {prev_id}"
                        ));
                    }
                    Ok(Some(row)) => {
                        if row.brain_id != ctx.brain_id() {
                            return ToolCallResult::error(
                                "continues: cross-brain references are not yet supported (predecessor is in a different brain)",
                            );
                        }
                        if row.kind != "episode" {
                            return ToolCallResult::error(format!(
                                "continues: predecessor must be an episode (got kind: {})",
                                row.kind
                            ));
                        }
                    }
                    Err(e) => {
                        error!(error = %e, prev_id = %prev_id, "failed to validate continues predecessor");
                        return ToolCallResult::error(format!(
                            "continues: failed to validate predecessor: {e}"
                        ));
                    }
                }
            }

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

            // Lower the typed `continues` shortcut to a generic inline link
            // entry. The synthesized entry is prepended so it appears first
            // in the response — agents looking for "did the thread extension
            // succeed?" find it without scanning. Passing both `continues`
            // and a redundant entry in `links` is the agent's choice; we
            // do not de-duplicate.
            let mut effective_links = params.links.clone();
            if let Some(prev_id) = &params.continues {
                effective_links.insert(
                    0,
                    InlineLinkInput {
                        to: EntityRefInput {
                            entity_type: entity_type_str(EntityType::Episode).to_string(),
                            id: prev_id.clone(),
                        },
                        edge_kind: Some(edge_kind_str(EdgeKind::Continues).to_string()),
                    },
                );
            }

            if !effective_links.is_empty() {
                let from_ref = EntityRef::new(EntityType::Episode, summary_id.clone())
                    .expect("summary_id is non-empty");
                let links_block = apply_inline_links(from_ref, effective_links, ctx);
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
    async fn test_write_episode_continues_extends_thread() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Seed the head of the thread.
        let head = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Start the thread",
                    "actions": "First episode",
                    "outcome": "Thread head stored"
                }),
                &ctx,
            )
            .await;
        assert!(head.is_error.is_none());
        let head_parsed: serde_json::Value = serde_json::from_str(&head.content[0].text).unwrap();
        let head_id = head_parsed["summary_id"].as_str().unwrap().to_string();

        // Extend the thread via `continues`.
        let next = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Extend the thread",
                    "actions": "Second episode continues the first",
                    "outcome": "Continues edge persisted",
                    "continues": head_id,
                }),
                &ctx,
            )
            .await;
        assert!(next.is_error.is_none());

        let next_parsed: serde_json::Value = serde_json::from_str(&next.content[0].text).unwrap();
        assert_eq!(next_parsed["status"], "stored");
        assert_eq!(next_parsed["links"]["summary"]["succeeded"], 1);
        assert_eq!(next_parsed["links"]["summary"]["failed"], 0);
        let succeeded = next_parsed["links"]["succeeded"].as_array().unwrap();
        assert_eq!(succeeded.len(), 1);
        assert_eq!(succeeded[0]["edge_kind"], "continues");
        assert_eq!(succeeded[0]["to"]["type"], "EPISODE");
    }

    #[tokio::test]
    async fn test_write_episode_continues_round_trips_via_links_for_entity() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let head = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Round-trip head",
                    "actions": "Write head",
                    "outcome": "Head stored"
                }),
                &ctx,
            )
            .await;
        let head_parsed: serde_json::Value = serde_json::from_str(&head.content[0].text).unwrap();
        let head_id = head_parsed["summary_id"].as_str().unwrap().to_string();

        let next = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Round-trip continuation",
                    "actions": "Write continuation",
                    "outcome": "Continuation stored",
                    "continues": &head_id,
                }),
                &ctx,
            )
            .await;
        let next_parsed: serde_json::Value = serde_json::from_str(&next.content[0].text).unwrap();
        let next_id = next_parsed["summary_id"].as_str().unwrap().to_string();

        // From the head, an incoming edge of kind `continues` from the next episode must be visible.
        let lookup = registry
            .dispatch(
                "links.for_entity",
                json!({
                    "entity": { "type": "EPISODE", "id": head_id },
                    "direction": "in"
                }),
                &ctx,
            )
            .await;
        assert!(lookup.is_error.is_none(), "lookup failed: {:?}", lookup);
        let lookup_parsed: serde_json::Value =
            serde_json::from_str(&lookup.content[0].text).unwrap();
        let incoming = lookup_parsed["incoming"]
            .as_array()
            .expect("incoming array");
        assert_eq!(incoming.len(), 1, "expected one incoming continues edge");
        assert_eq!(incoming[0]["edge_kind"], "continues");
        assert_eq!(incoming[0]["from"]["type"], "EPISODE");
        assert_eq!(incoming[0]["from"]["id"], next_id);
    }

    #[tokio::test]
    async fn test_write_episode_continues_missing_predecessor_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Reject missing predecessor",
                    "actions": "Pass a non-existent continues id",
                    "outcome": "Should error",
                    "continues": "01KQNONEXISTENTEPISODE0000",
                }),
                &ctx,
            )
            .await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(
            text.contains("predecessor episode not found"),
            "got: {text}"
        );
    }

    #[tokio::test]
    async fn test_write_episode_continues_empty_string_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Reject empty continues",
                    "actions": "Pass empty string",
                    "outcome": "Should error",
                    "continues": "",
                }),
                &ctx,
            )
            .await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("must not be empty"), "got: {text}");
    }

    #[tokio::test]
    async fn test_write_episode_continues_non_episode_rejected() {
        // A reflection (kind != "episode") cannot be a thread predecessor.
        // We seed a reflection summary directly, then attempt to continue it.
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let reflection_id = ctx
            .stores
            .store_reflection(
                "title",
                "content",
                &[],
                &[],
                1.0,
                ctx.brain_id(),
            )
            .expect("seed reflection");

        let result = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Reject non-episode predecessor",
                    "actions": "Pass a reflection id",
                    "outcome": "Should error",
                    "continues": &reflection_id,
                }),
                &ctx,
            )
            .await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(
            text.contains("must be an episode"),
            "got: {text}"
        );
    }

    #[tokio::test]
    async fn test_write_episode_continues_alongside_explicit_links() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Seed head episode.
        let head = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Combined head",
                    "actions": "Seed",
                    "outcome": "Stored"
                }),
                &ctx,
            )
            .await;
        let head_parsed: serde_json::Value = serde_json::from_str(&head.content[0].text).unwrap();
        let head_id = head_parsed["summary_id"].as_str().unwrap().to_string();

        // Combine `continues` (synthesized first) with an explicit `links` entry to a TASK.
        let result = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Combined write",
                    "actions": "continues + explicit link",
                    "outcome": "Both edges land",
                    "continues": &head_id,
                    "links": [
                        { "to": { "type": "TASK", "id": "TST-01COMBINEDTASK0001" } }
                    ]
                }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());

        let parsed: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["links"]["summary"]["succeeded"], 2);
        assert_eq!(parsed["links"]["summary"]["failed"], 0);

        // Continues entry must appear first (prepended) — agents looking for thread-extension status find it without scanning.
        let succeeded = parsed["links"]["succeeded"].as_array().unwrap();
        assert_eq!(succeeded.len(), 2);
        assert_eq!(succeeded[0]["edge_kind"], "continues");
        assert_eq!(succeeded[0]["to"]["type"], "EPISODE");
        assert_eq!(succeeded[1]["to"]["type"], "TASK");
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
                "continues": {
                    "type": "string",
                    "description": "Optional. The `summary_id` of a prior episode this episode continues. Internally lowered to a `links` entry of edge_kind `continues` (DAG-validated). The synthesized entry is reported in the response's `links` block, prepended before any explicit entries from `links`."
                },
                "links": {
                    "type": "array",
                    "description": "Optional. After the episode is stored, create polymorphic edges from it (as EPISODE) to the listed entities. Partial failures are reported per-link without aborting the write. Prefer the top-level `continues` parameter for thread-extension edges; use `links` for non-thread relationships (covers, relates_to, see_also, etc.).",
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
            "required": ["goal", "actions", "outcome"]
        });
        assert_eq!(
            schema, expected,
            "memory.write_episode schema changed — update golden or revert"
        );
    }
}
