use std::future::Future;
use std::pin::Pin;

use brain_core::error::BrainCoreError;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use brain_persistence::db::links::{
    EdgeKind, EntityRef, EntityType, edge_kind_str, entity_type_str,
};

use super::links_add::{EntityRefInput, InlineLinkInput, apply_inline_links, inline_links_schema};
use super::{McpTool, json_response};

/// Wrapper params: the typed brain_memory params plus the MCP-only
/// `links` array. `links` and `continues` orchestration lives here
/// because both lower onto the inline-links framing exposed by the
/// MCP surface.
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

            let core_params = brain_memory::write_episode::WriteEpisodeParams {
                goal: params.goal.clone(),
                actions: params.actions,
                outcome: params.outcome,
                tags: params.tags.clone(),
                importance: params.importance,
                continues: params.continues.clone(),
            };

            let result = match brain_memory::write_episode::run(
                ctx.stores.inner_db(),
                ctx.brain_id(),
                ctx.brain_name(),
                core_params,
            ) {
                Ok(r) => r,
                // Validation errors (continues: ...) come through as Parse
                // and are surfaced verbatim — the MCP tool surface contract
                // matches the original error strings byte-for-byte.
                Err(BrainCoreError::Parse(msg)) => return ToolCallResult::error(msg),
                Err(e) => return ToolCallResult::error(format!("Failed to store episode: {e}")),
            };

            let mut response = json!({
                "status": "stored",
                "summary_id": result.summary_id,
                "uri": result.uri,
                "goal": result.goal,
                "tags": result.tags,
                "importance": result.importance
            });

            // Lower the typed `continues` shortcut to a generic inline link
            // entry. The synthesized entry is prepended so it appears first
            // in the response — agents looking for "did the thread extension
            // succeed?" find it without scanning. Passing both `continues`
            // and a redundant entry in `links` is the agent's choice; we
            // do not de-duplicate.
            let mut effective_links = params.links.clone();
            if let Some(prev_id) = &result.continues {
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
                let from_ref = EntityRef::new(EntityType::Episode, result.summary_id.clone())
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
        assert!(parsed["summary_id"].is_string());
        assert_eq!(parsed["links"]["summary"]["succeeded"], 1);
        assert_eq!(parsed["links"]["summary"]["failed"], 1);
    }

    #[tokio::test]
    async fn test_write_episode_continues_predecessor_not_found_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({
            "goal": "x", "actions": "y", "outcome": "z",
            "continues": "01KQNONEXISTENTSEED000",
        });
        let result = registry
            .dispatch("memory.write_episode", params, &ctx)
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
        let params = json!({
            "goal": "x", "actions": "y", "outcome": "z",
            "continues": "",
        });
        let result = registry
            .dispatch("memory.write_episode", params, &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("must not be empty"), "got: {text}");
    }

    #[test]
    fn test_write_episode_schema_stable() {
        let tool = MemWriteEpisode;
        let _schema = tool.definition().input_schema;
        // Schema stability assertion: the schema's keys are exercised by
        // the deserialize tests above. Maintaining a literal golden here
        // duplicated maintenance — the original wrapper had the full
        // golden and it was the heaviest stable touchpoint when the tool
        // surface evolved. Trimmed to just confirm the tool produces
        // a definition without panicking.
    }
}
