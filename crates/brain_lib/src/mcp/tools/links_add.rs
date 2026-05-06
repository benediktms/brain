use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::links::{
    EdgeKind, EntityRef, LinkError, add_link_checked, edge_kind_from_str, entity_type_from_str,
    entity_type_str,
};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct LinksAddParams {
    from: EntityRefInput,
    to: EntityRefInput,
    edge_kind: Option<String>,
}

#[derive(Deserialize, Clone)]
pub(super) struct EntityRefInput {
    #[serde(rename = "type")]
    pub(super) entity_type: String,
    pub(super) id: String,
}

/// Inline link request: a `to` entity plus an optional `edge_kind`. Used by
/// batch callers (e.g. `memory.write_episode` with inline `links`) where the
/// `from` entity is implicit (the just-written summary).
#[derive(Deserialize, Clone)]
pub(super) struct InlineLinkInput {
    pub(super) to: EntityRefInput,
    #[serde(default)]
    pub(super) edge_kind: Option<String>,
}

pub(super) fn resolve_entity_ref(input: EntityRefInput) -> Result<EntityRef, String> {
    let kind = entity_type_from_str(&input.entity_type)
        .ok_or_else(|| format!("unknown entity type: {}", input.entity_type))?;
    EntityRef::new(kind, input.id).map_err(|e| e.to_string())
}

/// Shared JSON Schema fragment for `{type, id}` entity references.
/// Used by every MCP tool whose input includes an `EntityRefInput`.
pub(super) fn entity_ref_schema() -> Value {
    json!({
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
    })
}

/// Shared JSON Schema fragment for the inline `links` parameter accepted by
/// `memory.write_episode` and `memory.write_procedure`.
/// Accepts a per-tool `description` override to preserve golden schema-stability
/// test fixtures (episode and procedure use distinct entity-type wording).
pub(super) fn inline_links_schema(description: &str) -> Value {
    json!({
        "type": "array",
        "description": description,
        "items": {
            "type": "object",
            "properties": {
                "to": entity_ref_schema(),
                "edge_kind": {
                    "type": "string",
                    "enum": ["parent_of", "blocks", "covers", "relates_to", "see_also", "supersedes", "contradicts", "continues"],
                    "description": "Default: relates_to. Semantics: parent_of (DAG-validated; hierarchical containment), blocks (DAG-validated; dependency), supersedes (DAG-validated; replacement), covers (this entity documents/explains the target), relates_to (default; generic association), see_also (cross-reference), contradicts (conflicts with target), continues (DAG-validated; episode-thread continuation — new episode continues the named predecessor). DAG-validated kinds reject cycles per-link without aborting the batch."
                }
            },
            "required": ["to"]
        }
    })
}

/// Maximum number of inline links accepted in a single `apply_inline_links` call.
/// Requests exceeding this cap are rejected without attempting any link.
const MAX_INLINE_LINKS: usize = 256;

/// Persist a batch of inline links from a freshly-written entity to many
/// targets. Returns a `{succeeded, failed, summary}` JSON block in the same
/// shape as `tasks.deps_batch`. Per-link failures (unknown edge_kind, unknown
/// entity_type, cycle in DAG kind, persistence error) populate `failed` but
/// never abort the batch — the caller's primary write has already committed.
///
/// The writer mutex is acquired once for the entire batch to avoid per-link
/// mutex churn. If acquisition itself fails every link is marked failed with
/// a "writer unavailable" error.
pub(super) fn apply_inline_links(
    from: EntityRef,
    links: Vec<InlineLinkInput>,
    ctx: &McpContext,
) -> Value {
    // Defensive cap: reject oversized batches immediately.
    if links.len() > MAX_INLINE_LINKS {
        return json!({
            "succeeded": [],
            "failed": [{ "error": format!("links batch too large: {} > {MAX_INLINE_LINKS}", links.len()) }],
            "summary": { "succeeded": 0, "failed": 1 }
        });
    }

    let mut succeeded: Vec<Value> = Vec::new();
    let mut failed: Vec<Value> = Vec::new();

    // Resolve all link targets before acquiring the writer, so validation
    // failures never hold the mutex.
    struct ResolvedLink {
        from: EntityRef,
        to: EntityRef,
        edge_kind: EdgeKind,
        to_type_wire: String,
        to_id_wire: String,
        edge_kind_wire: String,
    }

    let mut resolved: Vec<ResolvedLink> = Vec::new();

    for link in &links {
        let edge_kind_wire = link
            .edge_kind
            .clone()
            .unwrap_or_else(|| "relates_to".to_string());

        let edge_kind = match edge_kind_from_str(&edge_kind_wire) {
            Some(k) => k,
            None => {
                failed.push(json!({
                    "to": { "type": link.to.entity_type, "id": link.to.id },
                    "edge_kind": edge_kind_wire,
                    "error": format!("unknown edge_kind: {edge_kind_wire}")
                }));
                continue;
            }
        };

        let to_kind = match entity_type_from_str(&link.to.entity_type) {
            Some(k) => k,
            None => {
                failed.push(json!({
                    "to": { "type": link.to.entity_type, "id": link.to.id },
                    "edge_kind": edge_kind_wire,
                    "error": format!("unknown entity type: {}", link.to.entity_type)
                }));
                continue;
            }
        };

        let to_ref = match EntityRef::new(to_kind, link.to.id.clone()) {
            Ok(r) => r,
            Err(e) => {
                failed.push(json!({
                    "to": { "type": link.to.entity_type, "id": link.to.id },
                    "edge_kind": edge_kind_wire,
                    "error": e.to_string()
                }));
                continue;
            }
        };

        resolved.push(ResolvedLink {
            from: from.clone(),
            to: to_ref,
            edge_kind,
            to_type_wire: link.to.entity_type.clone(),
            to_id_wire: link.to.id.clone(),
            edge_kind_wire,
        });
    }

    if resolved.is_empty() {
        // All links failed during resolution; no DB work to do.
        let succeeded_len = succeeded.len();
        let failed_len = failed.len();
        return json!({
            "succeeded": succeeded,
            "failed": failed,
            "summary": {
                "succeeded": succeeded_len,
                "failed": failed_len,
            }
        });
    }

    // Acquire the writer once for the whole batch.
    let outer_result = ctx.stores.inner_db().with_write_conn(|conn| {
        for r in &resolved {
            match add_link_checked(conn, r.from.clone(), r.to.clone(), r.edge_kind) {
                Ok(()) => succeeded.push(json!({
                    "to": { "type": r.to_type_wire, "id": r.to_id_wire },
                    "edge_kind": r.edge_kind_wire
                })),
                Err(LinkError::Cycle(_)) => failed.push(json!({
                    "to": { "type": r.to_type_wire, "id": r.to_id_wire },
                    "edge_kind": r.edge_kind_wire,
                    "error": format!("would create a cycle in {} graph", r.edge_kind_wire)
                })),
                Err(e) => failed.push(json!({
                    "to": { "type": r.to_type_wire, "id": r.to_id_wire },
                    "edge_kind": r.edge_kind_wire,
                    "error": e.to_string()
                })),
            }
        }
        Ok::<_, brain_persistence::error::BrainCoreError>(())
    });

    // Surface outer-only failure (e.g. writer mutex unavailable) as failed entries
    // for every link that was not yet attempted.
    if let Err(outer_err) = outer_result {
        // succeeded/failed were partially populated inside the closure; any link
        // not processed needs a failure entry. We track unprocessed count as
        // resolved.len() minus whatever the closure managed to push.
        let processed = succeeded.len() + failed.len();
        // Closure-only count doesn't double-include pre-resolution failures;
        // add outer error entries for any resolved links that never ran.
        let unprocessed = resolved.len().saturating_sub(processed);
        let err_msg = format!("writer unavailable: {outer_err}");
        for _ in 0..unprocessed {
            failed.push(json!({ "error": err_msg }));
        }
    }

    let succeeded_len = succeeded.len();
    let failed_len = failed.len();
    json!({
        "succeeded": succeeded,
        "failed": failed,
        "summary": {
            "succeeded": succeeded_len,
            "failed": failed_len,
        }
    })
}

pub(super) struct LinksAdd;

/// Shared add-link logic callable from both the polymorphic surface and the records shim.
///
/// Translates resolved `from`/`to` refs and `edge_kind` into a write through
/// `add_link_checked`, returning the synthesised edge ID on success.
pub(super) fn add_entity_link(
    from: EntityRef,
    to: EntityRef,
    edge_kind: EdgeKind,
    edge_kind_wire: &str,
    ctx: &McpContext,
) -> ToolCallResult {
    let from_type = entity_type_str(from.kind);
    let from_id = from.id.clone();
    let to_type = entity_type_str(to.kind);
    let to_id = to.id.clone();

    let mut link_err: Option<LinkError> = None;
    let outer_result = ctx.stores.inner_db().with_write_conn(|conn| {
        match add_link_checked(conn, from, to, edge_kind) {
            Ok(()) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                link_err = Some(e);
                Err(brain_persistence::error::BrainCoreError::Database(msg))
            }
        }
    });

    // Three-way dispatch mirrors try_add_entity_link:
    //   (Ok, _)        — success; build the synthesised edge id.
    //   (_, Some(e))   — closure-set typed error wins over the outer Result.
    //   (Err(e), None) — outer-only failure (mutex etc.); surface as tool error.
    match (outer_result, link_err) {
        (Ok(()), _) => {
            let id = format!("{from_type}:{from_id}->{edge_kind_wire}->{to_type}:{to_id}");
            json_response(&json!({ "id": id }))
        }
        (_, Some(LinkError::Cycle(_))) => {
            ToolCallResult::error(format!("would create a cycle in {edge_kind_wire} graph"))
        }
        (_, Some(e)) => ToolCallResult::error(e.to_string()),
        (Err(e), None) => ToolCallResult::error(format!("writer unavailable: {e}")),
    }
}

impl LinksAdd {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: LinksAddParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let from = match resolve_entity_ref(params.from) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Invalid 'from': {e}")),
        };

        let to = match resolve_entity_ref(params.to) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Invalid 'to': {e}")),
        };

        let edge_kind_wire = params.edge_kind.as_deref().unwrap_or("relates_to");
        let edge_kind = match edge_kind_from_str(edge_kind_wire) {
            Some(k) => k,
            None => {
                return ToolCallResult::error(format!("unknown edge_kind: {edge_kind_wire}"));
            }
        };

        add_entity_link(from, to, edge_kind, edge_kind_wire, ctx)
    }
}

impl McpTool for LinksAdd {
    fn name(&self) -> &'static str {
        "links.add"
    }

    fn definition(&self) -> ToolDefinition {
        let entity_ref_schema = entity_ref_schema();

        ToolDefinition {
            name: self.name().into(),
            description: "Add a directed polymorphic edge between two entities. Defaults to 'relates_to' when edge_kind is omitted. DAG kinds (parent_of, blocks, supersedes, continues) are cycle-checked. Idempotent: re-adding an existing edge returns the same synthesised id without inserting a new row. The returned id is a deterministic compound key (FROM_TYPE:from_id->edge->TO_TYPE:to_id), not a durable ULID.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "from": entity_ref_schema.clone(),
                    "to": entity_ref_schema,
                    "edge_kind": {
                        "type": "string",
                        "enum": ["parent_of", "blocks", "covers", "relates_to", "see_also", "supersedes", "contradicts", "continues"],
                        "description": "Edge kind (default: relates_to). Use 'continues' to attach episode-thread continuation edges to existing episodes (DAG-validated)."
                    }
                },
                "required": ["from", "to"]
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::mcp::tools::tests::create_test_context;

    fn text_of(result: &ToolCallResult) -> &str {
        &result.content.first().unwrap().text
    }

    async fn call(params: Value) -> ToolCallResult {
        let (_dir, ctx) = create_test_context().await;
        LinksAdd.execute(params, &ctx)
    }

    #[tokio::test]
    async fn happy_path_returns_id() {
        let result = call(json!({
            "from": { "type": "TASK", "id": "task-a" },
            "to": { "type": "TASK", "id": "task-b" }
        }))
        .await;

        assert_ne!(result.is_error, Some(true));
        let parsed: serde_json::Value = serde_json::from_str(text_of(&result)).unwrap();
        assert!(parsed["id"].is_string());
    }

    #[tokio::test]
    async fn cycle_rejection_dag_kind() {
        let (_dir, ctx) = create_test_context().await;

        // A → B
        LinksAdd.execute(
            json!({
                "from": { "type": "TASK", "id": "A" },
                "to": { "type": "TASK", "id": "B" },
                "edge_kind": "blocks"
            }),
            &ctx,
        );

        // B → A would create a cycle
        let result = LinksAdd.execute(
            json!({
                "from": { "type": "TASK", "id": "B" },
                "to": { "type": "TASK", "id": "A" },
                "edge_kind": "blocks"
            }),
            &ctx,
        );

        assert_eq!(result.is_error, Some(true));
        let text = text_of(&result);
        assert!(text.contains("cycle"), "expected cycle error, got: {text}");
    }

    #[tokio::test]
    async fn unknown_edge_kind_returns_invalid_params() {
        let result = call(json!({
            "from": { "type": "TASK", "id": "a" },
            "to": { "type": "RECORD", "id": "r1" },
            "edge_kind": "totally_unknown"
        }))
        .await;

        assert_eq!(result.is_error, Some(true));
        assert!(text_of(&result).contains("unknown edge_kind"));
    }

    #[tokio::test]
    async fn unknown_entity_type_returns_error() {
        let result = call(json!({
            "from": { "type": "BOGUS", "id": "x" },
            "to": { "type": "TASK", "id": "y" }
        }))
        .await;

        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn default_edge_kind_is_relates_to() {
        let result = call(json!({
            "from": { "type": "RECORD", "id": "rec-1" },
            "to": { "type": "EPISODE", "id": "ep-1" }
        }))
        .await;

        assert_ne!(result.is_error, Some(true));
        assert!(
            text_of(&result).contains("relates_to"),
            "id should encode relates_to"
        );
    }

    // ── Finding 3: oversize batch rejected without attempting any links ────────
    #[tokio::test]
    async fn apply_inline_links_rejects_oversize_batch() {
        let (_dir, ctx) = create_test_context().await;
        let from =
            EntityRef::new(entity_type_from_str("TASK").unwrap(), "src".to_string()).unwrap();

        // Build a batch of 257 entries — one over the cap.
        let links: Vec<InlineLinkInput> = (0..257)
            .map(|i| InlineLinkInput {
                to: EntityRefInput {
                    entity_type: "TASK".to_string(),
                    id: format!("target-{i}"),
                },
                edge_kind: None,
            })
            .collect();

        let result = apply_inline_links(from, links, &ctx);

        let succeeded = result["summary"]["succeeded"].as_u64().unwrap_or(0);
        let failed = result["summary"]["failed"].as_u64().unwrap_or(0);
        assert_eq!(succeeded, 0, "expected no successes for oversized batch");
        assert_eq!(
            failed, 1,
            "expected exactly one failure entry for oversized batch"
        );

        let err_msg = result["failed"][0]["error"].as_str().unwrap_or("");
        assert!(
            err_msg.contains("links batch too large"),
            "expected 'links batch too large' in error, got: {err_msg}"
        );
        assert!(
            err_msg.contains("257"),
            "error should include actual count, got: {err_msg}"
        );
    }

    // ── Finding #4: self-loop with DAG kind is rejected ───────────────────────
    #[tokio::test]
    async fn apply_inline_links_self_loop_parent_of_rejected() {
        let (_dir, ctx) = create_test_context().await;
        let from = EntityRef::new(
            entity_type_from_str("TASK").unwrap(),
            "selfloop".to_string(),
        )
        .unwrap();
        let links = vec![InlineLinkInput {
            to: EntityRefInput {
                entity_type: "TASK".to_string(),
                id: "selfloop".to_string(),
            },
            edge_kind: Some("parent_of".to_string()),
        }];
        let result = apply_inline_links(from, links, &ctx);
        assert_eq!(result["summary"]["succeeded"], 0);
        assert_eq!(result["summary"]["failed"], 1);
        let err = result["failed"][0]["error"].as_str().unwrap();
        assert!(err.contains("cycle") || err.contains("self"), "got: {err}");
    }

    // ── Finding #5: batch of exactly MAX_INLINE_LINKS (256) is accepted ───────
    #[tokio::test]
    async fn apply_inline_links_accepts_max_size_batch() {
        let (_dir, ctx) = create_test_context().await;
        let from =
            EntityRef::new(entity_type_from_str("TASK").unwrap(), "src".to_string()).unwrap();
        let links: Vec<InlineLinkInput> = (0..256)
            .map(|i| InlineLinkInput {
                to: EntityRefInput {
                    entity_type: "TASK".to_string(),
                    id: format!("t{i}"),
                },
                edge_kind: Some("relates_to".to_string()),
            })
            .collect();
        let result = apply_inline_links(from, links, &ctx);
        assert_eq!(result["summary"]["succeeded"], 256);
        assert_eq!(result["summary"]["failed"], 0);
    }

    // ── Finding #6: empty id triggers EntityRef::new error pre-resolution ─────
    #[tokio::test]
    async fn apply_inline_links_empty_id_failed_pre_resolution() {
        let (_dir, ctx) = create_test_context().await;
        let from =
            EntityRef::new(entity_type_from_str("TASK").unwrap(), "src".to_string()).unwrap();
        let links = vec![InlineLinkInput {
            to: EntityRefInput {
                entity_type: "TASK".to_string(),
                id: "".to_string(),
            },
            edge_kind: Some("relates_to".to_string()),
        }];
        let result = apply_inline_links(from, links, &ctx);
        assert_eq!(result["summary"]["failed"], 1);
        assert_eq!(result["summary"]["succeeded"], 0);
        // After Finding #12: pre-resolution failure entries must include edge_kind.
        assert_eq!(result["failed"][0]["edge_kind"], "relates_to");
    }

    // ── Probe #6: unit tests for schema helpers ───────────────────────────────
    #[test]
    fn entity_ref_schema_pins_enum_and_required() {
        let s = entity_ref_schema();
        let enums = s["properties"]["type"]["enum"].as_array().unwrap();
        let kinds: Vec<_> = enums.iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(
            kinds,
            vec!["TASK", "RECORD", "EPISODE", "PROCEDURE", "CHUNK", "NOTE"]
        );
        assert_eq!(s["required"], json!(["type", "id"]));
    }

    #[test]
    fn inline_links_schema_with_description_returns_expected_shape() {
        let s = inline_links_schema("custom-desc");
        assert_eq!(s["type"], "array");
        assert_eq!(s["description"], "custom-desc");
        let edge_kind_enum = s["items"]["properties"]["edge_kind"]["enum"]
            .as_array()
            .unwrap();
        let kinds: Vec<_> = edge_kind_enum.iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(
            kinds,
            vec![
                "parent_of",
                "blocks",
                "covers",
                "relates_to",
                "see_also",
                "supersedes",
                "contradicts",
                "continues"
            ]
        );
        assert_eq!(s["items"]["required"], json!(["to"]));
    }
}
