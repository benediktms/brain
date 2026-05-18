//! Framework-free helpers shared by tool handlers.
//!
//! Ported from `brain_lib::mcp::tools::helpers` minus `build_federated_brains`.
//! The federated-search helper is server-side now — the daemon handles
//! cross-brain resolution; MCP tool bodies pass brain names through
//! the wire and let the daemon expand `["all"]` and open remote
//! stores.

use serde::Serialize;
use serde_json::{Value, json};

use brain_rpc::{LinksAddParams, SagaCascadeOutcome, SagaCascadeResult, WireEntityRef};

use crate::context::McpContext;
use crate::protocol::ToolCallResult;

/// One inline-link request: target entity plus optional `edge_kind`.
///
/// The `from` entity is implicit (the just-written episode/procedure).
/// Defaults to `relates_to` when `edge_kind` is omitted.
#[derive(serde::Deserialize, Clone, Debug)]
pub struct InlineLinkInput {
    pub to: InlineEntityInput,
    #[serde(default)]
    pub edge_kind: Option<String>,
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct InlineEntityInput {
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
}

/// Shared JSON Schema fragment for inline `links` arrays accepted by
/// `memory.write_episode` and `memory.write_procedure`. Description is
/// passed in so each call site can preserve its own wording.
pub fn inline_links_schema(description: &str) -> Value {
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

/// Cap on inline-link batch size; oversized requests are rejected before
/// any wire call.
pub const MAX_INLINE_LINKS: usize = 256;

/// Apply a batch of inline links via `links_add` wire calls, mirroring
/// the legacy `{succeeded, failed, summary}` envelope.
///
/// Each link becomes one `DaemonClient::links_add` round-trip; failures
/// are accumulated without aborting the batch. Edge_kind defaults to
/// `relates_to`. The episode/procedure is already persisted server-side
/// before this runs — partial failures don't roll the write back.
pub async fn apply_inline_links(
    from_kind: &str,
    from_id: &str,
    links: Vec<InlineLinkInput>,
    ctx: &McpContext,
) -> Value {
    if links.len() > MAX_INLINE_LINKS {
        return json!({
            "succeeded": [],
            "failed": [{ "error": format!("links batch too large: {} > {MAX_INLINE_LINKS}", links.len()) }],
            "summary": { "succeeded": 0, "failed": 1 }
        });
    }

    let mut succeeded: Vec<Value> = Vec::new();
    let mut failed: Vec<Value> = Vec::new();

    for link in links {
        let edge_kind_wire = link
            .edge_kind
            .clone()
            .unwrap_or_else(|| "relates_to".to_string());

        let to_entity = json!({ "type": link.to.kind, "id": link.to.id });

        let wire_params = LinksAddParams {
            from: WireEntityRef {
                kind: from_kind.to_string(),
                id: from_id.to_string(),
            },
            to: WireEntityRef {
                kind: link.to.kind.clone(),
                id: link.to.id.clone(),
            },
            edge_kind: edge_kind_wire.clone(),
        };

        match ctx.with_client(|c| c.links_add(wire_params)).await {
            Ok(_) => succeeded.push(json!({
                "to": to_entity,
                "edge_kind": edge_kind_wire,
            })),
            Err(err) => {
                let msg = err.to_string();
                let mapped = if msg.contains("cycle") {
                    format!("would create a cycle in {edge_kind_wire} graph")
                } else {
                    msg
                };
                failed.push(json!({
                    "to": to_entity,
                    "edge_kind": edge_kind_wire,
                    "error": mapped,
                }));
            }
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

/// Shared JSON Schema fragment for a polymorphic `{type, id}` entity
/// reference. Used by every tool whose input includes an entity ref
/// (links.add, links.remove, links.for_entity, and record/saga tools).
/// Byte-identical to the legacy `entity_ref_schema()` in
/// `brain_lib::mcp::tools::links_add` — preserve verbatim.
pub fn entity_ref_schema() -> Value {
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

#[derive(Serialize, Debug, Clone)]
pub struct Warning {
    pub source: String,
    pub error: String,
}

pub fn json_response(value: &impl Serialize) -> ToolCallResult {
    match serde_json::to_string_pretty(value) {
        Ok(json) => ToolCallResult::text(json),
        Err(err) => ToolCallResult::error(format!("Internal serialization error: {err}")),
    }
}

/// Convert `Vec<SagaCascadeResult>` to the wire-format JSON array used
/// by the saga close/cancel MCP responses. Variants map to:
///
/// - `Closed`    → `{ "task_id": "...", "closed":    true }`
/// - `Cancelled` → `{ "task_id": "...", "cancelled": true }`
/// - `Skipped`   → `{ "task_id": "...", "skipped":   true, "reason": "..." }`
/// - `Failed`    → `{ "task_id": "...", "failed":    true, "error":  "..." }`
pub fn cascade_results_to_json(results: &[SagaCascadeResult]) -> Vec<Value> {
    results
        .iter()
        .map(|r| match &r.outcome {
            SagaCascadeOutcome::Closed => json!({
                "task_id": r.task_id,
                "closed": true,
            }),
            SagaCascadeOutcome::Cancelled => json!({
                "task_id": r.task_id,
                "cancelled": true,
            }),
            SagaCascadeOutcome::Skipped { reason } => json!({
                "task_id": r.task_id,
                "skipped": true,
                "reason": reason,
            }),
            SagaCascadeOutcome::Failed { error } => json!({
                "task_id": r.task_id,
                "failed": true,
                "error": error,
            }),
        })
        .collect()
}

pub fn store_or_warn<T: Default>(
    result: Result<T, impl std::fmt::Display>,
    source: &str,
    warnings: &mut Vec<Warning>,
) -> T {
    match result {
        Ok(value) => value,
        Err(err) => {
            warnings.push(Warning {
                source: source.to_string(),
                error: err.to_string(),
            });
            T::default()
        }
    }
}

pub fn inject_warnings(response: &mut Value, warnings: Vec<Warning>) {
    if warnings.is_empty() {
        return;
    }

    if let Value::Object(map) = response
        && let Ok(warnings_value) = serde_json::to_value(warnings)
    {
        map.insert("warnings".to_string(), warnings_value);
    }
}

#[cfg(test)]
mod tests {
    use serde::ser::Error as _;
    use serde::ser::Serializer;
    use serde_json::json;

    use super::*;

    struct AlwaysFailSerialize;

    impl Serialize for AlwaysFailSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(S::Error::custom("boom"))
        }
    }

    #[derive(Serialize)]
    struct ResponseWithWarnings {
        data: String,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<Warning>,
    }

    #[test]
    fn json_response_success() {
        let response = json_response(&json!({"ok": true, "count": 1}));
        assert_eq!(response.is_error, None);

        let text = &response.content[0].text;
        let parsed: Value = serde_json::from_str(text).expect("json response should be valid");
        assert_eq!(parsed["ok"], true);
        assert_eq!(parsed["count"], 1);
    }

    #[test]
    fn json_response_failure() {
        let response = json_response(&AlwaysFailSerialize);
        assert_eq!(response.is_error, Some(true));
        assert!(
            response.content[0]
                .text
                .to_lowercase()
                .contains("serialization error")
        );
    }

    #[test]
    fn store_or_warn_ok() {
        let mut warnings = Vec::new();
        let input: Result<Vec<&str>, &str> = Ok(vec!["a"]);
        let result = store_or_warn(input, "get_items", &mut warnings);

        assert_eq!(result, vec!["a"]);
        assert!(warnings.is_empty());
    }

    #[test]
    fn store_or_warn_err() {
        let mut warnings = Vec::new();
        let result: Vec<String> = store_or_warn(Err("db broken"), "get_task_labels", &mut warnings);

        assert!(result.is_empty());
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].source, "get_task_labels");
        assert_eq!(warnings[0].error, "db broken");
    }

    #[test]
    fn warnings_skip_when_empty() {
        let payload = ResponseWithWarnings {
            data: "ok".into(),
            warnings: vec![],
        };

        let value = serde_json::to_value(payload).expect("serializes");
        let object = value.as_object().expect("object");

        assert_eq!(object.get("data"), Some(&json!("ok")));
        assert!(!object.contains_key("warnings"));
    }

    #[test]
    fn cascade_skipped_renders_reason() {
        let results = vec![SagaCascadeResult {
            task_id: "brn-abc.1".into(),
            outcome: SagaCascadeOutcome::Skipped {
                reason: "already done".into(),
            },
        }];
        let arr = cascade_results_to_json(&results);
        assert_eq!(arr[0]["task_id"], "brn-abc.1");
        assert_eq!(arr[0]["skipped"], true);
        assert_eq!(arr[0]["reason"], "already done");
    }
}
