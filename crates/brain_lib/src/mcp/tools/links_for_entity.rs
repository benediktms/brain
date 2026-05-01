use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::links::{EdgeKind, EntityRef, EntityType, LinkError, for_entity};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct LinksForEntityParams {
    entity: EntityRefInput,
    direction: Option<String>,
}

#[derive(Deserialize)]
struct EntityRefInput {
    #[serde(rename = "type")]
    entity_type: String,
    id: String,
}

fn parse_entity_type(s: &str) -> Option<EntityType> {
    match s {
        "TASK" => Some(EntityType::Task),
        "RECORD" => Some(EntityType::Record),
        "EPISODE" => Some(EntityType::Episode),
        "PROCEDURE" => Some(EntityType::Procedure),
        "CHUNK" => Some(EntityType::Chunk),
        "NOTE" => Some(EntityType::Note),
        _ => None,
    }
}

fn edge_kind_str(k: EdgeKind) -> &'static str {
    match k {
        EdgeKind::ParentOf => "parent_of",
        EdgeKind::Blocks => "blocks",
        EdgeKind::Covers => "covers",
        EdgeKind::RelatesTo => "relates_to",
        EdgeKind::SeeAlso => "see_also",
        EdgeKind::Supersedes => "supersedes",
        EdgeKind::Contradicts => "contradicts",
    }
}

fn entity_type_str(t: EntityType) -> &'static str {
    match t {
        EntityType::Task => "TASK",
        EntityType::Record => "RECORD",
        EntityType::Episode => "EPISODE",
        EntityType::Procedure => "PROCEDURE",
        EntityType::Chunk => "CHUNK",
        EntityType::Note => "NOTE",
    }
}

fn entity_ref_to_json(r: &EntityRef) -> Value {
    json!({
        "type": entity_type_str(r.kind),
        "id": r.id
    })
}

pub(super) struct LinksForEntity;

impl LinksForEntity {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: LinksForEntityParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        let kind = match parse_entity_type(&params.entity.entity_type) {
            Some(k) => k,
            None => {
                return ToolCallResult::error(format!(
                    "unknown entity type: {}",
                    params.entity.entity_type
                ));
            }
        };

        let entity = match EntityRef::new(kind, params.entity.id) {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Invalid entity: {e}")),
        };

        let direction = params.direction.as_deref().unwrap_or("both");
        if !matches!(direction, "out" | "in" | "both") {
            return ToolCallResult::error(format!(
                "invalid direction '{direction}': must be 'out', 'in', or 'both'"
            ));
        }

        let links = ctx.stores.inner_db().with_read_conn(|conn| {
            for_entity(conn, entity.clone()).map_err(|e| match e {
                LinkError::Database(msg) => brain_persistence::error::BrainCoreError::Database(msg),
                LinkError::Cycle(_) => unreachable!("for_entity never returns Cycle"),
            })
        });

        match links {
            Err(e) => ToolCallResult::error(format!("Internal error: {e}")),
            Ok(all_links) => {
                let mut outgoing = Vec::new();
                let mut incoming = Vec::new();

                for link in &all_links {
                    let entry = json!({
                        "from": entity_ref_to_json(&link.from),
                        "to": entity_ref_to_json(&link.to),
                        "edge_kind": edge_kind_str(link.edge_kind)
                    });
                    if link.from == entity {
                        outgoing.push(entry);
                    } else {
                        incoming.push(entry);
                    }
                }

                let response = match direction {
                    "out" => json!({ "outgoing": outgoing, "incoming": [] }),
                    "in" => json!({ "outgoing": [], "incoming": incoming }),
                    _ => json!({ "outgoing": outgoing, "incoming": incoming }),
                };

                json_response(&response)
            }
        }
    }
}

impl McpTool for LinksForEntity {
    fn name(&self) -> &'static str {
        "links.for_entity"
    }

    fn definition(&self) -> ToolDefinition {
        let entity_ref_schema = json!({
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": ["TASK", "RECORD", "EPISODE", "PROCEDURE", "CHUNK", "NOTE"],
                    "description": "The entity type"
                },
                "id": {
                    "type": "string",
                    "description": "The entity ID"
                }
            },
            "required": ["type", "id"]
        });

        ToolDefinition {
            name: self.name().into(),
            description: "Return all polymorphic edges where the given entity participates. Partitioned into outgoing (entity is source) and incoming (entity is target). Use direction to filter.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "entity": entity_ref_schema,
                    "direction": {
                        "type": "string",
                        "enum": ["out", "in", "both"],
                        "description": "Filter direction (default: both)"
                    }
                },
                "required": ["entity"]
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
    use crate::mcp::tools::McpTool;
    use crate::mcp::tools::links_add::LinksAdd;
    use crate::mcp::tools::tests::create_test_context;

    fn parse_response(result: &ToolCallResult) -> serde_json::Value {
        serde_json::from_str(&result.content.first().unwrap().text).unwrap()
    }

    #[tokio::test]
    async fn round_trip_add_then_for_entity() {
        let (_dir, ctx) = create_test_context().await;

        // A→B (relates_to), B→A (see_also, non-DAG so cycles ok)
        LinksAdd
            .call(
                json!({
                    "from": { "type": "TASK", "id": "A" },
                    "to": { "type": "TASK", "id": "B" },
                    "edge_kind": "relates_to"
                }),
                &ctx,
            )
            .await;
        LinksAdd
            .call(
                json!({
                    "from": { "type": "TASK", "id": "B" },
                    "to": { "type": "TASK", "id": "A" },
                    "edge_kind": "see_also"
                }),
                &ctx,
            )
            .await;

        // Query for entity B: outgoing=[B→A], incoming=[A→B]
        let result =
            LinksForEntity.execute(json!({ "entity": { "type": "TASK", "id": "B" } }), &ctx);
        assert_ne!(result.is_error, Some(true));
        let parsed = parse_response(&result);

        let outgoing = parsed["outgoing"].as_array().unwrap();
        let incoming = parsed["incoming"].as_array().unwrap();

        assert_eq!(outgoing.len(), 1, "B has one outgoing edge");
        assert_eq!(outgoing[0]["from"]["id"], "B");
        assert_eq!(outgoing[0]["to"]["id"], "A");
        assert_eq!(outgoing[0]["edge_kind"], "see_also");

        assert_eq!(incoming.len(), 1, "B has one incoming edge");
        assert_eq!(incoming[0]["from"]["id"], "A");
        assert_eq!(incoming[0]["to"]["id"], "B");
        assert_eq!(incoming[0]["edge_kind"], "relates_to");
    }

    #[tokio::test]
    async fn direction_out_only() {
        let (_dir, ctx) = create_test_context().await;

        LinksAdd
            .call(
                json!({
                    "from": { "type": "TASK", "id": "X" },
                    "to": { "type": "RECORD", "id": "R1" },
                    "edge_kind": "covers"
                }),
                &ctx,
            )
            .await;
        LinksAdd
            .call(
                json!({
                    "from": { "type": "EPISODE", "id": "E1" },
                    "to": { "type": "TASK", "id": "X" },
                    "edge_kind": "relates_to"
                }),
                &ctx,
            )
            .await;

        let result = LinksForEntity.execute(
            json!({
                "entity": { "type": "TASK", "id": "X" },
                "direction": "out"
            }),
            &ctx,
        );
        assert_ne!(result.is_error, Some(true));
        let parsed = parse_response(&result);

        assert_eq!(parsed["outgoing"].as_array().unwrap().len(), 1);
        assert_eq!(parsed["incoming"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn direction_in_only() {
        let (_dir, ctx) = create_test_context().await;

        LinksAdd
            .call(
                json!({
                    "from": { "type": "TASK", "id": "P" },
                    "to": { "type": "TASK", "id": "Q" },
                    "edge_kind": "relates_to"
                }),
                &ctx,
            )
            .await;

        let result = LinksForEntity.execute(
            json!({
                "entity": { "type": "TASK", "id": "Q" },
                "direction": "in"
            }),
            &ctx,
        );
        assert_ne!(result.is_error, Some(true));
        let parsed = parse_response(&result);

        assert_eq!(parsed["outgoing"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["incoming"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn both_direction_returns_empty_arrays_for_orphan() {
        let (_dir, ctx) = create_test_context().await;

        let result = LinksForEntity.execute(
            json!({ "entity": { "type": "TASK", "id": "orphan" } }),
            &ctx,
        );
        assert_ne!(result.is_error, Some(true));
        let parsed = parse_response(&result);

        assert_eq!(parsed["outgoing"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["incoming"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn unknown_entity_type_returns_error() {
        let (_dir, ctx) = create_test_context().await;

        let result =
            LinksForEntity.execute(json!({ "entity": { "type": "UNKNOWN", "id": "x" } }), &ctx);
        assert_eq!(result.is_error, Some(true));
    }
}
