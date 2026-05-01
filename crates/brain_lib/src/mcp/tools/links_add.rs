use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::links::{
    EntityRef, LinkError, add_link_checked, edge_kind_from_str, entity_type_from_str,
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

#[derive(Deserialize)]
struct EntityRefInput {
    #[serde(rename = "type")]
    entity_type: String,
    id: String,
}

fn resolve_entity_ref(input: EntityRefInput) -> Result<EntityRef, String> {
    let kind = entity_type_from_str(&input.entity_type)
        .ok_or_else(|| format!("unknown entity type: {}", input.entity_type))?;
    EntityRef::new(kind, input.id).map_err(|e| e.to_string())
}

pub(super) struct LinksAdd;

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

        let from_type = entity_type_str(from.kind);
        let from_id = from.id.clone();
        let to_type = entity_type_str(to.kind);
        let to_id = to.id.clone();

        let mut link_err: Option<LinkError> = None;
        let result = ctx.stores.inner_db().with_write_conn(|conn| {
            match add_link_checked(conn, from, to, edge_kind) {
                Ok(()) => Ok(()),
                Err(e) => {
                    let msg = e.to_string();
                    link_err = Some(e);
                    Err(brain_persistence::error::BrainCoreError::Database(msg))
                }
            }
        });

        match result {
            Ok(()) => {
                let id = format!("{from_type}:{from_id}->{edge_kind_wire}->{to_type}:{to_id}");
                json_response(&json!({ "id": id }))
            }
            Err(e) => match link_err {
                Some(LinkError::Cycle(_)) => {
                    ToolCallResult::error(format!("would create a cycle in {edge_kind_wire} graph"))
                }
                _ => ToolCallResult::error(format!("Internal error: {e}")),
            },
        }
    }
}

impl McpTool for LinksAdd {
    fn name(&self) -> &'static str {
        "links.add"
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
            description: "Add a directed polymorphic edge between two entities. Defaults to 'relates_to' when edge_kind is omitted. DAG kinds (parent_of, blocks, supersedes) are cycle-checked. Idempotent: re-adding an existing edge returns the same synthesised id without inserting a new row. The returned id is a deterministic compound key (FROM_TYPE:from_id->edge->TO_TYPE:to_id), not a durable ULID.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "from": entity_ref_schema.clone(),
                    "to": entity_ref_schema,
                    "edge_kind": {
                        "type": "string",
                        "enum": ["parent_of", "blocks", "covers", "relates_to", "see_also", "supersedes", "contradicts"],
                        "description": "Edge kind (default: relates_to)"
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
}
