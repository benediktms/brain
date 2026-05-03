use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::links::{
    EdgeKind, EntityRef, LinkError, edge_kind_from_str, remove_link,
};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};
use super::links_add::{EntityRefInput, resolve_entity_ref, entity_ref_schema};

#[derive(Deserialize)]
struct LinksRemoveParams {
    from: EntityRefInput,
    to: EntityRefInput,
    edge_kind: String,
}

pub(super) struct LinksRemove;

/// Shared remove-link logic callable from both the polymorphic surface and the records shim.
pub(super) fn remove_entity_link(
    from: EntityRef,
    to: EntityRef,
    edge_kind: EdgeKind,
    ctx: &McpContext,
) -> ToolCallResult {
    let result = ctx.stores.inner_db().with_write_conn(|conn| {
        remove_link(conn, from, to, edge_kind).map_err(|e| match e {
            LinkError::Database(msg) => brain_persistence::error::BrainCoreError::Database(msg),
            LinkError::Cycle(_) => unreachable!("remove_link never returns Cycle"),
        })
    });

    match result {
        Ok(removed) => json_response(&json!({ "removed": removed })),
        Err(e) => ToolCallResult::error(format!("Internal error: {e}")),
    }
}

impl LinksRemove {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: LinksRemoveParams = match serde_json::from_value(params) {
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

        let edge_kind = match edge_kind_from_str(&params.edge_kind) {
            Some(k) => k,
            None => {
                return ToolCallResult::error(format!("unknown edge_kind: {}", params.edge_kind));
            }
        };

        remove_entity_link(from, to, edge_kind, ctx)
    }
}

impl McpTool for LinksRemove {
    fn name(&self) -> &'static str {
        "links.remove"
    }

    fn definition(&self) -> ToolDefinition {
        let entity_ref_schema = entity_ref_schema();

        ToolDefinition {
            name: self.name().into(),
            description: "Remove a directed polymorphic edge between two entities. Returns { removed: true } when the edge existed and was deleted, { removed: false } when no matching edge was found (idempotent).".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "from": entity_ref_schema.clone(),
                    "to": entity_ref_schema,
                    "edge_kind": {
                        "type": "string",
                        "enum": ["parent_of", "blocks", "covers", "relates_to", "see_also", "supersedes", "contradicts"],
                        "description": "Edge kind to remove"
                    }
                },
                "required": ["from", "to", "edge_kind"]
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

    fn text_of(result: &ToolCallResult) -> &str {
        &result.content.first().unwrap().text
    }

    #[tokio::test]
    async fn happy_path_removes_existing_edge() {
        let (_dir, ctx) = create_test_context().await;

        // Add edge first via the trait's call method
        LinksAdd
            .call(
                json!({
                    "from": { "type": "TASK", "id": "t1" },
                    "to": { "type": "TASK", "id": "t2" },
                    "edge_kind": "relates_to"
                }),
                &ctx,
            )
            .await;

        let result = LinksRemove.execute(
            json!({
                "from": { "type": "TASK", "id": "t1" },
                "to": { "type": "TASK", "id": "t2" },
                "edge_kind": "relates_to"
            }),
            &ctx,
        );

        assert_ne!(result.is_error, Some(true));
        let parsed: serde_json::Value = serde_json::from_str(text_of(&result)).unwrap();
        assert_eq!(parsed["removed"], true);
    }

    #[tokio::test]
    async fn non_existent_edge_returns_removed_false() {
        let (_dir, ctx) = create_test_context().await;

        let result = LinksRemove.execute(
            json!({
                "from": { "type": "TASK", "id": "ghost" },
                "to": { "type": "RECORD", "id": "nowhere" },
                "edge_kind": "blocks"
            }),
            &ctx,
        );

        assert_ne!(result.is_error, Some(true));
        let parsed: serde_json::Value = serde_json::from_str(text_of(&result)).unwrap();
        assert_eq!(parsed["removed"], false);
    }

    #[tokio::test]
    async fn unknown_edge_kind_returns_error() {
        let (_dir, ctx) = create_test_context().await;

        let result = LinksRemove.execute(
            json!({
                "from": { "type": "TASK", "id": "x" },
                "to": { "type": "TASK", "id": "y" },
                "edge_kind": "nonexistent_kind"
            }),
            &ctx,
        );

        assert_eq!(result.is_error, Some(true));
        assert!(text_of(&result).contains("unknown edge_kind"));
    }
}
