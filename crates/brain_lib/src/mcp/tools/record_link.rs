use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use brain_persistence::db::links::{EdgeKind, EntityRef, EntityType};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::records::events::{LinkPayload, RecordEvent, RecordEventType};
use crate::uri::resolve_id;

use super::links_add::add_entity_link;
use super::links_remove::remove_entity_link;
use super::{McpTool, json_response};

// -- LinkAdd --

#[derive(Deserialize)]
struct LinkParams {
    record_id: String,
    task_id: Option<String>,
    chunk_id: Option<String>,
}

pub(super) struct RecordLinkAdd;

impl RecordLinkAdd {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: LinkParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if params.task_id.is_none() && params.chunk_id.is_none() {
            return ToolCallResult::error(
                "At least one of task_id or chunk_id must be provided".to_string(),
            );
        }

        let record_id_input = resolve_id(&params.record_id);
        let record_id = match ctx.stores.records.resolve_record_id(&record_id_input) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve record_id: {e}")),
        };

        match ctx.stores.records.get_record(&record_id) {
            Ok(Some(_)) => {}
            Ok(None) => return ToolCallResult::error(format!("Record not found: {record_id}")),
            Err(e) => return ToolCallResult::error(format!("Failed to get record: {e}")),
        }

        // Emit RecordEvent::LinkAdded so the projection dual-write fires.
        // This produces both record_links (legacy) + entity_links (polymorphic) rows.
        let event = RecordEvent::new(
            &record_id,
            "mcp",
            RecordEventType::LinkAdded,
            &LinkPayload {
                task_id: params.task_id.clone(),
                chunk_id: params.chunk_id.clone(),
            },
        );

        if let Err(e) = ctx.stores.records.apply_event(&event) {
            return ToolCallResult::error(format!("Failed to add link: {e}"));
        }

        // Also route through the generic polymorphic path for structural alignment.
        // The entity_links row produced here is a safe duplicate — INSERT OR IGNORE
        // in the projection layer silently skips it if already present.
        let to_entity = if let Some(ref task_id) = params.task_id {
            EntityRef {
                kind: EntityType::Task,
                id: task_id.clone(),
            }
        } else {
            EntityRef {
                kind: EntityType::Chunk,
                id: params.chunk_id.clone().unwrap(),
            }
        };
        let from_entity = EntityRef {
            kind: EntityType::Record,
            id: record_id.clone(),
        };
        let _ = add_entity_link(from_entity, to_entity, EdgeKind::Covers, "covers", ctx);

        let compact_id = ctx
            .stores
            .records
            .compact_record_id(&record_id)
            .unwrap_or(record_id.clone());

        json_response(&json!({
            "record_id": compact_id,
            "task_id": params.task_id,
            "chunk_id": params.chunk_id,
            "action": "linked",
            "deprecated": true,
            "deprecation_message": "records.link_add is deprecated; prefer links.add with from_type=RECORD, to_type=TASK|CHUNK, edge_kind=covers",
        }))
    }
}

impl McpTool for RecordLinkAdd {
    fn name(&self) -> &'static str {
        "records.link_add"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Add a link from a record to a task or note chunk. At least one of task_id or chunk_id must be provided. Idempotent — duplicate links are deduplicated by the projection.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to link from (full ID or unique prefix)"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Task ID to link to (optional if chunk_id is provided)"
                    },
                    "chunk_id": {
                        "type": "string",
                        "description": "Note chunk ID to link to (optional if task_id is provided)"
                    }
                },
                "required": ["record_id"]
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

// -- LinkRemove --

pub(super) struct RecordLinkRemove;

impl RecordLinkRemove {
    fn execute(&self, params: Value, ctx: &McpContext) -> ToolCallResult {
        let params: LinkParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
        };

        if params.task_id.is_none() && params.chunk_id.is_none() {
            return ToolCallResult::error(
                "At least one of task_id or chunk_id must be provided".to_string(),
            );
        }

        let record_id_input = resolve_id(&params.record_id);
        let record_id = match ctx.stores.records.resolve_record_id(&record_id_input) {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to resolve record_id: {e}")),
        };

        match ctx.stores.records.get_record(&record_id) {
            Ok(Some(_)) => {}
            Ok(None) => return ToolCallResult::error(format!("Record not found: {record_id}")),
            Err(e) => return ToolCallResult::error(format!("Failed to get record: {e}")),
        }

        // Emit RecordEvent::LinkRemoved so the projection dual-write fires.
        // This removes from both record_links (legacy) and entity_links (polymorphic).
        let event = RecordEvent::new(
            &record_id,
            "mcp",
            RecordEventType::LinkRemoved,
            &LinkPayload {
                task_id: params.task_id.clone(),
                chunk_id: params.chunk_id.clone(),
            },
        );

        if let Err(e) = ctx.stores.records.apply_event(&event) {
            return ToolCallResult::error(format!("Failed to remove link: {e}"));
        }

        // Also route through the generic polymorphic path for structural alignment.
        // If the entity_links row was already removed by the projection, this is a no-op.
        let to_entity = if let Some(ref task_id) = params.task_id {
            EntityRef {
                kind: EntityType::Task,
                id: task_id.clone(),
            }
        } else {
            EntityRef {
                kind: EntityType::Chunk,
                id: params.chunk_id.clone().unwrap(),
            }
        };
        let from_entity = EntityRef {
            kind: EntityType::Record,
            id: record_id.clone(),
        };
        let _ = remove_entity_link(from_entity, to_entity, EdgeKind::Covers, ctx);

        let compact_id = ctx
            .stores
            .records
            .compact_record_id(&record_id)
            .unwrap_or(record_id.clone());

        json_response(&json!({
            "record_id": compact_id,
            "task_id": params.task_id,
            "chunk_id": params.chunk_id,
            "action": "unlinked",
            "deprecated": true,
            "deprecation_message": "records.link_remove is deprecated; prefer links.remove with from_type=RECORD, to_type=TASK|CHUNK, edge_kind=covers",
        }))
    }
}

impl McpTool for RecordLinkRemove {
    fn name(&self) -> &'static str {
        "records.link_remove"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Remove a link from a record to a task or note chunk. At least one of task_id or chunk_id must be provided. Idempotent — removing a link that doesn't exist has no effect.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "record_id": {
                        "type": "string",
                        "description": "The record ID to unlink from (full ID or unique prefix)"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Task ID to unlink (optional if chunk_id is provided)"
                    },
                    "chunk_id": {
                        "type": "string",
                        "description": "Note chunk ID to unlink (optional if task_id is provided)"
                    }
                },
                "required": ["record_id"]
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

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;
    use super::{McpTool, RecordLinkAdd, RecordLinkRemove};

    // Schema stability: the JSON Schema for records.link_add must not change
    // without a deliberate breaking-change decision.
    #[test]
    fn test_records_link_add_schema_stable() {
        let tool = RecordLinkAdd;
        let schema = tool.definition().input_schema;
        let expected = json!({
            "type": "object",
            "properties": {
                "record_id": {
                    "type": "string",
                    "description": "The record ID to link from (full ID or unique prefix)"
                },
                "task_id": {
                    "type": "string",
                    "description": "Task ID to link to (optional if chunk_id is provided)"
                },
                "chunk_id": {
                    "type": "string",
                    "description": "Note chunk ID to link to (optional if task_id is provided)"
                }
            },
            "required": ["record_id"]
        });
        assert_eq!(
            schema, expected,
            "records.link_add schema changed — update golden or revert"
        );
    }

    // Schema stability: the JSON Schema for records.link_remove must not change.
    #[test]
    fn test_records_link_remove_schema_stable() {
        let tool = RecordLinkRemove;
        let schema = tool.definition().input_schema;
        let expected = json!({
            "type": "object",
            "properties": {
                "record_id": {
                    "type": "string",
                    "description": "The record ID to unlink from (full ID or unique prefix)"
                },
                "task_id": {
                    "type": "string",
                    "description": "Task ID to unlink (optional if chunk_id is provided)"
                },
                "chunk_id": {
                    "type": "string",
                    "description": "Note chunk ID to unlink (optional if task_id is provided)"
                }
            },
            "required": ["record_id"]
        });
        assert_eq!(
            schema, expected,
            "records.link_remove schema changed — update golden or revert"
        );
    }

    // Round-trip: add_link → verify via entity_links reader → remove → verify empty.
    #[tokio::test]
    async fn test_record_link_round_trip() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Create a record first.
        let create_result = registry
            .dispatch(
                "records.create_document",
                json!({
                    "title": "Link Round-Trip Test",
                    "text": "body"
                }),
                &ctx,
            )
            .await;
        assert_ne!(
            create_result.is_error,
            Some(true),
            "create_document failed: {create_result:?}"
        );

        let created: serde_json::Value =
            serde_json::from_str(&create_result.content[0].text).unwrap();
        let record_id = created["record_id"].as_str().unwrap().to_string();

        // Add a link.
        let add_result = registry
            .dispatch(
                "records.link_add",
                json!({
                    "record_id": record_id,
                    "task_id": "TST-01FAKE000000000000000000"
                }),
                &ctx,
            )
            .await;
        assert_ne!(
            add_result.is_error,
            Some(true),
            "link_add failed: {add_result:?}"
        );

        let add_body: serde_json::Value =
            serde_json::from_str(&add_result.content[0].text).unwrap();
        assert_eq!(add_body["action"].as_str(), Some("linked"));
        assert_eq!(
            add_body["task_id"].as_str(),
            Some("TST-01FAKE000000000000000000")
        );

        // Verify link exists via entity_links reader (Wave 5 cutover path).
        let links = ctx
            .stores
            .db_for_tests()
            .with_read_conn(|conn| {
                brain_persistence::db::records::queries::get_record_links(conn, &record_id)
            })
            .expect("get_record_links should succeed after add");
        assert_eq!(links.len(), 1, "exactly one link expected after add");
        assert_eq!(
            links[0].task_id.as_deref(),
            Some("TST-01FAKE000000000000000000")
        );

        // Remove the link.
        let remove_result = registry
            .dispatch(
                "records.link_remove",
                json!({
                    "record_id": record_id,
                    "task_id": "TST-01FAKE000000000000000000"
                }),
                &ctx,
            )
            .await;
        assert_ne!(
            remove_result.is_error,
            Some(true),
            "link_remove failed: {remove_result:?}"
        );

        let remove_body: serde_json::Value =
            serde_json::from_str(&remove_result.content[0].text).unwrap();
        assert_eq!(remove_body["action"].as_str(), Some("unlinked"));

        // Verify link is gone via entity_links reader.
        let links_after = ctx
            .stores
            .db_for_tests()
            .with_read_conn(|conn| {
                brain_persistence::db::records::queries::get_record_links(conn, &record_id)
            })
            .expect("get_record_links should succeed after remove");
        assert!(links_after.is_empty(), "links must be empty after remove");
    }

    // New: assert the response JSON includes `deprecated: true`.
    #[tokio::test]
    async fn test_records_link_add_response_includes_deprecated_field() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let create_result = registry
            .dispatch(
                "records.create_document",
                json!({ "title": "Deprecated Test", "text": "body" }),
                &ctx,
            )
            .await;
        let created: serde_json::Value =
            serde_json::from_str(&create_result.content[0].text).unwrap();
        let record_id = created["record_id"].as_str().unwrap().to_string();

        let result = registry
            .dispatch(
                "records.link_add",
                json!({
                    "record_id": record_id,
                    "task_id": "TST-01FAKE000000000000000001"
                }),
                &ctx,
            )
            .await;
        assert_ne!(result.is_error, Some(true));

        let body: serde_json::Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(
            body["deprecated"].as_bool(),
            Some(true),
            "response must include deprecated: true"
        );
        assert!(
            body["deprecation_message"].is_string(),
            "response must include deprecation_message"
        );
    }

    // New: assert the legacy record_links row is still produced (dual-write defensive).
    #[tokio::test]
    async fn test_records_link_add_still_writes_record_links_for_dual_write() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let create_result = registry
            .dispatch(
                "records.create_document",
                json!({ "title": "Dual-Write Test", "text": "body" }),
                &ctx,
            )
            .await;
        let created: serde_json::Value =
            serde_json::from_str(&create_result.content[0].text).unwrap();
        let record_id = created["record_id"].as_str().unwrap().to_string();

        let task_id = "TST-01DUAL000000000000000002";
        let add_result = registry
            .dispatch(
                "records.link_add",
                json!({ "record_id": record_id, "task_id": task_id }),
                &ctx,
            )
            .await;
        assert_ne!(add_result.is_error, Some(true));

        // Verify legacy record_links row exists via the approved persistence API.
        let links = ctx
            .stores
            .db_for_tests()
            .with_read_conn(|conn| {
                brain_persistence::db::records::queries::get_record_links(conn, &record_id)
            })
            .expect("get_record_links should succeed");
        assert_eq!(
            links.len(),
            1,
            "legacy record_links row must exist for dual-write"
        );
        assert_eq!(
            links[0].task_id.as_deref(),
            Some(task_id),
            "record_links task_id must match"
        );

        // Verify entity_links row exists via the approved persistence API.
        let entity_links = ctx
            .stores
            .db_for_tests()
            .with_read_conn(|conn| {
                brain_persistence::db::links::for_entity(
                    conn,
                    brain_persistence::db::links::EntityRef {
                        kind: brain_persistence::db::links::EntityType::Record,
                        id: record_id.clone(),
                    },
                )
                .map_err(|e| brain_persistence::error::BrainCoreError::Database(e.to_string()))
            })
            .expect("for_entity should succeed");
        let covers_edge = entity_links.iter().find(|l| {
            l.to.id == task_id
                && matches!(l.edge_kind, brain_persistence::db::links::EdgeKind::Covers)
        });
        assert!(
            covers_edge.is_some(),
            "entity_links Covers edge must exist for polymorphic graph"
        );
    }

    // New: dispatch records.link_add and links.add with equivalent payloads,
    // assert entity_links row tuple is identical.
    #[tokio::test]
    async fn test_records_link_add_routes_through_generic_path_produces_equivalent_entity_links_row()
     {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Create two records for two separate link operations.
        let create_r1 = registry
            .dispatch(
                "records.create_document",
                json!({ "title": "Shim Record", "text": "body" }),
                &ctx,
            )
            .await;
        let r1: serde_json::Value = serde_json::from_str(&create_r1.content[0].text).unwrap();
        let record_id_1 = r1["record_id"].as_str().unwrap().to_string();

        let create_r2 = registry
            .dispatch(
                "records.create_document",
                json!({ "title": "Generic Record", "text": "body" }),
                &ctx,
            )
            .await;
        let r2: serde_json::Value = serde_json::from_str(&create_r2.content[0].text).unwrap();
        let record_id_2 = r2["record_id"].as_str().unwrap().to_string();

        let task_id_1 = "TST-01SHIM000000000000000003";
        let task_id_2 = "TST-01GENR000000000000000004";

        // Use records.link_add (shim path) for record 1.
        let shim_result = registry
            .dispatch(
                "records.link_add",
                json!({ "record_id": record_id_1, "task_id": task_id_1 }),
                &ctx,
            )
            .await;
        assert_ne!(shim_result.is_error, Some(true), "shim path failed");

        // Use links.add (generic path) for record 2.
        let generic_result = registry
            .dispatch(
                "links.add",
                json!({
                    "from": { "type": "RECORD", "id": record_id_2 },
                    "to": { "type": "TASK", "id": task_id_2 },
                    "edge_kind": "covers"
                }),
                &ctx,
            )
            .await;
        assert_ne!(generic_result.is_error, Some(true), "generic path failed");

        // Fetch entity_links for both records via the approved persistence API.
        let shim_links = ctx
            .stores
            .db_for_tests()
            .with_read_conn(|conn| {
                brain_persistence::db::links::for_entity(
                    conn,
                    brain_persistence::db::links::EntityRef {
                        kind: brain_persistence::db::links::EntityType::Record,
                        id: record_id_1.clone(),
                    },
                )
                .map_err(|e| brain_persistence::error::BrainCoreError::Database(e.to_string()))
            })
            .expect("for_entity (shim) should succeed");

        let generic_links = ctx
            .stores
            .db_for_tests()
            .with_read_conn(|conn| {
                brain_persistence::db::links::for_entity(
                    conn,
                    brain_persistence::db::links::EntityRef {
                        kind: brain_persistence::db::links::EntityType::Record,
                        id: record_id_2.clone(),
                    },
                )
                .map_err(|e| brain_persistence::error::BrainCoreError::Database(e.to_string()))
            })
            .expect("for_entity (generic) should succeed");

        let shim_edge = shim_links
            .iter()
            .find(|l| l.to.id == task_id_1)
            .expect("shim entity_links edge must exist");
        let generic_edge = generic_links
            .iter()
            .find(|l| l.to.id == task_id_2)
            .expect("generic entity_links edge must exist");

        // from_type, to_type, edge_kind must be structurally identical.
        assert_eq!(
            shim_edge.from.kind, generic_edge.from.kind,
            "from entity_type must match"
        );
        assert_eq!(
            shim_edge.to.kind, generic_edge.to.kind,
            "to entity_type must match"
        );
        assert_eq!(
            shim_edge.edge_kind, generic_edge.edge_kind,
            "edge_kind must match"
        );
    }
}
