//! Smoke tests for `ToolRegistry` — registry resolution and schema shape.
//!
//! `test_registry_resolves_all_expected_tools` requires a live daemon
//! (spawned over a temp-dir Unix socket). `test_all_definitions_have_object_schema`
//! is pure introspection and does not need a daemon.

#![cfg(unix)]

mod common;

use brain_mcp::ToolRegistry;
use serde_json::json;

/// Tool names reproduced from the `cfg(test)` constant in
/// `crates/brain_mcp/src/tools/mod.rs`. Integration tests cannot
/// reference that constant directly (it lives inside a `#[cfg(test)]`
/// block), so we maintain the list here. Divergence between the two
/// lists will be caught by the unit test `registry_registers_expected_tools`
/// in `tools/mod.rs` and by CI.
const EXPECTED_TOOL_NAMES: &[&str] = &[
    "brains.list",
    "jobs.status",
    "links.add",
    "links.for_entity",
    "links.remove",
    "memory.consolidate",
    "memory.reflect",
    "memory.retrieve",
    "memory.summarize_scope",
    "memory.walk_thread",
    "memory.write_episode",
    "memory.write_procedure",
    "records.archive",
    "records.create_analysis",
    "records.create_document",
    "records.create_plan",
    "records.get",
    "records.fetch_content",
    "records.link_add",
    "records.link_remove",
    "records.list",
    "records.save_snapshot",
    "records.search",
    "records.tag_add",
    "records.tag_remove",
    "sagas.add_tasks",
    "sagas.cancel",
    "sagas.close",
    "sagas.create",
    "sagas.frontier",
    "sagas.get",
    "sagas.list",
    "sagas.remove_tasks",
    "sagas.reopen",
    "sagas.start",
    "sagas.stats",
    "sagas.update",
    "status",
    "tags.aliases_list",
    "tags.aliases_status",
    "tags.recluster",
    "tasks.apply_event",
    "tasks.close",
    "tasks.create",
    "tasks.deps_batch",
    "tasks.get",
    "tasks.labels_batch",
    "tasks.labels_summary",
    "tasks.list",
    "tasks.next",
    "tasks.transfer",
];

/// Verify that every expected tool name resolves in the registry.
///
/// We dispatch each tool with empty params and assert that the response is
/// NOT the "Unknown tool" sentinel. Tools will typically return an error
/// result due to missing required params — that is expected and acceptable.
/// What this test verifies is registry membership, not tool correctness.
#[tokio::test]
async fn test_registry_resolves_all_expected_tools() {
    let (_tmp, sock_path, _guard) = common::spawn_daemon();
    let ctx = common::connect_mcp_context(&sock_path).await;
    let registry = ToolRegistry::new();

    for name in EXPECTED_TOOL_NAMES {
        let result = common::dispatch(&registry, &ctx, name, json!({})).await;
        // A legitimate tool call (even one that fails due to bad params)
        // returns content that does NOT start with "Unknown tool".
        assert!(
            !result.content.is_empty(),
            "tool '{name}' dispatch returned empty content"
        );
        let first_text = result
            .content
            .first()
            .map(|c| c.text.as_str())
            .unwrap_or("");
        assert!(
            !first_text.starts_with("Unknown tool"),
            "tool '{name}' not found in registry — got: {first_text:?}"
        );
    }
}

/// Verify that every registered tool definition has a well-formed JSON
/// Schema with `"type": "object"` and a `"properties"` key.
///
/// No daemon is needed — this is pure registry introspection.
#[test]
fn test_all_definitions_have_object_schema() {
    let registry = ToolRegistry::new();
    for def in registry.definitions() {
        let schema = &def.input_schema;
        assert!(
            schema.is_object(),
            "tool '{}' input_schema is not a JSON object: {schema}",
            def.name
        );
        assert_eq!(
            schema.get("type").and_then(|v| v.as_str()),
            Some("object"),
            "tool '{}' input_schema missing or wrong 'type' field: {schema}",
            def.name
        );
        assert!(
            schema.get("properties").is_some(),
            "tool '{}' input_schema missing 'properties' key: {schema}",
            def.name
        );
    }
}
