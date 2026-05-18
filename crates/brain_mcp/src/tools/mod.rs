//! MCP tool handlers — thin clients of `brain_rpc::DaemonClient`.
//!
//! Each tool implements [`McpTool`] and is registered in
//! [`ToolRegistry::new`]. Tool bodies parse params, dispatch a typed
//! RPC call via [`crate::context::McpContext::with_client`], and
//! shape the response into a JSON envelope. No store access; no
//! storage- or search-owning resources.

use std::future::Future;
use std::pin::Pin;

use serde_json::Value;

use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub mod helpers;

mod brains_list;
mod jobs_status;
mod links_add;
mod links_for_entity;
mod links_remove;
mod mem_consolidate;
mod mem_reflect;
mod mem_retrieve;
mod mem_summarize_scope;
mod mem_walk_thread;
mod mem_write_episode;
mod mem_write_procedure;
mod record_archive;
mod record_create_analysis;
mod record_create_document;
mod record_create_plan;
mod record_fetch_content;
mod record_get;
mod record_link;
mod record_list;
mod record_save_snapshot;
mod record_search;
mod record_tag;
mod saga_add_tasks;
mod saga_cancel;
mod saga_close;
mod saga_create;
mod saga_frontier;
mod saga_get;
mod saga_list;
mod saga_remove_tasks;
mod saga_reopen;
mod saga_start;
mod saga_stats;
mod saga_update;
mod status;
mod tags_aliases_list;
mod tags_aliases_status;
mod tags_recluster;
mod task_apply_event;
mod task_close;
mod task_create;
mod task_deps_batch;
mod task_get;
mod task_labels_batch;
mod task_labels_summary;
mod task_list;
mod task_next;
mod task_transfer;

pub use helpers::{
    Warning, cascade_results_to_json, entity_ref_schema, inject_warnings, json_response,
    store_or_warn,
};

/// Trait for MCP tool handlers. Each tool provides its name, JSON Schema
/// definition, and an async `call` method that executes the tool logic.
pub trait McpTool: Send + Sync {
    /// Tool name as it appears in MCP (e.g. `"brains.list"`).
    fn name(&self) -> &'static str;

    /// Underscore alias for the tool name (e.g. `"brains_list"`).
    ///
    /// Derived automatically from [`McpTool::name`] by replacing `.`
    /// with `_`. Tools that have no `.` in their name return the same
    /// value as `name()`.
    fn underscore_alias(&self) -> String {
        self.name().replace('.', "_")
    }

    /// Tool definition including JSON Schema for input parameters.
    fn definition(&self) -> ToolDefinition;

    /// Execute the tool with the given parameters.
    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>>;
}

/// Registry of all available MCP tools.
pub struct ToolRegistry {
    tools: Vec<Box<dyn McpTool>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: vec![
                Box::new(brains_list::BrainsList),
                Box::new(jobs_status::JobsStatus),
                Box::new(links_add::LinksAdd),
                Box::new(links_for_entity::LinksForEntity),
                Box::new(links_remove::LinksRemove),
                Box::new(mem_consolidate::MemoryConsolidate),
                Box::new(mem_reflect::MemoryReflect),
                Box::new(mem_retrieve::MemoryRetrieve),
                Box::new(mem_summarize_scope::MemorySummarizeScope),
                Box::new(mem_walk_thread::MemoryWalkThread),
                Box::new(mem_write_episode::MemoryWriteEpisode),
                Box::new(mem_write_procedure::MemoryWriteProcedure),
                Box::new(record_archive::RecordArchive),
                Box::new(record_create_analysis::RecordCreateAnalysis),
                Box::new(record_create_document::RecordCreateDocument),
                Box::new(record_create_plan::RecordCreatePlan),
                Box::new(record_fetch_content::RecordFetchContent),
                Box::new(record_get::RecordGet),
                Box::new(record_link::RecordsLinkAdd),
                Box::new(record_link::RecordsLinkRemove),
                Box::new(record_list::RecordList),
                Box::new(record_save_snapshot::RecordSaveSnapshot),
                Box::new(record_search::RecordSearch),
                Box::new(record_tag::RecordTagAdd),
                Box::new(record_tag::RecordTagRemove),
                Box::new(saga_add_tasks::SagaAddTasks),
                Box::new(saga_cancel::SagaCancel),
                Box::new(saga_close::SagaClose),
                Box::new(saga_create::SagaCreate),
                Box::new(saga_frontier::SagaFrontier),
                Box::new(saga_get::SagaGet),
                Box::new(saga_list::SagaList),
                Box::new(saga_remove_tasks::SagaRemoveTasks),
                Box::new(saga_reopen::SagaReopen),
                Box::new(saga_start::SagaStart),
                Box::new(saga_stats::SagaStats),
                Box::new(saga_update::SagaUpdate),
                Box::new(status::Status),
                Box::new(tags_aliases_list::TagsAliasesList),
                Box::new(tags_aliases_status::TagsAliasesStatus),
                Box::new(tags_recluster::TagsRecluster),
                Box::new(task_apply_event::TaskApplyEvent),
                Box::new(task_close::TaskClose),
                Box::new(task_create::TaskCreate),
                Box::new(task_deps_batch::TaskDepsBatch),
                Box::new(task_get::TaskGet),
                Box::new(task_labels_batch::TaskLabelsBatch),
                Box::new(task_labels_summary::TaskLabelsSummary),
                Box::new(task_list::TaskList),
                Box::new(task_next::TaskNext),
                Box::new(task_transfer::TaskTransfer),
            ],
        }
    }

    /// All registered tool definitions (used by `tools/list`).
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools.iter().map(|t| t.definition()).collect()
    }

    /// Dispatch a `tools/call` invocation by name or underscore alias.
    ///
    /// Returns `ToolCallResult::error` with an "Unknown tool" message
    /// if no registered tool matches. Tools that are not yet migrated
    /// to `brain_mcp` fall through this path until their batch lands.
    pub async fn dispatch(&self, name: &str, params: Value, ctx: &McpContext) -> ToolCallResult {
        for tool in &self.tools {
            if tool.name() == name || tool.underscore_alias() == name {
                return tool.call(params, ctx).await;
            }
        }
        ToolCallResult::error(format!(
            "Unknown tool '{name}' (not yet migrated to brain_mcp)"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        "records.fetch_content",
        "records.get",
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

    #[test]
    fn registry_registers_expected_tools() {
        use std::collections::HashSet;

        let registry = ToolRegistry::new();
        let defs = registry.definitions();
        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();

        // Duplicates would shadow each other in dispatch — fail loudly
        // rather than silently overriding.
        let name_set: HashSet<&str> = names.iter().copied().collect();
        assert_eq!(
            names.len(),
            name_set.len(),
            "registry has duplicate tool names; have {names:?}"
        );

        // Expected and actual sets must match exactly — extras flag a
        // forgotten EXPECTED_TOOL_NAMES update, missing tools flag a
        // missing registration.
        let expected_set: HashSet<&str> = EXPECTED_TOOL_NAMES.iter().copied().collect();
        assert_eq!(
            name_set, expected_set,
            "registered tools do not match EXPECTED_TOOL_NAMES; \
             registered={name_set:?} expected={expected_set:?}"
        );
    }

    #[test]
    fn underscore_aliases_derived() {
        let registry = ToolRegistry::new();
        let aliases: Vec<String> = registry
            .tools
            .iter()
            .map(|t| t.underscore_alias())
            .collect();
        for expected_dotted in EXPECTED_TOOL_NAMES {
            let alias = expected_dotted.replace('.', "_");
            assert!(
                aliases.iter().any(|a| a == &alias),
                "registry missing alias {alias}"
            );
        }
    }

    #[test]
    fn all_definitions_have_valid_schema() {
        let registry = ToolRegistry::new();
        for def in registry.definitions() {
            assert!(def.input_schema.is_object());
            assert_eq!(
                def.input_schema.get("type").and_then(|v| v.as_str()),
                Some("object"),
                "tool '{}' input_schema.type must be 'object'",
                def.name
            );
        }
    }
}
