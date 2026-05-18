//! MCP tool handlers — thin clients of `brain_rpc::DaemonClient`.
//!
//! Each tool implements [`McpTool`] and is registered in
//! [`ToolRegistry::new`]. Tool bodies parse params, dispatch a typed
//! RPC call via [`crate::context::McpContext::with_client`], and
//! shape the response into a JSON envelope. No store access; no
//! storage- or search-owning resources.
//!
//! ## Migration status
//!
//! Phase D batch 1 lands two tools:
//! - `brains.list`
//! - `links.remove`
//!
//! `jobs.status` was attempted in this batch but deferred: the legacy
//! response shape includes `status` and `started_at` per-job fields
//! that the current `brain_rpc::JobSummary` wire type does not carry.
//! Migrating it requires first extending the wire (Phase A-style
//! addition) — a follow-up batch lands both together.
//!
//! The remaining 49 tools migrate in follow-up batches: memory (7),
//! tasks (10), records (11), sagas (12), tags (3), `status`,
//! `links.add`, `links.for_entity`, `jobs.status`.

use std::future::Future;
use std::pin::Pin;

use serde_json::Value;

use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub mod helpers;

mod brains_list;
mod links_remove;

pub use helpers::{
    Warning, cascade_results_to_json, inject_warnings, json_response, store_or_warn,
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
                Box::new(links_remove::LinksRemove),
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
            "Unknown tool '{name}' (not yet migrated to brain_mcp; the remaining 49 tools land in follow-up batches)"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_two_tools() {
        let registry = ToolRegistry::new();
        let defs = registry.definitions();
        assert_eq!(defs.len(), 2);
        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"brains.list"));
        assert!(names.contains(&"links.remove"));
    }

    #[test]
    fn underscore_aliases_derived() {
        let registry = ToolRegistry::new();
        let aliases: Vec<String> = registry
            .tools
            .iter()
            .map(|t| t.underscore_alias())
            .collect();
        assert!(aliases.iter().any(|a| a == "brains_list"));
        assert!(aliases.iter().any(|a| a == "links_remove"));
    }

    #[test]
    fn all_definitions_have_valid_schema() {
        let registry = ToolRegistry::new();
        for def in registry.definitions() {
            assert!(def.input_schema.is_object());
            assert!(def.input_schema.get("type").is_some());
        }
    }
}
