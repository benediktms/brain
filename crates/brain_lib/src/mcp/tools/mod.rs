mod brains_list;
mod helpers;
/// MCP tool definitions and handlers.
mod mem_expand;
mod mem_reflect;
mod mem_search_minimal;
mod mem_write_episode;
mod record_archive;
mod record_create_artifact;
mod record_fetch_content;
mod record_get;
mod record_link;
mod record_list;
mod record_save_snapshot;
mod record_tag;
mod status;
mod task_apply_event;
mod task_close;
mod task_create;
mod task_deps_batch;
mod task_get;
mod task_labels_batch;
mod task_labels_summary;
mod task_list;
mod task_next;

pub use helpers::*;

use std::future::Future;
use std::pin::Pin;

use serde_json::Value;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

pub(super) const MEMORY_UNAVAILABLE: &str = "Memory tools are unavailable: embedding model not found.\n\
To download the model, either run the setup script:\n  \
curl -sSL https://raw.githubusercontent.com/benediktms/brain/master/scripts/setup-model.sh | bash\n\
Or install the HuggingFace CLI manually:\n  \
pip install huggingface_hub\n  \
hf download BAAI/bge-small-en-v1.5 config.json tokenizer.json model.safetensors --local-dir ~/.brain/models/bge-small-en-v1.5";

/// Trait for MCP tool handlers. Each tool provides its name, JSON Schema
/// definition, and an async `call` method that executes the tool logic.
pub trait McpTool: Send + Sync {
    /// Tool name as it appears in MCP (e.g. "tasks.apply_event").
    fn name(&self) -> &'static str;

    /// Underscore alias for the tool name (e.g. "tasks_apply_event").
    ///
    /// Derived automatically from [`name`] by replacing `.` with `_`.
    /// Tools that have no `.` in their name return the same value as `name()`.
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
                Box::new(mem_search_minimal::MemSearchMinimal),
                Box::new(mem_expand::MemExpand),
                Box::new(mem_write_episode::MemWriteEpisode),
                Box::new(mem_reflect::MemReflect),
                Box::new(task_apply_event::TaskApplyEvent),
                Box::new(task_close::TaskClose),
                Box::new(task_create::TaskCreate),
                Box::new(task_deps_batch::TaskDepsBatch),
                Box::new(task_get::TaskGet),
                Box::new(task_labels_batch::TaskLabelsBatch),
                Box::new(task_labels_summary::TaskLabelsSummary),
                Box::new(task_list::TaskList),
                Box::new(task_next::TaskNext),
                Box::new(status::Status),
                Box::new(record_create_artifact::RecordCreateArtifact),
                Box::new(record_save_snapshot::RecordSaveSnapshot),
                Box::new(record_get::RecordGet),
                Box::new(record_list::RecordList),
                Box::new(record_fetch_content::RecordFetchContent),
                Box::new(record_archive::RecordArchive),
                Box::new(record_tag::RecordTagAdd),
                Box::new(record_tag::RecordTagRemove),
                Box::new(record_link::RecordLinkAdd),
                Box::new(record_link::RecordLinkRemove),
                Box::new(brains_list::BrainsList),
            ],
        }
    }

    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools.iter().map(|t| t.definition()).collect()
    }

    pub async fn dispatch(&self, name: &str, params: Value, ctx: &McpContext) -> ToolCallResult {
        for tool in &self.tools {
            if tool.name() == name || tool.underscore_alias() == name {
                return tool.call(params, ctx).await;
            }
        }

        ToolCallResult::error(format!("Unknown tool: {name}"))
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;

    #[test]
    fn test_tool_definitions_valid() {
        let registry = ToolRegistry::new();
        let defs = registry.definitions();
        assert_eq!(defs.len(), 25);

        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"memory.search_minimal"));
        assert!(names.contains(&"memory.expand"));
        assert!(names.contains(&"memory.write_episode"));
        assert!(names.contains(&"memory.reflect"));
        assert!(names.contains(&"tasks.apply_event"));
        assert!(names.contains(&"tasks.create"));
        assert!(names.contains(&"tasks.get"));
        assert!(names.contains(&"tasks.list"));
        assert!(names.contains(&"tasks.next"));
        assert!(!names.contains(&"tasks.create_remote"));

        // All should have valid JSON schemas
        for def in &defs {
            assert!(def.input_schema.is_object());
            assert!(def.input_schema.get("type").is_some());
        }
    }

    #[test]
    fn test_underscore_aliases_derived_from_tool_names() {
        // Aliases are derived from each tool's canonical dot-notation name.
        // Verify a sample of tools produce the expected underscore aliases.
        let registry = ToolRegistry::new();
        let aliases: Vec<String> = registry
            .tools
            .iter()
            .map(|t| t.underscore_alias())
            .collect();

        assert!(aliases.iter().any(|a| a == "tasks_list"));
        assert!(aliases.iter().any(|a| a == "memory_search_minimal"));
        // Tools without a dot keep the same alias as their name.
        assert!(aliases.iter().any(|a| a == "status"));
        // Dot-notation names produce underscore aliases, not themselves.
        assert!(!aliases.iter().any(|a| a == "tasks.list"));
    }

    #[tokio::test]
    async fn test_dispatch_unknown_tool() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry.dispatch("nonexistent", json!({}), &ctx).await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_dispatch_underscore_alias() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry.dispatch("tasks_list", json!({}), &ctx).await;
        assert_ne!(result.is_error, Some(true));
    }

    pub(in crate::mcp) async fn create_test_context() -> (tempfile::TempDir, McpContext) {
        let tmp = tempfile::TempDir::new().unwrap();
        let brain_home = tmp.path().to_path_buf();
        let sqlite_path = tmp.path().join("test.db");
        let lance_path = tmp.path().join("test_lance");
        let tasks_dir = tmp.path().join("tasks");

        let db = crate::db::Db::open(&sqlite_path).unwrap();
        let store = crate::store::Store::open_or_create(&lance_path)
            .await
            .unwrap();
        let store_reader = crate::store::StoreReader::from_store(&store);
        let embedder = Arc::new(crate::embedder::MockEmbedder);
        let tasks_db = crate::db::Db::open(&sqlite_path).unwrap();
        let tasks = crate::tasks::TaskStore::new(&tasks_dir, tasks_db).unwrap();

        let records_dir = tmp.path().join("records");
        let records_db = crate::db::Db::open(&sqlite_path).unwrap();
        let records = crate::records::RecordStore::new(&records_dir, records_db).unwrap();

        let objects_dir = tmp.path().join("objects");
        let objects = crate::records::objects::ObjectStore::new(&objects_dir).unwrap();

        (
            tmp,
            McpContext {
                db,
                store: Some(store_reader),
                writable_store: Some(store),
                embedder: Some(embedder),
                tasks,
                records,
                objects,
                metrics: Arc::new(crate::metrics::Metrics::new()),
                brain_home,
                brain_name: "test-brain".to_string(),
            },
        )
    }
}
