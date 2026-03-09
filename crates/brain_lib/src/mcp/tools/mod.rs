/// MCP tool definitions and handlers.
mod mem_expand;
mod mem_reflect;
mod mem_search_minimal;
mod mem_write_episode;
mod status;
mod task_apply_event;
mod task_get;
mod task_list;
mod task_next;

use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

/// Extract a required string parameter, returning a ToolCallResult error if missing.
pub(super) fn require_str<'a>(params: &'a Value, name: &str) -> Result<&'a str, ToolCallResult> {
    params
        .get(name)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolCallResult::error(format!("Missing required parameter: {name}")))
}

/// Extract a required JSON array parameter.
pub(super) fn require_array<'a>(
    params: &'a Value,
    name: &str,
) -> Result<&'a Vec<Value>, ToolCallResult> {
    params
        .get(name)
        .and_then(|v| v.as_array())
        .ok_or_else(|| ToolCallResult::error(format!("Missing required parameter: {name}")))
}

/// Extract an optional u64 parameter with a default value.
pub(super) fn opt_u64(params: &Value, name: &str, default: u64) -> u64 {
    params.get(name).and_then(|v| v.as_u64()).unwrap_or(default)
}

/// Extract an optional string parameter with a default value.
pub(super) fn opt_str<'a>(params: &'a Value, name: &str, default: &'a str) -> &'a str {
    params.get(name).and_then(|v| v.as_str()).unwrap_or(default)
}

/// Return all available tool definitions.
pub fn tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "memory.search_minimal".into(),
            description: "Search the knowledge base and return compact memory stubs within a token budget. Use this first to find relevant memories, then expand specific ones.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query"
                    },
                    "intent": {
                        "type": "string",
                        "enum": ["lookup", "planning", "reflection", "synthesis", "auto"],
                        "description": "Retrieval intent — controls ranking weight profile. Default: auto",
                        "default": "auto"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 800",
                        "default": 800
                    },
                    "k": {
                        "type": "integer",
                        "description": "Maximum number of results. Default: 10",
                        "default": 10
                    }
                },
                "required": ["query"]
            }),
        },
        ToolDefinition {
            name: "memory.expand".into(),
            description: "Expand memory stubs to full content. Pass memory_ids from search_minimal results.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "memory_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Memory IDs to expand (from search_minimal results)"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 2000",
                        "default": 2000
                    }
                },
                "required": ["memory_ids"]
            }),
        },
        ToolDefinition {
            name: "memory.write_episode".into(),
            description: "Record an episode (goal, actions, outcome) to the knowledge base.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "goal": {
                        "type": "string",
                        "description": "What was the goal"
                    },
                    "actions": {
                        "type": "string",
                        "description": "What actions were taken"
                    },
                    "outcome": {
                        "type": "string",
                        "description": "What was the outcome"
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Tags for categorization"
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance score (0.0 to 1.0). Default: 1.0",
                        "default": 1.0
                    }
                },
                "required": ["goal", "actions", "outcome"]
            }),
        },
        ToolDefinition {
            name: "memory.reflect".into(),
            description: "Retrieve source material for reflection. Returns relevant memories that the LLM can synthesize into a reflection, then call back to store.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Topic to reflect on"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens for source material. Default: 2000",
                        "default": 2000
                    }
                },
                "required": ["topic"]
            }),
        },
        ToolDefinition {
            name: "tasks.apply_event".into(),
            description: "Apply an event to the task system. Creates, updates, or changes tasks via event sourcing. Returns the resulting task state and any newly unblocked task IDs.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "event_type": {
                        "type": "string",
                        "enum": ["task_created", "task_updated", "status_changed",
                                 "dependency_added", "dependency_removed",
                                 "note_linked", "note_unlinked",
                                 "label_added", "label_removed", "comment_added",
                                 "parent_set"],
                        "description": "The type of task event to apply"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Task ID (full or prefix). Optional for task_created (auto-generates prefixed ULID). For other events, accepts full ID or a unique prefix (e.g. 'BRN-01JPH')."
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is performing this action. Default: 'mcp'",
                        "default": "mcp"
                    },
                    "payload": {
                        "type": "object",
                        "description": "Event-type-specific fields. task_created: {title, description?, priority?, due_ts?, task_type?, assignee?, defer_until?, parent_task_id?}. task_updated: {title?, description?, priority?, due_ts?, blocked_reason?, task_type?, assignee?, defer_until?}. status_changed: {new_status}. dependency_added/removed: {depends_on_task_id}. note_linked/unlinked: {chunk_id}. label_added/removed: {label}. comment_added: {body}. parent_set: {parent_task_id?} (null to clear). Timestamps (due_ts, defer_until) accept ISO 8601 strings (preferred, e.g. \"2026-03-15T00:00:00Z\") or Unix-seconds integers. Responses always return timestamps as ISO 8601 strings."
                    }
                },
                "required": ["event_type", "payload"]
            }),
        },
        ToolDefinition {
            name: "tasks.get".into(),
            description: "Get a single task by ID (full or prefix) with full details including relationships, comments, labels, and linked notes. Relationships (parent, children, blocked_by, blocks) are returned as compact stubs by default; use the expand parameter to get full task objects.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "The task ID to retrieve (full ID or unique prefix, e.g. 'BRN-01JPH')"
                    },
                    "expand": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["parent", "children", "blocked_by", "blocks"]
                        },
                        "description": "Expand relationship stubs to full task objects"
                    }
                },
                "required": ["task_id"]
            }),
        },
        ToolDefinition {
            name: "tasks.list".into(),
            description: "List tasks filtered by status. Returns summary task objects (descriptions omitted by default — use tasks.get for full details). Results are sorted by priority and paginated.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["open", "ready", "blocked", "done"],
                        "description": "Filter tasks by status. 'open' (default): excludes done/cancelled. 'ready': no unresolved deps. 'blocked': has unresolved deps or blocked_reason. 'done': completed or cancelled tasks.",
                        "default": "open"
                    },
                    "task_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Fetch specific tasks by ID or prefix (ignores status filter). Unresolvable IDs are silently skipped."
                    },
                    "include_description": {
                        "type": "boolean",
                        "description": "Include task descriptions in output. Default: false (omitted to reduce response size).",
                        "default": false
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of tasks to return. Default: 50. Response includes 'total' and 'has_more' for pagination.",
                        "default": 50
                    }
                }
            }),
        },
        ToolDefinition {
            name: "tasks.next".into(),
            description: "Get the next highest-priority ready task(s). Returns tasks with no unresolved dependencies, sorted by configurable policy. Includes dependency summary and linked notes for each task.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "policy": {
                        "type": "string",
                        "enum": ["priority", "due_date"],
                        "description": "Sorting policy. 'priority' (default): by priority then due date. 'due_date': by due date then priority.",
                        "default": "priority"
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of tasks to return. Default: 1",
                        "default": 1
                    }
                }
            }),
        },
        ToolDefinition {
            name: "status".into(),
            description: "Get runtime health metrics: indexing/query latency (p50/p95), stale hash prevention count, token usage, queue depth, LanceDB unoptimized rows, stuck files, and uptime.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
    ]
}

const MEMORY_UNAVAILABLE: &str = "Memory tools are unavailable: embedding model not found. Run `brain setup-model` to download it.";

/// Dispatch a tool call to the appropriate handler.
pub async fn dispatch_tool_call(name: &str, params: &Value, ctx: &McpContext) -> ToolCallResult {
    match name {
        "memory.search_minimal" | "memory.expand" | "memory.reflect" => {
            if ctx.store.is_none() || ctx.embedder.is_none() {
                return ToolCallResult::error(MEMORY_UNAVAILABLE.to_string());
            }
            match name {
                "memory.search_minimal" => mem_search_minimal::handle(params, ctx).await,
                "memory.expand" => mem_expand::handle(params, ctx).await,
                "memory.reflect" => mem_reflect::handle(params, ctx).await,
                _ => unreachable!(),
            }
        }
        "memory.write_episode" => mem_write_episode::handle(params, ctx),
        "tasks.apply_event" => task_apply_event::handle(params, ctx),
        "tasks.get" => task_get::handle(params, ctx),
        "tasks.list" => task_list::handle(params, ctx),
        "tasks.next" => task_next::handle(params, ctx),
        "status" => status::handle(ctx).await,
        _ => ToolCallResult::error(format!("Unknown tool: {name}")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;

    #[test]
    fn test_tool_definitions_valid() {
        let defs = tool_definitions();
        assert_eq!(defs.len(), 9);

        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"memory.search_minimal"));
        assert!(names.contains(&"memory.expand"));
        assert!(names.contains(&"memory.write_episode"));
        assert!(names.contains(&"memory.reflect"));
        assert!(names.contains(&"tasks.apply_event"));
        assert!(names.contains(&"tasks.get"));
        assert!(names.contains(&"tasks.list"));
        assert!(names.contains(&"tasks.next"));

        // All should have valid JSON schemas
        for def in &defs {
            assert!(def.input_schema.is_object());
            assert!(def.input_schema.get("type").is_some());
        }
    }

    #[test]
    fn test_dispatch_unknown_tool() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });
        let result = rt.block_on(dispatch_tool_call("nonexistent", &json!({}), &ctx));
        assert_eq!(result.is_error, Some(true));
    }

    pub(super) async fn create_test_context() -> (tempfile::TempDir, McpContext) {
        let tmp = tempfile::TempDir::new().unwrap();
        let sqlite_path = tmp.path().join("test.db");
        let lance_path = tmp.path().join("test_lance");
        let tasks_dir = tmp.path().join("tasks");

        let db = crate::db::Db::open(&sqlite_path).unwrap();
        let store = crate::store::Store::open_or_create(&lance_path)
            .await
            .unwrap();
        let store_reader = crate::store::StoreReader::from_store(&store);
        // Keep store alive so the Arc<Table> remains valid.
        let _store = store;
        let embedder = Arc::new(crate::embedder::MockEmbedder);
        let tasks_db = crate::db::Db::open(&sqlite_path).unwrap();
        let tasks = crate::tasks::TaskStore::new(&tasks_dir, tasks_db).unwrap();

        (
            tmp,
            McpContext {
                db,
                store: Some(store_reader),
                embedder: Some(embedder),
                tasks,
                metrics: Arc::new(crate::metrics::Metrics::new()),
            },
        )
    }
}
