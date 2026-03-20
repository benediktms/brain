use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::McpTool;

#[derive(Deserialize)]
struct Params {
    query: String,
    #[serde(default = "default_k")]
    k: u64,
    #[serde(default = "default_budget")]
    budget_tokens: u64,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    brains: Vec<String>,
}

fn default_k() -> u64 {
    10
}

fn default_budget() -> u64 {
    800
}

pub(super) struct RecordSearch;

impl McpTool for RecordSearch {
    fn name(&self) -> &'static str {
        "records.search"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Search records (artifacts, snapshots, documents) using semantic + \
                keyword hybrid retrieval. Returns only record-kind results, filtered from the \
                full hybrid pipeline. Use this to find previously saved artifacts, snapshots, \
                reports, and documents by content."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query"
                    },
                    "k": {
                        "type": "integer",
                        "description": "Maximum number of results to return. Default: 10",
                        "default": 10
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 800",
                        "default": 800
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional tags to filter results. Pass as a JSON array, e.g. [\"drone-checkpoint\", \"wave:1\"]"
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional list of brain names or IDs to search across multiple brains. Use [\"all\"] to search all registered brains. When omitted, searches only the current brain."
                    }
                },
                "required": ["query"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        _ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            // Validate params first — if query is missing, return a validation error.
            let _params: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            ToolCallResult::error("records.search not yet implemented")
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;
    use super::RecordSearch;
    use crate::mcp::McpContext;
    use crate::mcp::protocol::ToolDefinition;
    use crate::mcp::tools::McpTool;

    /// Verify the tool is registered and dispatches without "Unknown tool" error.
    #[tokio::test]
    async fn test_records_search_exists() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("records.search", json!({"query": "test"}), &ctx)
            .await;

        // Must NOT be "Unknown tool" — the tool must be registered.
        let error_text = &result.content[0].text;
        assert!(
            !error_text.contains("Unknown tool"),
            "records.search is not registered in ToolRegistry; got: {error_text}"
        );
    }

    /// Missing required `query` field should produce a validation error.
    #[tokio::test]
    async fn test_records_search_missing_query() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("records.search", json!({}), &ctx)
            .await;

        assert_eq!(
            result.is_error,
            Some(true),
            "missing query should yield an error"
        );
        assert!(
            result.content[0].text.contains("Invalid parameters"),
            "error message should mention invalid parameters; got: {}",
            result.content[0].text
        );
    }

    /// The tool definition must carry the expected name and required fields.
    #[tokio::test]
    async fn test_records_search_schema() {
        let tool = RecordSearch;
        let def: ToolDefinition = tool.definition();

        assert_eq!(def.name, "records.search");

        let required = def.input_schema.get("required");
        assert!(required.is_some(), "schema must have a 'required' field");

        let required_arr = required.unwrap().as_array().unwrap();
        assert!(
            required_arr.iter().any(|v| v.as_str() == Some("query")),
            "'query' must be in required; got: {required_arr:?}"
        );

        let props = def.input_schema.get("properties").unwrap();
        assert!(props.get("query").is_some(), "schema must include 'query' property");
        assert!(props.get("k").is_some(), "schema must include 'k' property");
        assert!(
            props.get("budget_tokens").is_some(),
            "schema must include 'budget_tokens' property"
        );
        assert!(props.get("tags").is_some(), "schema must include 'tags' property");
        assert!(props.get("brains").is_some(), "schema must include 'brains' property");
    }

    /// Underscore alias must also dispatch correctly.
    #[tokio::test]
    async fn test_records_search_underscore_alias() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("records_search", json!({"query": "test"}), &ctx)
            .await;

        let error_text = &result.content[0].text;
        assert!(
            !error_text.contains("Unknown tool"),
            "records_search alias not registered; got: {error_text}"
        );
    }
}
