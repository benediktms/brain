use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::hierarchy::{ScopeType, generate_scope_summary_with_options, get_scope_summary};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

fn default_regenerate() -> bool {
    false
}

fn default_async_llm() -> bool {
    true
}

#[derive(Deserialize)]
struct Params {
    /// "directory" or "tag"
    scope_type: String,
    /// The directory path or tag name.
    scope_value: String,
    /// When true, force regeneration even if a summary already exists.
    #[serde(default = "default_regenerate")]
    regenerate: bool,
    /// When true, enqueue an async LLM job to replace the placeholder summary.
    #[serde(default = "default_async_llm")]
    async_llm: bool,
}

pub(super) struct MemSummarizeScope;

impl McpTool for MemSummarizeScope {
    fn name(&self) -> &'static str {
        "memory.summarize_scope"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: concat!(
                "Generate or retrieve an extractive summary of memory chunks ",
                "scoped to a directory path or tag.\n\n",
                "When `regenerate` is false (default), returns any cached summary. ",
                "Set `regenerate: true` to force a fresh extraction. ",
                "When `async_llm` is true (default), the tool enqueues an async LLM job ",
                "and returns the extractive placeholder immediately. ",
                "The response includes a `stale` flag when the cached summary is ",
                "out of date and should be regenerated on the next call."
            )
            .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "scope_type": {
                        "type": "string",
                        "enum": ["directory", "tag"],
                        "description": "Whether to scope the summary by directory path or by tag."
                    },
                    "scope_value": {
                        "type": "string",
                        "description": "The directory path (e.g. \"src/auth/\") or tag name (e.g. \"rust\")."
                    },
                    "regenerate": {
                        "type": "boolean",
                        "description": "Force regeneration of the summary even if one already exists. Default: false.",
                        "default": false
                    },
                    "async_llm": {
                        "type": "boolean",
                        "description": "Enqueue an async LLM refresh after generating the placeholder summary. Default: true.",
                        "default": true
                    }
                },
                "required": ["scope_type", "scope_value"]
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let params: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            let scope_type = match params.scope_type.as_str() {
                "directory" => ScopeType::Directory,
                "tag" => ScopeType::Tag,
                other => {
                    return ToolCallResult::error(format!(
                        "Invalid scope_type \"{other}\": must be \"directory\" or \"tag\""
                    ));
                }
            };

            let stores = &ctx.stores;

            let mut llm_pending = false;

            if params.regenerate {
                // Force-generate a fresh summary.
                match generate_scope_summary_with_options(
                    stores,
                    &scope_type,
                    &params.scope_value,
                    params.async_llm,
                ) {
                    Err(e) => {
                        return ToolCallResult::error(format!("Failed to generate summary: {e}"));
                    }
                    Ok(generation) => llm_pending = generation.llm_pending,
                }
            }

            // Retrieve the (possibly just-generated) summary.
            match get_scope_summary(stores, &scope_type, &params.scope_value) {
                Err(e) => ToolCallResult::error(format!("Failed to retrieve summary: {e}")),
                Ok(None) => {
                    // No existing summary — generate one now.
                    match generate_scope_summary_with_options(
                        stores,
                        &scope_type,
                        &params.scope_value,
                        params.async_llm,
                    ) {
                        Err(e) => ToolCallResult::error(format!("Failed to generate summary: {e}")),
                        Ok(generation) => {
                            llm_pending = generation.llm_pending;
                            match get_scope_summary(stores, &scope_type, &params.scope_value) {
                                Err(e) => ToolCallResult::error(format!(
                                    "Failed to retrieve summary after generation: {e}"
                                )),
                                Ok(None) => {
                                    ToolCallResult::error("Summary generation produced no result")
                                }
                                Ok(Some(summary)) => {
                                    let response = json!({
                                        "scope_type": summary.scope_type,
                                        "scope_value": summary.scope_value,
                                        "content": summary.content,
                                        "stale": summary.stale,
                                        "llm_pending": llm_pending,
                                        "generated_at": summary.generated_at,
                                    });
                                    json_response(&response)
                                }
                            }
                        }
                    }
                }
                Ok(Some(summary)) => {
                    let response = json!({
                        "scope_type": summary.scope_type,
                        "scope_value": summary.scope_value,
                        "content": summary.content,
                        "stale": summary.stale,
                        "llm_pending": llm_pending,
                        "generated_at": summary.generated_at,
                    });
                    json_response(&response)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_summarize_scope_missing_params_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch("memory.summarize_scope", json!({}), &ctx)
            .await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_summarize_scope_invalid_scope_type_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.summarize_scope",
                json!({ "scope_type": "invalid", "scope_value": "src/" }),
                &ctx,
            )
            .await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_summarize_scope_directory_returns_ok() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.summarize_scope",
                json!({ "scope_type": "directory", "scope_value": "src/" }),
                &ctx,
            )
            .await;
        // Empty scope still produces a valid (empty-content) response.
        assert!(
            result.is_error.is_none(),
            "unexpected error: {:?}",
            result.content
        );
        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["scope_type"], "directory");
        assert_eq!(parsed["scope_value"], "src/");
        assert_eq!(parsed["llm_pending"], false);
    }

    #[tokio::test]
    async fn test_summarize_scope_regenerate_flag() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.summarize_scope",
                json!({ "scope_type": "tag", "scope_value": "rust", "regenerate": true }),
                &ctx,
            )
            .await;
        assert!(
            result.is_error.is_none(),
            "unexpected error: {:?}",
            result.content
        );
    }

    #[tokio::test]
    async fn test_summarize_scope_async_llm_enqueues_job() {
        let (_dir, ctx) = create_test_context().await;
        ctx.stores
            .upsert_record_chunk("src/example.rs", "Example content for async scope summary")
            .expect("checked in test assertions");
        let registry = ToolRegistry::new();
        let result = registry
            .dispatch(
                "memory.summarize_scope",
                json!({
                    "scope_type": "directory",
                    "scope_value": "src/",
                    "regenerate": true,
                    "async_llm": true
                }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["llm_pending"], true);

        let jobs = ctx
            .stores
            .list_jobs(None, 10)
            .expect("checked in test assertions");
        assert_eq!(jobs.len(), 1);
    }
}
