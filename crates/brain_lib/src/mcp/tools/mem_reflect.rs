use std::future::Future;
use std::pin::Pin;

use brain_core::error::BrainCoreError;
use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, json_response};

pub(super) struct MemReflect;

impl McpTool for MemReflect {
    fn name(&self) -> &'static str {
        "memory.reflect"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: concat!(
                "Two-phase episodic reflection.\n\n",
                "**prepare** (default): Retrieve source material — recent episodes and related chunks — ",
                "that the LLM can synthesize into a reflection. Returns structured source material.\n\n",
                "**commit**: Store a completed reflection linked to its source episodes. ",
                "Requires title, content, and source_ids from a prior prepare call."
            ).into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["prepare", "commit"],
                        "description": "Operation mode. Default: 'prepare'",
                        "default": "prepare"
                    },
                    "topic": {
                        "type": "string",
                        "description": "(prepare) Topic to reflect on"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "(prepare) Maximum tokens for source material. Default: 2000",
                        "default": 2000
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "(prepare) Brain names/IDs to include. Empty = current brain. 'all' = all brains."
                    },
                    "title": {
                        "type": "string",
                        "description": "(commit) Title of the reflection"
                    },
                    "content": {
                        "type": "string",
                        "description": "(commit) Synthesized reflection content"
                    },
                    "source_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "(commit) summary_ids of source episodes used"
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "(commit) Tags for the reflection"
                    },
                    "importance": {
                        "type": "number",
                        "description": "(commit) Importance score (0.0–1.0). Default: 1.0",
                        "default": 1.0
                    }
                },
                "required": []
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let params: brain_memory::reflect::ReflectParams =
                match serde_json::from_value(params) {
                    Ok(p) => p,
                    Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
                };

            let sem_ctx = brain_memory::context::SemanticContext {
                db: ctx.stores.inner_db(),
                brain_id: ctx.brain_id(),
                brain_name: ctx.brain_name(),
                store: ctx.store(),
                embedder: ctx.embedder(),
                metrics: &ctx.metrics,
            };

            match brain_memory::reflect::run_as_json(&sem_ctx, params).await {
                Ok(v) => json_response(&v),
                // Parse-typed errors carry the validation message
                // verbatim — the MCP wire contract preserves the
                // original strings byte-identical.
                Err(BrainCoreError::Parse(msg)) => ToolCallResult::error(msg),
                // Embedding errors surface as the tasks-only-mode
                // "memory unavailable" message.
                Err(BrainCoreError::Embedding(_)) => ToolCallResult::error(super::MEMORY_UNAVAILABLE),
                Err(e) => ToolCallResult::error(format!("Reflect failed: {e}")),
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
    async fn test_reflect_prepare() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({ "topic": "project architecture" });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert!(result.is_error.is_none());
        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["mode"], "prepare");
    }

    #[tokio::test]
    async fn test_reflect_invalid_mode_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({ "mode": "unknown" });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("Invalid mode"));
    }

    #[tokio::test]
    async fn test_reflect_commit_missing_source_id_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({
            "mode": "commit",
            "title": "My Reflection",
            "content": "I learned that...",
            "source_ids": ["nonexistent-id"]
        });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("source_id not found"));
    }

    #[tokio::test]
    async fn test_reflect_commit_clamps_importance() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Store an episode first to use as source_id.
        let ep_result = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Learn Rust",
                    "actions": "Read the book",
                    "outcome": "Learned Rust"
                }),
                &ctx,
            )
            .await;
        assert!(ep_result.is_error.is_none());
        let ep_text = &ep_result.content[0].text;
        let ep_parsed: serde_json::Value =
            serde_json::from_str(ep_text).expect("checked in test assertions");
        let source_id = ep_parsed["summary_id"]
            .as_str()
            .expect("checked in test assertions")
            .to_string();

        // Commit with out-of-range importance (2.5 should clamp to 1.0).
        let params = json!({
            "mode": "commit",
            "title": "Reflection on Rust",
            "content": "Rust is great",
            "source_ids": [source_id],
            "importance": 2.5
        });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert!(
            result.is_error.is_none(),
            "reflect commit failed: {}",
            result.content[0].text
        );
        let text = &result.content[0].text;
        let parsed: serde_json::Value =
            serde_json::from_str(text).expect("checked in test assertions");
        assert_eq!(parsed["mode"], "commit");
        // Importance should be clamped to 1.0.
        assert!(
            (parsed["importance"]
                .as_f64()
                .expect("checked in test assertions")
                - 1.0)
                .abs()
                < f64::EPSILON
        );
    }
}
