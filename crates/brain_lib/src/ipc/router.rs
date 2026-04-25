/// Multi-brain JSON-RPC router.
///
/// Resolves the `brain` parameter on each request to the appropriate
/// brain_id via a DB lookup, then clones the shared `McpContext` with that
/// brain_id before delegating to the shared `ToolRegistry`.
use std::sync::Arc;

use serde_json::Value;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::mcp::tools::ToolRegistry;

/// Routes JSON-RPC tool calls by resolving brain name → brain_id via the DB,
/// then dispatching with a per-request McpContext scoped to that brain_id.
pub struct BrainRouter {
    /// Single shared context (Db, embedder, object store, metrics).
    ctx: Arc<McpContext>,
    /// brain_id of the default brain (used when no `brain` param is supplied).
    default_brain_id: String,
    /// Shared tool registry (stateless; all handlers receive context at call time).
    registry: ToolRegistry,
}

impl BrainRouter {
    /// Create a new router with a shared context and a default brain_id.
    pub fn new(ctx: Arc<McpContext>, default_brain_id: String) -> Arc<Self> {
        Arc::new(Self {
            ctx,
            default_brain_id,
            registry: ToolRegistry::new(),
        })
    }

    /// Dispatch a tool call, resolving the brain context first via DB lookup.
    ///
    /// If `brain` is `None`, the default brain is used.
    /// If the specified brain is not found in the DB, returns an error result.
    pub async fn dispatch(
        &self,
        brain: Option<&str>,
        tool_name: &str,
        params: Value,
    ) -> ToolCallResult {
        let (brain_id, brain_name) = match brain.filter(|s| !s.is_empty()) {
            Some(input) => {
                // DB lookup via resolve_brain (name, id, alias, or root path)
                match self.ctx.stores.resolve_brain(input) {
                    Ok(pair) => pair,
                    Err(_) => {
                        return ToolCallResult::error(format!("Brain not found: {input}"));
                    }
                }
            }
            None => {
                // Default brain — resolve by ID
                match self.ctx.stores.resolve_brain(&self.default_brain_id) {
                    Ok(pair) => pair,
                    Err(_) => (self.default_brain_id.clone(), String::new()),
                }
            }
        };

        let ctx = match self.ctx.with_brain_id(&brain_id, &brain_name) {
            Ok(c) => c,
            Err(e) => {
                return ToolCallResult::error(format!(
                    "Failed to create context for brain {brain_name}: {e}"
                ));
            }
        };

        self.registry.dispatch(tool_name, params, &ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::tools::tests::create_test_context;

    #[tokio::test]
    async fn test_dispatch_default_brain_status() {
        let (_dir, ctx) = create_test_context().await;
        // Use a non-empty brain_id — empty strings skip registration
        let brain_id = if ctx.brain_id().is_empty() {
            "test-id".to_string()
        } else {
            ctx.brain_id().to_string()
        };
        ctx.stores
            .db()
            .ensure_brain_registered(&brain_id, "test-brain")
            .unwrap();
        let router = BrainRouter::new(Arc::new(ctx), brain_id);

        let result = router.dispatch(None, "status", serde_json::json!({})).await;
        assert_ne!(result.is_error, Some(true), "status call should succeed");
    }

    #[tokio::test]
    async fn test_dispatch_nonexistent_brain_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let brain_id = if ctx.brain_id().is_empty() {
            "test-id".to_string()
        } else {
            ctx.brain_id().to_string()
        };
        ctx.stores
            .db()
            .ensure_brain_registered(&brain_id, "test-brain")
            .unwrap();
        let router = BrainRouter::new(Arc::new(ctx), brain_id);

        let result = router
            .dispatch(Some("no-such-brain"), "status", serde_json::json!({}))
            .await;
        assert_eq!(result.is_error, Some(true));
        let msg = &result.content[0].text;
        assert!(
            msg.contains("no-such-brain"),
            "error should name the missing brain"
        );
    }

    #[tokio::test]
    async fn test_dispatch_empty_brain_uses_default() {
        let (_dir, ctx) = create_test_context().await;
        let brain_id = if ctx.brain_id().is_empty() {
            "test-id".to_string()
        } else {
            ctx.brain_id().to_string()
        };
        ctx.stores
            .db()
            .ensure_brain_registered(&brain_id, "test-brain")
            .unwrap();
        let router = BrainRouter::new(Arc::new(ctx), brain_id);

        // Empty string should be treated the same as None (default brain).
        let result = router
            .dispatch(Some(""), "status", serde_json::json!({}))
            .await;
        assert_ne!(
            result.is_error,
            Some(true),
            "empty brain name should fall back to default"
        );
    }

    #[tokio::test]
    async fn test_dispatch_explicit_brain_name() {
        let (_dir, ctx) = create_test_context().await;
        let brain_id = if ctx.brain_id().is_empty() {
            "test-id".to_string()
        } else {
            ctx.brain_id().to_string()
        };
        ctx.stores
            .db()
            .ensure_brain_registered(&brain_id, "my-brain")
            .unwrap();
        let router = BrainRouter::new(Arc::new(ctx), brain_id);

        let result = router
            .dispatch(Some("my-brain"), "status", serde_json::json!({}))
            .await;
        let err_text = result
            .content
            .first()
            .map(|c| c.text.as_str())
            .unwrap_or("(no content)");
        assert_ne!(result.is_error, Some(true), "dispatch failed: {err_text}");
    }
}
