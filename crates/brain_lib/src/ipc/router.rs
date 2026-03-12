/// Multi-brain JSON-RPC router.
///
/// Resolves the `brain` parameter on each request to the appropriate
/// `McpContext`, then delegates to the shared `ToolRegistry`.
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::RwLock;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::mcp::tools::ToolRegistry;

/// Routes JSON-RPC tool calls to the correct brain's `McpContext`.
pub struct BrainRouter {
    /// Map of brain name → context.
    brains: RwLock<HashMap<String, Arc<McpContext>>>,
    /// Name of the default brain (used when no `brain` param is supplied).
    default_brain: RwLock<Option<String>>,
    /// Shared tool registry (stateless; all handlers receive context at call time).
    registry: ToolRegistry,
}

impl BrainRouter {
    /// Create a new router with an initial brain map.
    ///
    /// The first entry in `brains` becomes the default brain.
    pub fn new(brains: HashMap<String, Arc<McpContext>>) -> Arc<Self> {
        let default_brain = brains.keys().next().cloned();
        Arc::new(Self {
            brains: RwLock::new(brains),
            default_brain: RwLock::new(default_brain),
            registry: ToolRegistry::new(),
        })
    }

    /// Dispatch a tool call, resolving the brain context first.
    ///
    /// If `brain` is `None`, the default brain is used.
    /// If the specified brain is not registered, returns an error result.
    pub async fn dispatch(
        &self,
        brain: Option<&str>,
        tool_name: &str,
        params: Value,
    ) -> ToolCallResult {
        let ctx = {
            let brains = self.brains.read().await;
            match brain {
                Some(name) => match brains.get(name) {
                    Some(ctx) => Arc::clone(ctx),
                    None => {
                        return ToolCallResult::error(format!("Brain not found: {name}"));
                    }
                },
                None => {
                    let default = self.default_brain.read().await;
                    match default.as_deref().and_then(|n| brains.get(n)) {
                        Some(ctx) => Arc::clone(ctx),
                        None => {
                            return ToolCallResult::error(
                                "No default brain registered".to_string(),
                            );
                        }
                    }
                }
            }
        };

        self.registry.dispatch(tool_name, params, &ctx).await
    }

    /// Replace the brain registry (called on SIGHUP reload).
    ///
    /// The new default brain is the first entry in the updated map,
    /// unless the previous default is still present (in which case it is kept).
    pub async fn update_brains(&self, map: HashMap<String, Arc<McpContext>>) {
        let new_default = {
            let old_default = self.default_brain.read().await;
            match old_default.as_deref() {
                Some(name) if map.contains_key(name) => Some(name.to_string()),
                _ => map.keys().next().cloned(),
            }
        };
        *self.brains.write().await = map;
        *self.default_brain.write().await = new_default;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::tools::tests::create_test_context;

    #[tokio::test]
    async fn test_dispatch_default_brain_status() {
        let (_dir, ctx) = create_test_context().await;
        let mut map = HashMap::new();
        map.insert("test-brain".to_string(), Arc::new(ctx));
        let router = BrainRouter::new(map);

        let result = router.dispatch(None, "status", serde_json::json!({})).await;
        assert_ne!(result.is_error, Some(true), "status call should succeed");
    }

    #[tokio::test]
    async fn test_dispatch_nonexistent_brain_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let mut map = HashMap::new();
        map.insert("test-brain".to_string(), Arc::new(ctx));
        let router = BrainRouter::new(map);

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
    async fn test_dispatch_explicit_brain_name() {
        let (_dir, ctx) = create_test_context().await;
        let mut map = HashMap::new();
        map.insert("my-brain".to_string(), Arc::new(ctx));
        let router = BrainRouter::new(map);

        let result = router
            .dispatch(Some("my-brain"), "status", serde_json::json!({}))
            .await;
        assert_ne!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_update_brains_keeps_default_when_present() {
        let (_dir, ctx1) = create_test_context().await;
        let (_dir2, ctx2) = create_test_context().await;

        let mut initial = HashMap::new();
        initial.insert("alpha".to_string(), Arc::new(ctx1));
        let router = BrainRouter::new(initial);

        // Default should be "alpha" now.
        let mut updated = HashMap::new();
        updated.insert("alpha".to_string(), Arc::new(ctx2));
        updated.insert("beta".to_string(), {
            let (_d, c) = create_test_context().await;
            Arc::new(c)
        });
        router.update_brains(updated).await;

        // Default still "alpha" — it survived the reload.
        let result = router
            .dispatch(Some("alpha"), "status", serde_json::json!({}))
            .await;
        assert_ne!(result.is_error, Some(true));
    }
}
