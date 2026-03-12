/// Multi-brain JSON-RPC router.
///
/// Resolves the `brain` parameter on each request to the appropriate
/// brain_id, then clones the shared `McpContext` with that brain_id
/// before delegating to the shared `ToolRegistry`.
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::RwLock;

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::mcp::tools::ToolRegistry;

/// Routes JSON-RPC tool calls by resolving brain name → brain_id, then
/// dispatching with a per-request McpContext scoped to that brain_id.
pub struct BrainRouter {
    /// Single shared context (Db, embedder, object store, metrics).
    ctx: Arc<McpContext>,
    /// Map of brain name → brain_id.
    brain_map: RwLock<HashMap<String, String>>,
    /// brain_id of the default brain (used when no `brain` param is supplied).
    default_brain_id: RwLock<Option<String>>,
    /// Shared tool registry (stateless; all handlers receive context at call time).
    registry: ToolRegistry,
}

impl BrainRouter {
    /// Create a new router with a shared context and brain name→id map.
    ///
    /// The first entry in `brain_map` whose value becomes the default brain_id,
    /// unless `ctx.brain_id` is non-empty (in which case that is used).
    pub fn new(ctx: Arc<McpContext>, brain_map: HashMap<String, String>) -> Arc<Self> {
        let default_brain_id = if !ctx.brain_id.is_empty() {
            Some(ctx.brain_id.clone())
        } else {
            brain_map.values().next().cloned()
        };
        Arc::new(Self {
            ctx,
            brain_map: RwLock::new(brain_map),
            default_brain_id: RwLock::new(default_brain_id),
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
        let (brain_id, brain_name) = {
            let map = self.brain_map.read().await;
            match brain {
                Some(name) => match map.get(name) {
                    Some(id) => (id.clone(), name.to_string()),
                    None => {
                        return ToolCallResult::error(format!("Brain not found: {name}"));
                    }
                },
                None => {
                    let default = self.default_brain_id.read().await;
                    match default.as_deref() {
                        Some(id) => {
                            // Reverse-lookup name for the default brain_id.
                            let name = map
                                .iter()
                                .find(|(_, v)| v.as_str() == id)
                                .map(|(k, _)| k.clone())
                                .unwrap_or_default();
                            (id.to_string(), name)
                        }
                        None => {
                            return ToolCallResult::error(
                                "No default brain registered".to_string(),
                            );
                        }
                    }
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

    /// Replace the brain map (called on SIGHUP reload).
    ///
    /// The new default brain_id is the first entry in the updated map,
    /// unless the previous default_brain_id is still present (in which case
    /// it is kept).
    pub async fn update_brains(&self, map: HashMap<String, String>) {
        let new_default = {
            let old_default = self.default_brain_id.read().await;
            match old_default.as_deref() {
                Some(id) if map.values().any(|v| v == id) => Some(id.to_string()),
                _ => map.values().next().cloned(),
            }
        };
        *self.brain_map.write().await = map;
        *self.default_brain_id.write().await = new_default;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::tools::tests::create_test_context;

    fn make_router(name: &str, ctx: McpContext) -> Arc<BrainRouter> {
        let mut map = HashMap::new();
        // Use brain_id if set, otherwise use name as a stand-in.
        let brain_id = if ctx.brain_id.is_empty() {
            name.to_string()
        } else {
            ctx.brain_id.clone()
        };
        map.insert(name.to_string(), brain_id);
        BrainRouter::new(Arc::new(ctx), map)
    }

    #[tokio::test]
    async fn test_dispatch_default_brain_status() {
        let (_dir, ctx) = create_test_context().await;
        let router = make_router("test-brain", ctx);

        let result = router.dispatch(None, "status", serde_json::json!({})).await;
        assert_ne!(result.is_error, Some(true), "status call should succeed");
    }

    #[tokio::test]
    async fn test_dispatch_nonexistent_brain_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let router = make_router("test-brain", ctx);

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
        let router = make_router("my-brain", ctx);

        let result = router
            .dispatch(Some("my-brain"), "status", serde_json::json!({}))
            .await;
        assert_ne!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn test_update_brains_keeps_default_when_present() {
        let (_dir, ctx1) = create_test_context().await;
        let (_dir2, _ctx2) = create_test_context().await;

        let mut initial = HashMap::new();
        initial.insert("alpha".to_string(), "id-alpha".to_string());
        let router = BrainRouter::new(Arc::new(ctx1), initial);

        // Default should be "id-alpha" now.
        let mut updated = HashMap::new();
        updated.insert("alpha".to_string(), "id-alpha".to_string());
        updated.insert("beta".to_string(), "id-beta".to_string());
        router.update_brains(updated).await;

        // Default still "id-alpha" — it survived the reload.
        let result = router
            .dispatch(Some("alpha"), "status", serde_json::json!({}))
            .await;
        assert_ne!(result.is_error, Some(true));
    }
}
