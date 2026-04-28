//! Brain-scope resolution for MCP tool handlers.
//!
//! Read tools accept `brains: Option<Vec<String>>`:
//! - `["all"]` or `["*"]` — federate across every active registered brain.
//! - `["A", "B"]` — federate across the listed brains (single brain when one entry).
//! - omitted — fall back to the ambient brain on `ctx`.
//!
//! Write tools accept `brain: Option<String>` and resolve to exactly one brain.
//!
//! Both paths emit a structured error listing registered brains when no
//! explicit param is given and the ambient `ctx.brain_id()` is unresolvable.

use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;

/// Canonical description for the `brains` input-schema property on read tools.
/// Mirrored verbatim in the AGENTS.md template (`init.rs::BRAIN_SECTION_BODY`)
/// and CLI clap doc-comments — keep wording in sync to avoid drift.
pub const BRAINS_PARAM_DESCRIPTION: &str = "Brains to query. Pass [\"all\"] (or [\"*\"]) to query every registered brain. Pass a list of names to federate across them. Omit to query the ambient brain.";

/// Identifier pair for a single brain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrainRef {
    pub brain_id: String,
    pub brain_name: String,
}

impl BrainRef {
    pub fn new(brain_id: impl Into<String>, brain_name: impl Into<String>) -> Self {
        Self {
            brain_id: brain_id.into(),
            brain_name: brain_name.into(),
        }
    }
}

/// What brains a read-tool call should query.
#[derive(Debug, Clone)]
pub enum Scope {
    /// Run against a single brain's scoped store.
    Single(BrainRef),
    /// Iterate every listed brain and merge results.
    Federated(Vec<BrainRef>),
}

impl Scope {
    /// Brains this scope covers, as borrowed refs.
    pub fn brains(&self) -> Vec<&BrainRef> {
        match self {
            Self::Single(b) => vec![b],
            Self::Federated(bs) => bs.iter().collect(),
        }
    }

    /// True when the scope covers more than one brain.
    pub fn is_federated(&self) -> bool {
        matches!(self, Self::Federated(_))
    }
}

/// Resolve the scope for a read tool from its `brains` parameter.
pub fn resolve_scope(
    ctx: &McpContext,
    brains_param: Option<&[String]>,
) -> Result<Scope, ToolCallResult> {
    // Drop empty strings up front. An empty entry (`brains: [""]`, or the
    // back-compat `brain: ""` → `brains: [""]`) would otherwise resolve via
    // the unscoped sentinel row (brain_id = "") and silently produce an
    // unfiltered cross-brain query.
    let cleaned: Option<Vec<&String>> =
        brains_param.map(|refs| refs.iter().filter(|s| !s.trim().is_empty()).collect());

    let has_sentinel = cleaned
        .as_ref()
        .map(|refs| {
            refs.iter()
                .any(|s| s.as_str() == "all" || s.as_str() == "*")
        })
        .unwrap_or(false);
    let has_named = cleaned
        .as_ref()
        .map(|refs| {
            refs.iter()
                .any(|s| s.as_str() != "all" && s.as_str() != "*")
        })
        .unwrap_or(false);

    if has_sentinel && has_named {
        return Err(ToolCallResult::error(
            "Cannot mix the \"all\" / \"*\" sentinel with named brains. \
             Pass either `brains: [\"all\"]` to query every registered brain, \
             or a list of brain names — not both."
                .to_string(),
        ));
    }

    match cleaned {
        Some(refs) if has_sentinel => {
            let pairs = ctx.stores.list_brain_keys().map_err(|e| {
                ToolCallResult::error(format!("Failed to list registered brains: {e}"))
            })?;
            if pairs.is_empty() {
                return Err(no_brains_registered_error());
            }
            let _ = refs; // sentinel resolution ignores any other entries
            Ok(Scope::Federated(
                pairs
                    .into_iter()
                    .map(|(name, id)| BrainRef::new(id, name))
                    .collect(),
            ))
        }
        Some(refs) if !refs.is_empty() => {
            let mut resolved = Vec::with_capacity(refs.len());
            for input in refs {
                let (brain_id, brain_name) = ctx.stores.resolve_brain(input).map_err(|e| {
                    ToolCallResult::error(format!("Failed to resolve brain '{input}': {e}"))
                })?;
                resolved.push(BrainRef::new(brain_id, brain_name));
            }
            if resolved.len() == 1 {
                Ok(Scope::Single(
                    resolved.into_iter().next().expect("checked len == 1"),
                ))
            } else {
                Ok(Scope::Federated(resolved))
            }
        }
        _ => Ok(Scope::Single(ambient_brain(ctx)?)),
    }
}

/// Resolve a write tool's single-brain scope.
pub fn resolve_single_scope(
    ctx: &McpContext,
    brain_param: Option<&str>,
) -> Result<BrainRef, ToolCallResult> {
    if let Some(name) = brain_param.filter(|s| !s.is_empty()) {
        let (brain_id, brain_name) = ctx
            .stores
            .resolve_brain(name)
            .map_err(|e| ToolCallResult::error(format!("Failed to resolve brain '{name}': {e}")))?;
        return Ok(BrainRef::new(brain_id, brain_name));
    }
    ambient_brain(ctx)
}

fn ambient_brain(ctx: &McpContext) -> Result<BrainRef, ToolCallResult> {
    let id = ctx.brain_id();
    if id.is_empty() {
        return Err(unresolvable_default_error(ctx));
    }
    Ok(BrainRef::new(id, ctx.brain_name()))
}

fn no_brains_registered_error() -> ToolCallResult {
    ToolCallResult::error(
        "No registered brains. Run `brain init` in the brain you want to scope to.".to_string(),
    )
}

fn unresolvable_default_error(ctx: &McpContext) -> ToolCallResult {
    let brains = match ctx.stores.list_brain_keys() {
        Ok(pairs) => pairs
            .into_iter()
            .map(|(n, _)| n)
            .collect::<Vec<_>>()
            .join(", "),
        Err(_) => String::new(),
    };
    if brains.is_empty() {
        ToolCallResult::error(
            "No brain context could be resolved. Pass `brains: [\"<name>\"]` to scope to a single brain or `brains: [\"all\"]` to query every brain.".to_string(),
        )
    } else {
        ToolCallResult::error(format!(
            "No brain context could be resolved. Registered brains: {brains}. \
             Pass `brains: [\"<name>\"]` to scope to a single brain or `brains: [\"all\"]` to query every brain."
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::mcp::McpContext;
    use crate::mcp::tools::tests::create_test_context;
    use brain_persistence::db::schema::BrainUpsert;

    /// Register a brain with `projected=1` so it shows up in `list_brain_keys()`.
    fn register_active(ctx: &McpContext, brain_id: &str, name: &str) {
        ctx.stores
            .db_for_tests()
            .upsert_brain(&BrainUpsert {
                brain_id,
                name,
                prefix: "TST",
                roots_json: "[]",
                notes_json: "[]",
                aliases_json: "[]",
                archived: false,
            })
            .expect("upsert_brain should succeed");
    }

    /// Build an `McpContext` with empty `brain_id` for testing the strict
    /// ambient-resolution error path.
    async fn unscoped_ctx() -> (tempfile::TempDir, McpContext) {
        let (tmp, stores) =
            crate::stores::BrainStores::in_memory().expect("in_memory should succeed");
        let lance_path = tmp.path().join("lance");
        let store = brain_persistence::store::Store::open_or_create(&lance_path)
            .await
            .expect("lance store");
        let store_reader = brain_persistence::store::StoreReader::from_store(&store);
        let embedder: Arc<dyn crate::embedder::Embed> = Arc::new(crate::embedder::MockEmbedder);
        let ctx = McpContext {
            stores,
            search: Some(crate::search_service::SearchService {
                store: store_reader,
                embedder,
            }),
            writable_store: Some(store),
            metrics: Arc::new(crate::metrics::Metrics::new()),
        };
        (tmp, ctx)
    }

    #[tokio::test]
    async fn ambient_brain_errors_when_ctx_unscoped() {
        let (_dir, ctx) = unscoped_ctx().await;
        // brain_id="" — ambient resolution must surface a helpful error
        // instead of silently returning empty results.
        let err = resolve_scope(&ctx, None).expect_err("unscoped ctx should error");
        assert!(err.content[0].text.contains("No brain context"));
    }

    #[tokio::test]
    async fn all_sentinel_returns_federated() {
        let (_dir, ctx) = create_test_context().await;
        register_active(&ctx, "brain-a-id", "brain-a");
        register_active(&ctx, "brain-b-id", "brain-b");

        let scope =
            resolve_scope(&ctx, Some(&["all".to_string()])).expect("federated scope resolves");
        match scope {
            Scope::Federated(refs) => {
                let names: Vec<_> = refs.into_iter().map(|r| r.brain_name).collect();
                assert!(names.contains(&"brain-a".to_string()));
                assert!(names.contains(&"brain-b".to_string()));
            }
            Scope::Single(_) => panic!("expected Federated for ['all']"),
        }
    }

    #[tokio::test]
    async fn star_sentinel_equals_all() {
        let (_dir, ctx) = create_test_context().await;
        register_active(&ctx, "brain-a-id", "brain-a");

        let scope =
            resolve_scope(&ctx, Some(&["*".to_string()])).expect("federated scope resolves");
        assert!(scope.is_federated());
    }

    #[tokio::test]
    async fn single_named_brain_returns_single() {
        let (_dir, ctx) = create_test_context().await;
        register_active(&ctx, "brain-a-id", "brain-a");

        let scope = resolve_scope(&ctx, Some(&["brain-a".to_string()])).expect("scope resolves");
        match scope {
            Scope::Single(b) => {
                assert_eq!(b.brain_name, "brain-a");
                assert_eq!(b.brain_id, "brain-a-id");
            }
            Scope::Federated(_) => panic!("expected Single for one brain"),
        }
    }

    #[tokio::test]
    async fn multi_named_returns_federated() {
        let (_dir, ctx) = create_test_context().await;
        register_active(&ctx, "brain-a-id", "brain-a");
        register_active(&ctx, "brain-b-id", "brain-b");

        let scope = resolve_scope(&ctx, Some(&["brain-a".to_string(), "brain-b".to_string()]))
            .expect("scope resolves");
        match scope {
            Scope::Federated(refs) => assert_eq!(refs.len(), 2),
            Scope::Single(_) => panic!("expected Federated for two brains"),
        }
    }

    #[tokio::test]
    async fn unknown_brain_errors_with_name() {
        let (_dir, ctx) = create_test_context().await;
        let result = resolve_scope(&ctx, Some(&["nonexistent".to_string()]));
        let err = result.expect_err("unknown brain should error");
        let msg = &err.content[0].text;
        assert!(
            msg.contains("nonexistent"),
            "error should name the brain: {msg}"
        );
    }

    #[tokio::test]
    async fn resolve_single_scope_with_explicit_brain() {
        let (_dir, ctx) = create_test_context().await;
        register_active(&ctx, "brain-a-id", "brain-a");

        let r = resolve_single_scope(&ctx, Some("brain-a")).expect("resolves");
        assert_eq!(r.brain_name, "brain-a");
    }

    #[tokio::test]
    async fn resolve_single_scope_errors_when_unscoped() {
        let (_dir, ctx) = unscoped_ctx().await;
        let err = resolve_single_scope(&ctx, None).expect_err("unscoped ctx should error");
        assert!(err.content[0].text.contains("No brain context"));
    }

    #[tokio::test]
    async fn empty_string_in_brains_array_is_ignored() {
        let (_dir, ctx) = create_test_context().await;
        register_active(&ctx, "brain-a-id", "brain-a");

        // `brains: [""]` should NOT silently match the unscoped sentinel
        // (brain_id = "") and produce a cross-brain unfiltered query. After
        // empty-string filtering the array is empty, so resolution falls
        // through to the ambient brain.
        let scope = resolve_scope(&ctx, Some(&["".to_string()]))
            .expect("empty-only array should resolve to ambient");
        match scope {
            Scope::Single(b) => assert_eq!(b.brain_name, "test-brain"),
            Scope::Federated(_) => panic!("expected Single ambient"),
        }

        // `brains: ["", "brain-a"]` should treat the empty string as noise
        // and resolve to the named brain only.
        let scope = resolve_scope(&ctx, Some(&["".to_string(), "brain-a".to_string()]))
            .expect("named brain after empty entry resolves");
        match scope {
            Scope::Single(b) => assert_eq!(b.brain_name, "brain-a"),
            Scope::Federated(_) => panic!("expected Single brain-a"),
        }
    }

    #[tokio::test]
    async fn mixing_all_sentinel_with_named_brains_errors() {
        let (_dir, ctx) = create_test_context().await;
        register_active(&ctx, "brain-a-id", "brain-a");

        let err = resolve_scope(&ctx, Some(&["all".to_string(), "brain-a".to_string()]))
            .expect_err("mixed sentinel + named must error");
        let msg = &err.content[0].text;
        assert!(
            msg.contains("mix") && msg.contains("\"all\""),
            "error should explain the conflict: {msg}"
        );
    }

    #[tokio::test]
    async fn resolve_single_scope_treats_empty_string_as_ambient() {
        let (_dir, ctx) = create_test_context().await;
        // `brain: ""` (empty string after deserialization) must NOT match the
        // unscoped sentinel — it falls through to the ambient brain instead.
        let r = resolve_single_scope(&ctx, Some("")).expect("empty brain falls through");
        assert_eq!(r.brain_name, "test-brain");
        assert!(!r.brain_id.is_empty());
    }

    #[tokio::test]
    async fn unresolvable_default_error_lists_registered_brains() {
        let (_dir, ctx) = unscoped_ctx().await;
        register_active(&ctx, "brain-a-id", "brain-a");

        let err = resolve_scope(&ctx, None).expect_err("ctx is unscoped");
        let msg = &err.content[0].text;
        assert!(
            msg.contains("brain-a"),
            "error should list registered brains: {msg}"
        );
        assert!(
            msg.contains("\"all\""),
            "error should mention the all sentinel: {msg}"
        );
    }
}
