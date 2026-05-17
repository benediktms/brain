//! Read engine for brain memory — query → embedding → FTS+vector fusion →
//! ranking → LOD-aware projection.
//!
//! - [`capsule`] generates compact stubs from chunk content for search results.
//! - [`lod`] holds the LOD (level-of-detail) domain types + persistence port.
//! - [`retrieval`] defines the shared search result shapes (`SearchResult`,
//!   `MemoryStub`, `MemoryKind`).
//! - [`ranking`] implements signal scoring, weight profiles, and score fusion.
//! - [`hierarchy`] aggregates derived summaries by directory or tag scope.
//! - [`lod_resolver`] selects the right LOD level per result based on budget.
//! - [`query_pipeline`] orchestrates the full query → ranked-result flow.
//! - [`domain`] holds the public domain types (`ReflectedEpisode`) used by
//!   handlers; row types from `brain_persistence` are hidden behind
//!   `From<...Row>` impls at this boundary.
//! - [`ports`] holds the cross-crate abstractions over `brain_persistence`
//!   types (`VectorSearchStrategy` over `VectorSearchMode`); brn-2fe.15
//!   will extend these with the `VectorStore` trait.

pub mod capsule;
pub mod domain;
pub mod hierarchy;
pub mod lod;
pub mod lod_resolver;
pub mod ports;
pub mod query_pipeline;
pub mod ranking;
pub mod retrieval;

pub use domain::ReflectedEpisode;
pub use ports::VectorSearchStrategy;

// ---------------------------------------------------------------------------
// Embedder bridge
// ---------------------------------------------------------------------------
//
// `query_pipeline` needs `embed_batch_async` — a blocking-friendly wrapper
// around `Embed::embed_batch`. When `embed` is enabled the helper comes from
// `brain_embedder`. When disabled, the stub here errors at runtime if reached.
// Every reachable call site sits behind an `Option<Arc<dyn Embed>>` soft-gate.

#[cfg(feature = "embed")]
pub use brain_embedder::embed_batch_async;

#[cfg(not(feature = "embed"))]
pub(crate) async fn embed_batch_async(
    _embedder: &std::sync::Arc<dyn brain_core::ports::Embed>,
    _texts: Vec<String>,
) -> brain_core::error::Result<Vec<Vec<f32>>> {
    Err(brain_core::error::BrainCoreError::Embedding(
        "embed_batch_async called with `embed` feature disabled at compile time \
         — every reachable call site should sit behind an `Option<Arc<dyn Embed>>` \
         soft-gate; reaching this stub means the gate was bypassed"
            .into(),
    ))
}
