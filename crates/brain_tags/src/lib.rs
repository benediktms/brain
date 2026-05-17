//! Tag-domain logic — synonym clustering, alias resolution, and the
//! per-brain recluster job.
//!
//! - [`clustering`] is the pure algorithm core (IO-free, fixture-tested).
//! - [`recluster`] wires that algorithm into the v43 schema as a runnable
//!   per-brain job.
//! - [`domain`] holds the public-facing types that callers consume; these
//!   are kept independent of `brain_persistence::*Row` types so the SQL
//!   projection does not leak across the domain boundary.

pub mod clustering;
pub mod domain;
pub mod recluster;

pub use clustering::{ClusterParams, TagCandidate, TagCluster, cluster_tags};
pub use domain::{AliasCoverage, ClusterRun, TagAlias};
pub use recluster::{ReclusterReport, run_recluster};

// ---------------------------------------------------------------------------
// Embedder bridge
// ---------------------------------------------------------------------------
//
// `recluster::run_recluster` needs `embed_batch_async` — a blocking-friendly
// wrapper around `Embed::embed_batch`. When `embed` is enabled the helper
// comes from `brain_embedder`. When disabled, we still need the symbol to
// resolve so `recluster.rs` compiles in both feature modes — the stub mirrors
// the precedent in `brain_lib` and errors at runtime if reached. The runtime
// soft-gate at the caller (every reachable call site sits behind an
// `Option<Arc<dyn Embed>>` slot) prevents this stub from being executed in
// practice.

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
