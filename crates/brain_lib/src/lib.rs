pub mod capsule;
pub mod chunker;
pub mod config;
pub mod consolidation;
pub mod dedup;
pub mod doctor;
#[cfg(feature = "embed")]
pub use brain_embedder as embedder;

/// Stub `embedder` module surfaced when the `embed` feature is disabled.
///
/// Re-exposes the same item names callers depend on (`embed_batch_async`) so
/// that code paths which call into the embedder *unconditionally* still
/// type-check under `--no-default-features`. The runtime soft-gate at
/// [`crate::pipeline::IndexPipeline::embedder`] (an `Option`) prevents these
/// stubs from ever being reached in practice — they return
/// [`brain_core::error::BrainCoreError::Embedding`] defensively in case a
/// caller skips the gate.
#[cfg(not(feature = "embed"))]
pub mod embedder {
    use std::sync::Arc;

    use brain_core::error::{BrainCoreError, Result};
    // Re-export `Embed` (and `MockEmbedder` under test-utils) so callers that
    // import `crate::embedder::{Embed, ...}` or `brain_lib::embedder::Embed`
    // resolve in both feature modes. Mirrors the `pub use` in
    // `brain_embedder/src/lib.rs` so the alias and the stub expose the same
    // shape.
    pub use brain_core::ports::Embed;
    #[cfg(any(test, feature = "test-utils"))]
    pub use brain_core::ports::mock::MockEmbedder;

    pub async fn embed_batch_async(
        _embedder: &Arc<dyn Embed>,
        _texts: Vec<String>,
    ) -> Result<Vec<Vec<f32>>> {
        Err(BrainCoreError::Embedding(
            "embed_batch_async called with `embed` feature disabled at compile time \
                 — every reachable call site should sit behind an `IndexPipeline.embedder` \
                 or `McpContext::embedder()` soft-gate; reaching this stub means the gate \
                 was bypassed"
                .into(),
        ))
    }
}
pub use brain_persistence::error;
pub mod fs_permissions;
pub mod git;
pub mod hash_gate;
pub mod hierarchy;
pub mod ipc;
pub mod l0_abstract;
pub mod l0_generate;
pub mod llm;
pub mod lod;
pub mod lod_resolver;
pub use brain_persistence::links;
pub mod mcp;
pub use brain_core::metrics;
pub use brain_persistence::pagerank;
pub mod parser;
pub mod pipeline;
pub mod ports;
pub mod query_pipeline;
pub mod ranking;
pub mod retrieval;
pub use brain_sagas as sagas;
pub mod scanner;
pub mod search_service;
pub mod stores;
pub mod summarizer;
pub(crate) mod tags;
// Targeted public re-exports from `tags` so integration tests and `.7.2.5`'s
// future MCP/CLI surface can reach the recluster job without exposing the
// module namespace itself (preserves a deliberate review gate on what becomes
// public API).
pub use crate::tags::{ClusterParams, ReclusterReport, run_recluster};
pub use brain_core::tokens;
pub use brain_core::uri;
pub use brain_core::utils;
pub mod watcher;
pub mod work_queue;

pub mod prelude {
    pub use crate::chunker::{Chunk, chunk_document, chunk_text};
    pub use crate::doctor::{CheckStatus, DoctorReport};
    #[cfg(feature = "embed")]
    pub use crate::embedder::Embedder;
    pub use crate::error::{BrainCoreError, Result};
    pub use crate::hash_gate::{GateVerdict, HashGate};
    pub use crate::links::{Link, extract_links};
    pub use crate::metrics::Metrics;
    pub use crate::parser::{ParsedDocument, parse_document};
    pub use crate::pipeline::{IndexPipeline, ScanStats, VacuumStats};
    pub use crate::query_pipeline::{
        FederatedPipeline, FederatedRankedResult, QueryPipeline, SearchParams,
    };
    pub use crate::ranking::{
        FusionConfidence, RankedResult, RerankCandidate, RerankResult, Reranker, RerankerPolicy,
        WeightProfile,
    };
    pub use crate::retrieval::{MemoryStub, SearchResult};
    pub use crate::scanner::{ScannedFile, scan_brain};
    pub use crate::tokens::estimate_tokens;
    pub use crate::utils::content_hash;
    pub use crate::watcher::{BrainWatcher, FileEvent};
    pub use crate::work_queue::WorkQueue;
    pub use brain_core::ports::Embed;
    pub use brain_persistence::store::{QueryResult, Store, VectorSearchMode};
}
