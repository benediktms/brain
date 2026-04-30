pub mod capsule;
pub mod chunker;
pub mod config;
pub mod consolidation;
pub mod dedup;
pub mod doctor;
pub mod embedder;
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
pub mod metrics;
pub use brain_persistence::pagerank;
pub mod parser;
pub mod pipeline;
pub mod ports;
pub mod query_pipeline;
pub mod ranking;
pub mod records;
pub mod retrieval;
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
pub mod tasks;
pub mod tokens;
pub mod uri;
pub mod utils;
pub mod watcher;
pub mod work_queue;

pub mod prelude {
    pub use crate::chunker::{Chunk, chunk_document, chunk_text};
    pub use crate::doctor::{CheckStatus, DoctorReport};
    pub use crate::embedder::{Embed, Embedder};
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
    pub use brain_persistence::store::{QueryResult, Store, VectorSearchMode};
}
