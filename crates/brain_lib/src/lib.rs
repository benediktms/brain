pub mod capsule;
pub mod chunker;
pub mod config;
pub mod db;
pub mod doctor;
pub mod embedder;
pub mod error;
pub mod fs_permissions;
pub mod hash_gate;
pub mod links;
pub mod mcp;
pub mod metrics;
pub mod parser;
pub mod pipeline;
pub mod query_pipeline;
pub mod ranking;
pub mod records;
pub mod retrieval;
pub mod scanner;
pub mod store;
pub mod tasks;
pub mod tokens;
pub mod utils;
pub mod watcher;
pub mod work_queue;

pub mod prelude {
    pub use crate::chunker::{Chunk, chunk_document, chunk_text};
    pub use crate::db::Db;
    pub use crate::doctor::{CheckStatus, DoctorReport};
    pub use crate::embedder::{Embed, Embedder};
    pub use crate::error::{BrainCoreError, Result};
    pub use crate::hash_gate::{GateVerdict, HashGate};
    pub use crate::links::{Link, extract_links};
    pub use crate::metrics::Metrics;
    pub use crate::parser::{ParsedDocument, parse_document};
    pub use crate::pipeline::{IndexPipeline, ScanStats, VacuumStats};
    pub use crate::query_pipeline::{FederatedPipeline, QueryPipeline};
    pub use crate::ranking::{
        FusionConfidence, RankedResult, RerankCandidate, RerankResult, Reranker, RerankerPolicy,
        WeightProfile,
    };
    pub use crate::retrieval::{ExpandResult, ExpandedMemory, MemoryStub, SearchResult};
    pub use crate::scanner::{ScannedFile, scan_brain};
    pub use crate::store::{QueryResult, Store};
    pub use crate::tokens::estimate_tokens;
    pub use crate::utils::content_hash;
    pub use crate::watcher::{BrainWatcher, FileEvent};
    pub use crate::work_queue::WorkQueue;
}
