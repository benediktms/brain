pub mod capsule;
pub mod chunker;
pub mod db;
pub mod embedder;
pub mod error;
pub mod hash_gate;
pub mod links;
pub mod mcp;
pub mod parser;
pub mod pipeline;
pub mod query_pipeline;
pub mod ranking;
pub mod retrieval;
pub mod scanner;
pub mod store;
pub mod tasks;
pub mod tokens;
pub mod utils;
pub mod watcher;

pub mod prelude {
    pub use crate::chunker::{Chunk, chunk_document, chunk_text};
    pub use crate::db::Db;
    pub use crate::embedder::{Embed, Embedder, MockEmbedder};
    pub use crate::error::{BrainCoreError, Result};
    pub use crate::hash_gate::{GateVerdict, HashGate, content_hash};
    pub use crate::links::{Link, extract_links};
    pub use crate::parser::{ParsedDocument, parse_document};
    pub use crate::pipeline::{IndexPipeline, ScanStats};
    pub use crate::scanner::{ScannedFile, scan_brain};
    pub use crate::store::{QueryResult, Store};
    pub use crate::tokens::estimate_tokens;
}
