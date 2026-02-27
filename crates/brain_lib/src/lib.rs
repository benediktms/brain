pub mod chunker;
pub mod db;
pub mod embedder;
pub mod error;
pub mod hash_gate;
pub mod pipeline;
pub mod scanner;
pub mod store;
pub mod watcher;

#[cfg(test)]
mod indexing_tests;

pub mod prelude {
    pub use crate::chunker::{chunk_text, Chunk};
    pub use crate::db::Db;
    pub use crate::embedder::{Embed, Embedder, MockEmbedder};
    pub use crate::error::{BrainCoreError, Result};
    pub use crate::hash_gate::{content_hash, GateVerdict, HashGate};
    pub use crate::pipeline::{IndexPipeline, ScanStats};
    pub use crate::scanner::{scan_brain, ScannedFile};
    pub use crate::store::{QueryResult, Store};
}
