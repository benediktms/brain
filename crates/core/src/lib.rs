pub mod chunker;
pub mod embedder;
pub mod error;
pub mod scanner;
pub mod store;

pub mod prelude {
    pub use crate::chunker::{chunk_text, Chunk};
    pub use crate::embedder::Embedder;
    pub use crate::error::{BrainCoreError, Result};
    pub use crate::scanner::{scan_brain, ScannedFile};
    pub use crate::store::{QueryResult, Store};
}
