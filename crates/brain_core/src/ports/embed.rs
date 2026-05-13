//! `Embed` trait — framework-free port for text-to-vector embedding.
//!
//! The concrete implementation backed by `candle` lives in the
//! `brain-embedder` adapter crate. In-memory deterministic mocks live in
//! [`super::mock`] behind the `test-utils` feature.

use crate::error::Result;

/// Trait for embedding text into vectors.
pub trait Embed: Send + Sync {
    /// Embed a batch of text strings, returning unit-norm vectors of
    /// length [`Self::hidden_size`].
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>>;

    /// Vector dimension produced by this embedder. Must remain constant
    /// across the lifetime of the instance — vector stores pin this at
    /// table creation time.
    fn hidden_size(&self) -> usize;

    /// Identifier of the embedder model + revision in use.
    ///
    /// Stamped onto cached embeddings and audit rows so consumers can
    /// invalidate cached vectors when the underlying model changes.
    fn version(&self) -> &str;
}
