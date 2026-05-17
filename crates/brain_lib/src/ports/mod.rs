//! Brain_lib-resident persistence-port residues.
//!
//! Most port trait definitions and their production implementations now live
//! in `brain_core::ports` (framework-free contracts) and
//! `brain_persistence::ports` (contracts whose signatures bind to
//! `brain_persistence` types, plus production impls for `Db` / `Store` /
//! `StoreReader`). This module re-exports both so every existing
//! `crate::ports::*` import keeps resolving.
//!
//! What remains here is the residue that genuinely belongs in brain_lib —
//! ports whose contracts reference brain_lib-local types (e.g.
//! `DerivedSummary`, `ScopeType` from `crate::hierarchy`), the blanket
//! adapters bridging brain_lib-internal stores (`DerivedSummaryStore`,
//! `LodChunkStore`) to the public-facing port traits, and the concrete
//! `impl ... for Db` blocks for traits owned by `crate::hierarchy` /
//! `crate::lod`. BrainStores adapter impls live next to their type in
//! `crate::stores`.

// Re-export every persistence-port trait so `crate::ports::*` paths keep
// working without code changes at call sites. Listed explicitly (rather
// than glob-imported) because each parent module also exposes its own
// `mock` submodule, and glob re-exports would ambiguate them.
use crate::error::Result;
pub use brain_core::ports::{
    BrainRegistry, ChunkIndexWriter, FileMetaReader, SchemaMeta, SummaryStoreWriter,
};
pub use brain_persistence::ports::{
    BrainManager, ChunkMetaReader, ChunkMetaWriter, ChunkSearcher, EmbeddingOps, EmbeddingResetter,
    EpisodeReader, EpisodeWriter, FileMetaWriter, FtsSearcher, GraphLinkReader, JobPersistence,
    JobQueue, LinkWriter, MaintenanceOps, ProcedureWriter, ProviderStore, ReflectionWriter,
    StatusReader, SummaryReader, SummaryWriter, TagAliasReader, TagAliasWriter,
};

#[cfg(any(test, feature = "test-utils"))]
pub mod mock;
// ---------------------------------------------------------------------------
// SQLite read/write path — derived summaries (hierarchy module)
// ---------------------------------------------------------------------------
//
// The `DerivedSummaryStore` trait is defined in `crate::hierarchy` alongside
// its types (`DerivedSummary`, `ScopeType`, `GeneratedScopeSummary`). The
// brain_lib-resident impls (concrete `Db` impl plus the blanket adapters
// from `DerivedSummaryStore` to the public `DerivedSummaryWriter` /
// `DerivedSummaryReader` ports) live here because the trait and the types
// it references both live in brain_lib.

use crate::hierarchy::{DerivedSummary, DerivedSummaryStore, GeneratedScopeSummary, ScopeType};

/// Write operations for derived scope summaries.
///
/// Consumers: hierarchy scope-summary generation paths.
pub trait DerivedSummaryWriter: Send + Sync {
    /// Generate or refresh a derived summary for a scope.
    fn generate_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<GeneratedScopeSummary>;

    /// Read back the current summary for a scope.
    fn get_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<Option<DerivedSummary>>;

    /// Mark a scope summary stale so sweep/recompute can pick it up.
    fn mark_scope_stale(&self, scope_type: &ScopeType, scope_value: &str) -> Result<usize>;
}

/// Read operations for querying derived scope summaries.
///
/// Consumers: MCP summary lookup/list paths and job sweeps.
pub trait DerivedSummaryReader: Send + Sync {
    /// Search derived summaries by query text.
    fn search_derived_summaries(&self, query: &str, limit: usize) -> Result<Vec<DerivedSummary>>;

    /// List stale summaries in oldest-first order.
    fn list_stale_summaries(&self, limit: usize) -> Result<Vec<DerivedSummary>>;
}

impl<T: DerivedSummaryStore + ?Sized> DerivedSummaryWriter for T {
    fn generate_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<GeneratedScopeSummary> {
        DerivedSummaryStore::generate_scope_summary(self, scope_type, scope_value)
    }

    fn get_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<Option<DerivedSummary>> {
        DerivedSummaryStore::get_scope_summary(self, scope_type, scope_value)
    }

    fn mark_scope_stale(&self, scope_type: &ScopeType, scope_value: &str) -> Result<usize> {
        DerivedSummaryStore::mark_scope_stale(self, scope_type, scope_value)
    }
}

impl<T: DerivedSummaryStore + ?Sized> DerivedSummaryReader for T {
    fn search_derived_summaries(&self, query: &str, limit: usize) -> Result<Vec<DerivedSummary>> {
        DerivedSummaryStore::search_derived_summaries(self, query, limit)
    }

    fn list_stale_summaries(&self, limit: usize) -> Result<Vec<DerivedSummary>> {
        DerivedSummaryStore::list_stale_summaries(self, limit)
    }
}

// `impl DerivedSummaryStore for Db` and `impl LodChunkStore for Db` now
// live next to their trait declarations in `brain_retrieval::hierarchy` and
// `brain_retrieval::lod` (Rust's orphan rule requires the impl to live with
// either the trait or the type, and the traits moved to brain_retrieval).
// BrainStores adapter impls remain in `crate::stores`.
