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
pub use brain_core::ports::{
    BrainRegistry, ChunkIndexWriter, FileMetaReader, SchemaMeta, SummaryStoreWriter,
};
pub use brain_persistence::ports::{
    BrainManager, ChunkMetaReader, ChunkMetaWriter, ChunkSearcher, EmbeddingOps, EmbeddingResetter,
    EpisodeReader, EpisodeWriter, FileMetaWriter, FtsSearcher, GraphLinkReader, JobPersistence,
    JobQueue, LinkWriter, MaintenanceOps, ProcedureWriter, ProviderStore, ReflectionWriter,
    StatusReader, SummaryReader, SummaryWriter, TagAliasReader, TagAliasWriter,
};

use crate::error::Result;

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

// -- DerivedSummaryStore for Db --------------------------------------------

use brain_persistence::db::Db;

impl DerivedSummaryStore for Db {
    fn generate_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<GeneratedScopeSummary> {
        let scope_type = scope_type.as_str().to_string();
        let scope_value = scope_value.to_string();
        let generated = self.with_write_conn(move |conn| {
            brain_persistence::derived_summaries::generate_scope_summary(
                conn,
                &scope_type,
                &scope_value,
            )
        })?;

        Ok(GeneratedScopeSummary {
            id: generated.id,
            source_content: generated.source_content,
            content_changed: generated.content_changed,
        })
    }

    fn get_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<Option<DerivedSummary>> {
        let scope_type = scope_type.as_str().to_string();
        let scope_value = scope_value.to_string();
        let row = self.with_read_conn(move |conn| {
            brain_persistence::derived_summaries::get_scope_summary(conn, &scope_type, &scope_value)
        })?;

        Ok(row.map(|summary| DerivedSummary {
            id: summary.id,
            scope_type: summary.scope_type,
            scope_value: summary.scope_value,
            content: summary.content,
            stale: summary.stale,
            generated_at: summary.generated_at,
        }))
    }

    fn mark_scope_stale(&self, scope_type: &ScopeType, scope_value: &str) -> Result<usize> {
        let scope_type = scope_type.as_str().to_string();
        let scope_value = scope_value.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::derived_summaries::mark_scope_stale(conn, &scope_type, &scope_value)
        })
    }

    fn search_derived_summaries(&self, query: &str, limit: usize) -> Result<Vec<DerivedSummary>> {
        let query = query.to_string();
        let rows = self.with_read_conn(move |conn| {
            brain_persistence::derived_summaries::search_derived_summaries(conn, &query, limit)
        })?;

        Ok(rows
            .into_iter()
            .map(|summary| DerivedSummary {
                id: summary.id,
                scope_type: summary.scope_type,
                scope_value: summary.scope_value,
                content: summary.content,
                stale: summary.stale,
                generated_at: summary.generated_at,
            })
            .collect())
    }

    fn list_stale_summaries(&self, limit: usize) -> Result<Vec<DerivedSummary>> {
        let rows = self.with_read_conn(move |conn| {
            brain_persistence::derived_summaries::list_stale_summaries(conn, limit)
        })?;

        Ok(rows
            .into_iter()
            .map(|summary| DerivedSummary {
                id: summary.id,
                scope_type: summary.scope_type,
                scope_value: summary.scope_value,
                content: summary.content,
                stale: summary.stale,
                generated_at: summary.generated_at,
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// LOD chunk operations — used by Retrieve+ LOD storage layer
// ---------------------------------------------------------------------------

use crate::lod::{LodChunk, LodChunkStore, LodLevel, LodMethod, UpsertLodChunk};
use brain_persistence::error::BrainCoreError;

impl LodChunkStore for Db {
    fn upsert_lod_chunk(&self, input: &UpsertLodChunk<'_>) -> Result<String> {
        if !input.lod_level.is_stored() {
            return Err(BrainCoreError::Database(
                "L2 chunks are passthrough and must not be stored".into(),
            ));
        }
        let id = ulid::Ulid::new().to_string();
        let now = chrono::Utc::now().to_rfc3339();
        let persist = brain_persistence::db::lod_chunks::InsertLodChunk {
            id: &id,
            object_uri: input.object_uri,
            brain_id: input.brain_id,
            lod_level: input.lod_level.as_str(),
            content: input.content,
            token_est: input.token_est,
            method: input.method.as_str(),
            model_id: input.model_id,
            source_hash: input.source_hash,
            created_at: &now,
            expires_at: input.expires_at,
            job_id: input.job_id,
        };
        self.with_write_conn(|conn| {
            brain_persistence::db::lod_chunks::upsert_lod_chunk(conn, &persist)
        })?;
        Ok(id)
    }

    fn get_lod_chunk(&self, object_uri: &str, lod_level: LodLevel) -> Result<Option<LodChunk>> {
        let uri = object_uri.to_string();
        let level = lod_level.as_str().to_string();
        let row = self.with_read_conn(move |conn| {
            brain_persistence::db::lod_chunks::get_lod_chunk(conn, &uri, &level)
        })?;
        row.map(row_to_lod_chunk).transpose()
    }

    fn get_lod_chunks_for_uri(&self, object_uri: &str) -> Result<Vec<LodChunk>> {
        let uri = object_uri.to_string();
        let rows = self.with_read_conn(move |conn| {
            brain_persistence::db::lod_chunks::get_lod_chunks_for_uri(conn, &uri)
        })?;
        rows.into_iter().map(row_to_lod_chunk).collect()
    }

    fn delete_lod_chunks_for_uri(&self, object_uri: &str) -> Result<usize> {
        let uri = object_uri.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::lod_chunks::delete_lod_chunks_for_uri(conn, &uri)
        })
    }

    fn delete_expired_lod_chunks(&self, now_iso: &str) -> Result<usize> {
        let now = now_iso.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::lod_chunks::delete_expired_lod_chunks(conn, &now)
        })
    }

    fn is_lod_fresh(
        &self,
        object_uri: &str,
        lod_level: LodLevel,
        current_source_hash: &str,
    ) -> Result<bool> {
        let chunk = LodChunkStore::get_lod_chunk(self, object_uri, lod_level)?;
        Ok(chunk.is_some_and(|c| c.source_hash == current_source_hash))
    }

    fn is_l1_fresh(&self, object_uri: &str, current_source_hash: &str) -> Result<bool> {
        let chunk = LodChunkStore::get_lod_chunk(self, object_uri, LodLevel::L1)?;
        Ok(chunk.is_some_and(|c| {
            if c.source_hash != current_source_hash {
                return false;
            }
            match &c.expires_at {
                None => true,
                Some(exp) => chrono::DateTime::parse_from_rfc3339(exp)
                    .map(|e| e > chrono::Utc::now())
                    .unwrap_or(false), // treat unparseable expiry as stale
            }
        }))
    }

    fn count_lod_chunks_by_brain(
        &self,
        brain_id: &str,
        lod_level: Option<LodLevel>,
    ) -> Result<usize> {
        let bid = brain_id.to_string();
        let level = lod_level.map(|l| l.as_str().to_string());
        self.with_read_conn(move |conn| {
            brain_persistence::db::lod_chunks::count_lod_chunks_by_brain(
                conn,
                &bid,
                level.as_deref(),
            )
        })
    }

    fn list_lod_chunks_by_brain(
        &self,
        brain_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<LodChunk>> {
        let bid = brain_id.to_string();
        let rows = self.with_read_conn(move |conn| {
            brain_persistence::db::lod_chunks::list_lod_chunks_by_brain(conn, &bid, limit, offset)
        })?;
        rows.into_iter().map(row_to_lod_chunk).collect()
    }
}
fn row_to_lod_chunk(row: brain_persistence::db::lod_chunks::LodChunkRow) -> Result<LodChunk> {
    let lod_level = LodLevel::parse(&row.lod_level).ok_or_else(|| {
        BrainCoreError::Database(format!(
            "unknown lod_level '{}' for chunk {}",
            row.lod_level, row.id
        ))
    })?;
    let method = LodMethod::parse(&row.method).ok_or_else(|| {
        BrainCoreError::Database(format!(
            "unknown method '{}' for chunk {}",
            row.method, row.id
        ))
    })?;
    Ok(LodChunk {
        id: row.id,
        object_uri: row.object_uri,
        brain_id: row.brain_id,
        lod_level,
        content: row.content,
        token_est: row.token_est,
        method,
        model_id: row.model_id,
        source_hash: row.source_hash,
        created_at: row.created_at,
        expires_at: row.expires_at,
        job_id: row.job_id,
    })
}
