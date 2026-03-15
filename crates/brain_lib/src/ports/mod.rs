//! Use-case-oriented persistence ports for the pipeline and query layers.
//!
//! Traits are defined here in `brain_lib`. Concrete implementations are
//! provided below (in the `impl` blocks at the bottom of this file) for the
//! types re-exported from `brain_persistence`.
//!
//! # Design
//! - **Use-case-oriented, not table-oriented.** Each trait groups the methods a
//!   single consumer actually calls.
//! - **Narrow.** No method appears in more than one trait.
//! - **Trait-object safe** where dynamic dispatch is needed (`Arc<dyn Trait>`).
//! - **Async** for LanceDB operations; SQLite methods are sync (the
//!   `with_read_conn`/`with_write_conn` wrappers handle blocking internally).
//!
//! # Scope
//! Only pipeline-facing traits are defined here: `ChunkIndexWriter`,
//! `ChunkSearcher`, `ChunkMetaReader`, `FileMetaReader`, `FtsSearcher`, and
//! `SchemaMeta`. `TaskPersistence` and `RecordPersistence` are deferred — the
//! task/record stores are event-sourced and require a separate extraction pass.

use std::collections::HashMap;

use crate::db::chunks::ChunkRow;
use crate::db::fts::FtsResult;
use crate::error::Result;
use crate::store::QueryResult;

// ---------------------------------------------------------------------------
// Write path — used by IndexPipeline, embed_poll
// ---------------------------------------------------------------------------

/// LanceDB write operations required by the indexing pipeline.
///
/// Consumers: `IndexPipeline`, `embed_poll::poll_stale_chunks`,
/// `embed_poll::poll_stale_tasks`.
pub trait ChunkIndexWriter: Send + Sync {
    /// Upsert all chunks for a file: matched chunks are updated, new ones
    /// inserted, orphaned chunks for this `file_id` are deleted.
    fn upsert_chunks<'a>(
        &'a self,
        file_id: &'a str,
        file_path: &'a str,
        chunks: &'a [(usize, &'a str)],
        embeddings: &'a [Vec<f32>],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;

    /// Delete all LanceDB chunks for a given `file_id`.
    fn delete_file_chunks<'a>(
        &'a self,
        file_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;

    /// Bulk-delete LanceDB chunks for a list of `file_ids` (orphan cleanup).
    fn delete_chunks_by_file_ids<'a>(
        &'a self,
        file_ids: &'a [String],
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a;

    /// Update the `file_path` column for all chunks belonging to `file_id`.
    fn update_file_path<'a>(
        &'a self,
        file_id: &'a str,
        new_path: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;
}

// ---------------------------------------------------------------------------
// Schema management — used by IndexPipeline startup
// ---------------------------------------------------------------------------

/// LanceDB schema version management and maintenance operations.
///
/// Consumers: `pipeline::ensure_schema_version`, `IndexPipeline::repair`,
/// `IndexPipeline::vacuum`, `embed_poll::self_heal_if_lance_missing`.
pub trait SchemaMeta: Send + Sync {
    /// Return `true` if the live LanceDB table schema matches the expected
    /// schema. Used for diagnostic logging.
    fn current_schema_matches_expected(
        &self,
    ) -> impl std::future::Future<Output = bool> + Send + '_;

    /// Drop and recreate the LanceDB `chunks` table with the current schema.
    ///
    /// Called during schema upgrades. Callers are responsible for clearing
    /// content hashes in SQLite so files get re-indexed.
    fn drop_and_recreate_table(
        &mut self,
    ) -> impl std::future::Future<Output = Result<()>> + Send + '_;

    /// Return all distinct `file_id`s that have chunks in LanceDB.
    ///
    /// Used by `IndexPipeline::doctor` for orphan detection.
    fn get_file_ids_with_chunks(
        &self,
    ) -> impl std::future::Future<Output = Result<std::collections::HashSet<String>>> + Send + '_;

    /// Force an optimize of the LanceDB table (compaction + auto-index).
    ///
    /// Used by `IndexPipeline::vacuum`.
    fn force_optimize(&self) -> impl std::future::Future<Output = ()> + Send + '_;
}

// ---------------------------------------------------------------------------
// Read path — used by QueryPipeline (vector search)
// ---------------------------------------------------------------------------

/// LanceDB vector search required by the query pipeline.
///
/// Consumers: `QueryPipeline`, `FederatedPipeline`.
pub trait ChunkSearcher: Send + Sync {
    /// Search for the top-`top_k` most similar chunks to `embedding`.
    fn query<'a>(
        &'a self,
        embedding: &'a [f32],
        top_k: usize,
        nprobes: usize,
    ) -> impl std::future::Future<Output = Result<Vec<QueryResult>>> + Send + 'a;
}

// ---------------------------------------------------------------------------
// Read path — used by QueryPipeline (SQLite chunk enrichment)
// ---------------------------------------------------------------------------

/// SQLite chunk enrichment and summary loading required by the query pipeline.
///
/// Consumers: `QueryPipeline::search_ranked`, `QueryPipeline::expand`.
pub trait ChunkMetaReader: Send + Sync {
    /// Look up chunks by IDs, joining with `files` for path, timestamp, and
    /// PageRank score.
    fn get_chunks_by_ids(&self, chunk_ids: &[String]) -> Result<Vec<ChunkRow>>;

    /// Batch-load ML summaries for a set of `chunk_ids`.
    ///
    /// Returns a map from `chunk_id` to summary text. Prefers the most recent
    /// summary when multiple exist.
    fn get_ml_summaries_for_chunks(&self, chunk_ids: &[&str]) -> Result<HashMap<String, String>>;
}

// ---------------------------------------------------------------------------
// Read path — used by IndexPipeline doctor/scan
// ---------------------------------------------------------------------------

/// SQLite file metadata reads required by the maintenance pipeline.
///
/// Consumers: `IndexPipeline::doctor`, `pipeline::scan`.
pub trait FileMetaReader: Send + Sync {
    /// Return all active (non-deleted) `(file_id, path)` pairs.
    fn get_all_active_paths(&self) -> Result<Vec<(String, String)>>;

    /// Return `(file_id, path, content_hash)` for all active files.
    fn get_files_with_hashes(&self) -> Result<Vec<(String, String, Option<String>)>>;

    /// Return `(file_id, path)` pairs for files stuck in `indexing_started`.
    fn find_stuck_files(&self) -> Result<Vec<(String, String)>>;
}

// ---------------------------------------------------------------------------
// Read path — FTS search
// ---------------------------------------------------------------------------

/// Full-text search over the SQLite FTS5 index.
///
/// Consumers: `QueryPipeline::search_ranked`.
pub trait FtsSearcher: Send + Sync {
    /// Search the FTS5 index and return BM25-ranked results (scores
    /// normalized to [0, 1]).
    fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<FtsResult>>;
}

// ---------------------------------------------------------------------------
// Concrete implementations
//
// brain_lib depends on brain_persistence, so trait impls for concrete types
// from brain_persistence live here (in brain_lib), not in brain_persistence.
// This avoids a circular dependency.
// ---------------------------------------------------------------------------

use crate::db::Db;
use crate::store::{Store, StoreReader};

// -- ChunkIndexWriter for Store ---------------------------------------------

impl ChunkIndexWriter for Store {
    fn upsert_chunks<'a>(
        &'a self,
        file_id: &'a str,
        file_path: &'a str,
        chunks: &'a [(usize, &'a str)],
        embeddings: &'a [Vec<f32>],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        Store::upsert_chunks(self, file_id, file_path, chunks, embeddings)
    }

    fn delete_file_chunks<'a>(
        &'a self,
        file_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        Store::delete_file_chunks(self, file_id)
    }

    fn delete_chunks_by_file_ids<'a>(
        &'a self,
        file_ids: &'a [String],
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
        Store::delete_chunks_by_file_ids(self, file_ids)
    }

    fn update_file_path<'a>(
        &'a self,
        file_id: &'a str,
        new_path: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        Store::update_file_path(self, file_id, new_path)
    }
}

// -- SchemaMeta for Store --------------------------------------------------

impl SchemaMeta for Store {
    fn current_schema_matches_expected(
        &self,
    ) -> impl std::future::Future<Output = bool> + Send + '_ {
        Store::current_schema_matches_expected(self)
    }

    fn drop_and_recreate_table(
        &mut self,
    ) -> impl std::future::Future<Output = Result<()>> + Send + '_ {
        Store::drop_and_recreate_table(self)
    }

    fn get_file_ids_with_chunks(
        &self,
    ) -> impl std::future::Future<Output = Result<std::collections::HashSet<String>>> + Send + '_
    {
        Store::get_file_ids_with_chunks(self)
    }

    fn force_optimize(&self) -> impl std::future::Future<Output = ()> + Send + '_ {
        self.optimizer().force_optimize()
    }
}

// -- ChunkSearcher for StoreReader -----------------------------------------

impl ChunkSearcher for StoreReader {
    fn query<'a>(
        &'a self,
        embedding: &'a [f32],
        top_k: usize,
        nprobes: usize,
    ) -> impl std::future::Future<Output = Result<Vec<QueryResult>>> + Send + 'a {
        StoreReader::query(self, embedding, top_k, nprobes)
    }
}

// -- ChunkSearcher for Store (Store also supports read queries) ------------

impl ChunkSearcher for Store {
    fn query<'a>(
        &'a self,
        embedding: &'a [f32],
        top_k: usize,
        nprobes: usize,
    ) -> impl std::future::Future<Output = Result<Vec<QueryResult>>> + Send + 'a {
        Store::query(self, embedding, top_k, nprobes)
    }
}

// -- ChunkMetaReader for Db ------------------------------------------------

impl ChunkMetaReader for Db {
    fn get_chunks_by_ids(&self, chunk_ids: &[String]) -> Result<Vec<ChunkRow>> {
        self.with_read_conn(|conn| crate::db::chunks::get_chunks_by_ids(conn, chunk_ids))
    }

    fn get_ml_summaries_for_chunks(&self, chunk_ids: &[&str]) -> Result<HashMap<String, String>> {
        self.with_read_conn(|conn| {
            crate::db::summaries::get_ml_summaries_for_chunks(conn, chunk_ids)
        })
    }
}

// -- FileMetaReader for Db -------------------------------------------------

impl FileMetaReader for Db {
    fn get_all_active_paths(&self) -> Result<Vec<(String, String)>> {
        self.with_read_conn(crate::db::files::get_all_active_paths)
    }

    fn get_files_with_hashes(&self) -> Result<Vec<(String, String, Option<String>)>> {
        self.with_read_conn(crate::db::files::get_files_with_hashes)
    }

    fn find_stuck_files(&self) -> Result<Vec<(String, String)>> {
        self.with_read_conn(crate::db::files::find_stuck_files)
    }
}

// -- FtsSearcher for Db ----------------------------------------------------

impl FtsSearcher for Db {
    fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<FtsResult>> {
        let query = query.to_string();
        self.with_read_conn(move |conn| crate::db::fts::search_fts(conn, &query, limit))
    }
}

// ---------------------------------------------------------------------------
// Mock implementations for testing
// ---------------------------------------------------------------------------

#[cfg(test)]
pub mod mock;
