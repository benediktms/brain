//! Core persistence-port trait definitions shared across the workspace.
//!
//! These traits are framework-free: their method signatures use only stdlib
//! and `brain_core` types. Production implementations live in
//! `brain_persistence` (where the storage types are defined) and `brain_lib`
//! (for blanket adapters bridging brain_lib-internal stores).
//!
//! # Design
//! - **Use-case-oriented, not table-oriented.** Each trait groups the methods
//!   a single consumer actually calls.
//! - **Narrow.** No method appears in more than one trait.
//! - **Trait-object safe** where dynamic dispatch is needed (`Arc<dyn Trait>`).
//! - **Async** for LanceDB-style operations; SQLite methods stay sync (the
//!   `with_read_conn` / `with_write_conn` wrappers handle blocking internally).
//!
//! This module only carries the traits whose signatures can stand alone
//! without referencing types from `brain_persistence`. Row-coupled traits and
//! their concrete impls live in `brain_persistence::ports`; `brain_lib::ports`
//! re-exports both so callers have a single import path.

use crate::error::Result;

#[cfg(any(test, feature = "test-utils"))]
pub mod mock;

// ---------------------------------------------------------------------------
// LanceDB write path — chunk upsert + delete
// ---------------------------------------------------------------------------

/// LanceDB write operations required by the indexing pipeline.
pub trait ChunkIndexWriter: Send + Sync {
    /// Upsert all chunks for a file: matched chunks are updated, new ones
    /// inserted, orphaned chunks for this `file_id` within `brain_id` are deleted.
    fn upsert_chunks<'a>(
        &'a self,
        file_id: &'a str,
        file_path: &'a str,
        brain_id: &'a str,
        chunks: &'a [(usize, &'a str)],
        embeddings: &'a [Vec<f32>],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;

    /// Delete all LanceDB chunks for a given `file_id` within `brain_id`.
    fn delete_file_chunks<'a>(
        &'a self,
        file_id: &'a str,
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;

    /// Bulk-delete LanceDB chunks for a list of `file_ids` within `brain_id`.
    fn delete_chunks_by_file_ids<'a>(
        &'a self,
        file_ids: &'a [String],
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a;

    /// Update the `file_path` column for all chunks belonging to `file_id` within `brain_id`.
    fn update_file_path<'a>(
        &'a self,
        file_id: &'a str,
        new_path: &'a str,
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;
}

// ---------------------------------------------------------------------------
// LanceDB schema management — used at pipeline startup + vacuum
// ---------------------------------------------------------------------------

/// LanceDB schema version management and maintenance operations.
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

    /// Return all distinct `file_id`s that have chunks in LanceDB for a brain.
    ///
    /// Used for orphan detection by the maintenance doctor pass.
    fn get_file_ids_with_chunks<'a>(
        &'a self,
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<std::collections::HashSet<String>>> + Send + 'a;

    /// Force an optimize of the LanceDB table (compaction + auto-index).
    fn force_optimize(&self) -> impl std::future::Future<Output = ()> + Send + '_;

    /// Aggressively prune LanceDB version manifests older than `older_than`.
    ///
    /// `force_optimize` uses LanceDB's default version retention; this method
    /// lets vacuum honour a user-supplied `--older-than` cutoff (including
    /// `--older-than 0` for immediate disk reclamation) on the LanceDB side
    /// in addition to the SQLite soft-delete side.
    fn prune_versions(
        &self,
        older_than: std::time::Duration,
    ) -> impl std::future::Future<Output = ()> + Send + '_;
}

// ---------------------------------------------------------------------------
// SQLite read path — file metadata reads for maintenance + scan
// ---------------------------------------------------------------------------

/// SQLite file metadata reads required by the maintenance pipeline.
pub trait FileMetaReader: Send + Sync {
    /// Return all active (non-deleted) `(file_id, path)` pairs.
    fn get_all_active_paths(&self) -> Result<Vec<(String, String)>>;

    /// Return active (non-deleted) `(file_id, path)` pairs for a specific brain.
    /// When `brain_id` is empty, returns all active files.
    fn get_active_paths_for_brain(&self, brain_id: &str) -> Result<Vec<(String, String)>>;

    /// Return `(file_id, path, content_hash)` for all active files.
    fn get_files_with_hashes(&self) -> Result<Vec<(String, String, Option<String>)>>;

    /// Return `(file_id, path)` pairs for files stuck in `indexing_started`.
    fn find_stuck_files(&self) -> Result<Vec<(String, String)>>;

    /// Get the stored content hash for a file.
    fn get_content_hash(&self, file_id: &str) -> Result<Option<String>>;

    /// Get the stored chunker version for a file.
    fn get_chunker_version(&self, file_id: &str) -> Result<Option<u32>>;
}

// ---------------------------------------------------------------------------
// LanceDB write path — summary embeddings
// ---------------------------------------------------------------------------

/// LanceDB write operations for summary (episode/reflection) embeddings.
pub trait SummaryStoreWriter: Send + Sync {
    /// Upsert a summary embedding. Uses `file_id = "sum:{summary_id}"` so
    /// each summary occupies exactly one vector row.
    fn upsert_summary<'a>(
        &'a self,
        summary_id: &'a str,
        content: &'a str,
        brain_id: &'a str,
        embedding: &'a [f32],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;
}
