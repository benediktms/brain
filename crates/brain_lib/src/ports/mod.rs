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

use crate::error::Result;
use brain_persistence::db::chunks::{ChunkPollRow, ChunkRow};
use brain_persistence::db::fts::{FtsResult, FtsSummaryResult};
use brain_persistence::db::summaries::SummaryPollRow;
use brain_persistence::db::tasks::queries::TaskPollRow;
use brain_persistence::store::{QueryResult, VectorSearchMode};

// ---------------------------------------------------------------------------
// Write path — used by IndexPipeline, embed_poll
// ---------------------------------------------------------------------------

/// LanceDB write operations required by the indexing pipeline.
///
/// Consumers: `IndexPipeline`, `embed_poll::poll_stale_chunks`,
/// `embed_poll::poll_stale_tasks`.
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

    /// Return all distinct `file_id`s that have chunks in LanceDB for a brain.
    ///
    /// Used by `IndexPipeline::doctor` for orphan detection.
    fn get_file_ids_with_chunks<'a>(
        &'a self,
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<std::collections::HashSet<String>>> + Send + 'a;

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
    ///
    /// `mode` controls the ANN (Approximate Nearest Neighbor) vs exact search
    /// tradeoff — see [`VectorSearchMode`].
    ///
    /// When `brain_id` is `Some`, results are restricted to that brain.
    /// When `None`, all brains are searched.
    fn query<'a>(
        &'a self,
        embedding: &'a [f32],
        top_k: usize,
        nprobes: usize,
        mode: VectorSearchMode,
        brain_id: Option<&'a str>,
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

    /// Batch-load (summary_id → kind) for a list of summary IDs.
    ///
    /// Used by the query pipeline to populate `summary_kind` on `sum:` prefixed
    /// candidates, enabling `derive_kind` to emit `"procedure"` instead of
    /// the default `"episode"`.
    fn get_summary_metadata(
        &self,
        summary_ids: &[String],
    ) -> Result<HashMap<String, brain_persistence::db::summaries::SummaryMeta>>;
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
// Read path — FTS search
// ---------------------------------------------------------------------------

/// Full-text search over the SQLite FTS5 index.
///
/// Consumers: `QueryPipeline::search_ranked`.
pub trait FtsSearcher: Send + Sync {
    /// Search the FTS5 index and return BM25-ranked results (scores
    /// normalized to [0, 1]).
    ///
    /// When `brain_ids` is `Some`, results are filtered to chunks belonging
    /// to the specified brains.  `None` means no filter (workspace-global).
    fn search_fts(
        &self,
        query: &str,
        limit: usize,
        brain_ids: Option<&[String]>,
    ) -> Result<Vec<FtsResult>>;

    /// Search the FTS5 summaries index (episodes + reflections) and return
    /// BM25-ranked results (scores normalized to [0, 1]).
    ///
    /// When `brain_ids` is `Some`, results are filtered to summaries belonging
    /// to the specified brains.  `None` means no filter (workspace-global).
    fn search_summaries_fts(
        &self,
        query: &str,
        limit: usize,
        brain_ids: Option<&[String]>,
    ) -> Result<Vec<FtsSummaryResult>>;
}

// ---------------------------------------------------------------------------
// Concrete implementations
//
// brain_lib depends on brain_persistence, so trait impls for concrete types
// from brain_persistence live here (in brain_lib), not in brain_persistence.
// This avoids a circular dependency.
// ---------------------------------------------------------------------------

use brain_persistence::db::Db;
use brain_persistence::store::{Store, StoreReader};

// -- ChunkIndexWriter for Store ---------------------------------------------

impl ChunkIndexWriter for Store {
    fn upsert_chunks<'a>(
        &'a self,
        file_id: &'a str,
        file_path: &'a str,
        brain_id: &'a str,
        chunks: &'a [(usize, &'a str)],
        embeddings: &'a [Vec<f32>],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        Store::upsert_chunks(self, file_id, file_path, brain_id, chunks, embeddings)
    }

    fn delete_file_chunks<'a>(
        &'a self,
        file_id: &'a str,
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        Store::delete_file_chunks(self, file_id, brain_id)
    }

    fn delete_chunks_by_file_ids<'a>(
        &'a self,
        file_ids: &'a [String],
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
        Store::delete_chunks_by_file_ids(self, file_ids, brain_id)
    }

    fn update_file_path<'a>(
        &'a self,
        file_id: &'a str,
        new_path: &'a str,
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        Store::update_file_path(self, file_id, new_path, brain_id)
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

    fn get_file_ids_with_chunks<'a>(
        &'a self,
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<std::collections::HashSet<String>>> + Send + 'a
    {
        Store::get_file_ids_with_chunks(self, brain_id)
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
        mode: VectorSearchMode,
        brain_id: Option<&'a str>,
    ) -> impl std::future::Future<Output = Result<Vec<QueryResult>>> + Send + 'a {
        StoreReader::query(self, embedding, top_k, nprobes, mode, brain_id)
    }
}

// -- ChunkSearcher for Store (Store also supports read queries) ------------

impl ChunkSearcher for Store {
    fn query<'a>(
        &'a self,
        embedding: &'a [f32],
        top_k: usize,
        nprobes: usize,
        mode: VectorSearchMode,
        brain_id: Option<&'a str>,
    ) -> impl std::future::Future<Output = Result<Vec<QueryResult>>> + Send + 'a {
        Store::query(self, embedding, top_k, nprobes, mode, brain_id)
    }
}

// -- ChunkMetaReader for Db ------------------------------------------------

impl ChunkMetaReader for Db {
    fn get_chunks_by_ids(&self, chunk_ids: &[String]) -> Result<Vec<ChunkRow>> {
        self.with_read_conn(|conn| {
            brain_persistence::db::chunks::get_chunks_by_ids(conn, chunk_ids)
        })
    }

    fn get_ml_summaries_for_chunks(&self, chunk_ids: &[&str]) -> Result<HashMap<String, String>> {
        self.with_read_conn(|conn| {
            brain_persistence::db::summaries::get_ml_summaries_for_chunks(conn, chunk_ids)
        })
    }

    fn get_summary_metadata(
        &self,
        summary_ids: &[String],
    ) -> Result<HashMap<String, brain_persistence::db::summaries::SummaryMeta>> {
        let ids = summary_ids.to_vec();
        self.with_read_conn(move |conn| {
            brain_persistence::db::summaries::get_summary_metadata(conn, &ids)
        })
    }
}

// -- FileMetaReader for Db -------------------------------------------------

impl FileMetaReader for Db {
    fn get_all_active_paths(&self) -> Result<Vec<(String, String)>> {
        self.with_read_conn(brain_persistence::db::files::get_all_active_paths)
    }

    fn get_active_paths_for_brain(&self, brain_id: &str) -> Result<Vec<(String, String)>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::files::get_active_paths_for_brain(conn, &brain_id)
        })
    }

    fn get_files_with_hashes(&self) -> Result<Vec<(String, String, Option<String>)>> {
        self.with_read_conn(brain_persistence::db::files::get_files_with_hashes)
    }

    fn find_stuck_files(&self) -> Result<Vec<(String, String)>> {
        self.with_read_conn(brain_persistence::db::files::find_stuck_files)
    }

    fn get_content_hash(&self, file_id: &str) -> Result<Option<String>> {
        let file_id = file_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::files::get_content_hash(conn, &file_id)
        })
    }

    fn get_chunker_version(&self, file_id: &str) -> Result<Option<u32>> {
        let file_id = file_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::files::get_chunker_version(conn, &file_id)
        })
    }
}

// -- FtsSearcher for Db ----------------------------------------------------

impl FtsSearcher for Db {
    fn search_fts(
        &self,
        query: &str,
        limit: usize,
        brain_ids: Option<&[String]>,
    ) -> Result<Vec<FtsResult>> {
        let query = query.to_string();
        let brain_ids = brain_ids.map(|ids| ids.to_vec());
        self.with_read_conn(move |conn| {
            brain_persistence::db::fts::search_fts(conn, &query, limit, brain_ids.as_deref())
        })
    }

    fn search_summaries_fts(
        &self,
        query: &str,
        limit: usize,
        brain_ids: Option<&[String]>,
    ) -> Result<Vec<FtsSummaryResult>> {
        let query = query.to_string();
        let brain_ids = brain_ids.map(|ids| ids.to_vec());
        self.with_read_conn(move |conn| {
            brain_persistence::db::fts::search_summaries_fts(
                conn,
                &query,
                limit,
                brain_ids.as_deref(),
            )
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite write path — file metadata
// ---------------------------------------------------------------------------

/// SQLite file-metadata write operations required by the indexing pipeline.
///
/// Consumers: `IndexPipeline`, `hash_gate::HashGate`, `pipeline::scan`,
/// `pipeline::maintenance`.
pub trait FileMetaWriter: Send + Sync {
    /// Get or create a file record for the given path. Returns `(file_id, is_new)`.
    /// When `brain_id` is non-empty, it is set on new files and used to match
    /// existing files (for multi-brain scoping).
    fn get_or_create_file_id(&self, path: &str, brain_id: &str) -> Result<(String, bool)>;

    /// Soft-delete a file by path. Returns the `file_id` if found.
    fn handle_delete(&self, path: &str) -> Result<Option<String>>;

    /// Handle a file rename: update the path in SQLite.
    fn handle_rename(&self, file_id: &str, new_path: &str) -> Result<()>;

    /// Hard-delete soft-deleted files older than `older_than_ts` (Unix seconds).
    /// Returns the list of purged `file_id`s.
    fn purge_deleted_files(&self, older_than_ts: i64) -> Result<Vec<String>>;

    /// Clear all content hashes and reset indexing state (forces full re-index).
    /// Returns the number of rows updated.
    fn clear_all_content_hashes(&self) -> Result<usize>;

    /// Clear content hash for a single file path (forces re-index of that file).
    /// Returns `true` if the file was found and updated.
    fn clear_content_hash_by_path(&self, path: &str) -> Result<bool>;

    /// Set the indexing state for a file (`idle` | `indexing_started` | `indexed`).
    fn set_indexing_state(&self, file_id: &str, state: &str) -> Result<()>;

    fn reset_stuck_file_for_reindex(&self, file_id: &str) -> Result<()>;

    /// Mark a file as fully indexed: update hash, chunker version, timestamp, and state.
    ///
    /// `disk_modified_at` is the file's OS-level mtime (Unix seconds).
    fn mark_indexed(
        &self,
        file_id: &str,
        content_hash: &str,
        chunker_version: u32,
        disk_modified_at: Option<i64>,
    ) -> Result<()>;

    /// Count files where `chunker_version` doesn't match `current_version` (stale or NULL).
    fn count_stale_chunker_version(&self, current_version: u32) -> Result<usize>;
}

// -- FileMetaWriter for Db -------------------------------------------------

impl FileMetaWriter for Db {
    fn get_or_create_file_id(&self, path: &str, brain_id: &str) -> Result<(String, bool)> {
        let path = path.to_string();
        let brain_id = brain_id.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::files::get_or_create_file_id(conn, &path, &brain_id)
        })
    }

    fn handle_delete(&self, path: &str) -> Result<Option<String>> {
        let path = path.to_string();
        self.with_write_conn(move |conn| brain_persistence::db::files::handle_delete(conn, &path))
    }

    fn handle_rename(&self, file_id: &str, new_path: &str) -> Result<()> {
        let file_id = file_id.to_string();
        let new_path = new_path.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::files::handle_rename(conn, &file_id, &new_path)
        })
    }

    fn purge_deleted_files(&self, older_than_ts: i64) -> Result<Vec<String>> {
        self.with_write_conn(move |conn| {
            brain_persistence::db::files::purge_deleted_files(conn, older_than_ts)
        })
    }

    fn clear_all_content_hashes(&self) -> Result<usize> {
        self.with_write_conn(brain_persistence::db::files::clear_all_content_hashes)
    }

    fn clear_content_hash_by_path(&self, path: &str) -> Result<bool> {
        let path = path.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::files::clear_content_hash_by_path(conn, &path)
        })
    }

    fn set_indexing_state(&self, file_id: &str, state: &str) -> Result<()> {
        let file_id = file_id.to_string();
        let state = state.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::files::set_indexing_state(conn, &file_id, &state)
        })
    }

    fn reset_stuck_file_for_reindex(&self, file_id: &str) -> Result<()> {
        let file_id = file_id.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::files::reset_stuck_file_for_reindex(conn, &file_id)
        })
    }

    fn mark_indexed(
        &self,
        file_id: &str,
        content_hash: &str,
        chunker_version: u32,
        disk_modified_at: Option<i64>,
    ) -> Result<()> {
        let file_id = file_id.to_string();
        let content_hash = content_hash.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::files::mark_indexed(
                conn,
                &file_id,
                &content_hash,
                chunker_version,
                disk_modified_at,
            )
        })
    }

    fn count_stale_chunker_version(&self, current_version: u32) -> Result<usize> {
        self.with_read_conn(move |conn| {
            brain_persistence::db::files::count_stale_chunker_version(conn, current_version)
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite write path — chunk metadata
// ---------------------------------------------------------------------------

/// SQLite chunk-metadata write operations required by the indexing pipeline.
///
/// Consumers: `IndexPipeline`, `embed_poll::poll_stale_tasks`,
/// `embed_poll::poll_stale_chunks`, `pipeline::indexing`.
pub trait ChunkMetaWriter: Send + Sync {
    /// Replace all chunk metadata for a file in a single transaction.
    /// Deletes existing chunks for `file_id` and inserts new ones.
    /// When `brain_id` is non-empty, it is set on all inserted chunks.
    fn replace_chunk_metadata(
        &self,
        file_id: &str,
        chunks: &[brain_persistence::db::chunks::ChunkMeta],
        brain_id: &str,
    ) -> Result<()>;

    /// Get ordered chunk hashes for a file (by `chunk_ord`).
    fn get_chunk_hashes(&self, file_id: &str) -> Result<Vec<String>>;

    /// Set `embedded_at` on a batch of chunks, marking them as current in LanceDB.
    fn mark_chunks_embedded(&self, chunk_ids: &[&str], timestamp: i64) -> Result<()>;

    /// Upsert a task capsule chunk into SQLite (creates synthetic `files` row if needed).
    /// When `brain_id` is non-empty, it is set on both the files row and chunk.
    fn upsert_task_chunk(
        &self,
        task_file_id: &str,
        capsule_text: &str,
        brain_id: &str,
    ) -> Result<()>;

    /// Upsert a record capsule chunk into SQLite (creates synthetic `files` row if needed).
    /// When `brain_id` is non-empty, it is set on both the files row and chunk.
    fn upsert_record_chunk(
        &self,
        record_file_id: &str,
        capsule_text: &str,
        brain_id: &str,
    ) -> Result<()>;
}

// -- ChunkMetaWriter for Db ------------------------------------------------

impl ChunkMetaWriter for Db {
    fn replace_chunk_metadata(
        &self,
        file_id: &str,
        chunks: &[brain_persistence::db::chunks::ChunkMeta],
        brain_id: &str,
    ) -> Result<()> {
        // ChunkMeta is not Clone/Send, so we must call within the closure directly.
        // Caller must ensure chunks slice lifetime covers the closure.
        // We delegate via with_write_conn using a shared reference approach.
        self.with_write_conn(|conn| {
            brain_persistence::db::chunks::replace_chunk_metadata(conn, file_id, chunks, brain_id)
        })
    }

    fn get_chunk_hashes(&self, file_id: &str) -> Result<Vec<String>> {
        let file_id = file_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::chunks::get_chunk_hashes(conn, &file_id)
        })
    }

    fn mark_chunks_embedded(&self, chunk_ids: &[&str], timestamp: i64) -> Result<()> {
        let ids: Vec<String> = chunk_ids.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            brain_persistence::db::chunks::mark_chunks_embedded(conn, &refs, timestamp)
        })
    }

    fn upsert_task_chunk(
        &self,
        task_file_id: &str,
        capsule_text: &str,
        brain_id: &str,
    ) -> Result<()> {
        let task_file_id = task_file_id.to_string();
        let capsule_text = capsule_text.to_string();
        let brain_id = brain_id.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::chunks::upsert_task_chunk(
                conn,
                &task_file_id,
                &capsule_text,
                &brain_id,
            )
        })
    }

    fn upsert_record_chunk(
        &self,
        record_file_id: &str,
        capsule_text: &str,
        brain_id: &str,
    ) -> Result<()> {
        let record_file_id = record_file_id.to_string();
        let capsule_text = capsule_text.to_string();
        let brain_id = brain_id.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::chunks::upsert_record_chunk(
                conn,
                &record_file_id,
                &capsule_text,
                &brain_id,
            )
        })
    }
}
// ---------------------------------------------------------------------------
// SQLite write path — links
// ---------------------------------------------------------------------------

/// Link write operations required by the indexing pipeline.
///
/// Consumers: `pipeline::indexing`.
pub trait LinkWriter: Send + Sync {
    /// Atomically replace all outgoing links for a file.
    fn replace_links(&self, file_id: &str, links: &[brain_persistence::links::Link]) -> Result<()>;
}

// -- LinkWriter for Db -------------------------------------------------------

impl LinkWriter for Db {
    fn replace_links(&self, file_id: &str, links: &[brain_persistence::links::Link]) -> Result<()> {
        let file_id = file_id.to_string();
        let links = links.to_vec();
        self.with_write_conn(move |conn| {
            brain_persistence::db::links::replace_links(conn, &file_id, &links)
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite write path — self-heal / embedded_at reset
// ---------------------------------------------------------------------------

/// Reset all `embedded_at` columns so items are re-embedded on the next poll cycle.
///
/// Consumers: `embed_poll::self_heal_if_lance_missing`.
pub trait EmbeddingResetter: Send + Sync {
    /// Reset `embedded_at` to NULL on all tasks rows in this database.
    fn reset_tasks_embedded_at(&self) -> Result<()>;

    /// Reset `embedded_at` to NULL on all chunks rows in this database.
    fn reset_chunks_embedded_at(&self) -> Result<()>;

    /// Reset `embedded_at` to NULL on all records rows in this database.
    fn reset_records_embedded_at(&self) -> Result<()>;
}

// -- EmbeddingResetter for Db ----------------------------------------------

impl EmbeddingResetter for Db {
    fn reset_tasks_embedded_at(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            conn.execute_batch("UPDATE tasks SET embedded_at = NULL;")?;
            Ok(())
        })
    }

    fn reset_chunks_embedded_at(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            conn.execute_batch("UPDATE chunks SET embedded_at = NULL;")?;
            Ok(())
        })
    }

    fn reset_records_embedded_at(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            conn.execute_batch("UPDATE records SET embedded_at = NULL;")?;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite read path — summaries
// ---------------------------------------------------------------------------

/// SQLite summary read operations required by the consolidation pipeline.
///
/// Consumers: `pipeline::consolidation::ConsolidationScheduler`.
pub trait SummaryReader: Send + Sync {
    /// Find `(chunk_id, content)` pairs that have no ML summary from `summarizer`.
    /// Returns results ordered by most recently indexed first, up to `limit` entries.
    fn find_chunks_lacking_summary(
        &self,
        summarizer: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>>;
}

// -- SummaryReader for Db --------------------------------------------------

impl SummaryReader for Db {
    fn find_chunks_lacking_summary(
        &self,
        summarizer: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        let summarizer = summarizer.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::summaries::find_chunks_lacking_summary(conn, &summarizer, limit)
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite write path — summaries
// ---------------------------------------------------------------------------

/// SQLite summary write operations required by the consolidation pipeline.
///
/// Consumers: `pipeline::consolidation::ConsolidationScheduler`.
pub trait SummaryWriter: Send + Sync {
    /// Store an ML-generated summary for a chunk (upserts by chunk_id + summarizer).
    /// Returns the `summary_id`.
    fn store_ml_summary(
        &self,
        chunk_id: &str,
        summary_text: &str,
        summarizer: &str,
    ) -> Result<String>;
}

// -- SummaryWriter for Db --------------------------------------------------

impl SummaryWriter for Db {
    fn store_ml_summary(
        &self,
        chunk_id: &str,
        summary_text: &str,
        summarizer: &str,
    ) -> Result<String> {
        let chunk_id = chunk_id.to_string();
        let summary_text = summary_text.to_string();
        let summarizer = summarizer.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::summaries::store_ml_summary(
                conn,
                &chunk_id,
                &summary_text,
                &summarizer,
            )
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite write path — episodes
// ---------------------------------------------------------------------------

/// SQLite episode write operations required by the MCP `memory.write_episode` tool.
///
/// Consumers: `mcp::tools::mem_write_episode`.
pub trait EpisodeWriter: Send + Sync {
    /// Store an episode in the summaries table. Returns the `summary_id`.
    fn store_episode(&self, episode: &brain_persistence::db::summaries::Episode) -> Result<String>;
}

// -- EpisodeWriter for Db --------------------------------------------------

impl EpisodeWriter for Db {
    fn store_episode(&self, episode: &brain_persistence::db::summaries::Episode) -> Result<String> {
        // Episode fields must be cloned to cross the closure boundary.
        let brain_id = episode.brain_id.clone();
        let goal = episode.goal.clone();
        let actions = episode.actions.clone();
        let outcome = episode.outcome.clone();
        let tags = episode.tags.clone();
        let importance = episode.importance;
        self.with_write_conn(move |conn| {
            brain_persistence::db::summaries::store_episode(
                conn,
                &brain_persistence::db::summaries::Episode {
                    brain_id,
                    goal,
                    actions,
                    outcome,
                    tags,
                    importance,
                },
            )
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite read path — episodes
// ---------------------------------------------------------------------------

/// SQLite episode read operations required by the query pipeline.
///
/// Consumers: `QueryPipeline::reflect`, `QueryPipeline::search_ranked`.
pub trait EpisodeReader: Send + Sync {
    /// List recent episodes, newest first, up to `limit` entries.
    /// When `brain_id` is non-empty, filters to that brain. Empty string returns all brains.
    fn list_episodes(
        &self,
        limit: usize,
        brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryRow>>;

    /// List recent episodes across multiple brains.
    fn list_episodes_multi_brain(
        &self,
        limit: usize,
        brain_ids: &[String],
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryRow>>;

    /// Batch-load summaries by a list of summary IDs.
    fn get_summaries_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryRow>>;
}

// -- EpisodeReader for Db --------------------------------------------------

impl EpisodeReader for Db {
    fn list_episodes(
        &self,
        limit: usize,
        brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::summaries::list_episodes(conn, limit, &brain_id)
        })
    }

    fn list_episodes_multi_brain(
        &self,
        limit: usize,
        brain_ids: &[String],
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryRow>> {
        let brain_ids = brain_ids.to_vec();
        self.with_read_conn(move |conn| {
            brain_persistence::db::summaries::list_episodes_multi_brain(conn, limit, &brain_ids)
        })
    }

    fn get_summaries_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<brain_persistence::db::summaries::SummaryRow>> {
        let ids = ids.to_vec();
        self.with_read_conn(move |conn| {
            brain_persistence::db::summaries::get_summaries_by_ids(conn, &ids)
        })
    }
}

// ---------------------------------------------------------------------------
// Brain registry — used by MCP tools for brain lookups
// ---------------------------------------------------------------------------

/// Brain registry queries for archive checks and brain listing.
///
/// Consumers: `mcp::tools::helpers`, `mcp::tools::brains_list`,
/// `mcp::tools::task_create`, `mcp::tools::task_apply_event`,
/// `mcp::mod::resolve_brain_from_roots`.
pub trait BrainRegistry: Send + Sync {
    /// Check whether a brain has been archived.
    ///
    /// Returns `false` when no matching row exists (brain not yet registered).
    fn is_brain_archived(&self, brain_id: &str) -> Result<bool>;

    /// List all brain rows, optionally filtered to active-only.
    fn list_brains(
        &self,
        active_only: bool,
    ) -> Result<Vec<brain_persistence::db::schema::BrainRow>>;

    /// Return all active brain `(name, id)` pairs.
    fn list_brain_keys(&self) -> Result<Vec<(String, String)>>;
}

// -- BrainRegistry for Db --------------------------------------------------

impl BrainRegistry for Db {
    fn is_brain_archived(&self, brain_id: &str) -> Result<bool> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::schema::is_brain_archived(conn, &brain_id)
        })
    }

    fn list_brains(
        &self,
        active_only: bool,
    ) -> Result<Vec<brain_persistence::db::schema::BrainRow>> {
        self.with_read_conn(move |conn| {
            brain_persistence::db::schema::list_brains(conn, active_only)
        })
    }

    fn list_brain_keys(&self) -> Result<Vec<(String, String)>> {
        let rows =
            self.with_read_conn(|conn| brain_persistence::db::schema::list_brains(conn, true))?;
        let mut pairs: Vec<(String, String)> =
            rows.into_iter().map(|r| (r.name, r.brain_id)).collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pairs)
    }
}

impl BrainRegistry for crate::stores::BrainStores {
    fn is_brain_archived(&self, brain_id: &str) -> Result<bool> {
        crate::stores::BrainStores::is_brain_archived(self, brain_id)
    }

    fn list_brains(
        &self,
        active_only: bool,
    ) -> Result<Vec<brain_persistence::db::schema::BrainRow>> {
        crate::stores::BrainStores::list_brains(self, active_only)
    }

    fn list_brain_keys(&self) -> Result<Vec<(String, String)>> {
        crate::stores::BrainStores::list_brain_keys(self)
    }
}

// ---------------------------------------------------------------------------
// SQLite status queries — used by MCP status tool
// ---------------------------------------------------------------------------

/// SQLite queries for runtime health metrics.
///
/// Consumers: `mcp::tools::status::Status`.
pub trait StatusReader: Send + Sync {
    /// Count files stuck in `indexing_started` state.
    fn count_stuck_files(&self) -> Result<u64>;

    /// Read the `stale_hashes_prevented` counter from `brain_meta`.
    fn stale_hashes_prevented(&self) -> Result<u64>;
}

// -- StatusReader for Db ---------------------------------------------------

impl StatusReader for Db {
    fn count_stuck_files(&self) -> Result<u64> {
        self.with_read_conn(brain_persistence::db::files::count_stuck_indexing)
    }

    fn stale_hashes_prevented(&self) -> Result<u64> {
        self.with_read_conn(brain_persistence::db::meta::stale_hashes_prevented)
    }
}

// ---------------------------------------------------------------------------
// SQLite maintenance operations — used by pipeline::maintenance
// ---------------------------------------------------------------------------

/// SQLite maintenance operations for the indexing pipeline.
///
/// Consumers: `pipeline::maintenance`.
pub trait MaintenanceOps: Send + Sync {
    /// Rename a file by its path. Returns the file_id if found.
    fn rename_file_by_path(&self, from_path: &str, to_path: &str) -> Result<Option<String>>;

    /// Run SQLite VACUUM.
    fn vacuum_db(&self) -> Result<()>;

    /// Rebuild the FTS5 index from the chunks table.
    fn reindex_fts(&self) -> Result<()>;

    /// Check FTS5 consistency: return (chunk_count, fts_count).
    fn fts_consistency(&self) -> Result<(i64, i64)>;

    /// Rebuild the FTS5 summaries index from the summaries table.
    /// Returns the number of summaries indexed.
    fn reindex_summaries_fts(&self) -> Result<usize>;
}

// -- MaintenanceOps for Db -------------------------------------------------

impl MaintenanceOps for Db {
    fn rename_file_by_path(&self, from_path: &str, to_path: &str) -> Result<Option<String>> {
        let from = from_path.to_string();
        let to = to_path.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::files::rename_by_path(conn, &from, &to)
        })
    }

    fn vacuum_db(&self) -> Result<()> {
        self.with_write_conn(brain_persistence::db::files::vacuum)
    }

    fn reindex_fts(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            brain_persistence::db::fts::reindex_fts(conn)?;
            Ok(())
        })
    }

    fn fts_consistency(&self) -> Result<(i64, i64)> {
        self.with_read_conn(brain_persistence::db::fts::fts_consistency)
    }

    fn reindex_summaries_fts(&self) -> Result<usize> {
        self.with_write_conn(brain_persistence::db::fts::reindex_summaries_fts)
    }
}

// ---------------------------------------------------------------------------
// SQLite write path — reflections
// ---------------------------------------------------------------------------

/// SQLite reflection write operations required by the MCP `memory.reflect` tool
/// in commit mode.
///
/// Consumers: `mcp::tools::mem_reflect`.
pub trait ReflectionWriter: Send + Sync {
    /// Store a reflection in the summaries table, linked to source summaries.
    /// Returns the `summary_id`.
    fn store_reflection(
        &self,
        title: &str,
        content: &str,
        source_ids: &[String],
        tags: &[String],
        importance: f64,
        brain_id: &str,
    ) -> Result<String>;
}

// -- ReflectionWriter for Db -----------------------------------------------

impl ReflectionWriter for Db {
    fn store_reflection(
        &self,
        title: &str,
        content: &str,
        source_ids: &[String],
        tags: &[String],
        importance: f64,
        brain_id: &str,
    ) -> Result<String> {
        let title = title.to_string();
        let content = content.to_string();
        let source_ids = source_ids.to_vec();
        let tags = tags.to_vec();
        let brain_id = brain_id.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::summaries::store_reflection(
                conn,
                &title,
                &content,
                &source_ids,
                &tags,
                importance,
                &brain_id,
            )
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite write path — procedures
// ---------------------------------------------------------------------------

/// SQLite procedure write operations required by the MCP `memory.write_procedure` tool.
///
/// Consumers: `mcp::tools::mem_write_procedure`.
pub trait ProcedureWriter: Send + Sync {
    /// Store a procedure in the summaries table. Returns the `summary_id`.
    fn store_procedure(
        &self,
        title: &str,
        steps: &str,
        tags: &[String],
        importance: f64,
        brain_id: &str,
    ) -> Result<String>;
}

// -- ProcedureWriter for Db ------------------------------------------------

impl ProcedureWriter for Db {
    fn store_procedure(
        &self,
        title: &str,
        steps: &str,
        tags: &[String],
        importance: f64,
        brain_id: &str,
    ) -> Result<String> {
        let title = title.to_string();
        let steps = steps.to_string();
        let tags = tags.to_vec();
        let brain_id = brain_id.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::summaries::store_procedure(
                conn, &title, &steps, &tags, importance, &brain_id,
            )
        })
    }
}
// ---------------------------------------------------------------------------
// SQLite read/write path — brain management
// ---------------------------------------------------------------------------

/// Brain registry management operations.
///
/// Consumers: `mcp::tools::task_apply_event` (archive), `pipeline::job_worker` (lookup).
pub trait BrainManager: Send + Sync {
    /// Archive a brain by setting its `archived` flag.
    fn archive_brain(&self, brain_id: &str) -> Result<()>;

    /// Get a brain row by its `brain_id`.
    fn get_brain(&self, brain_id: &str) -> Result<Option<brain_persistence::db::schema::BrainRow>>;
}

// -- BrainManager for Db -----------------------------------------------------

impl BrainManager for Db {
    fn archive_brain(&self, brain_id: &str) -> Result<()> {
        let brain_id = brain_id.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::schema::archive_brain(conn, &brain_id)
        })
    }

    fn get_brain(&self, brain_id: &str) -> Result<Option<brain_persistence::db::schema::BrainRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| brain_persistence::db::schema::get_brain(conn, &brain_id))
    }
}

// ---------------------------------------------------------------------------
// LanceDB write path — summary embeddings
// ---------------------------------------------------------------------------

/// LanceDB write operations for summary (episode/reflection) embeddings.
///
/// Consumers: `mcp::tools::mem_write_episode`, `mcp::tools::mem_reflect`.
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

// -- SummaryStoreWriter for Store ------------------------------------------

impl SummaryStoreWriter for Store {
    fn upsert_summary<'a>(
        &'a self,
        summary_id: &'a str,
        content: &'a str,
        brain_id: &'a str,
        embedding: &'a [f32],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        Store::upsert_summary(self, summary_id, content, brain_id, embedding)
    }
}

// ---------------------------------------------------------------------------
// SQLite read path — graph link traversal
// ---------------------------------------------------------------------------

/// 1-hop graph link traversal required by the query pipeline's graph expansion.
///
/// Consumers: `QueryPipeline::search_ranked` (when `graph_expand` is true).
pub trait GraphLinkReader: Send + Sync {
    /// Return all `target_file_id` values for outgoing links from `source_file_id`.
    ///
    /// Excludes unresolved links (target_file_id IS NULL) and external links.
    fn get_outlinks(&self, source_file_id: &str) -> Result<Vec<String>>;

    /// Return all chunks for a set of `file_id`s, joining with the files table.
    ///
    /// Used to fetch chunks for graph-expansion neighbour files.
    fn get_chunks_by_file_ids(&self, file_ids: &[String]) -> Result<Vec<ChunkRow>>;
}

// -- GraphLinkReader for Db ------------------------------------------------

impl GraphLinkReader for Db {
    fn get_outlinks(&self, source_file_id: &str) -> Result<Vec<String>> {
        let source_file_id = source_file_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::links::get_outlinks(conn, &source_file_id)
        })
    }

    fn get_chunks_by_file_ids(&self, file_ids: &[String]) -> Result<Vec<ChunkRow>> {
        let file_ids = file_ids.to_vec();
        self.with_read_conn(move |conn| {
            brain_persistence::db::chunks::get_chunks_by_file_ids(conn, &file_ids)
        })
    }
}
// ---------------------------------------------------------------------------
// SQLite read/write path — embedding poll operations
// ---------------------------------------------------------------------------

/// Embedding poll operations required by the async embedding pipeline.
///
/// Consumers: `pipeline::embed_poll`.
pub trait EmbeddingOps: Send + Sync {
    /// Find chunks that need embedding (stale or never embedded).
    fn find_stale_chunks_for_embedding(&self, brain_id: &str) -> Result<Vec<ChunkPollRow>>;

    /// Find summaries that need embedding (stale or never embedded).
    fn find_stale_summaries_for_embedding(&self, brain_id: &str) -> Result<Vec<SummaryPollRow>>;

    /// Find tasks that need embedding (stale or never embedded).
    fn find_stale_tasks_for_embedding(&self, brain_id: &str) -> Result<Vec<TaskPollRow>>;

    /// Find records that need embedding (stale or never embedded).
    fn find_stale_records_for_embedding(
        &self,
        brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::records::queries::RecordPollRow>>;

    /// Mark summaries as embedded.
    fn mark_summaries_embedded(&self, summary_ids: &[&str]) -> Result<()>;

    /// Mark tasks as embedded.
    fn mark_tasks_embedded(&self, task_ids: &[&str]) -> Result<()>;

    /// Mark records as embedded.
    fn mark_records_embedded(&self, record_ids: &[&str]) -> Result<()>;
}

// -- EmbeddingOps for Db -----------------------------------------------------

impl EmbeddingOps for Db {
    fn find_stale_chunks_for_embedding(&self, brain_id: &str) -> Result<Vec<ChunkPollRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::chunks::find_stale_for_embedding(conn, &brain_id)
        })
    }

    fn find_stale_summaries_for_embedding(&self, brain_id: &str) -> Result<Vec<SummaryPollRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::summaries::find_stale_summaries_for_embedding(conn, &brain_id)
        })
    }

    fn find_stale_tasks_for_embedding(&self, brain_id: &str) -> Result<Vec<TaskPollRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::tasks::queries::find_stale_tasks_for_embedding(conn, &brain_id)
        })
    }

    fn find_stale_records_for_embedding(
        &self,
        brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::records::queries::RecordPollRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            brain_persistence::db::records::queries::find_stale_records_for_embedding(
                conn, &brain_id,
            )
        })
    }

    fn mark_summaries_embedded(&self, summary_ids: &[&str]) -> Result<()> {
        let summary_ids: Vec<String> = summary_ids.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = summary_ids.iter().map(|s| s.as_str()).collect();
            brain_persistence::db::summaries::mark_summaries_embedded(conn, &refs)
        })
    }

    fn mark_tasks_embedded(&self, task_ids: &[&str]) -> Result<()> {
        let task_ids: Vec<String> = task_ids.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = task_ids.iter().map(|s| s.as_str()).collect();
            brain_persistence::db::chunks::mark_tasks_embedded(conn, &refs)
        })
    }

    fn mark_records_embedded(&self, record_ids: &[&str]) -> Result<()> {
        let record_ids: Vec<String> = record_ids.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = record_ids.iter().map(|s| s.as_str()).collect();
            brain_persistence::db::records::queries::mark_records_embedded(conn, &refs)
        })
    }
}

// ---------------------------------------------------------------------------
// SQLite read/write path — derived summaries (hierarchy module)
// ---------------------------------------------------------------------------
//
// The `DerivedSummaryStore` trait is defined in `crate::hierarchy` alongside
// its types. The concrete `Db` implementation lives here to follow the
// established pattern: traits defined in brain_lib, impls for brain_persistence
// types also in brain_lib (in this file).

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

/// Persistence writes produced by async job execution.
///
/// Consumers: `pipeline::job_worker::persist_job_result` extraction.
pub trait JobPersistence: Send + Sync {
    /// Persist a generated scope summary payload into `derived_summaries`.
    fn persist_scope_summary_result(&self, summary_id: &str, result: &str) -> Result<()>;

    /// Persist a consolidation/reflection result and its lineage updates in
    /// `summaries`, `summary_sources`, and related compatibility tables.
    fn persist_consolidation_result(
        &self,
        suggested_title: &str,
        result: &str,
        episode_ids: &[String],
        brain_id: &str,
    ) -> Result<()>;
}

impl JobPersistence for Db {
    fn persist_scope_summary_result(&self, summary_id: &str, result: &str) -> Result<()> {
        let summary_id = summary_id.to_string();
        let result = result.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::job_results::persist_scope_summary_result(conn, &summary_id, &result)
        })
    }

    fn persist_consolidation_result(
        &self,
        suggested_title: &str,
        result: &str,
        episode_ids: &[String],
        brain_id: &str,
    ) -> Result<()> {
        let suggested_title = suggested_title.to_string();
        let result = result.to_string();
        let episode_ids = episode_ids.to_vec();
        let brain_id = brain_id.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::job_results::persist_consolidation_result(
                conn,
                &suggested_title,
                &result,
                &episode_ids,
                &brain_id,
            )
        })
    }
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

impl LodChunkStore for crate::stores::BrainStores {
    fn upsert_lod_chunk(&self, input: &UpsertLodChunk<'_>) -> Result<String> {
        LodChunkStore::upsert_lod_chunk(self.inner_db(), input)
    }
    fn get_lod_chunk(&self, object_uri: &str, lod_level: LodLevel) -> Result<Option<LodChunk>> {
        LodChunkStore::get_lod_chunk(self.inner_db(), object_uri, lod_level)
    }
    fn get_lod_chunks_for_uri(&self, object_uri: &str) -> Result<Vec<LodChunk>> {
        LodChunkStore::get_lod_chunks_for_uri(self.inner_db(), object_uri)
    }
    fn delete_lod_chunks_for_uri(&self, object_uri: &str) -> Result<usize> {
        LodChunkStore::delete_lod_chunks_for_uri(self.inner_db(), object_uri)
    }
    fn delete_expired_lod_chunks(&self, now_iso: &str) -> Result<usize> {
        LodChunkStore::delete_expired_lod_chunks(self.inner_db(), now_iso)
    }
    fn is_lod_fresh(
        &self,
        object_uri: &str,
        lod_level: LodLevel,
        current_source_hash: &str,
    ) -> Result<bool> {
        LodChunkStore::is_lod_fresh(self.inner_db(), object_uri, lod_level, current_source_hash)
    }
    fn is_l1_fresh(&self, object_uri: &str, current_source_hash: &str) -> Result<bool> {
        LodChunkStore::is_l1_fresh(self.inner_db(), object_uri, current_source_hash)
    }
    fn count_lod_chunks_by_brain(
        &self,
        brain_id: &str,
        lod_level: Option<LodLevel>,
    ) -> Result<usize> {
        LodChunkStore::count_lod_chunks_by_brain(self.inner_db(), brain_id, lod_level)
    }
    fn list_lod_chunks_by_brain(
        &self,
        brain_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<LodChunk>> {
        LodChunkStore::list_lod_chunks_by_brain(self.inner_db(), brain_id, limit, offset)
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

// ---------------------------------------------------------------------------
// Job queue operations — used by job_worker and daemon event loop
// ---------------------------------------------------------------------------

use brain_persistence::db::job::Job;
use brain_persistence::db::job::JobStatus;
use brain_persistence::db::jobs::EnqueueJobInput;

/// Job queue operations required by the daemon event loop and job worker.
///
/// Consumers: `pipeline::job_worker::process_jobs`, daemon watch loop
/// (job tick + reaper), and the `jobs.status` MCP tool.
pub trait JobQueue: Send + Sync {
    /// Atomically claim up to `limit` ready jobs. Sets status to `pending`,
    /// increments attempts, records `started_at`.
    fn claim_ready_jobs(&self, limit: i32) -> Result<Vec<Job>>;

    /// Advance a job from `pending` to `in_progress`. No-op if already past that state.
    fn advance_to_in_progress(&self, job_id: &str) -> Result<()>;

    /// Mark a job as done with an optional result. Sets `processed_at`.
    fn complete_job(&self, job_id: &str, result: Option<&str>) -> Result<()>;

    /// Handle a job failure. If retries remain, reschedule to `ready` with
    /// backoff. If exhausted, mark as `failed`.
    fn fail_job(&self, job_id: &str, error_msg: &str) -> Result<()>;

    /// Reset stuck `in_progress`/`pending` jobs that exceeded their
    /// `stuck_threshold_secs` back to `ready`. Returns the count of reaped jobs.
    fn reap_stuck_jobs(&self) -> Result<usize>;

    /// Enqueue a new job. Returns the `job_id`.
    fn enqueue_job(&self, input: &EnqueueJobInput) -> Result<String>;

    /// Delete old completed jobs older than `age_secs`. Returns deleted count.
    /// Protected kinds (recurring singletons) are excluded from deletion.
    fn gc_completed_jobs(&self, age_secs: i64, protected_kinds: &[&str]) -> Result<usize>;

    /// Count jobs with the given status.
    fn count_jobs_by_status(&self, status: &JobStatus) -> Result<i64>;

    /// List recent jobs filtered by status, ordered by most recent first.
    fn list_jobs_by_status(&self, status: &JobStatus, limit: i32) -> Result<Vec<Job>>;

    /// List stuck jobs (in_progress/pending past their threshold) that are retryable.
    fn list_stuck_jobs(&self) -> Result<Vec<Job>>;

    /// Reset a failed job back to `ready`. Returns true if the job was updated.
    fn retry_failed_job(&self, job_id: &str) -> Result<bool>;

    /// Get a job by its `kind` column.
    fn get_job_by_kind(&self, kind: &str) -> Result<Option<Job>>;

    /// Get a single job by its `job_id`.
    fn get_job(&self, job_id: &str) -> Result<Option<Job>>;

    /// Update a job's status directly. Returns true if a row was updated.
    fn update_job_status(&self, job_id: &str, status: &JobStatus) -> Result<bool>;
    /// Ensure a singleton job row exists for the given kind.
    /// Returns `Some(job_id)` if inserted, `None` if already exists.
    fn ensure_singleton_job(&self, input: &EnqueueJobInput) -> Result<Option<String>>;

    /// If the singleton job for `kind` is terminal (done/failed), reset to ready.
    /// Returns `true` if reset, `false` if active or missing.
    fn reschedule_terminal_job(&self, kind: &str, brain_id: Option<&str>) -> Result<bool>;

    /// Enqueue a dedup job. If a non-terminal job of the same kind exists,
    /// returns its job_id. Returns `(job_id, was_created)`.
    fn enqueue_dedup_job(&self, input: &EnqueueJobInput) -> Result<(String, bool)>;

    /// Ensure a singleton job exists and is schedulable (combined
    /// ensure + reschedule in one write transaction).
    fn reconcile_singleton_job(&self, input: &EnqueueJobInput) -> Result<()>;

    /// Like `reconcile_singleton_job` but reschedules with a delay (seconds).
    fn reconcile_singleton_job_with_delay(
        &self,
        input: &EnqueueJobInput,
        delay_secs: i64,
    ) -> Result<()>;

    /// Check if an active (non-terminal) `lod_summarize` job exists for `object_uri`.
    fn has_active_lod_job(&self, object_uri: &str) -> Result<bool>;
}

// -- JobQueue for Db -------------------------------------------------------

impl JobQueue for Db {
    fn claim_ready_jobs(&self, limit: i32) -> Result<Vec<Job>> {
        self.with_write_conn(|conn| brain_persistence::db::jobs::claim_ready_jobs(conn, limit))
    }

    fn advance_to_in_progress(&self, job_id: &str) -> Result<()> {
        let job_id = job_id.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::jobs::advance_to_in_progress(conn, &job_id)
        })
    }

    fn complete_job(&self, job_id: &str, result: Option<&str>) -> Result<()> {
        let job_id = job_id.to_string();
        let result = result.map(|s| s.to_string());
        self.with_write_conn(move |conn| {
            brain_persistence::db::jobs::complete_job(conn, &job_id, result.as_deref())
        })
    }

    fn fail_job(&self, job_id: &str, error_msg: &str) -> Result<()> {
        let job_id = job_id.to_string();
        let error_msg = error_msg.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::db::jobs::fail_job(conn, &job_id, &error_msg)
        })
    }

    fn reap_stuck_jobs(&self) -> Result<usize> {
        self.with_write_conn(brain_persistence::db::jobs::reap_stuck_jobs)
    }

    fn enqueue_job(&self, input: &EnqueueJobInput) -> Result<String> {
        let input = input.clone();
        self.with_write_conn(move |conn| brain_persistence::db::jobs::enqueue_job(conn, &input))
    }

    fn gc_completed_jobs(&self, age_secs: i64, protected_kinds: &[&str]) -> Result<usize> {
        let protected: Vec<String> = protected_kinds.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = protected.iter().map(|s| s.as_str()).collect();
            brain_persistence::db::jobs::gc_completed_jobs(conn, age_secs, &refs)
        })
    }

    fn count_jobs_by_status(&self, status: &JobStatus) -> Result<i64> {
        self.with_read_conn(move |conn| {
            brain_persistence::db::jobs::count_jobs_by_status(conn, status)
        })
    }

    fn list_jobs_by_status(&self, status: &JobStatus, limit: i32) -> Result<Vec<Job>> {
        self.with_read_conn(move |conn| {
            brain_persistence::db::jobs::list_jobs_by_status(conn, status, limit)
        })
    }

    fn list_stuck_jobs(&self) -> Result<Vec<Job>> {
        self.with_read_conn(brain_persistence::db::jobs::list_stuck_jobs)
    }

    fn retry_failed_job(&self, job_id: &str) -> Result<bool> {
        let id = job_id.to_string();
        self.with_write_conn(move |conn| brain_persistence::db::jobs::retry_failed_job(conn, &id))
    }

    fn get_job_by_kind(&self, kind: &str) -> Result<Option<Job>> {
        let kind = kind.to_string();
        self.with_read_conn(move |conn| brain_persistence::db::jobs::get_job_by_kind(conn, &kind))
    }
    fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        let job_id = job_id.to_string();
        self.with_read_conn(move |conn| brain_persistence::db::jobs::get_job(conn, &job_id))
    }

    fn update_job_status(&self, job_id: &str, status: &JobStatus) -> Result<bool> {
        let job_id = job_id.to_string();
        let status = *status;
        self.with_write_conn(move |conn| {
            brain_persistence::db::jobs::update_job_status(conn, &job_id, &status)
        })
    }

    fn ensure_singleton_job(&self, input: &EnqueueJobInput) -> Result<Option<String>> {
        let input = input.clone();
        self.with_write_conn(move |conn| {
            brain_persistence::db::jobs::ensure_singleton_job(conn, &input)
        })
    }

    fn reschedule_terminal_job(&self, kind: &str, brain_id: Option<&str>) -> Result<bool> {
        let kind = kind.to_string();
        let brain_id = brain_id.map(|s| s.to_string());
        self.with_write_conn(move |conn| {
            brain_persistence::db::jobs::reschedule_terminal_job(
                conn,
                &kind,
                brain_id.as_deref(),
                0,
            )
        })
    }

    fn enqueue_dedup_job(&self, input: &EnqueueJobInput) -> Result<(String, bool)> {
        let input = input.clone();
        self.with_write_conn(move |conn| {
            brain_persistence::db::jobs::enqueue_dedup_job(conn, &input)
        })
    }

    fn reconcile_singleton_job(&self, input: &EnqueueJobInput) -> Result<()> {
        let input = input.clone();
        self.with_write_conn(move |conn| {
            brain_persistence::db::jobs::reconcile_singleton_job(conn, &input)
        })
    }

    fn reconcile_singleton_job_with_delay(
        &self,
        input: &EnqueueJobInput,
        delay_secs: i64,
    ) -> Result<()> {
        let input = input.clone();
        self.with_write_conn(move |conn| {
            brain_persistence::db::jobs::reconcile_singleton_job_with_delay(
                conn, &input, delay_secs,
            )
        })
    }

    fn has_active_lod_job(&self, object_uri: &str) -> Result<bool> {
        let uri = object_uri.to_string();
        self.with_read_conn(move |conn| brain_persistence::db::jobs::has_active_lod_job(conn, &uri))
    }
}

impl JobQueue for crate::stores::BrainStores {
    fn claim_ready_jobs(&self, limit: i32) -> Result<Vec<Job>> {
        JobQueue::claim_ready_jobs(self.inner_db(), limit)
    }
    fn advance_to_in_progress(&self, job_id: &str) -> Result<()> {
        JobQueue::advance_to_in_progress(self.inner_db(), job_id)
    }
    fn complete_job(&self, job_id: &str, result: Option<&str>) -> Result<()> {
        JobQueue::complete_job(self.inner_db(), job_id, result)
    }
    fn fail_job(&self, job_id: &str, error_msg: &str) -> Result<()> {
        JobQueue::fail_job(self.inner_db(), job_id, error_msg)
    }
    fn reap_stuck_jobs(&self) -> Result<usize> {
        JobQueue::reap_stuck_jobs(self.inner_db())
    }
    fn enqueue_job(&self, input: &EnqueueJobInput) -> Result<String> {
        JobQueue::enqueue_job(self.inner_db(), input)
    }
    fn gc_completed_jobs(&self, age_secs: i64, protected_kinds: &[&str]) -> Result<usize> {
        JobQueue::gc_completed_jobs(self.inner_db(), age_secs, protected_kinds)
    }
    fn count_jobs_by_status(&self, status: &JobStatus) -> Result<i64> {
        JobQueue::count_jobs_by_status(self.inner_db(), status)
    }
    fn list_jobs_by_status(&self, status: &JobStatus, limit: i32) -> Result<Vec<Job>> {
        JobQueue::list_jobs_by_status(self.inner_db(), status, limit)
    }
    fn list_stuck_jobs(&self) -> Result<Vec<Job>> {
        JobQueue::list_stuck_jobs(self.inner_db())
    }
    fn retry_failed_job(&self, job_id: &str) -> Result<bool> {
        JobQueue::retry_failed_job(self.inner_db(), job_id)
    }
    fn get_job_by_kind(&self, kind: &str) -> Result<Option<Job>> {
        JobQueue::get_job_by_kind(self.inner_db(), kind)
    }
    fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        JobQueue::get_job(self.inner_db(), job_id)
    }
    fn update_job_status(&self, job_id: &str, status: &JobStatus) -> Result<bool> {
        JobQueue::update_job_status(self.inner_db(), job_id, status)
    }
    fn ensure_singleton_job(&self, input: &EnqueueJobInput) -> Result<Option<String>> {
        JobQueue::ensure_singleton_job(self.inner_db(), input)
    }
    fn reschedule_terminal_job(&self, kind: &str, brain_id: Option<&str>) -> Result<bool> {
        JobQueue::reschedule_terminal_job(self.inner_db(), kind, brain_id)
    }
    fn enqueue_dedup_job(&self, input: &EnqueueJobInput) -> Result<(String, bool)> {
        JobQueue::enqueue_dedup_job(self.inner_db(), input)
    }
    fn reconcile_singleton_job(&self, input: &EnqueueJobInput) -> Result<()> {
        JobQueue::reconcile_singleton_job(self.inner_db(), input)
    }
    fn reconcile_singleton_job_with_delay(
        &self,
        input: &EnqueueJobInput,
        delay_secs: i64,
    ) -> Result<()> {
        JobQueue::reconcile_singleton_job_with_delay(self.inner_db(), input, delay_secs)
    }
    fn has_active_lod_job(&self, object_uri: &str) -> Result<bool> {
        JobQueue::has_active_lod_job(self.inner_db(), object_uri)
    }
}

impl DerivedSummaryStore for crate::stores::BrainStores {
    fn generate_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<GeneratedScopeSummary> {
        DerivedSummaryStore::generate_scope_summary(self.inner_db(), scope_type, scope_value)
    }
    fn get_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<Option<DerivedSummary>> {
        DerivedSummaryStore::get_scope_summary(self.inner_db(), scope_type, scope_value)
    }
    fn mark_scope_stale(&self, scope_type: &ScopeType, scope_value: &str) -> Result<usize> {
        DerivedSummaryStore::mark_scope_stale(self.inner_db(), scope_type, scope_value)
    }
    fn search_derived_summaries(&self, query: &str, limit: usize) -> Result<Vec<DerivedSummary>> {
        DerivedSummaryStore::search_derived_summaries(self.inner_db(), query, limit)
    }
    fn list_stale_summaries(&self, limit: usize) -> Result<Vec<DerivedSummary>> {
        DerivedSummaryStore::list_stale_summaries(self.inner_db(), limit)
    }
}

// ---------------------------------------------------------------------------
// Provider store — used by llm::resolve_provider and CLI
// ---------------------------------------------------------------------------

use brain_persistence::db::providers::{InsertProvider, ProviderRow};

/// Provider credential operations.
///
/// Consumers: `llm::resolve_provider`, CLI `brain config provider` commands.
pub trait ProviderStore: Send + Sync {
    /// Insert a new provider. Returns the generated ID.
    fn insert_provider(&self, input: &InsertProvider) -> Result<String>;

    /// Get a provider by ID.
    fn get_provider(&self, id: &str) -> Result<Option<ProviderRow>>;

    /// Get the most recently updated provider for a given name.
    fn get_provider_by_name(&self, name: &str) -> Result<Option<ProviderRow>>;

    /// List all providers.
    fn list_providers(&self) -> Result<Vec<ProviderRow>>;

    /// Delete a provider by ID. Returns true if deleted.
    fn delete_provider(&self, id: &str) -> Result<bool>;

    /// Check if a provider with the given name and key hash exists.
    fn provider_exists(&self, name: &str, api_key_hash: &str) -> Result<bool>;
}

// -- ProviderStore for Db --------------------------------------------------

impl ProviderStore for Db {
    fn insert_provider(&self, input: &InsertProvider) -> Result<String> {
        self.insert_provider(input)
    }

    fn get_provider(&self, id: &str) -> Result<Option<ProviderRow>> {
        self.get_provider(id)
    }

    fn get_provider_by_name(&self, name: &str) -> Result<Option<ProviderRow>> {
        self.get_provider_by_name(name)
    }

    fn list_providers(&self) -> Result<Vec<ProviderRow>> {
        self.list_providers()
    }

    fn delete_provider(&self, id: &str) -> Result<bool> {
        self.delete_provider(id)
    }

    fn provider_exists(&self, name: &str, api_key_hash: &str) -> Result<bool> {
        self.provider_exists(name, api_key_hash)
    }
}

impl ProviderStore for crate::stores::BrainStores {
    fn insert_provider(&self, input: &InsertProvider) -> Result<String> {
        ProviderStore::insert_provider(self.inner_db(), input)
    }
    fn get_provider(&self, id: &str) -> Result<Option<ProviderRow>> {
        ProviderStore::get_provider(self.inner_db(), id)
    }
    fn get_provider_by_name(&self, name: &str) -> Result<Option<ProviderRow>> {
        ProviderStore::get_provider_by_name(self.inner_db(), name)
    }
    fn list_providers(&self) -> Result<Vec<ProviderRow>> {
        ProviderStore::list_providers(self.inner_db())
    }
    fn delete_provider(&self, id: &str) -> Result<bool> {
        ProviderStore::delete_provider(self.inner_db(), id)
    }
    fn provider_exists(&self, name: &str, api_key_hash: &str) -> Result<bool> {
        ProviderStore::provider_exists(self.inner_db(), name, api_key_hash)
    }
}

// ---------------------------------------------------------------------------
// Mock implementations for testing
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::manual_async_fn, clippy::type_complexity)]
pub mod mock;
