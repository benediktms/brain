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
use crate::db::fts::{FtsResult, FtsSummaryResult};
use crate::error::Result;
use crate::store::{QueryResult, VectorSearchMode};

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
    ///
    /// `mode` controls the ANN (Approximate Nearest Neighbor) vs exact search
    /// tradeoff — see [`VectorSearchMode`].
    fn query<'a>(
        &'a self,
        embedding: &'a [f32],
        top_k: usize,
        nprobes: usize,
        mode: VectorSearchMode,
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
    fn get_summary_kinds(&self, summary_ids: &[String]) -> Result<HashMap<String, String>>;
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

    /// Search the FTS5 summaries index (episodes + reflections) and return
    /// BM25-ranked results (scores normalized to [0, 1]).
    fn search_summaries_fts(&self, query: &str, limit: usize) -> Result<Vec<FtsSummaryResult>>;
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
        mode: VectorSearchMode,
    ) -> impl std::future::Future<Output = Result<Vec<QueryResult>>> + Send + 'a {
        StoreReader::query(self, embedding, top_k, nprobes, mode)
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
    ) -> impl std::future::Future<Output = Result<Vec<QueryResult>>> + Send + 'a {
        Store::query(self, embedding, top_k, nprobes, mode)
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

    fn get_summary_kinds(&self, summary_ids: &[String]) -> Result<HashMap<String, String>> {
        let ids = summary_ids.to_vec();
        self.with_read_conn(move |conn| crate::db::summaries::get_summary_kinds(conn, &ids))
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

    fn search_summaries_fts(&self, query: &str, limit: usize) -> Result<Vec<FtsSummaryResult>> {
        let query = query.to_string();
        self.with_read_conn(move |conn| crate::db::fts::search_summaries_fts(conn, &query, limit))
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
    fn get_or_create_file_id(&self, path: &str) -> Result<(String, bool)>;

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

    /// Mark a file as fully indexed: update hash, chunker version, timestamp, and state.
    fn mark_indexed(&self, file_id: &str, content_hash: &str, chunker_version: u32) -> Result<()>;

    /// Count files where `chunker_version` doesn't match `current_version` (stale or NULL).
    fn count_stale_chunker_version(&self, current_version: u32) -> Result<usize>;
}

// -- FileMetaWriter for Db -------------------------------------------------

impl FileMetaWriter for Db {
    fn get_or_create_file_id(&self, path: &str) -> Result<(String, bool)> {
        let path = path.to_string();
        self.with_write_conn(move |conn| crate::db::files::get_or_create_file_id(conn, &path))
    }

    fn handle_delete(&self, path: &str) -> Result<Option<String>> {
        let path = path.to_string();
        self.with_write_conn(move |conn| crate::db::files::handle_delete(conn, &path))
    }

    fn handle_rename(&self, file_id: &str, new_path: &str) -> Result<()> {
        let file_id = file_id.to_string();
        let new_path = new_path.to_string();
        self.with_write_conn(move |conn| crate::db::files::handle_rename(conn, &file_id, &new_path))
    }

    fn purge_deleted_files(&self, older_than_ts: i64) -> Result<Vec<String>> {
        self.with_write_conn(move |conn| crate::db::files::purge_deleted_files(conn, older_than_ts))
    }

    fn clear_all_content_hashes(&self) -> Result<usize> {
        self.with_write_conn(crate::db::files::clear_all_content_hashes)
    }

    fn clear_content_hash_by_path(&self, path: &str) -> Result<bool> {
        let path = path.to_string();
        self.with_write_conn(move |conn| crate::db::files::clear_content_hash_by_path(conn, &path))
    }

    fn set_indexing_state(&self, file_id: &str, state: &str) -> Result<()> {
        let file_id = file_id.to_string();
        let state = state.to_string();
        self.with_write_conn(move |conn| {
            crate::db::files::set_indexing_state(conn, &file_id, &state)
        })
    }

    fn mark_indexed(&self, file_id: &str, content_hash: &str, chunker_version: u32) -> Result<()> {
        let file_id = file_id.to_string();
        let content_hash = content_hash.to_string();
        self.with_write_conn(move |conn| {
            crate::db::files::mark_indexed(conn, &file_id, &content_hash, chunker_version)
        })
    }

    fn count_stale_chunker_version(&self, current_version: u32) -> Result<usize> {
        self.with_read_conn(move |conn| {
            crate::db::files::count_stale_chunker_version(conn, current_version)
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
    fn replace_chunk_metadata(
        &self,
        file_id: &str,
        chunks: &[crate::db::chunks::ChunkMeta],
    ) -> Result<()>;

    /// Get ordered chunk hashes for a file (by `chunk_ord`).
    fn get_chunk_hashes(&self, file_id: &str) -> Result<Vec<String>>;

    /// Set `embedded_at` on a batch of chunks, marking them as current in LanceDB.
    fn mark_chunks_embedded(&self, chunk_ids: &[&str], timestamp: i64) -> Result<()>;

    /// Upsert a task capsule chunk into SQLite (creates synthetic `files` row if needed).
    fn upsert_task_chunk(&self, task_file_id: &str, capsule_text: &str) -> Result<()>;

    /// Upsert a record capsule chunk into SQLite (creates synthetic `files` row if needed).
    fn upsert_record_chunk(&self, record_file_id: &str, capsule_text: &str) -> Result<()>;
}

// -- ChunkMetaWriter for Db ------------------------------------------------

impl ChunkMetaWriter for Db {
    fn replace_chunk_metadata(
        &self,
        file_id: &str,
        chunks: &[crate::db::chunks::ChunkMeta],
    ) -> Result<()> {
        // ChunkMeta is not Clone/Send, so we must call within the closure directly.
        // Caller must ensure chunks slice lifetime covers the closure.
        // We delegate via with_write_conn using a shared reference approach.
        self.with_write_conn(|conn| {
            crate::db::chunks::replace_chunk_metadata(conn, file_id, chunks)
        })
    }

    fn get_chunk_hashes(&self, file_id: &str) -> Result<Vec<String>> {
        let file_id = file_id.to_string();
        self.with_read_conn(move |conn| crate::db::chunks::get_chunk_hashes(conn, &file_id))
    }

    fn mark_chunks_embedded(&self, chunk_ids: &[&str], timestamp: i64) -> Result<()> {
        let ids: Vec<String> = chunk_ids.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            crate::db::chunks::mark_chunks_embedded(conn, &refs, timestamp)
        })
    }

    fn upsert_task_chunk(&self, task_file_id: &str, capsule_text: &str) -> Result<()> {
        let task_file_id = task_file_id.to_string();
        let capsule_text = capsule_text.to_string();
        self.with_write_conn(move |conn| {
            crate::db::chunks::upsert_task_chunk(conn, &task_file_id, &capsule_text)
        })
    }

    fn upsert_record_chunk(&self, record_file_id: &str, capsule_text: &str) -> Result<()> {
        let record_file_id = record_file_id.to_string();
        let capsule_text = capsule_text.to_string();
        self.with_write_conn(move |conn| {
            crate::db::chunks::upsert_record_chunk(conn, &record_file_id, &capsule_text)
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
            crate::db::summaries::find_chunks_lacking_summary(conn, &summarizer, limit)
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
            crate::db::summaries::store_ml_summary(conn, &chunk_id, &summary_text, &summarizer)
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
    fn store_episode(&self, episode: &crate::db::summaries::Episode) -> Result<String>;
}

// -- EpisodeWriter for Db --------------------------------------------------

impl EpisodeWriter for Db {
    fn store_episode(&self, episode: &crate::db::summaries::Episode) -> Result<String> {
        // Episode fields must be cloned to cross the closure boundary.
        let brain_id = episode.brain_id.clone();
        let goal = episode.goal.clone();
        let actions = episode.actions.clone();
        let outcome = episode.outcome.clone();
        let tags = episode.tags.clone();
        let importance = episode.importance;
        self.with_write_conn(move |conn| {
            crate::db::summaries::store_episode(
                conn,
                &crate::db::summaries::Episode {
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
    ) -> Result<Vec<crate::db::summaries::SummaryRow>>;

    /// List recent episodes across multiple brains.
    fn list_episodes_multi_brain(
        &self,
        limit: usize,
        brain_ids: &[String],
    ) -> Result<Vec<crate::db::summaries::SummaryRow>>;

    /// Batch-load summaries by a list of summary IDs.
    fn get_summaries_by_ids(&self, ids: &[String])
    -> Result<Vec<crate::db::summaries::SummaryRow>>;
}

// -- EpisodeReader for Db --------------------------------------------------

impl EpisodeReader for Db {
    fn list_episodes(
        &self,
        limit: usize,
        brain_id: &str,
    ) -> Result<Vec<crate::db::summaries::SummaryRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| crate::db::summaries::list_episodes(conn, limit, &brain_id))
    }

    fn list_episodes_multi_brain(
        &self,
        limit: usize,
        brain_ids: &[String],
    ) -> Result<Vec<crate::db::summaries::SummaryRow>> {
        let brain_ids = brain_ids.to_vec();
        self.with_read_conn(move |conn| {
            crate::db::summaries::list_episodes_multi_brain(conn, limit, &brain_ids)
        })
    }

    fn get_summaries_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<crate::db::summaries::SummaryRow>> {
        let ids = ids.to_vec();
        self.with_read_conn(move |conn| crate::db::summaries::get_summaries_by_ids(conn, &ids))
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
        self.with_read_conn(crate::db::files::count_stuck_indexing)
    }

    fn stale_hashes_prevented(&self) -> Result<u64> {
        self.with_read_conn(crate::db::meta::stale_hashes_prevented)
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
        self.with_write_conn(move |conn| crate::db::files::rename_by_path(conn, &from, &to))
    }

    fn vacuum_db(&self) -> Result<()> {
        self.with_write_conn(crate::db::files::vacuum)
    }

    fn reindex_fts(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            crate::db::fts::reindex_fts(conn)?;
            Ok(())
        })
    }

    fn fts_consistency(&self) -> Result<(i64, i64)> {
        self.with_read_conn(crate::db::fts::fts_consistency)
    }

    fn reindex_summaries_fts(&self) -> Result<usize> {
        self.with_write_conn(crate::db::fts::reindex_summaries_fts)
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
            crate::db::summaries::store_reflection(
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
            crate::db::summaries::store_procedure(
                conn,
                &title,
                &steps,
                &tags,
                importance,
                &brain_id,
            )
        })
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
        embedding: &'a [f32],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;
}

// -- SummaryStoreWriter for Store ------------------------------------------

impl SummaryStoreWriter for Store {
    fn upsert_summary<'a>(
        &'a self,
        summary_id: &'a str,
        content: &'a str,
        embedding: &'a [f32],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        Store::upsert_summary(self, summary_id, content, embedding)
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
        self.with_read_conn(move |conn| crate::db::links::get_outlinks(conn, &source_file_id))
    }

    fn get_chunks_by_file_ids(&self, file_ids: &[String]) -> Result<Vec<ChunkRow>> {
        let file_ids = file_ids.to_vec();
        self.with_read_conn(move |conn| crate::db::chunks::get_chunks_by_file_ids(conn, &file_ids))
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

use crate::hierarchy::{DerivedSummary, DerivedSummaryStore, ScopeType};

// -- DerivedSummaryStore for Db --------------------------------------------

impl DerivedSummaryStore for Db {
    fn generate_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<String> {
        use rusqlite::params;
        use std::time::{SystemTime, UNIX_EPOCH};
        use ulid::Ulid;
        use brain_persistence::error::BrainCoreError;

        let scope_type_str = scope_type.as_str();
        let scope_value_owned = scope_value.to_string();
        let scope_type_clone = scope_type.clone();

        let contents: Vec<String> = self.with_read_conn(|conn| {
            let rows: Vec<String> = match scope_type_clone {
                ScopeType::Directory => {
                    let pattern = format!("{}%", scope_value_owned);
                    let mut stmt = conn.prepare(
                        "SELECT c.content
                         FROM chunks c
                         JOIN files f ON c.file_id = f.file_id
                         WHERE f.path LIKE ?1
                         ORDER BY f.path, c.chunk_ord",
                    )?;
                    stmt.query_map(params![pattern], |row| row.get(0))
                        .map_err(|e| BrainCoreError::Database(e.to_string()))?
                        .collect::<std::result::Result<Vec<String>, _>>()
                        .map_err(|e| BrainCoreError::Database(e.to_string()))?
                }
                ScopeType::Tag => {
                    let pattern = format!("%{}%", scope_value_owned);
                    let mut stmt = conn.prepare(
                        "SELECT content
                         FROM summaries
                         WHERE tags LIKE ?1
                         ORDER BY created_at",
                    )?;
                    stmt.query_map(params![pattern], |row| row.get(0))
                        .map_err(|e| BrainCoreError::Database(e.to_string()))?
                        .collect::<std::result::Result<Vec<String>, _>>()
                        .map_err(|e| BrainCoreError::Database(e.to_string()))?
                }
            };
            Ok(rows)
        })?;

        let summary_content: String = contents
            .iter()
            .map(|c| if c.len() > 200 { &c[..200] } else { c.as_str() })
            .collect::<Vec<&str>>()
            .join("\n");

        let id = Ulid::new().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let id_clone = id.clone();
        let content_clone = summary_content.clone();
        let scope_value_clone = scope_value.to_string();

        self.with_write_conn(|conn| {
            conn.execute(
                "INSERT OR REPLACE INTO derived_summaries
                     (id, scope_type, scope_value, content, stale, generated_at)
                 VALUES (?1, ?2, ?3, ?4, 0, ?5)",
                params![id_clone, scope_type_str, scope_value_clone, content_clone, now],
            )
            .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            Ok(())
        })?;

        Ok(id)
    }

    fn get_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<Option<DerivedSummary>> {
        use rusqlite::params;
        use brain_persistence::error::BrainCoreError;

        let scope_type_str = scope_type.as_str().to_string();
        let scope_value_owned = scope_value.to_string();

        self.with_read_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, scope_type, scope_value, content, stale, generated_at
                 FROM derived_summaries
                 WHERE scope_type = ?1 AND scope_value = ?2",
            )?;

            let mut rows = stmt
                .query_map(params![scope_type_str, scope_value_owned], |row| {
                    Ok(DerivedSummary {
                        id: row.get(0)?,
                        scope_type: row.get(1)?,
                        scope_value: row.get(2)?,
                        content: row.get(3)?,
                        stale: row.get::<_, i64>(4)? != 0,
                        generated_at: row.get(5)?,
                    })
                })
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;

            match rows.next() {
                Some(Ok(summary)) => Ok(Some(summary)),
                Some(Err(e)) => Err(BrainCoreError::Database(e.to_string())),
                None => Ok(None),
            }
        })
    }

    fn mark_scope_stale(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<usize> {
        use rusqlite::params;
        use brain_persistence::error::BrainCoreError;

        let scope_type_str = scope_type.as_str().to_string();
        let scope_value_owned = scope_value.to_string();

        self.with_write_conn(|conn| {
            let n = conn
                .execute(
                    "UPDATE derived_summaries SET stale = 1
                     WHERE scope_type = ?1 AND scope_value = ?2",
                    params![scope_type_str, scope_value_owned],
                )
                .map_err(|e| BrainCoreError::Database(e.to_string()))?;
            Ok(n)
        })
    }

    fn search_derived_summaries(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<DerivedSummary>> {
        use rusqlite::params;
        use brain_persistence::error::BrainCoreError;

        let query_owned = query.to_string();
        let limit_i64 = limit as i64;

        self.with_read_conn(|conn| {
            let fts_exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type='table' AND name='fts_derived_summaries'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map(|n| n > 0)
                .unwrap_or(false);

            if fts_exists {
                let mut stmt = conn.prepare(
                    "SELECT ds.id, ds.scope_type, ds.scope_value, ds.content,
                            ds.stale, ds.generated_at
                     FROM fts_derived_summaries fts
                     JOIN derived_summaries ds ON ds.rowid = fts.rowid
                     WHERE fts_derived_summaries MATCH ?1
                     LIMIT ?2",
                )?;
                let summaries = stmt
                    .query_map(params![query_owned, limit_i64], |row| {
                        Ok(DerivedSummary {
                            id: row.get(0)?,
                            scope_type: row.get(1)?,
                            scope_value: row.get(2)?,
                            content: row.get(3)?,
                            stale: row.get::<_, i64>(4)? != 0,
                            generated_at: row.get(5)?,
                        })
                    })
                    .map_err(|e| BrainCoreError::Database(e.to_string()))?
                    .collect::<std::result::Result<Vec<DerivedSummary>, _>>()
                    .map_err(|e| BrainCoreError::Database(e.to_string()))?;
                Ok(summaries)
            } else {
                let pattern = format!("%{}%", query_owned);
                let mut stmt = conn.prepare(
                    "SELECT id, scope_type, scope_value, content, stale, generated_at
                     FROM derived_summaries
                     WHERE content LIKE ?1
                     LIMIT ?2",
                )?;
                let summaries = stmt
                    .query_map(params![pattern, limit_i64], |row| {
                        Ok(DerivedSummary {
                            id: row.get(0)?,
                            scope_type: row.get(1)?,
                            scope_value: row.get(2)?,
                            content: row.get(3)?,
                            stale: row.get::<_, i64>(4)? != 0,
                            generated_at: row.get(5)?,
                        })
                    })
                    .map_err(|e| BrainCoreError::Database(e.to_string()))?
                    .collect::<std::result::Result<Vec<DerivedSummary>, _>>()
                    .map_err(|e| BrainCoreError::Database(e.to_string()))?;
                Ok(summaries)
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Mock implementations for testing
// ---------------------------------------------------------------------------

#[cfg(test)]
pub mod mock;
