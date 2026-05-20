//! Persistence ports for `brain_persistence` storage types.
//!
//! This module:
//! - Defines the persistence-facing trait contracts whose method signatures
//!   reference types owned by `brain_persistence` (e.g. `ChunkRow`,
//!   `SummaryRow`, `Job`, `EnqueueJobInput`). Co-locating these with the
//!   storage types keeps protocol changes adjacent to their implementations.
//! - Provides the production implementations of every persistence-facing
//!   trait — both the brain_core-defined contracts (`ChunkIndexWriter`,
//!   `SchemaMeta`, `FileMetaReader`) and the brain_persistence-defined ones
//!   — for the concrete `Db`, `Store`, and `StoreReader` types.
//!
//! Brain_lib re-exports everything from this module so existing
//! `brain_lib::ports::*` paths keep resolving without touching call sites.

use std::collections::HashMap;

use brain_core::error::Result;
// Bring core trait names into module scope so the `impl X for Db / Store`
// blocks below can refer to them unqualified. Not re-exported (no `pub`) —
// brain_lib's `ports::*` consumer surface imports these from brain_core
// directly, and no caller relies on the `brain_persistence::ports::X` path
// for the core traits.
use brain_core::ports::{
    BrainRegistry, ChunkIndexWriter, FileMetaReader, SchemaMeta, SummaryStoreWriter,
};

use crate::db::Db;
use crate::db::chunks::{ChunkPollRow, ChunkRow};
use crate::db::fts::{FtsResult, FtsSummaryResult};
use crate::db::summaries::SummaryPollRow;
use crate::db::tasks::queries::TaskPollRow;
use crate::sql::SqlResultExt;
use crate::store::{QueryResult, Store, StoreReader, VectorSearchMode};

#[cfg(any(test, feature = "test-utils"))]
pub mod mock;

// ---------------------------------------------------------------------------
// Concrete implementations of the core write/schema/file-meta-reader traits
// ---------------------------------------------------------------------------

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

    fn prune_versions(
        &self,
        older_than: std::time::Duration,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        self.optimizer().prune_versions(older_than)
    }
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
    ) -> Result<HashMap<String, crate::db::summaries::SummaryMeta>>;
}

// -- ChunkMetaReader for Db ------------------------------------------------

impl ChunkMetaReader for Db {
    fn get_chunks_by_ids(&self, chunk_ids: &[String]) -> Result<Vec<ChunkRow>> {
        self.with_read_conn(|conn| crate::db::chunks::get_chunks_by_ids(conn, chunk_ids))
            .into_brain_core()
    }

    fn get_ml_summaries_for_chunks(&self, chunk_ids: &[&str]) -> Result<HashMap<String, String>> {
        self.with_read_conn(|conn| {
            crate::db::summaries::get_ml_summaries_for_chunks(conn, chunk_ids)
        })
        .into_brain_core()
    }

    fn get_summary_metadata(
        &self,
        summary_ids: &[String],
    ) -> Result<HashMap<String, crate::db::summaries::SummaryMeta>> {
        let ids = summary_ids.to_vec();
        self.with_read_conn(move |conn| crate::db::summaries::get_summary_metadata(conn, &ids))
            .into_brain_core()
    }
}

// -- FileMetaReader for Db -------------------------------------------------

impl FileMetaReader for Db {
    fn get_all_active_paths(&self) -> Result<Vec<(String, String)>> {
        self.with_read_conn(crate::db::files::get_all_active_paths)
            .into_brain_core()
    }

    fn get_active_paths_for_brain(&self, brain_id: &str) -> Result<Vec<(String, String)>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            crate::db::files::get_active_paths_for_brain(conn, &brain_id)
        })
        .into_brain_core()
    }

    fn get_files_with_hashes(&self) -> Result<Vec<(String, String, Option<String>)>> {
        self.with_read_conn(crate::db::files::get_files_with_hashes)
            .into_brain_core()
    }

    fn find_stuck_files(&self) -> Result<Vec<(String, String)>> {
        self.with_read_conn(crate::db::files::find_stuck_files)
            .into_brain_core()
    }

    fn get_content_hash(&self, file_id: &str) -> Result<Option<String>> {
        let file_id = file_id.to_string();
        self.with_read_conn(move |conn| crate::db::files::get_content_hash(conn, &file_id))
            .into_brain_core()
    }

    fn get_chunker_version(&self, file_id: &str) -> Result<Option<u32>> {
        let file_id = file_id.to_string();
        self.with_read_conn(move |conn| crate::db::files::get_chunker_version(conn, &file_id))
            .into_brain_core()
    }
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
            crate::db::fts::search_fts(conn, &query, limit, brain_ids.as_deref())
        })
        .into_brain_core()
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
            crate::db::fts::search_summaries_fts(conn, &query, limit, brain_ids.as_deref())
        })
        .into_brain_core()
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
            crate::db::files::get_or_create_file_id(conn, &path, &brain_id)
        })
        .into_brain_core()
    }

    fn handle_delete(&self, path: &str) -> Result<Option<String>> {
        let path = path.to_string();
        self.with_write_conn(move |conn| crate::db::files::handle_delete(conn, &path))
            .into_brain_core()
    }

    fn handle_rename(&self, file_id: &str, new_path: &str) -> Result<()> {
        let file_id = file_id.to_string();
        let new_path = new_path.to_string();
        self.with_write_conn(move |conn| crate::db::files::handle_rename(conn, &file_id, &new_path))
            .into_brain_core()
    }

    fn purge_deleted_files(&self, older_than_ts: i64) -> Result<Vec<String>> {
        self.with_write_conn(move |conn| crate::db::files::purge_deleted_files(conn, older_than_ts))
            .into_brain_core()
    }

    fn clear_all_content_hashes(&self) -> Result<usize> {
        self.with_write_conn(crate::db::files::clear_all_content_hashes)
            .into_brain_core()
    }

    fn clear_content_hash_by_path(&self, path: &str) -> Result<bool> {
        let path = path.to_string();
        self.with_write_conn(move |conn| crate::db::files::clear_content_hash_by_path(conn, &path))
            .into_brain_core()
    }

    fn set_indexing_state(&self, file_id: &str, state: &str) -> Result<()> {
        let file_id = file_id.to_string();
        let state = state.to_string();
        self.with_write_conn(move |conn| {
            crate::db::files::set_indexing_state(conn, &file_id, &state)
        })
        .into_brain_core()
    }

    fn reset_stuck_file_for_reindex(&self, file_id: &str) -> Result<()> {
        let file_id = file_id.to_string();
        self.with_write_conn(move |conn| {
            crate::db::files::reset_stuck_file_for_reindex(conn, &file_id)
        })
        .into_brain_core()
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
            crate::db::files::mark_indexed(
                conn,
                &file_id,
                &content_hash,
                chunker_version,
                disk_modified_at,
            )
        })
        .into_brain_core()
    }

    fn count_stale_chunker_version(&self, current_version: u32) -> Result<usize> {
        self.with_read_conn(move |conn| {
            crate::db::files::count_stale_chunker_version(conn, current_version)
        })
        .into_brain_core()
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
        chunks: &[crate::db::chunks::ChunkMeta],
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
        chunks: &[crate::db::chunks::ChunkMeta],
        brain_id: &str,
    ) -> Result<()> {
        // ChunkMeta is not Clone/Send, so we must call within the closure directly.
        // Caller must ensure chunks slice lifetime covers the closure.
        // We delegate via with_write_conn using a shared reference approach.
        self.with_write_conn(|conn| {
            crate::db::chunks::replace_chunk_metadata(conn, file_id, chunks, brain_id)
        })
        .into_brain_core()
    }

    fn get_chunk_hashes(&self, file_id: &str) -> Result<Vec<String>> {
        let file_id = file_id.to_string();
        self.with_read_conn(move |conn| crate::db::chunks::get_chunk_hashes(conn, &file_id))
            .into_brain_core()
    }

    fn mark_chunks_embedded(&self, chunk_ids: &[&str], timestamp: i64) -> Result<()> {
        let ids: Vec<String> = chunk_ids.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            crate::db::chunks::mark_chunks_embedded(conn, &refs, timestamp)
        })
        .into_brain_core()
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
            crate::db::chunks::upsert_task_chunk(conn, &task_file_id, &capsule_text, &brain_id)
        })
        .into_brain_core()
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
            crate::db::chunks::upsert_record_chunk(conn, &record_file_id, &capsule_text, &brain_id)
        })
        .into_brain_core()
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
    fn replace_links(&self, file_id: &str, links: &[crate::links::Link]) -> Result<()>;
}

// -- LinkWriter for Db -------------------------------------------------------

impl LinkWriter for Db {
    fn replace_links(&self, file_id: &str, links: &[crate::links::Link]) -> Result<()> {
        let file_id = file_id.to_string();
        let links = links.to_vec();
        self.with_write_conn(move |conn| crate::db::links::replace_links(conn, &file_id, &links))
            .into_brain_core()
    }
}

// ---------------------------------------------------------------------------
// SQLite write path — self-heal / embedded_at reset
// ---------------------------------------------------------------------------

/// Key used in brain_meta to store the last embedded_at reset timestamp (Unix s).
pub(crate) const EMBED_RESET_META_KEY: &str = "embed_reset_at";

/// Key used in brain_meta to store the count of consecutive self-heal resets.
/// Reset to 0 when a successful schema check is observed.
pub(crate) const EMBED_CONSECUTIVE_RESETS_KEY: &str = "embed_consecutive_resets";

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

    /// Record the current time as the last embed reset timestamp in brain_meta.
    fn record_embed_reset(&self) -> Result<()>;

    /// Seconds since the last embed reset, or `None` if no reset has been recorded.
    fn last_embed_reset_before(&self) -> Result<Option<i64>>;

    /// Number of consecutive self-heal resets that have occurred without a
    /// successful schema check in between. Returns 0 when no value is stored.
    fn get_consecutive_resets(&self) -> Result<u32>;

    /// Persist the consecutive reset count to brain_meta.
    fn set_consecutive_resets(&self, count: u32) -> Result<()>;
}

// -- EmbeddingResetter for Db ----------------------------------------------

impl EmbeddingResetter for Db {
    fn reset_tasks_embedded_at(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            conn.execute_batch("UPDATE tasks SET embedded_at = NULL;")?;
            Ok(())
        })
        .into_brain_core()
    }

    fn reset_chunks_embedded_at(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            conn.execute_batch("UPDATE chunks SET embedded_at = NULL;")?;
            Ok(())
        })
        .into_brain_core()
    }

    fn reset_records_embedded_at(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            conn.execute_batch("UPDATE records SET embedded_at = NULL;")?;
            Ok(())
        })
        .into_brain_core()
    }

    fn record_embed_reset(&self) -> Result<()> {
        use crate::db::meta;
        self.with_write_conn(|conn| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            meta::set_meta(conn, EMBED_RESET_META_KEY, &now.to_string())
        })
        .into_brain_core()
    }

    fn last_embed_reset_before(&self) -> Result<Option<i64>> {
        use crate::db::meta;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        self.with_read_conn(|conn| {
            let last = meta::get_meta_i64(conn, EMBED_RESET_META_KEY)?;
            Ok(last.map(|t| now.saturating_sub(t)))
        })
        .into_brain_core()
    }

    fn get_consecutive_resets(&self) -> Result<u32> {
        use crate::db::meta;
        self.with_read_conn(|conn| {
            Ok(meta::get_meta_u32(conn, EMBED_CONSECUTIVE_RESETS_KEY)?.unwrap_or(0))
        })
        .into_brain_core()
    }

    fn set_consecutive_resets(&self, count: u32) -> Result<()> {
        use crate::db::meta;
        self.with_write_conn(move |conn| {
            meta::set_meta(conn, EMBED_CONSECUTIVE_RESETS_KEY, &count.to_string())
        })
        .into_brain_core()
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
        .into_brain_core()
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
        .into_brain_core()
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
        .into_brain_core()
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
            .into_brain_core()
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
        .into_brain_core()
    }

    fn get_summaries_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<crate::db::summaries::SummaryRow>> {
        let ids = ids.to_vec();
        self.with_read_conn(move |conn| crate::db::summaries::get_summaries_by_ids(conn, &ids))
            .into_brain_core()
    }
}

// ---------------------------------------------------------------------------
// Brain registry — production impl for the core-defined trait
// ---------------------------------------------------------------------------
//
// The trait itself lives in `brain_core::ports` so its signature can speak
// purely in core DTOs. The `Db` impl below maps each `BrainRow` to a
// `brain_core::brain::Brain` at the boundary, so persistence row types stay
// inside `brain_persistence`.

// -- BrainRegistry for Db --------------------------------------------------

impl BrainRegistry for Db {
    fn is_brain_archived(&self, brain_id: &str) -> Result<bool> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| crate::db::schema::is_brain_archived(conn, &brain_id))
            .into_brain_core()
    }

    fn list_brains(&self, active_only: bool) -> Result<Vec<brain_core::brain::Brain>> {
        let rows = self
            .with_read_conn(move |conn| crate::db::schema::list_brains(conn, active_only))
            .into_brain_core()?;
        Ok(rows.into_iter().map(brain_row_to_brain).collect())
    }

    fn list_brain_keys(&self) -> Result<Vec<(String, String)>> {
        let rows = self
            .with_read_conn(|conn| crate::db::schema::list_brains(conn, true))
            .into_brain_core()?;
        let mut pairs: Vec<(String, String)> =
            rows.into_iter().map(|r| (r.name, r.brain_id)).collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pairs)
    }
}

/// Map a persistence-layer `BrainRow` to the framework-free `Brain` DTO.
///
/// Drops `notes_json` and `projected` — neither is consumed via the
/// `BrainRegistry` trait surface. Callers that still need the rich row can
/// use the inherent `Db::list_brains` helper.
fn brain_row_to_brain(row: crate::db::schema::BrainRow) -> brain_core::brain::Brain {
    brain_core::brain::Brain {
        brain_id: row.brain_id,
        name: row.name,
        prefix: row.prefix,
        roots_json: row.roots_json,
        aliases_json: row.aliases_json,
        archived: row.archived,
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
            .into_brain_core()
    }

    fn stale_hashes_prevented(&self) -> Result<u64> {
        self.with_read_conn(crate::db::meta::stale_hashes_prevented)
            .into_brain_core()
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
            .into_brain_core()
    }

    fn vacuum_db(&self) -> Result<()> {
        self.with_write_conn(crate::db::files::vacuum)
            .into_brain_core()
    }

    fn reindex_fts(&self) -> Result<()> {
        self.with_write_conn(|conn| {
            crate::db::fts::reindex_fts(conn)?;
            Ok(())
        })
        .into_brain_core()
    }

    fn fts_consistency(&self) -> Result<(i64, i64)> {
        self.with_read_conn(crate::db::fts::fts_consistency)
            .into_brain_core()
    }

    fn reindex_summaries_fts(&self) -> Result<usize> {
        self.with_write_conn(crate::db::fts::reindex_summaries_fts)
            .into_brain_core()
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
        .into_brain_core()
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
                conn, &title, &steps, &tags, importance, &brain_id,
            )
        })
        .into_brain_core()
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
    fn get_brain(&self, brain_id: &str) -> Result<Option<crate::db::schema::BrainRow>>;
}

// -- BrainManager for Db -----------------------------------------------------

impl BrainManager for Db {
    fn archive_brain(&self, brain_id: &str) -> Result<()> {
        let brain_id = brain_id.to_string();
        self.with_write_conn(move |conn| crate::db::schema::archive_brain(conn, &brain_id))
            .into_brain_core()
    }

    fn get_brain(&self, brain_id: &str) -> Result<Option<crate::db::schema::BrainRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| crate::db::schema::get_brain(conn, &brain_id))
            .into_brain_core()
    }
}

// ---------------------------------------------------------------------------
// LanceDB write path — summary embeddings
// ---------------------------------------------------------------------------

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
        self.with_read_conn(move |conn| crate::db::links::get_outlinks(conn, &source_file_id))
            .into_brain_core()
    }

    fn get_chunks_by_file_ids(&self, file_ids: &[String]) -> Result<Vec<ChunkRow>> {
        let file_ids = file_ids.to_vec();
        self.with_read_conn(move |conn| crate::db::chunks::get_chunks_by_file_ids(conn, &file_ids))
            .into_brain_core()
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
    ) -> Result<Vec<crate::db::records::queries::RecordPollRow>>;

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
            crate::db::chunks::find_stale_for_embedding(conn, &brain_id)
        })
        .into_brain_core()
    }

    fn find_stale_summaries_for_embedding(&self, brain_id: &str) -> Result<Vec<SummaryPollRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            crate::db::summaries::find_stale_summaries_for_embedding(conn, &brain_id)
        })
        .into_brain_core()
    }

    fn find_stale_tasks_for_embedding(&self, brain_id: &str) -> Result<Vec<TaskPollRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            crate::db::tasks::queries::find_stale_tasks_for_embedding(conn, &brain_id)
        })
        .into_brain_core()
    }

    fn find_stale_records_for_embedding(
        &self,
        brain_id: &str,
    ) -> Result<Vec<crate::db::records::queries::RecordPollRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            crate::db::records::queries::find_stale_records_for_embedding(conn, &brain_id)
        })
        .into_brain_core()
    }

    fn mark_summaries_embedded(&self, summary_ids: &[&str]) -> Result<()> {
        let summary_ids: Vec<String> = summary_ids.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = summary_ids.iter().map(|s| s.as_str()).collect();
            crate::db::summaries::mark_summaries_embedded(conn, &refs)
        })
        .into_brain_core()
    }

    fn mark_tasks_embedded(&self, task_ids: &[&str]) -> Result<()> {
        let task_ids: Vec<String> = task_ids.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = task_ids.iter().map(|s| s.as_str()).collect();
            crate::db::chunks::mark_tasks_embedded(conn, &refs)
        })
        .into_brain_core()
    }

    fn mark_records_embedded(&self, record_ids: &[&str]) -> Result<()> {
        let record_ids: Vec<String> = record_ids.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = record_ids.iter().map(|s| s.as_str()).collect();
            crate::db::records::queries::mark_records_embedded(conn, &refs)
        })
        .into_brain_core()
    }
}

// ---------------------------------------------------------------------------
// Job persistence — used by job_worker
// ---------------------------------------------------------------------------

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
            crate::job_results::persist_scope_summary_result(conn, &summary_id, &result)
        })
        .into_brain_core()
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
            crate::job_results::persist_consolidation_result(
                conn,
                &suggested_title,
                &result,
                &episode_ids,
                &brain_id,
            )
        })
        .into_brain_core()
    }
}

// ---------------------------------------------------------------------------
// Job queue operations — used by job_worker and daemon event loop
// ---------------------------------------------------------------------------

use crate::db::job::{Job, JobStatus};
use crate::db::jobs::EnqueueJobInput;

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
        self.with_write_conn(|conn| crate::db::jobs::claim_ready_jobs(conn, limit))
            .into_brain_core()
    }

    fn advance_to_in_progress(&self, job_id: &str) -> Result<()> {
        let job_id = job_id.to_string();
        self.with_write_conn(move |conn| crate::db::jobs::advance_to_in_progress(conn, &job_id))
            .into_brain_core()
    }

    fn complete_job(&self, job_id: &str, result: Option<&str>) -> Result<()> {
        let job_id = job_id.to_string();
        let result = result.map(|s| s.to_string());
        self.with_write_conn(move |conn| {
            crate::db::jobs::complete_job(conn, &job_id, result.as_deref())
        })
        .into_brain_core()
    }

    fn fail_job(&self, job_id: &str, error_msg: &str) -> Result<()> {
        let job_id = job_id.to_string();
        let error_msg = error_msg.to_string();
        self.with_write_conn(move |conn| crate::db::jobs::fail_job(conn, &job_id, &error_msg))
            .into_brain_core()
    }

    fn reap_stuck_jobs(&self) -> Result<usize> {
        self.with_write_conn(crate::db::jobs::reap_stuck_jobs)
            .into_brain_core()
    }

    fn enqueue_job(&self, input: &EnqueueJobInput) -> Result<String> {
        let input = input.clone();
        self.with_write_conn(move |conn| crate::db::jobs::enqueue_job(conn, &input))
            .into_brain_core()
    }

    fn gc_completed_jobs(&self, age_secs: i64, protected_kinds: &[&str]) -> Result<usize> {
        let protected: Vec<String> = protected_kinds.iter().map(|s| s.to_string()).collect();
        self.with_write_conn(move |conn| {
            let refs: Vec<&str> = protected.iter().map(|s| s.as_str()).collect();
            crate::db::jobs::gc_completed_jobs(conn, age_secs, &refs)
        })
        .into_brain_core()
    }

    fn count_jobs_by_status(&self, status: &JobStatus) -> Result<i64> {
        self.with_read_conn(move |conn| crate::db::jobs::count_jobs_by_status(conn, status))
            .into_brain_core()
    }

    fn list_jobs_by_status(&self, status: &JobStatus, limit: i32) -> Result<Vec<Job>> {
        self.with_read_conn(move |conn| crate::db::jobs::list_jobs_by_status(conn, status, limit))
            .into_brain_core()
    }

    fn list_stuck_jobs(&self) -> Result<Vec<Job>> {
        self.with_read_conn(crate::db::jobs::list_stuck_jobs)
            .into_brain_core()
    }

    fn retry_failed_job(&self, job_id: &str) -> Result<bool> {
        let id = job_id.to_string();
        self.with_write_conn(move |conn| crate::db::jobs::retry_failed_job(conn, &id))
            .into_brain_core()
    }

    fn get_job_by_kind(&self, kind: &str) -> Result<Option<Job>> {
        let kind = kind.to_string();
        self.with_read_conn(move |conn| crate::db::jobs::get_job_by_kind(conn, &kind))
            .into_brain_core()
    }
    fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        let job_id = job_id.to_string();
        self.with_read_conn(move |conn| crate::db::jobs::get_job(conn, &job_id))
            .into_brain_core()
    }

    fn update_job_status(&self, job_id: &str, status: &JobStatus) -> Result<bool> {
        let job_id = job_id.to_string();
        let status = *status;
        self.with_write_conn(move |conn| crate::db::jobs::update_job_status(conn, &job_id, &status))
            .into_brain_core()
    }

    fn ensure_singleton_job(&self, input: &EnqueueJobInput) -> Result<Option<String>> {
        let input = input.clone();
        self.with_write_conn(move |conn| crate::db::jobs::ensure_singleton_job(conn, &input))
            .into_brain_core()
    }

    fn reschedule_terminal_job(&self, kind: &str, brain_id: Option<&str>) -> Result<bool> {
        let kind = kind.to_string();
        let brain_id = brain_id.map(|s| s.to_string());
        self.with_write_conn(move |conn| {
            crate::db::jobs::reschedule_terminal_job(conn, &kind, brain_id.as_deref(), 0)
        })
        .into_brain_core()
    }

    fn enqueue_dedup_job(&self, input: &EnqueueJobInput) -> Result<(String, bool)> {
        let input = input.clone();
        self.with_write_conn(move |conn| crate::db::jobs::enqueue_dedup_job(conn, &input))
            .into_brain_core()
    }

    fn reconcile_singleton_job(&self, input: &EnqueueJobInput) -> Result<()> {
        let input = input.clone();
        self.with_write_conn(move |conn| crate::db::jobs::reconcile_singleton_job(conn, &input))
            .into_brain_core()
    }

    fn reconcile_singleton_job_with_delay(
        &self,
        input: &EnqueueJobInput,
        delay_secs: i64,
    ) -> Result<()> {
        let input = input.clone();
        self.with_write_conn(move |conn| {
            crate::db::jobs::reconcile_singleton_job_with_delay(conn, &input, delay_secs)
        })
        .into_brain_core()
    }

    fn has_active_lod_job(&self, object_uri: &str) -> Result<bool> {
        let uri = object_uri.to_string();
        self.with_read_conn(move |conn| crate::db::jobs::has_active_lod_job(conn, &uri))
            .into_brain_core()
    }
}

// ---------------------------------------------------------------------------
// Provider store — used by llm::resolve_provider and CLI
// ---------------------------------------------------------------------------

use crate::db::providers::{InsertProvider, ProviderRow};

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

    /// Get the provider matching a given name and key hash.
    fn get_provider_by_name_and_hash(
        &self,
        name: &str,
        api_key_hash: &str,
    ) -> Result<Option<ProviderRow>>;

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

    fn get_provider_by_name_and_hash(
        &self,
        name: &str,
        api_key_hash: &str,
    ) -> Result<Option<ProviderRow>> {
        self.get_provider_by_name_and_hash(name, api_key_hash)
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

// ---------------------------------------------------------------------------
// SQLite read path — synonym clustering (tag_aliases / tag_cluster_runs)
// ---------------------------------------------------------------------------

/// Read-side operations for the synonym-clustering job.
///
/// Consumers: `brain_lib::tags::recluster::run_recluster`. The write side
/// (Tx-1, Tx-2, Tx-3) lives in `TagAliasWriter`.
pub trait TagAliasReader: Send + Sync {
    /// Per-brain raw-tag collector. Calls
    /// `crate::db::tags::collect_raw_tags` with `Some(brain_id)`,
    /// then folds the resulting `Vec<RawTag>` across `(tag, source)` pairs
    /// into a `Vec<DedupedRawTag>` keyed by tag string.
    fn collect_raw_tags(
        &self,
        brain_id: &str,
    ) -> Result<Vec<crate::db::tag_aliases::DedupedRawTag>>;

    /// Snapshot every `tag_aliases` row for the given brain, keyed by
    /// `raw_tag`.
    fn read_alias_snapshot(
        &self,
        brain_id: &str,
    ) -> Result<HashMap<String, crate::db::tag_aliases::ExistingAlias>>;

    /// Per-brain `(raw_tag → canonical_tag)` lookup for read-time alias
    /// expansion in the query path. Both keys and values are lowercased.
    /// Returns an empty map for brains that have never been reclustered.
    fn alias_lookup_for_brain(&self, brain_id: &str) -> Result<HashMap<String, String>>;

    /// List `tag_aliases` rows for a brain with optional filtering and
    /// pagination. Used by the `tags.aliases_list` MCP tool and CLI.
    fn list_aliases_for_brain(
        &self,
        brain_id: &str,
        canonical: Option<&str>,
        cluster_id: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<crate::db::tag_aliases::AliasRow>>;

    /// Aggregate counts over `tag_aliases` for a brain (raw / distinct
    /// canonicals / distinct clusters). Used by the `tags.aliases_status`
    /// MCP tool and CLI.
    fn count_aliases_for_brain(
        &self,
        brain_id: &str,
    ) -> Result<crate::db::tag_aliases::AliasCounts>;

    /// Most recent `tag_cluster_runs` row for a brain. Returns `None` for
    /// brains that have never been reclustered.
    fn latest_run_for_brain(
        &self,
        brain_id: &str,
    ) -> Result<Option<crate::db::tag_aliases::TagClusterRunRow>>;
}

// -- TagAliasReader for Db -------------------------------------------------

impl TagAliasReader for Db {
    fn collect_raw_tags(
        &self,
        brain_id: &str,
    ) -> Result<Vec<crate::db::tag_aliases::DedupedRawTag>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            let raw = crate::db::tags::collect_raw_tags(conn, Some(&brain_id))?;
            Ok(crate::db::tag_aliases::dedupe_by_tag(raw))
        })
        .into_brain_core()
    }

    fn read_alias_snapshot(
        &self,
        brain_id: &str,
    ) -> Result<HashMap<String, crate::db::tag_aliases::ExistingAlias>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            crate::db::tag_aliases::read_alias_snapshot(conn, &brain_id)
        })
        .into_brain_core()
    }

    fn alias_lookup_for_brain(&self, brain_id: &str) -> Result<HashMap<String, String>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            crate::db::tag_aliases::alias_lookup_for_brain(conn, &brain_id)
        })
        .into_brain_core()
    }

    fn list_aliases_for_brain(
        &self,
        brain_id: &str,
        canonical: Option<&str>,
        cluster_id: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<crate::db::tag_aliases::AliasRow>> {
        let brain_id = brain_id.to_string();
        let canonical = canonical.map(|s| s.to_string());
        let cluster_id = cluster_id.map(|s| s.to_string());
        self.with_read_conn(move |conn| {
            crate::db::tag_aliases::list_aliases_for_brain(
                conn,
                &brain_id,
                canonical.as_deref(),
                cluster_id.as_deref(),
                limit,
                offset,
            )
        })
        .into_brain_core()
    }

    fn count_aliases_for_brain(
        &self,
        brain_id: &str,
    ) -> Result<crate::db::tag_aliases::AliasCounts> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            crate::db::tag_aliases::count_aliases_for_brain(conn, &brain_id)
        })
        .into_brain_core()
    }

    fn latest_run_for_brain(
        &self,
        brain_id: &str,
    ) -> Result<Option<crate::db::tag_aliases::TagClusterRunRow>> {
        let brain_id = brain_id.to_string();
        self.with_read_conn(move |conn| {
            crate::db::tag_aliases::latest_run_for_brain(conn, &brain_id)
        })
        .into_brain_core()
    }
}

// ---------------------------------------------------------------------------
// SQLite write path — synonym clustering (tag_aliases / tag_cluster_runs)
// ---------------------------------------------------------------------------

/// Write-side operations for the synonym-clustering job.
///
/// Three transactions, in order:
/// - [`insert_run`](TagAliasWriter::insert_run): Tx-1, FK precondition.
/// - [`apply_alias_upserts`](TagAliasWriter::apply_alias_upserts): Tx-2,
///   atomic upsert + run-row finalize.
/// - [`record_run_failure`](TagAliasWriter::record_run_failure): Tx-3,
///   error path only.
pub trait TagAliasWriter: Send + Sync {
    /// Tx-1: INSERT a `tag_cluster_runs` row with `finished_at = NULL`.
    fn insert_run(&self, input: crate::db::tag_aliases::InsertRun) -> Result<()>;

    /// Tx-2: atomic UPSERT of all alias rows for the given run, DELETE of
    /// `stale` rows scoped to `brain_id`, and finalize the run row.
    fn apply_alias_upserts(
        &self,
        brain_id: &str,
        upserts: Vec<crate::db::tag_aliases::AliasUpsert>,
        stale: Vec<String>,
        finalize: crate::db::tag_aliases::FinalizeRun,
    ) -> Result<()>;

    /// Tx-3: record a failure on the existing run row.
    fn record_run_failure(&self, run_id: &str, finished_at_iso: &str, notes: &str) -> Result<()>;
}

// -- TagAliasWriter for Db -------------------------------------------------

impl TagAliasWriter for Db {
    fn insert_run(&self, input: crate::db::tag_aliases::InsertRun) -> Result<()> {
        self.with_write_conn(move |conn| crate::db::tag_aliases::insert_run(conn, &input))
            .into_brain_core()
    }

    fn apply_alias_upserts(
        &self,
        brain_id: &str,
        upserts: Vec<crate::db::tag_aliases::AliasUpsert>,
        stale: Vec<String>,
        finalize: crate::db::tag_aliases::FinalizeRun,
    ) -> Result<()> {
        let brain_id = brain_id.to_string();
        self.with_write_conn(move |conn| {
            crate::db::tag_aliases::apply_alias_upserts(
                conn, &brain_id, &upserts, &stale, &finalize,
            )
        })
        .into_brain_core()
    }

    fn record_run_failure(&self, run_id: &str, finished_at_iso: &str, notes: &str) -> Result<()> {
        let run_id = run_id.to_string();
        let finished_at_iso = finished_at_iso.to_string();
        let notes = notes.to_string();
        self.with_write_conn(move |conn| {
            crate::db::tag_aliases::record_run_failure(conn, &run_id, &finished_at_iso, &notes)
        })
        .into_brain_core()
    }
}
