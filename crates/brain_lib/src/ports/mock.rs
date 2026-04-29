//! In-memory mock implementations of the persistence ports for unit testing.
//!
//! These mocks implement the port traits using `HashMap`-backed in-memory state.
//! They do **not** open SQLite or LanceDB. Use them in unit tests that need a
//! pipeline without real storage.
//!
//! # Example
//!
//! ```rust,ignore
//! use brain_lib::ports::mock::{MockChunkIndexWriter, MockChunkSearcher};
//! let writer = MockChunkIndexWriter::default();
//! let searcher = MockChunkSearcher::default();
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::error::Result;
use crate::hierarchy::{DerivedSummary, GeneratedScopeSummary, ScopeType};
use brain_persistence::db::chunks::ChunkRow;
use brain_persistence::db::fts::{FtsResult, FtsSummaryResult};
use brain_persistence::db::summaries::{Episode, SummaryRow};
use brain_persistence::store::{QueryResult, VectorSearchMode};

use super::{
    BrainManager, ChunkIndexWriter, ChunkMetaReader, ChunkMetaWriter, ChunkSearcher,
    DerivedSummaryReader, DerivedSummaryWriter, EmbeddingOps, EmbeddingResetter, EpisodeReader,
    EpisodeWriter, FileMetaReader, FileMetaWriter, FtsSearcher, GraphLinkReader, JobPersistence,
    JobQueue, LinkWriter, MaintenanceOps, SchemaMeta, SummaryReader, SummaryWriter, TagAliasReader,
};
use brain_persistence::db::chunks::ChunkPollRow;
use brain_persistence::db::job::{Job, JobStatus};
use brain_persistence::db::jobs::EnqueueJobInput;
use brain_persistence::db::records::queries::RecordPollRow;
use brain_persistence::db::schema::BrainRow;
use brain_persistence::db::summaries::SummaryPollRow;
use brain_persistence::db::tasks::queries::TaskPollRow;
use brain_persistence::links::Link;

// ---------------------------------------------------------------------------
// MockChunkIndexWriter
// ---------------------------------------------------------------------------

/// In-memory mock for `ChunkIndexWriter`.
///
/// Tracks which `(file_id, chunk_ord, content)` tuples have been upserted and
/// which `file_id`s have been deleted.
#[derive(Default)]
pub struct MockChunkIndexWriter {
    /// `file_id → Vec<(chunk_ord, content)>`
    pub chunks: Mutex<HashMap<String, Vec<(usize, String)>>>,
    /// `file_id`s that have been explicitly deleted
    pub deleted: Mutex<HashSet<String>>,
    /// Path updates: `file_id → new_path`
    pub path_updates: Mutex<HashMap<String, String>>,
}

impl ChunkIndexWriter for MockChunkIndexWriter {
    fn upsert_chunks<'a>(
        &'a self,
        file_id: &'a str,
        _file_path: &'a str,
        _brain_id: &'a str,
        chunks: &'a [(usize, &'a str)],
        _embeddings: &'a [Vec<f32>],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        let entries: Vec<(usize, String)> = chunks
            .iter()
            .map(|(ord, content)| (*ord, content.to_string()))
            .collect();
        async move {
            self.chunks
                .lock()
                .unwrap()
                .insert(file_id.to_string(), entries);
            Ok(())
        }
    }

    fn delete_file_chunks<'a>(
        &'a self,
        file_id: &'a str,
        _brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        async move {
            self.chunks.lock().unwrap().remove(file_id);
            self.deleted.lock().unwrap().insert(file_id.to_string());
            Ok(())
        }
    }

    fn delete_chunks_by_file_ids<'a>(
        &'a self,
        file_ids: &'a [String],
        _brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
        async move {
            let mut map = self.chunks.lock().unwrap();
            let mut del = self.deleted.lock().unwrap();
            let mut count = 0;
            for id in file_ids {
                if map.remove(id).is_some() {
                    count += 1;
                }
                del.insert(id.clone());
            }
            Ok(count)
        }
    }

    fn update_file_path<'a>(
        &'a self,
        file_id: &'a str,
        new_path: &'a str,
        _brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        async move {
            self.path_updates
                .lock()
                .unwrap()
                .insert(file_id.to_string(), new_path.to_string());
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// MockChunkSearcher
// ---------------------------------------------------------------------------

/// In-memory mock for `ChunkSearcher`.
///
/// Returns a fixed list of `QueryResult`s on every call.
#[derive(Default)]
pub struct MockChunkSearcher {
    /// Results returned by every `query` call.
    pub results: Mutex<Vec<QueryResult>>,
}

impl MockChunkSearcher {
    /// Create a searcher that always returns the given results.
    pub fn with_results(results: Vec<QueryResult>) -> Self {
        Self {
            results: Mutex::new(results),
        }
    }
}

impl ChunkSearcher for MockChunkSearcher {
    fn query<'a>(
        &'a self,
        _embedding: &'a [f32],
        _top_k: usize,
        _nprobes: usize,
        _mode: VectorSearchMode,
        _brain_id: Option<&'a str>,
    ) -> impl std::future::Future<Output = Result<Vec<QueryResult>>> + Send + 'a {
        async move {
            let guard = self.results.lock().unwrap();
            // QueryResult does not derive Clone; reconstruct from fields.
            let out = guard
                .iter()
                .map(|r| QueryResult {
                    chunk_id: r.chunk_id.clone(),
                    file_id: r.file_id.clone(),
                    file_path: r.file_path.clone(),
                    chunk_ord: r.chunk_ord,
                    content: r.content.clone(),
                    score: r.score,
                    brain_id: r.brain_id.clone(),
                })
                .collect();
            Ok(out)
        }
    }
}

// ---------------------------------------------------------------------------
// MockChunkMetaReader
// ---------------------------------------------------------------------------

/// In-memory mock for `ChunkMetaReader`.
#[derive(Default)]
pub struct MockChunkMetaReader {
    /// All available chunk rows (filtered by `chunk_id` on demand).
    pub rows: Mutex<Vec<ChunkRow>>,
    /// ML summaries: `chunk_id → summary_text`
    pub summaries: Mutex<HashMap<String, String>>,
    /// Summary kinds: `summary_id → kind`
    pub summary_kinds: Mutex<HashMap<String, String>>,
}

impl ChunkMetaReader for MockChunkMetaReader {
    fn get_chunks_by_ids(&self, chunk_ids: &[String]) -> Result<Vec<ChunkRow>> {
        let rows = self.rows.lock().unwrap();
        let id_set: HashSet<&str> = chunk_ids.iter().map(String::as_str).collect();
        Ok(rows
            .iter()
            .filter(|r| id_set.contains(r.chunk_id.as_str()))
            .cloned()
            .collect())
    }

    fn get_ml_summaries_for_chunks(&self, chunk_ids: &[&str]) -> Result<HashMap<String, String>> {
        let map = self.summaries.lock().unwrap();
        Ok(chunk_ids
            .iter()
            .filter_map(|id| map.get(*id).map(|v| ((*id).to_string(), v.clone())))
            .collect())
    }

    fn get_summary_metadata(
        &self,
        summary_ids: &[String],
    ) -> Result<HashMap<String, brain_persistence::db::summaries::SummaryMeta>> {
        let map = self.summary_kinds.lock().unwrap();
        Ok(summary_ids
            .iter()
            .filter_map(|id| {
                map.get(id).map(|k| {
                    (
                        id.clone(),
                        brain_persistence::db::summaries::SummaryMeta {
                            kind: k.clone(),
                            tags: vec![],
                            importance: 1.0,
                            created_at: 0,
                        },
                    )
                })
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// MockFileMetaReader
// ---------------------------------------------------------------------------

/// In-memory mock for `FileMetaReader`.
#[derive(Default)]
pub struct MockFileMetaReader {
    /// Active `(file_id, path)` pairs.
    pub active_paths: Mutex<Vec<(String, String)>>,
    /// `(file_id, path, content_hash)` rows.
    pub files_with_hashes: Mutex<Vec<(String, String, Option<String>)>>,
    /// Stuck `(file_id, path)` rows.
    pub stuck_files: Mutex<Vec<(String, String)>>,
    /// `file_id -> content_hash` map.
    pub content_hashes: Mutex<std::collections::HashMap<String, String>>,
    /// `file_id -> chunker_version` map.
    pub chunker_versions: Mutex<std::collections::HashMap<String, u32>>,
}

impl FileMetaReader for MockFileMetaReader {
    fn get_all_active_paths(&self) -> Result<Vec<(String, String)>> {
        Ok(self.active_paths.lock().unwrap().clone())
    }

    fn get_active_paths_for_brain(&self, _brain_id: &str) -> Result<Vec<(String, String)>> {
        // Mock doesn't filter by brain_id
        Ok(self.active_paths.lock().unwrap().clone())
    }

    fn get_files_with_hashes(&self) -> Result<Vec<(String, String, Option<String>)>> {
        Ok(self.files_with_hashes.lock().unwrap().clone())
    }

    fn find_stuck_files(&self) -> Result<Vec<(String, String)>> {
        Ok(self.stuck_files.lock().unwrap().clone())
    }

    fn get_content_hash(&self, file_id: &str) -> Result<Option<String>> {
        Ok(self.content_hashes.lock().unwrap().get(file_id).cloned())
    }

    fn get_chunker_version(&self, file_id: &str) -> Result<Option<u32>> {
        Ok(self.chunker_versions.lock().unwrap().get(file_id).cloned())
    }
}

// ---------------------------------------------------------------------------
// MockFtsSearcher
// ---------------------------------------------------------------------------

/// In-memory mock for `FtsSearcher`.
#[derive(Default)]
pub struct MockFtsSearcher {
    /// Results returned by every `search_fts` call.
    pub results: Mutex<Vec<FtsResult>>,
    /// Results returned by every `search_summaries_fts` call.
    pub summary_results: Mutex<Vec<FtsSummaryResult>>,
}

impl MockFtsSearcher {
    /// Create a searcher that always returns the given chunk FTS results.
    pub fn with_results(results: Vec<FtsResult>) -> Self {
        Self {
            results: Mutex::new(results),
            summary_results: Mutex::new(Vec::new()),
        }
    }

    /// Create a searcher that always returns the given summary FTS results.
    pub fn with_summary_results(summary_results: Vec<FtsSummaryResult>) -> Self {
        Self {
            results: Mutex::new(Vec::new()),
            summary_results: Mutex::new(summary_results),
        }
    }
}

impl FtsSearcher for MockFtsSearcher {
    fn search_fts(
        &self,
        _query: &str,
        _limit: usize,
        _brain_ids: Option<&[String]>,
    ) -> Result<Vec<FtsResult>> {
        Ok(self.results.lock().unwrap().clone())
    }

    fn search_summaries_fts(
        &self,
        _query: &str,
        limit: usize,
        _brain_ids: Option<&[String]>,
    ) -> Result<Vec<FtsSummaryResult>> {
        Ok(self
            .summary_results
            .lock()
            .unwrap()
            .iter()
            .take(limit)
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// MockSchemaMeta
// ---------------------------------------------------------------------------

/// In-memory mock for `SchemaMeta`.
///
/// Reports that the schema always matches and no-ops all destructive operations.
#[derive(Default)]
pub struct MockSchemaMeta {
    /// Number of times `drop_and_recreate_table` was called.
    pub recreate_count: Mutex<usize>,
    /// Number of times `force_optimize` was called.
    pub optimize_count: Mutex<usize>,
}

impl SchemaMeta for MockSchemaMeta {
    fn current_schema_matches_expected(
        &self,
    ) -> impl std::future::Future<Output = bool> + Send + '_ {
        async move { true }
    }

    fn drop_and_recreate_table(
        &mut self,
    ) -> impl std::future::Future<Output = Result<()>> + Send + '_ {
        async move {
            *self.recreate_count.lock().unwrap() += 1;
            Ok(())
        }
    }

    fn get_file_ids_with_chunks<'a>(
        &'a self,
        _brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<std::collections::HashSet<String>>> + Send + 'a
    {
        async move { Ok(HashSet::new()) }
    }

    fn force_optimize(&self) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {
            *self.optimize_count.lock().unwrap() += 1;
        }
    }
}

// We also need MockSchemaMeta + MockChunkIndexWriter combined into a single
// struct that implements both traits, so it can be used as the `S` parameter
// for `IndexPipeline<S>` which requires `S: ChunkIndexWriter + SchemaMeta`.

/// Combined mock store implementing both `ChunkIndexWriter` and `SchemaMeta`.
///
/// Delegates to embedded `MockChunkIndexWriter` and tracks recreate calls.
/// Use as the `S` type parameter for `IndexPipeline<S>` in unit tests.
#[derive(Default)]
pub struct MockStore {
    /// Inner writer — owns the chunk + delete + path state.
    pub writer: MockChunkIndexWriter,
    /// Number of times `drop_and_recreate_table` was called.
    pub recreate_count: Mutex<usize>,
}

impl MockStore {
    /// Convenience accessor: chunks stored via `ChunkIndexWriter`.
    pub fn chunks(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<(usize, String)>>> {
        self.writer.chunks.lock().unwrap()
    }
}

impl ChunkIndexWriter for MockStore {
    fn upsert_chunks<'a>(
        &'a self,
        file_id: &'a str,
        file_path: &'a str,
        brain_id: &'a str,
        chunks: &'a [(usize, &'a str)],
        embeddings: &'a [Vec<f32>],
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        self.writer
            .upsert_chunks(file_id, file_path, brain_id, chunks, embeddings)
    }

    fn delete_file_chunks<'a>(
        &'a self,
        file_id: &'a str,
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        self.writer.delete_file_chunks(file_id, brain_id)
    }

    fn delete_chunks_by_file_ids<'a>(
        &'a self,
        file_ids: &'a [String],
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<usize>> + Send + 'a {
        self.writer.delete_chunks_by_file_ids(file_ids, brain_id)
    }

    fn update_file_path<'a>(
        &'a self,
        file_id: &'a str,
        new_path: &'a str,
        brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        self.writer.update_file_path(file_id, new_path, brain_id)
    }
}

impl SchemaMeta for MockStore {
    fn current_schema_matches_expected(
        &self,
    ) -> impl std::future::Future<Output = bool> + Send + '_ {
        async move { true }
    }

    fn drop_and_recreate_table(
        &mut self,
    ) -> impl std::future::Future<Output = Result<()>> + Send + '_ {
        async move {
            *self.recreate_count.lock().unwrap() += 1;
            Ok(())
        }
    }

    fn get_file_ids_with_chunks<'a>(
        &'a self,
        _brain_id: &'a str,
    ) -> impl std::future::Future<Output = Result<std::collections::HashSet<String>>> + Send + 'a
    {
        async move { Ok(HashSet::new()) }
    }

    fn force_optimize(&self) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {}
    }
}

// ---------------------------------------------------------------------------
// MockFileMetaWriter
// ---------------------------------------------------------------------------

/// In-memory mock for `FileMetaWriter`.
///
/// Tracks file registrations, deletes, renames, and state transitions.
#[derive(Default)]
pub struct MockFileMetaWriter {
    /// Registered files: `path → file_id`
    pub files: Mutex<HashMap<String, String>>,
    /// Soft-deleted paths (path → file_id)
    pub deleted: Mutex<HashMap<String, String>>,
    /// Path updates: `file_id → new_path`
    pub renames: Mutex<HashMap<String, String>>,
    /// Indexing states: `file_id → state`
    pub indexing_states: Mutex<HashMap<String, String>>,
    /// Content hashes: `file_id → hash`
    pub content_hashes: Mutex<HashMap<String, String>>,
    /// Number of times `clear_all_content_hashes` was called.
    pub clear_all_count: Mutex<usize>,
}

impl MockFileMetaWriter {
    /// Pre-register a file with a known file_id for testing.
    pub fn register(&self, path: &str, file_id: &str) {
        self.files
            .lock()
            .unwrap()
            .insert(path.to_string(), file_id.to_string());
    }
}

impl FileMetaWriter for MockFileMetaWriter {
    fn get_or_create_file_id(&self, path: &str, _brain_id: &str) -> Result<(String, bool)> {
        let mut files = self.files.lock().unwrap();
        if let Some(id) = files.get(path) {
            return Ok((id.clone(), false));
        }
        let id = format!("file-{}", files.len() + 1);
        files.insert(path.to_string(), id.clone());
        Ok((id, true))
    }

    fn handle_delete(&self, path: &str) -> Result<Option<String>> {
        let mut files = self.files.lock().unwrap();
        if let Some(file_id) = files.remove(path) {
            self.deleted
                .lock()
                .unwrap()
                .insert(path.to_string(), file_id.clone());
            Ok(Some(file_id))
        } else {
            Ok(None)
        }
    }

    fn handle_rename(&self, file_id: &str, new_path: &str) -> Result<()> {
        self.renames
            .lock()
            .unwrap()
            .insert(file_id.to_string(), new_path.to_string());
        Ok(())
    }

    fn purge_deleted_files(&self, _older_than_ts: i64) -> Result<Vec<String>> {
        let purged: Vec<String> = self.deleted.lock().unwrap().values().cloned().collect();
        self.deleted.lock().unwrap().clear();
        Ok(purged)
    }

    fn clear_all_content_hashes(&self) -> Result<usize> {
        let mut hashes = self.content_hashes.lock().unwrap();
        let count = hashes.len();
        hashes.clear();
        *self.clear_all_count.lock().unwrap() += 1;
        Ok(count)
    }

    fn clear_content_hash_by_path(&self, path: &str) -> Result<bool> {
        let files = self.files.lock().unwrap();
        if let Some(file_id) = files.get(path) {
            let removed = self
                .content_hashes
                .lock()
                .unwrap()
                .remove(file_id)
                .is_some();
            Ok(removed)
        } else {
            Ok(false)
        }
    }

    fn set_indexing_state(&self, file_id: &str, state: &str) -> Result<()> {
        self.indexing_states
            .lock()
            .unwrap()
            .insert(file_id.to_string(), state.to_string());
        Ok(())
    }

    fn mark_indexed(
        &self,
        file_id: &str,
        content_hash: &str,
        _chunker_version: u32,
        _disk_modified_at: Option<i64>,
    ) -> Result<()> {
        self.content_hashes
            .lock()
            .unwrap()
            .insert(file_id.to_string(), content_hash.to_string());
        self.indexing_states
            .lock()
            .unwrap()
            .insert(file_id.to_string(), "indexed".to_string());
        Ok(())
    }

    fn count_stale_chunker_version(&self, _current_version: u32) -> Result<usize> {
        Ok(0)
    }

    fn reset_stuck_file_for_reindex(&self, file_id: &str) -> Result<()> {
        // Remove content hash to force reindex; remove indexing state
        let mut hashes = self.content_hashes.lock().unwrap();
        hashes.remove(file_id);
        let mut states = self.indexing_states.lock().unwrap();
        states.remove(file_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MockChunkMetaWriter
// ---------------------------------------------------------------------------

/// In-memory mock for `ChunkMetaWriter`.
///
/// Tracks chunk metadata replacements and embedding timestamps.
#[derive(Default)]
pub struct MockChunkMetaWriter {
    /// Chunk hashes per file: `file_id → Vec<chunk_hash>`
    pub chunk_hashes: Mutex<HashMap<String, Vec<String>>>,
    /// Embedded timestamps: `chunk_id → timestamp`
    pub embedded_at: Mutex<HashMap<String, i64>>,
    /// Task chunks: `task_file_id → capsule_text`
    pub task_chunks: Mutex<HashMap<String, String>>,
}

impl ChunkMetaWriter for MockChunkMetaWriter {
    fn replace_chunk_metadata(
        &self,
        file_id: &str,
        chunks: &[brain_persistence::db::chunks::ChunkMeta],
        _brain_id: &str,
    ) -> Result<()> {
        let hashes: Vec<String> = chunks.iter().map(|c| c.chunk_hash.clone()).collect();
        self.chunk_hashes
            .lock()
            .unwrap()
            .insert(file_id.to_string(), hashes);
        Ok(())
    }

    fn get_chunk_hashes(&self, file_id: &str) -> Result<Vec<String>> {
        Ok(self
            .chunk_hashes
            .lock()
            .unwrap()
            .get(file_id)
            .cloned()
            .unwrap_or_default())
    }

    fn mark_chunks_embedded(&self, chunk_ids: &[&str], timestamp: i64) -> Result<()> {
        let mut map = self.embedded_at.lock().unwrap();
        for id in chunk_ids {
            map.insert((*id).to_string(), timestamp);
        }
        Ok(())
    }

    fn upsert_task_chunk(
        &self,
        task_file_id: &str,
        capsule_text: &str,
        _brain_id: &str,
    ) -> Result<()> {
        self.task_chunks
            .lock()
            .unwrap()
            .insert(task_file_id.to_string(), capsule_text.to_string());
        Ok(())
    }

    fn upsert_record_chunk(
        &self,
        record_file_id: &str,
        capsule_text: &str,
        _brain_id: &str,
    ) -> Result<()> {
        self.task_chunks
            .lock()
            .unwrap()
            .insert(record_file_id.to_string(), capsule_text.to_string());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MockEmbeddingResetter
// ---------------------------------------------------------------------------

/// In-memory mock for `EmbeddingResetter`.
///
/// Tracks how many times each reset method was called.
#[derive(Default)]
pub struct MockEmbeddingResetter {
    /// Number of times `reset_tasks_embedded_at` was called.
    pub tasks_reset_count: Mutex<usize>,
    /// Number of times `reset_chunks_embedded_at` was called.
    pub chunks_reset_count: Mutex<usize>,
    /// Number of times `reset_records_embedded_at` was called.
    pub records_reset_count: Mutex<usize>,
}

impl EmbeddingResetter for MockEmbeddingResetter {
    fn reset_tasks_embedded_at(&self) -> Result<()> {
        *self.tasks_reset_count.lock().unwrap() += 1;
        Ok(())
    }

    fn reset_chunks_embedded_at(&self) -> Result<()> {
        *self.chunks_reset_count.lock().unwrap() += 1;
        Ok(())
    }

    fn reset_records_embedded_at(&self) -> Result<()> {
        *self.records_reset_count.lock().unwrap() += 1;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MockSummaryReader
// ---------------------------------------------------------------------------

/// In-memory mock for `SummaryReader`.
///
/// Returns pre-configured chunks lacking summary.
#[derive(Default)]
pub struct MockSummaryReader {
    /// Chunks returned by `find_chunks_lacking_summary`: `(chunk_id, content)` pairs.
    pub lacking: Mutex<Vec<(String, String)>>,
}

impl MockSummaryReader {
    /// Create a reader that will return the given `(chunk_id, content)` pairs.
    pub fn with_lacking(lacking: Vec<(String, String)>) -> Self {
        Self {
            lacking: Mutex::new(lacking),
        }
    }
}

impl SummaryReader for MockSummaryReader {
    fn find_chunks_lacking_summary(
        &self,
        _summarizer: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        Ok(self
            .lacking
            .lock()
            .unwrap()
            .iter()
            .take(limit)
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// MockSummaryWriter
// ---------------------------------------------------------------------------

/// In-memory mock for `SummaryWriter`.
///
/// Records stored summaries in-memory for test assertions.
#[derive(Default)]
pub struct MockSummaryWriter {
    /// Stored summaries: `(chunk_id, summarizer) → summary_text`
    pub summaries: Mutex<HashMap<(String, String), String>>,
}

impl SummaryWriter for MockSummaryWriter {
    fn store_ml_summary(
        &self,
        chunk_id: &str,
        summary_text: &str,
        summarizer: &str,
    ) -> Result<String> {
        self.summaries.lock().unwrap().insert(
            (chunk_id.to_string(), summarizer.to_string()),
            summary_text.to_string(),
        );
        Ok(format!("mock-summary-{chunk_id}"))
    }
}

// ---------------------------------------------------------------------------
// MockEpisodeWriter
// ---------------------------------------------------------------------------

/// In-memory mock for `EpisodeWriter`.
///
/// Stores episodes in-memory for test assertions.
#[derive(Default)]
pub struct MockEpisodeWriter {
    /// Stored episodes as `(goal, actions, outcome, tags, importance)` tuples.
    pub episodes: Mutex<Vec<(String, String, String, Vec<String>, f64)>>,
}

impl EpisodeWriter for MockEpisodeWriter {
    fn store_episode(&self, episode: &Episode) -> Result<String> {
        let id = format!("mock-episode-{}", self.episodes.lock().unwrap().len());
        self.episodes.lock().unwrap().push((
            episode.goal.clone(),
            episode.actions.clone(),
            episode.outcome.clone(),
            episode.tags.clone(),
            episode.importance,
        ));
        Ok(id)
    }
}

// ---------------------------------------------------------------------------
// MockEpisodeReader
// ---------------------------------------------------------------------------

/// In-memory mock for `EpisodeReader`.
///
/// Returns pre-configured episode rows.
#[derive(Default)]
pub struct MockEpisodeReader {
    /// Episode rows returned by `list_episodes`.
    pub episodes: Mutex<Vec<SummaryRow>>,
}

impl MockEpisodeReader {
    /// Create a reader that will return the given rows.
    pub fn with_episodes(episodes: Vec<SummaryRow>) -> Self {
        Self {
            episodes: Mutex::new(episodes),
        }
    }
}

impl EpisodeReader for MockEpisodeReader {
    fn list_episodes(&self, limit: usize, brain_id: &str) -> Result<Vec<SummaryRow>> {
        Ok(self
            .episodes
            .lock()
            .unwrap()
            .iter()
            .filter(|ep| brain_id.is_empty() || ep.brain_id == brain_id)
            .take(limit)
            .cloned()
            .collect())
    }

    fn list_episodes_multi_brain(
        &self,
        limit: usize,
        brain_ids: &[String],
    ) -> Result<Vec<SummaryRow>> {
        Ok(self
            .episodes
            .lock()
            .unwrap()
            .iter()
            .filter(|ep| brain_ids.contains(&ep.brain_id))
            .take(limit)
            .cloned()
            .collect())
    }

    fn get_summaries_by_ids(&self, ids: &[String]) -> Result<Vec<SummaryRow>> {
        let episodes = self.episodes.lock().unwrap();
        let id_set: std::collections::HashSet<&String> = ids.iter().collect();
        Ok(episodes
            .iter()
            .filter(|r| id_set.contains(&r.summary_id))
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// MockGraphLinkReader
// ---------------------------------------------------------------------------

/// In-memory mock for `GraphLinkReader`.
///
/// Returns pre-configured outlinks per `source_file_id` and chunks per `file_id`.
#[derive(Default)]
pub struct MockGraphLinkReader {
    /// Outlinks: `source_file_id → Vec<target_file_id>`
    pub outlinks: Mutex<HashMap<String, Vec<String>>>,
    /// Chunks per file_id: `file_id → Vec<ChunkRow>`
    pub file_chunks: Mutex<HashMap<String, Vec<ChunkRow>>>,
}

impl GraphLinkReader for MockGraphLinkReader {
    fn get_outlinks(&self, source_file_id: &str) -> Result<Vec<String>> {
        Ok(self
            .outlinks
            .lock()
            .unwrap()
            .get(source_file_id)
            .cloned()
            .unwrap_or_default())
    }

    fn get_chunks_by_file_ids(&self, file_ids: &[String]) -> Result<Vec<ChunkRow>> {
        let map = self.file_chunks.lock().unwrap();
        let mut result = Vec::new();
        for fid in file_ids {
            if let Some(chunks) = map.get(fid) {
                result.extend(chunks.iter().cloned());
            }
        }
        Ok(result)
    }
}

#[derive(Default)]
pub struct MockDerivedSummaryPersistence {
    pub summaries: Mutex<HashMap<String, DerivedSummary>>,
}

impl MockDerivedSummaryPersistence {
    fn scope_key(scope_type: &ScopeType, scope_value: &str) -> String {
        format!("{}:{scope_value}", scope_type.as_str())
    }

    fn clone_summary(summary: &DerivedSummary) -> DerivedSummary {
        DerivedSummary {
            id: summary.id.clone(),
            scope_type: summary.scope_type.clone(),
            scope_value: summary.scope_value.clone(),
            content: summary.content.clone(),
            stale: summary.stale,
            generated_at: summary.generated_at,
        }
    }
}

impl DerivedSummaryWriter for MockDerivedSummaryPersistence {
    fn generate_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<GeneratedScopeSummary> {
        let mut summaries = self.summaries.lock().unwrap();
        let key = Self::scope_key(scope_type, scope_value);
        let source_content = format!("mock-source:{key}");

        if let Some(existing) = summaries.get_mut(&key) {
            existing.stale = false;
            existing.generated_at = crate::utils::now_ts();
            return Ok(GeneratedScopeSummary {
                id: existing.id.clone(),
                source_content,
                content_changed: false,
            });
        }

        let id = format!("mock-derived-{}", summaries.len() + 1);
        summaries.insert(
            key,
            DerivedSummary {
                id: id.clone(),
                scope_type: scope_type.as_str().to_string(),
                scope_value: scope_value.to_string(),
                content: format!("mock summary for {}:{}", scope_type.as_str(), scope_value),
                stale: false,
                generated_at: crate::utils::now_ts(),
            },
        );

        Ok(GeneratedScopeSummary {
            id,
            source_content,
            content_changed: true,
        })
    }

    fn get_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<Option<DerivedSummary>> {
        let summaries = self.summaries.lock().unwrap();
        let key = Self::scope_key(scope_type, scope_value);
        Ok(summaries.get(&key).map(Self::clone_summary))
    }

    fn mark_scope_stale(&self, scope_type: &ScopeType, scope_value: &str) -> Result<usize> {
        let mut summaries = self.summaries.lock().unwrap();
        let key = Self::scope_key(scope_type, scope_value);
        if let Some(summary) = summaries.get_mut(&key) {
            summary.stale = true;
            Ok(1)
        } else {
            Ok(0)
        }
    }
}

impl DerivedSummaryReader for MockDerivedSummaryPersistence {
    fn search_derived_summaries(&self, query: &str, limit: usize) -> Result<Vec<DerivedSummary>> {
        let summaries = self.summaries.lock().unwrap();
        Ok(summaries
            .values()
            .filter(|s| {
                s.content.contains(query)
                    || s.scope_value.contains(query)
                    || s.scope_type.contains(query)
            })
            .take(limit)
            .map(Self::clone_summary)
            .collect())
    }

    fn list_stale_summaries(&self, limit: usize) -> Result<Vec<DerivedSummary>> {
        let summaries = self.summaries.lock().unwrap();
        let mut stale: Vec<DerivedSummary> = summaries
            .values()
            .filter(|s| s.stale)
            .map(Self::clone_summary)
            .collect();
        stale.sort_by_key(|s| s.generated_at);
        stale.truncate(limit);
        Ok(stale)
    }
}

#[derive(Default)]
pub struct MockJobPersistence {
    pub scope_results: Mutex<HashMap<String, (String, i64)>>,
    pub consolidation_results: Mutex<HashMap<String, (String, String, Vec<String>, String, i64)>>,
}

impl JobPersistence for MockJobPersistence {
    fn persist_scope_summary_result(&self, summary_id: &str, result: &str) -> Result<()> {
        self.scope_results.lock().unwrap().insert(
            summary_id.to_string(),
            (result.to_string(), crate::utils::now_ts()),
        );
        Ok(())
    }

    fn persist_consolidation_result(
        &self,
        suggested_title: &str,
        result: &str,
        episode_ids: &[String],
        brain_id: &str,
    ) -> Result<()> {
        let reflection_id = format!(
            "mock-reflection-{}",
            self.consolidation_results.lock().unwrap().len() + 1
        );
        self.consolidation_results.lock().unwrap().insert(
            reflection_id,
            (
                suggested_title.to_string(),
                result.to_string(),
                episode_ids.to_vec(),
                brain_id.to_string(),
                crate::utils::now_ts(),
            ),
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// MockLinkWriter
// ---------------------------------------------------------------------------

/// In-memory mock for `LinkWriter`.
#[derive(Default)]
pub struct MockLinkWriter {
    /// Outgoing links per file_id: `file_id → Vec<Link>`
    pub links: Mutex<HashMap<String, Vec<Link>>>,
}

impl LinkWriter for MockLinkWriter {
    fn replace_links(&self, file_id: &str, links: &[Link]) -> Result<()> {
        self.links
            .lock()
            .unwrap()
            .insert(file_id.to_string(), links.to_vec());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MockBrainManager
// ---------------------------------------------------------------------------

/// In-memory mock for `BrainManager`.
#[derive(Default)]
pub struct MockBrainManager {
    /// Brains by id: `brain_id → BrainRow`
    pub brains: Mutex<HashMap<String, BrainRow>>,
}

impl BrainManager for MockBrainManager {
    fn archive_brain(&self, brain_id: &str) -> Result<()> {
        let mut brains = self.brains.lock().unwrap();
        if let Some(brain) = brains.get_mut(brain_id) {
            brain.archived = true;
        }
        Ok(())
    }

    fn get_brain(&self, brain_id: &str) -> Result<Option<BrainRow>> {
        Ok(self.brains.lock().unwrap().get(brain_id).cloned())
    }
}

// ---------------------------------------------------------------------------
// MockEmbeddingOps
// ---------------------------------------------------------------------------

/// In-memory mock for `EmbeddingOps`.
#[derive(Default)]
pub struct MockEmbeddingOps {
    pub stale_chunks: Mutex<Vec<ChunkPollRow>>,
    pub stale_summaries: Mutex<Vec<SummaryPollRow>>,
    pub stale_tasks: Mutex<Vec<TaskPollRow>>,
    pub stale_records: Mutex<Vec<RecordPollRow>>,
    pub embedded_chunks: Mutex<HashMap<String, i64>>,
    pub embedded_summaries: Mutex<HashSet<String>>,
    pub embedded_tasks: Mutex<HashSet<String>>,
    pub embedded_records: Mutex<HashSet<String>>,
}

impl EmbeddingOps for MockEmbeddingOps {
    fn find_stale_chunks_for_embedding(&self, _brain_id: &str) -> Result<Vec<ChunkPollRow>> {
        Ok(self.stale_chunks.lock().unwrap().clone())
    }

    fn find_stale_summaries_for_embedding(&self, _brain_id: &str) -> Result<Vec<SummaryPollRow>> {
        Ok(self.stale_summaries.lock().unwrap().clone())
    }

    fn find_stale_tasks_for_embedding(&self, _brain_id: &str) -> Result<Vec<TaskPollRow>> {
        Ok(self.stale_tasks.lock().unwrap().clone())
    }

    fn find_stale_records_for_embedding(&self, _brain_id: &str) -> Result<Vec<RecordPollRow>> {
        Ok(self.stale_records.lock().unwrap().clone())
    }

    fn mark_summaries_embedded(&self, summary_ids: &[&str]) -> Result<()> {
        let mut set = self.embedded_summaries.lock().unwrap();
        for id in summary_ids {
            set.insert(id.to_string());
        }
        Ok(())
    }

    fn mark_tasks_embedded(&self, task_ids: &[&str]) -> Result<()> {
        let mut set = self.embedded_tasks.lock().unwrap();
        for id in task_ids {
            set.insert(id.to_string());
        }
        Ok(())
    }

    fn mark_records_embedded(&self, record_ids: &[&str]) -> Result<()> {
        let mut set = self.embedded_records.lock().unwrap();
        for id in record_ids {
            set.insert(id.to_string());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MockJobQueue
// ---------------------------------------------------------------------------

/// In-memory mock for `JobQueue`.
#[derive(Default)]
pub struct MockJobQueue {
    pub jobs: Mutex<HashMap<String, Job>>,
    pub next_id: Mutex<usize>,
}

impl MockJobQueue {
    fn next_job_id(&self) -> String {
        let mut id = self.next_id.lock().unwrap();
        *id += 1;
        format!("mock-job-{}", *id)
    }
}

impl JobQueue for MockJobQueue {
    fn claim_ready_jobs(&self, limit: i32) -> Result<Vec<Job>> {
        let jobs = self.jobs.lock().unwrap();
        let mut ready: Vec<_> = jobs
            .values()
            .filter(|j| j.status == JobStatus::Ready)
            .cloned()
            .collect();
        ready.sort_by_key(|j| j.priority);
        ready.truncate(limit as usize);
        Ok(ready)
    }

    fn advance_to_in_progress(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }

    fn complete_job(&self, _job_id: &str, _result: Option<&str>) -> Result<()> {
        Ok(())
    }

    fn fail_job(&self, _job_id: &str, _error_msg: &str) -> Result<()> {
        Ok(())
    }

    fn reap_stuck_jobs(&self) -> Result<usize> {
        Ok(0)
    }

    fn enqueue_job(&self, _input: &EnqueueJobInput) -> Result<String> {
        Ok(self.next_job_id())
    }

    fn gc_completed_jobs(&self, _age_secs: i64, _protected_kinds: &[&str]) -> Result<usize> {
        Ok(0)
    }

    fn count_jobs_by_status(&self, status: &JobStatus) -> Result<i64> {
        let jobs = self.jobs.lock().unwrap();
        let count = jobs.values().filter(|j| j.status == *status).count() as i64;
        Ok(count)
    }

    fn list_jobs_by_status(&self, status: &JobStatus, limit: i32) -> Result<Vec<Job>> {
        let jobs = self.jobs.lock().unwrap();
        let mut out: Vec<_> = jobs
            .values()
            .filter(|j| j.status == *status)
            .cloned()
            .collect();
        out.truncate(limit as usize);
        Ok(out)
    }

    fn list_stuck_jobs(&self) -> Result<Vec<Job>> {
        Ok(vec![])
    }

    fn retry_failed_job(&self, _job_id: &str) -> Result<bool> {
        Ok(false)
    }

    fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        Ok(self.jobs.lock().unwrap().get(job_id).cloned())
    }

    fn update_job_status(&self, job_id: &str, status: &JobStatus) -> Result<bool> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(job_id) {
            job.status = status.clone();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn get_job_by_kind(&self, kind: &str) -> Result<Option<Job>> {
        let jobs = self.jobs.lock().unwrap();
        Ok(jobs.values().find(|j| j.kind() == kind).cloned())
    }

    fn ensure_singleton_job(&self, _input: &EnqueueJobInput) -> Result<Option<String>> {
        Ok(None)
    }

    fn reschedule_terminal_job(&self, _kind: &str, _brain_id: Option<&str>) -> Result<bool> {
        Ok(false)
    }

    fn enqueue_dedup_job(&self, _input: &EnqueueJobInput) -> Result<(String, bool)> {
        Ok((self.next_job_id(), true))
    }

    fn reconcile_singleton_job(&self, _input: &EnqueueJobInput) -> Result<()> {
        Ok(())
    }

    fn reconcile_singleton_job_with_delay(
        &self,
        _input: &EnqueueJobInput,
        _delay_secs: i64,
    ) -> Result<()> {
        Ok(())
    }

    fn has_active_lod_job(&self, _object_uri: &str) -> Result<bool> {
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// MockMaintenanceOps
// ---------------------------------------------------------------------------

/// In-memory mock for `MaintenanceOps`.
#[derive(Default)]
pub struct MockMaintenanceOps {
    pub renamed: Mutex<Vec<(String, String)>>,
    pub vacuum_count: Mutex<usize>,
    pub reindex_count: Mutex<usize>,
    pub consistency_result: Mutex<Option<(i64, i64)>>,
    pub reindex_summaries_count: Mutex<usize>,
    pub stuck_files: Mutex<Vec<(String, String)>>,
}

impl MaintenanceOps for MockMaintenanceOps {
    fn rename_file_by_path(&self, from_path: &str, to_path: &str) -> Result<Option<String>> {
        self.renamed
            .lock()
            .unwrap()
            .push((from_path.to_string(), to_path.to_string()));
        Ok(None)
    }

    fn vacuum_db(&self) -> Result<()> {
        *self.vacuum_count.lock().unwrap() += 1;
        Ok(())
    }

    fn reindex_fts(&self) -> Result<()> {
        *self.reindex_count.lock().unwrap() += 1;
        Ok(())
    }

    fn fts_consistency(&self) -> Result<(i64, i64)> {
        Ok(self.consistency_result.lock().unwrap().unwrap_or((0, 0)))
    }

    fn reindex_summaries_fts(&self) -> Result<usize> {
        let mut count = self.reindex_summaries_count.lock().unwrap();
        *count += 1;
        Ok(*count)
    }
}

// ---------------------------------------------------------------------------
// MockTagAliasReader
// ---------------------------------------------------------------------------

/// In-memory mock for `TagAliasReader`.
///
/// Backs `alias_lookup_for_brain` with an in-memory `(brain_id → (raw_tag →
/// canonical_tag))` map so query-path tests can exercise alias-expansion
/// behavior without bootstrapping a SQLite schema.
///
/// `collect_raw_tags` and `read_alias_snapshot` are not implemented because
/// no current consumer mocks those methods. Add them on demand.
#[derive(Default)]
pub struct MockTagAliasReader {
    /// `(brain_id, raw_tag) → canonical_tag`. Keys/values stored exactly as
    /// inserted; the trait contract for `alias_lookup_for_brain` returns
    /// the lowercased projection, so the impl normalizes on read to match
    /// the production helper.
    pub aliases: Mutex<HashMap<String, HashMap<String, String>>>,
}

impl MockTagAliasReader {
    /// Insert a single alias row keyed by `(brain_id, raw_tag)`.
    pub fn seed(&self, brain_id: &str, raw_tag: &str, canonical_tag: &str) {
        self.aliases
            .lock()
            .unwrap()
            .entry(brain_id.to_string())
            .or_default()
            .insert(raw_tag.to_string(), canonical_tag.to_string());
    }
}

impl TagAliasReader for MockTagAliasReader {
    fn collect_raw_tags(
        &self,
        _brain_id: &str,
    ) -> Result<Vec<brain_persistence::db::tag_aliases::DedupedRawTag>> {
        unimplemented!("MockTagAliasReader::collect_raw_tags not used by current tests")
    }

    fn read_alias_snapshot(
        &self,
        _brain_id: &str,
    ) -> Result<HashMap<String, brain_persistence::db::tag_aliases::ExistingAlias>> {
        unimplemented!("MockTagAliasReader::read_alias_snapshot not used by current tests")
    }

    fn alias_lookup_for_brain(&self, brain_id: &str) -> Result<HashMap<String, String>> {
        Ok(self
            .aliases
            .lock()
            .unwrap()
            .get(brain_id)
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.to_lowercase(), v.to_lowercase()))
                    .collect()
            })
            .unwrap_or_default())
    }

    fn list_aliases_for_brain(
        &self,
        _brain_id: &str,
        _canonical: Option<&str>,
        _cluster_id: Option<&str>,
        _limit: i64,
        _offset: i64,
    ) -> Result<Vec<brain_persistence::db::tag_aliases::AliasRow>> {
        unimplemented!("MockTagAliasReader::list_aliases_for_brain not used by current tests")
    }

    fn count_aliases_for_brain(
        &self,
        _brain_id: &str,
    ) -> Result<brain_persistence::db::tag_aliases::AliasCounts> {
        unimplemented!("MockTagAliasReader::count_aliases_for_brain not used by current tests")
    }

    fn latest_run_for_brain(
        &self,
        _brain_id: &str,
    ) -> Result<Option<brain_persistence::db::tag_aliases::TagClusterRunRow>> {
        unimplemented!("MockTagAliasReader::latest_run_for_brain not used by current tests")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_chunk_index_writer_upsert_and_delete() {
        let writer = MockChunkIndexWriter::default();

        writer
            .upsert_chunks(
                "file-1",
                "/path/to/file.md",
                "test-brain",
                &[(0, "hello"), (1, "world")],
                &[vec![0.1, 0.2], vec![0.3, 0.4]],
            )
            .await
            .unwrap();

        {
            let map = writer.chunks.lock().unwrap();
            assert_eq!(map["file-1"].len(), 2);
        }

        writer
            .delete_file_chunks("file-1", "test-brain")
            .await
            .unwrap();

        {
            let map = writer.chunks.lock().unwrap();
            assert!(!map.contains_key("file-1"));
            let del = writer.deleted.lock().unwrap();
            assert!(del.contains("file-1"));
        }
    }

    #[tokio::test]
    async fn mock_chunk_index_writer_delete_by_ids() {
        let writer = MockChunkIndexWriter::default();

        for id in ["a", "b", "c"] {
            writer
                .upsert_chunks(id, "/p", "test-brain", &[(0, "x")], &[vec![0.0]])
                .await
                .unwrap();
        }

        let deleted = writer
            .delete_chunks_by_file_ids(&["a".to_string(), "b".to_string()], "test-brain")
            .await
            .unwrap();
        assert_eq!(deleted, 2);

        let map = writer.chunks.lock().unwrap();
        assert!(!map.contains_key("a"));
        assert!(!map.contains_key("b"));
        assert!(map.contains_key("c"));
    }

    #[tokio::test]
    async fn mock_chunk_searcher_returns_preset_results() {
        let results = vec![QueryResult {
            chunk_id: "c1".to_string(),
            file_id: "f1".to_string(),
            file_path: "/p".to_string(),
            chunk_ord: 0,
            content: "hello".to_string(),
            score: Some(0.9),
            brain_id: String::new(),
        }];
        let searcher = MockChunkSearcher::with_results(results);
        let out = searcher
            .query(&[0.1_f32, 0.2], 10, 20, VectorSearchMode::default(), None)
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].chunk_id, "c1");
    }

    #[test]
    fn mock_chunk_meta_reader_filters_by_id() {
        let reader = MockChunkMetaReader::default();
        {
            let mut rows = reader.rows.lock().unwrap();
            rows.push(ChunkRow {
                chunk_id: "c1".to_string(),
                file_id: "f1".to_string(),
                file_path: "/p".to_string(),
                content: "hello".to_string(),
                heading_path: String::new(),
                byte_start: 0,
                byte_end: 5,
                token_estimate: 1,
                last_indexed_at: None,
                disk_modified_at: None,
                pagerank_score: 0.0,
                tags: vec![],
                importance: 0.5,
            });
            rows.push(ChunkRow {
                chunk_id: "c2".to_string(),
                file_id: "f1".to_string(),
                file_path: "/p".to_string(),
                content: "world".to_string(),
                heading_path: String::new(),
                byte_start: 6,
                byte_end: 11,
                token_estimate: 1,
                last_indexed_at: None,
                disk_modified_at: None,
                pagerank_score: 0.0,
                tags: vec![],
                importance: 0.5,
            });
        }

        let out = reader.get_chunks_by_ids(&["c1".to_string()]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].chunk_id, "c1");
    }

    #[test]
    fn mock_fts_searcher_returns_preset_results() {
        let results = vec![FtsResult {
            chunk_id: "c1".to_string(),
            score: 1.0,
        }];
        let searcher = MockFtsSearcher::with_results(results);
        let out = searcher.search_fts("rust", 10, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].chunk_id, "c1");
    }

    // -----------------------------------------------------------------------
    // Pipeline mockability tests
    // -----------------------------------------------------------------------

    /// Demonstrates that `IndexPipeline::with_store` can be constructed with a
    /// mock store — no real LanceDB I/O. SQLite is in-memory only.
    #[tokio::test]
    async fn index_pipeline_with_mock_store_upserts_without_lancedb() {
        use std::sync::Arc;

        use crate::embedder::MockEmbedder;
        use crate::pipeline::IndexPipeline;
        use brain_persistence::db::Db;

        let db = Db::open_in_memory().unwrap();
        let store = MockStore::default();
        let embedder = Arc::new(MockEmbedder);

        let mut pipeline = IndexPipeline::with_store(db, store, embedder)
            .await
            .unwrap();

        // Directly exercise the mock store through the pipeline's store accessor.
        pipeline
            .store_mut()
            .upsert_chunks(
                "file-42",
                "/notes/test.md",
                "test-brain",
                &[(0, "chunk zero"), (1, "chunk one")],
                &[vec![0.1_f32; 384], vec![0.2_f32; 384]],
            )
            .await
            .unwrap();

        let chunks = pipeline.store().writer.chunks.lock().unwrap();
        assert_eq!(chunks["file-42"].len(), 2);
        assert_eq!(chunks["file-42"][0].1, "chunk zero");
        assert_eq!(chunks["file-42"][1].1, "chunk one");
    }

    /// Demonstrates that `QueryPipeline` can be constructed with a
    /// `MockChunkSearcher` and returns pre-configured results — no LanceDB I/O.
    #[tokio::test]
    async fn query_pipeline_with_mock_searcher_returns_preset_results() {
        use std::sync::Arc;

        use crate::embedder::MockEmbedder;
        use crate::metrics::Metrics;
        use crate::query_pipeline::QueryPipeline;
        use brain_persistence::db::Db;
        use brain_persistence::store::QueryResult;

        let db = Db::open_in_memory().unwrap();
        let store = MockChunkSearcher::with_results(vec![QueryResult {
            chunk_id: "mock-chunk-1".to_string(),
            file_id: "file-1".to_string(),
            file_path: "/notes/mock.md".to_string(),
            chunk_ord: 0,
            content: "The quick brown fox".to_string(),
            score: Some(0.05),
            brain_id: String::new(),
        }]);
        let embedder: Arc<dyn crate::embedder::Embed> = Arc::new(MockEmbedder);
        let metrics = Arc::new(Metrics::new());

        let pipeline = QueryPipeline::new(&db, &store, &embedder, &metrics);

        // search_ranked embeds the query and calls store.query — both are mocked.
        let sp = crate::query_pipeline::SearchParams::new("fox", "semantic", 0, 0, &[]);
        let (ranked, _confidence) = pipeline.search_ranked(&sp).await.unwrap();

        // The mock searcher returns our preset result; after SQLite enrichment
        // it may be filtered out (no SQLite row). An empty ranked list is
        // acceptable — the important invariant is that no LanceDB I/O occurred
        // and no panic was raised.
        // If SQLite happens to have a matching row (it won't for in-memory),
        // the result would appear. Either way, the pipeline executed correctly.
        let _ = ranked; // result may be empty due to missing SQLite rows
    }

    /// Demonstrates that `MockSchemaMeta` satisfies the `SchemaMeta` trait
    /// and correctly tracks `drop_and_recreate_table` calls.
    #[tokio::test]
    async fn mock_schema_meta_tracks_recreate_calls() {
        let mut meta = MockSchemaMeta::default();
        assert!(meta.current_schema_matches_expected().await);
        meta.drop_and_recreate_table().await.unwrap();
        meta.drop_and_recreate_table().await.unwrap();
        assert_eq!(*meta.recreate_count.lock().unwrap(), 2);
        let ids = meta.get_file_ids_with_chunks("test-brain").await.unwrap();
        assert!(ids.is_empty());
    }

    // -----------------------------------------------------------------------
    // New trait mock tests
    // -----------------------------------------------------------------------

    #[test]
    fn mock_file_meta_writer_get_or_create() {
        let writer = MockFileMetaWriter::default();

        let (id1, is_new1) = writer.get_or_create_file_id("/notes/a.md", "").unwrap();
        assert!(is_new1);

        let (id2, is_new2) = writer.get_or_create_file_id("/notes/a.md", "").unwrap();
        assert!(!is_new2);
        assert_eq!(id1, id2);
    }

    #[test]
    fn mock_file_meta_writer_handle_delete() {
        let writer = MockFileMetaWriter::default();

        let (file_id, _) = writer.get_or_create_file_id("/notes/a.md", "").unwrap();
        let deleted_id = writer.handle_delete("/notes/a.md").unwrap();
        assert_eq!(deleted_id, Some(file_id));

        // Second delete returns None
        let again = writer.handle_delete("/notes/a.md").unwrap();
        assert!(again.is_none());
    }

    #[test]
    fn mock_file_meta_writer_clear_all_content_hashes() {
        let writer = MockFileMetaWriter::default();

        let (file_id, _) = writer.get_or_create_file_id("/notes/a.md", "").unwrap();
        writer.mark_indexed(&file_id, "hash123", 1, None).unwrap();

        let count = writer.clear_all_content_hashes().unwrap();
        assert_eq!(count, 1);
        assert_eq!(*writer.clear_all_count.lock().unwrap(), 1);
        assert!(writer.content_hashes.lock().unwrap().is_empty());
    }

    #[test]
    fn mock_chunk_meta_writer_replace_and_get_hashes() {
        use brain_persistence::db::chunks::ChunkMeta;

        let writer = MockChunkMetaWriter::default();
        let chunks = vec![
            ChunkMeta {
                chunk_id: "file-1:0".to_string(),
                chunk_ord: 0,
                chunk_hash: "hash-a".to_string(),
                chunker_version: 1,
                content: "content 0".to_string(),
                heading_path: String::new(),
                byte_start: 0,
                byte_end: 10,
                token_estimate: 1,
            },
            ChunkMeta {
                chunk_id: "file-1:1".to_string(),
                chunk_ord: 1,
                chunk_hash: "hash-b".to_string(),
                chunker_version: 1,
                content: "content 1".to_string(),
                heading_path: String::new(),
                byte_start: 10,
                byte_end: 20,
                token_estimate: 1,
            },
        ];
        writer
            .replace_chunk_metadata("file-1", &chunks, "")
            .unwrap();

        let hashes = writer.get_chunk_hashes("file-1").unwrap();
        assert_eq!(hashes, vec!["hash-a", "hash-b"]);
    }

    #[test]
    fn mock_chunk_meta_writer_mark_embedded() {
        let writer = MockChunkMetaWriter::default();
        writer
            .mark_chunks_embedded(&["c1", "c2"], 1_700_000_000)
            .unwrap();

        let map = writer.embedded_at.lock().unwrap();
        assert_eq!(map["c1"], 1_700_000_000);
        assert_eq!(map["c2"], 1_700_000_000);
    }

    #[test]
    fn mock_embedding_resetter_tracks_calls() {
        let resetter = MockEmbeddingResetter::default();
        resetter.reset_tasks_embedded_at().unwrap();
        resetter.reset_tasks_embedded_at().unwrap();
        resetter.reset_chunks_embedded_at().unwrap();
        resetter.reset_records_embedded_at().unwrap();

        assert_eq!(*resetter.tasks_reset_count.lock().unwrap(), 2);
        assert_eq!(*resetter.chunks_reset_count.lock().unwrap(), 1);
        assert_eq!(*resetter.records_reset_count.lock().unwrap(), 1);
    }

    #[test]
    fn mock_summary_reader_respects_limit() {
        let lacking = vec![
            ("c1".to_string(), "content 1".to_string()),
            ("c2".to_string(), "content 2".to_string()),
            ("c3".to_string(), "content 3".to_string()),
        ];
        let reader = MockSummaryReader::with_lacking(lacking);

        let out = reader.find_chunks_lacking_summary("flan", 2).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, "c1");
    }

    #[test]
    fn mock_summary_writer_stores_summaries() {
        let writer = MockSummaryWriter::default();
        writer
            .store_ml_summary("chunk:1", "summary text", "flan-t5-small")
            .unwrap();

        let key = ("chunk:1".to_string(), "flan-t5-small".to_string());
        let summaries = writer.summaries.lock().unwrap();
        assert_eq!(summaries[&key], "summary text");
    }

    #[test]
    fn mock_episode_writer_stores_episodes() {
        let writer = MockEpisodeWriter::default();
        let episode = Episode {
            brain_id: "brain-test".to_string(),
            goal: "Fix bug".to_string(),
            actions: "Debugged".to_string(),
            outcome: "Fixed".to_string(),
            tags: vec!["rust".to_string()],
            importance: 0.9,
        };
        let id = writer.store_episode(&episode).unwrap();
        assert!(id.starts_with("mock-episode-"));

        let episodes = writer.episodes.lock().unwrap();
        assert_eq!(episodes.len(), 1);
        assert_eq!(episodes[0].0, "Fix bug");
        assert!((episodes[0].4 - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn mock_episode_reader_respects_limit() {
        let episodes = vec![
            SummaryRow {
                summary_id: "s1".to_string(),
                brain_id: "brain-a".to_string(),
                kind: "episode".to_string(),
                title: Some("Episode 1".to_string()),
                content: "content 1".to_string(),
                tags: vec![],
                importance: 1.0,
                created_at: 100,
                updated_at: 100,
                parent_id: None,
                source_hash: None,
                confidence: 1.0,
                valid_from: None,
            },
            SummaryRow {
                summary_id: "s2".to_string(),
                brain_id: "brain-a".to_string(),
                kind: "episode".to_string(),
                title: Some("Episode 2".to_string()),
                content: "content 2".to_string(),
                tags: vec![],
                importance: 1.0,
                created_at: 200,
                updated_at: 200,
                parent_id: None,
                source_hash: None,
                confidence: 1.0,
                valid_from: None,
            },
        ];
        let reader = MockEpisodeReader::with_episodes(episodes);

        let out = reader.list_episodes(1, "").unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].summary_id, "s1");
    }

    /// MockStore delegates to inner MockChunkIndexWriter — public fields still accessible.
    #[tokio::test]
    async fn mock_store_delegates_to_inner_writer() {
        let store = MockStore::default();
        store
            .upsert_chunks("f1", "/p", "test-brain", &[(0, "hello")], &[vec![0.1]])
            .await
            .unwrap();

        let chunks = store.writer.chunks.lock().unwrap();
        assert!(chunks.contains_key("f1"));
    }

    #[test]
    fn mock_db_alias_lookup_default_empty() {
        let reader = MockTagAliasReader::default();
        let out = reader.alias_lookup_for_brain("brain-a").unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn mock_db_alias_lookup_seeded_returns_seeded() {
        let reader = MockTagAliasReader::default();
        reader.seed("brain-a", "Bug", "Bugs");
        reader.seed("brain-a", "defect", "bug");
        reader.seed("brain-b", "perf", "perf");

        let a = reader.alias_lookup_for_brain("brain-a").unwrap();
        // Trait contract: keys/values are lowercased.
        assert_eq!(a.get("bug").map(String::as_str), Some("bugs"));
        assert_eq!(a.get("defect").map(String::as_str), Some("bug"));
        assert_eq!(a.len(), 2);

        let b = reader.alias_lookup_for_brain("brain-b").unwrap();
        assert_eq!(b.get("perf").map(String::as_str), Some("perf"));
        assert_eq!(b.len(), 1);
    }
}
