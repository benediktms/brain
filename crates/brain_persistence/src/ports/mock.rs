//! In-memory mocks for the persistence-port trait contracts.
//!
//! These mocks satisfy traits whose method signatures reference
//! `brain_persistence` types (`ChunkRow`, `SummaryRow`, `Job`, …) using
//! `HashMap`-backed in-memory state. They do not open SQLite or LanceDB.
//!
//! Brain_lib's `ports::mock` module re-exports everything here so existing
//! test imports keep resolving without changes.

#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use brain_core::error::Result;

use super::{
    BrainManager, ChunkMetaReader, ChunkMetaWriter, ChunkSearcher, EmbeddingOps, EmbeddingResetter,
    EpisodeReader, EpisodeWriter, FileMetaWriter, FtsSearcher, GraphLinkReader, JobQueue,
    LinkWriter, MaintenanceOps, SummaryReader, SummaryWriter,
};
use crate::db::chunks::{ChunkPollRow, ChunkRow};
use crate::db::fts::{FtsResult, FtsSummaryResult};
use crate::db::job::{Job, JobStatus};
use crate::db::jobs::EnqueueJobInput;
use crate::db::records::queries::RecordPollRow;
use crate::db::schema::BrainRow;
use crate::db::summaries::{Episode, SummaryPollRow, SummaryRow};
use crate::db::tasks::queries::TaskPollRow;
use crate::links::Link;
use crate::store::{QueryResult, VectorSearchMode};

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
    ) -> Result<HashMap<String, crate::db::summaries::SummaryMeta>> {
        let map = self.summary_kinds.lock().unwrap();
        Ok(summary_ids
            .iter()
            .filter_map(|id| {
                map.get(id).map(|k| {
                    (
                        id.clone(),
                        crate::db::summaries::SummaryMeta {
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
        chunks: &[crate::db::chunks::ChunkMeta],
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

    fn record_embed_reset(&self) -> Result<()> {
        Ok(())
    }

    fn last_embed_reset_before(&self) -> Result<Option<i64>> {
        Ok(None)
    }

    fn get_consecutive_resets(&self) -> Result<u32> {
        Ok(0)
    }

    fn set_consecutive_resets(&self, _count: u32) -> Result<()> {
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
            job.status = *status;
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
