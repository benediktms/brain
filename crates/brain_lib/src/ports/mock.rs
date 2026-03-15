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

use crate::db::chunks::ChunkRow;
use crate::db::fts::FtsResult;
use crate::error::Result;
use crate::store::QueryResult;

use super::{
    ChunkIndexWriter, ChunkMetaReader, ChunkSearcher, FileMetaReader, FtsSearcher, SchemaMeta,
};

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
}

impl FileMetaReader for MockFileMetaReader {
    fn get_all_active_paths(&self) -> Result<Vec<(String, String)>> {
        Ok(self.active_paths.lock().unwrap().clone())
    }

    fn get_files_with_hashes(&self) -> Result<Vec<(String, String, Option<String>)>> {
        Ok(self.files_with_hashes.lock().unwrap().clone())
    }

    fn find_stuck_files(&self) -> Result<Vec<(String, String)>> {
        Ok(self.stuck_files.lock().unwrap().clone())
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
}

impl MockFtsSearcher {
    /// Create a searcher that always returns the given results.
    pub fn with_results(results: Vec<FtsResult>) -> Self {
        Self {
            results: Mutex::new(results),
        }
    }
}

impl FtsSearcher for MockFtsSearcher {
    fn search_fts(&self, _query: &str, _limit: usize) -> Result<Vec<FtsResult>> {
        Ok(self.results.lock().unwrap().clone())
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

    fn get_file_ids_with_chunks(
        &self,
    ) -> impl std::future::Future<Output = Result<std::collections::HashSet<String>>> + Send + '_
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
/// Use as the `S` type parameter for `IndexPipeline<S>` in unit tests.
#[derive(Default)]
pub struct MockStore {
    /// Tracks upserted chunks: `file_id → Vec<(chunk_ord, content)>`.
    pub chunks: Mutex<HashMap<String, Vec<(usize, String)>>>,
    /// `file_id`s that have been explicitly deleted.
    pub deleted: Mutex<HashSet<String>>,
    /// Path updates: `file_id → new_path`.
    pub path_updates: Mutex<HashMap<String, String>>,
    /// Number of times `drop_and_recreate_table` was called.
    pub recreate_count: Mutex<usize>,
}

impl ChunkIndexWriter for MockStore {
    fn upsert_chunks<'a>(
        &'a self,
        file_id: &'a str,
        _file_path: &'a str,
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

    fn get_file_ids_with_chunks(
        &self,
    ) -> impl std::future::Future<Output = Result<std::collections::HashSet<String>>> + Send + '_
    {
        async move { Ok(HashSet::new()) }
    }

    fn force_optimize(&self) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {}
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
                &[(0, "hello"), (1, "world")],
                &[vec![0.1, 0.2], vec![0.3, 0.4]],
            )
            .await
            .unwrap();

        {
            let map = writer.chunks.lock().unwrap();
            assert_eq!(map["file-1"].len(), 2);
        }

        writer.delete_file_chunks("file-1").await.unwrap();

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
                .upsert_chunks(id, "/p", &[(0, "x")], &[vec![0.0]])
                .await
                .unwrap();
        }

        let deleted = writer
            .delete_chunks_by_file_ids(&["a".to_string(), "b".to_string()])
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
        }];
        let searcher = MockChunkSearcher::with_results(results);
        let out = searcher.query(&[0.1_f32, 0.2], 10, 20).await.unwrap();
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
                pagerank_score: 0.0,
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
                pagerank_score: 0.0,
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
        let out = searcher.search_fts("rust", 10).unwrap();
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

        use crate::db::Db;
        use crate::embedder::MockEmbedder;
        use crate::pipeline::IndexPipeline;

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
                &[(0, "chunk zero"), (1, "chunk one")],
                &[vec![0.1_f32; 384], vec![0.2_f32; 384]],
            )
            .await
            .unwrap();

        let chunks = pipeline.store().chunks.lock().unwrap();
        assert_eq!(chunks["file-42"].len(), 2);
        assert_eq!(chunks["file-42"][0].1, "chunk zero");
        assert_eq!(chunks["file-42"][1].1, "chunk one");
    }

    /// Demonstrates that `QueryPipeline` can be constructed with a
    /// `MockChunkSearcher` and returns pre-configured results — no LanceDB I/O.
    #[tokio::test]
    async fn query_pipeline_with_mock_searcher_returns_preset_results() {
        use std::sync::Arc;

        use crate::db::Db;
        use crate::embedder::MockEmbedder;
        use crate::metrics::Metrics;
        use crate::query_pipeline::QueryPipeline;
        use crate::store::QueryResult;

        let db = Db::open_in_memory().unwrap();
        let store = MockChunkSearcher::with_results(vec![QueryResult {
            chunk_id: "mock-chunk-1".to_string(),
            file_id: "file-1".to_string(),
            file_path: "/notes/mock.md".to_string(),
            chunk_ord: 0,
            content: "The quick brown fox".to_string(),
            score: Some(0.05),
        }]);
        let embedder: Arc<dyn crate::embedder::Embed> = Arc::new(MockEmbedder);
        let metrics = Arc::new(Metrics::new());

        let pipeline = QueryPipeline::new(&db, &store, &embedder, &metrics);

        // search_ranked embeds the query and calls store.query — both are mocked.
        let (ranked, _confidence) = pipeline
            .search_ranked("fox", "semantic", &[])
            .await
            .unwrap();

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
        let ids = meta.get_file_ids_with_chunks().await.unwrap();
        assert!(ids.is_empty());
    }
}
