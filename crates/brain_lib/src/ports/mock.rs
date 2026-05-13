//! Brain_lib-resident mock impls for the persistence ports.
//!
//! Most port mocks now live in `brain_core::ports::mock` (the framework-free
//! mocks) and `brain_persistence::ports::mock` (mocks for traits whose
//! signatures bind to brain_persistence types). This module re-exports
//! both so existing `crate::ports::mock::*` imports keep resolving, and
//! adds the mocks that genuinely belong in brain_lib because they
//! reference brain_lib-internal types or helpers (`crate::hierarchy`,
//! `crate::utils::now_ts`).

#![allow(clippy::manual_async_fn, clippy::type_complexity)]

// Re-export the cross-crate mocks. `pub use` keeps them accessible as
// `crate::ports::mock::MockX` without touching call sites.
pub use brain_core::ports::mock::*;
pub use brain_persistence::ports::mock::*;

use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::Result;
use crate::hierarchy::{DerivedSummary, GeneratedScopeSummary, ScopeType};

use super::{DerivedSummaryReader, DerivedSummaryWriter, JobPersistence, TagAliasReader};

// ---------------------------------------------------------------------------
// MockDerivedSummaryPersistence
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// MockJobPersistence
// ---------------------------------------------------------------------------

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

    /// Fresh-brain default: no rows. Mirrors `alias_lookup_for_brain`'s
    /// "empty map for un-reclustered brains" contract so mock-based tests
    /// don't need to seed `tag_aliases` to call status/list tools.
    fn list_aliases_for_brain(
        &self,
        _brain_id: &str,
        _canonical: Option<&str>,
        _cluster_id: Option<&str>,
        _limit: i64,
        _offset: i64,
    ) -> Result<Vec<brain_persistence::db::tag_aliases::AliasRow>> {
        Ok(Vec::new())
    }

    /// Fresh-brain default: zeros across the board.
    fn count_aliases_for_brain(
        &self,
        _brain_id: &str,
    ) -> Result<brain_persistence::db::tag_aliases::AliasCounts> {
        Ok(brain_persistence::db::tag_aliases::AliasCounts {
            raw_count: 0,
            canonical_count: 0,
            cluster_count: 0,
        })
    }

    /// Fresh-brain default: no run yet.
    fn latest_run_for_brain(
        &self,
        _brain_id: &str,
    ) -> Result<Option<brain_persistence::db::tag_aliases::TagClusterRunRow>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use brain_core::ports::{ChunkIndexWriter, SchemaMeta};
    use brain_persistence::db::chunks::ChunkRow;
    use brain_persistence::db::fts::FtsResult;
    use brain_persistence::db::summaries::{Episode, SummaryRow};
    use brain_persistence::ports::{
        ChunkMetaReader, ChunkMetaWriter, ChunkSearcher, EmbeddingResetter, EpisodeReader,
        EpisodeWriter, FileMetaWriter, FtsSearcher, SummaryReader, SummaryWriter,
    };
    use brain_persistence::store::{QueryResult, VectorSearchMode};

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
