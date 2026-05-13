//! In-memory mock implementations of the core persistence ports.
//!
//! These mocks satisfy the trait contracts defined in `brain_core::ports`
//! using `HashMap`-backed in-memory state. They do not open SQLite or
//! LanceDB. Use them in unit tests that need a pipeline without real storage.

#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::error::Result;

use super::{ChunkIndexWriter, Embed, FileMetaReader, SchemaMeta};

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

    fn prune_versions(
        &self,
        _older_than: std::time::Duration,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {}
    }
}

// ---------------------------------------------------------------------------
// MockStore — implements both `ChunkIndexWriter` and `SchemaMeta`
// ---------------------------------------------------------------------------

// We need MockSchemaMeta + MockChunkIndexWriter combined into a single
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

    fn prune_versions(
        &self,
        _older_than: std::time::Duration,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {}
    }
}

// ---------------------------------------------------------------------------
// MockEmbedder
// ---------------------------------------------------------------------------

/// In-memory mock for the [`Embed`] port. Produces deterministic 384-dim unit
/// vectors derived from BLAKE3 of the input text, so tests are reproducible
/// without needing model weights on disk.
pub struct MockEmbedder;

impl Embed for MockEmbedder {
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|text| mock_embedding(text)).collect())
    }

    fn hidden_size(&self) -> usize {
        384
    }

    fn version(&self) -> &str {
        "mock-v1"
    }
}

/// Generate a deterministic 384-dim unit vector from text content using BLAKE3.
fn mock_embedding(text: &str) -> Vec<f32> {
    let hash = blake3::hash(text.as_bytes());
    let bytes = hash.as_bytes();

    let mut embedding = Vec::with_capacity(384);
    for i in 0..384 {
        // Cycle through 32 hash bytes to fill 384 dimensions.
        let byte = bytes[i % 32];
        embedding.push((byte as f32 / 255.0) - 0.5);
    }

    let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in &mut embedding {
            *v /= norm;
        }
    }

    embedding
}
