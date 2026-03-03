use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use arrow_array::{
    FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray,
    types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::table::OptimizeAction;
use tracing::{info, warn};

use crate::error::BrainCoreError;

const EMBEDDING_DIM: i32 = 384;

const DEFAULT_ROW_THRESHOLD: u64 = 200;
const DEFAULT_TIME_THRESHOLD: Duration = Duration::from_secs(300);

/// Tracks unoptimized mutations and schedules LanceDB optimize() calls.
///
/// Two primitives: `AtomicU64` keeps `record_mutation()` lock-free (called on
/// every Store write), while a single `tokio::sync::Mutex<Instant>` combines
/// run-exclusion with the last-optimize timestamp. All current callers are
/// sequential within a single tokio task, so the mutex is effectively
/// uncontended — but the design remains safe if concurrency is added later.
pub struct OptimizeScheduler {
    table: lancedb::Table,
    pending_mutations: AtomicU64,
    /// Guards optimize execution and tracks when the last optimize completed.
    guard: tokio::sync::Mutex<Instant>,
    row_threshold: u64,
    time_threshold: Duration,
}

impl OptimizeScheduler {
    pub fn new(table: lancedb::Table, row_threshold: u64, time_threshold: Duration) -> Self {
        Self {
            table,
            pending_mutations: AtomicU64::new(0),
            guard: tokio::sync::Mutex::new(Instant::now()),
            row_threshold,
            time_threshold,
        }
    }

    /// Record that `n` mutations occurred (called from all write methods).
    pub fn record_mutation(&self, n: u64) {
        self.pending_mutations.fetch_add(n, Ordering::Relaxed);
    }

    /// Returns the current pending mutation count.
    pub fn pending_count(&self) -> u64 {
        self.pending_mutations.load(Ordering::Relaxed)
    }

    /// Check triggers and run optimize if thresholds met. Skips if already running.
    pub async fn maybe_optimize(&self) {
        if self.pending_mutations.load(Ordering::Relaxed) == 0 {
            return;
        }
        // try_lock: skip if another optimize is in progress
        let Ok(guard) = self.guard.try_lock() else {
            return;
        };
        // Re-read after acquiring lock for a fresh value
        let pending = self.pending_mutations.load(Ordering::Relaxed);
        if pending == 0 {
            return;
        }
        if !self.should_run(pending, &guard) {
            return;
        }
        self.run_optimize(guard).await;
    }

    /// Wait for any in-progress optimize, then run unconditionally.
    pub async fn force_optimize(&self) {
        if self.pending_mutations.load(Ordering::Relaxed) == 0 {
            return;
        }
        let guard = self.guard.lock().await;
        if self.pending_mutations.load(Ordering::Relaxed) == 0 {
            return;
        }
        self.run_optimize(guard).await;
    }

    /// Check whether either trigger threshold has been reached.
    fn should_run(&self, pending: u64, last_optimize: &Instant) -> bool {
        if pending == 0 {
            return false;
        }
        pending >= self.row_threshold || last_optimize.elapsed() >= self.time_threshold
    }

    /// Execute optimize. Caller must hold the guard.
    async fn run_optimize(&self, mut last_optimize: tokio::sync::MutexGuard<'_, Instant>) {
        let snapshot = self.pending_mutations.load(Ordering::Relaxed);
        if snapshot == 0 {
            return;
        }

        match self.table.optimize(OptimizeAction::All).await {
            Ok(stats) => {
                self.pending_mutations.fetch_sub(snapshot, Ordering::Relaxed);
                *last_optimize = Instant::now();
                info!(
                    pending_before = snapshot,
                    compaction = ?stats.compaction.as_ref().map(|c| c.fragments_removed),
                    pruned = ?stats.prune.as_ref().map(|p| p.bytes_removed),
                    "LanceDB optimize complete"
                );
            }
            Err(e) => {
                // Don't subtract — mutations still pending for next trigger
                warn!(error = %e, "LanceDB optimize failed, will retry on next trigger");
            }
        }
    }
}

pub struct Store {
    /// Kept alive so the LanceDB table handle remains valid.
    #[allow(dead_code)]
    db: lancedb::Connection,
    table: lancedb::Table,
    optimize_scheduler: OptimizeScheduler,
}

impl Store {
    /// Open or create a LanceDB store at the given directory.
    pub async fn open_or_create(db_path: &Path) -> crate::error::Result<Self> {
        let db = lancedb::connect(db_path.to_str().unwrap_or(".brain/lancedb"))
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to connect: {e}")))?;

        let table_names = db
            .table_names()
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to list tables: {e}")))?;

        let table = if table_names.iter().any(|n| n == "chunks") {
            let t = db
                .open_table("chunks")
                .execute()
                .await
                .map_err(|e| BrainCoreError::VectorDb(format!("failed to open table: {e}")))?;

            // POC migration: detect old schema (no file_id column) and recreate
            if Self::needs_migration(&t).await {
                info!("detected old POC schema without file_id column — recreating table");
                db.drop_table("chunks", &[]).await.map_err(|e| {
                    BrainCoreError::VectorDb(format!("failed to drop old table: {e}"))
                })?;
                let schema = chunks_schema();
                let empty_batch = empty_record_batch(&schema);
                let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], Arc::new(schema));
                db.create_table("chunks", Box::new(batches))
                    .execute()
                    .await
                    .map_err(|e| BrainCoreError::VectorDb(format!("failed to create table: {e}")))?
            } else {
                t
            }
        } else {
            let schema = chunks_schema();
            let empty_batch = empty_record_batch(&schema);
            let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], Arc::new(schema));
            db.create_table("chunks", Box::new(batches))
                .execute()
                .await
                .map_err(|e| BrainCoreError::VectorDb(format!("failed to create table: {e}")))?
        };

        let optimize_scheduler =
            OptimizeScheduler::new(table.clone(), DEFAULT_ROW_THRESHOLD, DEFAULT_TIME_THRESHOLD);

        info!("LanceDB store ready");
        Ok(Self {
            db,
            table,
            optimize_scheduler,
        })
    }

    /// Access the optimize scheduler (for triggering optimize from CLI commands).
    pub fn optimizer(&self) -> &OptimizeScheduler {
        &self.optimize_scheduler
    }

    /// Check if the table uses the old POC schema (no file_id column).
    async fn needs_migration(table: &lancedb::Table) -> bool {
        match table.schema().await {
            Ok(schema) => schema.field_with_name("file_id").is_err(),
            Err(_) => false,
        }
    }

    /// Upsert chunks for a file using merge_insert.
    ///
    /// - Matched chunks (by chunk_id) are updated
    /// - New chunks are inserted
    /// - Orphaned chunks for this file_id are deleted
    pub async fn upsert_chunks(
        &self,
        file_id: &str,
        file_path: &str,
        chunks: &[(usize, &str)],
        embeddings: &[Vec<f32>],
    ) -> crate::error::Result<()> {
        if chunks.len() != embeddings.len() {
            return Err(BrainCoreError::VectorDb(format!(
                "chunk/embedding count mismatch: {} vs {}",
                chunks.len(),
                embeddings.len()
            )));
        }
        if chunks.is_empty() {
            // No chunks — just delete any existing chunks for this file
            self.delete_file_chunks(file_id).await?;
            return Ok(());
        }

        let schema = chunks_schema();
        let batch = make_record_batch(&schema, file_id, file_path, chunks, embeddings)?;
        let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));

        let mut builder = self.table.merge_insert(&["chunk_id"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .when_not_matched_by_source_delete(Some(format!(
                "file_id = '{}'",
                validate_file_id(file_id)?
            )));
        builder
            .execute(Box::new(batches))
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("upsert failed: {e}")))?;

        self.optimize_scheduler
            .record_mutation(chunks.len() as u64);

        info!(
            file_path,
            file_id,
            chunk_count = chunks.len(),
            "chunks upserted"
        );
        Ok(())
    }

    /// Update the file_path column for all chunks belonging to a file_id.
    pub async fn update_file_path(
        &self,
        file_id: &str,
        new_path: &str,
    ) -> crate::error::Result<()> {
        let fid = validate_file_id(file_id)?;
        self.table
            .update()
            .only_if(format!("file_id = '{fid}'"))
            .column("file_path", format!("'{}'", new_path.replace('\'', "''")))
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("update file_path failed: {e}")))?;

        self.optimize_scheduler.record_mutation(1);

        info!(file_id, new_path, "file_path updated in LanceDB");
        Ok(())
    }

    /// Delete all chunks for a given file_id.
    pub async fn delete_file_chunks(&self, file_id: &str) -> crate::error::Result<()> {
        self.table
            .delete(&format!("file_id = '{}'", validate_file_id(file_id)?))
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("delete failed: {e}")))?;

        self.optimize_scheduler.record_mutation(1);

        info!(file_id, "file chunks deleted from LanceDB");
        Ok(())
    }

    /// Search for the top-k most similar chunks to the given embedding.
    pub async fn query(
        &self,
        embedding: &[f32],
        top_k: usize,
    ) -> crate::error::Result<Vec<QueryResult>> {
        let results = self
            .table
            .vector_search(embedding)
            .map_err(|e| BrainCoreError::VectorDb(format!("search setup failed: {e}")))?
            .distance_type(lancedb::DistanceType::Dot)
            .limit(top_k)
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("search failed: {e}")))?;

        let mut output = Vec::new();
        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = results
            .try_collect()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("result collection failed: {e}")))?;

        for batch in &batches {
            let chunk_ids = batch
                .column_by_name("chunk_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| BrainCoreError::VectorDb("missing chunk_id column".into()))?;
            let file_ids = batch
                .column_by_name("file_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| BrainCoreError::VectorDb("missing file_id column".into()))?;
            let file_paths = batch
                .column_by_name("file_path")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| BrainCoreError::VectorDb("missing file_path column".into()))?;
            let chunk_ords = batch
                .column_by_name("chunk_ord")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
                .ok_or_else(|| BrainCoreError::VectorDb("missing chunk_ord column".into()))?;
            let contents = batch
                .column_by_name("content")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| BrainCoreError::VectorDb("missing content column".into()))?;
            let distances = batch
                .column_by_name("_distance")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>());

            for i in 0..batch.num_rows() {
                output.push(QueryResult {
                    chunk_id: chunk_ids.value(i).to_string(),
                    file_id: file_ids.value(i).to_string(),
                    file_path: file_paths.value(i).to_string(),
                    chunk_ord: chunk_ords.value(i) as usize,
                    content: contents.value(i).to_string(),
                    score: distances.map(|d| d.value(i)),
                });
            }
        }

        Ok(output)
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub chunk_id: String,
    pub file_id: String,
    pub file_path: String,
    pub chunk_ord: usize,
    pub content: String,
    pub score: Option<f32>,
}

fn chunks_schema() -> Schema {
    Schema::new(vec![
        Field::new("chunk_id", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("chunk_ord", DataType::Int32, false),
        Field::new("content", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                EMBEDDING_DIM,
            ),
            false,
        ),
    ])
}

fn empty_record_batch(schema: &Schema) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    Vec::<Option<Vec<Option<f32>>>>::new(),
                    EMBEDDING_DIM,
                ),
            ),
        ],
    )
    .expect("empty batch should be valid")
}

/// Validate that a file_id is safe to interpolate into a LanceDB filter expression.
///
/// Accepts both legacy UUID format (hex digits + hyphens) and ULID format
/// (Crockford Base32: alphanumeric characters). The key constraint is preventing
/// SQL injection in filter strings, so we allow only `[a-zA-Z0-9-]`.
fn validate_file_id(file_id: &str) -> crate::error::Result<&str> {
    if !file_id.is_empty()
        && file_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        Ok(file_id)
    } else {
        Err(BrainCoreError::VectorDb(format!(
            "invalid file_id for filter: {file_id}"
        )))
    }
}

fn make_record_batch(
    schema: &Schema,
    file_id: &str,
    file_path: &str,
    chunks: &[(usize, &str)],
    embeddings: &[Vec<f32>],
) -> crate::error::Result<RecordBatch> {
    let chunk_ids: Vec<String> = chunks
        .iter()
        .map(|(ord, _)| format!("{file_id}:{ord}"))
        .collect();
    let file_ids: Vec<&str> = vec![file_id; chunks.len()];
    let file_paths: Vec<&str> = vec![file_path; chunks.len()];
    let ords: Vec<i32> = chunks.iter().map(|(ord, _)| *ord as i32).collect();
    let contents: Vec<&str> = chunks.iter().map(|(_, content)| *content).collect();

    let embedding_values: Vec<Option<Vec<Option<f32>>>> = embeddings
        .iter()
        .map(|emb| Some(emb.iter().map(|v| Some(*v)).collect()))
        .collect();

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(chunk_ids)),
            Arc::new(StringArray::from(file_ids)),
            Arc::new(StringArray::from(file_paths)),
            Arc::new(Int32Array::from(ords)),
            Arc::new(StringArray::from(contents)),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    embedding_values,
                    EMBEDDING_DIM,
                ),
            ),
        ],
    )
    .map_err(|e| BrainCoreError::VectorDb(format!("failed to build record batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a scheduler backed by a real LanceDB table in a temp dir.
    async fn test_scheduler(
        row_threshold: u64,
        time_threshold: Duration,
    ) -> (OptimizeScheduler, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = lancedb::connect(tmp.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = chunks_schema();
        let empty_batch = empty_record_batch(&schema);
        let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], Arc::new(schema));
        let table = db
            .create_table("chunks", Box::new(batches))
            .execute()
            .await
            .unwrap();
        let scheduler = OptimizeScheduler::new(table, row_threshold, time_threshold);
        (scheduler, tmp)
    }

    #[test]
    fn test_record_mutation_increments_counter() {
        // Use a dummy — we only need the atomic counter, not a real table
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (sched, _tmp) = test_scheduler(200, Duration::from_secs(300)).await;
            assert_eq!(sched.pending_count(), 0);

            sched.record_mutation(5);
            assert_eq!(sched.pending_count(), 5);

            sched.record_mutation(3);
            assert_eq!(sched.pending_count(), 8);
        });
    }

    #[tokio::test]
    async fn test_maybe_optimize_noop_when_zero_pending() {
        let (sched, _tmp) = test_scheduler(10, Duration::from_millis(1)).await;
        // Even with a tiny time threshold, zero pending → no optimize
        tokio::time::sleep(Duration::from_millis(5)).await;
        sched.maybe_optimize().await;
        assert_eq!(sched.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_maybe_optimize_triggers_at_row_threshold() {
        let (sched, _tmp) = test_scheduler(10, Duration::from_secs(300)).await;

        sched.record_mutation(9);
        sched.maybe_optimize().await;
        // Below threshold — counter unchanged
        assert_eq!(sched.pending_count(), 9);

        sched.record_mutation(1);
        sched.maybe_optimize().await;
        // At threshold — optimize ran, counter reset
        assert_eq!(sched.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_maybe_optimize_triggers_on_time_threshold() {
        tokio::time::pause();
        let (sched, _tmp) = test_scheduler(1000, Duration::from_millis(100)).await;

        sched.record_mutation(1);
        tokio::time::advance(Duration::from_millis(200)).await;
        sched.maybe_optimize().await;
        // Time threshold exceeded — optimize ran, counter reset
        assert_eq!(sched.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_maybe_optimize_skips_when_running() {
        let (sched, _tmp) = test_scheduler(1, Duration::from_secs(300)).await;
        sched.record_mutation(10);

        // Hold the guard — simulates an in-progress optimize
        let _guard = sched.guard.try_lock().unwrap();

        // maybe_optimize should skip (try_lock fails) without panic
        sched.maybe_optimize().await;

        // Counter unchanged because optimize was skipped
        assert_eq!(sched.pending_count(), 10);
    }

    #[tokio::test]
    async fn test_force_optimize_noop_when_zero_pending() {
        let (sched, _tmp) = test_scheduler(10, Duration::from_secs(300)).await;
        assert_eq!(sched.pending_count(), 0);

        // force_optimize with 0 pending → no-op, no panic
        sched.force_optimize().await;
        assert_eq!(sched.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_force_optimize_waits_not_skips() {
        let (sched, _tmp) = test_scheduler(1000, Duration::from_secs(300)).await;
        let sched = Arc::new(sched);
        sched.record_mutation(5);

        // Hold the guard to simulate an in-progress optimize
        let guard = sched.guard.lock().await;

        // Spawn force_optimize on a separate task — it should block on the mutex
        let sched2 = Arc::clone(&sched);
        let handle = tokio::spawn(async move {
            sched2.force_optimize().await;
        });

        // Give the spawned task a chance to reach the lock
        tokio::task::yield_now().await;

        // force_optimize should still be waiting (not completed)
        assert!(!handle.is_finished());
        // Counter still set because optimize hasn't run
        assert_eq!(sched.pending_count(), 5);

        // Release the guard — force_optimize can now proceed
        drop(guard);
        handle.await.unwrap();

        // Counter reset after optimize ran
        assert_eq!(sched.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_multi_cycle_counter_resets() {
        let (sched, _tmp) = test_scheduler(5, Duration::from_secs(300)).await;

        // Cycle 1: accumulate past threshold and optimize
        sched.record_mutation(10);
        assert_eq!(sched.pending_count(), 10);
        sched.maybe_optimize().await;
        assert_eq!(sched.pending_count(), 0);

        // Cycle 2: accumulate again past threshold and optimize
        sched.record_mutation(5);
        assert_eq!(sched.pending_count(), 5);
        sched.maybe_optimize().await;
        assert_eq!(sched.pending_count(), 0);
    }
}
