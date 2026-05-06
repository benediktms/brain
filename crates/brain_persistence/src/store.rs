use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
    StringArray, types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use lancedb::index::{Index, vector::IvfPqIndexBuilder};
use lancedb::table::OptimizeAction;
use tracing::{info, instrument, warn};

use crate::error::BrainCoreError;

const EMBEDDING_DIM: i32 = 384;

/// Bump this whenever the LanceDB Arrow schema changes (new columns, type
/// changes, vector dimension changes). On startup the pipeline compares the
/// stored version in `brain_meta` against this constant and triggers a full
/// table rebuild + content-hash clear when they differ.
pub const LANCE_SCHEMA_VERSION: u32 = 2;

/// Mutations before auto-optimize triggers. Set high to avoid memory-heavy
/// compaction during bulk re-index. The daemon's shutdown sequence and
/// `brain vacuum` both call `force_optimize()` which bypasses this threshold.
const DEFAULT_ROW_THRESHOLD: u64 = 5_000;
const DEFAULT_TIME_THRESHOLD: Duration = Duration::from_secs(600);

/// Minimum link count before PageRank scores become meaningful.
///
/// Below this size, PageRank cost dominates and benefit is marginal; the
/// threshold was chosen empirically — adjust as link-graph density evolves.
///
/// A monotonic latch (`pagerank_links_threshold_met` on `OptimizeScheduler`)
/// short-circuits the per-cycle `COUNT(*)` query once this threshold is first
/// observed. Link counts only grow during normal operation; the worst-case of
/// counts shrinking past the threshold yields one extra PageRank cycle, which
/// is acceptable.
const PAGERANK_MIN_LINKS: i64 = 1000;

/// Minimum rows required before IVF-PQ index creation is worthwhile.
///
/// Set high because with the unified store all brains share one table —
/// IVF-PQ training loads all vectors into memory at once.  At typical
/// brain scales (~20k vectors, 384-dim) brute-force search is <5ms,
/// so the index is unnecessary overhead.  Users who need ANN speed at
/// 100k+ scale can create one explicitly via `brain vacuum`.
const MIN_ROWS_FOR_INDEX: u64 = 100_000;

/// Default nprobes used when querying with an IVF index.
pub const DEFAULT_NPROBES: usize = 20;

/// Default refine factor for ANN-refined mode.
const DEFAULT_REFINE_FACTOR: u32 = 10;

/// Controls the vector search strategy, trading determinism for speed.
///
/// Vector search can operate in two fundamental modes: **exact** (brute-force
/// comparison against every stored vector) or **ANN** (Approximate Nearest
/// Neighbor — uses index structures like IVF-PQ to narrow candidates quickly,
/// trading some accuracy for much faster search at scale).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchMode {
    /// Brute-force scan — bypasses any ANN index and compares the query vector
    /// against every stored vector. O(n) cost, 100% accurate, fully
    /// deterministic. Use for golden tests and debugging.
    Exact,
    /// ANN search with refinement — the index finds approximate candidates,
    /// then LanceDB fetches the full uncompressed vectors and rescores them
    /// (`refine_factor`). Good balance of speed and fidelity: fast ANN
    /// candidate selection with accurate final ordering.
    #[default]
    AnnRefined,
    /// Pure ANN search — uses only the compressed (quantized) vectors stored
    /// in the index. Fastest mode, but distances are approximate and results
    /// can shift across index rebuilds.
    AnnFast,
}

impl std::fmt::Display for VectorSearchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact => write!(f, "exact"),
            Self::AnnRefined => write!(f, "ann_refined"),
            Self::AnnFast => write!(f, "ann_fast"),
        }
    }
}

impl std::str::FromStr for VectorSearchMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "exact" => Ok(Self::Exact),
            "ann_refined" => Ok(Self::AnnRefined),
            "ann_fast" => Ok(Self::AnnFast),
            other => Err(format!(
                "unknown vector_search_mode '{other}'; expected exact, ann_refined, or ann_fast"
            )),
        }
    }
}

/// Configuration for IVF-PQ vector index creation.
#[derive(Debug, Clone)]
pub struct IvfPqConfig {
    pub num_partitions: Option<u32>,
    pub num_sub_vectors: Option<u32>,
    pub nprobes: usize,
}

impl IvfPqConfig {
    /// Auto-calculate partitions from row count (sqrt(N)).
    pub fn auto_partitions(row_count: u64) -> u32 {
        (row_count as f64).sqrt().ceil().max(1.0) as u32
    }
}

impl Default for IvfPqConfig {
    fn default() -> Self {
        Self {
            num_partitions: None,  // let LanceDB auto-calculate (sqrt(N))
            num_sub_vectors: None, // let LanceDB auto-calculate (dim/16 or dim/8)
            nprobes: DEFAULT_NPROBES,
        }
    }
}

/// Tracks unoptimized mutations and schedules LanceDB optimize() calls.
///
/// Two primitives: `AtomicU64` keeps `record_mutation()` lock-free (called on
/// every Store write), while a single `tokio::sync::Mutex<Instant>` combines
/// run-exclusion with the last-optimize timestamp. All current callers are
/// sequential within a single tokio task, so the mutex is effectively
/// uncontended — but the design remains safe if concurrency is added later.
pub struct OptimizeScheduler {
    table: Arc<lancedb::Table>,
    pending_mutations: AtomicU64,
    /// Guards optimize execution and tracks when the last optimize completed.
    guard: tokio::sync::Mutex<Instant>,
    row_threshold: u64,
    time_threshold: Duration,
    db: std::sync::Mutex<Option<Arc<crate::db::Db>>>,
    /// Monotonic latch: flips to `true` on first observation of
    /// `link_count >= PAGERANK_MIN_LINKS`, short-circuiting the per-cycle
    /// `COUNT(*)` query thereafter. Reset on `set_db` (DB swap = stale latch).
    pagerank_links_threshold_met: AtomicBool,
}

impl OptimizeScheduler {
    pub fn new(table: Arc<lancedb::Table>, row_threshold: u64, time_threshold: Duration) -> Self {
        Self {
            table,
            pending_mutations: AtomicU64::new(0),
            guard: tokio::sync::Mutex::new(Instant::now()),
            row_threshold,
            time_threshold,
            db: std::sync::Mutex::new(None),
            pagerank_links_threshold_met: AtomicBool::new(false),
        }
    }

    /// Attach a SQLite database pool for post-optimize PageRank computation.
    ///
    /// Resets the PageRank threshold latch so the new DB is probed fresh.
    pub fn set_db(&self, db: Arc<crate::db::Db>) {
        *self.db.lock().expect("set_db lock") = Some(db);
        // DB swap invalidates any cached threshold observation.
        self.pagerank_links_threshold_met
            .store(false, Ordering::Relaxed);
    }

    /// Take the current db reference (for preservation across table rebuilds).
    pub fn take_db(&self) -> Option<Arc<crate::db::Db>> {
        self.db.lock().expect("take_db lock").take()
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
        self.run_optimize(guard, false).await;
    }

    /// Wait for any in-progress optimize, then run unconditionally.
    ///
    /// Used by `brain vacuum`, daemon shutdown, and tests — all callers that
    /// want compaction to run regardless of the in-memory `pending_mutations`
    /// counter. The counter resets on every process restart, so gating on it
    /// would defeat callers that operate on a freshly-opened scheduler (the
    /// most common case for `brain vacuum`, which always runs as a fresh CLI
    /// invocation). Also bypasses the PageRank link-count gate so PageRank
    /// runs regardless of graph size.
    pub async fn force_optimize(&self) {
        let guard = self.guard.lock().await;
        self.run_optimize(guard, true).await;
    }

    /// Run compaction unconditionally — ignores the `pending_mutations` counter.
    ///
    /// Use at daemon startup to compact historical fragment debt from previous
    /// runs. The pending counter starts at 0 on restart, so `force_optimize`
    /// and `maybe_optimize` would never trigger for pre-existing fragments.
    pub async fn startup_compact(&self) {
        let mut last_optimize = self.guard.lock().await;
        info!("running startup compaction");
        match self.table.optimize(OptimizeAction::All).await {
            Ok(stats) => {
                // Reset pending counter — startup compact subsumes any early mutations.
                self.pending_mutations.store(0, Ordering::Relaxed);
                *last_optimize = Instant::now();
                info!(
                    compaction = ?stats.compaction.as_ref().map(|c| c.fragments_removed),
                    pruned = ?stats.prune.as_ref().map(|p| p.bytes_removed),
                    "startup compaction complete"
                );
            }
            Err(e) => {
                warn!(error = %e, "startup compaction failed, will retry via normal optimize");
            }
        }
    }

    /// Check whether either trigger threshold has been reached.
    fn should_run(&self, pending: u64, last_optimize: &Instant) -> bool {
        if pending == 0 {
            return false;
        }
        pending >= self.row_threshold || last_optimize.elapsed() >= self.time_threshold
    }

    /// Execute optimize. Caller must hold the guard.
    ///
    /// `bypass_pagerank_gate`: when `true`, run PageRank regardless of the
    /// link-count threshold. Used by `force_optimize`.
    async fn run_optimize(
        &self,
        mut last_optimize: tokio::sync::MutexGuard<'_, Instant>,
        bypass_pagerank_gate: bool,
    ) {
        // The pending_mutations counter resets on process restart, so a
        // freshly-opened scheduler with on-disk fragment debt sees snapshot=0.
        // Run unconditionally — `fetch_sub(0)` is a no-op and `OptimizeAction::All`
        // is cheap when there's nothing to compact. Maybe_optimize callers
        // pre-check for `pending == 0` before reaching here.
        let snapshot = self.pending_mutations.load(Ordering::Relaxed);

        match self.table.optimize(OptimizeAction::All).await {
            Ok(stats) => {
                self.pending_mutations
                    .fetch_sub(snapshot, Ordering::Relaxed);
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
                return;
            }
        }

        // Auto-create IVF-PQ index if enough rows and no index exists yet
        self.maybe_create_index().await;

        // Recompute PageRank scores after compaction so link scores stay fresh.
        // Gate with a link-count threshold: PageRank is O(N) on the links table
        // and becomes the dominant cost in the optimize hot path for large link
        // sets. A monotonic latch avoids a redundant COUNT(*) once the threshold
        // is first observed — see PAGERANK_MIN_LINKS for the rationale.
        if let Some(ref db) = *self.db.lock().expect("optimize db lock") {
            let above_threshold = if bypass_pagerank_gate
                || self.pagerank_links_threshold_met.load(Ordering::Relaxed)
            {
                true
            } else {
                let link_count = db
                    .with_read_conn(|conn| {
                        conn.query_row("SELECT COUNT(*) FROM links", [], |row| row.get::<_, i64>(0))
                            .map_err(BrainCoreError::from)
                    })
                    .inspect_err(|e| tracing::warn!(error = %e, "link count query failed; skipping PageRank for this cycle"))
                    .unwrap_or(0);
                if link_count >= PAGERANK_MIN_LINKS {
                    self.pagerank_links_threshold_met
                        .store(true, Ordering::Relaxed);
                    true
                } else {
                    tracing::debug!(
                        link_count,
                        "skipping PageRank recompute: link count below threshold"
                    );
                    false
                }
            };
            if above_threshold
                && let Err(e) = db.with_write_conn(crate::pagerank::compute_and_store_pagerank)
            {
                warn!(error = %e, "PageRank computation failed, will retry on next optimize");
            }
        }
    }

    /// Create IVF-PQ vector index if the table has enough rows and no vector index exists.
    async fn maybe_create_index(&self) {
        let indices = match self.table.list_indices().await {
            Ok(i) => i,
            Err(e) => {
                warn!(error = %e, "failed to list indices, skipping auto-index");
                return;
            }
        };

        let has_vector_index = indices
            .iter()
            .any(|i| i.columns.contains(&"embedding".to_string()));

        if has_vector_index {
            return;
        }

        let count = match self.table.count_rows(None).await {
            Ok(c) => c as u64,
            Err(e) => {
                warn!(error = %e, "failed to count rows for auto-index");
                return;
            }
        };

        if count < MIN_ROWS_FOR_INDEX {
            return;
        }

        let config = IvfPqConfig {
            num_partitions: Some(IvfPqConfig::auto_partitions(count)),
            ..Default::default()
        };

        let mut builder = IvfPqIndexBuilder::default().distance_type(lancedb::DistanceType::Dot);

        if let Some(np) = config.num_partitions {
            builder = builder.num_partitions(np);
        }

        match self
            .table
            .create_index(&["embedding"], Index::IvfPq(builder))
            .replace(true)
            .execute()
            .await
        {
            Ok(()) => {
                info!(
                    row_count = count,
                    num_partitions = ?config.num_partitions,
                    "IVF-PQ vector index auto-created during optimize"
                );
            }
            Err(e) => {
                warn!(error = %e, "auto index creation failed, will retry on next optimize");
            }
        }
    }
}

#[derive(Clone)]
pub struct Store {
    /// Kept alive so the LanceDB table handle remains valid.
    #[allow(dead_code)]
    db: lancedb::Connection,
    table: Arc<lancedb::Table>,
    optimize_scheduler: Arc<OptimizeScheduler>,
}

/// Read-only handle to a LanceDB table for query operations.
///
/// Cheap to clone (wraps `Arc<Table>`). Created from an existing `Store`
/// via `StoreReader::from_store()` — no extra connection needed.
#[derive(Clone)]
pub struct StoreReader {
    table: Arc<lancedb::Table>,
}

impl StoreReader {
    /// Create a reader from an existing store (shares the same table handle).
    pub fn from_store(store: &Store) -> Self {
        Self {
            table: Arc::clone(&store.table),
        }
    }

    /// Search for the top-k most similar chunks to the given embedding.
    pub async fn query(
        &self,
        embedding: &[f32],
        top_k: usize,
        nprobes: usize,
        mode: VectorSearchMode,
        brain_id: Option<&str>,
    ) -> crate::error::Result<Vec<QueryResult>> {
        query_impl(&self.table, embedding, top_k, nprobes, mode, brain_id).await
    }
}

impl Store {
    /// Open or create a LanceDB store at the given directory.
    pub async fn open_or_create(db_path: &Path) -> crate::error::Result<Self> {
        let db_path_str = db_path.to_str().ok_or_else(|| {
            BrainCoreError::VectorDb("LanceDB path contains non-UTF-8 characters".to_string())
        })?;
        let db = lancedb::connect(db_path_str)
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to connect: {e}")))?;

        let table_names = db
            .table_names()
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to list tables: {e}")))?;

        let table = if table_names.iter().any(|n| n == "chunks") {
            db.open_table("chunks")
                .execute()
                .await
                .map_err(|e| BrainCoreError::VectorDb(format!("failed to open table: {e}")))?
        } else {
            let schema = chunks_schema();
            let empty_batch = empty_record_batch(&schema)?;
            let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], Arc::new(schema));
            db.create_table("chunks", Box::new(batches))
                .execute()
                .await
                .map_err(|e| BrainCoreError::VectorDb(format!("failed to create table: {e}")))?
        };

        let table = Arc::new(table);
        let optimize_scheduler = OptimizeScheduler::new(
            Arc::clone(&table),
            DEFAULT_ROW_THRESHOLD,
            DEFAULT_TIME_THRESHOLD,
        );

        info!("LanceDB store ready");
        Ok(Self {
            db,
            table,
            optimize_scheduler: Arc::new(optimize_scheduler),
        })
    }

    /// Access the optimize scheduler (for triggering optimize from CLI commands).
    pub fn optimizer(&self) -> &OptimizeScheduler {
        &self.optimize_scheduler
    }

    /// Attach a SQLite database pool so that PageRank is recomputed after each optimize.
    pub fn set_db(&self, db: Arc<crate::db::Db>) {
        self.optimize_scheduler.set_db(db);
    }

    /// Access the underlying LanceDB table (for index inspection/management).
    pub fn table(&self) -> &lancedb::Table {
        &self.table
    }

    /// Drop the `chunks` table and recreate it with the current schema.
    ///
    /// Called when `LANCE_SCHEMA_VERSION` changes. The caller is responsible
    /// for clearing content hashes in SQLite so all files get re-indexed.
    pub async fn drop_and_recreate_table(&mut self) -> crate::error::Result<()> {
        warn!("dropping and recreating LanceDB chunks table for schema upgrade");

        self.db
            .drop_table("chunks", &[])
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to drop chunks table: {e}")))?;

        let schema = chunks_schema();
        let empty_batch = empty_record_batch(&schema)?;
        let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], Arc::new(schema));
        let table = self
            .db
            .create_table("chunks", Box::new(batches))
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("failed to recreate table: {e}")))?;

        let table = Arc::new(table);
        let saved_db = self.optimize_scheduler.take_db();
        self.optimize_scheduler = Arc::new(OptimizeScheduler::new(
            Arc::clone(&table),
            DEFAULT_ROW_THRESHOLD,
            DEFAULT_TIME_THRESHOLD,
        ));
        if let Some(db) = saved_db {
            self.optimize_scheduler.set_db(db);
        }
        self.table = table;

        info!("LanceDB chunks table recreated with current schema");
        Ok(())
    }

    /// Check whether the live table schema matches the expected `chunks_schema()`.
    ///
    /// Compares field names and types. Used for diagnostic logging — the version
    /// number in `brain_meta` is the actual migration trigger, not this check.
    pub async fn current_schema_matches_expected(&self) -> bool {
        let Ok(live) = self.table.schema().await else {
            return false;
        };
        let expected = chunks_schema();
        if live.fields().len() != expected.fields().len() {
            return false;
        }
        live.fields()
            .iter()
            .zip(expected.fields())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
    }

    /// Upsert chunks for a file using merge_insert.
    ///
    /// - Matched chunks (by chunk_id) are updated
    /// - New chunks are inserted
    /// - Orphaned chunks for this file_id are deleted
    #[instrument(skip_all)]
    pub async fn upsert_chunks(
        &self,
        file_id: &str,
        file_path: &str,
        brain_id: &str,
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
            self.delete_file_chunks(file_id, brain_id).await?;
            return Ok(());
        }

        let schema = chunks_schema();
        let batch = make_record_batch(&schema, file_id, file_path, brain_id, chunks, embeddings)?;
        let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));

        let mut builder = self.table.merge_insert(&["chunk_id"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .when_not_matched_by_source_delete(Some(format!(
                "file_id = '{}' AND brain_id = '{}'",
                validate_filter_value(file_id)?,
                validate_filter_value(brain_id)?
            )));
        builder
            .execute(Box::new(batches))
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("upsert failed: {e}")))?;

        self.optimize_scheduler.record_mutation(chunks.len() as u64);

        info!(
            file_path,
            file_id,
            brain_id,
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
        brain_id: &str,
    ) -> crate::error::Result<()> {
        let fid = validate_filter_value(file_id)?;
        let bid = validate_filter_value(brain_id)?;
        self.table
            .update()
            .only_if(format!("file_id = '{fid}' AND brain_id = '{bid}'"))
            .column("file_path", format!("'{}'", new_path.replace('\'', "''")))
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("update file_path failed: {e}")))?;

        self.optimize_scheduler.record_mutation(1);

        info!(file_id, new_path, "file_path updated in LanceDB");
        Ok(())
    }

    /// Update the brain_id column for all chunks belonging to the given file_ids.
    ///
    /// Used by the task-transfer transaction to re-attribute vector rows to the
    /// target brain after the SQLite `brain_id` has been updated. Only the
    /// LanceDB filter metadata changes — vector content is untouched.
    pub async fn update_brain_id_for_files(
        &self,
        file_ids: &[&str],
        old_brain_id: &str,
        new_brain_id: &str,
    ) -> crate::error::Result<()> {
        let bid_old = validate_filter_value(old_brain_id)?;
        let bid_new = validate_filter_value(new_brain_id)?;
        for file_id in file_ids {
            let fid = validate_filter_value(file_id)?;
            self.table
                .update()
                .only_if(format!("file_id = '{fid}' AND brain_id = '{bid_old}'"))
                .column("brain_id", format!("'{bid_new}'"))
                .execute()
                .await
                .map_err(|e| {
                    BrainCoreError::VectorDb(format!("update brain_id failed for {file_id}: {e}"))
                })?;
        }
        if !file_ids.is_empty() {
            self.optimize_scheduler
                .record_mutation(file_ids.len() as u64);
            info!(
                count = file_ids.len(),
                new_brain_id, "brain_id updated in LanceDB for file_ids"
            );
        }
        Ok(())
    }

    /// Delete all chunks for a given file_id within a brain.
    #[instrument(skip_all)]
    pub async fn delete_file_chunks(
        &self,
        file_id: &str,
        brain_id: &str,
    ) -> crate::error::Result<()> {
        let fid = validate_filter_value(file_id)?;
        let bid = validate_filter_value(brain_id)?;
        self.table
            .delete(&format!("file_id = '{fid}' AND brain_id = '{bid}'"))
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("delete failed: {e}")))?;

        self.optimize_scheduler.record_mutation(1);

        info!(file_id, "file chunks deleted from LanceDB");
        Ok(())
    }

    /// Get all distinct file_ids that have chunks in LanceDB for a brain.
    pub async fn get_file_ids_with_chunks(
        &self,
        brain_id: &str,
    ) -> crate::error::Result<std::collections::HashSet<String>> {
        use futures::TryStreamExt;
        use lancedb::query::{ExecutableQuery, QueryBase};

        let bid = validate_filter_value(brain_id)?;
        let results = self
            .table
            .query()
            .select(lancedb::query::Select::columns(&["file_id"]))
            .only_if(format!("brain_id = '{bid}'"))
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("file_id query failed: {e}")))?;

        let batches: Vec<RecordBatch> = results
            .try_collect()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("file_id collection failed: {e}")))?;

        let mut file_ids = std::collections::HashSet::new();
        for batch in &batches {
            let col = batch
                .column_by_name("file_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| BrainCoreError::VectorDb("missing file_id column".into()))?;
            for i in 0..col.len() {
                file_ids.insert(col.value(i).to_string());
            }
        }

        Ok(file_ids)
    }

    /// Delete all chunks for the given file_ids (bulk orphan cleanup).
    pub async fn delete_chunks_by_file_ids(
        &self,
        file_ids: &[String],
        brain_id: &str,
    ) -> crate::error::Result<usize> {
        if file_ids.is_empty() {
            return Ok(0);
        }
        let mut deleted = 0;
        for fid in file_ids {
            self.delete_file_chunks(fid, brain_id).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    /// Search for the top-k most similar chunks to the given embedding.
    #[instrument(skip_all)]
    pub async fn query(
        &self,
        embedding: &[f32],
        top_k: usize,
        nprobes: usize,
        mode: VectorSearchMode,
        brain_id: Option<&str>,
    ) -> crate::error::Result<Vec<QueryResult>> {
        query_impl(&self.table, embedding, top_k, nprobes, mode, brain_id).await
    }

    /// Upsert a single summary embedding into LanceDB.
    ///
    /// Uses `chunk_id = "sum:{summary_id}"`, `file_id = "sum:{summary_id}"`,
    /// `file_path = ""`, `chunk_ord = 0`. No orphan-delete clause — each
    /// summary is its own file_id scope, so only match/insert arms are needed.
    #[instrument(skip_all)]
    pub async fn upsert_summary(
        &self,
        summary_id: &str,
        content: &str,
        brain_id: &str,
        embedding: &[f32],
    ) -> crate::error::Result<()> {
        let key = format!("sum:{summary_id}");
        let schema = chunks_schema();

        let embedding_values: Vec<Option<Vec<Option<f32>>>> =
            vec![Some(embedding.iter().map(|v| Some(*v)).collect())];

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(StringArray::from(vec![key.as_str()])),
                Arc::new(StringArray::from(vec![key.as_str()])),
                Arc::new(StringArray::from(vec![""])),
                Arc::new(Int32Array::from(vec![0i32])),
                Arc::new(StringArray::from(vec![content])),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        embedding_values,
                        EMBEDDING_DIM,
                    ),
                ),
                Arc::new(StringArray::from(vec![brain_id])),
            ],
        )
        .map_err(|e| BrainCoreError::VectorDb(format!("failed to build summary batch: {e}")))?;

        let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));

        let mut builder = self.table.merge_insert(&["chunk_id"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        builder
            .execute(Box::new(batches))
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("summary upsert failed: {e}")))?;

        self.optimize_scheduler.record_mutation(1);

        info!(summary_id, "summary upserted into LanceDB");
        Ok(())
    }

    /// Delete a summary's embedding from LanceDB.
    ///
    /// Deletes the row with `chunk_id = "sum:{summary_id}"` within the brain.
    #[instrument(skip_all)]
    pub async fn delete_summary(
        &self,
        summary_id: &str,
        brain_id: &str,
    ) -> crate::error::Result<()> {
        let key = format!("sum:{summary_id}");
        let safe_key = validate_filter_value(&key)?;
        let bid = validate_filter_value(brain_id)?;
        self.table
            .delete(&format!("chunk_id = '{safe_key}' AND brain_id = '{bid}'"))
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("summary delete failed: {e}")))?;

        self.optimize_scheduler.record_mutation(1);

        info!(summary_id, "summary deleted from LanceDB");
        Ok(())
    }

    /// Create an IVF-PQ vector index on the embedding column.
    pub async fn create_vector_index(&self, config: &IvfPqConfig) -> crate::error::Result<()> {
        let mut builder = IvfPqIndexBuilder::default().distance_type(lancedb::DistanceType::Dot);

        if let Some(np) = config.num_partitions {
            builder = builder.num_partitions(np);
        }
        if let Some(nsv) = config.num_sub_vectors {
            builder = builder.num_sub_vectors(nsv);
        }

        self.table
            .create_index(&["embedding"], Index::IvfPq(builder))
            .replace(true)
            .execute()
            .await
            .map_err(|e| BrainCoreError::VectorDb(format!("index creation failed: {e}")))?;

        info!(
            num_partitions = ?config.num_partitions,
            num_sub_vectors = ?config.num_sub_vectors,
            "IVF-PQ vector index created"
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub chunk_id: String,
    pub file_id: String,
    pub file_path: String,
    pub brain_id: String,
    pub chunk_ord: usize,
    pub content: String,
    pub score: Option<f32>,
}

/// Shared vector search implementation used by both `Store` and `StoreReader`.
///
/// When `brain_id` is `Some`, a post-filter restricts results to that brain.
/// When `None`, all brains are searched (useful for cross-brain federated queries).
async fn query_impl(
    table: &lancedb::Table,
    embedding: &[f32],
    top_k: usize,
    nprobes: usize,
    mode: VectorSearchMode,
    brain_id: Option<&str>,
) -> crate::error::Result<Vec<QueryResult>> {
    use futures::TryStreamExt;
    use lancedb::query::{ExecutableQuery, QueryBase};

    let mut builder = table
        .vector_search(embedding)
        .map_err(|e| BrainCoreError::VectorDb(format!("search setup failed: {e}")))?
        .distance_type(lancedb::DistanceType::Dot)
        .nprobes(nprobes)
        .limit(top_k);

    if let Some(bid) = brain_id {
        let bid = validate_filter_value(bid)?;
        builder = builder.only_if(format!("brain_id = '{bid}'"));
    }

    match mode {
        VectorSearchMode::Exact => {
            builder = builder.bypass_vector_index();
        }
        VectorSearchMode::AnnRefined => {
            builder = builder.refine_factor(DEFAULT_REFINE_FACTOR);
        }
        VectorSearchMode::AnnFast => {
            // Use ANN defaults — no additional configuration.
        }
    }

    let results = builder
        .execute()
        .await
        .map_err(|e| BrainCoreError::VectorDb(format!("search failed: {e}")))?;

    let batches: Vec<RecordBatch> = results
        .try_collect()
        .await
        .map_err(|e| BrainCoreError::VectorDb(format!("result collection failed: {e}")))?;

    let mut output = Vec::new();
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
        let brain_ids = batch
            .column_by_name("brain_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| BrainCoreError::VectorDb("missing brain_id column".into()))?;
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
                brain_id: brain_ids.value(i).to_string(),
                chunk_ord: chunk_ords.value(i) as usize,
                content: contents.value(i).to_string(),
                score: distances.map(|d| d.value(i)),
            });
        }
    }

    Ok(output)
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
        Field::new("brain_id", DataType::Utf8, false),
    ])
}

fn empty_record_batch(schema: &Schema) -> crate::error::Result<RecordBatch> {
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
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .map_err(|e| BrainCoreError::VectorDb(format!("failed to build empty record batch: {e}")))
}

/// Validate that a value is safe to interpolate into a LanceDB filter expression.
///
/// Accepted character classes and their origins:
/// - alphanumeric (`A-Z a-z 0-9`): ULID (Crockford Base32) and nanoid SAFE alphabet
/// - `-`: separator in ULID Crockford encoding and nanoid SAFE alphabet
/// - `:`: capsule prefix delimiter (e.g. `task:`, `sum:`)
/// - `_`: nanoid SAFE alphabet
///
/// The sequence `--` is explicitly rejected as a SQL comment guard even
/// though `-` is individually allowed. Empty strings are accepted for
/// legacy/test scenarios where brain_id may be unset.
///
/// # Contract
///
/// This validator is sound only for `=` equality predicates and `IN (...)`
/// lists. Do NOT feed validated values into `LIKE` patterns — `_` becomes a
/// single-char wildcard there. If a future call site needs `LIKE`, escape `_`
/// and `%` first, or extend the validator with a parallel `validate_for_like`
/// variant.
fn validate_filter_value(value: &str) -> crate::error::Result<&str> {
    // Reject SQL comment sequence `--` even though `-` is individually allowed.
    if value.contains("--") {
        return Err(BrainCoreError::VectorDb(format!(
            "invalid value for LanceDB filter: {value}"
        )));
    }
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == ':' || c == '_')
    {
        Ok(value)
    } else {
        Err(BrainCoreError::VectorDb(format!(
            "invalid value for LanceDB filter: {value}"
        )))
    }
}

fn make_record_batch(
    schema: &Schema,
    file_id: &str,
    file_path: &str,
    brain_id: &str,
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
    let brain_ids: Vec<&str> = vec![brain_id; chunks.len()];

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
            Arc::new(StringArray::from(brain_ids)),
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
        let empty_batch = empty_record_batch(&schema).unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(empty_batch)], Arc::new(schema));
        let table = db
            .create_table("chunks", Box::new(batches))
            .execute()
            .await
            .unwrap();
        let scheduler = OptimizeScheduler::new(Arc::new(table), row_threshold, time_threshold);
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
    async fn test_force_optimize_runs_when_zero_pending() {
        // Regression test: `force_optimize` must actually invoke
        // `OptimizeAction::All` on the underlying table even when the
        // in-memory `pending_mutations` counter is 0. The counter resets on
        // every process restart, so the most common caller (`brain vacuum`,
        // which always runs as a fresh CLI invocation) starts with pending=0
        // — gating compaction on the counter was a real bug that allowed
        // historical fragment debt to accumulate to tens of GB.
        let (sched, _tmp) = test_scheduler(10, Duration::from_secs(300)).await;
        assert_eq!(sched.pending_count(), 0);

        // Should reach `table.optimize` and complete cleanly. Pre-fix this
        // was an early-return; post-fix the call exercises the LanceDB
        // optimize path.
        sched.force_optimize().await;

        // Counter is unchanged on a zero-pending scheduler — `fetch_sub(0)`
        // is a no-op. The compaction itself is the side-effect we care about.
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

    #[test]
    fn test_lance_schema_version_is_positive() {
        const _: () = assert!(LANCE_SCHEMA_VERSION >= 1);
    }

    #[tokio::test]
    async fn test_drop_and_recreate_table() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut store = Store::open_or_create(tmp.path()).await.unwrap();

        // Insert a chunk so the table is non-empty
        let embedding = vec![0.0f32; EMBEDDING_DIM as usize];
        store
            .upsert_chunks(
                "file-1",
                "/test.md",
                "test-brain",
                &[(0, "hello world")],
                &[embedding],
            )
            .await
            .unwrap();

        // Verify data exists
        let ids = store.get_file_ids_with_chunks("test-brain").await.unwrap();
        assert!(ids.contains("file-1"));

        // Rebuild
        store.drop_and_recreate_table().await.unwrap();

        // Table should be empty but functional
        let ids = store.get_file_ids_with_chunks("test-brain").await.unwrap();
        assert!(ids.is_empty(), "table should be empty after rebuild");

        // Schema should match expected
        assert!(store.current_schema_matches_expected().await);

        // Should be able to insert again
        let embedding = vec![0.0f32; EMBEDDING_DIM as usize];
        store
            .upsert_chunks(
                "file-2",
                "/test2.md",
                "test-brain",
                &[(0, "new data")],
                &[embedding],
            )
            .await
            .unwrap();
        let ids = store.get_file_ids_with_chunks("test-brain").await.unwrap();
        assert!(ids.contains("file-2"));
    }

    #[tokio::test]
    async fn test_current_schema_matches_expected() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Store::open_or_create(tmp.path()).await.unwrap();
        assert!(store.current_schema_matches_expected().await);
    }

    // ─── VectorSearchMode tests ──────────────────────────────────────

    #[test]
    fn vector_search_mode_display_roundtrip() {
        for mode in [
            VectorSearchMode::Exact,
            VectorSearchMode::AnnRefined,
            VectorSearchMode::AnnFast,
        ] {
            let s = mode.to_string();
            let parsed: VectorSearchMode = s.parse().unwrap();
            assert_eq!(mode, parsed, "round-trip failed for {s}");
        }
    }

    #[test]
    fn vector_search_mode_from_str_rejects_unknown() {
        let result = "turbo".parse::<VectorSearchMode>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown vector_search_mode"));
    }

    #[test]
    fn vector_search_mode_default_is_ann_refined() {
        assert_eq!(VectorSearchMode::default(), VectorSearchMode::AnnRefined);
    }

    // ─── validate_filter_value tests ─────────────────────────────────────────

    #[test]
    fn validate_filter_value_accepts_lowercase() {
        assert!(super::validate_filter_value("abcdef").is_ok());
    }

    #[test]
    fn validate_filter_value_accepts_uppercase() {
        assert!(super::validate_filter_value("ABCDEF").is_ok());
    }

    #[test]
    fn validate_filter_value_accepts_digits() {
        assert!(super::validate_filter_value("0123456789").is_ok());
    }

    #[test]
    fn validate_filter_value_accepts_hyphen() {
        assert!(super::validate_filter_value("BRN-01ABC").is_ok());
    }

    #[test]
    fn validate_filter_value_accepts_colon() {
        assert!(super::validate_filter_value("task:BRN-01ABC").is_ok());
    }

    #[test]
    fn validate_filter_value_accepts_underscore() {
        assert!(super::validate_filter_value("brain_id").is_ok());
    }

    #[test]
    fn validate_filter_value_rejects_tilde() {
        // nanoid 0.4 SAFE alphabet is A-Z a-z 0-9 _ - and never produces `~`.
        assert!(super::validate_filter_value("brain~id").is_err());
    }

    #[test]
    fn validate_filter_value_accepts_mixed_with_underscore() {
        // Realistic nanoid SAFE-alphabet brain ID containing `_`
        assert!(super::validate_filter_value("a1_B2Cd").is_ok());
    }

    #[test]
    fn validate_filter_value_accepts_empty_string() {
        assert!(super::validate_filter_value("").is_ok());
    }

    #[test]
    fn validate_filter_value_rejects_space() {
        assert!(super::validate_filter_value("brain id").is_err());
    }

    #[test]
    fn validate_filter_value_rejects_single_quote() {
        assert!(super::validate_filter_value("brain'id").is_err());
    }

    #[test]
    fn validate_filter_value_rejects_semicolon() {
        assert!(super::validate_filter_value("brain;id").is_err());
    }

    #[test]
    fn validate_filter_value_rejects_sql_comment() {
        // `--` is the SQL comment sequence; reject even though `-` is individually allowed.
        assert!(super::validate_filter_value("--").is_err());
        assert!(super::validate_filter_value("brain--id").is_err());
    }

    #[test]
    fn validate_filter_value_no_only_if_like_usage() {
        // Regression guard: validate_filter_value is sound only for = and IN
        // predicates. `_` is a single-char wildcard in SQL LIKE patterns.
        // Assert no production .only_if(...) call site in this file uses LIKE.
        let src = include_str!("./store.rs");
        let only_if_calls: Vec<&str> = src
            .lines()
            .filter(|line| {
                let trimmed = line.trim_start();
                // Skip comment lines — only check production code.
                !trimmed.starts_with("//") && line.contains(".only_if(")
            })
            .collect();
        for line in &only_if_calls {
            assert!(
                !line.to_uppercase().contains("LIKE"),
                "only_if call site uses LIKE — validate_filter_value is not safe here: {line}"
            );
        }
    }
}
