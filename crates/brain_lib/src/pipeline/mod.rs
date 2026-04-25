pub mod embed_poll;
mod indexing;
pub mod job_worker;
mod maintenance;
pub mod recurring_jobs;
mod scan;

use std::path::Path;
use std::sync::Arc;

use tracing::{info, warn};

use crate::embedder::{Embed, Embedder};
use crate::metrics::Metrics;
use crate::ports::{ChunkIndexWriter, SchemaMeta};
use crate::summarizer::Summarize;
use brain_persistence::db::Db;
use brain_persistence::db::meta;
use brain_persistence::store::Store;

/// Statistics from a full scan operation.
#[derive(Debug, Default)]
pub struct ScanStats {
    pub indexed: usize,
    pub skipped: usize,
    pub deleted: usize,
    pub errors: usize,
    pub stuck_recovered: usize,
}

/// Statistics from a vacuum operation.
#[derive(Debug, Default)]
pub struct VacuumStats {
    pub purged_files: usize,
}

/// Orchestrates Db + LanceDB store + Embedder for incremental indexing.
///
/// The `S` type parameter is the LanceDB store implementation, which must
/// implement [`ChunkIndexWriter`] and [`SchemaMeta`]. It defaults to
/// [`Store`] for production use. Tests may substitute any type that
/// implements these traits without opening real LanceDB storage.
///
/// The `db` field retains the concrete [`Db`] type because the sub-modules
/// call raw SQLite helpers (`with_read_conn` / `with_write_conn`) that are
/// not (yet) covered by a persistence port trait.
pub struct IndexPipeline<S = Store>
where
    S: ChunkIndexWriter + SchemaMeta + Send + Sync,
{
    /// SQLite database — retains concrete type for raw SQL helpers.
    pub(crate) db: Db,
    /// LanceDB store — abstracted via port traits; defaults to [`Store`].
    pub(crate) store: S,
    pub(crate) embedder: Arc<dyn Embed>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) summarizer: Option<Arc<dyn Summarize>>,
    /// Brain ID used to stamp files and chunks created during indexing.
    pub(crate) brain_id: String,
}

/// Check/stamp the LanceDB schema version, rebuilding the table when needed.
///
/// Distinguishes three cases:
/// - **No stored version** (first run): just stamp, no rebuild needed.
/// - **Stored but wrong/unparseable**: full rebuild + clear content hashes + stamp.
/// - **Matches**: skip rebuild, but warn if the live schema diverges (safety net).
pub async fn ensure_schema_version(
    db: &Db,
    store: &mut impl SchemaMeta,
) -> crate::error::Result<()> {
    let expected = brain_persistence::store::LANCE_SCHEMA_VERSION;

    let raw: Option<String> =
        db.with_read_conn(|conn| meta::get_meta(conn, "lancedb_schema_version"))?;
    let parsed: Option<u32> =
        db.with_read_conn(|conn| meta::get_meta_u32(conn, "lancedb_schema_version"))?;

    match (raw.is_some(), parsed) {
        // First run — no key at all. Stamp without rebuilding.
        (false, _) => {
            info!("First run: stamping LanceDB schema version {expected}");
            db.with_write_conn(|conn| {
                meta::set_meta(conn, "lancedb_schema_version", &expected.to_string())
            })?;
        }
        // Key exists and parses to the expected version — all good.
        (true, Some(v)) if v == expected => {
            if !store.current_schema_matches_expected().await {
                warn!("LanceDB schema version matches but actual table schema differs");
            }
        }
        // Key exists but is wrong (either unparseable or different version).
        (true, other) => {
            match other {
                None => {
                    let raw_val = raw.as_deref().unwrap_or("?");
                    warn!(
                        raw_value = raw_val,
                        expected, "LanceDB schema version is unparseable, rebuilding table"
                    );
                }
                Some(v) => {
                    info!(
                        stored = v,
                        expected, "LanceDB schema version changed, rebuilding table"
                    );
                }
            }
            store.drop_and_recreate_table().await?;
            let cleared =
                db.with_write_conn(brain_persistence::db::files::clear_all_content_hashes)?;
            info!(cleared, "cleared content hashes to trigger full re-index");
            db.with_write_conn(|conn| {
                meta::set_meta(conn, "lancedb_schema_version", &expected.to_string())
            })?;
        }
    }

    Ok(())
}

impl IndexPipeline<Store> {
    /// Create a new pipeline, opening SQLite, LanceDB, and loading the embedder.
    pub async fn new(
        model_dir: &Path,
        lance_path: &Path,
        sqlite_path: &Path,
    ) -> crate::error::Result<Self> {
        let db = tokio::task::spawn_blocking({
            let sqlite_path = sqlite_path.to_path_buf();
            move || Db::open(&sqlite_path)
        })
        .await
        .map_err(|e| crate::error::BrainCoreError::Database(format!("spawn_blocking: {e}")))??;

        let mut store = Store::open_or_create(lance_path).await?;
        ensure_schema_version(&db, &mut store).await?;
        // Attach the SQLite DB so PageRank is recomputed after each optimize cycle.
        store.set_db(Arc::new(db.clone()));

        let embedder = {
            let model_dir = model_dir.to_path_buf();
            tokio::task::spawn_blocking(move || Embedder::load(&model_dir))
                .await
                .map_err(|e| {
                    crate::error::BrainCoreError::Embedding(format!("spawn_blocking: {e}"))
                })??
        };

        Ok(Self {
            db,
            store,
            embedder: Arc::new(embedder),
            metrics: Arc::new(Metrics::new()),
            summarizer: None,
            brain_id: String::new(),
        })
    }

    /// Create a pipeline with a custom embedder (for testing with MockEmbedder).
    ///
    /// Also performs the schema version check — rebuilds the table on version
    /// mismatch, or just stamps the version on first run.
    pub async fn with_embedder(
        db: Db,
        mut store: Store,
        embedder: Arc<dyn Embed>,
    ) -> crate::error::Result<Self> {
        ensure_schema_version(&db, &mut store).await?;

        Ok(Self {
            db,
            store,
            embedder,
            metrics: Arc::new(Metrics::new()),
            summarizer: None,
            brain_id: String::new(),
        })
    }
}

impl<S> IndexPipeline<S>
where
    S: ChunkIndexWriter + SchemaMeta + Send + Sync,
{
    /// Create a pipeline with a custom store implementing the persistence port
    /// traits. The schema version check is performed on `store`.
    ///
    /// Use this constructor to inject test doubles without opening real storage.
    pub async fn with_store(
        db: Db,
        mut store: S,
        embedder: Arc<dyn Embed>,
    ) -> crate::error::Result<Self> {
        ensure_schema_version(&db, &mut store).await?;

        Ok(Self {
            db,
            store,
            embedder,
            metrics: Arc::new(Metrics::new()),
            summarizer: None,
            brain_id: String::new(),
        })
    }

    /// Set the brain_id on an existing pipeline.
    pub fn set_brain_id(&mut self, brain_id: String) {
        self.brain_id = brain_id;
    }

    /// Test-only accessor for the underlying `Db` handle. Available only when
    /// the `test-utils` feature is enabled. Production code must use the
    /// inherent delegation methods (e.g. `wal_checkpoint`, `gc_completed_jobs`)
    /// or pass the pipeline through port traits.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn db_for_tests(&self) -> &Db {
        &self.db
    }

    /// Clone the underlying `Db` handle. Used for spawning tasks that need
    /// owned access to the database (e.g. `process_jobs`).
    pub fn clone_db(&self) -> Db {
        self.db.clone()
    }

    /// Get a reference to the LanceDB store (as the concrete type `S`).
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Get a mutable reference to the LanceDB store.
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Get a reference to the embedder.
    pub fn embedder(&self) -> &Arc<dyn Embed> {
        &self.embedder
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Set the summarizer on an existing pipeline.
    pub fn set_summarizer(&mut self, summarizer: Arc<dyn Summarize>) {
        self.summarizer = Some(summarizer);
    }

    /// Get a reference to the optional summarizer.
    pub fn summarizer(&self) -> Option<&Arc<dyn Summarize>> {
        self.summarizer.as_ref()
    }

    pub fn wal_checkpoint(&self) -> crate::error::Result<()> {
        self.db.wal_checkpoint()
    }

    pub fn reindex_summaries_fts(&self) -> crate::error::Result<usize> {
        use crate::ports::MaintenanceOps;
        MaintenanceOps::reindex_summaries_fts(&self.db)
    }

    pub fn gc_completed_jobs(
        &self,
        age_secs: i64,
        protected_kinds: &[&str],
    ) -> crate::error::Result<usize> {
        use crate::ports::JobQueue;
        JobQueue::gc_completed_jobs(&self.db, age_secs, protected_kinds)
    }

    pub fn find_stuck_files(&self) -> crate::error::Result<Vec<(String, String)>> {
        self.db
            .with_read_conn(brain_persistence::db::files::find_stuck_files)
    }

    /// View the underlying `Db` as a `&dyn JobQueue`. Use to pass the
    /// pipeline's database to functions that take a JobQueue trait object.
    pub fn job_queue(&self) -> &dyn crate::ports::JobQueue {
        &self.db
    }

    /// View the underlying `Db` as a `&dyn ProviderStore`.
    pub fn provider_store(&self) -> &dyn crate::ports::ProviderStore {
        &self.db
    }

    /// View the underlying `Db` as a `&dyn EmbeddingResetter`.
    pub fn embedding_resetter(&self) -> &dyn crate::ports::EmbeddingResetter {
        &self.db
    }
}
