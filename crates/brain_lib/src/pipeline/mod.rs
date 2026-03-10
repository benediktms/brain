mod indexing;
mod maintenance;
mod scan;

use std::path::Path;
use std::sync::Arc;

use tracing::{info, warn};

use crate::db::Db;
use crate::db::meta;
use crate::embedder::{Embed, Embedder};
use crate::metrics::Metrics;
use crate::store::Store;

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

/// Orchestrates Db + Store + Embedder for incremental indexing.
pub struct IndexPipeline {
    pub(crate) db: Db,
    pub(crate) store: Store,
    pub(crate) embedder: Arc<dyn Embed>,
    pub(crate) metrics: Arc<Metrics>,
}

/// Check/stamp the LanceDB schema version, rebuilding the table when needed.
///
/// Distinguishes three cases:
/// - **No stored version** (first run): just stamp, no rebuild needed.
/// - **Stored but wrong/unparseable**: full rebuild + clear content hashes + stamp.
/// - **Matches**: skip rebuild, but warn if the live schema diverges (safety net).
async fn ensure_schema_version(db: &Db, store: &mut Store) -> crate::error::Result<()> {
    let expected = crate::store::LANCE_SCHEMA_VERSION;

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
            let cleared = db.with_write_conn(crate::db::files::clear_all_content_hashes)?;
            info!(cleared, "cleared content hashes to trigger full re-index");
            db.with_write_conn(|conn| {
                meta::set_meta(conn, "lancedb_schema_version", &expected.to_string())
            })?;
        }
    }

    Ok(())
}

impl IndexPipeline {
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
        })
    }

    /// Get a reference to the SQLite database.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Get a reference to the LanceDB store.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Get a reference to the embedder.
    pub fn embedder(&self) -> &Arc<dyn Embed> {
        &self.embedder
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}
