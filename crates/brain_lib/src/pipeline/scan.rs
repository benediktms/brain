use std::path::{Path, PathBuf};

use tracing::{info, instrument, warn};

use crate::chunker::CHUNKER_VERSION;
use crate::db::files;
use crate::scanner::scan_brain;

use super::IndexPipeline;

impl IndexPipeline {
    /// Full scan: index all files, detect deletions, recover stuck states.
    #[instrument(skip(self))]
    pub async fn full_scan(&self, dirs: &[PathBuf]) -> crate::error::Result<super::ScanStats> {
        let start = std::time::Instant::now();
        let mut stats = super::ScanStats::default();

        // 1. Recover stuck files (crash recovery)
        let stuck = self.db.with_write_conn(files::find_stuck_files)?;
        if !stuck.is_empty() {
            info!(
                count = stuck.len(),
                "recovering stuck files from previous crash"
            );
        }
        for (file_id, path) in &stuck {
            self.db.with_write_conn(|conn| {
                files::set_indexing_state(conn, file_id, "idle")?;
                conn.execute(
                    "UPDATE files SET content_hash = NULL WHERE file_id = ?1",
                    [file_id],
                )?;
                Ok(())
            })?;
            info!(path, "reset stuck file for re-indexing");
            stats.stuck_recovered += 1;
        }

        // 2. Scan disk for all markdown files
        let dirs_owned: Vec<PathBuf> = dirs.to_vec();
        let scanned_files = tokio::task::spawn_blocking(move || scan_brain(&dirs_owned))
            .await
            .map_err(|e| {
                crate::error::BrainCoreError::Internal(format!("scan task panicked: {e}"))
            })?;
        let disk_paths: std::collections::HashSet<String> = scanned_files
            .iter()
            .map(|f| f.path.to_string_lossy().to_string())
            .collect();

        // 3. Detect deletions (files in DB but not on disk)
        info!("checking for stale files in DB");
        let active_paths = self.db.with_write_conn(files::get_all_active_paths)?;
        info!(
            active_in_db = active_paths.len(),
            on_disk = disk_paths.len(),
            "deletion check"
        );
        for (file_id, db_path) in &active_paths {
            if !disk_paths.contains(db_path.as_str()) {
                self.db
                    .with_write_conn(|conn| files::handle_delete(conn, db_path))?;
                self.store.delete_file_chunks(file_id).await?;
                info!(path = %db_path, "deleted stale file from index");
                stats.deleted += 1;
            }
        }

        // 4. Check for stale chunker versions
        let stale_count = self
            .db
            .with_read_conn(|conn| files::count_stale_chunker_version(conn, CHUNKER_VERSION))?;
        if stale_count > 0 {
            warn!(
                count = stale_count,
                "chunker version changed, file(s) have stale chunker version"
            );
        }

        // 5. Batch-index all scanned files (hash gate skips unchanged)
        let paths: Vec<PathBuf> = scanned_files.iter().map(|f| f.path.clone()).collect();
        let batch_stats = self.index_files_batch(&paths).await?;
        stats.indexed += batch_stats.indexed;
        stats.skipped += batch_stats.skipped;
        stats.errors += batch_stats.errors;

        let elapsed = start.elapsed();
        info!(
            indexed = stats.indexed,
            skipped = stats.skipped,
            deleted = stats.deleted,
            errors = stats.errors,
            stuck_recovered = stats.stuck_recovered,
            elapsed_ms = elapsed.as_millis(),
            "full scan complete"
        );

        Ok(stats)
    }

    /// Force re-index all files: clear all content hashes, then run full_scan + optimize.
    #[instrument(skip(self))]
    pub async fn reindex_full(&self, dirs: &[PathBuf]) -> crate::error::Result<super::ScanStats> {
        let cleared = self.db.with_write_conn(files::clear_all_content_hashes)?;
        info!(cleared, "cleared all content hashes for full reindex");

        let stats = self.full_scan(dirs).await?;
        self.store.optimizer().force_optimize().await;

        info!(
            indexed = stats.indexed,
            skipped = stats.skipped,
            deleted = stats.deleted,
            errors = stats.errors,
            "full reindex complete"
        );
        Ok(stats)
    }

    /// Re-index a single file: clear its content hash, then re-index it.
    #[instrument(skip(self))]
    pub async fn reindex_file(&self, path: &Path) -> crate::error::Result<bool> {
        let path_str = path.to_string_lossy().to_string();
        let found = self
            .db
            .with_write_conn(|conn| files::clear_content_hash_by_path(conn, &path_str))?;
        if !found {
            info!(path = %path_str, "file not in index, indexing fresh");
        }
        self.index_file(path).await
    }
}
