use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tracing::{info, instrument, warn};

use crate::chunker::CHUNKER_VERSION;
use crate::ports::{ChunkIndexWriter, FileMetaReader, FileMetaWriter, SchemaMeta};
use crate::scanner::scan_brain;

use super::IndexPipeline;

impl<S> IndexPipeline<S>
where
    S: ChunkIndexWriter + SchemaMeta + Send + Sync,
{
    /// Full scan: index all files, recover stuck states.
    #[instrument(skip(self))]
    pub async fn full_scan(&self, dirs: &[PathBuf]) -> crate::error::Result<super::ScanStats> {
        let start = std::time::Instant::now();
        let mut stats = super::ScanStats::default();

        // 1. Recover stuck files (crash recovery)
        let stuck = self.db.find_stuck_files()?;
        if !stuck.is_empty() {
            info!(
                count = stuck.len(),
                "recovering stuck files from previous crash"
            );
        }
        for (file_id, path) in &stuck {
            self.db.reset_stuck_file_for_reindex(file_id)?;
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
        // 3. Check for stale chunker versions
        let stale_count = self.db.count_stale_chunker_version(CHUNKER_VERSION)?;
        if stale_count > 0 {
            warn!(
                count = stale_count,
                "chunker version changed, file(s) have stale chunker version"
            );
        }

        // 4. Batch-index all scanned files (hash gate skips unchanged)
        let paths: Vec<PathBuf> = scanned_files.iter().map(|f| f.path.clone()).collect();
        let batch_stats = self.index_files_batch(&paths).await?;
        stats.indexed += batch_stats.indexed;
        stats.skipped += batch_stats.skipped;
        stats.errors += batch_stats.errors;

        // 5. Soft-delete DB files not found in scan results.
        // This catches files that were previously indexed but are now excluded
        // by .gitignore, deleted from disk, or outside the configured roots.
        let scanned_set: HashSet<String> = scanned_files
            .iter()
            .map(|f| f.path.to_string_lossy().to_string())
            .collect();

        let active_db_files = self.db.get_all_active_paths()?;
        for (file_id, db_path) in &active_db_files {
            if !scanned_set.contains(db_path.as_str()) {
                if let Err(e) = self.db.handle_delete(db_path) {
                    warn!(path = %db_path, error = %e, "failed to soft-delete stale file");
                } else {
                    // Also remove from LanceDB immediately
                    if let Err(e) = self.store.delete_file_chunks(file_id).await {
                        warn!(file_id = %file_id, error = %e, "failed to delete LanceDB chunks for stale file");
                    }
                    stats.deleted += 1;
                }
            }
        }

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
        let cleared = self.db.clear_all_content_hashes()?;
        info!(cleared, "cleared all content hashes for full reindex");

        let stats = self.full_scan(dirs).await?;
        self.store.force_optimize().await;

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
        let found = self.db.clear_content_hash_by_path(&path_str)?;
        if !found {
            info!(path = %path_str, "file not in index, indexing fresh");
        }
        self.index_file(path).await
    }
}
