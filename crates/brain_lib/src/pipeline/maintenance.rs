use std::path::{Path, PathBuf};

use tracing::{info, instrument, warn};

use crate::db::files;
use crate::doctor::{CheckStatus, DoctorReport};
use crate::ports::{ChunkIndexWriter, FileMetaReader, FileMetaWriter, SchemaMeta};
use crate::scanner::scan_brain;

use super::{IndexPipeline, VacuumStats};

impl<S> IndexPipeline<S>
where
    S: ChunkIndexWriter + SchemaMeta + Send + Sync,
{
    /// Delete a file from the index (soft-delete in SQLite, hard-delete in LanceDB).
    pub async fn delete_file(&self, path: &Path) -> crate::error::Result<bool> {
        let path_str = path.to_string_lossy().to_string();

        let file_id = self.db.handle_delete(&path_str)?;
        if let Some(ref fid) = file_id {
            self.store.delete_file_chunks(fid).await?;
            info!(path = %path_str, "file deleted from index");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Handle a file rename (update path in SQLite and LanceDB).
    pub async fn rename_file(&self, from: &Path, to: &Path) -> crate::error::Result<bool> {
        let from_str = from.to_string_lossy().to_string();
        let to_str = to.to_string_lossy().to_string();

        let file_id = self.db.with_write_conn(|conn| {
            use rusqlite::OptionalExtension;
            let file_id: Option<String> = conn
                .query_row(
                    "SELECT file_id FROM files WHERE path = ?1 AND deleted_at IS NULL",
                    [&from_str],
                    |row| row.get(0),
                )
                .optional()?;

            if let Some(ref fid) = file_id {
                files::handle_rename(conn, fid, &to_str)?;
            }
            Ok(file_id)
        })?;

        if let Some(ref fid) = file_id {
            self.store.update_file_path(fid, &to_str).await?;
            info!(from = %from_str, to = %to_str, "file renamed in index");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Vacuum: purge old soft-deleted files, SQLite VACUUM, LanceDB optimize.
    #[instrument(skip(self))]
    pub async fn vacuum(&self, older_than_days: u32) -> crate::error::Result<VacuumStats> {
        let threshold_secs = older_than_days as i64 * 86400;
        let cutoff = crate::utils::now_ts() - threshold_secs;

        // 1. Purge soft-deleted files older than threshold
        let purged_ids = self.db.purge_deleted_files(cutoff)?;
        let purged_count = purged_ids.len();

        // 2. Delete their LanceDB chunks
        if !purged_ids.is_empty() {
            self.store.delete_chunks_by_file_ids(&purged_ids).await?;
            info!(purged = purged_count, "purged old deleted files");
        }

        // 3. SQLite VACUUM
        self.db.with_write_conn(|conn| {
            conn.execute_batch("VACUUM")?;
            Ok(())
        })?;
        info!("SQLite VACUUM complete");

        // 4. LanceDB optimize
        self.store.force_optimize().await;
        info!("LanceDB optimize complete");

        Ok(VacuumStats {
            purged_files: purged_count,
        })
    }

    /// Run diagnostic checks and return a report.
    #[instrument(skip(self))]
    pub async fn doctor(&self, dirs: &[PathBuf]) -> crate::error::Result<DoctorReport> {
        let mut report = DoctorReport::new();

        // 1. Orphan chunks in LanceDB (file_id not in SQLite)
        let lance_file_ids = self.store.get_file_ids_with_chunks().await?;
        let sqlite_file_ids: std::collections::HashSet<String> = self
            .db
            .get_all_active_paths()?
            .into_iter()
            .map(|(fid, _)| fid)
            .collect();
        let orphan_ids: Vec<&String> = lance_file_ids
            .iter()
            .filter(|fid| !sqlite_file_ids.contains(*fid))
            .collect();
        if orphan_ids.is_empty() {
            report.add(
                "Orphan chunks",
                CheckStatus::Ok,
                "no orphan chunks in LanceDB",
            );
        } else {
            report.add(
                "Orphan chunks",
                CheckStatus::Problem,
                format!(
                    "{} file_id(s) in LanceDB with no matching SQLite entry",
                    orphan_ids.len()
                ),
            );
        }

        // 2. Files in SQLite with no chunks in LanceDB
        let missing: Vec<&String> = sqlite_file_ids
            .iter()
            .filter(|fid| !lance_file_ids.contains(*fid))
            .collect();
        if missing.is_empty() {
            report.add(
                "Missing chunks",
                CheckStatus::Ok,
                "all SQLite files have LanceDB chunks",
            );
        } else {
            report.add(
                "Missing chunks",
                CheckStatus::Warning,
                format!(
                    "{} file(s) in SQLite have no chunks in LanceDB",
                    missing.len()
                ),
            );
        }

        // 3. Content hash mismatches (stored hash vs actual file on disk)
        let files_with_hashes = self.db.get_files_with_hashes()?;
        let mut hash_mismatches = 0;
        let mut hash_errors = 0;
        for (_file_id, path, stored_hash) in &files_with_hashes {
            if let Some(stored) = stored_hash {
                match std::fs::read_to_string(path) {
                    Ok(content) => {
                        let actual = crate::utils::content_hash(&content);
                        if actual != *stored {
                            hash_mismatches += 1;
                        }
                    }
                    Err(_) => {
                        hash_errors += 1;
                    }
                }
            }
        }
        if hash_mismatches == 0 && hash_errors == 0 {
            report.add(
                "Content hashes",
                CheckStatus::Ok,
                "all hashes match on-disk content",
            );
        } else {
            let mut detail = Vec::new();
            if hash_mismatches > 0 {
                detail.push(format!("{hash_mismatches} hash mismatch(es)"));
            }
            if hash_errors > 0 {
                detail.push(format!("{hash_errors} file(s) unreadable"));
            }
            report.add(
                "Content hashes",
                if hash_mismatches > 0 {
                    CheckStatus::Problem
                } else {
                    CheckStatus::Warning
                },
                detail.join(", "),
            );
        }

        // 4. FTS5 consistency (rebuild and compare counts)
        let fts_result = self.db.with_read_conn(|conn| {
            let chunk_count: i64 =
                conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
            let fts_count: i64 =
                conn.query_row("SELECT COUNT(*) FROM fts_chunks", [], |row| row.get(0))?;
            Ok((chunk_count, fts_count))
        });
        match fts_result {
            Ok((chunk_count, fts_count)) => {
                if chunk_count == fts_count {
                    report.add(
                        "FTS5 consistency",
                        CheckStatus::Ok,
                        format!("{chunk_count} chunks, {fts_count} FTS entries"),
                    );
                } else {
                    report.add(
                        "FTS5 consistency",
                        CheckStatus::Problem,
                        format!("{chunk_count} chunks vs {fts_count} FTS entries (mismatch)"),
                    );
                }
            }
            Err(e) => {
                report.add(
                    "FTS5 consistency",
                    CheckStatus::Problem,
                    format!("query error: {e}"),
                );
            }
        }

        // 5. Stuck files
        let stuck = self.db.find_stuck_files()?;
        if stuck.is_empty() {
            report.add(
                "Stuck files",
                CheckStatus::Ok,
                "no files stuck in indexing_started",
            );
        } else {
            report.add(
                "Stuck files",
                CheckStatus::Warning,
                format!("{} file(s) stuck in indexing_started state", stuck.len()),
            );
        }

        // 6. Scan disk files vs indexed files
        let dirs_owned: Vec<PathBuf> = dirs.to_vec();
        let scanned = tokio::task::spawn_blocking(move || scan_brain(&dirs_owned))
            .await
            .map_err(|e| {
                crate::error::BrainCoreError::Internal(format!("scan task panicked: {e}"))
            })?;
        let disk_count = scanned.len();
        let indexed_count = files_with_hashes
            .iter()
            .filter(|(_, _, h)| h.is_some())
            .count();
        report.add(
            "Index coverage",
            if indexed_count >= disk_count {
                CheckStatus::Ok
            } else {
                CheckStatus::Warning
            },
            format!("{indexed_count} indexed / {disk_count} on disk"),
        );

        Ok(report)
    }

    /// Repair corrupted index state: rebuild FTS5, drop and recreate LanceDB
    /// table, and clear all content hashes so the next scan re-indexes everything.
    ///
    /// Used as self-healing when a startup scan fails with database corruption.
    #[instrument(skip(self))]
    pub async fn repair(&mut self) -> crate::error::Result<()> {
        // 1. Rebuild FTS5 index from chunks table
        self.db.with_write_conn(|conn| {
            crate::db::fts::reindex_fts(conn)?;
            Ok(())
        })?;
        info!("FTS5 index rebuilt");

        // 2. Drop and recreate the LanceDB chunks table
        self.store_mut().drop_and_recreate_table().await?;
        info!("LanceDB table rebuilt");

        // 3. Clear all content hashes so every file gets re-indexed
        let cleared = self.db.clear_all_content_hashes()?;
        info!(cleared, "content hashes cleared for full re-index");

        Ok(())
    }

    /// Handle a single file event from the watcher.
    #[instrument(skip(self))]
    pub async fn handle_event(&self, event: crate::watcher::FileEvent) -> crate::error::Result<()> {
        match event {
            crate::watcher::FileEvent::Created(path) | crate::watcher::FileEvent::Changed(path) => {
                match self.index_file(&path).await {
                    Ok(true) => info!(path = %path.display(), "re-indexed"),
                    Ok(false) => info!(path = %path.display(), "unchanged, skipped"),
                    Err(e) => warn!(path = %path.display(), error = %e, "index failed"),
                }
            }
            crate::watcher::FileEvent::Deleted(path) => {
                self.delete_file(&path).await?;
            }
            crate::watcher::FileEvent::Renamed { from, to } => {
                self.rename_file(&from, &to).await?;
            }
        }
        Ok(())
    }
}
