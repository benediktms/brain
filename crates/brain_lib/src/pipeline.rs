use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{info, instrument, warn};

use crate::chunker::{CHUNKER_VERSION, Chunk, chunk_document};
use crate::metrics::Metrics;
use rusqlite::OptionalExtension;

use crate::db::Db;
use crate::db::chunks::{ChunkMeta, replace_chunk_metadata};
use crate::db::files;
use crate::db::links::replace_links;
use crate::doctor::{CheckStatus, DoctorReport};
use crate::embedder::{Embed, Embedder};
use crate::hash_gate::HashGate;
use crate::links::{Link, extract_links};
use crate::parser::parse_document;
use crate::scanner::scan_brain;
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

/// Max chunks to accumulate before flushing an embedding wave.
/// Caps memory at ~few MB of text + embeddings during large imports.
const MAX_PENDING_CHUNKS: usize = 256;

/// File that passed hash gate and is waiting for batch embedding.
struct PendingFile {
    file_id: String,
    path_str: String,
    hash: String,
    chunks: Vec<Chunk>,
    links: Vec<Link>,
}

/// Orchestrates Db + Store + Embedder for incremental indexing.
pub struct IndexPipeline {
    db: Db,
    store: Store,
    embedder: Arc<dyn Embed>,
    metrics: Arc<Metrics>,
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

        let store = Store::open_or_create(lance_path).await?;

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
    pub fn with_embedder(db: Db, store: Store, embedder: Arc<dyn Embed>) -> Self {
        Self {
            db,
            store,
            embedder,
            metrics: Arc::new(Metrics::new()),
        }
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

    /// Index a single file. Returns true if it was actually re-indexed (not skipped).
    #[instrument(skip(self))]
    pub async fn index_file(&self, path: &Path) -> crate::error::Result<bool> {
        let start = std::time::Instant::now();
        let content = tokio::fs::read_to_string(path).await?;
        let path_str = path.to_string_lossy().to_string();

        let gate = HashGate::new(&self.db);
        let verdict = gate.check(&path_str, &content)?;
        if !verdict.should_index {
            self.metrics.record_stale_hash_prevented();
            return Ok(false);
        }

        gate.mark_in_progress(&verdict.file_id)?;

        // Parse → Chunk → Extract links
        let doc = parse_document(&content);
        let chunks = chunk_document(&doc);
        let links = extract_links(&content);

        if chunks.is_empty() {
            // Empty file — clear any existing chunks and links
            self.store.delete_file_chunks(&verdict.file_id).await?;
            self.db.with_write_conn(|conn| {
                replace_chunk_metadata(conn, &verdict.file_id, &[])?;
                replace_links(conn, &verdict.file_id, &[])?;
                Ok(())
            })?;
            gate.mark_passed(&verdict.file_id, &verdict.hash)?;
            return Ok(true);
        }

        // Embed (in blocking task since it's CPU-intensive)
        let texts_owned: Vec<String> = chunks.iter().map(|c| c.content.clone()).collect();
        let embeddings = crate::embedder::embed_batch_async(&self.embedder, texts_owned).await?;

        // SQLite: replace chunk metadata + links
        let chunk_metas: Vec<ChunkMeta> = chunks
            .iter()
            .map(|c| ChunkMeta {
                chunk_id: format!("{}:{}", verdict.file_id, c.ord),
                chunk_ord: c.ord,
                chunk_hash: c.chunk_hash.clone(),
                chunker_version: CHUNKER_VERSION,
                content: c.content.clone(),
                heading_path: c.heading_path.clone(),
                byte_start: c.byte_start,
                byte_end: c.byte_end,
                token_estimate: c.token_estimate,
            })
            .collect();
        self.db.with_write_conn(|conn| {
            replace_chunk_metadata(conn, &verdict.file_id, &chunk_metas)?;
            replace_links(conn, &verdict.file_id, &links)?;
            Ok(())
        })?;

        // LanceDB: upsert
        let chunk_pairs: Vec<(usize, &str)> =
            chunks.iter().map(|c| (c.ord, c.content.as_str())).collect();
        self.store
            .upsert_chunks(&verdict.file_id, &path_str, &chunk_pairs, &embeddings)
            .await?;

        // Mark indexed (sets hash + state=indexed)
        gate.mark_passed(&verdict.file_id, &verdict.hash)?;

        self.metrics.record_index_latency(start.elapsed());
        Ok(true)
    }

    /// Batch-index multiple files. Groups chunks across files and flushes
    /// in waves when the pending chunk count exceeds MAX_PENDING_CHUNKS.
    #[instrument(skip(self))]
    pub async fn index_files_batch(&self, paths: &[PathBuf]) -> crate::error::Result<ScanStats> {
        let mut stats = ScanStats::default();
        let mut pending: Vec<PendingFile> = Vec::new();
        let mut total_chunks: usize = 0;

        let gate = HashGate::new(&self.db);

        for path in paths {
            let content = match tokio::fs::read_to_string(path).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "failed to read file in batch");
                    self.metrics
                        .indexing_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    stats.errors += 1;
                    continue;
                }
            };

            let path_str = path.to_string_lossy().to_string();

            let verdict = match gate.check(&path_str, &content) {
                Ok(v) => v,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "hash gate error in batch");
                    self.metrics
                        .indexing_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    stats.errors += 1;
                    continue;
                }
            };

            if !verdict.should_index {
                self.metrics.record_stale_hash_prevented();
                stats.skipped += 1;
                continue;
            }

            gate.mark_in_progress(&verdict.file_id)?;

            let doc = parse_document(&content);
            let chunks = chunk_document(&doc);
            let links = extract_links(&content);

            // Handle empty files immediately (no embedding needed)
            if chunks.is_empty() {
                self.store.delete_file_chunks(&verdict.file_id).await?;
                self.db.with_write_conn(|conn| {
                    replace_chunk_metadata(conn, &verdict.file_id, &[])?;
                    replace_links(conn, &verdict.file_id, &[])?;
                    Ok(())
                })?;
                gate.mark_passed(&verdict.file_id, &verdict.hash)?;
                stats.indexed += 1;
                continue;
            }

            total_chunks += chunks.len();
            pending.push(PendingFile {
                file_id: verdict.file_id,
                path_str,
                hash: verdict.hash,
                chunks,
                links,
            });

            if total_chunks >= MAX_PENDING_CHUNKS {
                info!(
                    files = pending.len(),
                    chunks = total_chunks,
                    "flushing embedding wave"
                );
                self.flush_wave(&mut pending, &mut stats).await?;
                total_chunks = 0;
            }
        }

        if !pending.is_empty() {
            info!(
                files = pending.len(),
                chunks = total_chunks,
                "flushing final embedding wave"
            );
            self.flush_wave(&mut pending, &mut stats).await?;
        }

        info!(
            indexed = stats.indexed,
            skipped = stats.skipped,
            errors = stats.errors,
            "batch indexed"
        );

        Ok(stats)
    }

    /// Flush a wave of pending files: embed all chunks in one batch call,
    /// then redistribute embeddings and upsert per-file.
    #[instrument(skip_all)]
    async fn flush_wave(
        &self,
        pending: &mut Vec<PendingFile>,
        stats: &mut ScanStats,
    ) -> crate::error::Result<()> {
        if pending.is_empty() {
            return Ok(());
        }

        let mut all_texts: Vec<String> = Vec::new();
        let mut offsets: Vec<(usize, usize)> = Vec::new();
        for pf in pending.iter() {
            let start = all_texts.len();
            all_texts.extend(pf.chunks.iter().map(|c| c.content.clone()));
            offsets.push((start, pf.chunks.len()));
        }

        info!(chunk_count = all_texts.len(), "embedding wave…");
        let all_embeddings = crate::embedder::embed_batch_async(&self.embedder, all_texts).await?;
        info!("embedding wave complete");

        let gate = HashGate::new(&self.db);
        let drained: Vec<PendingFile> = std::mem::take(pending);

        for (pf, &(offset_start, chunk_count)) in drained.iter().zip(offsets.iter()) {
            let file_start = std::time::Instant::now();
            let file_embeddings = &all_embeddings[offset_start..offset_start + chunk_count];

            let chunk_metas: Vec<ChunkMeta> = pf
                .chunks
                .iter()
                .map(|c| ChunkMeta {
                    chunk_id: format!("{}:{}", pf.file_id, c.ord),
                    chunk_ord: c.ord,
                    chunk_hash: c.chunk_hash.clone(),
                    chunker_version: CHUNKER_VERSION,
                    content: c.content.clone(),
                    heading_path: c.heading_path.clone(),
                    byte_start: c.byte_start,
                    byte_end: c.byte_end,
                    token_estimate: c.token_estimate,
                })
                .collect();

            let file_id = &pf.file_id;
            let path_str = &pf.path_str;
            let links = &pf.links;

            match async {
                self.db.with_write_conn(|conn| {
                    replace_chunk_metadata(conn, file_id, &chunk_metas)?;
                    replace_links(conn, file_id, links)?;
                    Ok(())
                })?;

                let chunk_pairs: Vec<(usize, &str)> = pf
                    .chunks
                    .iter()
                    .map(|c| (c.ord, c.content.as_str()))
                    .collect();
                self.store
                    .upsert_chunks(file_id, path_str, &chunk_pairs, file_embeddings)
                    .await?;

                gate.mark_passed(file_id, &pf.hash)?;
                Ok::<(), crate::error::BrainCoreError>(())
            }
            .await
            {
                Ok(()) => {
                    self.metrics.record_index_latency(file_start.elapsed());
                    stats.indexed += 1;
                }
                Err(e) => {
                    warn!(
                        path = %pf.path_str,
                        error = %e,
                        "failed to upsert file in batch"
                    );
                    self.metrics
                        .indexing_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    stats.errors += 1;
                }
            }
        }

        Ok(())
    }

    /// Delete a file from the index (soft-delete in SQLite, hard-delete in LanceDB).
    pub async fn delete_file(&self, path: &Path) -> crate::error::Result<bool> {
        let path_str = path.to_string_lossy().to_string();

        let file_id = self
            .db
            .with_write_conn(|conn| files::handle_delete(conn, &path_str))?;
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

    /// Full scan: index all files, detect deletions, recover stuck states.
    #[instrument(skip(self))]
    pub async fn full_scan(&self, dirs: &[PathBuf]) -> crate::error::Result<ScanStats> {
        let start = std::time::Instant::now();
        let mut stats = ScanStats::default();

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

        // 4. Batch-index all scanned files (hash gate skips unchanged)
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
    pub async fn reindex_full(&self, dirs: &[PathBuf]) -> crate::error::Result<ScanStats> {
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

    /// Vacuum: purge old soft-deleted files, SQLite VACUUM, LanceDB optimize.
    #[instrument(skip(self))]
    pub async fn vacuum(&self, older_than_days: u32) -> crate::error::Result<VacuumStats> {
        let threshold_secs = older_than_days as i64 * 86400;
        let cutoff = crate::utils::now_ts() - threshold_secs;

        // 1. Purge soft-deleted files older than threshold
        let purged_ids = self
            .db
            .with_write_conn(|conn| files::purge_deleted_files(conn, cutoff))?;
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
        self.store.optimizer().force_optimize().await;
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
        let sqlite_file_ids: std::collections::HashSet<String> =
            self.db.with_read_conn(|conn| {
                let pairs = files::get_all_active_paths(conn)?;
                Ok(pairs.into_iter().map(|(fid, _)| fid).collect())
            })?;
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
        let files_with_hashes = self.db.with_read_conn(files::get_files_with_hashes)?;
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
        let stuck = self.db.with_read_conn(files::find_stuck_files)?;
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
