//! Periodic poll loops that embed stale tasks and chunks into LanceDB.
//!
//! Called from the daemon watch loop on a 10-second interval. Each poll cycle
//! processes up to 256 items to prevent memory spikes on the first run after
//! `embedded_at` is introduced.

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::db::Db;
use crate::embedder::{Embed, embed_batch_async};
use crate::store::Store;
use crate::tasks::capsule::{build_outcome_capsule, build_task_capsule, store_task_capsule};
use crate::tasks::queries::get_labels_for_tasks;

// ── Tasks ───────────────────────────────────────────────────────────────────

/// Poll for tasks whose capsule is stale (updated_at > embedded_at or embedded_at IS NULL).
///
/// Builds task + outcome capsules, batch-embeds them, upserts to LanceDB and
/// SQLite FTS, then marks the tasks as embedded.
///
/// `brain_id` — when non-empty, filters tasks to this brain only; when empty,
/// processes all tasks (used by the single-brain `run()` path).
///
/// Returns the number of tasks successfully embedded.
///
/// `db` must be the database containing the `tasks` table — the unified DB
/// (`~/.brain/brain.db`) in multi-brain mode, or the per-brain DB in
/// single-brain mode.
pub async fn poll_stale_tasks(
    db: &Db,
    store: &Store,
    embedder: &Arc<dyn Embed>,
    brain_id: &str,
) -> usize {
    debug!("embed_poll: scanning stale tasks");

    // ── 1. Fetch stale task rows ─────────────────────────────────────────
    #[derive(Debug)]
    struct TaskPollRow {
        task_id: String,
        title: String,
        description: Option<String>,
        status: String,
        priority: i32,
        blocked_reason: Option<String>,
    }

    let rows: Vec<TaskPollRow> = match db.with_read_conn(|conn| {
        // Build query dynamically: filter by brain_id only when non-empty.
        let (sql, has_brain_filter) = if brain_id.is_empty() {
            (
                "SELECT task_id, title, description, status, priority, blocked_reason
                 FROM tasks
                 WHERE (updated_at > COALESCE(embedded_at, 0) OR embedded_at IS NULL)
                 LIMIT 256"
                    .to_string(),
                false,
            )
        } else {
            (
                "SELECT task_id, title, description, status, priority, blocked_reason
                 FROM tasks
                 WHERE (updated_at > COALESCE(embedded_at, 0) OR embedded_at IS NULL)
                   AND brain_id = ?1
                 LIMIT 256"
                    .to_string(),
                true,
            )
        };

        let mut stmt = conn.prepare(&sql)?;

        let map_row = |row: &rusqlite::Row<'_>| {
            Ok(TaskPollRow {
                task_id: row.get(0)?,
                title: row.get(1)?,
                description: row.get(2)?,
                status: row.get(3)?,
                priority: row.get(4)?,
                blocked_reason: row.get(5)?,
            })
        };

        let rows = if has_brain_filter {
            stmt.query_map([brain_id], map_row)?
        } else {
            stmt.query_map([], map_row)?
        };
        crate::db::collect_rows(rows)
    }) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to query stale tasks");
            return 0;
        }
    };

    if rows.is_empty() {
        debug!("embed_poll: no stale tasks");
        return 0;
    }

    info!(count = rows.len(), "embed_poll: embedding stale tasks");

    // ── 2. Fetch labels for each task ────────────────────────────────────
    let task_id_refs: Vec<&str> = rows.iter().map(|r| r.task_id.as_str()).collect();

    let label_map: std::collections::HashMap<String, Vec<String>> =
        match db.with_read_conn(|conn| get_labels_for_tasks(conn, &task_id_refs)) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "embed_poll: failed to fetch labels for stale tasks");
                std::collections::HashMap::new()
            }
        };

    // ── 3. Build capsule texts ────────────────────────────────────────────
    struct CapsuleEntry {
        task_id: String,
        title: String,
        file_id: String,
        capsule_text: String,
    }

    let mut capsules: Vec<CapsuleEntry> = Vec::new();

    for row in &rows {
        let labels = label_map.get(&row.task_id).cloned().unwrap_or_default();
        let capsule_text = build_task_capsule(
            &row.title,
            row.description.as_deref(),
            &labels,
            row.priority,
        );
        let file_id = format!("task:{}", row.task_id);
        capsules.push(CapsuleEntry {
            task_id: row.task_id.clone(),
            title: row.title.clone(),
            file_id,
            capsule_text,
        });

        // Outcome capsule for terminal tasks
        let status = row.status.as_str();
        if status == "done" || status == "cancelled" {
            let reason = if status == "cancelled" {
                row.blocked_reason.as_deref()
            } else {
                None
            };
            let outcome_text = build_outcome_capsule(&row.title, reason);
            let outcome_file_id = format!("task-outcome:{}", row.task_id);
            capsules.push(CapsuleEntry {
                task_id: row.task_id.clone(),
                title: row.title.clone(),
                file_id: outcome_file_id,
                capsule_text: outcome_text,
            });
        }
    }

    // ── 4. Batch embed ────────────────────────────────────────────────────
    let texts: Vec<String> = capsules.iter().map(|c| c.capsule_text.clone()).collect();

    let embeddings = match embed_batch_async(embedder, texts).await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to embed task capsules");
            return 0;
        }
    };

    // ── 5. Upsert to LanceDB + SQLite FTS ────────────────────────────────
    let mut embedded_task_ids: HashSet<String> = HashSet::new();

    for (entry, embedding) in capsules.iter().zip(embeddings.iter()) {
        // LanceDB upsert
        if let Err(e) = store
            .upsert_chunks(
                &entry.file_id,
                &entry.title,
                &[(0, entry.capsule_text.as_str())],
                std::slice::from_ref(embedding),
            )
            .await
        {
            warn!(
                task_id = %entry.task_id,
                file_id = %entry.file_id,
                error = %e,
                "embed_poll: LanceDB upsert failed for task capsule"
            );
            continue;
        }

        // SQLite FTS upsert
        if let Err(e) = store_task_capsule(db, &entry.file_id, &entry.capsule_text) {
            warn!(
                task_id = %entry.task_id,
                file_id = %entry.file_id,
                error = %e,
                "embed_poll: SQLite FTS upsert failed for task capsule"
            );
            continue;
        }

        // Track unique task IDs (task + outcome capsule both count once)
        embedded_task_ids.insert(entry.task_id.clone());
    }

    // ── 6. Mark embedded ─────────────────────────────────────────────────
    if !embedded_task_ids.is_empty() {
        let ids_ref: Vec<&str> = embedded_task_ids.iter().map(|s| s.as_str()).collect();
        if let Err(e) = db.with_write_conn(|conn| mark_tasks_embedded(conn, &ids_ref)) {
            warn!(error = %e, "embed_poll: failed to mark tasks as embedded");
        }
    }

    let count = embedded_task_ids.len();
    info!(count, "embed_poll: tasks embedded");
    count
}

/// Set `embedded_at = now()` on a batch of tasks.
///
/// `task_ids` must be non-empty. Skips gracefully if the slice is empty.
fn mark_tasks_embedded(conn: &rusqlite::Connection, task_ids: &[&str]) -> crate::error::Result<()> {
    if task_ids.is_empty() {
        return Ok(());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let placeholders: Vec<String> = (2..=task_ids.len() + 1).map(|i| format!("?{i}")).collect();
    let sql = format!(
        "UPDATE tasks SET embedded_at = ?1 WHERE task_id IN ({})",
        placeholders.join(", ")
    );
    let mut params: Vec<&dyn rusqlite::types::ToSql> = Vec::with_capacity(task_ids.len() + 1);
    let ts_ref: &dyn rusqlite::types::ToSql = &now;
    params.push(ts_ref);
    for id in task_ids {
        params.push(id as &dyn rusqlite::types::ToSql);
    }
    conn.execute(&sql, params.as_slice())?;
    Ok(())
}

// ── Chunks ──────────────────────────────────────────────────────────────────

/// Poll for file chunks that have not yet been embedded into LanceDB.
///
/// Batch-embeds up to 256 chunks, upserts to LanceDB, then marks them
/// as embedded in SQLite.
///
/// Returns the number of chunks successfully embedded.
pub async fn poll_stale_chunks(db: &Db, store: &Store, embedder: &Arc<dyn Embed>) -> usize {
    debug!("embed_poll: scanning stale chunks");

    #[derive(Debug)]
    struct ChunkPollRow {
        chunk_id: String,
        file_id: String,
        file_path: String,
        chunk_ord: i32,
        content: String,
    }

    let rows: Vec<ChunkPollRow> = match db.with_read_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT c.chunk_id, c.file_id, COALESCE(f.path, c.file_id), c.chunk_ord, c.content
             FROM chunks c
             LEFT JOIN files f ON f.file_id = c.file_id
             WHERE c.embedded_at IS NULL
             LIMIT 256",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ChunkPollRow {
                chunk_id: row.get(0)?,
                file_id: row.get(1)?,
                file_path: row.get(2)?,
                chunk_ord: row.get(3)?,
                content: row.get(4)?,
            })
        })?;
        crate::db::collect_rows(rows)
    }) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to query stale chunks");
            return 0;
        }
    };

    if rows.is_empty() {
        debug!("embed_poll: no stale chunks");
        return 0;
    }

    info!(count = rows.len(), "embed_poll: embedding stale chunks");

    // ── Batch embed ───────────────────────────────────────────────────────
    let texts: Vec<String> = rows.iter().map(|r| r.content.clone()).collect();
    let embeddings = match embed_batch_async(embedder, texts).await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "embed_poll: failed to embed chunks");
            return 0;
        }
    };

    // ── Group by file_id for upsert ───────────────────────────────────────
    // LanceDB upsert works per-file. We group chunks, upsert each file's
    // chunks as a batch, then mark all chunk_ids as embedded.
    use std::collections::HashMap;

    struct FileGroup<'a> {
        file_path: &'a str,
        chunks: Vec<(usize, &'a str)>,
        embeddings: Vec<Vec<f32>>,
        chunk_ids: Vec<&'a str>,
    }

    let mut file_groups: HashMap<&str, FileGroup<'_>> = HashMap::new();

    for (row, embedding) in rows.iter().zip(embeddings.iter()) {
        let group = file_groups
            .entry(row.file_id.as_str())
            .or_insert_with(|| FileGroup {
                file_path: row.file_path.as_str(),
                chunks: Vec::new(),
                embeddings: Vec::new(),
                chunk_ids: Vec::new(),
            });
        group
            .chunks
            .push((row.chunk_ord as usize, row.content.as_str()));
        group.embeddings.push(embedding.clone());
        group.chunk_ids.push(row.chunk_id.as_str());
    }

    // ── Upsert each file group ────────────────────────────────────────────
    let mut embedded_chunk_ids: Vec<&str> = Vec::new();

    for (file_id, group) in &file_groups {
        if let Err(e) = store
            .upsert_chunks(file_id, group.file_path, &group.chunks, &group.embeddings)
            .await
        {
            warn!(
                file_id,
                error = %e,
                "embed_poll: LanceDB upsert failed for file chunks"
            );
            continue;
        }
        embedded_chunk_ids.extend_from_slice(&group.chunk_ids);
    }

    // ── Mark embedded ─────────────────────────────────────────────────────
    if !embedded_chunk_ids.is_empty() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if let Err(e) = db.with_write_conn(|conn| {
            crate::db::chunks::mark_chunks_embedded(conn, &embedded_chunk_ids, now)
        }) {
            warn!(error = %e, "embed_poll: failed to mark chunks as embedded");
        }
    }

    let count = embedded_chunk_ids.len();
    info!(count, "embed_poll: chunks embedded");
    count
}

// ── Self-heal ────────────────────────────────────────────────────────────────

/// Check if LanceDB is accessible. If not, reset all `embedded_at` columns so
/// items will be re-embedded on the next poll cycle.
///
/// `db` is the per-brain database (contains `chunks`).
/// `unified_db` is the unified database (contains `tasks`).
/// In single-brain mode, both point to the same database.
///
/// Returns `true` if a reset occurred (LanceDB was missing/inaccessible).
pub async fn self_heal_if_lance_missing(db: &Db, unified_db: &Db, store: &Store) -> bool {
    // Use schema() as a lightweight accessibility probe — it sends a trivial
    // request to the underlying table handle.
    if store.current_schema_matches_expected().await {
        return false;
    }

    warn!("LanceDB not found — resetting embedded_at for full re-embed");

    if let Err(e) = unified_db.with_write_conn(|conn| {
        conn.execute_batch("UPDATE tasks SET embedded_at = NULL;")?;
        Ok(())
    }) {
        warn!(error = %e, "embed_poll: failed to reset tasks.embedded_at");
    }

    if let Err(e) = db.with_write_conn(|conn| {
        conn.execute_batch("UPDATE chunks SET embedded_at = NULL;")?;
        Ok(())
    }) {
        warn!(error = %e, "embed_poll: failed to reset chunks.embedded_at");
    }

    true
}
