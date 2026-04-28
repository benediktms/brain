//! Storage layer for synonym-clustering: `tag_aliases` and
//! `tag_cluster_runs` tables (v43 schema).
//!
//! Read-side helpers consumed by `brain_lib::tags::recluster`
//! (`brn-83a.7.2.3`) via the `TagAliasReader` port trait.

use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::error::{BrainCoreError, Result};

/// Per-tag fold of `RawTag` rows across the (Records, Tasks) sources for a
/// single brain. Sums reference counts and takes max of last-seen
/// timestamps because clustering is a property of the tag *string*, not
/// the source.
#[derive(Debug, Clone)]
pub struct DedupedRawTag {
    pub tag: String,
    pub total_reference_count: i64,
    pub last_seen_at: i64,
}

/// Snapshot row read from `tag_aliases` before computing new clusters.
#[derive(Debug, Clone)]
pub struct ExistingAlias {
    pub raw_tag: String,
    pub canonical_tag: String,
    pub cluster_id: String,
    pub last_run_id: String,
    /// Cached embedding for the cache-hit comparison. Decoded on read.
    pub embedding: Option<Vec<f32>>,
    pub embedder_version: Option<String>,
    pub updated_at: String,
}

/// One row of upsert intent produced by the diff phase. Mirrors the
/// schema columns of `tag_aliases` minus the audit metadata.
#[derive(Debug, Clone)]
pub struct AliasUpsert {
    pub raw_tag: String,
    pub canonical_tag: String,
    pub cluster_id: String,
    pub embedding: Vec<f32>,
    pub embedder_version: String,
}

/// Owned input for [`insert_run`]: the `tag_cluster_runs` row inserted by
/// Tx-1 with `finished_at = NULL` and `notes = NULL`.
#[derive(Debug, Clone)]
pub struct InsertRun {
    pub run_id: String,
    pub started_at_iso: String,
    pub embedder_version: String,
    pub threshold: f32,
    pub triggered_by: String,
}

/// Owned finalize-run payload consumed by [`apply_alias_upserts`] in Tx-2.
#[derive(Debug, Clone)]
pub struct FinalizeRun {
    pub run_id: String,
    pub finished_at_iso: String,
    pub source_count: i64,
    pub cluster_count: i64,
}

/// Per-brain raw-tag collector. Folds across (Records, Tasks) sources.
///
/// **Why this duplicates [`super::tags::collect_raw_tags`].** The public
/// collector is brain-unscoped: `tag_cluster_runs` and `tag_aliases` (v43)
/// have no `brain_id` column, so the read side could not be safely
/// brain-scoped without conflating brains. We add `WHERE r.brain_id = ?1`
/// / `WHERE t.brain_id = ?1` filters to the same SELECTs here as a
/// contained workaround.
///
/// **Lifecycle.** Removed when the v44 schema migration (`brn-83a.7.2.7`)
/// adds `brain_id` to both tables; at that point the public
/// `collect_raw_tags` gains its own `brain_id: Option<&str>` parameter and
/// callers switch to it. Introduced by `brn-83a.7.2.3`.
pub fn collect_raw_tags_for_brain(conn: &Connection, brain_id: &str) -> Result<Vec<DedupedRawTag>> {
    let mut folded: HashMap<String, DedupedRawTag> = HashMap::new();

    {
        let mut stmt = conn.prepare(
            "SELECT rt.tag, COUNT(*), COALESCE(MAX(r.updated_at), 0)
             FROM record_tags rt
             JOIN records r ON r.record_id = rt.record_id
             WHERE r.brain_id = ?1
             GROUP BY rt.tag",
        )?;
        let mut rows = stmt.query(params![brain_id])?;
        while let Some(row) = rows.next()? {
            let tag: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            let last_seen: i64 = row.get(2)?;
            folded
                .entry(tag.clone())
                .and_modify(|t| {
                    t.total_reference_count += count;
                    t.last_seen_at = t.last_seen_at.max(last_seen);
                })
                .or_insert(DedupedRawTag {
                    tag,
                    total_reference_count: count,
                    last_seen_at: last_seen,
                });
        }
    }

    {
        let mut stmt = conn.prepare(
            "SELECT tl.label, COUNT(*), COALESCE(MAX(t.updated_at), 0)
             FROM task_labels tl
             JOIN tasks t ON t.task_id = tl.task_id
             WHERE t.brain_id = ?1
             GROUP BY tl.label",
        )?;
        let mut rows = stmt.query(params![brain_id])?;
        while let Some(row) = rows.next()? {
            let tag: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            let last_seen: i64 = row.get(2)?;
            folded
                .entry(tag.clone())
                .and_modify(|t| {
                    t.total_reference_count += count;
                    t.last_seen_at = t.last_seen_at.max(last_seen);
                })
                .or_insert(DedupedRawTag {
                    tag,
                    total_reference_count: count,
                    last_seen_at: last_seen,
                });
        }
    }

    Ok(folded.into_values().collect())
}

/// Read every `tag_aliases` row into an in-memory map keyed by `raw_tag`.
///
/// `tag_aliases` has no `brain_id` column today, so this returns the
/// global table — callers must use the per-brain raw-tag set to bound the
/// diff (see `collect_raw_tags_for_brain`).
pub fn read_alias_snapshot(conn: &Connection) -> Result<HashMap<String, ExistingAlias>> {
    let mut stmt = conn.prepare(
        "SELECT raw_tag, canonical_tag, cluster_id, last_run_id,
                embedding, embedder_version, updated_at
         FROM tag_aliases",
    )?;
    let mut rows = stmt.query([])?;

    let mut out = HashMap::new();
    while let Some(row) = rows.next()? {
        let raw_tag: String = row.get(0)?;
        let canonical_tag: String = row.get(1)?;
        let cluster_id: String = row.get(2)?;
        let last_run_id: String = row.get(3)?;
        let embedding_blob: Option<Vec<u8>> = row.get(4)?;
        let embedder_version: Option<String> = row.get(5)?;
        let updated_at: String = row.get(6)?;

        let embedding = match embedding_blob {
            Some(bytes) => Some(decode_embedding(&bytes)?),
            None => None,
        };

        out.insert(
            raw_tag.clone(),
            ExistingAlias {
                raw_tag,
                canonical_tag,
                cluster_id,
                last_run_id,
                embedding,
                embedder_version,
                updated_at,
            },
        );
    }
    Ok(out)
}

/// Tx-1: insert a `tag_cluster_runs` row with `finished_at = NULL` so the
/// `tag_aliases.last_run_id` FK can resolve when Tx-2 runs.
pub fn insert_run(conn: &Connection, input: &InsertRun) -> Result<()> {
    conn.execute(
        "INSERT INTO tag_cluster_runs
             (run_id, started_at, finished_at, source_count, cluster_count,
              embedder_version, threshold, triggered_by, notes)
         VALUES (?1, ?2, NULL, NULL, NULL, ?3, ?4, ?5, NULL)",
        params![
            input.run_id,
            input.started_at_iso,
            input.embedder_version,
            input.threshold,
            input.triggered_by,
        ],
    )?;
    Ok(())
}

/// Tx-2: atomic UPSERT of all `tag_aliases` rows for the given run plus
/// finalization of the matching `tag_cluster_runs` row.
///
/// Both happen inside a single rusqlite transaction so the alias state and
/// the audit-row finalization commit atomically. Rolls back via `Drop` if
/// any prepared-statement execution returns `Err`.
pub fn apply_alias_upserts(
    conn: &Connection,
    upserts: &[AliasUpsert],
    finalize: &FinalizeRun,
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO tag_aliases
                 (raw_tag, canonical_tag, cluster_id, last_run_id,
                  embedding, embedder_version, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(raw_tag) DO UPDATE SET
                 canonical_tag    = excluded.canonical_tag,
                 cluster_id       = excluded.cluster_id,
                 last_run_id      = excluded.last_run_id,
                 embedding        = excluded.embedding,
                 embedder_version = excluded.embedder_version,
                 updated_at       = excluded.updated_at",
        )?;
        for u in upserts {
            stmt.execute(params![
                u.raw_tag,
                u.canonical_tag,
                u.cluster_id,
                finalize.run_id,
                encode_embedding(&u.embedding),
                u.embedder_version,
                finalize.finished_at_iso,
            ])?;
        }
    }
    tx.execute(
        "UPDATE tag_cluster_runs
         SET finished_at = ?1, source_count = ?2, cluster_count = ?3
         WHERE run_id = ?4",
        params![
            finalize.finished_at_iso,
            finalize.source_count,
            finalize.cluster_count,
            finalize.run_id,
        ],
    )?;
    tx.commit()?;
    Ok(())
}

/// Tx-3: record a failed run on the existing `tag_cluster_runs` row.
/// Called only if `run_recluster` returns `Err` after [`insert_run`]
/// committed but before/while Tx-2 was being attempted.
pub fn record_run_failure(
    conn: &Connection,
    run_id: &str,
    finished_at_iso: &str,
    notes: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE tag_cluster_runs
         SET finished_at = ?1, notes = ?2
         WHERE run_id = ?3",
        params![finished_at_iso, notes, run_id],
    )?;
    Ok(())
}

/// Encode an L2-normalized embedding as a little-endian f32 byte run for
/// the `tag_aliases.embedding` BLOB column.
///
/// No prior code in the workspace stores f32 vectors in SQLite (chunks
/// and summaries push to LanceDB), so this codec is local to this module.
/// We deliberately avoid `bytemuck::cast_slice` to keep the dependency
/// tree small — the loop has no `unsafe` and matches native endianness on
/// darwin x86_64/aarch64.
pub(crate) fn encode_embedding(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Decode a `tag_aliases.embedding` BLOB back into a `Vec<f32>`. Errors if
/// the length is not a multiple of 4 (defensive — only triggers on a row
/// hand-edited or written by a different codec).
pub(crate) fn decode_embedding(bytes: &[u8]) -> Result<Vec<f32>> {
    if !bytes.len().is_multiple_of(4) {
        return Err(BrainCoreError::Database(format!(
            "tag_aliases.embedding length {} is not a multiple of 4",
            bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(bytes.len() / 4);
    for chunk in bytes.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Test/integration helpers (gated behind `test-utils` for brain_lib tests)
// ---------------------------------------------------------------------------

/// Inspect a single `tag_cluster_runs` row by `run_id`. Returns `None` if
/// the run is absent (e.g. Tx-1 never committed).
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone)]
pub struct TagClusterRunRow {
    pub run_id: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub source_count: Option<i64>,
    pub cluster_count: Option<i64>,
    pub embedder_version: String,
    pub threshold: f32,
    pub triggered_by: String,
    pub notes: Option<String>,
}

#[cfg(any(test, feature = "test-utils"))]
pub fn get_run(conn: &Connection, run_id: &str) -> Result<Option<TagClusterRunRow>> {
    let mut stmt = conn.prepare(
        "SELECT run_id, started_at, finished_at, source_count, cluster_count,
                embedder_version, threshold, triggered_by, notes
         FROM tag_cluster_runs WHERE run_id = ?1",
    )?;
    let mut rows = stmt.query(params![run_id])?;
    if let Some(row) = rows.next()? {
        Ok(Some(TagClusterRunRow {
            run_id: row.get(0)?,
            started_at: row.get(1)?,
            finished_at: row.get(2)?,
            source_count: row.get(3)?,
            cluster_count: row.get(4)?,
            embedder_version: row.get(5)?,
            threshold: row.get(6)?,
            triggered_by: row.get(7)?,
            notes: row.get(8)?,
        }))
    } else {
        Ok(None)
    }
}

/// List every `tag_cluster_runs` row, newest first by `started_at`. Used
/// by tests that want to inspect run state without knowing the run_id
/// (e.g. the failure path).
#[cfg(any(test, feature = "test-utils"))]
pub fn list_runs(conn: &Connection) -> Result<Vec<TagClusterRunRow>> {
    let mut stmt = conn.prepare(
        "SELECT run_id, started_at, finished_at, source_count, cluster_count,
                embedder_version, threshold, triggered_by, notes
         FROM tag_cluster_runs ORDER BY started_at DESC",
    )?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(TagClusterRunRow {
            run_id: row.get(0)?,
            started_at: row.get(1)?,
            finished_at: row.get(2)?,
            source_count: row.get(3)?,
            cluster_count: row.get(4)?,
            embedder_version: row.get(5)?,
            threshold: row.get(6)?,
            triggered_by: row.get(7)?,
            notes: row.get(8)?,
        });
    }
    Ok(out)
}

/// Seed a minimal `records` row with associated `record_tags` entries for
/// integration tests. Mirrors only the columns required by
/// `collect_raw_tags_for_brain`: `record_id`, `brain_id`, `updated_at`,
/// plus the FK target columns set to safe defaults.
#[cfg(any(test, feature = "test-utils"))]
pub fn seed_record_with_tags(
    conn: &Connection,
    record_id: &str,
    brain_id: &str,
    updated_at: i64,
    tags: &[&str],
) -> Result<()> {
    conn.execute(
        "INSERT INTO records (
             record_id, title, kind, status, description, content_hash,
             content_size, media_type, task_id, actor, created_at, updated_at,
             retention_class, pinned, payload_available, content_encoding,
             original_size, brain_id, searchable, embedded_at
         ) VALUES (
             ?1, ?2, 'document', 'active', NULL, 'hash',
             4, 'text/plain', NULL, 'test-agent', ?3, ?3,
             NULL, 0, 1, 'identity',
             NULL, ?4, 1, NULL
         )",
        params![
            record_id,
            format!("record {record_id}"),
            updated_at,
            brain_id
        ],
    )?;
    for tag in tags {
        conn.execute(
            "INSERT INTO record_tags (record_id, tag) VALUES (?1, ?2)",
            params![record_id, tag],
        )?;
    }
    Ok(())
}

/// Seed a minimal `tasks` row with associated `task_labels` entries for
/// integration tests.
#[cfg(any(test, feature = "test-utils"))]
pub fn seed_task_with_labels(
    conn: &Connection,
    task_id: &str,
    brain_id: &str,
    updated_at: i64,
    labels: &[&str],
) -> Result<()> {
    conn.execute(
        "INSERT INTO tasks (task_id, title, status, priority, task_type,
                            brain_id, created_at, updated_at)
         VALUES (?1, ?2, 'open', 1, 'task', ?3, ?4, ?4)",
        params![task_id, format!("task {task_id}"), brain_id, updated_at,],
    )?;
    for label in labels {
        conn.execute(
            "INSERT INTO task_labels (task_id, label) VALUES (?1, ?2)",
            params![task_id, label],
        )?;
    }
    Ok(())
}

/// Test-only: stamp a different `embedder_version` onto every
/// `tag_aliases` row so the next `run_recluster` sees the cache as stale
/// and re-embeds. Used by the cache-invalidation test.
#[cfg(any(test, feature = "test-utils"))]
pub fn override_alias_embedder_version(conn: &Connection, version: &str) -> Result<usize> {
    conn.execute(
        "UPDATE tag_aliases SET embedder_version = ?1",
        params![version],
    )
    .map_err(BrainCoreError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let v = vec![0.0_f32, 1.0, -1.5, 1e-6, f32::EPSILON];
        let bytes = encode_embedding(&v);
        assert_eq!(bytes.len(), v.len() * 4);
        let decoded = decode_embedding(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn decode_rejects_bad_length() {
        let result = decode_embedding(&[0u8, 1, 2]);
        assert!(result.is_err(), "3-byte input should fail");
        let result = decode_embedding(&[0u8; 5]);
        assert!(result.is_err(), "5-byte input should fail");
    }

    #[test]
    fn decode_empty_yields_empty() {
        let out = decode_embedding(&[]).unwrap();
        assert!(out.is_empty());
    }
}
