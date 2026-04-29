//! Storage layer for synonym-clustering: `tag_aliases` and
//! `tag_cluster_runs` tables (v43 schema).
//!
//! Read-side helpers consumed by `brain_lib::tags::recluster`
//! (`brn-83a.7.2.3`) via the `TagAliasReader` port trait.

use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::db::tags::RawTag;
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

/// Fold a `Vec<RawTag>` (one row per `(tag, source)` pair) into a
/// `Vec<DedupedRawTag>` keyed by tag string: sum reference counts, take
/// max of last-seen timestamps. Clustering is a property of the tag
/// string, not the source.
pub fn dedupe_by_tag(raw_tags: Vec<RawTag>) -> Vec<DedupedRawTag> {
    let mut folded: HashMap<String, DedupedRawTag> = HashMap::new();
    for raw in raw_tags {
        folded
            .entry(raw.tag.clone())
            .and_modify(|t| {
                t.total_reference_count += raw.reference_count;
                t.last_seen_at = t.last_seen_at.max(raw.last_seen_at);
            })
            .or_insert(DedupedRawTag {
                tag: raw.tag,
                total_reference_count: raw.reference_count,
                last_seen_at: raw.last_seen_at,
            });
    }
    folded.into_values().collect()
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
    pub brain_id: String,
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
    pub brain_id: String,
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

/// Per-brain snapshot of `tag_aliases` for a given `brain_id`.
///
/// Filters by `brain_id` so the diff phase compares only this brain's
/// existing aliases against its freshly-collected raw tags. Cross-brain
/// rows stay in place and are invisible to the caller.
pub fn read_alias_snapshot(
    conn: &Connection,
    brain_id: &str,
) -> Result<HashMap<String, ExistingAlias>> {
    let mut stmt = conn.prepare(
        "SELECT raw_tag, canonical_tag, cluster_id, last_run_id,
                embedding, embedder_version, updated_at
         FROM tag_aliases
         WHERE brain_id = ?1",
    )?;
    let mut rows = stmt.query(params![brain_id])?;

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

/// Per-brain `(raw_tag → canonical_tag)` projection for read-time alias
/// expansion in the query path (`brn-83a.7.2.4`).
///
/// Returns the **lowercased** projection (both keys and values). Original
/// casing is not preserved by design — see plan `brn-83a.7.2.4`
/// "Write-side normalization": the filter block in `query_pipeline.rs`
/// already lowercases candidate tags, so the read-side lookup must match.
/// A future case-sensitive caller adds a peer `alias_lookup_raw_for_brain`.
///
/// Returns an empty map for brains that have never been reclustered (no
/// rows in `tag_aliases` for `brain_id`) — that case must remain
/// bit-identical to today's literal-only behavior.
pub fn alias_lookup_for_brain(
    conn: &Connection,
    brain_id: &str,
) -> Result<HashMap<String, String>> {
    let started = std::time::Instant::now();
    let mut stmt = conn.prepare(
        "SELECT raw_tag, canonical_tag
         FROM tag_aliases
         WHERE brain_id = ?1",
    )?;
    let mut rows = stmt.query(params![brain_id])?;

    let mut out = HashMap::new();
    while let Some(row) = rows.next()? {
        let raw_tag: String = row.get(0)?;
        let canonical_tag: String = row.get(1)?;
        out.insert(raw_tag.to_lowercase(), canonical_tag.to_lowercase());
    }

    tracing::debug!(
        target: "brain_persistence::tag_aliases",
        brain_id = brain_id,
        row_count = out.len(),
        elapsed_us = started.elapsed().as_micros() as u64,
        "alias_lookup_for_brain"
    );

    Ok(out)
}

/// Tx-1: insert a `tag_cluster_runs` row with `finished_at = NULL` so the
/// `tag_aliases.last_run_id` FK can resolve when Tx-2 runs.
pub fn insert_run(conn: &Connection, input: &InsertRun) -> Result<()> {
    conn.execute(
        "INSERT INTO tag_cluster_runs
             (run_id, brain_id, started_at, finished_at, source_count, cluster_count,
              embedder_version, threshold, triggered_by, notes)
         VALUES (?1, ?2, ?3, NULL, NULL, NULL, ?4, ?5, ?6, NULL)",
        params![
            input.run_id,
            input.brain_id,
            input.started_at_iso,
            input.embedder_version,
            input.threshold,
            input.triggered_by,
        ],
    )?;
    Ok(())
}

/// Tx-2: atomic UPSERT of all `tag_aliases` rows for the given run, DELETE
/// of stale rows scoped to `brain_id`, plus finalization of the matching
/// `tag_cluster_runs` row.
///
/// All three happen inside a single rusqlite transaction so the alias state
/// and the audit-row finalization commit atomically. Rolls back via `Drop`
/// if any prepared-statement execution returns `Err`.
///
/// `stale` is the set of `raw_tag` values present in this brain's snapshot
/// but absent from the brain's freshly-collected raw-tag set. They are
/// DELETEd inside the same transaction so `tag_aliases` stays a faithful
/// projection of the current canonical-pick.
pub fn apply_alias_upserts(
    conn: &Connection,
    brain_id: &str,
    upserts: &[AliasUpsert],
    stale: &[String],
    finalize: &FinalizeRun,
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO tag_aliases
                 (brain_id, raw_tag, canonical_tag, cluster_id, last_run_id,
                  embedding, embedder_version, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(brain_id, raw_tag) DO UPDATE SET
                 canonical_tag    = excluded.canonical_tag,
                 cluster_id       = excluded.cluster_id,
                 last_run_id      = excluded.last_run_id,
                 embedding        = excluded.embedding,
                 embedder_version = excluded.embedder_version,
                 updated_at       = excluded.updated_at",
        )?;
        for u in upserts {
            stmt.execute(params![
                u.brain_id,
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
    {
        let mut del = tx.prepare("DELETE FROM tag_aliases WHERE brain_id = ?1 AND raw_tag = ?2")?;
        for tag in stale {
            del.execute(params![brain_id, tag])?;
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
// Inspection helpers (consumed by both tests and the production MCP/CLI
// surface — see `brn-83a.7.2.5`).
// ---------------------------------------------------------------------------

/// Inspect a single `tag_cluster_runs` row by `run_id`. Returns `None` if
/// the run is absent (e.g. Tx-1 never committed).
#[derive(Debug, Clone, serde::Serialize)]
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

/// Most recent `tag_cluster_runs` row for a brain, ordered by `started_at`
/// DESC. Returns `None` for brains that have never been reclustered.
pub fn latest_run_for_brain(conn: &Connection, brain_id: &str) -> Result<Option<TagClusterRunRow>> {
    let mut stmt = conn.prepare(
        "SELECT run_id, started_at, finished_at, source_count, cluster_count,
                embedder_version, threshold, triggered_by, notes
         FROM tag_cluster_runs
         WHERE brain_id = ?1
         ORDER BY started_at DESC
         LIMIT 1",
    )?;
    let mut rows = stmt.query(params![brain_id])?;
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

/// One `tag_aliases` row for inspection (no embedding BLOB). Used by the
/// `tags.aliases_list` MCP tool and `brain tags aliases list` CLI.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AliasRow {
    pub raw_tag: String,
    pub canonical_tag: String,
    pub cluster_id: String,
    pub last_run_id: String,
    pub embedder_version: Option<String>,
    pub updated_at: String,
}

/// List `tag_aliases` rows for a brain with optional `canonical_tag` and
/// `cluster_id` filters, ordered by `(canonical_tag, raw_tag)` for stable
/// pagination. Returns at most `limit` rows starting at `offset`.
pub fn list_aliases_for_brain(
    conn: &Connection,
    brain_id: &str,
    canonical: Option<&str>,
    cluster_id: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<AliasRow>> {
    let mut sql = String::from(
        "SELECT raw_tag, canonical_tag, cluster_id, last_run_id,
                embedder_version, updated_at
         FROM tag_aliases
         WHERE brain_id = ?1",
    );
    if canonical.is_some() {
        sql.push_str(" AND canonical_tag = ?2");
    }
    if cluster_id.is_some() {
        // Bind index depends on whether `canonical` is present. Compute below.
        if canonical.is_some() {
            sql.push_str(" AND cluster_id = ?3");
        } else {
            sql.push_str(" AND cluster_id = ?2");
        }
    }
    sql.push_str(" ORDER BY canonical_tag, raw_tag LIMIT ? OFFSET ?");

    let mut stmt = conn.prepare(&sql)?;
    let mut binds: Vec<&dyn rusqlite::ToSql> = vec![&brain_id];
    if let Some(c) = canonical.as_ref() {
        binds.push(c);
    }
    if let Some(cl) = cluster_id.as_ref() {
        binds.push(cl);
    }
    binds.push(&limit);
    binds.push(&offset);

    let mut rows = stmt.query(binds.as_slice())?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(AliasRow {
            raw_tag: row.get(0)?,
            canonical_tag: row.get(1)?,
            cluster_id: row.get(2)?,
            last_run_id: row.get(3)?,
            embedder_version: row.get(4)?,
            updated_at: row.get(5)?,
        });
    }
    Ok(out)
}

/// Per-brain count summary used by `tags.aliases_status`.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct AliasCounts {
    /// Total `tag_aliases` rows for the brain (one per raw tag).
    pub raw_count: i64,
    /// Distinct `canonical_tag` values across the brain's aliases.
    pub canonical_count: i64,
    /// Distinct `cluster_id` values across the brain's aliases.
    pub cluster_count: i64,
}

/// Aggregate counts over `tag_aliases` for a brain. Returns zeros for
/// brains that have never been reclustered.
pub fn count_aliases_for_brain(conn: &Connection, brain_id: &str) -> Result<AliasCounts> {
    let raw_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tag_aliases WHERE brain_id = ?1",
        params![brain_id],
        |row| row.get(0),
    )?;
    let canonical_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT canonical_tag) FROM tag_aliases WHERE brain_id = ?1",
        params![brain_id],
        |row| row.get(0),
    )?;
    let cluster_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT cluster_id) FROM tag_aliases WHERE brain_id = ?1",
        params![brain_id],
        |row| row.get(0),
    )?;
    Ok(AliasCounts {
        raw_count,
        canonical_count,
        cluster_count,
    })
}

// ---------------------------------------------------------------------------
// Test/integration helpers (gated behind `test-utils` for brain_lib tests)
// ---------------------------------------------------------------------------

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
/// [`crate::db::tags::collect_raw_tags`]: `record_id`, `brain_id`,
/// `updated_at`, plus the FK target columns set to safe defaults.
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

/// Seed `tag_aliases` rows for a single brain in tests. Inserts a stub
/// `tag_cluster_runs` row first to satisfy the `last_run_id` FK target,
/// then bulk-inserts the supplied `(raw_tag, canonical_tag, cluster_id)`
/// triples. Reuses the same synthetic run row across all triples so
/// callers can compose multi-cluster fixtures with a single call.
///
/// Used by the query-path alias-expansion tests in `brn-83a.7.2.4`. Not
/// used by production code paths.
#[cfg(any(test, feature = "test-utils"))]
pub fn seed_tag_aliases(
    conn: &Connection,
    brain_id: &str,
    rows: &[(&str, &str, &str)],
) -> Result<()> {
    let run_id = format!("test-run-{brain_id}");
    let updated_at = "1970-01-01T00:00:00Z";
    conn.execute(
        "INSERT OR IGNORE INTO tag_cluster_runs
             (run_id, brain_id, started_at, finished_at, source_count, cluster_count,
              embedder_version, threshold, triggered_by, notes)
         VALUES (?1, ?2, ?3, ?3, 0, 0, 'test-embedder-v1', 0.85, 'test', NULL)",
        params![run_id, brain_id, updated_at],
    )?;
    for (raw_tag, canonical_tag, cluster_id) in rows {
        conn.execute(
            "INSERT INTO tag_aliases
                 (brain_id, raw_tag, canonical_tag, cluster_id, last_run_id,
                  embedding, embedder_version, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, 'test-embedder-v1', ?6)",
            params![
                brain_id,
                raw_tag,
                canonical_tag,
                cluster_id,
                run_id,
                updated_at
            ],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

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

    #[test]
    fn alias_lookup_empty_table_returns_empty() {
        let db = Db::open_in_memory().unwrap();
        let out = db
            .with_read_conn(|conn| alias_lookup_for_brain(conn, "brain-a"))
            .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn alias_lookup_lowercases_mixed_case_rows() {
        let db = Db::open_in_memory().unwrap();
        db.with_write_conn(|conn| {
            seed_tag_aliases(conn, "brain-a", &[("Bug", "Bugs", "c1")])?;
            Ok(())
        })
        .unwrap();
        let out = db
            .with_read_conn(|conn| alias_lookup_for_brain(conn, "brain-a"))
            .unwrap();
        assert_eq!(out.get("bug").map(String::as_str), Some("bugs"));
        // Original-case key must NOT be present — see "Write-side normalization"
        // in plan brn-83a.7.2.4: read-side projection is lowercase-only.
        assert!(!out.contains_key("Bug"));
    }

    #[test]
    fn alias_lookup_filters_by_brain_id() {
        let db = Db::open_in_memory().unwrap();
        db.with_write_conn(|conn| {
            seed_tag_aliases(
                conn,
                "brain-a",
                &[("bug", "bug", "c1"), ("bugs", "bug", "c1")],
            )?;
            seed_tag_aliases(conn, "brain-b", &[("perf", "perf", "c2")])?;
            Ok(())
        })
        .unwrap();
        let a = db
            .with_read_conn(|conn| alias_lookup_for_brain(conn, "brain-a"))
            .unwrap();
        let b = db
            .with_read_conn(|conn| alias_lookup_for_brain(conn, "brain-b"))
            .unwrap();
        assert_eq!(a.len(), 2);
        assert!(a.contains_key("bug"));
        assert!(a.contains_key("bugs"));
        assert!(!a.contains_key("perf"));
        assert_eq!(b.len(), 1);
        assert!(b.contains_key("perf"));
    }
}
