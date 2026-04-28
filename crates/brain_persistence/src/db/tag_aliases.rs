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

/// Encode an L2-normalized embedding as a little-endian f32 byte run for
/// the `tag_aliases.embedding` BLOB column.
///
/// No prior code in the workspace stores f32 vectors in SQLite (chunks
/// and summaries push to LanceDB), so this codec is local to this module.
/// We deliberately avoid `bytemuck::cast_slice` to keep the dependency
/// tree small — the loop has no `unsafe` and matches native endianness on
/// darwin x86_64/aarch64.
#[allow(dead_code)] // wired by the Tx-2 writer in subsequent commits within `brn-83a.7.2.3`
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
