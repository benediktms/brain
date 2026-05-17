//! Domain types for the tags crate.
//!
//! Each type below mirrors a `brain_persistence::db::tag_aliases::*Row`
//! shape but is owned by this crate so callers depend on the tag-domain
//! vocabulary rather than the SQL projection. Conversion impls
//! (`From<...Row>`) sit at the persistence boundary — the only place where
//! row types are mentioned by name.
//!
//! All timestamps are RFC 3339 strings (matching what the persistence
//! layer emits) so the wire format produced by `#[derive(Serialize)]`
//! stays byte-identical to the row types these wrap.

use serde::Serialize;

use brain_persistence::db::tag_aliases::{AliasCounts, AliasRow, TagClusterRunRow};

/// One alias row for the calling brain.
///
/// Reading-only projection of `tag_aliases` — the embedding BLOB is
/// intentionally absent so the wire payload stays small.
#[derive(Debug, Clone, Serialize)]
pub struct TagAlias {
    pub raw_tag: String,
    pub canonical_tag: String,
    pub cluster_id: String,
    pub last_run_id: String,
    pub embedder_version: Option<String>,
    pub updated_at: String,
}

impl From<AliasRow> for TagAlias {
    fn from(row: AliasRow) -> Self {
        Self {
            raw_tag: row.raw_tag,
            canonical_tag: row.canonical_tag,
            cluster_id: row.cluster_id,
            last_run_id: row.last_run_id,
            embedder_version: row.embedder_version,
            updated_at: row.updated_at,
        }
    }
}

/// One clustering-run row for the calling brain.
///
/// Mirrors `tag_cluster_runs`. `finished_at` is `None` for an in-flight
/// run; `notes` carries the failure summary when Tx-3 records an error.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterRun {
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

impl From<TagClusterRunRow> for ClusterRun {
    fn from(row: TagClusterRunRow) -> Self {
        Self {
            run_id: row.run_id,
            started_at: row.started_at,
            finished_at: row.finished_at,
            source_count: row.source_count,
            cluster_count: row.cluster_count,
            embedder_version: row.embedder_version,
            threshold: row.threshold,
            triggered_by: row.triggered_by,
            notes: row.notes,
        }
    }
}

/// Aggregate alias coverage for the calling brain.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct AliasCoverage {
    /// Total `tag_aliases` rows for the brain (one per raw tag).
    pub raw_count: i64,
    /// Distinct `canonical_tag` values across the brain's aliases.
    pub canonical_count: i64,
    /// Distinct `cluster_id` values across the brain's aliases.
    pub cluster_count: i64,
}

impl From<AliasCounts> for AliasCoverage {
    fn from(counts: AliasCounts) -> Self {
        Self {
            raw_count: counts.raw_count,
            canonical_count: counts.canonical_count,
            cluster_count: counts.cluster_count,
        }
    }
}
