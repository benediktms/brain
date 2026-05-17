//! Domain types for the retrieval crate.
//!
//! Each type below mirrors a `brain_persistence::*Row` shape but is owned by
//! this crate so callers depend on the retrieval-domain vocabulary rather
//! than the SQL projection. Conversion impls (`From<...Row>`) sit at the
//! persistence boundary — the only place where row types are mentioned by
//! name.

use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;

use brain_persistence::db::summaries::SummaryRow;

/// One reflected episode emitted by `memory.reflect`.
///
/// Wraps `SummaryRow` so the SQL row type does not cross the public API
/// boundary. Field names mirror `SummaryRow` byte-identical *except* for
/// the timestamps: `SummaryRow` stores `i64` Unix epochs, but the wire
/// contract for MCP / CLI is RFC 3339 strings per project convention.
/// Epochs are converted here at the boundary.
#[derive(Debug, Clone, Serialize)]
pub struct ReflectedEpisode {
    pub summary_id: String,
    pub brain_id: String,
    pub kind: String,
    pub title: Option<String>,
    pub content: String,
    pub tags: Vec<String>,
    pub importance: f64,
    pub created_at: String,
    pub updated_at: String,
    pub parent_id: Option<String>,
    pub source_hash: Option<String>,
    pub confidence: f64,
    pub valid_from: Option<String>,
}

impl From<SummaryRow> for ReflectedEpisode {
    fn from(row: SummaryRow) -> Self {
        Self {
            summary_id: row.summary_id,
            brain_id: row.brain_id,
            kind: row.kind,
            title: row.title,
            content: row.content,
            tags: row.tags,
            importance: row.importance,
            created_at: epoch_to_rfc3339(row.created_at),
            updated_at: epoch_to_rfc3339(row.updated_at),
            parent_id: row.parent_id,
            source_hash: row.source_hash,
            confidence: row.confidence,
            valid_from: row.valid_from.map(epoch_to_rfc3339),
        }
    }
}

fn epoch_to_rfc3339(secs: i64) -> String {
    Utc.timestamp_opt(secs, 0)
        .single()
        .map(|dt: DateTime<Utc>| dt.to_rfc3339())
        .unwrap_or_default()
}
