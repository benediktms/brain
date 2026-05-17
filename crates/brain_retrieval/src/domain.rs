//! Domain types for the retrieval crate.
//!
//! Each type below mirrors a `brain_persistence::*Row` shape but is owned by
//! this crate so callers depend on the retrieval-domain vocabulary rather
//! than the SQL projection. Conversion impls (`From<...Row>`) sit at the
//! persistence boundary — the only place where row types are mentioned by
//! name.

use chrono::{TimeZone, Utc};
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
            // Required RFC 3339 fields: fall back to the epoch-zero sentinel
            // (a valid timestamp consumers can still parse) so an out-of-range
            // i64 in `SummaryRow` never poisons the wire with an invalid
            // RFC 3339 string. The warn log inside `epoch_to_rfc3339` surfaces
            // the corruption to observability.
            created_at: epoch_to_rfc3339(row.created_at).unwrap_or_else(epoch_zero_rfc3339),
            updated_at: epoch_to_rfc3339(row.updated_at).unwrap_or_else(epoch_zero_rfc3339),
            parent_id: row.parent_id,
            source_hash: row.source_hash,
            confidence: row.confidence,
            // Optional field: `and_then` collapses an out-of-range epoch to
            // `None` rather than `Some("")`, preserving the contract that
            // `Some(_)` means a parseable timestamp.
            valid_from: row.valid_from.and_then(epoch_to_rfc3339),
        }
    }
}

fn epoch_to_rfc3339(secs: i64) -> Option<String> {
    // `timestamp_opt` returns `None` only for i64 values well outside any
    // realistic Unix timestamp (e.g., u64::MAX cast to i64). On the rare
    // chance such a value reaches us, log it so corrupted SummaryRow data
    // surfaces in observability instead of becoming a silently invalid field.
    match Utc.timestamp_opt(secs, 0).single() {
        Some(dt) => Some(dt.to_rfc3339()),
        None => {
            tracing::warn!(secs, "epoch_to_rfc3339: timestamp out of range");
            None
        }
    }
}

fn epoch_zero_rfc3339() -> String {
    // Sentinel for corrupted-but-required timestamp fields. Always a valid
    // RFC 3339 string (1970-01-01T00:00:00+00:00) so consumers can parse it
    // without special-casing; the upstream `tracing::warn!` is the signal
    // that the underlying epoch was out of range.
    Utc.timestamp_opt(0, 0)
        .single()
        .expect("epoch 0 is always representable in chrono")
        .to_rfc3339()
}
