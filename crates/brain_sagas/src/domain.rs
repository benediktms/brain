//! Domain types for the saga subsystem.
//!
//! The `brain_persistence::db::sagas::queries::*Row` types are the SQL
//! projection shape — an implementation detail of the rusqlite backend.
//! This module exposes the typed domain vocabulary that SagaStore returns
//! to consumers. Mirrors the `brain_tasks::domain::Task` precedent.
//!
//! Every persistence-row type that previously appeared in SagaStore's
//! public API has a domain counterpart here with a `From<RowType>`
//! boundary impl. SagaStore methods convert at the persistence boundary
//! via `.into()`; consumers see only domain types.

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use brain_persistence::db::sagas::queries::{
    BrainSummary as BrainSummaryRow, CascadeOutcome as CascadeOutcomeRow,
    CascadeResult as CascadeResultRow, LabelCount as LabelCountRow,
    SagaListFilter as SagaListFilterRow, SagaMemberStub, SagaRow, SagaStatsRow,
};
use brain_tasks::TaskId;
use brain_tasks::events::{TaskStatus, TaskType};

use crate::status::SagaStatus;

/// Newtype wrapping the bare 26-char ULID `saga_id` value.
///
/// Distinct from the user-facing `saga-<hex>` short form, which lives
/// in `Saga::display_id` (the hex portion without the `saga-` prefix).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SagaId(String);

impl SagaId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for SagaId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SagaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for SagaId {
    fn from(s: String) -> Self {
        SagaId(s)
    }
}

impl From<&str> for SagaId {
    fn from(s: &str) -> Self {
        SagaId(s.to_string())
    }
}

/// A saga in its domain shape — typed status, parsed timestamps, newtyped ID.
///
/// Converted from `SagaRow` at the persistence boundary via `From<SagaRow>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Saga {
    pub id: SagaId,
    /// Short hex display form (no `saga-` prefix). Render via
    /// `brain_persistence::db::sagas::display_id::compact_saga_id` for the
    /// user-facing `saga-<hex>` form.
    pub display_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: SagaStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
}

impl From<SagaRow> for Saga {
    fn from(row: SagaRow) -> Self {
        // Defense-in-depth: SagaStore methods pre-validate the status
        // string before any row is returned; the fallback here exists for
        // defense against direct-read paths that bypass that validation.
        let status: SagaStatus = row.status.parse().unwrap_or(SagaStatus::Planning);
        Saga {
            id: SagaId::from(row.saga_id),
            display_id: row.display_id,
            title: row.title,
            description: row.description,
            status,
            created_at: Utc.timestamp_opt(row.created_at, 0).unwrap(),
            updated_at: Utc.timestamp_opt(row.updated_at, 0).unwrap(),
            closed_at: row
                .closed_at
                .and_then(|ts| Utc.timestamp_opt(ts, 0).single()),
        }
    }
}

impl From<&SagaRow> for Saga {
    fn from(row: &SagaRow) -> Self {
        Saga::from(row.clone())
    }
}

/// Member-task stub for saga membership rendering — task identity + brain +
/// status snapshot. Cross-domain by design: every saga member lives in some
/// brain's task table, possibly a different brain than the saga itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaMember {
    pub task_id: TaskId,
    pub brain_id: String,
    pub title: String,
    pub status: TaskStatus,
    pub task_type: TaskType,
}

impl From<SagaMemberStub> for SagaMember {
    fn from(stub: SagaMemberStub) -> Self {
        let status: TaskStatus = stub.status.parse().unwrap_or(TaskStatus::Open);
        let task_type: TaskType = stub.task_type.parse().unwrap_or(TaskType::Task);
        SagaMember {
            task_id: TaskId::from(stub.task_id),
            brain_id: stub.brain_id,
            title: stub.title,
            status,
            task_type,
        }
    }
}

impl From<&SagaMemberStub> for SagaMember {
    fn from(stub: &SagaMemberStub) -> Self {
        SagaMember::from(stub.clone())
    }
}

/// Summary of a brain that has member tasks in a saga.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrainSummary {
    pub brain_id: String,
    pub name: String,
    pub prefix: Option<String>,
}

impl From<BrainSummaryRow> for BrainSummary {
    fn from(row: BrainSummaryRow) -> Self {
        BrainSummary {
            brain_id: row.brain_id,
            name: row.name,
            prefix: row.prefix,
        }
    }
}

impl From<&BrainSummaryRow> for BrainSummary {
    fn from(row: &BrainSummaryRow) -> Self {
        BrainSummary {
            brain_id: row.brain_id.clone(),
            name: row.name.clone(),
            prefix: row.prefix.clone(),
        }
    }
}

/// Aggregate counts for a saga's member tasks.
///
/// `total` is the count of live (JOIN-resolved) members; `orphan` counts
/// `saga_tasks` rows whose underlying task has been deleted in another
/// brain. `completion_pct` is `done / (total - cancelled)`, `None` when
/// the denominator is zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaStatsCounts {
    pub total: i64,
    pub open: i64,
    pub in_progress: i64,
    pub blocked: i64,
    pub done: i64,
    pub cancelled: i64,
    pub orphan: i64,
    pub completion_pct: Option<f64>,
}

impl From<SagaStatsRow> for SagaStatsCounts {
    fn from(row: SagaStatsRow) -> Self {
        SagaStatsCounts {
            total: row.total,
            open: row.open,
            in_progress: row.in_progress,
            blocked: row.blocked,
            done: row.done,
            cancelled: row.cancelled,
            orphan: row.orphan,
            completion_pct: row.completion_pct,
        }
    }
}

impl From<&SagaStatsRow> for SagaStatsCounts {
    fn from(row: &SagaStatsRow) -> Self {
        SagaStatsCounts {
            total: row.total,
            open: row.open,
            in_progress: row.in_progress,
            blocked: row.blocked,
            done: row.done,
            cancelled: row.cancelled,
            orphan: row.orphan,
            completion_pct: row.completion_pct,
        }
    }
}

/// `(label, count)` pair for the label-histogram surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelCount {
    pub label: String,
    pub count: i64,
}

impl From<LabelCountRow> for LabelCount {
    fn from(row: LabelCountRow) -> Self {
        LabelCount {
            label: row.label,
            count: row.count,
        }
    }
}

impl From<&LabelCountRow> for LabelCount {
    fn from(row: &LabelCountRow) -> Self {
        LabelCount {
            label: row.label.clone(),
            count: row.count,
        }
    }
}

/// Filter input for `SagaStore::list`. Domain type; converted to the
/// persistence-row form at the boundary via `From<&SagaListFilter>`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SagaListFilter {
    pub include_closed: bool,
    pub include_cancelled: bool,
    /// Only return sagas that have at least one member-task in this brain.
    pub containing_brain: Option<String>,
}

impl From<&SagaListFilter> for SagaListFilterRow {
    fn from(filter: &SagaListFilter) -> Self {
        SagaListFilterRow {
            include_closed: filter.include_closed,
            include_cancelled: filter.include_cancelled,
            containing_brain: filter.containing_brain.clone(),
        }
    }
}

impl From<SagaListFilter> for SagaListFilterRow {
    fn from(filter: SagaListFilter) -> Self {
        SagaListFilterRow {
            include_closed: filter.include_closed,
            include_cancelled: filter.include_cancelled,
            containing_brain: filter.containing_brain,
        }
    }
}

/// Per-task outcome of a `close --cascade` or `cancel --cascade`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CascadeResult {
    pub task_id: TaskId,
    pub outcome: CascadeOutcome,
}

impl CascadeResult {
    pub fn is_failure(&self) -> bool {
        matches!(self.outcome, CascadeOutcome::Failed { .. })
    }
}

/// Outcome of cascading a single saga member task to a terminal status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CascadeOutcome {
    /// Task transitioned to Done (close-cascade success).
    Closed,
    /// Task transitioned to Cancelled (cancel-cascade success).
    Cancelled,
    /// Task was already terminal — left untouched.
    Skipped { reason: String },
    /// Task event append failed; saga's own state still committed.
    Failed { error: String },
}

impl From<CascadeOutcomeRow> for CascadeOutcome {
    fn from(row: CascadeOutcomeRow) -> Self {
        match row {
            CascadeOutcomeRow::Closed => CascadeOutcome::Closed,
            CascadeOutcomeRow::Cancelled => CascadeOutcome::Cancelled,
            CascadeOutcomeRow::Skipped { reason } => CascadeOutcome::Skipped { reason },
            CascadeOutcomeRow::Failed { error } => CascadeOutcome::Failed { error },
        }
    }
}

impl From<CascadeResultRow> for CascadeResult {
    fn from(row: CascadeResultRow) -> Self {
        CascadeResult {
            task_id: TaskId::from(row.task_id),
            outcome: row.outcome.into(),
        }
    }
}
