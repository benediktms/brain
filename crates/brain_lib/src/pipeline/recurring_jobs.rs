//! Recurring job registry and daemon reconciliation.
//!
//! Defines which job kinds are managed as singletons by the daemon.
//! The reconciliation loop ensures each registered kind has exactly one
//! row in the jobs table and reschedules it when it reaches a terminal state.

use crate::db::jobs::{EnqueueJobInput, JobPayload, RetryStrategy};
use crate::error::Result;
use crate::ports::JobQueue;

/// Specification for a recurring singleton job.
pub struct RecurringJobSpec {
    /// The `kind` discriminant (must match a `JobPayload` variant's `kind()`).
    pub kind: &'static str,
    /// Factory for the default payload when creating the initial row.
    pub make_payload: fn() -> JobPayload,
    /// Default priority for this job.
    pub priority: i32,
    /// Retry strategy for this job.
    pub retry_strategy: RetryStrategy,
    /// How long before a running job is considered stuck (seconds).
    pub stuck_threshold_secs: i64,
}

/// All recurring jobs the daemon should maintain.
pub const RECURRING_JOBS: &[RecurringJobSpec] = &[
    RecurringJobSpec {
        kind: "stale_scope_sweep",
        make_payload: || JobPayload::StaleScopeSweep,
        priority: crate::db::jobs::priority::BACKGROUND,
        retry_strategy: RetryStrategy::Infinite,
        stuck_threshold_secs: 60,
    },
    RecurringJobSpec {
        kind: "consolidation_sweep",
        make_payload: || JobPayload::ConsolidationSweep,
        priority: crate::db::jobs::priority::BACKGROUND,
        retry_strategy: RetryStrategy::Infinite,
        stuck_threshold_secs: 60,
    },
];

/// Reconcile recurring singleton jobs. Called once per daemon tick.
///
/// For each spec in [`RECURRING_JOBS`]:
/// 1. If no job of this kind exists → enqueue one as `ready`.
/// 2. If a job exists in a terminal state (done/failed) → reset to `ready`.
/// 3. If a job exists and is active (ready/pending/in_progress) → skip.
///
/// Each spec is reconciled in a single `with_write_conn` call for atomicity.
pub fn reconcile_recurring_jobs(queue: &dyn JobQueue) -> Result<()> {
    for spec in RECURRING_JOBS {
        let input = EnqueueJobInput {
            payload: (spec.make_payload)(),
            priority: spec.priority,
            retry_config: Some(spec.retry_strategy.clone()),
            stuck_threshold_secs: Some(spec.stuck_threshold_secs),
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };

        queue.reconcile_singleton_job(&input)?;
    }

    Ok(())
}

/// Returns `true` if the given `kind` is a registered recurring singleton.
pub fn is_recurring_kind(kind: &str) -> bool {
    RECURRING_JOBS.iter().any(|spec| spec.kind == kind)
}

/// Returns the list of recurring job kinds (for GC exclusion).
pub fn protected_kinds() -> Vec<&'static str> {
    RECURRING_JOBS.iter().map(|spec| spec.kind).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    #[test]
    fn test_reconcile_empty_registry_is_noop() {
        let db = Db::open_in_memory().unwrap();
        reconcile_recurring_jobs(&db).unwrap();
    }

    #[test]
    fn test_is_recurring_kind() {
        assert!(is_recurring_kind("stale_scope_sweep"));
        assert!(is_recurring_kind("consolidation_sweep"));
        assert!(!is_recurring_kind("summarize_scope"));
        assert!(!is_recurring_kind("consolidate_cluster"));
    }

    #[test]
    fn test_protected_kinds() {
        let protected = protected_kinds();
        assert!(protected.contains(&"stale_scope_sweep"));
        assert!(protected.contains(&"consolidation_sweep"));
    }
}
