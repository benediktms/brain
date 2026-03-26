//! Recurring job registry and daemon reconciliation.
//!
//! Defines which job kinds are managed as singletons by the daemon.
//! The reconciliation loop ensures each registered kind has exactly one
//! row in the jobs table and reschedules it when it reaches a terminal state.
//!
//! Global jobs (stale_scope_sweep, consolidation_sweep) have one row each.
//! Per-brain jobs (full_scan_sweep, embed_poll_sweep) have one row per brain,
//! disambiguated by `brain_id` in the payload.

use crate::db::jobs::{self, EnqueueJobInput, JobPayload, RetryStrategy};
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
    /// Delay (seconds) before rescheduling after completion. 0 = immediate.
    pub reschedule_delay_secs: i64,
}

/// Global recurring jobs (not brain-scoped).
pub const GLOBAL_RECURRING_JOBS: &[RecurringJobSpec] = &[
    RecurringJobSpec {
        kind: "stale_scope_sweep",
        make_payload: || JobPayload::StaleScopeSweep,
        priority: jobs::priority::BACKGROUND,
        retry_strategy: RetryStrategy::Infinite,
        stuck_threshold_secs: 60,
        reschedule_delay_secs: 300, // 5 minutes
    },
    RecurringJobSpec {
        kind: "consolidation_sweep",
        make_payload: || JobPayload::ConsolidationSweep,
        priority: jobs::priority::BACKGROUND,
        retry_strategy: RetryStrategy::Infinite,
        stuck_threshold_secs: 60,
        reschedule_delay_secs: 300, // 5 minutes
    },
];

/// A registered brain with its note directories.
pub struct BrainInfo {
    pub brain_id: String,
    pub note_dirs: Vec<String>,
}

/// Reconcile all recurring singleton jobs. Called once per daemon tick.
///
/// Creates/reschedules global jobs and per-brain jobs.
pub fn reconcile_recurring_jobs(queue: &dyn JobQueue, brains: &[BrainInfo]) -> Result<()> {
    // Global jobs
    for spec in GLOBAL_RECURRING_JOBS {
        let input = EnqueueJobInput {
            payload: (spec.make_payload)(),
            priority: spec.priority,
            retry_config: Some(spec.retry_strategy.clone()),
            stuck_threshold_secs: Some(spec.stuck_threshold_secs),
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };
        queue.reconcile_singleton_job_with_delay(&input, spec.reschedule_delay_secs)?;
    }

    // Per-brain jobs
    for brain in brains {
        // Full scan sweep — every 5 minutes
        let input = EnqueueJobInput {
            payload: JobPayload::FullScanSweep {
                brain_id: brain.brain_id.clone(),
                dirs: brain.note_dirs.clone(),
            },
            priority: jobs::priority::BACKGROUND,
            retry_config: Some(RetryStrategy::Infinite),
            stuck_threshold_secs: Some(600),
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };
        queue.reconcile_singleton_job_with_delay(&input, 300)?;

        // Embed poll sweep — every 10 seconds
        let input = EnqueueJobInput {
            payload: JobPayload::EmbedPollSweep {
                brain_id: brain.brain_id.clone(),
            },
            priority: jobs::priority::BACKGROUND,
            retry_config: Some(RetryStrategy::Infinite),
            stuck_threshold_secs: Some(600),
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };
        queue.reconcile_singleton_job_with_delay(&input, 10)?;
    }

    Ok(())
}

/// Returns `true` if the given `kind` is a registered recurring singleton.
pub fn is_recurring_kind(kind: &str) -> bool {
    matches!(
        kind,
        "stale_scope_sweep" | "consolidation_sweep" | "full_scan_sweep" | "embed_poll_sweep"
    )
}

/// Returns the list of recurring job kinds (for GC exclusion).
pub fn protected_kinds() -> Vec<&'static str> {
    vec![
        "stale_scope_sweep",
        "consolidation_sweep",
        "full_scan_sweep",
        "embed_poll_sweep",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    #[test]
    fn test_reconcile_no_brains() {
        let db = Db::open_in_memory().unwrap();
        reconcile_recurring_jobs(&db, &[]).unwrap();
    }

    #[test]
    fn test_is_recurring_kind() {
        assert!(is_recurring_kind("stale_scope_sweep"));
        assert!(is_recurring_kind("consolidation_sweep"));
        assert!(is_recurring_kind("full_scan_sweep"));
        assert!(is_recurring_kind("embed_poll_sweep"));
        assert!(!is_recurring_kind("summarize_scope"));
        assert!(!is_recurring_kind("consolidate_cluster"));
    }

    #[test]
    fn test_protected_kinds() {
        let protected = protected_kinds();
        assert_eq!(protected.len(), 4);
        assert!(protected.contains(&"full_scan_sweep"));
        assert!(protected.contains(&"embed_poll_sweep"));
    }
}
