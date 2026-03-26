//! Typed job domain objects.
//!
//! - [`JobPayload`] — tagged enum carrying kind + typed fields for each job variant
//! - [`JobStatus`] — valid state values for a job
//! - [`RetryStrategy`] — retry policy, stored as JSON in `retry_config` column
//! - [`Job`] — the job entity, populated from a database row

use serde::{Deserialize, Serialize};

// ─── JobStatus ───────────────────────────────────────────────────

/// Job status values. Stored as lowercase strings in SQLite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Waiting to be claimed by the runner.
    Ready,
    /// Claimed by runner, about to start executing.
    Pending,
    /// Actively executing.
    InProgress,
    /// Finished successfully. Terminal state.
    Done,
    /// Exhausted retries or failed. Terminal state.
    Failed,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl AsRef<str> for JobStatus {
    fn as_ref(&self) -> &str {
        match self {
            JobStatus::Ready => "ready",
            JobStatus::Pending => "pending",
            JobStatus::InProgress => "in_progress",
            JobStatus::Done => "done",
            JobStatus::Failed => "failed",
        }
    }
}

impl std::str::FromStr for JobStatus {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "ready" => Ok(JobStatus::Ready),
            "pending" => Ok(JobStatus::Pending),
            "in_progress" => Ok(JobStatus::InProgress),
            "done" => Ok(JobStatus::Done),
            "failed" => Ok(JobStatus::Failed),
            _ => Err(format!("invalid job status: '{s}'")),
        }
    }
}

// ─── RetryStrategy ───────────────────────────────────────────────

/// Retry policy for a job. Serialized as JSON into the `retry_config` column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum RetryStrategy {
    /// Never retry. The job is side-effectful or non-idempotent.
    #[default]
    NoRetry,
    /// Retry up to a fixed number of times.
    Fixed { attempts: u32 },
    /// Retry indefinitely until success or explicit cancellation.
    Infinite,
}

impl RetryStrategy {
    /// Returns `Some(0)` for `NoRetry`, `Some(n)` for `Fixed`, `None` for `Infinite`.
    pub fn max_attempts(&self) -> Option<u32> {
        match self {
            RetryStrategy::NoRetry => Some(0),
            RetryStrategy::Fixed { attempts } => Some(*attempts),
            RetryStrategy::Infinite => None,
        }
    }

    /// True if this strategy permits retries (Fixed or Infinite).
    pub fn is_retryable(&self) -> bool {
        !matches!(self, RetryStrategy::NoRetry)
    }

    /// True if attempts have been exhausted.
    /// `Infinite` never exhausts. `NoRetry` is always exhausted (max=0).
    pub fn is_exhausted(&self, attempts: u32) -> bool {
        match self {
            RetryStrategy::NoRetry => true,
            RetryStrategy::Fixed { attempts: max } => attempts >= *max,
            RetryStrategy::Infinite => false,
        }
    }

    /// Human-readable label for observability.
    pub fn label(&self) -> &'static str {
        match self {
            RetryStrategy::NoRetry => "no-retry",
            RetryStrategy::Fixed { .. } => "fixed",
            RetryStrategy::Infinite => "infinite",
        }
    }
}

// ─── JobPayload ──────────────────────────────────────────────────

/// Typed job payload. Each variant represents a distinct job kind.
///
/// The `kind` column in SQLite is derived from the variant discriminant
/// via [`JobPayload::kind()`]. The full payload is stored as JSON in the
/// `payload` column.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobPayload {
    /// Summarize a scope (directory or tag) using an LLM.
    SummarizeScope {
        scope_type: String,
        scope_value: String,
        content: String,
    },
    /// Consolidate a cluster of episodes into a single reflection.
    ConsolidateCluster {
        /// Newline-separated episode content.
        episodes: String,
    },
}

impl JobPayload {
    /// The kind discriminant as stored in the `kind` DB column.
    pub fn kind(&self) -> &'static str {
        match self {
            JobPayload::SummarizeScope { .. } => "summarize_scope",
            JobPayload::ConsolidateCluster { .. } => "consolidate_cluster",
        }
    }

    /// Default retry strategy for this job kind.
    pub fn default_retry_strategy(&self) -> RetryStrategy {
        match self {
            JobPayload::SummarizeScope { .. } => RetryStrategy::Fixed { attempts: 3 },
            JobPayload::ConsolidateCluster { .. } => RetryStrategy::Fixed { attempts: 3 },
        }
    }

    /// Default stuck threshold (seconds) for this job kind.
    pub fn default_stuck_threshold_secs(&self) -> i64 {
        match self {
            JobPayload::SummarizeScope { .. } => 300,
            JobPayload::ConsolidateCluster { .. } => 300,
        }
    }
}

// ─── Job ─────────────────────────────────────────────────────────

/// A job entity as stored in the database.
#[derive(Debug, Clone)]
pub struct Job {
    pub job_id: String,
    pub payload: JobPayload,
    pub status: JobStatus,
    pub priority: i32,
    pub retry_config: RetryStrategy,
    /// Seconds after which an in-progress job is considered stuck. Default: 300.
    pub stuck_threshold_secs: i64,
    pub result: Option<String>,
    /// Number of execution attempts (incremented on each claim).
    pub attempts: u32,
    pub last_error: Option<String>,
    /// Free-form metadata (JSON object).
    pub metadata: serde_json::Value,
    pub created_at: i64,
    /// When the job becomes eligible to be claimed.
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    /// Set when the job enters a terminal state (`done` or `failed`).
    pub processed_at: Option<i64>,
    pub updated_at: i64,
}

impl Job {
    /// The kind discriminant, derived from the payload variant.
    pub fn kind(&self) -> &'static str {
        self.payload.kind()
    }

    /// True if the job is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self.status, JobStatus::Done | JobStatus::Failed)
    }

    /// True if the job has exceeded its stuck threshold and should be reaped.
    pub fn is_stuck(&self, now_secs: i64) -> bool {
        self.processed_at.is_none()
            && self.started_at.is_some()
            && (now_secs - self.started_at.unwrap()) > self.stuck_threshold_secs
    }
}

// ─── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_strategy_max_attempts() {
        assert_eq!(RetryStrategy::NoRetry.max_attempts(), Some(0));
        assert!(!RetryStrategy::NoRetry.is_retryable());
        assert_eq!(RetryStrategy::Fixed { attempts: 3 }.max_attempts(), Some(3));
        assert!(RetryStrategy::Fixed { attempts: 3 }.is_retryable());
        assert_eq!(RetryStrategy::Infinite.max_attempts(), None);
        assert!(RetryStrategy::Infinite.is_retryable());
    }

    #[test]
    fn test_retry_strategy_is_exhausted() {
        assert!(RetryStrategy::NoRetry.is_exhausted(0));
        assert!(RetryStrategy::NoRetry.is_exhausted(5));
        assert!(!RetryStrategy::Fixed { attempts: 3 }.is_exhausted(1));
        assert!(!RetryStrategy::Fixed { attempts: 3 }.is_exhausted(2));
        assert!(RetryStrategy::Fixed { attempts: 3 }.is_exhausted(3));
        assert!(RetryStrategy::Fixed { attempts: 3 }.is_exhausted(5));
        assert!(!RetryStrategy::Infinite.is_exhausted(0));
        assert!(!RetryStrategy::Infinite.is_exhausted(9999));
    }

    #[test]
    fn test_retry_strategy_serialization() {
        assert_eq!(
            serde_json::to_string(&RetryStrategy::NoRetry).unwrap(),
            r#"{"type":"noRetry"}"#
        );
        assert_eq!(
            serde_json::to_string(&RetryStrategy::Fixed { attempts: 3 }).unwrap(),
            r#"{"type":"fixed","attempts":3}"#
        );
        assert_eq!(
            serde_json::to_string(&RetryStrategy::Infinite).unwrap(),
            r#"{"type":"infinite"}"#
        );
    }

    #[test]
    fn test_job_status_roundtrip() {
        for status in [
            JobStatus::Ready,
            JobStatus::Pending,
            JobStatus::InProgress,
            JobStatus::Done,
            JobStatus::Failed,
        ] {
            let s: &str = status.as_ref();
            let parsed: JobStatus = s.parse().unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_payload_kind_derivation() {
        let p = JobPayload::SummarizeScope {
            scope_type: "directory".into(),
            scope_value: "src/".into(),
            content: "hello".into(),
        };
        assert_eq!(p.kind(), "summarize_scope");

        let p = JobPayload::ConsolidateCluster {
            episodes: "ep1\nep2".into(),
        };
        assert_eq!(p.kind(), "consolidate_cluster");
    }

    #[test]
    fn test_payload_serialization_roundtrip() {
        let p = JobPayload::SummarizeScope {
            scope_type: "directory".into(),
            scope_value: "src/".into(),
            content: "fn main() {}".into(),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains(r#""kind":"summarize_scope""#));
        let deserialized: JobPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.kind(), "summarize_scope");
    }

    #[test]
    fn test_payload_default_retry_strategy() {
        let p = JobPayload::SummarizeScope {
            scope_type: "directory".into(),
            scope_value: "src/".into(),
            content: "".into(),
        };
        assert_eq!(
            p.default_retry_strategy(),
            RetryStrategy::Fixed { attempts: 3 }
        );
    }

    fn make_job(status: JobStatus) -> Job {
        Job {
            job_id: "j1".into(),
            payload: JobPayload::SummarizeScope {
                scope_type: "directory".into(),
                scope_value: "src/".into(),
                content: "".into(),
            },
            status,
            priority: 100,
            retry_config: RetryStrategy::NoRetry,
            stuck_threshold_secs: 300,
            result: None,
            attempts: 0,
            last_error: None,
            metadata: serde_json::Value::Object(Default::default()),
            created_at: 0,
            scheduled_at: 0,
            started_at: None,
            processed_at: None,
            updated_at: 0,
        }
    }

    #[test]
    fn test_job_is_terminal() {
        assert!(!make_job(JobStatus::Ready).is_terminal());
        assert!(!make_job(JobStatus::Pending).is_terminal());
        assert!(!make_job(JobStatus::InProgress).is_terminal());
        assert!(make_job(JobStatus::Done).is_terminal());
        assert!(make_job(JobStatus::Failed).is_terminal());
    }

    #[test]
    fn test_job_is_stuck() {
        let mut job = make_job(JobStatus::InProgress);
        job.started_at = Some(1000);
        job.retry_config = RetryStrategy::Fixed { attempts: 3 };

        assert!(!job.is_stuck(1200));
        assert!(job.is_stuck(1400));

        job.processed_at = Some(1300);
        assert!(!job.is_stuck(1400));
    }
}
