//! Hierarchy summaries: directory and tag scope aggregation.
//!
//! **STATUS: WIP** — summary generation uses naive extractive concatenation
//! (first 200 chars per chunk). Quality summarization requires an external LLM
//! via the planned job queue.
//!
//! This module provides types and functions for generating and querying
//! derived summaries scoped to a directory path or tag. Summaries are
//! stored in the `derived_summaries` table and indexed for full-text search.
//!
//! All database operations are performed through the [`DerivedSummaryStore`]
//! port trait so that callers are not coupled to the concrete `Db` type.

use brain_core::error::Result;
use brain_persistence::db::Db;
use brain_persistence::db::jobs::{self, EnqueueJobInput, JobPayload};
use brain_persistence::ports::JobQueue;
use brain_persistence::sql::SqlResultExt;

// ─── Types ────────────────────────────────────────────────────────────────────

/// Scope discriminant for a derived summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeType {
    /// A directory path scope, e.g. `src/auth/`.
    Directory,
    /// A tag scope, e.g. `rust`.
    Tag,
}

impl ScopeType {
    /// Canonical string representation stored in the DB.
    pub fn as_str(&self) -> &'static str {
        match self {
            ScopeType::Directory => "directory",
            ScopeType::Tag => "tag",
        }
    }

    /// Parse from the DB string. Returns `None` for unknown values.
    pub fn parse_db(s: &str) -> Option<Self> {
        match s {
            "directory" => Some(ScopeType::Directory),
            "tag" => Some(ScopeType::Tag),
            _ => None,
        }
    }
}

/// A derived summary row returned from the `derived_summaries` table.
#[derive(Debug, Clone)]
pub struct DerivedSummary {
    /// Auto-assigned row identifier (ULID string).
    pub id: String,
    /// Scope discriminant: "directory" or "tag".
    pub scope_type: String,
    /// The directory path or tag name this summary describes.
    pub scope_value: String,
    /// Extractive summary text derived from chunks matching the scope.
    pub content: String,
    /// When `true`, the summary is out of date and must be regenerated.
    pub stale: bool,
    /// Unix timestamp (seconds) when this summary was generated.
    pub generated_at: i64,
}

/// Result of generating a scope summary placeholder.
#[derive(Debug, Clone)]
pub struct GeneratedScopeSummary {
    /// Newly assigned row identifier.
    pub id: String,
    /// Full source content sent to the async LLM job.
    pub source_content: String,
    /// Whether the source content actually changed (false = hash matched, skip LLM).
    pub content_changed: bool,
}

/// Result of generating a scope summary, including whether an LLM job is pending.
#[derive(Debug, Clone)]
pub struct ScopeSummaryGeneration {
    pub id: String,
    pub llm_pending: bool,
}

// ─── Port trait ───────────────────────────────────────────────────────────────

/// Persistence port for derived summary operations.
///
/// Implementations are provided in `brain_lib::ports` for the concrete `Db`
/// type. Tests may provide in-memory or mock implementations.
pub trait DerivedSummaryStore: Send + Sync {
    /// Generate and persist a derived summary for the given scope.
    /// Returns the newly assigned summary id.
    fn generate_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<GeneratedScopeSummary>;

    /// Retrieve an existing derived summary for the given scope.
    /// Returns `Ok(None)` if no row exists.
    fn get_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<Option<DerivedSummary>>;

    /// Mark any existing derived summary for the given scope as stale.
    /// Returns the number of rows updated (0 or 1).
    fn mark_scope_stale(&self, scope_type: &ScopeType, scope_value: &str) -> Result<usize>;

    /// Search derived summaries by keyword across all scopes.
    fn search_derived_summaries(&self, query: &str, limit: usize) -> Result<Vec<DerivedSummary>>;

    /// List derived summaries that are marked stale, ordered by oldest first.
    fn list_stale_summaries(&self, limit: usize) -> Result<Vec<DerivedSummary>>;
}

// ─── Implementation ───────────────────────────────────────────────────────────

/// Generate and persist a derived summary for the given scope.
///
/// Collects all chunk content matching the scope, truncates each chunk to
/// 200 characters, joins them with newlines, and persists the result as an
/// extractive summary via `INSERT OR REPLACE` into `derived_summaries`.
///
/// # Arguments
/// * `store`       — persistence port implementing [`DerivedSummaryStore`]
/// * `scope_type`  — whether the scope is a directory or tag
/// * `scope_value` — the concrete path or tag name
///
/// # Returns
/// The newly assigned summary `id` on success.
pub fn generate_scope_summary(
    store: &(impl DerivedSummaryStore + JobQueue),
    scope_type: &ScopeType,
    scope_value: &str,
) -> Result<String> {
    Ok(generate_scope_summary_with_options(store, scope_type, scope_value, true)?.id)
}

/// Generate a placeholder summary and optionally enqueue an async LLM refresh.
pub fn generate_scope_summary_with_options(
    store: &(impl DerivedSummaryStore + JobQueue),
    scope_type: &ScopeType,
    scope_value: &str,
    async_llm: bool,
) -> Result<ScopeSummaryGeneration> {
    let generated = store.generate_scope_summary(scope_type, scope_value)?;
    let llm_pending =
        if async_llm && generated.content_changed && !generated.source_content.is_empty() {
            enqueue_scope_summary(
                store,
                &generated.id,
                scope_type.as_str(),
                scope_value,
                &generated.source_content,
            )?;
            true
        } else {
            false
        };

    Ok(ScopeSummaryGeneration {
        id: generated.id,
        llm_pending,
    })
}

/// Retrieve an existing derived summary for the given scope.
///
/// Returns `Ok(None)` if no summary row exists for the given scope.
pub fn get_scope_summary(
    store: &impl DerivedSummaryStore,
    scope_type: &ScopeType,
    scope_value: &str,
) -> Result<Option<DerivedSummary>> {
    store.get_scope_summary(scope_type, scope_value)
}

/// Mark any existing derived summary for the given scope as stale.
///
/// Called when a file inside a directory is re-indexed so that the
/// directory summary is queued for regeneration.
///
/// # Returns
/// Number of rows updated (0 or 1).
pub fn mark_scope_stale(
    store: &impl DerivedSummaryStore,
    scope_type: &ScopeType,
    scope_value: &str,
) -> Result<usize> {
    store.mark_scope_stale(scope_type, scope_value)
}

/// Search derived summaries by keyword across all scopes.
///
/// Queries the `fts_derived_summaries` FTS5 virtual table when it exists,
/// falling back to a LIKE search on `derived_summaries.content`.
///
/// Returns matching summaries ordered by relevance.
///
/// TODO: Integrate derived summaries into `memory.retrieve` results.
/// Embedding-based search requires indexing summaries into LanceDB, which is
/// deferred. Until then, use the `memory.summarize_scope` MCP tool for direct
/// scope-based access to derived summaries.
pub fn search_derived_summaries(
    store: &impl DerivedSummaryStore,
    query: &str,
    limit: usize,
) -> Result<Vec<DerivedSummary>> {
    store.search_derived_summaries(query, limit)
}

// ─── `DerivedSummaryStore` impl for the concrete `Db` ───────────────────────
//
// The trait lives in this module; per Rust's orphan rule, the impl for
// brain_persistence's `Db` must live in this crate. Delegates to typed SQL
// writers in `brain_persistence::derived_summaries`.

impl DerivedSummaryStore for Db {
    fn generate_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<GeneratedScopeSummary> {
        let scope_type = scope_type.as_str().to_string();
        let scope_value = scope_value.to_string();
        let generated = self
            .with_write_conn(move |conn| {
                brain_persistence::derived_summaries::generate_scope_summary(
                    conn,
                    &scope_type,
                    &scope_value,
                )
            })
            .into_brain_core()?;

        Ok(GeneratedScopeSummary {
            id: generated.id,
            source_content: generated.source_content,
            content_changed: generated.content_changed,
        })
    }

    fn get_scope_summary(
        &self,
        scope_type: &ScopeType,
        scope_value: &str,
    ) -> Result<Option<DerivedSummary>> {
        let scope_type = scope_type.as_str().to_string();
        let scope_value = scope_value.to_string();
        let row = self
            .with_read_conn(move |conn| {
                brain_persistence::derived_summaries::get_scope_summary(
                    conn,
                    &scope_type,
                    &scope_value,
                )
            })
            .into_brain_core()?;

        Ok(row.map(|summary| DerivedSummary {
            id: summary.id,
            scope_type: summary.scope_type,
            scope_value: summary.scope_value,
            content: summary.content,
            stale: summary.stale,
            generated_at: summary.generated_at,
        }))
    }

    fn mark_scope_stale(&self, scope_type: &ScopeType, scope_value: &str) -> Result<usize> {
        let scope_type = scope_type.as_str().to_string();
        let scope_value = scope_value.to_string();
        self.with_write_conn(move |conn| {
            brain_persistence::derived_summaries::mark_scope_stale(conn, &scope_type, &scope_value)
        })
        .into_brain_core()
    }

    fn search_derived_summaries(&self, query: &str, limit: usize) -> Result<Vec<DerivedSummary>> {
        let query = query.to_string();
        let rows = self
            .with_read_conn(move |conn| {
                brain_persistence::derived_summaries::search_derived_summaries(conn, &query, limit)
            })
            .into_brain_core()?;

        Ok(rows
            .into_iter()
            .map(|summary| DerivedSummary {
                id: summary.id,
                scope_type: summary.scope_type,
                scope_value: summary.scope_value,
                content: summary.content,
                stale: summary.stale,
                generated_at: summary.generated_at,
            })
            .collect())
    }

    fn list_stale_summaries(&self, limit: usize) -> Result<Vec<DerivedSummary>> {
        let rows = self
            .with_read_conn(move |conn| {
                brain_persistence::derived_summaries::list_stale_summaries(conn, limit)
            })
            .into_brain_core()?;

        Ok(rows
            .into_iter()
            .map(|summary| DerivedSummary {
                id: summary.id,
                scope_type: summary.scope_type,
                scope_value: summary.scope_value,
                content: summary.content,
                stale: summary.stale,
                generated_at: summary.generated_at,
            })
            .collect())
    }
}

// ─── Job enqueue helper ──────────────────────────────────────────────────────
//
// Inlined from brain_lib::pipeline::job_worker so brain_retrieval does not
// take a back-dep on brain_lib. Builds the scope-summarize job payload and
// submits it through the `JobQueue` port.

fn enqueue_scope_summary(
    queue: &dyn JobQueue,
    summary_id: &str,
    scope_type: &str,
    scope_value: &str,
    content: &str,
) -> Result<String> {
    let input = EnqueueJobInput {
        payload: JobPayload::SummarizeScope {
            summary_id: summary_id.to_string(),
            scope_type: scope_type.to_string(),
            scope_value: scope_value.to_string(),
            content: content.to_string(),
        },
        priority: jobs::priority::NORMAL,
        retry_config: None,
        stuck_threshold_secs: None,
        metadata: serde_json::json!({}),
        scheduled_at: 0,
    };
    queue.enqueue_job(&input)
}
