//! Hierarchy summaries: directory and tag scope aggregation.
//!
//! **STATUS: WIP** — summary generation uses naive extractive concatenation
//! (first 200 chars per chunk). Quality summarization requires an external LLM
//! via the planned job queue (see task BRN-01KM5Z5TMJV0ANN0H6QCHVB9KW).
//!
//! This module provides types and functions for generating and querying
//! derived summaries scoped to a directory path or tag. Summaries are
//! stored in the `derived_summaries` table and indexed for full-text search.
//!
//! All database operations are performed through the [`DerivedSummaryStore`]
//! port trait so that callers are not coupled to the concrete `Db` type.

use brain_persistence::error::Result;

use crate::pipeline::job_worker::enqueue_scope_summary;
use crate::ports::JobQueue;

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
    let llm_pending = if async_llm && !generated.source_content.is_empty() {
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
/// TODO: Integrate derived summaries into `memory.search_minimal` results.
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
