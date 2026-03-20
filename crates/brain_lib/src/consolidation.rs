//! Memory consolidation: groups recent episodes by temporal proximity and
//! produces consolidation candidates for agent review.
//!
//! This module defines the types and the stub function for the
//! `memory.consolidate` MCP tool. The stub returns an empty result;
//! the real grouping algorithm is wired in a subsequent implementation step.

use brain_persistence::db::summaries::SummaryRow;

/// A group of temporally proximate episodes with a suggested consolidation.
#[derive(Debug, Clone)]
pub struct ConsolidationCluster {
    /// IDs of the episodes in this cluster.
    pub episode_ids: Vec<String>,
    /// The full episode rows (for review).
    pub episodes: Vec<SummaryRow>,
    /// A machine-generated title suggestion for the consolidated reflection.
    pub suggested_title: String,
    /// A brief summary of the cluster content.
    pub summary: String,
}

/// The result returned by [`consolidate_episodes`].
#[derive(Debug, Clone, Default)]
pub struct ConsolidateResult {
    /// One entry per temporal cluster found.
    pub clusters: Vec<ConsolidationCluster>,
}

/// Group `episodes` into temporal clusters and return consolidation candidates.
///
/// # Algorithm (stub)
/// The current implementation is a TDD stub that returns an empty result.
/// The real algorithm will:
/// 1. Sort episodes by `created_at` ascending.
/// 2. Split into clusters whenever the gap between consecutive episodes
///    exceeds `gap_seconds`.
/// 3. For each cluster, generate a `suggested_title` and `summary`.
///
/// # Parameters
/// - `episodes`: pre-fetched episode rows, newest-first or any order.
/// - `gap_seconds`: minimum gap (in seconds) that separates two clusters.
pub fn consolidate_episodes(
    _episodes: Vec<SummaryRow>,
    _gap_seconds: i64,
) -> ConsolidateResult {
    // TDD red-phase stub: returns empty result.
    ConsolidateResult::default()
}
