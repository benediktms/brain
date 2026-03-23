//! Memory consolidation: groups recent episodes by temporal proximity and
//! produces consolidation candidates for agent review.
//!
//! **STATUS: WIP** — cluster summaries use naive extractive concatenation
//! (first 200 chars per episode). Quality synthesis requires an external LLM
//! via the planned job queue (see task BRN-01KM5Z5TMJV0ANN0H6QCHVB9KW).
//!
//! This module defines the types and the `consolidate_episodes` function that
//! implements the `memory.consolidate` MCP tool.

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
/// # Algorithm
/// 1. Sort episodes by `created_at` ascending.
/// 2. Split into clusters whenever the gap between consecutive episodes
///    exceeds `gap_seconds`.
/// 3. For each cluster, generate a `suggested_title` and `summary`.
/// 4. Return clusters ordered newest-first (reverse chronological).
///
/// # Parameters
/// - `episodes`: pre-fetched episode rows, any order.
/// - `gap_seconds`: minimum gap (in seconds) that separates two clusters.
pub fn consolidate_episodes(episodes: Vec<SummaryRow>, gap_seconds: i64) -> ConsolidateResult {
    if episodes.is_empty() {
        return ConsolidateResult::default();
    }

    // Sort ascending by created_at.
    let mut sorted = episodes;
    sorted.sort_by_key(|e| e.created_at);

    // Gap-split into clusters.
    let mut raw_clusters: Vec<Vec<SummaryRow>> = Vec::new();
    let mut current_cluster: Vec<SummaryRow> = Vec::new();

    for episode in sorted {
        if let Some(last) = current_cluster.last()
            && episode.created_at - last.created_at > gap_seconds
        {
            // Gap exceeded — push current cluster and start a new one.
            raw_clusters.push(current_cluster);
            current_cluster = Vec::new();
        }
        current_cluster.push(episode);
    }
    if !current_cluster.is_empty() {
        raw_clusters.push(current_cluster);
    }

    // Build ConsolidationCluster for each raw cluster; reverse for newest-first.
    let mut clusters: Vec<ConsolidationCluster> = raw_clusters
        .into_iter()
        .map(|group| {
            let episode_ids: Vec<String> = group.iter().map(|e| e.summary_id.clone()).collect();

            // suggested_title: first episode's title, or "Episodes from {date}".
            let suggested_title =
                group
                    .first()
                    .and_then(|e| e.title.clone())
                    .unwrap_or_else(|| {
                        let ts = group.first().map(|e| e.created_at).unwrap_or(0);
                        format!("Episodes from {}", format_date(ts))
                    });

            // summary: first 200 chars of each episode's content, joined.
            let summary = group
                .iter()
                .map(|e| {
                    let s = e.content.as_str();
                    s.get(..200).unwrap_or(s)
                })
                .collect::<Vec<_>>()
                .join("\n---\n");

            ConsolidationCluster {
                episode_ids,
                episodes: group,
                suggested_title,
                summary,
            }
        })
        .collect();

    // Reverse so newest cluster is first.
    clusters.reverse();

    ConsolidateResult { clusters }
}

/// Format a Unix timestamp as a simple date string (YYYY-MM-DD).
fn format_date(ts: i64) -> String {
    // Compute date components from Unix timestamp.
    // We use a simple algorithm — no external chrono dep required.
    if ts <= 0 {
        return "unknown".to_string();
    }
    let days_since_epoch = ts / 86400;
    // Use a simple Julian-day algorithm to convert to year-month-day.
    // Clamp to valid u32 range (negative timestamps handled by ts <= 0 guard above).
    let (y, m, d) = days_to_ymd(days_since_epoch.clamp(0, u32::MAX as i64) as u32);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Convert days since Unix epoch (1970-01-01) to (year, month, day).
fn days_to_ymd(days: u32) -> (u32, u32, u32) {
    // Julian Day Number for 1970-01-01 is 2440588.
    let jdn = days + 2_440_588;
    // Algorithm from https://en.wikipedia.org/wiki/Julian_day#Julian_day_number_calculation
    let l = jdn + 68569;
    let n = (4 * l) / 146097;
    let l = l - (146097 * n).div_ceil(4);
    let i = (4000 * (l + 1)) / 1461001;
    let l = l - (1461 * i) / 4 + 31;
    let j = (80 * l) / 2447;
    let d = l - (2447 * j) / 80;
    let l = j / 11;
    let m = j + 2 - 12 * l;
    let y = 100 * (n - 49) + i + l;
    (y, m, d)
}
