//! `memory.consolidate` — group recent episodes by temporal proximity
//! into consolidation clusters, optionally enqueuing async LLM
//! synthesis jobs.
//!
//! **STATUS: WIP** — cluster summaries use naive extractive
//! concatenation (first 200 chars per episode). Quality synthesis is
//! deferred to the planned external-LLM job pipeline.

use brain_core::error::Result;
use brain_persistence::db::Db;
use brain_persistence::db::jobs::{self, EnqueueJobInput, JobPayload};
use brain_persistence::db::summaries::{self, SummaryRow};
use brain_persistence::ports::JobQueue;
use brain_persistence::sql::SqlResultExt;
use serde::Deserialize;
use serde_json::{Value, json};

fn default_limit() -> usize {
    50
}

fn default_gap_seconds() -> i64 {
    3600
}

fn default_auto_summarize() -> bool {
    false
}

/// Typed params for `memory.consolidate`. Mirrors the MCP wire shape.
#[derive(Deserialize, Debug, Clone)]
pub struct ConsolidateParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub brain_id: Option<String>,
    #[serde(default = "default_gap_seconds")]
    pub gap_seconds: i64,
    #[serde(default = "default_auto_summarize")]
    pub auto_summarize: bool,
}

/// A group of temporally proximate episodes with a suggested
/// consolidation. Mirrors the original brain_lib::consolidation shape.
#[derive(Debug, Clone)]
pub struct ConsolidationCluster {
    pub episode_ids: Vec<String>,
    pub episodes: Vec<SummaryRow>,
    pub suggested_title: String,
    pub summary: String,
}

/// Result of [`consolidate_episodes`].
#[derive(Debug, Clone, Default)]
pub struct ConsolidateResult {
    pub clusters: Vec<ConsolidationCluster>,
}

/// Run the consolidate operation end-to-end: load recent episodes,
/// group them by temporal proximity, optionally enqueue async
/// summarisation jobs, and emit the wire-format JSON envelope.
pub fn run_as_json(db: &Db, default_brain_id: &str, params: ConsolidateParams) -> Result<Value> {
    // The MCP contract treats `Some("")` as "current brain" — same as
    // `None`. Filter empty strings before the unwrap so an empty
    // brain_id parameter does not change episode-listing scope or
    // queued summarization jobs.
    let effective_brain_id = params
        .brain_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(default_brain_id)
        .to_string();
    let limit = params.limit.min(500);

    let episodes = {
        let brain_id = effective_brain_id.clone();
        db.with_read_conn(move |conn| summaries::list_episodes(conn, limit, &brain_id))
            .into_brain_core()?
    };

    let result = consolidate_episodes(episodes, params.gap_seconds);

    let jobs_enqueued = if params.auto_summarize {
        enqueue_cluster_summarization(db, &result.clusters, &effective_brain_id)?
    } else {
        0
    };

    let clusters_json: Vec<Value> = result
        .clusters
        .iter()
        .map(|c| {
            json!({
                "episode_ids": c.episode_ids,
                "episode_count": c.episodes.len(),
                "suggested_title": c.suggested_title,
                "summary": c.summary,
                "oldest_ts": c.episodes.iter().map(|e| e.created_at).min(),
                "newest_ts": c.episodes.iter().map(|e| e.created_at).max(),
            })
        })
        .collect();

    Ok(json!({
        "cluster_count": clusters_json.len(),
        "jobs_enqueued": jobs_enqueued,
        "clusters": clusters_json,
    }))
}

/// Group `episodes` into temporal clusters by `gap_seconds`. Pure;
/// no I/O. Reverse-chronological so newest cluster is first.
pub fn consolidate_episodes(episodes: Vec<SummaryRow>, gap_seconds: i64) -> ConsolidateResult {
    if episodes.is_empty() {
        return ConsolidateResult::default();
    }

    let mut sorted = episodes;
    sorted.sort_by_key(|e| e.created_at);

    let mut raw_clusters: Vec<Vec<SummaryRow>> = Vec::new();
    let mut current_cluster: Vec<SummaryRow> = Vec::new();

    for episode in sorted {
        if let Some(last) = current_cluster.last()
            && episode.created_at - last.created_at > gap_seconds
        {
            raw_clusters.push(current_cluster);
            current_cluster = Vec::new();
        }
        current_cluster.push(episode);
    }
    if !current_cluster.is_empty() {
        raw_clusters.push(current_cluster);
    }

    let mut clusters: Vec<ConsolidationCluster> = raw_clusters
        .into_iter()
        .map(|group| {
            let episode_ids: Vec<String> = group.iter().map(|e| e.summary_id.clone()).collect();
            let suggested_title =
                group
                    .first()
                    .and_then(|e| e.title.clone())
                    .unwrap_or_else(|| {
                        let ts = group.first().map(|e| e.created_at).unwrap_or(0);
                        format!("Episodes from {}", format_date(ts))
                    });
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

    clusters.reverse();

    ConsolidateResult { clusters }
}

/// Enqueue async consolidation jobs for the provided clusters.
/// Inlined from the original brain_lib::pipeline::job_worker helper so
/// brain_memory does not take a back-dep on brain_lib.
pub fn enqueue_cluster_summarization(
    queue: &dyn JobQueue,
    clusters: &[ConsolidationCluster],
    brain_id: &str,
) -> Result<usize> {
    let mut count = 0;

    for (cluster_index, cluster) in clusters.iter().enumerate() {
        let episodes = cluster
            .episodes
            .iter()
            .map(|episode| episode.content.as_str())
            .collect::<Vec<_>>()
            .join("\n\n---\n\n");

        if episodes.is_empty() {
            continue;
        }

        let input = EnqueueJobInput {
            payload: JobPayload::ConsolidateCluster {
                cluster_index,
                suggested_title: cluster.suggested_title.clone(),
                episode_ids: cluster.episode_ids.clone(),
                episodes,
                brain_id: brain_id.to_string(),
            },
            priority: jobs::priority::NORMAL,
            retry_config: None,
            stuck_threshold_secs: None,
            metadata: serde_json::json!({}),
            scheduled_at: 0,
        };
        queue.enqueue_job(&input)?;
        count += 1;
    }

    Ok(count)
}

/// Format a Unix timestamp as YYYY-MM-DD. Returns "unknown" for
/// non-positive timestamps. Inlined from brain_lib::consolidation.
fn format_date(ts: i64) -> String {
    if ts <= 0 {
        return "unknown".to_string();
    }
    let days_since_epoch = ts / 86400;
    let (y, m, d) = days_to_ymd(days_since_epoch.clamp(0, u32::MAX as i64) as u32);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Julian-day algorithm: convert days-since-epoch (1970-01-01) to
/// (year, month, day). Inlined from brain_lib::consolidation.
fn days_to_ymd(days: u32) -> (u32, u32, u32) {
    let jdn = days + 2_440_588;
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
