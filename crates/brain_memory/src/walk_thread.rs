//! `memory.walk_thread` — DAG traversal following `continues` edges
//! between episodes. Returns the ordered chain from a given start.
//!
//! Walks bidirectionally — both predecessors and successors of the
//! seed are returned, ordered by `created_at` ASC (with `summary_id`
//! as tiebreaker). Cross-brain rows are filtered out defensively, so
//! a thread that somehow leaked across brain boundaries still returns
//! only the rows belonging to the caller's brain.

use brain_core::error::Result;
use brain_core::uri::SynapseUri;
use brain_persistence::db::Db;
use brain_persistence::db::links::collect_thread_episode_rows;
use brain_persistence::sql::SqlResultExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::warn;

/// Default BFS depth bound when the caller does not specify `max_depth`.
/// Threads are linear via DAG-validated `continues` edges, so 32 hops
/// covers the long tail of typical agent saga lengths without risking
/// pathological neighbourhoods.
pub const DEFAULT_MAX_DEPTH: u32 = 32;

/// Typed params for the walk-thread operation. Mirrors the MCP wire
/// shape (`seed_summary_id` + optional `max_depth`).
#[derive(Deserialize, Debug, Clone)]
pub struct WalkThreadParams {
    pub seed_summary_id: String,
    #[serde(default)]
    pub max_depth: Option<u32>,
}

/// Per-episode entry in a walk_thread response.
#[derive(Serialize, Debug, Clone)]
pub struct WalkedEpisode {
    pub summary_id: String,
    pub uri: String,
    pub kind: &'static str,
    pub title: Option<String>,
    pub content: String,
    pub tags: Vec<String>,
    pub importance: f64,
    pub created_at: i64,
}

/// Result of walking a thread.
#[derive(Serialize, Debug, Clone)]
pub struct WalkThreadResult {
    pub seed_summary_id: String,
    pub count: usize,
    pub truncated: bool,
    pub thread: Vec<WalkedEpisode>,
}

/// Run the walk-thread operation.
///
/// `db` must reference the caller's-brain database. `brain_id` and
/// `brain_name` are used to (a) filter cross-brain rows and (b) build
/// per-episode synapse URIs in the result.
///
/// Returns an empty thread (count=0) for unknown seeds — the MCP
/// surface intentionally treats a missing seed as "thread of zero",
/// not an error.
pub fn run(
    db: &Db,
    brain_id: &str,
    brain_name: &str,
    params: WalkThreadParams,
) -> Result<WalkThreadResult> {
    let max_depth = params.max_depth.unwrap_or(DEFAULT_MAX_DEPTH);

    // Single hydration: the helper returns sorted rows + a truncation
    // flag. No re-query, no re-sort.
    let result = db
        .with_read_conn(|conn| {
            collect_thread_episode_rows(conn, &params.seed_summary_id, max_depth)
        })
        .into_brain_core()?;

    let total_before_filter = result.rows.len();
    let rows: Vec<_> = result
        .rows
        .into_iter()
        .filter(|row| row.brain_id == brain_id)
        .collect();
    if rows.len() < total_before_filter {
        warn!(
            seed = %params.seed_summary_id,
            brain_id = %brain_id,
            dropped = total_before_filter - rows.len(),
            "memory.walk_thread: dropped cross-brain rows from thread"
        );
    }

    let thread: Vec<WalkedEpisode> = rows
        .into_iter()
        .map(|row| WalkedEpisode {
            uri: SynapseUri::for_episode(brain_name, &row.summary_id).to_string(),
            summary_id: row.summary_id,
            kind: "episode",
            title: row.title,
            content: row.content,
            tags: row.tags,
            importance: row.importance,
            created_at: row.created_at,
        })
        .collect();

    Ok(WalkThreadResult {
        count: thread.len(),
        seed_summary_id: params.seed_summary_id,
        truncated: result.truncated,
        thread,
    })
}

/// JSON-shape variant used by the MCP wrapper to preserve the exact
/// wire format that callers depend on. Mirrors the existing MCP tool's
/// response field order.
pub fn run_as_json(
    db: &Db,
    brain_id: &str,
    brain_name: &str,
    params: WalkThreadParams,
) -> Result<Value> {
    let result = run(db, brain_id, brain_name, params)?;
    Ok(json!({
        "seed_summary_id": result.seed_summary_id,
        "count": result.count,
        "truncated": result.truncated,
        "thread": result.thread.into_iter().map(|e| json!({
            "summary_id": e.summary_id,
            "uri": e.uri,
            "kind": e.kind,
            "title": e.title,
            "content": e.content,
            "tags": e.tags,
            "importance": e.importance,
            "created_at": e.created_at,
        })).collect::<Vec<_>>(),
    }))
}
