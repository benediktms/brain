//! `memory.write_episode` — append a new episode (goal, actions,
//! outcome) with optional thread-extension validation.
//!
//! The `continues` predecessor is validated pre-write (exists, same
//! brain, kind=episode) so the typical request never produces an
//! episode without its planned thread edge. Lowering `continues` into
//! a `links` entry + actually inserting the edge is the caller's job
//! — that machinery (the MCP `apply_inline_links` helper) lives at
//! the wrapper layer because it's tied to the inline-links framing.

use brain_core::error::{BrainCoreError, Result};
use brain_core::uri::SynapseUri;
use brain_persistence::db::Db;
use brain_persistence::db::summaries::{self, Episode};
use brain_persistence::sql::SqlResultExt;
use serde::{Deserialize, Serialize};
use tracing::error;

fn default_importance() -> f64 {
    1.0
}

/// Typed params for `memory.write_episode`. The MCP-only `links`
/// parameter is not modelled here — callers compose links at the
/// wrapper layer after the episode persists.
#[derive(Deserialize, Debug, Clone)]
pub struct WriteEpisodeParams {
    pub goal: String,
    pub actions: String,
    pub outcome: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_importance")]
    pub importance: f64,
    #[serde(default)]
    pub continues: Option<String>,
}

/// Result of a successful episode write.
#[derive(Serialize, Debug, Clone)]
pub struct WriteEpisodeResult {
    pub summary_id: String,
    pub uri: String,
    pub goal: String,
    pub tags: Vec<String>,
    pub importance: f64,
    /// Echoed back so the wrapper can lower it into a `continues` link
    /// entry without re-parsing the input params.
    pub continues: Option<String>,
}

/// Run the write-episode operation. Validates the `continues`
/// predecessor (when present), persists the episode, and returns the
/// canonical summary_id + URI.
///
/// `continues` validation rules (matching the MCP tool surface):
/// - non-empty
/// - row exists
/// - same brain as the new episode
/// - predecessor's `kind` is `"episode"`
///
/// Each failure produces a typed [`BrainCoreError::Parse`] with the
/// same message text the original MCP surface emits. The wrapper
/// maps these to `ToolCallResult::error` for the JSON-RPC client.
pub fn run(
    db: &Db,
    brain_id: &str,
    brain_name: &str,
    params: WriteEpisodeParams,
) -> Result<WriteEpisodeResult> {
    if let Some(prev_id) = &params.continues {
        if prev_id.is_empty() {
            return Err(BrainCoreError::Parse(
                "continues: predecessor summary_id must not be empty".into(),
            ));
        }
        match db.get_summary_by_id(prev_id)? {
            None => {
                return Err(BrainCoreError::Parse(format!(
                    "continues: predecessor episode not found: {prev_id}"
                )));
            }
            Some(row) => {
                if row.brain_id != brain_id {
                    return Err(BrainCoreError::Parse(
                        "continues: cross-brain references are not yet supported (predecessor is in a different brain)".into(),
                    ));
                }
                if row.kind != "episode" {
                    return Err(BrainCoreError::Parse(format!(
                        "continues: predecessor must be an episode (got kind: {})",
                        row.kind
                    )));
                }
            }
        }
    }

    let episode = Episode {
        brain_id: brain_id.to_string(),
        goal: params.goal.clone(),
        actions: params.actions,
        outcome: params.outcome,
        tags: params.tags.clone(),
        importance: params.importance,
    };

    let summary_id = db
        .with_write_conn(move |conn| summaries::store_episode(conn, &episode))
        .into_brain_core()
        .map_err(|e| {
            error!(error = %e, "failed to store episode");
            e
        })?;

    let uri = SynapseUri::for_episode(brain_name, &summary_id).to_string();

    Ok(WriteEpisodeResult {
        summary_id,
        uri,
        goal: params.goal,
        tags: params.tags,
        importance: params.importance,
        continues: params.continues,
    })
}
