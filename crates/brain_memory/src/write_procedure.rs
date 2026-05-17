//! `memory.write_procedure` — store a step-by-step reusable procedure.
//!
//! Writes a new row with `kind = 'procedure'` into the `summaries`
//! table. Link handling (the MCP `links` parameter) is intentionally
//! NOT in scope here — that machinery lives in the MCP wrapper because
//! it depends on the inline-links framing only the MCP surface exposes.

use brain_core::error::Result;
use brain_core::uri::SynapseUri;
use brain_persistence::db::Db;
use brain_persistence::db::summaries;
use brain_persistence::sql::SqlResultExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

fn default_importance() -> f64 {
    0.9
}

/// Typed params for `memory.write_procedure`. Mirrors the MCP wire
/// shape. The `links` parameter is not modelled here — callers that
/// need link handling do it at the MCP wrapper layer after the
/// procedure persists.
#[derive(Deserialize, Debug, Clone)]
pub struct WriteProcedureParams {
    pub title: String,
    pub steps: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_importance")]
    pub importance: f64,
}

/// Result of a procedure write.
#[derive(Serialize, Debug, Clone)]
pub struct WriteProcedureResult {
    pub summary_id: String,
    pub uri: String,
    pub title: String,
    pub tags: Vec<String>,
    pub importance: f64,
}

/// Run the write-procedure operation.
///
/// `db` must reference the caller's-brain database. `brain_id` identifies
/// the brain the procedure belongs to (stored on the row). `brain_name`
/// is used to build the synapse URI in the result.
///
/// `importance` is clamped to `[0.0, 1.0]` at the boundary.
pub fn run(
    db: &Db,
    brain_id: &str,
    brain_name: &str,
    params: WriteProcedureParams,
) -> Result<WriteProcedureResult> {
    let importance = params.importance.clamp(0.0, 1.0);
    let title = params.title.clone();
    let steps = params.steps.clone();
    let tags = params.tags.clone();
    let brain_id_owned = brain_id.to_string();

    let summary_id = db
        .with_write_conn(move |conn| {
            summaries::store_procedure(conn, &title, &steps, &tags, importance, &brain_id_owned)
        })
        .into_brain_core()?;

    let uri = SynapseUri::for_procedure(brain_name, &summary_id).to_string();

    Ok(WriteProcedureResult {
        summary_id,
        uri,
        title: params.title,
        tags: params.tags,
        // Return the clamped value so the response reflects what was
        // actually persisted, not what the caller sent. A caller that
        // passed `importance: 1.5` sees `1.0` in the response rather
        // than the un-clamped input.
        importance,
    })
}

/// JSON-shape variant used by the MCP wrapper to preserve the exact
/// wire format (`status: "stored"` envelope).
pub fn run_as_json(
    db: &Db,
    brain_id: &str,
    brain_name: &str,
    params: WriteProcedureParams,
) -> Result<Value> {
    let result = run(db, brain_id, brain_name, params)?;
    Ok(json!({
        "status": "stored",
        "summary_id": result.summary_id,
        "uri": result.uri,
        "title": result.title,
        "tags": result.tags,
        "importance": result.importance,
    }))
}
