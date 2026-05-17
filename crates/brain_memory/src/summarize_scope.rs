//! `memory.summarize_scope` — generate or retrieve a derived summary
//! for a directory or tag scope. Composes brain_retrieval's hierarchy
//! summaries with the job-queue-driven LLM refresh path.

use brain_core::error::{BrainCoreError, Result};
use brain_persistence::ports::JobQueue;
use brain_retrieval::hierarchy::{
    DerivedSummaryStore, ScopeType, generate_scope_summary_with_options, get_scope_summary,
};
use serde::Deserialize;
use serde_json::{Value, json};

fn default_async_llm() -> bool {
    true
}

/// Typed params for `memory.summarize_scope`. Mirrors the MCP wire
/// shape (`scope_type` as `"directory"` or `"tag"`).
#[derive(Deserialize, Debug, Clone)]
pub struct SummarizeScopeParams {
    pub scope_type: String,
    pub scope_value: String,
    #[serde(default)]
    pub regenerate: bool,
    #[serde(default = "default_async_llm")]
    pub async_llm: bool,
}

/// Run the summarize-scope operation. Returns the JSON-shaped value the
/// MCP wrapper emits unchanged.
///
/// Generic over a single store that implements both [`DerivedSummaryStore`]
/// (from brain_retrieval) and [`JobQueue`] (from brain_persistence).
/// `BrainStores` satisfies both; `&Db` does too.
pub fn run_as_json<S>(store: &S, params: SummarizeScopeParams) -> Result<Value>
where
    S: DerivedSummaryStore + JobQueue,
{
    let scope_type = match params.scope_type.as_str() {
        "directory" => ScopeType::Directory,
        "tag" => ScopeType::Tag,
        other => {
            return Err(BrainCoreError::Parse(format!(
                "Invalid scope_type \"{other}\": must be \"directory\" or \"tag\""
            )));
        }
    };

    let mut llm_pending = false;

    if params.regenerate {
        let generation = generate_scope_summary_with_options(
            store,
            &scope_type,
            &params.scope_value,
            params.async_llm,
        )?;
        llm_pending = generation.llm_pending;
    }

    if let Some(summary) = get_scope_summary(store, &scope_type, &params.scope_value)? {
        return Ok(json!({
            "scope_type": summary.scope_type,
            "scope_value": summary.scope_value,
            "content": summary.content,
            "stale": summary.stale,
            "llm_pending": llm_pending,
            "generated_at": summary.generated_at,
        }));
    }

    let generation = generate_scope_summary_with_options(
        store,
        &scope_type,
        &params.scope_value,
        params.async_llm,
    )?;
    llm_pending = generation.llm_pending;

    let summary = get_scope_summary(store, &scope_type, &params.scope_value)?
        .ok_or_else(|| BrainCoreError::Database("Summary generation produced no result".into()))?;

    Ok(json!({
        "scope_type": summary.scope_type,
        "scope_value": summary.scope_value,
        "content": summary.content,
        "stale": summary.stale,
        "llm_pending": llm_pending,
        "generated_at": summary.generated_at,
    }))
}
