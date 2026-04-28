//! Task capsule generation and storage.
//!
//! Builds short text capsules from task metadata and stores them into
//! SQLite for BM25/FTS keyword search alongside note chunks.

use std::sync::Arc;

use tracing::warn;

use crate::embedder::{Embed, embed_batch_async};
use crate::error::Result;
use crate::tokens::estimate_tokens;
use crate::uri::SynapseUri;
use brain_persistence::db::Db;
use brain_persistence::db::lod_chunks::{self, InsertLodChunk};
use brain_persistence::store::Store;

/// Maximum description length (in Unicode scalar values) included in a task
/// capsule. Sized to stay comfortably inside BGE-small-en's 512-token window
/// while preserving enough context for keyword (FTS/BM25) recall.
pub const TASK_CAPSULE_DESC_CHAR_CAP: usize = 1000;

/// Build a capsule string from a task's current state.
///
/// Format: "{title}. {desc_summary}. Tags: {labels}. Priority: {priority}."
/// - `desc_summary`: first sentence (find `. ` or `\n`), then truncated to at
///   most [`TASK_CAPSULE_DESC_CHAR_CAP`] Unicode chars; omit if None/empty
/// - Tags segment omitted if labels is empty
/// - Priority segment always included
pub fn build_task_capsule(
    title: &str,
    description: Option<&str>,
    labels: &[String],
    priority: i32,
) -> String {
    let mut parts = vec![title.to_string()];

    if let Some(desc) = description {
        let desc = desc.trim();
        if !desc.is_empty() {
            // Take first sentence (up to ". " or "\n"), then char-truncate.
            // `find` returns valid UTF-8 byte boundaries, and `chars().take(N)`
            // is inherently codepoint-bounded so no manual boundary walk needed.
            let end = desc
                .find(". ")
                .map(|p| p + 1)
                .or_else(|| desc.find('\n'))
                .unwrap_or(desc.len());
            let summary: String = desc[..end]
                .chars()
                .take(TASK_CAPSULE_DESC_CHAR_CAP)
                .collect();
            if !summary.is_empty() {
                parts.push(summary);
            }
        }
    }

    if !labels.is_empty() {
        parts.push(format!("Tags: {}", labels.join(", ")));
    }

    parts.push(format!("Priority: {}", priority_label(priority)));

    parts.join(". ") + "."
}

/// Build an outcome capsule for a closed task.
///
/// Format:
/// - With reason: "{title}. Outcome: {reason}."
/// - Without: "{title}. Completed."
pub fn build_outcome_capsule(title: &str, completion_reason: Option<&str>) -> String {
    match completion_reason {
        Some(reason) => {
            let reason = reason.trim();
            if reason.is_empty() {
                format!("{title}. Completed.")
            } else {
                format!("{title}. Outcome: {reason}.")
            }
        }
        None => format!("{title}. Completed."),
    }
}

/// Upsert a task capsule into SQLite for BM25/FTS indexing.
///
/// This is a low-level helper called after task mutations. Best-effort:
/// errors propagate to the caller.
pub fn store_task_capsule(
    db: &Db,
    file_id: &str, // e.g. "task:BRN-01ABC" or "task-outcome:BRN-01ABC"
    capsule_text: &str,
    brain_id: &str,
) -> Result<()> {
    db.with_write_conn(|conn| {
        brain_persistence::db::chunks::upsert_task_chunk(conn, file_id, capsule_text, brain_id)
    })
}

/// Metadata required to build and embed a task capsule.
pub struct TaskCapsuleParams<'a> {
    pub task_id: &'a str,
    pub title: &'a str,
    pub description: Option<&'a str>,
    pub labels: &'a [String],
    pub priority: i32,
}

/// Build, embed, and store a task capsule into LanceDB + SQLite.
///
/// Standalone version of the embedding logic (no `McpContext` dependency).
/// Both stores use upsert semantics — safe to call repeatedly.
pub async fn embed_task_capsule(
    store: &Store,
    embedder: &Arc<dyn Embed>,
    db: &Db,
    params: TaskCapsuleParams<'_>,
    brain_id: &str,
) -> Result<()> {
    let capsule_text = build_task_capsule(
        params.title,
        params.description,
        params.labels,
        params.priority,
    );
    let file_id = format!("task:{}", params.task_id);

    let embeddings = embed_batch_async(embedder, vec![capsule_text.clone()]).await?;
    store
        .upsert_chunks(
            &file_id,
            params.title,
            brain_id,
            &[(0, &capsule_text)],
            &embeddings,
        )
        .await?;

    store_task_capsule(db, &file_id, &capsule_text, brain_id)?;
    upsert_task_lod_l0(db, &file_id, &capsule_text, brain_id);
    Ok(())
}

/// Build, embed, and store an outcome capsule into LanceDB + SQLite.
///
/// Standalone version of the embedding logic (no `McpContext` dependency).
/// Both stores use upsert semantics — safe to call repeatedly.
pub async fn embed_outcome_capsule(
    store: &Store,
    embedder: &Arc<dyn Embed>,
    db: &Db,
    task_id: &str,
    title: &str,
    completion_reason: Option<&str>,
    brain_id: &str,
) -> Result<()> {
    let capsule_text = build_outcome_capsule(title, completion_reason);
    let file_id = format!("task-outcome:{task_id}");

    let embeddings = embed_batch_async(embedder, vec![capsule_text.clone()]).await?;
    store
        .upsert_chunks(
            &file_id,
            title,
            brain_id,
            &[(0, &capsule_text)],
            &embeddings,
        )
        .await?;

    store_task_capsule(db, &file_id, &capsule_text, brain_id)?;
    upsert_task_lod_l0(db, &file_id, &capsule_text, brain_id);
    Ok(())
}

/// Best-effort L0 LOD upsert for a task/outcome capsule.
///
/// Uses the chunk_id format (`{file_id}:0`) for the URI to match the lookup
/// path in `lod_resolver::build_object_uri`, which receives `ranked.chunk_id`.
pub(crate) fn upsert_task_lod_l0(db: &Db, file_id: &str, capsule_text: &str, brain_id: &str) {
    // chunk_id = "{file_id}:0" — matches what chunks.rs:207 stores in FTS.
    let chunk_id = format!("{file_id}:0");
    let lod_uri = SynapseUri::for_task(brain_id, &chunk_id).to_string();
    let source_hash = crate::utils::content_hash(capsule_text);
    let token_est = estimate_tokens(capsule_text) as i64;
    let now = chrono::Utc::now().to_rfc3339();
    if let Err(e) = db.with_write_conn(|conn| {
        lod_chunks::upsert_lod_chunk(
            conn,
            &InsertLodChunk {
                id: &ulid::Ulid::new().to_string(),
                object_uri: &lod_uri,
                brain_id,
                lod_level: "L0",
                content: capsule_text,
                token_est: Some(token_est),
                method: "extractive",
                model_id: None,
                source_hash: &source_hash,
                created_at: &now,
                expires_at: None,
                job_id: None,
            },
        )
    }) {
        warn!(file_id = %file_id, error = %e, "LOD L0 upsert failed for task capsule");
    }
}

fn priority_label(p: i32) -> &'static str {
    match p {
        0 => "critical",
        1 => "high",
        2 => "medium",
        3 => "low",
        _ => "backlog",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_task_capsule_basic() {
        let capsule = build_task_capsule("Fix the bug", None, &[], 1);
        assert!(capsule.contains("Fix the bug"));
        assert!(capsule.contains("Priority: high"));
        assert!(!capsule.contains("Tags:"));
    }

    #[test]
    fn test_build_task_capsule_with_labels() {
        let labels = vec!["area:memory".to_string(), "type:feature".to_string()];
        let capsule = build_task_capsule(
            "Add search",
            Some("Implement semantic search for tasks."),
            &labels,
            2,
        );
        assert!(capsule.contains("Add search"));
        assert!(capsule.contains("Implement semantic search for tasks"));
        assert!(capsule.contains("Tags: area:memory, type:feature"));
        assert!(capsule.contains("Priority: medium"));
    }

    #[test]
    fn test_build_task_capsule_no_description() {
        let capsule = build_task_capsule("Simple task", None, &[], 3);
        assert!(capsule.contains("Simple task"));
        assert!(capsule.contains("Priority: low"));
        // Should not have a description segment
        assert_eq!(capsule, "Simple task. Priority: low.");
    }

    #[test]
    fn test_build_task_capsule_long_description_truncated() {
        // Use a description larger than the cap with no sentence boundary.
        let long_desc = "A".repeat(TASK_CAPSULE_DESC_CHAR_CAP * 2);
        let capsule = build_task_capsule("Long desc task", Some(&long_desc), &[], 4);
        // Count consecutive 'A's — that's the description segment length.
        let desc_chars = capsule.chars().filter(|c| *c == 'A').count();
        assert_eq!(desc_chars, TASK_CAPSULE_DESC_CHAR_CAP);
    }

    #[test]
    fn test_build_task_capsule_truncation_at_multibyte_char_boundary() {
        // Regression: byte index landing inside a 3-byte em-dash (—) used to
        // panic with "byte index N is not a char boundary". With char-based
        // truncation this can't happen, but exercising the path locks in the
        // invariant in case the implementation is reworked.
        let prefix = "A".repeat(TASK_CAPSULE_DESC_CHAR_CAP - 2);
        let long_desc =
            format!("{prefix}— rest of the description goes here and continues for a while");
        let capsule = build_task_capsule("T", Some(&long_desc), &[], 2);
        assert!(capsule.starts_with("T."));
        // Every char must be a valid scalar (trivially true for any String, but
        // also asserts we never produced sliced bytes mid-codepoint).
        assert!(capsule.is_char_boundary(capsule.len()));
    }

    #[test]
    fn test_build_task_capsule_empty_labels() {
        let capsule = build_task_capsule("Task", None, &[], 2);
        assert!(!capsule.contains("Tags:"));
    }

    #[test]
    fn test_build_outcome_capsule_with_reason() {
        let capsule = build_outcome_capsule("Deploy v2", Some("Deployed successfully"));
        assert!(capsule.contains("Deploy v2"));
        assert!(capsule.contains("Outcome: Deployed successfully"));
    }

    #[test]
    fn test_build_outcome_capsule_no_reason() {
        let capsule = build_outcome_capsule("Simple task", None);
        assert!(capsule.contains("Simple task"));
        assert!(capsule.contains("Completed"));
        assert_eq!(capsule, "Simple task. Completed.");
    }

    #[test]
    fn test_build_outcome_capsule_empty_reason() {
        let capsule = build_outcome_capsule("Task", Some("  "));
        assert_eq!(capsule, "Task. Completed.");
    }

    #[test]
    fn test_priority_labels() {
        assert_eq!(priority_label(0), "critical");
        assert_eq!(priority_label(1), "high");
        assert_eq!(priority_label(2), "medium");
        assert_eq!(priority_label(3), "low");
        assert_eq!(priority_label(4), "backlog");
        assert_eq!(priority_label(5), "backlog");
    }

    #[test]
    fn test_build_task_capsule_description_first_sentence() {
        let desc = "First sentence. Second sentence.";
        let capsule = build_task_capsule("Title", Some(desc), &[], 2);
        // Should include "First sentence." but not necessarily the rest
        assert!(capsule.contains("First sentence"));
        // Should not include "Second sentence" since we only take up to ". "
        assert!(!capsule.contains("Second sentence"));
    }

    #[test]
    fn test_build_task_capsule_description_newline_split() {
        let desc = "First line\nSecond line";
        let capsule = build_task_capsule("Title", Some(desc), &[], 2);
        assert!(capsule.contains("First line"));
        assert!(!capsule.contains("Second line"));
    }
}
