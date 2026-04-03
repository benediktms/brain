//! Record capsule generation.
//!
//! Builds short text capsules from record metadata for hybrid search indexing.
//! Capsules include title, kind, description summary, and tags — but NOT the
//! full content from the object store (which may be large or binary).

use crate::tokens::estimate_tokens;
use crate::uri::SynapseUri;
use brain_persistence::db::Db;
use brain_persistence::db::lod_chunks::{self, InsertLodChunk};

/// Build a capsule string from a record's metadata.
///
/// Format: "{title}. Kind: {kind}. {desc_summary}. Tags: {tags}."
/// - `desc_summary`: first sentence (find `. ` or `\n`, max 200 chars); omit if None/empty
/// - Tags segment omitted if tags is empty
/// - Kind always included (primary classification signal for ranking)
pub fn build_record_capsule(
    title: &str,
    kind: &str,
    description: Option<&str>,
    tags: &[String],
) -> String {
    let mut parts = vec![title.to_string()];

    parts.push(format!("Kind: {kind}"));

    if let Some(desc) = description {
        let desc = desc.trim();
        if !desc.is_empty() {
            // Take first sentence (up to ". " or "\n"), max 200 chars
            let end = desc
                .find(". ")
                .map(|p| p + 1)
                .or_else(|| desc.find('\n'))
                .unwrap_or(desc.len());
            let summary = desc[..end.min(200)].trim_end_matches('.');
            if !summary.is_empty() {
                parts.push(summary.to_string());
            }
        }
    }

    if !tags.is_empty() {
        parts.push(format!("Tags: {}", tags.join(", ")));
    }

    parts.join(". ") + "."
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_title_and_kind_only() {
        let capsule = build_record_capsule("Auth middleware rewrite", "analysis", None, &[]);
        assert_eq!(capsule, "Auth middleware rewrite. Kind: analysis.");
    }

    #[test]
    fn test_with_description() {
        let capsule = build_record_capsule(
            "Storage plan",
            "plan",
            Some("Migrate to B2 for cold storage. Then consolidate workspace."),
            &[],
        );
        assert!(capsule.contains("Storage plan"));
        assert!(capsule.contains("Kind: plan"));
        assert!(capsule.contains("Migrate to B2 for cold storage"));
        assert!(!capsule.contains("Then consolidate workspace"));
    }

    #[test]
    fn test_with_tags() {
        let tags = vec!["area:core".to_string(), "type:refactor".to_string()];
        let capsule = build_record_capsule("Prefix refactor", "review", None, &tags);
        assert!(capsule.contains("Tags: area:core, type:refactor"));
    }

    #[test]
    fn test_long_description_truncated() {
        let long_desc = "A".repeat(300);
        let capsule = build_record_capsule("Long desc", "document", Some(&long_desc), &[]);
        // Kind segment + 200 char desc + title — total should be well under 300
        assert!(capsule.contains(&"A".repeat(200)));
        assert!(!capsule.contains(&"A".repeat(201)));
    }

    #[test]
    fn test_description_splits_at_period() {
        let desc = "First sentence. Second sentence.";
        let capsule = build_record_capsule("Title", "analysis", Some(desc), &[]);
        assert!(capsule.contains("First sentence"));
        assert!(!capsule.contains("Second sentence"));
    }

    #[test]
    fn test_description_splits_at_newline() {
        let desc = "First line\nSecond line";
        let capsule = build_record_capsule("Title", "plan", Some(desc), &[]);
        assert!(capsule.contains("First line"));
        assert!(!capsule.contains("Second line"));
    }

    #[test]
    fn test_empty_description_omitted() {
        let capsule = build_record_capsule("Title", "review", Some(""), &[]);
        assert_eq!(capsule, "Title. Kind: review.");
    }

    #[test]
    fn test_whitespace_description_omitted() {
        let capsule = build_record_capsule("Title", "review", Some("   "), &[]);
        assert_eq!(capsule, "Title. Kind: review.");
    }

    #[test]
    fn test_empty_tags_omitted() {
        let capsule = build_record_capsule("Title", "analysis", None, &[]);
        assert!(!capsule.contains("Tags:"));
    }

    #[test]
    fn test_full_capsule() {
        let tags = vec!["area:records".to_string()];
        let capsule = build_record_capsule(
            "Record search",
            "implementation",
            Some("Added hybrid retrieval for records. Then verified it works."),
            &tags,
        );
        assert_eq!(
            capsule,
            "Record search. Kind: implementation. Added hybrid retrieval for records. Tags: area:records."
        );
    }
}

/// Best-effort L0 LOD upsert for a record capsule.
///
/// Uses the chunk_id format (`{file_id}:0`) for the URI to match the lookup
/// path in `lod_resolver::build_object_uri`.
pub fn upsert_record_lod_l0(db: &Db, file_id: &str, capsule_text: &str, brain_id: &str) {
    let chunk_id = format!("{file_id}:0");
    let lod_uri = SynapseUri::for_record(brain_id, &chunk_id).to_string();
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
        tracing::warn!(
            file_id = %file_id,
            error = %e,
            "upsert_record_lod_l0: failed to write L0 LOD chunk"
        );
    }
}
