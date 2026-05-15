//! Record capsule generation.
//!
//! Builds short text capsules from record metadata for hybrid search indexing.
//! Capsules include title, kind, description summary, and tags — but NOT the
//! full content from the object store (which may be large or binary).

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
