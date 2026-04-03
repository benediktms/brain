/// Extractive L0 abstract generation for record embedding.
///
/// Produces a compact, semantically dense text from a record's metadata and
/// content. This abstract is used as the embedding source instead of the full
///
/// ⚠️ DEPRECATED: Use `crate::records::capsule::build_record_capsule` instead.
/// This function is kept for backward compatibility but may be phased out.
/// The capsule approach uses description-based summaries (consistent with
/// episodes/reflections/procedures) rather than content extraction.
const MAX_CONTENT_CHARS: usize = 500;

/// Generate an extractive L0 abstract for a record.
///
/// The abstract is formatted as:
/// ```text
/// {title}
///
/// {first_sentences_or_truncated_content}
///
/// Tags: {tags}
/// ```
///
/// When `content` is shorter than `MAX_CONTENT_CHARS`, it is used verbatim.
/// When `content` exceeds the limit, it is truncated at a sentence boundary
/// (`.`, `!`, `?`) within the limit, or hard-truncated at the char boundary.
pub fn generate_l0_abstract(title: &str, content: &str, tags: &[&str]) -> String {
    let excerpt = extract_excerpt(content);
    let tags_str = if tags.is_empty() {
        String::new()
    } else {
        format!("\n\nTags: {}", tags.join(", "))
    };
    format!("{title}\n\n{excerpt}{tags_str}")
}

/// Extract the leading excerpt from content.
///
/// Returns the full content when it fits within `MAX_CONTENT_CHARS`.
/// Otherwise truncates at the last sentence-ending punctuation within the
/// limit, falling back to a hard char-boundary cut.
fn extract_excerpt(content: &str) -> &str {
    if content.len() <= MAX_CONTENT_CHARS {
        return content;
    }
    // Find a valid UTF-8 char boundary at or before MAX_CONTENT_CHARS.
    let safe_end = content
        .char_indices()
        .take_while(|(i, _)| *i < MAX_CONTENT_CHARS)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0);
    let window = &content[..safe_end];
    // Find the last sentence boundary within the window.
    let boundary = window.rfind(['.', '!', '?']).map(|pos| pos + 1); // include the punctuation character
    match boundary {
        Some(pos) => &content[..pos],
        None => window,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_content_passes_through_verbatim() {
        let content = "A brief record.";
        let result = generate_l0_abstract("My Title", content, &["rust", "test"]);
        assert!(
            result.contains(content),
            "full content must appear unchanged"
        );
        assert!(result.starts_with("My Title"), "title must be first");
        assert!(result.contains("Tags: rust, test"), "tags must be present");
    }

    #[test]
    fn long_content_is_truncated() {
        // Build content well over MAX_CONTENT_CHARS with clear sentence ends.
        let sentence = "This is a sentence. ";
        let content: String = sentence.repeat(60); // ~1200 chars
        let result = generate_l0_abstract("Long Record", &content, &[]);
        let excerpt_end = result.find("\n\nTags:").unwrap_or(result.len());
        // Skip the title line
        let title_end = "Long Record\n\n".len();
        let excerpt = &result[title_end..excerpt_end];
        assert!(
            excerpt.len() < content.len(),
            "excerpt must be shorter than full content"
        );
        assert!(
            excerpt.len() <= MAX_CONTENT_CHARS,
            "excerpt must not exceed MAX_CONTENT_CHARS"
        );
    }

    #[test]
    fn abstract_contains_title_and_first_sentences() {
        let content = "First sentence here. Second sentence here. Third sentence here. ".repeat(30);
        let result = generate_l0_abstract("Important Record", &content, &["search", "embed"]);
        assert!(result.starts_with("Important Record\n\n"), "title prefix");
        assert!(
            result.contains("First sentence here."),
            "first sentence must appear"
        );
        assert!(result.contains("Tags: search, embed"), "tags must appear");
    }

    #[test]
    fn no_tags_omits_tags_line() {
        let content = "Short content.";
        let result = generate_l0_abstract("Tagless", content, &[]);
        assert!(
            !result.contains("Tags:"),
            "tags line must be absent when no tags provided"
        );
    }
}
