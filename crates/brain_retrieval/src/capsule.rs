//! Capsule (short summary) generation for search results.
//!
//! Rule-based, deterministic — no LLM dependency. The retrieval-side helper
//! [`generate_stub_capsule`] composes a compact `title — first sentence`
//! summary from raw strings. The `ParsedDocument`-based variant lives in
//! `brain_lib::capsule` because it's only used by the indexing pipeline.

/// Generate a capsule from raw parts (used by the retrieval pipeline where
/// we don't have a full `ParsedDocument`).
///
/// Assembles: title — first sentence — (no heading outline at chunk level).
pub fn generate_stub_capsule(title: Option<&str>, content: &str) -> String {
    let mut parts = Vec::new();

    if let Some(t) = title {
        let t = t.trim();
        if !t.is_empty() {
            parts.push(t.to_string());
        }
    }

    if let Some(sentence) = first_sentence_from_text(content) {
        parts.push(sentence);
    }

    parts.join(" — ")
}

/// Extract the first meaningful sentence from raw text content.
fn first_sentence_from_text(text: &str) -> Option<String> {
    let content = text.trim();
    if content.is_empty() {
        return None;
    }

    let sentence = if let Some(pos) = content.find(". ") {
        &content[..=pos]
    } else if content.ends_with('.') {
        // Cap single-sentence-with-period chunks at 200 chars to match the
        // newline branch — keeps the capsule bounded for long single sentences.
        let end = content.len().min(200);
        &content[..end]
    } else {
        let end = content.find('\n').unwrap_or_else(|| content.len().min(200));
        &content[..end]
    };

    let sentence = sentence.trim();
    if sentence.is_empty() {
        None
    } else {
        Some(sentence.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_capsule_with_title_and_content() {
        let out = generate_stub_capsule(Some("Hello"), "World. More content.");
        assert!(out.starts_with("Hello"));
        assert!(out.contains("World."));
    }

    #[test]
    fn stub_capsule_empty_title_is_skipped() {
        let out = generate_stub_capsule(Some("  "), "Body sentence.");
        assert!(!out.starts_with(" "));
        assert!(out.contains("Body sentence"));
    }

    #[test]
    fn stub_capsule_empty_content_yields_only_title() {
        let out = generate_stub_capsule(Some("Title"), "");
        assert_eq!(out, "Title");
    }

    #[test]
    fn stub_capsule_no_title_no_content_is_empty() {
        let out = generate_stub_capsule(None, "");
        assert!(out.is_empty());
    }
}
