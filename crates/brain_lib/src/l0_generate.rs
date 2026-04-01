//! Extractive L0 abstract generation for note chunks.
//!
//! Produces a compact, semantically dense text from a chunk's heading path
//! and content. Used as the L0 representation in the LOD system — the
//! ultra-concise abstract (~100 tokens) for fast candidate scoring.

use regex::Regex;
use std::sync::LazyLock;

/// Maximum character budget for the L0 abstract (~100 tokens).
const MAX_L0_CHARS: usize = 400;

/// Regex for backtick-quoted code tokens (e.g., `foo_bar`).
static RE_BACKTICK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"`([^`]{2,40})`").expect("backtick regex"));

/// Regex for UPPER_CASE identifiers (e.g., API_KEY, HTTP_STATUS).
static RE_UPPER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b[A-Z][A-Z0-9_]{2,}\b").expect("upper regex"));

/// Regex for CamelCase identifiers (e.g., IndexPipeline, SynapseUri).
static RE_CAMEL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b[A-Z][a-z]+(?:[A-Z][a-z]+)+\b").expect("camel regex"));

/// Generate an extractive L0 abstract for a note chunk.
///
/// Algorithm (from RETRIEVE_PLUS.md §2.3):
/// 1. Extract first sentence from content
/// 2. Extract up to 5 key noun phrases (backtick tokens, UPPER_CASE, CamelCase)
/// 3. Format as `{heading_path}: {first_sentence}. Key: {terms}`
/// 4. Truncate to ~100 tokens (400 chars) at word boundary
pub fn generate_chunk_l0(heading_path: &str, content: &str) -> String {
    let content = content.trim();
    if content.is_empty() {
        return heading_path.to_string();
    }

    let first_sentence = extract_first_sentence(content);
    let key_terms = extract_key_terms(content);

    let mut result = if heading_path.is_empty() {
        first_sentence.to_string()
    } else {
        format!("{heading_path}: {first_sentence}")
    };

    if !key_terms.is_empty() {
        result.push_str(" | ");
        result.push_str(&key_terms.join(", "));
    }

    truncate_at_word_boundary(&result, MAX_L0_CHARS)
}

/// Extract the first meaningful sentence from content.
///
/// Splits on `. ` (sentence end followed by space) or `\n\n` (paragraph break),
/// takes the first non-empty segment, trimmed.
fn extract_first_sentence(content: &str) -> &str {
    let content = content.trim();
    if content.is_empty() {
        return "";
    }

    // Try paragraph break first (most precise boundary)
    if let Some(pos) = content.find("\n\n") {
        let candidate = content[..pos].trim();
        if !candidate.is_empty() {
            // Within the first paragraph, try sentence boundary
            return find_sentence_end(candidate);
        }
    }

    // No paragraph break — try sentence boundary in full content
    find_sentence_end(content)
}

/// Find the end of the first sentence within text.
fn find_sentence_end(text: &str) -> &str {
    // Look for ". " pattern (sentence end followed by space or newline)
    for (i, _) in text.match_indices(". ") {
        let candidate = &text[..=i];
        if !candidate.is_empty() {
            return candidate;
        }
    }
    // No sentence boundary found — return the full text
    text
}

/// Extract up to 5 key terms from content.
///
/// Priority: backtick-quoted tokens > UPPER_CASE identifiers > CamelCase words.
/// Deduplicates and caps at 5 terms.
fn extract_key_terms(content: &str) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    // Backtick tokens (highest priority)
    for cap in RE_BACKTICK.captures_iter(content) {
        if terms.len() >= 5 {
            break;
        }
        let t = cap[1].to_string();
        if seen.insert(t.clone()) {
            terms.push(t);
        }
    }

    // UPPER_CASE identifiers
    for m in RE_UPPER.find_iter(content) {
        if terms.len() >= 5 {
            break;
        }
        let t = m.as_str().to_string();
        if seen.insert(t.clone()) {
            terms.push(t);
        }
    }

    // CamelCase identifiers
    for m in RE_CAMEL.find_iter(content) {
        if terms.len() >= 5 {
            break;
        }
        let t = m.as_str().to_string();
        if seen.insert(t.clone()) {
            terms.push(t);
        }
    }

    terms
}

/// Truncate text to max_chars at a word boundary.
fn truncate_at_word_boundary(text: &str, max_chars: usize) -> String {
    if text.len() <= max_chars {
        return text.to_string();
    }

    // Find a valid UTF-8 char boundary at or before max_chars
    let safe_end = text
        .char_indices()
        .take_while(|(i, _)| *i < max_chars)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0);

    // Find last space within the safe range
    let truncated = &text[..safe_end];
    match truncated.rfind(' ') {
        Some(pos) if pos > safe_end / 2 => truncated[..pos].to_string(),
        _ => truncated.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_content_passthrough() {
        let result = generate_chunk_l0("## Setup", "Install the package with npm.");
        assert!(result.starts_with("## Setup: "));
        assert!(result.contains("Install the package with npm."));
    }

    #[test]
    fn test_long_content_truncated() {
        let long = "First sentence here. ".repeat(50);
        let result = generate_chunk_l0("# Docs", &long);
        assert!(result.len() <= MAX_L0_CHARS);
    }

    #[test]
    fn test_heading_path_prefix() {
        let result = generate_chunk_l0("# Guide > ## Auth", "Token refresh flow.");
        assert!(result.starts_with("# Guide > ## Auth: Token refresh flow."));
    }

    #[test]
    fn test_empty_heading_path() {
        let result = generate_chunk_l0("", "Some content here.");
        assert_eq!(result, "Some content here.");
    }

    #[test]
    fn test_key_terms_extracted() {
        let content = "The `IndexPipeline` handles BLAKE3 hashing. \
                       Use `SynapseUri` for addressing. API_KEY is required.";
        let result = generate_chunk_l0("## Pipeline", content);
        assert!(
            result.contains("IndexPipeline"),
            "should extract backtick term"
        );
        assert!(
            result.contains("BLAKE3") || result.contains("API_KEY"),
            "should extract UPPER_CASE term"
        );
    }

    #[test]
    fn test_backtick_terms() {
        let content = "Use `foo_bar` and `baz_qux` to configure the system.";
        let terms = extract_key_terms(content);
        assert!(terms.contains(&"foo_bar".to_string()));
        assert!(terms.contains(&"baz_qux".to_string()));
    }

    #[test]
    fn test_camel_case_terms() {
        let content = "The IndexPipeline and QueryPipeline classes.";
        let terms = extract_key_terms(content);
        assert!(terms.contains(&"IndexPipeline".to_string()));
        assert!(terms.contains(&"QueryPipeline".to_string()));
    }

    #[test]
    fn test_max_five_terms() {
        let content = "`a1` `b2` `c3` `d4` `e5` `f6` `g7`";
        let terms = extract_key_terms(content);
        assert_eq!(terms.len(), 5, "should cap at 5 terms");
    }

    #[test]
    fn test_empty_content() {
        let result = generate_chunk_l0("## Empty", "");
        assert_eq!(result, "## Empty");
    }

    #[test]
    fn test_whitespace_only_content() {
        let result = generate_chunk_l0("## Blank", "   \n  \n  ");
        assert_eq!(result, "## Blank");
    }

    #[test]
    fn test_first_sentence_extraction() {
        assert_eq!(
            extract_first_sentence("Hello world. This is second."),
            "Hello world."
        );
        assert_eq!(
            extract_first_sentence("First paragraph.\n\nSecond paragraph."),
            "First paragraph."
        );
        assert_eq!(extract_first_sentence("No period"), "No period");
    }

    #[test]
    fn test_deduplication() {
        let content = "`foo` and `foo` again. FOO_BAR and FOO_BAR.";
        let terms = extract_key_terms(content);
        assert_eq!(
            terms.iter().filter(|t| *t == "foo").count(),
            1,
            "duplicates should be removed"
        );
    }
}
