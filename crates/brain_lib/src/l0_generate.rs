//! Extractive L0 abstract generation for note chunks.
//!
//! Produces a compact, semantically dense text from a chunk's heading path
//! and content. Used as the L0 representation in the LOD system — the
//! ultra-concise abstract (~100 tokens) for fast candidate scoring.

use regex::Regex;
use std::sync::LazyLock;

/// Maximum character budget for the L0 abstract (~100 tokens).
const MAX_L0_CHARS: usize = 400;

/// Maximum character budget for the L1 extractive abstract (~2000 tokens).
const MAX_L1_CHARS: usize = 8000;

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

/// Generate an extractive L0 abstract for an episode.
///
/// Extracts the goal text from episode content by:
/// 1. Taking the first line and stripping the "Goal:" prefix
/// 2. If the goal is empty, falling back to the "Outcome:" line
/// 3. Truncating to MAX_L0_CHARS at word boundary
///
/// Episode content format:
/// ```text
/// Goal: {goal}
/// Actions: {actions}
/// Outcome: {outcome}
/// ```
pub fn generate_episode_l0(content: &str) -> String {
    let content = content.trim();
    if content.is_empty() {
        return String::new();
    }

    // Take first line
    let first_line = content.lines().next().unwrap_or("");

    // Strip "Goal: " prefix if present
    let goal = first_line
        .strip_prefix("Goal:")
        .unwrap_or(first_line)
        .trim();

    if goal.is_empty() {
        // Fall back to Outcome line (skip first line since we already checked it)
        for line in content.lines().skip(1) {
            if let Some(outcome) = line.strip_prefix("Outcome:") {
                let outcome = outcome.trim();
                if !outcome.is_empty() {
                    return truncate_at_word_boundary(outcome, MAX_L0_CHARS);
                }
            }
        }
        return String::new();
    }

    truncate_at_word_boundary(goal, MAX_L0_CHARS)
}

/// Generate an extractive L0 abstract for a reflection.
///
/// Format: "{title}: {first_paragraph}. Key: {terms}"
///
/// For reflections, the L0 captures:
/// - The reflection title (topic)
/// - First paragraph (main insight)
/// - Key terms for semantic specificity
pub fn generate_reflection_l0(title: &str, content: &str) -> String {
    let content = content.trim();

    if content.is_empty() {
        if title.is_empty() {
            return String::new();
        }
        return truncate_at_word_boundary(title, MAX_L0_CHARS);
    }

    let first_paragraph = content.split("\n\n").next().unwrap_or(content).trim();

    let key_terms = extract_key_terms(content);

    let mut result = if title.is_empty() {
        first_paragraph.to_string()
    } else {
        format!("{title}: {first_paragraph}")
    };

    if !key_terms.is_empty() {
        result.push_str(". Key: ");
        result.push_str(&key_terms.join(", "));
    }

    truncate_at_word_boundary(&result, MAX_L0_CHARS)
}

/// Generate an extractive L0 abstract for a procedure.

/// Format: "{title}: {first_step}. Key: {terms}"

/// For procedures, the L0 captures:
/// - The procedure title (what it does)
/// - First step (entry action)
/// - Key terms for semantic specificity
pub fn generate_procedure_l0(title: &str, content: &str) -> String {
    let content = content.trim();

    if content.is_empty() {
        if title.is_empty() {
            return String::new();
        }
        return truncate_at_word_boundary(title, MAX_L0_CHARS);
    }

    // Extract first line/step
    let first_step = content.lines().next().unwrap_or(content).trim();

    let key_terms = extract_key_terms(content);

    let mut result = if title.is_empty() {
        first_step.to_string()
    } else {
        format!("{title}: {first_step}")
    };

    if !key_terms.is_empty() {
        result.push_str(". Key: ");
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

/// Generate an extractive L1 abstract (~2000 tokens) as an LLM fallback.
///
/// Algorithm:
/// 1. Take the first 3 paragraphs (split on `\n\n`) verbatim.
/// 2. If content remains beyond those paragraphs, append key terms extracted
///    from the remainder.
/// 3. Truncate at `MAX_L1_CHARS` (≈8000 chars) at a word boundary.
pub fn generate_extractive_l1(content: &str) -> String {
    let content = content.trim();
    if content.is_empty() {
        return String::new();
    }

    let paragraphs: Vec<&str> = content.split("\n\n").collect();
    let first_three: Vec<&str> = paragraphs.iter().take(3).copied().collect();
    let remainder: Vec<&str> = paragraphs.iter().skip(3).copied().collect();

    let mut result = first_three.join("\n\n");

    if !remainder.is_empty() {
        let remainder_text = remainder.join(" ");
        let terms = extract_key_terms(&remainder_text);
        if !terms.is_empty() {
            result.push_str("\n\n...\n\nKey terms: ");
            result.push_str(&terms.join(", "));
        } else {
            result.push_str("\n\n...");
        }
    }

    truncate_at_word_boundary(&result, MAX_L1_CHARS)
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

    // ── generate_extractive_l1 tests ────────────────────────────

    #[test]
    fn test_extractive_l1_empty_content() {
        assert_eq!(generate_extractive_l1(""), "");
        assert_eq!(generate_extractive_l1("   "), "");
    }

    #[test]
    fn test_extractive_l1_single_paragraph() {
        let content = "This is a single paragraph with some content.";
        let result = generate_extractive_l1(content);
        assert_eq!(result, content);
    }

    #[test]
    fn test_extractive_l1_three_paragraphs_verbatim() {
        let content = "Para one.\n\nPara two.\n\nPara three.";
        let result = generate_extractive_l1(content);
        assert_eq!(result, content);
    }

    #[test]
    fn test_extractive_l1_more_than_three_paragraphs_appends_ellipsis() {
        let content = "Para one.\n\nPara two.\n\nPara three.\n\nPara four.";
        let result = generate_extractive_l1(content);
        assert!(result.starts_with("Para one.\n\nPara two.\n\nPara three."));
        assert!(result.contains("..."), "should contain ellipsis");
    }

    #[test]
    fn test_extractive_l1_key_terms_from_remainder() {
        let content =
            "Para one.\n\nPara two.\n\nPara three.\n\nThe `FooBar` and SOME_CONST are important.";
        let result = generate_extractive_l1(content);
        assert!(
            result.contains("Key terms:"),
            "should include key terms section"
        );
        assert!(result.contains("FooBar") || result.contains("SOME_CONST"));
    }

    #[test]
    fn test_extractive_l1_output_size() {
        // Generate content well over the budget.
        let big_para = "Word ".repeat(500);
        let content = std::iter::repeat(big_para.as_str())
            .take(10)
            .collect::<Vec<_>>()
            .join("\n\n");
        let result = generate_extractive_l1(&content);
        assert!(
            result.len() <= MAX_L1_CHARS,
            "result len {} exceeds budget {}",
            result.len(),
            MAX_L1_CHARS
        );
    }

    #[test]
    fn test_extractive_l1_within_budget_not_truncated() {
        let content = "Short paragraph.\n\nAnother short one.";
        let result = generate_extractive_l1(content);
        assert!(result.len() <= MAX_L1_CHARS);
        assert!(!result.is_empty());
    }

    // ── generate_episode_l0 tests ───────────────────────────────
    #[test]
    fn test_episode_l0_goal_only() {
        let content =
            "Goal: Fix the authentication bug\nActions: Debugged and patched\nOutcome: Bug fixed";
        let result = generate_episode_l0(content);
        assert_eq!(result, "Fix the authentication bug");
    }

    #[test]
    fn test_episode_l0_no_goal_prefix() {
        let content =
            "Fix the authentication bug\nActions: Debugged and patched\nOutcome: Bug fixed";
        let result = generate_episode_l0(content);
        assert_eq!(result, "Fix the authentication bug");
    }

    #[test]
    fn test_episode_l0_no_goal_falls_back_to_outcome() {
        // When Goal: is empty, fall back to Outcome line
        let content = "Goal: \nActions: Debugged and patched\nOutcome: Bug successfully fixed";
        let result = generate_episode_l0(content);
        assert_eq!(result, "Bug successfully fixed");
    }

    #[test]
    fn test_episode_l0_empty() {
        let result = generate_episode_l0("");
        assert_eq!(result, "");
    }

    #[test]
    fn test_episode_l0_whitespace_only() {
        let result = generate_episode_l0("   \n  \n  ");
        assert_eq!(result, "");
    }

    #[test]
    fn test_episode_l0_long_goal_truncated() {
        let long_goal = "A".repeat(500);
        let content = format!(
            "Goal: {}\nActions: Some actions\nOutcome: Some outcome",
            long_goal
        );
        let result = generate_episode_l0(&content);
        assert!(
            result.len() <= MAX_L0_CHARS,
            "result len {} exceeds MAX_L0_CHARS {}",
            result.len(),
            MAX_L0_CHARS
        );
    }

    #[test]
    fn test_episode_l0_actions_only() {
        // When no Goal: prefix, first line is returned as-is (even if it's Actions:)
        let content = "Actions: Debugged and patched";
        let result = generate_episode_l0(content);
        assert_eq!(result, "Actions: Debugged and patched");
    }

    #[test]
    fn test_episode_l0_goal_colon_only() {
        // Edge case: first line is exactly "Goal:" with no content - should fall back to Outcome
        let content = "Goal:\nActions: Debugged and patched\nOutcome: Bug fixed";
        let result = generate_episode_l0(content);
        assert_eq!(result, "Bug fixed");
    }

    #[test]
    fn test_episode_l0_outcome_empty() {
        let content = "Goal: Test goal\nActions: Actions\nOutcome: ";
        let result = generate_episode_l0(content);
        assert_eq!(result, "Test goal");
    }
    // ── generate_reflection_l0 tests ───────────────────────────
    #[test]
    fn test_reflection_l0_full() {
        let content = "The `SynapseUri` pattern enables BLAKE3 hashing for content addressing.\n\nSecond paragraph here.";
        let result = generate_reflection_l0("Architecture Decision", content);
        assert!(result.starts_with("Architecture Decision: The `SynapseUri`"));
        assert!(result.contains(". Key: "), "should include key terms");
        assert!(
            result.contains("SynapseUri"),
            "should include backtick term"
        );
    }

    #[test]
    fn test_reflection_l0_no_title() {
        let content = "First paragraph content.\n\nSecond paragraph.";
        let result = generate_reflection_l0("", content);
        assert_eq!(result, "First paragraph content.");
    }

    #[test]
    fn test_reflection_l0_no_content() {
        let result = generate_reflection_l0("My Reflection", "");
        assert_eq!(result, "My Reflection");
    }

    #[test]
    fn test_reflection_l0_empty() {
        let result = generate_reflection_l0("", "");
        assert_eq!(result, "");
    }

    #[test]
    fn test_reflection_l0_long_truncated() {
        let long_paragraph = "Word ".repeat(200);
        let content = format!("{}\n\nSecond paragraph.", long_paragraph);
        let result = generate_reflection_l0("Title", &content);
        assert!(
            result.len() <= MAX_L0_CHARS,
            "result len {} exceeds MAX_L0_CHARS {}",
            result.len(),
            MAX_L0_CHARS
        );
    }

    #[test]
    fn test_reflection_l0_key_terms_extraction() {
        let content = "The `SynapseUri` pattern enables BLAKE3 hashing. Reflections help consolidate knowledge.";
        let result = generate_reflection_l0("URI Pattern", content);
        assert!(
            result.contains("SynapseUri"),
            "should extract backtick term"
        );
        assert!(
            result.contains(". Key: "),
            "should include key terms section"
        );
    }

    // ── generate_procedure_l0 tests ─────────────────────────────
    #[test]
    fn test_procedure_l0_full() {
        let content = "Run the `migration_script` to update the DATABASE schema.\n\nVerify the changes in production.";
        let result = generate_procedure_l0("Deploy to Production", content);
        assert!(
            result.starts_with("Deploy to Production: Run the `migration_script`"),
            "should include title and first step"
        );
        assert!(
            result.contains(". Key: "),
            "should include key terms section"
        );
    }

    #[test]
    fn test_procedure_l0_no_title() {
        let content = "Run the migration script to update the database schema.\n\nVerify changes.";
        let result = generate_procedure_l0("", content);
        assert_eq!(
            result,
            "Run the migration script to update the database schema."
        );
    }

    #[test]
    fn test_procedure_l0_no_content() {
        let result = generate_procedure_l0("Deploy to Production", "");
        assert_eq!(result, "Deploy to Production");
    }

    #[test]
    fn test_procedure_l0_empty() {
        let result = generate_procedure_l0("", "");
        assert_eq!(result, "");
    }

    #[test]
    fn test_procedure_l0_long_truncated() {
        let long_step = "Word ".repeat(200);
        let content = format!("{}\n\nSecond step.", long_step);
        let result = generate_procedure_l0("Long Procedure", &content);
        assert!(
            result.len() <= MAX_L0_CHARS,
            "result len {} exceeds MAX_L0_CHARS {}",
            result.len(),
            MAX_L0_CHARS
        );
    }

    #[test]
    fn test_procedure_l0_key_terms_extraction() {
        let content = "Run the `migration_script` with DATABASE_URL set. Verify `BLAKE3` hashes.";
        let result = generate_procedure_l0("Deploy", content);
        assert!(
            result.contains("migration_script"),
            "should extract backtick term"
        );
        assert!(
            result.contains(". Key: "),
            "should include key terms section"
        );
    }
}
