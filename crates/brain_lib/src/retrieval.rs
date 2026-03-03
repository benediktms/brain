/// Token-budgeted minimal-first retrieval.
///
/// Provides a two-tier API:
/// - `search_minimal`: returns compact stubs within a token budget
/// - `expand`: returns full content for selected memory IDs
use crate::ranking::RankedResult;
use crate::tokens::estimate_tokens;

/// Trait for types that can be expanded to full memory content.
///
/// Implemented by `RankedResult` (for backward compatibility) and by
/// `ExpandableChunk` (used by `QueryPipeline::expand` to avoid constructing
/// dummy `RankedResult` objects with zero scores).
pub trait Expandable {
    fn chunk_id(&self) -> &str;
    fn content(&self) -> &str;
    fn file_path(&self) -> &str;
    fn heading_path(&self) -> &str;
    fn token_estimate(&self) -> usize;
}

/// A compact memory stub for search results.
#[derive(Debug, Clone)]
pub struct MemoryStub {
    pub memory_id: String,
    pub title: String,
    pub summary_2sent: String,
    pub hybrid_score: f64,
    pub file_path: String,
    pub heading_path: String,
    pub token_estimate: usize,
}

/// Result of a search_minimal call.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub budget_tokens: usize,
    pub used_tokens_est: usize,
    pub num_results: usize,
    pub total_available: usize,
    pub results: Vec<MemoryStub>,
}

/// A fully expanded memory entry.
#[derive(Debug, Clone)]
pub struct ExpandedMemory {
    pub memory_id: String,
    pub content: String,
    pub file_path: String,
    pub heading_path: String,
    pub byte_start: usize,
    pub byte_end: usize,
    pub truncated: bool,
}

/// Result of an expand call.
#[derive(Debug, Clone)]
pub struct ExpandResult {
    pub budget_tokens: usize,
    pub used_tokens_est: usize,
    pub memories: Vec<ExpandedMemory>,
}

/// A lightweight struct for expanding chunks without requiring a full `RankedResult`.
///
/// Used by `QueryPipeline::expand` to avoid constructing dummy `RankedResult`
/// objects with zero scores just to call `expand_results`.
#[derive(Debug, Clone)]
pub struct ExpandableChunk {
    pub chunk_id: String,
    pub content: String,
    pub file_path: String,
    pub heading_path: String,
    pub token_estimate: usize,
}

impl Expandable for ExpandableChunk {
    fn chunk_id(&self) -> &str {
        &self.chunk_id
    }
    fn content(&self) -> &str {
        &self.content
    }
    fn file_path(&self) -> &str {
        &self.file_path
    }
    fn heading_path(&self) -> &str {
        &self.heading_path
    }
    fn token_estimate(&self) -> usize {
        self.token_estimate
    }
}

/// Pack ranked results into compact stubs within a token budget.
///
/// Iterates ranked results in score order, adding stubs until the
/// token budget is exhausted. Returns at most `k` results.
pub fn pack_minimal(ranked: &[RankedResult], budget_tokens: usize, k: usize) -> SearchResult {
    let total_available = ranked.len();
    let mut results = Vec::new();
    let mut used_tokens = 0;

    for result in ranked.iter().take(k) {
        let stub = make_stub(result);
        let stub_tokens = estimate_stub_tokens(&stub);

        if used_tokens + stub_tokens > budget_tokens && !results.is_empty() {
            break;
        }

        used_tokens += stub_tokens;
        results.push(stub);
    }

    SearchResult {
        budget_tokens,
        used_tokens_est: used_tokens,
        num_results: results.len(),
        total_available,
        results,
    }
}

/// Expand a set of results to full content within a token budget.
///
/// Packs greedily in the given order. If the last entry exceeds the
/// remaining budget, its content is truncated with a `[truncated]` marker.
///
/// Accepts any type implementing `Expandable`, including `RankedResult` and
/// `ExpandableChunk`.
pub fn expand_results<T: Expandable>(results: &[T], budget_tokens: usize) -> ExpandResult {
    let mut memories = Vec::new();
    let mut used_tokens = 0;

    for result in results {
        let content_tokens = result.token_estimate();

        if used_tokens + content_tokens > budget_tokens {
            // Try to fit a truncated version
            let remaining = budget_tokens.saturating_sub(used_tokens);
            if remaining > 20 {
                // Truncate content to fit remaining budget
                let truncated_content =
                    truncate_to_tokens(result.content(), remaining.saturating_sub(5));
                let actual_tokens = estimate_tokens(&truncated_content) + 5; // +5 for [truncated] marker
                memories.push(ExpandedMemory {
                    memory_id: result.chunk_id().to_string(),
                    content: format!("{truncated_content}\n[truncated]"),
                    file_path: result.file_path().to_string(),
                    heading_path: result.heading_path().to_string(),
                    byte_start: 0,
                    byte_end: 0,
                    truncated: true,
                });
                used_tokens += actual_tokens;
            }
            break;
        }

        memories.push(ExpandedMemory {
            memory_id: result.chunk_id().to_string(),
            content: result.content().to_string(),
            file_path: result.file_path().to_string(),
            heading_path: result.heading_path().to_string(),
            byte_start: 0,
            byte_end: 0,
            truncated: false,
        });
        used_tokens += content_tokens;
    }

    ExpandResult {
        budget_tokens,
        used_tokens_est: used_tokens,
        memories,
    }
}

fn make_stub(result: &RankedResult) -> MemoryStub {
    // Title: use heading_path leaf, or first line of content
    let title = if !result.heading_path.is_empty() {
        result
            .heading_path
            .rsplit(" > ")
            .next()
            .unwrap_or(&result.heading_path)
            .trim_start_matches('#')
            .trim()
            .to_string()
    } else {
        result
            .content
            .lines()
            .next()
            .unwrap_or("")
            .chars()
            .take(80)
            .collect()
    };

    // Summary: first two sentences
    let summary_2sent = first_n_sentences(&result.content, 2);

    let stub_tokens = estimate_tokens(&title) + estimate_tokens(&summary_2sent) + 5;

    MemoryStub {
        memory_id: result.chunk_id.clone(),
        title,
        summary_2sent,
        hybrid_score: result.hybrid_score,
        file_path: result.file_path.clone(),
        heading_path: result.heading_path.clone(),
        token_estimate: stub_tokens,
    }
}

fn estimate_stub_tokens(stub: &MemoryStub) -> usize {
    stub.token_estimate
}

fn first_n_sentences(text: &str, n: usize) -> String {
    let mut count = 0;
    let mut end = 0;

    for (i, c) in text.char_indices() {
        if (c == '.' || c == '!' || c == '?') && i + 1 < text.len() {
            count += 1;
            end = i + 1;
            if count >= n {
                break;
            }
        }
    }

    if count == 0 {
        // No sentence boundary — take first 200 chars
        let end = text
            .char_indices()
            .nth(200)
            .map(|(i, _)| i)
            .unwrap_or(text.len());
        text[..end].trim().to_string()
    } else {
        text[..end].trim().to_string()
    }
}

fn truncate_to_tokens(text: &str, max_tokens: usize) -> String {
    let max_chars = max_tokens * 4;
    if text.len() <= max_chars {
        return text.to_string();
    }

    // Find a good break point (sentence or word boundary)
    let search = &text[..max_chars];
    let break_pos = search
        .rfind(". ")
        .or_else(|| search.rfind(' '))
        .unwrap_or(max_chars);

    text[..break_pos].trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ranking::{RankedResult, SignalScores};

    fn make_ranked(id: &str, score: f64, content: &str) -> RankedResult {
        let token_estimate = estimate_tokens(content);
        RankedResult {
            chunk_id: id.to_string(),
            hybrid_score: score,
            scores: SignalScores {
                vector: score,
                keyword: 0.0,
                recency: 0.0,
                links: 0.0,
                tag_match: 0.0,
                importance: 1.0,
            },
            file_path: format!("/notes/{id}.md"),
            heading_path: format!("## {id}"),
            content: content.to_string(),
            token_estimate,
        }
    }

    #[test]
    fn test_pack_minimal_within_budget() {
        let ranked = vec![
            make_ranked("a", 0.9, "First result content here."),
            make_ranked("b", 0.8, "Second result content here."),
            make_ranked("c", 0.7, "Third result content here."),
        ];

        let result = pack_minimal(&ranked, 1000, 10);
        assert_eq!(result.num_results, 3);
        assert!(result.used_tokens_est <= result.budget_tokens);
        assert_eq!(result.total_available, 3);
    }

    #[test]
    fn test_pack_minimal_respects_budget() {
        let ranked = vec![
            make_ranked("a", 0.9, "Content A."),
            make_ranked("b", 0.8, "Content B."),
            make_ranked("c", 0.7, "Content C."),
        ];

        // Very tight budget — should only fit 1 stub
        let result = pack_minimal(&ranked, 15, 10);
        assert!(result.num_results <= 2);
        assert!(result.used_tokens_est <= 15);
    }

    #[test]
    fn test_pack_minimal_respects_k() {
        let ranked = vec![
            make_ranked("a", 0.9, "Content."),
            make_ranked("b", 0.8, "Content."),
            make_ranked("c", 0.7, "Content."),
        ];

        let result = pack_minimal(&ranked, 10000, 2);
        assert_eq!(result.num_results, 2);
    }

    #[test]
    fn test_pack_minimal_empty() {
        let result = pack_minimal(&[], 1000, 10);
        assert_eq!(result.num_results, 0);
        assert_eq!(result.used_tokens_est, 0);
    }

    #[test]
    fn test_expand_within_budget() {
        let ranked = vec![
            make_ranked("a", 0.9, "Full content of result A."),
            make_ranked("b", 0.8, "Full content of result B."),
        ];

        let result = expand_results(&ranked, 1000);
        assert_eq!(result.memories.len(), 2);
        assert!(!result.memories[0].truncated);
        assert!(!result.memories[1].truncated);
        assert!(result.used_tokens_est <= result.budget_tokens);
    }

    #[test]
    fn test_expand_truncates_last() {
        let long_content = "This is a sentence. ".repeat(50);
        let ranked = vec![
            make_ranked("a", 0.9, "Short."),
            make_ranked("b", 0.8, &long_content),
        ];

        // Budget large enough for first, but not all of second
        let result = expand_results(&ranked, 100);
        assert_eq!(result.memories.len(), 2);
        assert!(!result.memories[0].truncated);
        assert!(result.memories[1].truncated);
        assert!(result.memories[1].content.contains("[truncated]"));
    }

    #[test]
    fn test_expand_empty() {
        let result = expand_results::<RankedResult>(&[], 1000);
        assert!(result.memories.is_empty());
        assert_eq!(result.used_tokens_est, 0);
    }

    #[test]
    fn test_first_n_sentences() {
        assert_eq!(
            first_n_sentences("Hello world. Goodbye.", 1),
            "Hello world."
        );
        assert_eq!(
            first_n_sentences("First. Second. Third.", 2),
            "First. Second."
        );
        assert_eq!(first_n_sentences("No period", 2), "No period");
    }

    #[test]
    fn test_stub_has_title_from_heading() {
        let ranked = vec![make_ranked("test", 0.9, "Some content.")];
        let result = pack_minimal(&ranked, 1000, 10);
        assert_eq!(result.results[0].title, "test");
    }
}
