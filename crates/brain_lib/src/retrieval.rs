/// Shared retrieval types used by `memory.retrieve`.
///
/// This module defines the result shapes (`SearchResult`, `MemoryStub`),
/// `MemoryKind` enum, and helpers like `pack_results_within_budget` consumed
/// by the unified retrieval tool.
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::capsule::generate_stub_capsule;
use crate::ranking::{ExpansionReason, FusionConfidence, RankedResult, SignalScores};
use crate::tokens::estimate_tokens;

/// Canonical memory result kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryKind {
    Note,
    Episode,
    Reflection,
    Procedure,
    Task,
    TaskOutcome,
    Record,
}

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
    fn byte_start(&self) -> usize;
    fn byte_end(&self) -> usize;
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
    /// Per-signal score breakdown. Present when `pack_minimal` is called with
    /// `include_scores: true`.
    pub signal_scores: Option<SignalScores>,
    /// Result kind: "note" for indexed note chunks, "task" for task capsules,
    /// "task-outcome" for completed task outcome capsules.
    pub kind: String,
    /// Which brain this result came from. `None` for single-brain search.
    pub brain_name: Option<String>,
    /// Discovery channel for this result.
    pub expansion_reason: ExpansionReason,
    /// Zero-indexed slot in the packed result list. Stable across identical
    /// queries against identical state.
    pub lod_plan_slot: usize,
}

/// Result of a search_minimal call.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub budget_tokens: usize,
    pub used_tokens_est: usize,
    pub num_results: usize,
    pub total_available: usize,
    pub results: Vec<MemoryStub>,
    /// Fusion confidence between vector and FTS retrieval paths.
    /// Present when the search used hybrid retrieval.
    pub fusion_confidence: Option<FusionConfidence>,
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
    pub byte_start: usize,
    pub byte_end: usize,
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
    fn byte_start(&self) -> usize {
        self.byte_start
    }
    fn byte_end(&self) -> usize {
        self.byte_end
    }
}

/// Pack ranked results into compact stubs within a token budget.
///
/// Iterates ranked results in score order, adding stubs until the
/// token budget is exhausted. Returns at most `k` results.
///
/// When `include_scores` is true, each stub carries the per-signal score
/// breakdown from ranking.
///
/// `ml_summaries` is a preloaded map from chunk_id to ML-generated summary
/// text. When a chunk_id is present in the map, its ML summary is used as
/// `summary_2sent` instead of the rule-based `generate_stub_capsule` output.
pub fn pack_minimal(
    ranked: &[RankedResult],
    budget_tokens: usize,
    k: usize,
    include_scores: bool,
    ml_summaries: &HashMap<String, String>,
) -> SearchResult {
    let total_available = ranked.len();
    let mut results = Vec::new();
    let mut used_tokens = 0;

    for result in ranked.iter().take(k) {
        let ml_summary = ml_summaries.get(&result.chunk_id).map(|s| s.as_str());
        let mut stub = make_stub(result, ml_summary);
        let stub_tokens = estimate_stub_tokens(&stub);

        if used_tokens + stub_tokens > budget_tokens && !results.is_empty() {
            break;
        }

        if include_scores {
            stub.signal_scores = Some(result.scores);
        }

        // Slot index in the packed list. Stable across runs because `ranked`
        // is sorted with a deterministic tiebreak in `rank_candidates`.
        stub.lod_plan_slot = results.len();

        used_tokens += stub_tokens;
        results.push(stub);
    }

    SearchResult {
        budget_tokens,
        used_tokens_est: used_tokens,
        num_results: results.len(),
        total_available,
        results,
        fusion_confidence: None,
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
                    byte_start: result.byte_start(),
                    byte_end: result.byte_end(),
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
            byte_start: result.byte_start(),
            byte_end: result.byte_end(),
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

fn make_stub(result: &RankedResult, ml_summary: Option<&str>) -> MemoryStub {
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

    // Summary: prefer ML summary when available, fall back to rule-based capsule
    let summary_2sent = match ml_summary {
        Some(ml) => ml.to_string(),
        None => generate_stub_capsule(Some(&title), &result.content),
    };

    let stub_tokens = estimate_tokens(&title) + estimate_tokens(&summary_2sent) + 5;

    let kind = derive_kind(&result.chunk_id, result.summary_kind.as_deref())
        .as_str()
        .to_string();

    MemoryStub {
        memory_id: result.chunk_id.clone(),
        title,
        summary_2sent,
        hybrid_score: result.hybrid_score,
        file_path: result.file_path.clone(),
        heading_path: result.heading_path.clone(),
        token_estimate: stub_tokens,
        signal_scores: None,
        kind,
        brain_name: None,
        expansion_reason: result.expansion_reason,
        // Set by pack_minimal at the moment of push. Sentinel value here.
        lod_plan_slot: 0,
    }
}

/// Derive the result kind from a chunk_id prefix and optional summary kind.
pub(crate) fn derive_kind(chunk_id: &str, summary_kind: Option<&str>) -> MemoryKind {
    if chunk_id.starts_with("sum:") {
        match summary_kind.unwrap_or("episode") {
            "reflection" => MemoryKind::Reflection,
            "procedure" => MemoryKind::Procedure,
            "summary" => MemoryKind::Note, // derived summaries map to Note
            _ => MemoryKind::Episode,
        }
    } else if chunk_id.starts_with("task-outcome:") {
        MemoryKind::TaskOutcome
    } else if chunk_id.starts_with("task:") {
        MemoryKind::Task
    } else if chunk_id.starts_with("record:") {
        MemoryKind::Record
    } else {
        MemoryKind::Note
    }
}

impl std::fmt::Display for MemoryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl MemoryKind {
    /// String representation matching the existing MCP JSON output format.
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryKind::Note => "note",
            MemoryKind::Episode => "episode",
            MemoryKind::Reflection => "reflection",
            MemoryKind::Procedure => "procedure",
            MemoryKind::Task => "task",
            MemoryKind::TaskOutcome => "task-outcome",
            MemoryKind::Record => "record",
        }
    }
}

fn estimate_stub_tokens(stub: &MemoryStub) -> usize {
    stub.token_estimate
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
            byte_start: 0,
            byte_end: 0,
            summary_kind: None,
            expansion_reason: crate::ranking::ExpansionReason::Hybrid,
        }
    }

    #[test]
    fn test_pack_minimal_within_budget() {
        let ranked = vec![
            make_ranked("a", 0.9, "First result content here."),
            make_ranked("b", 0.8, "Second result content here."),
            make_ranked("c", 0.7, "Third result content here."),
        ];

        let result = pack_minimal(&ranked, 1000, 10, false, &HashMap::new());
        assert_eq!(result.num_results, 3);
        assert!(result.used_tokens_est <= result.budget_tokens);
        assert_eq!(result.total_available, 3);
        // Without include_scores, signal_scores should be None
        assert!(result.results[0].signal_scores.is_none());
    }

    #[test]
    fn test_pack_minimal_respects_budget() {
        let ranked = vec![
            make_ranked("a", 0.9, "Content A."),
            make_ranked("b", 0.8, "Content B."),
            make_ranked("c", 0.7, "Content C."),
        ];

        // Very tight budget — should only fit 1 stub
        let result = pack_minimal(&ranked, 15, 10, false, &HashMap::new());
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

        let result = pack_minimal(&ranked, 10000, 2, false, &HashMap::new());
        assert_eq!(result.num_results, 2);
    }

    #[test]
    fn test_pack_minimal_empty() {
        let result = pack_minimal(&[], 1000, 10, false, &HashMap::new());
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
    fn test_stub_has_title_from_heading() {
        let ranked = vec![make_ranked("test", 0.9, "Some content.")];
        let result = pack_minimal(&ranked, 1000, 10, false, &HashMap::new());
        assert_eq!(result.results[0].title, "test");
    }

    #[test]
    fn test_pack_minimal_with_scores() {
        let ranked = vec![
            make_ranked("a", 0.9, "First result content here."),
            make_ranked("b", 0.8, "Second result content here."),
        ];

        let result = pack_minimal(&ranked, 1000, 10, true, &HashMap::new());
        assert_eq!(result.num_results, 2);
        assert!(result.used_tokens_est <= result.budget_tokens);

        // Every stub should have signal scores
        for stub in &result.results {
            assert!(stub.signal_scores.is_some());
            let scores = stub.signal_scores.unwrap();
            assert!(scores.vector > 0.0);
        }
    }

    #[test]
    fn test_pack_minimal_without_scores() {
        let ranked = vec![make_ranked("a", 0.9, "Content.")];
        let result = pack_minimal(&ranked, 1000, 10, false, &HashMap::new());
        assert!(result.results[0].signal_scores.is_none());
    }

    #[test]
    fn test_derive_kind() {
        assert_eq!(derive_kind("task:BRN-01ABC:0", None), MemoryKind::Task);
        assert_eq!(
            derive_kind("task-outcome:BRN-01XYZ:0", None),
            MemoryKind::TaskOutcome
        );
        assert_eq!(derive_kind("01JXYZ1234:0", None), MemoryKind::Note);
        assert_eq!(derive_kind("some-uuid:3", None), MemoryKind::Note);
        assert_eq!(
            derive_kind("sum:01JXYZ1234", Some("episode")),
            MemoryKind::Episode
        );
        assert_eq!(
            derive_kind("sum:01JXYZ1234", Some("reflection")),
            MemoryKind::Reflection
        );
        assert_eq!(derive_kind("record:BRN-01ABC:0", None), MemoryKind::Record);
        assert_eq!(derive_kind("sum:01JXYZ1234", None), MemoryKind::Episode);
        assert_eq!(
            derive_kind("sum:ABC123", Some("procedure")),
            MemoryKind::Procedure
        );
    }

    #[test]
    fn test_stub_kind_for_note() {
        let ranked = vec![make_ranked("01JABCDEFGH", 0.9, "Some content.")];
        let result = pack_minimal(&ranked, 1000, 10, false, &HashMap::new());
        assert_eq!(result.results[0].kind, "note");
    }

    #[test]
    fn test_stub_kind_for_task() {
        let mut r = make_ranked("task:BRN-01ABC:0", 0.9, "Task content.");
        r.chunk_id = "task:BRN-01ABC:0".to_string();
        let ranked = vec![r];
        let result = pack_minimal(&ranked, 1000, 10, false, &HashMap::new());
        assert_eq!(result.results[0].kind, "task");
    }

    #[test]
    fn test_stub_kind_for_task_outcome() {
        let mut r = make_ranked("task-outcome:BRN-01XYZ:0", 0.9, "Outcome content.");
        r.chunk_id = "task-outcome:BRN-01XYZ:0".to_string();
        let ranked = vec![r];
        let result = pack_minimal(&ranked, 1000, 10, false, &HashMap::new());
        assert_eq!(result.results[0].kind, "task-outcome");
    }

    #[test]
    fn test_make_stub_uses_ml_summary() {
        let ranked = vec![make_ranked("chunk:1", 0.9, "Raw content here.")];
        let mut ml_summaries = HashMap::new();
        ml_summaries.insert(
            "chunk:1".to_string(),
            "ML-generated summary text.".to_string(),
        );
        let result = pack_minimal(&ranked, 1000, 10, false, &ml_summaries);
        assert_eq!(
            result.results[0].summary_2sent,
            "ML-generated summary text."
        );
    }

    #[test]
    fn test_make_stub_falls_back_to_capsule_when_no_ml_summary() {
        let ranked = vec![make_ranked("chunk:1", 0.9, "Raw content here.")];
        let result = pack_minimal(&ranked, 1000, 10, false, &HashMap::new());
        // Falls back to generate_stub_capsule — should not be empty
        assert!(!result.results[0].summary_2sent.is_empty());
        // Must NOT equal the ML text (there is none)
        assert_ne!(
            result.results[0].summary_2sent,
            "ML-generated summary text."
        );
    }
}
