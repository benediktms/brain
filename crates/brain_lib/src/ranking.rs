/// Hybrid multi-signal ranking engine.
///
/// Combines vector similarity, BM25 keyword scores, recency, backlink graph,
/// tag overlap, and importance into a single weighted score.
///
/// Weight profile names for different retrieval intents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightProfile {
    /// Equal weights across all signals.
    Default,
    /// Upweight BM25 for exact keyword lookup.
    Lookup,
    /// Upweight recency and links for planning queries.
    Planning,
    /// Upweight recency for reflection queries.
    Reflection,
    /// Upweight vector similarity for semantic synthesis.
    Synthesis,
}

/// The six signal weights. Must sum to 1.0.
#[derive(Debug, Clone, Copy)]
pub struct Weights {
    /// Vector similarity weight.
    pub vector: f64,
    /// BM25 keyword weight.
    pub keyword: f64,
    /// Recency decay weight.
    pub recency: f64,
    /// Backlink graph weight.
    pub links: f64,
    /// Tag match weight.
    pub tag_match: f64,
    /// Importance weight.
    pub importance: f64,
}

impl Weights {
    pub fn from_profile(profile: WeightProfile) -> Self {
        match profile {
            WeightProfile::Default => Self::equal(),
            WeightProfile::Lookup => Self {
                vector: 0.10,
                keyword: 0.40,
                recency: 0.15,
                links: 0.15,
                tag_match: 0.10,
                importance: 0.10,
            },
            WeightProfile::Planning => Self {
                vector: 0.15,
                keyword: 0.10,
                recency: 0.30,
                links: 0.25,
                tag_match: 0.10,
                importance: 0.10,
            },
            WeightProfile::Reflection => Self {
                vector: 0.15,
                keyword: 0.15,
                recency: 0.35,
                links: 0.10,
                tag_match: 0.10,
                importance: 0.15,
            },
            WeightProfile::Synthesis => Self {
                vector: 0.40,
                keyword: 0.15,
                recency: 0.10,
                links: 0.15,
                tag_match: 0.10,
                importance: 0.10,
            },
        }
    }

    fn equal() -> Self {
        let w = 1.0 / 6.0;
        Self {
            vector: w,
            keyword: w,
            recency: w,
            links: w,
            tag_match: w,
            importance: w,
        }
    }

    /// Validate that weights sum to ~1.0.
    pub fn validate(&self) -> Result<(), String> {
        let sum = self.vector
            + self.keyword
            + self.recency
            + self.links
            + self.tag_match
            + self.importance;
        if (sum - 1.0).abs() > 0.01 {
            Err(format!("weights must sum to 1.0, got {sum:.4}"))
        } else {
            Ok(())
        }
    }
}

/// Raw signal inputs for a single candidate chunk.
#[derive(Debug, Clone)]
pub struct CandidateSignals {
    pub chunk_id: String,
    /// Vector similarity score (dot product), already in [0, 1].
    pub sim_vector: f64,
    /// BM25 score, already normalized to [0, 1] by FTS5 module.
    pub bm25: f64,
    /// Seconds since last update of the source file.
    pub age_seconds: f64,
    /// Number of backlinks to the source file.
    pub backlink_count: usize,
    /// Maximum backlink count in the candidate set (for normalization).
    pub max_backlinks: usize,
    /// Tags on this chunk/file.
    pub tags: Vec<String>,
    /// Importance score (default 1.0).
    pub importance: f64,
    /// Optional metadata to pass through.
    pub file_path: String,
    pub heading_path: String,
    pub content: String,
    pub token_estimate: usize,
}

/// A ranked result with hybrid score and signal breakdown.
#[derive(Debug, Clone)]
pub struct RankedResult {
    pub chunk_id: String,
    pub hybrid_score: f64,
    pub scores: SignalScores,
    pub file_path: String,
    pub heading_path: String,
    pub content: String,
    pub token_estimate: usize,
}

/// Individual signal scores for debugging/introspection.
#[derive(Debug, Clone, Copy)]
pub struct SignalScores {
    pub vector: f64,
    pub keyword: f64,
    pub recency: f64,
    pub links: f64,
    pub tag_match: f64,
    pub importance: f64,
}

/// Recency decay: exp(-age / tau), where tau = 30 days in seconds.
const RECENCY_TAU: f64 = 30.0 * 24.0 * 3600.0;

/// Compute recency score: exponential decay with 30-day half-life.
fn recency_score(age_seconds: f64) -> f64 {
    (-age_seconds / RECENCY_TAU).exp()
}

/// Compute normalized backlink score: log(1 + count) / log(1 + max).
fn link_score(backlink_count: usize, max_backlinks: usize) -> f64 {
    if max_backlinks == 0 {
        return 0.0;
    }
    let num = (1.0 + backlink_count as f64).ln();
    let den = (1.0 + max_backlinks as f64).ln();
    num / den
}

/// Compute Jaccard similarity between two tag sets.
/// Returns 0.0 if both sets are empty.
fn tag_match_score(query_tags: &[String], chunk_tags: &[String]) -> f64 {
    if query_tags.is_empty() && chunk_tags.is_empty() {
        return 0.0;
    }
    if query_tags.is_empty() || chunk_tags.is_empty() {
        return 0.0;
    }

    let query_set: std::collections::HashSet<&str> =
        query_tags.iter().map(|s| s.as_str()).collect();
    let chunk_set: std::collections::HashSet<&str> =
        chunk_tags.iter().map(|s| s.as_str()).collect();

    let intersection = query_set.intersection(&chunk_set).count();
    let union = query_set.union(&chunk_set).count();

    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

/// Rank candidates using the hybrid scoring formula.
///
/// Returns results sorted by descending hybrid score.
pub fn rank_candidates(
    candidates: &[CandidateSignals],
    weights: &Weights,
    query_tags: &[String],
) -> Vec<RankedResult> {
    let mut results: Vec<RankedResult> = candidates
        .iter()
        .map(|c| {
            let scores = SignalScores {
                vector: c.sim_vector,
                keyword: c.bm25,
                recency: recency_score(c.age_seconds),
                links: link_score(c.backlink_count, c.max_backlinks),
                tag_match: tag_match_score(query_tags, &c.tags),
                importance: c.importance,
            };

            let hybrid_score = weights.vector * scores.vector
                + weights.keyword * scores.keyword
                + weights.recency * scores.recency
                + weights.links * scores.links
                + weights.tag_match * scores.tag_match
                + weights.importance * scores.importance;

            RankedResult {
                chunk_id: c.chunk_id.clone(),
                hybrid_score,
                scores,
                file_path: c.file_path.clone(),
                heading_path: c.heading_path.clone(),
                content: c.content.clone(),
                token_estimate: c.token_estimate,
            }
        })
        .collect();

    results.sort_by(|a, b| {
        b.hybrid_score
            .partial_cmp(&a.hybrid_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results
}

/// Resolve an intent string to a weight profile.
pub fn resolve_intent(intent: &str) -> WeightProfile {
    match intent.to_lowercase().as_str() {
        "lookup" => WeightProfile::Lookup,
        "planning" => WeightProfile::Planning,
        "reflection" => WeightProfile::Reflection,
        "synthesis" => WeightProfile::Synthesis,
        _ => WeightProfile::Default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candidate(
        chunk_id: &str,
        sim_v: f64,
        bm25: f64,
        age_days: f64,
        backlinks: usize,
    ) -> CandidateSignals {
        CandidateSignals {
            chunk_id: chunk_id.to_string(),
            sim_vector: sim_v,
            bm25,
            age_seconds: age_days * 86400.0,
            backlink_count: backlinks,
            max_backlinks: 10,
            tags: vec![],
            importance: 1.0,
            file_path: format!("/notes/{chunk_id}.md"),
            heading_path: String::new(),
            content: format!("content of {chunk_id}"),
            token_estimate: 20,
        }
    }

    #[test]
    fn test_weights_validate() {
        assert!(
            Weights::from_profile(WeightProfile::Default)
                .validate()
                .is_ok()
        );
        assert!(
            Weights::from_profile(WeightProfile::Lookup)
                .validate()
                .is_ok()
        );
        assert!(
            Weights::from_profile(WeightProfile::Planning)
                .validate()
                .is_ok()
        );
        assert!(
            Weights::from_profile(WeightProfile::Reflection)
                .validate()
                .is_ok()
        );
        assert!(
            Weights::from_profile(WeightProfile::Synthesis)
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn test_weights_invalid_sum() {
        let w = Weights {
            vector: 0.5,
            keyword: 0.5,
            recency: 0.5,
            links: 0.0,
            tag_match: 0.0,
            importance: 0.0,
        };
        assert!(w.validate().is_err());
    }

    #[test]
    fn test_recency_score_fresh() {
        let score = recency_score(0.0);
        assert!(
            (score - 1.0).abs() < f64::EPSILON,
            "fresh content should score 1.0"
        );
    }

    #[test]
    fn test_recency_score_old() {
        let score = recency_score(90.0 * 86400.0); // 90 days
        assert!(
            score < 0.1,
            "90-day old content should have low recency: {score}"
        );
    }

    #[test]
    fn test_link_score_none() {
        assert_eq!(link_score(0, 0), 0.0);
        assert_eq!(link_score(0, 10), 0.0);
    }

    #[test]
    fn test_link_score_max() {
        let score = link_score(10, 10);
        assert!(
            (score - 1.0).abs() < f64::EPSILON,
            "max backlinks should score 1.0"
        );
    }

    #[test]
    fn test_tag_match_empty() {
        assert_eq!(tag_match_score(&[], &[]), 0.0);
        assert_eq!(tag_match_score(&["a".into()], &[]), 0.0);
    }

    #[test]
    fn test_tag_match_perfect() {
        let tags = vec!["rust".to_string(), "memory".to_string()];
        let score = tag_match_score(&tags, &tags);
        assert!(
            (score - 1.0).abs() < f64::EPSILON,
            "identical tags should score 1.0"
        );
    }

    #[test]
    fn test_tag_match_partial() {
        let query = vec!["rust".to_string(), "memory".to_string()];
        let chunk = vec!["rust".to_string(), "safety".to_string()];
        let score = tag_match_score(&query, &chunk);
        // intersection=1 (rust), union=3 (rust, memory, safety) -> 1/3
        assert!((score - 1.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_rank_candidates_ordering() {
        let candidates = vec![
            make_candidate("low", 0.1, 0.1, 60.0, 0),
            make_candidate("high", 0.9, 0.9, 1.0, 8),
            make_candidate("mid", 0.5, 0.5, 15.0, 3),
        ];

        let weights = Weights::from_profile(WeightProfile::Default);
        let results = rank_candidates(&candidates, &weights, &[]);

        assert_eq!(results[0].chunk_id, "high");
        assert_eq!(results[2].chunk_id, "low");
        assert!(results[0].hybrid_score > results[1].hybrid_score);
        assert!(results[1].hybrid_score > results[2].hybrid_score);
    }

    #[test]
    fn test_different_profiles_produce_different_orderings() {
        // Candidate A: high BM25, old
        // Candidate B: low BM25, very recent
        let candidates = vec![
            CandidateSignals {
                chunk_id: "keyword_hit".into(),
                sim_vector: 0.3,
                bm25: 0.95,
                age_seconds: 60.0 * 86400.0, // 60 days old
                backlink_count: 0,
                max_backlinks: 5,
                tags: vec![],
                importance: 1.0,
                file_path: "/notes/a.md".into(),
                heading_path: String::new(),
                content: "keyword content".into(),
                token_estimate: 10,
            },
            CandidateSignals {
                chunk_id: "recent".into(),
                sim_vector: 0.3,
                bm25: 0.1,
                age_seconds: 3600.0, // 1 hour old
                backlink_count: 0,
                max_backlinks: 5,
                tags: vec![],
                importance: 1.0,
                file_path: "/notes/b.md".into(),
                heading_path: String::new(),
                content: "recent content".into(),
                token_estimate: 10,
            },
        ];

        let lookup_results = rank_candidates(
            &candidates,
            &Weights::from_profile(WeightProfile::Lookup),
            &[],
        );
        let reflection_results = rank_candidates(
            &candidates,
            &Weights::from_profile(WeightProfile::Reflection),
            &[],
        );

        // Lookup should prefer the keyword hit
        assert_eq!(lookup_results[0].chunk_id, "keyword_hit");
        // Reflection should prefer recency
        assert_eq!(reflection_results[0].chunk_id, "recent");
    }

    #[test]
    fn test_rank_empty_candidates() {
        let results = rank_candidates(&[], &Weights::from_profile(WeightProfile::Default), &[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_resolve_intent() {
        assert_eq!(resolve_intent("lookup"), WeightProfile::Lookup);
        assert_eq!(resolve_intent("PLANNING"), WeightProfile::Planning);
        assert_eq!(resolve_intent("reflection"), WeightProfile::Reflection);
        assert_eq!(resolve_intent("synthesis"), WeightProfile::Synthesis);
        assert_eq!(resolve_intent("auto"), WeightProfile::Default);
        assert_eq!(resolve_intent("unknown"), WeightProfile::Default);
    }
}
