/// Hybrid multi-signal ranking engine.
///
/// Combines vector similarity, BM25 keyword scores, recency, backlink graph,
/// tag overlap, and importance into a single weighted score.
use serde::Serialize;

/// Why this candidate was surfaced. Discovery channel only — not a measure
/// of relevance. Used by callers to reason about retrieval coverage and by
/// the benchmark suite to slice quality metrics.
///
/// Tag-alias expansion is intentionally not represented here: it filters
/// eligibility, not discovery, and its contribution is visible via
/// `SignalScores::tag_match`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpansionReason {
    /// Vector similarity hit only (no FTS match).
    VectorOnly,
    /// BM25/FTS hit only (no vector similarity).
    KeywordOnly,
    /// Both vector and keyword paths matched.
    Hybrid,
    /// Pulled in by 1-hop outlink expansion from a top seed.
    GraphLink,
    /// Direct URI fetch — no ranking involved.
    UriDirect,
}

impl ExpansionReason {
    /// Classify a candidate from its raw signal scores. Graph-link candidates
    /// must be tagged separately by the caller — this function only sees
    /// vector and keyword signals.
    pub fn from_signals(sim_vector: f64, bm25: f64) -> Self {
        let has_vector = sim_vector > 0.0;
        let has_keyword = bm25 > 0.0;
        match (has_vector, has_keyword) {
            (true, true) => Self::Hybrid,
            (true, false) => Self::VectorOnly,
            (false, true) => Self::KeywordOnly,
            // No signal — default to Hybrid; this branch is unreachable for
            // legitimately-ranked candidates.
            (false, false) => Self::Hybrid,
        }
    }

    /// String form used in the MCP JSON surface.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::VectorOnly => "vector_only",
            Self::KeywordOnly => "keyword_only",
            Self::Hybrid => "hybrid",
            Self::GraphLink => "graph_link",
            Self::UriDirect => "uri_direct",
        }
    }
}

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
///
/// `alias_discount` is a separate tunable that does NOT participate in the
/// 6-signal sum constraint — see `validate`.
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
    /// Discount applied to the tag-match jaccard contribution from
    /// same-cluster (alias-only) matches. Literal matches contribute their
    /// full jaccard slot. `0.7` default; `1.0` disables the discount;
    /// `0.0` disables alias matching at the rank stage entirely (filter-side
    /// expansion in `query_pipeline` still applies). Not part of
    /// `validate`'s 6-signal sum constraint.
    pub alias_discount: f64,
}

impl Weights {
    pub fn from_profile(profile: WeightProfile) -> Self {
        let w = match profile {
            WeightProfile::Default => Self::equal(),
            WeightProfile::Lookup => Self {
                vector: 0.10,
                keyword: 0.40,
                recency: 0.15,
                links: 0.15,
                tag_match: 0.10,
                importance: 0.10,
                alias_discount: 0.7,
            },
            WeightProfile::Planning => Self {
                vector: 0.15,
                keyword: 0.10,
                recency: 0.30,
                links: 0.25,
                tag_match: 0.10,
                importance: 0.10,
                alias_discount: 0.7,
            },
            WeightProfile::Reflection => Self {
                vector: 0.15,
                keyword: 0.15,
                recency: 0.35,
                links: 0.10,
                tag_match: 0.10,
                importance: 0.15,
                alias_discount: 0.7,
            },
            WeightProfile::Synthesis => Self {
                vector: 0.40,
                keyword: 0.15,
                recency: 0.10,
                links: 0.15,
                tag_match: 0.10,
                importance: 0.10,
                alias_discount: 0.7,
            },
        };
        debug_assert!(
            w.validate().is_ok(),
            "weight profile {:?} failed validation: {:?}",
            profile,
            w.validate()
        );
        w
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
            alias_discount: 0.7,
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
    /// Precomputed PageRank score from the files table, normalized to [0, 1].
    pub pagerank_score: f64,
    /// Tags on this chunk/file.
    pub tags: Vec<String>,
    /// Importance score (default 1.0).
    pub importance: f64,
    /// Optional metadata to pass through.
    pub file_path: String,
    pub heading_path: String,
    pub content: String,
    pub token_estimate: usize,
    pub byte_start: usize,
    pub byte_end: usize,
    /// For `sum:` candidates: the kind field from the summaries table ("episode" or "reflection").
    /// `None` for regular chunk candidates.
    pub summary_kind: Option<String>,
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
    pub byte_start: usize,
    pub byte_end: usize,
    /// For `sum:` candidates: the kind field from the summaries table ("episode" or "reflection").
    /// `None` for regular chunk candidates.
    pub summary_kind: Option<String>,
    /// Discovery channel for this candidate. Initially set from signal
    /// classification in `rank_candidates`; the caller (query pipeline)
    /// overrides to `GraphLink` for chunks introduced by graph expansion.
    pub expansion_reason: ExpansionReason,
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

impl crate::retrieval::Expandable for RankedResult {
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

/// Recency decay: exp(-age / tau), where tau = 30 days in seconds.
const RECENCY_TAU: f64 = 30.0 * 24.0 * 3600.0;

/// Compute recency score: exponential decay with 30-day half-life.
fn recency_score(age_seconds: f64) -> f64 {
    (-age_seconds / RECENCY_TAU).exp()
}

/// Compute the alias-aware tag-match score.
///
/// Blends literal Jaccard overlap (full slot) with same-cluster-only overlap
/// (`alias_discount` slot, default 0.7). Inputs are lowercased before
/// scoring to match the filter stage in `query_pipeline` and the lowercased
/// alias-lookup boundary, so mixed-case query/chunk tags get correct
/// alias-discount credit (`brn-83a.7.2.4.6`). When `alias_lookup` is empty
/// every tag is its own canonical class and the formula collapses to a
/// pure case-insensitive literal Jaccard — pinned by
/// `tag_match_literal_only_with_empty_alias_map`.
///
/// score = (literal_overlap + alias_discount × same_cluster_only) / union_classes
fn tag_match_score(
    query_tags: &[String],
    chunk_tags: &[String],
    alias_lookup: &std::collections::HashMap<String, String>,
    alias_discount: f64,
) -> f64 {
    if query_tags.is_empty() && chunk_tags.is_empty() {
        return 0.0;
    }
    if query_tags.is_empty() || chunk_tags.is_empty() {
        return 0.0;
    }

    // Lowercase inputs once so all downstream comparisons (literal sets,
    // canonical-class lookup, union/intersection) operate in the same
    // namespace as the alias_lookup keys (which are already lowercased
    // at the boundary by `alias_lookup_for_brain`).
    let q_lower: Vec<String> = query_tags.iter().map(|s| s.to_lowercase()).collect();
    let c_lower: Vec<String> = chunk_tags.iter().map(|s| s.to_lowercase()).collect();

    let canonical_of = |t: &str| -> String {
        alias_lookup
            .get(t)
            .cloned()
            .unwrap_or_else(|| t.to_string())
    };

    let q_lit: std::collections::HashSet<&str> = q_lower.iter().map(|s| s.as_str()).collect();
    let c_lit: std::collections::HashSet<&str> = c_lower.iter().map(|s| s.as_str()).collect();
    let q_canon: std::collections::HashSet<String> =
        q_lower.iter().map(|t| canonical_of(t)).collect();
    let c_canon: std::collections::HashSet<String> =
        c_lower.iter().map(|t| canonical_of(t)).collect();

    let literal_overlap = q_lit.intersection(&c_lit).count();
    let canon_overlap = q_canon.intersection(&c_canon).count();
    let same_cluster_only = canon_overlap.saturating_sub(literal_overlap);
    let union_classes = q_canon.union(&c_canon).count();

    if union_classes == 0 {
        return 0.0;
    }

    let score =
        (literal_overlap as f64 + alias_discount * same_cluster_only as f64) / union_classes as f64;

    tracing::debug!(
        literal_overlap,
        same_cluster_only,
        union_classes,
        alias_discount,
        score,
        "tag_match_score components"
    );

    score
}

/// Rank candidates using the hybrid scoring formula.
///
/// `alias_lookup` is the per-brain `(raw_tag → canonical_tag)` projection
/// used by the alias-aware `tag_match_score`. Pass `&HashMap::new()` when
/// alias-awareness is not needed (e.g. unit tests, or callers without a
/// resolved brain context) — the score then collapses to literal Jaccard.
///
/// Returns results sorted by descending hybrid score.
pub fn rank_candidates(
    candidates: &[CandidateSignals],
    weights: &Weights,
    query_tags: &[String],
    alias_lookup: &std::collections::HashMap<String, String>,
) -> Vec<RankedResult> {
    let mut results: Vec<RankedResult> = candidates
        .iter()
        .map(|c| {
            let scores = SignalScores {
                vector: c.sim_vector,
                keyword: c.bm25,
                recency: recency_score(c.age_seconds),
                links: c.pagerank_score,
                tag_match: tag_match_score(
                    query_tags,
                    &c.tags,
                    alias_lookup,
                    weights.alias_discount,
                ),
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
                byte_start: c.byte_start,
                byte_end: c.byte_end,
                summary_kind: c.summary_kind.clone(),
                expansion_reason: ExpansionReason::from_signals(c.sim_vector, c.bm25),
            }
        })
        .collect();

    // Sort by hybrid score descending, with lexicographic chunk_id as a
    // stable tiebreaker so identical queries against identical state produce
    // byte-identical orderings (precondition for the benchmark suite).
    results.sort_by(|a, b| {
        b.hybrid_score
            .partial_cmp(&a.hybrid_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.chunk_id.cmp(&b.chunk_id))
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

// ─── Fusion Confidence ──────────────────────────────────────────

/// Fusion confidence: measures agreement between vector and FTS candidate sets.
///
/// `confidence = |intersection(top_k_vector, top_k_fts)| / k`
///
/// High confidence (>0.6): both retrieval paths agree — reranking likely unnecessary.
/// Low confidence (<0.3): significant disagreement — cross-encoder reranking recommended.
#[derive(Debug, Clone, Copy)]
pub struct FusionConfidence {
    /// The confidence value in [0, 1].
    pub confidence: f64,
    /// Number of top candidates compared (k).
    pub k: usize,
    /// Number of overlapping chunk_ids in the top-k sets.
    pub overlap: usize,
}

/// Compute fusion confidence from the top-k vector and FTS result chunk IDs.
///
/// Compares the top `k` results from each retrieval path and measures
/// overlap. When both paths are empty, returns confidence 1.0 (vacuous
/// agreement — no reranking needed).
pub fn compute_fusion_confidence(
    vector_ids: &[&str],
    fts_ids: &[&str],
    k: usize,
) -> FusionConfidence {
    if vector_ids.is_empty() && fts_ids.is_empty() {
        return FusionConfidence {
            confidence: 1.0,
            k: 0,
            overlap: 0,
        };
    }
    if vector_ids.is_empty() || fts_ids.is_empty() {
        return FusionConfidence {
            confidence: 0.0,
            k,
            overlap: 0,
        };
    }

    let effective_k = k.min(vector_ids.len()).min(fts_ids.len());
    if effective_k == 0 {
        return FusionConfidence {
            confidence: 0.0,
            k: 0,
            overlap: 0,
        };
    }

    let vector_top: std::collections::HashSet<&str> =
        vector_ids.iter().take(effective_k).copied().collect();
    let fts_top: std::collections::HashSet<&str> =
        fts_ids.iter().take(effective_k).copied().collect();

    let overlap = vector_top.intersection(&fts_top).count();
    let confidence = overlap as f64 / effective_k as f64;

    FusionConfidence {
        confidence,
        k: effective_k,
        overlap,
    }
}

// ─── Adaptive Reranking ─────────────────────────────────────────

/// Policy for adaptive reranking based on fusion confidence.
#[derive(Debug, Clone, Copy)]
pub struct RerankerPolicy {
    /// Below this threshold, trigger reranking.
    pub low_threshold: f64,
    /// Number of top candidates to compare for confidence.
    pub confidence_k: usize,
    /// Number of top fused candidates to pass to the reranker.
    pub rerank_depth: usize,
}

impl Default for RerankerPolicy {
    fn default() -> Self {
        Self {
            low_threshold: 0.3,
            confidence_k: 5,
            rerank_depth: 20,
        }
    }
}

impl RerankerPolicy {
    /// Whether the reranker should be triggered given the fusion confidence.
    pub fn should_rerank(&self, confidence: &FusionConfidence) -> bool {
        confidence.confidence < self.low_threshold
    }
}

/// A candidate for cross-encoder reranking.
#[derive(Debug, Clone)]
pub struct RerankCandidate {
    pub chunk_id: String,
    /// Capsule or snippet text (not full chunk content).
    pub text: String,
}

/// Result from a cross-encoder reranker.
#[derive(Debug, Clone)]
pub struct RerankResult {
    pub chunk_id: String,
    /// Cross-encoder relevance score in [0, 1].
    pub score: f64,
}

/// Trait for cross-encoder rerankers.
///
/// Implementations rerank candidates by semantic relevance to the query,
/// operating on capsules/snippets (not full chunks) for efficiency.
/// Target latency: 200-500ms on CPU for 10-30 candidates.
///
/// The model should be loaded lazily (not at startup) and unloaded
/// after an idle timeout.
pub trait Reranker: Send + Sync {
    /// Rerank candidates given a query. Returns results sorted by descending score.
    fn rerank(
        &self,
        query: &str,
        candidates: &[RerankCandidate],
    ) -> crate::error::Result<Vec<RerankResult>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candidate(
        chunk_id: &str,
        sim_v: f64,
        bm25: f64,
        age_days: f64,
        pagerank: f64,
    ) -> CandidateSignals {
        CandidateSignals {
            chunk_id: chunk_id.to_string(),
            sim_vector: sim_v,
            bm25,
            age_seconds: age_days * 86400.0,
            pagerank_score: pagerank,
            tags: vec![],
            importance: 1.0,
            file_path: format!("/notes/{chunk_id}.md"),
            heading_path: String::new(),
            content: format!("content of {chunk_id}"),
            token_estimate: 20,
            byte_start: 0,
            byte_end: 0,
            summary_kind: None,
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
            alias_discount: 0.7,
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
    fn test_tag_match_empty() {
        let lookup = std::collections::HashMap::new();
        assert_eq!(tag_match_score(&[], &[], &lookup, 0.7), 0.0);
        assert_eq!(tag_match_score(&["a".into()], &[], &lookup, 0.7), 0.0);
    }

    #[test]
    fn test_tag_match_perfect() {
        let tags = vec!["rust".to_string(), "memory".to_string()];
        let lookup = std::collections::HashMap::new();
        let score = tag_match_score(&tags, &tags, &lookup, 0.7);
        assert!(
            (score - 1.0).abs() < f64::EPSILON,
            "identical tags should score 1.0"
        );
    }

    #[test]
    fn test_tag_match_partial() {
        let query = vec!["rust".to_string(), "memory".to_string()];
        let chunk = vec!["rust".to_string(), "safety".to_string()];
        let lookup = std::collections::HashMap::new();
        let score = tag_match_score(&query, &chunk, &lookup, 0.7);
        // intersection=1 (rust), union=3 (rust, memory, safety) -> 1/3
        assert!((score - 1.0 / 3.0).abs() < 0.01);
    }

    // ─── Alias-aware tag_match_score tests (`brn-83a.7.2.4.4`) ───

    fn alias_map(rows: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        rows.iter()
            .map(|(raw, canonical)| (raw.to_string(), canonical.to_string()))
            .collect()
    }

    #[test]
    fn tag_match_literal_only_with_empty_alias_map() {
        // Regression pin: with an empty alias_lookup, scoring is bit-for-bit
        // the existing literal Jaccard. Mirrors `test_tag_match_partial`.
        let query = vec!["rust".to_string(), "memory".to_string()];
        let chunk = vec!["rust".to_string(), "safety".to_string()];
        let score = tag_match_score(&query, &chunk, &std::collections::HashMap::new(), 0.7);
        assert!((score - 1.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn tag_match_same_cluster_discounted() {
        // query=["bug"], chunk=["bugs"], lookup clusters both → "bug".
        // canonical_class(Q) = {bug}, canonical_class(C) = {bug}.
        // literal_overlap=0, same_cluster_only=1, union_classes=1
        // ⇒ score = (0 + 0.7*1) / 1 = 0.7
        let query = vec!["bug".to_string()];
        let chunk = vec!["bugs".to_string()];
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let score = tag_match_score(&query, &chunk, &lookup, 0.7);
        assert!((score - 0.7).abs() < 1e-9, "got {score}");
    }

    #[test]
    fn tag_match_mixed_literal_and_alias() {
        // query=["bug","rust"], chunk=["bugs","rust"], lookup clusters bug↔bugs.
        // canonical_class(Q) = {bug, rust}, canonical_class(C) = {bug, rust}.
        // literal_overlap=1 (rust), same_cluster_only=2-1=1, union_classes=2
        // ⇒ score = (1 + 0.7*1) / 2 = 0.85
        let query = vec!["bug".to_string(), "rust".to_string()];
        let chunk = vec!["bugs".to_string(), "rust".to_string()];
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let score = tag_match_score(&query, &chunk, &lookup, 0.7);
        assert!((score - 0.85).abs() < 1e-9, "got {score}");
    }

    #[test]
    fn tag_match_no_alias_no_overlap() {
        let query = vec!["bug".to_string()];
        let chunk = vec!["unrelated".to_string()];
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let score = tag_match_score(&query, &chunk, &lookup, 0.7);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn tag_match_singleton_cluster_behaves_like_literal() {
        // A singleton cluster (raw_tag → itself) means no aliases share its
        // canonical. literal match contributes its full slot; no same-cluster
        // bonus is possible. Score is identical to the empty-lookup case.
        let query = vec!["rust".to_string()];
        let chunk = vec!["rust".to_string()];
        let with_singleton = alias_map(&[("rust", "rust")]);
        let with_empty = std::collections::HashMap::new();
        let s1 = tag_match_score(&query, &chunk, &with_singleton, 0.7);
        let s2 = tag_match_score(&query, &chunk, &with_empty, 0.7);
        assert!((s1 - 1.0).abs() < 1e-9);
        assert!((s1 - s2).abs() < 1e-9);
    }

    #[test]
    fn tag_match_alias_discount_zero_disables_alias_contribution() {
        // alias_discount=0.0 ⇒ same-cluster matches contribute 0; the score
        // collapses to literal_overlap / union_classes.
        let query = vec!["bug".to_string()];
        let chunk = vec!["bugs".to_string()];
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let score = tag_match_score(&query, &chunk, &lookup, 0.0);
        assert_eq!(score, 0.0);
    }

    // ─── Case-insensitive scoring (`brn-83a.7.2.4.6`) ─────────────────

    #[test]
    fn tag_match_uppercase_query_matches_lowercase_chunk() {
        // Empty alias_lookup ⇒ score is pure literal Jaccard, but
        // case-insensitive: `["BUG"]` matches `["bug"]` perfectly.
        let query = vec!["BUG".to_string()];
        let chunk = vec!["bug".to_string()];
        let score = tag_match_score(&query, &chunk, &std::collections::HashMap::new(), 0.7);
        assert!(
            (score - 1.0).abs() < 1e-9,
            "case-insensitive literal match should score 1.0 (got {score})"
        );
    }

    #[test]
    fn tag_match_mixed_case_alias_match() {
        // The alias-discount slot must fire for mixed-case query inputs
        // — alias_lookup keys are lowercased at the boundary, so the
        // scoring path needs to lowercase its inputs to hit them.
        let query = vec!["BUG".to_string()];
        let chunk = vec!["bugs".to_string()];
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let score = tag_match_score(&query, &chunk, &lookup, 0.7);
        assert!(
            (score - 0.7).abs() < 1e-9,
            "mixed-case query should still get same-cluster discount (got {score})"
        );
    }

    #[test]
    fn tag_match_mixed_case_chunk_tags() {
        // Symmetric to the previous test: chunk tags from frontmatter may
        // be mixed-case (`#Bug`, `#BUGS`) — they must lowercase before
        // alias lookup so the cluster match still fires.
        let query = vec!["bug".to_string()];
        let chunk = vec!["BUGS".to_string()];
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let score = tag_match_score(&query, &chunk, &lookup, 0.7);
        assert!(
            (score - 0.7).abs() < 1e-9,
            "mixed-case chunk tag should still get same-cluster discount (got {score})"
        );
    }

    #[test]
    fn rank_candidates_orders_literal_above_alias_above_none() {
        // Three otherwise-identical candidates differing only in tags:
        //   literal-match `["bug"]`   → tag_match=1.0
        //   alias-match   `["bugs"]`  → tag_match=0.7
        //   no-match      `["unrel"]` → tag_match=0.0
        // All non-tag signals are zeroed below so the only contribution to
        // hybrid_score is `tag_match × tag_match_weight`. Under
        // `WeightProfile::Default` (1/6 each) the separation between literal
        // and alias is `(1.0 − alias_discount) × tag_match_weight = 0.05`
        // and between alias and none is `alias_discount × tag_match_weight
        // ≈ 0.117`. The bound `(1 − alias_discount) × tag_match_weight > 0`
        // is what guarantees the ordering — when other signals carry weight
        // (e.g. a real query mixing vector / bm25), the invariant only holds
        // for chunks that are otherwise tied. Pinning the zeroed-signals
        // baseline here is enough to catch regressions in the discount math.
        let make = |id: &str, tag: &str| CandidateSignals {
            chunk_id: id.into(),
            sim_vector: 0.0,
            bm25: 0.0,
            age_seconds: 0.0,
            pagerank_score: 0.0,
            tags: vec![tag.into()],
            importance: 0.0,
            file_path: format!("/{id}.md"),
            heading_path: String::new(),
            content: id.into(),
            token_estimate: 1,
            byte_start: 0,
            byte_end: 0,
            summary_kind: None,
        };
        let candidates = vec![
            make("alias", "bugs"),
            make("none", "unrelated"),
            make("literal", "bug"),
        ];
        let weights = Weights::from_profile(WeightProfile::Default);
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let query_tags = vec!["bug".to_string()];
        let results = rank_candidates(&candidates, &weights, &query_tags, &lookup);
        let order: Vec<&str> = results.iter().map(|r| r.chunk_id.as_str()).collect();
        assert_eq!(
            order,
            vec!["literal", "alias", "none"],
            "ordering invariant: literal > alias > none must hold"
        );
        // And the alias chunk must out-rank the no-match chunk strictly.
        let alias_score = results
            .iter()
            .find(|r| r.chunk_id == "alias")
            .unwrap()
            .hybrid_score;
        let none_score = results
            .iter()
            .find(|r| r.chunk_id == "none")
            .unwrap()
            .hybrid_score;
        assert!(
            alias_score > none_score,
            "alias-match must strictly out-rank no-match"
        );
    }

    #[test]
    fn test_rank_candidates_ordering() {
        let candidates = vec![
            make_candidate("low", 0.1, 0.1, 60.0, 0.0),
            make_candidate("high", 0.9, 0.9, 1.0, 0.8),
            make_candidate("mid", 0.5, 0.5, 15.0, 0.3),
        ];

        let weights = Weights::from_profile(WeightProfile::Default);
        let lookup = std::collections::HashMap::new();
        let results = rank_candidates(&candidates, &weights, &[], &lookup);

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
                pagerank_score: 0.0,
                tags: vec![],
                importance: 1.0,
                file_path: "/notes/a.md".into(),
                heading_path: String::new(),
                content: "keyword content".into(),
                token_estimate: 10,
                byte_start: 0,
                byte_end: 0,
                summary_kind: None,
            },
            CandidateSignals {
                chunk_id: "recent".into(),
                sim_vector: 0.3,
                bm25: 0.1,
                age_seconds: 3600.0, // 1 hour old
                pagerank_score: 0.0,
                tags: vec![],
                importance: 1.0,
                file_path: "/notes/b.md".into(),
                heading_path: String::new(),
                content: "recent content".into(),
                token_estimate: 10,
                byte_start: 0,
                byte_end: 0,
                summary_kind: None,
            },
        ];

        let alias_lookup = std::collections::HashMap::new();
        let lookup_results = rank_candidates(
            &candidates,
            &Weights::from_profile(WeightProfile::Lookup),
            &[],
            &alias_lookup,
        );
        let reflection_results = rank_candidates(
            &candidates,
            &Weights::from_profile(WeightProfile::Reflection),
            &[],
            &alias_lookup,
        );

        // Lookup should prefer the keyword hit
        assert_eq!(lookup_results[0].chunk_id, "keyword_hit");
        // Reflection should prefer recency
        assert_eq!(reflection_results[0].chunk_id, "recent");
    }

    #[test]
    fn test_rank_empty_candidates() {
        let lookup = std::collections::HashMap::new();
        let results = rank_candidates(
            &[],
            &Weights::from_profile(WeightProfile::Default),
            &[],
            &lookup,
        );
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

    // ─── Fusion confidence tests ─────────────────────────────────

    #[test]
    fn test_fusion_confidence_full_overlap() {
        let vector = vec!["a", "b", "c"];
        let fts = vec!["a", "b", "c"];
        let conf = compute_fusion_confidence(&vector, &fts, 3);
        assert_eq!(conf.confidence, 1.0);
        assert_eq!(conf.k, 3);
        assert_eq!(conf.overlap, 3);
    }

    #[test]
    fn test_fusion_confidence_no_overlap() {
        let vector = vec!["a", "b", "c"];
        let fts = vec!["d", "e", "f"];
        let conf = compute_fusion_confidence(&vector, &fts, 3);
        assert_eq!(conf.confidence, 0.0);
        assert_eq!(conf.k, 3);
        assert_eq!(conf.overlap, 0);
    }

    #[test]
    fn test_fusion_confidence_partial_overlap() {
        let vector = vec!["a", "b", "c", "d", "e"];
        let fts = vec!["c", "d", "x", "y", "z"];
        let conf = compute_fusion_confidence(&vector, &fts, 5);
        assert_eq!(conf.overlap, 2);
        assert!((conf.confidence - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn test_fusion_confidence_both_empty() {
        let conf = compute_fusion_confidence(&[], &[], 5);
        assert_eq!(conf.confidence, 1.0);
        assert_eq!(conf.k, 0);
    }

    #[test]
    fn test_fusion_confidence_one_empty() {
        let conf = compute_fusion_confidence(&["a", "b"], &[], 5);
        assert_eq!(conf.confidence, 0.0);
    }

    #[test]
    fn test_fusion_confidence_k_zero() {
        let conf = compute_fusion_confidence(&["a", "b"], &["a", "b"], 0);
        assert_eq!(conf.confidence, 0.0);
        assert_eq!(conf.k, 0);
        assert!(!conf.confidence.is_nan());
    }

    #[test]
    fn test_fusion_confidence_k_clamped() {
        let vector = vec!["a", "b"];
        let fts = vec!["a", "b", "c"];
        let conf = compute_fusion_confidence(&vector, &fts, 10);
        assert_eq!(conf.k, 2);
    }

    #[test]
    fn test_reranker_policy_default_thresholds() {
        let policy = RerankerPolicy::default();

        let high = FusionConfidence {
            confidence: 0.8,
            k: 5,
            overlap: 4,
        };
        assert!(!policy.should_rerank(&high));

        let low = FusionConfidence {
            confidence: 0.2,
            k: 5,
            overlap: 1,
        };
        assert!(policy.should_rerank(&low));

        let mid = FusionConfidence {
            confidence: 0.4,
            k: 5,
            overlap: 2,
        };
        assert!(!policy.should_rerank(&mid));
    }
}
