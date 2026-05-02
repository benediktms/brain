// Pre-existing technical debt: raw rusqlite usage inside test modules below.
// Architectural cleanup tracked separately; this allow keeps the workspace lint clean.
#![allow(clippy::disallowed_macros)]

/// Read-path orchestration pipeline.
///
/// Centralises hybrid search, expand, and reflect logic so that MCP handlers
/// and the CLI query command share a single implementation.
use std::collections::HashMap;
use std::sync::Arc;

use futures::future::join_all;
use tracing::{instrument, warn};

use std::sync::atomic::Ordering;

use crate::capsule::generate_stub_capsule;
use crate::embedder::Embed;
use crate::error::{BrainCoreError, Result};
use crate::metrics::Metrics;
use crate::ports::{
    ChunkMetaReader, ChunkSearcher, EpisodeReader, FtsSearcher, GraphLinkReader, TagAliasReader,
};
use crate::ranking::{
    CandidateSignals, FusionConfidence, RerankCandidate, Reranker, RerankerPolicy, Weights,
    compute_fusion_confidence, rank_candidates, resolve_intent,
};
use crate::retrieval::{MemoryKind, SearchResult, derive_kind, pack_minimal};
use crate::tokens::estimate_tokens;
use brain_persistence::db::Db;
use brain_persistence::db::summaries::SummaryRow;
use brain_persistence::store::VectorSearchMode;
use brain_persistence::store::{DEFAULT_NPROBES, StoreReader};

const CANDIDATE_LIMIT: usize = 50;

/// Expand a caller-supplied tag list through a per-brain alias projection.
///
/// For each input tag (lowercased), looks up its canonical form in
/// `alias_lookup`. If found, every raw tag in the same canonical class is
/// included; otherwise the tag passes through verbatim (as its own class).
/// Output is deduplicated and entirely lowercase. Idempotent on
/// re-application.
///
/// `alias_lookup` is expected to come from
/// `TagAliasReader::alias_lookup_for_brain`, which already lowercases its
/// keys and values. An empty map collapses this to a pure lowercasing pass —
/// the bit-for-bit behavior brains without a recluster run get.
fn expand_tags_via_aliases(
    input: &[String],
    alias_lookup: &std::collections::HashMap<String, String>,
) -> Vec<String> {
    if input.is_empty() {
        return Vec::new();
    }
    let lowered: Vec<String> = input.iter().map(|t| t.to_lowercase()).collect();
    if alias_lookup.is_empty() {
        let mut out = lowered;
        out.sort();
        out.dedup();
        return out;
    }
    // Build the inverse: canonical_tag → set of raw_tags sharing it.
    let mut by_canonical: HashMap<&str, Vec<&str>> = HashMap::new();
    for (raw, canonical) in alias_lookup {
        by_canonical
            .entry(canonical.as_str())
            .or_default()
            .push(raw.as_str());
    }
    let mut out: Vec<String> = Vec::with_capacity(lowered.len());
    for tag in &lowered {
        match alias_lookup.get(tag.as_str()) {
            Some(canonical) => {
                // The tag is known — emit the canonical itself plus every
                // raw_tag that maps to it. The canonical may not appear as
                // a key in `alias_lookup` (e.g. when the recluster job
                // chose a representative tag that has no separate raw row),
                // so it must be unioned in explicitly.
                out.push(canonical.clone());
                if let Some(members) = by_canonical.get(canonical.as_str()) {
                    out.extend(members.iter().map(|s| (*s).to_string()));
                }
            }
            None => {
                // Unknown tag — passes through as its own (singleton) class.
                out.push(tag.clone());
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Result of a reflect call.
#[derive(Debug)]
pub struct ReflectResult {
    pub topic: String,
    pub budget_tokens: usize,
    pub episodes: Vec<SummaryRow>,
    pub search_result: SearchResult,
}

/// Diagnostic counters from a single search_ranked execution.
#[derive(Debug, Clone, Default)]
pub struct PipelineDiagnostics {
    pub vector_candidates: usize,
    pub fts_candidates: usize,
    pub union_size: usize,
    pub reranked: bool,
}

/// Result of a federated ranked search (pre-pack).
///
/// Contains the merged ranked results with brain attribution, before packing
/// into stubs. Used by callers that need full content access (e.g., LOD
/// resolution) before the lossy pack step.
#[derive(Debug)]
pub struct FederatedRankedResult {
    /// Merged ranked results sorted by hybrid_score descending.
    pub ranked: Vec<crate::ranking::RankedResult>,
    /// Map from chunk_id → brain_name for each result.
    pub chunk_brain: HashMap<String, String>,
    /// Fusion confidence from the primary brain.
    pub fusion_confidence: Option<crate::ranking::FusionConfidence>,
}

/// Parameters for a hybrid search query.
pub struct SearchParams<'a> {
    pub query: &'a str,
    pub intent: &'a str,
    pub budget_tokens: usize,
    pub k: usize,
    pub query_tags: &'a [String],
    /// Controls the ANN (Approximate Nearest Neighbor) vs exact search
    /// tradeoff. Defaults to `AnnRefined`. See [`VectorSearchMode`] for
    /// details on each variant.
    pub mode: VectorSearchMode,
    /// When true, follow 1-hop outgoing links from top-K vector results and
    /// add the linked chunks to the candidate pool before ranking.
    /// Defaults to `false`.
    pub graph_expand: bool,
    /// Optional brain_id filter for FTS queries. `None` = workspace-global,
    /// `Some(&[id])` = scope to specific brain(s).
    pub brain_ids: Option<&'a [String]>,
    /// Owning brain for read-time projections that are brain-scoped (e.g.
    /// the tag-alias lookup). `None` = no brain context (workspace-wide
    /// introspection paths). Empty-string values are normalized to `None`
    /// by [`SearchParams::with_brain_id`] — callers do not need to handle
    /// this case explicitly.
    pub brain_id: Option<&'a str>,
    /// Filter by result kind. Empty = no filter (all kinds).
    pub kinds: &'a [MemoryKind],
    /// Only include results with effective timestamp >= this (Unix seconds).
    pub time_after: Option<i64>,
    /// Only include results with effective timestamp <= this (Unix seconds).
    pub time_before: Option<i64>,
    /// Require ALL of these tags (case-insensitive, AND logic).
    pub tags_require: &'a [String],
    /// Exclude results matching ANY of these tags (case-insensitive, NOR logic).
    pub tags_exclude: &'a [String],
}

impl<'a> SearchParams<'a> {
    /// Construct with required fields, defaulting to `AnnRefined` mode.
    pub fn new(
        query: &'a str,
        intent: &'a str,
        budget_tokens: usize,
        k: usize,
        query_tags: &'a [String],
    ) -> Self {
        Self {
            query,
            intent,
            budget_tokens,
            k,
            query_tags,
            mode: VectorSearchMode::default(),
            graph_expand: false,
            brain_ids: None,
            brain_id: None,
            kinds: &[],
            time_after: None,
            time_before: None,
            tags_require: &[],
            tags_exclude: &[],
        }
    }

    /// Override the vector search mode.
    pub fn with_mode(mut self, mode: VectorSearchMode) -> Self {
        self.mode = mode;
        self
    }

    /// Scope FTS queries to specific brain(s).
    pub fn with_brain_ids(mut self, brain_ids: Option<&'a [String]>) -> Self {
        self.brain_ids = brain_ids;
        self
    }

    /// Set the owning brain for read-time, brain-scoped projections (alias
    /// lookup, etc.). `Some("")` is normalized to `None` — callers do not
    /// need to guard against empty strings.
    pub fn with_brain_id(mut self, brain_id: Option<&'a str>) -> Self {
        self.brain_id = brain_id.filter(|s| !s.is_empty());
        self
    }

    /// Filter results by kind.
    pub fn with_kinds(mut self, kinds: &'a [MemoryKind]) -> Self {
        self.kinds = kinds;
        self
    }

    /// Only include results modified/created after this timestamp.
    pub fn with_time_after(mut self, ts: Option<i64>) -> Self {
        self.time_after = ts;
        self
    }

    /// Only include results modified/created before this timestamp.
    pub fn with_time_before(mut self, ts: Option<i64>) -> Self {
        self.time_before = ts;
        self
    }

    /// Require ALL of these tags (AND logic, case-insensitive).
    pub fn with_tags_require(mut self, tags: &'a [String]) -> Self {
        self.tags_require = tags;
        self
    }

    /// Exclude results matching ANY of these tags (NOR logic).
    pub fn with_tags_exclude(mut self, tags: &'a [String]) -> Self {
        self.tags_exclude = tags;
        self
    }
}

/// Orchestrates the read-path: hybrid search, expand, and reflect.
///
/// The `S` type parameter is the LanceDB store implementation, which must
/// implement [`ChunkSearcher`]. It defaults to [`StoreReader`] for production
/// use. Tests may substitute any type that implements `ChunkSearcher` without
/// opening real LanceDB storage.
///
/// The `D` type parameter is the SQLite-backed persistence implementation,
/// which must implement [`ChunkMetaReader`], [`FtsSearcher`], and
/// [`EpisodeReader`]. It defaults to [`Db`] for production use.
pub struct QueryPipeline<'a, S = StoreReader, D = Db>
where
    S: ChunkSearcher + Send + Sync,
    D: ChunkMetaReader
        + FtsSearcher
        + EpisodeReader
        + GraphLinkReader
        + TagAliasReader
        + Send
        + Sync,
{
    /// SQLite database — abstracted via port traits.
    db: &'a D,
    /// LanceDB store — abstracted via [`ChunkSearcher`]; defaults to [`StoreReader`].
    store: &'a S,
    embedder: &'a Arc<dyn Embed>,
    metrics: &'a Arc<Metrics>,
    reranker: Option<&'a dyn Reranker>,
    reranker_policy: RerankerPolicy,
}

impl<'a, S, D> QueryPipeline<'a, S, D>
where
    S: ChunkSearcher + Send + Sync,
    D: ChunkMetaReader
        + FtsSearcher
        + EpisodeReader
        + GraphLinkReader
        + TagAliasReader
        + Send
        + Sync,
{
    pub fn new(
        db: &'a D,
        store: &'a S,
        embedder: &'a Arc<dyn Embed>,
        metrics: &'a Arc<Metrics>,
    ) -> Self {
        Self {
            db,
            store,
            embedder,
            metrics,
            reranker: None,
            reranker_policy: RerankerPolicy::default(),
        }
    }

    /// Attach a cross-encoder reranker with the given policy.
    ///
    /// When attached, the pipeline computes fusion confidence after hybrid
    /// retrieval and conditionally invokes the reranker on the top-N fused
    /// candidates when confidence is below the policy threshold.
    pub fn with_reranker(mut self, reranker: &'a dyn Reranker, policy: RerankerPolicy) -> Self
    where
        Self: Sized,
    {
        self.reranker = Some(reranker);
        self.reranker_policy = policy;
        self
    }

    /// Hybrid search: vector + FTS union, enriched, ranked, packed within budget.
    #[instrument(skip_all)]
    pub async fn search(&self, params: &SearchParams<'_>) -> Result<SearchResult> {
        let (ranked, confidence) = self.search_ranked(params).await?;
        let ml_summaries = self.load_ml_summaries(&ranked)?;
        let mut result = pack_minimal(
            &ranked,
            params.budget_tokens,
            params.k,
            false,
            &ml_summaries,
        );
        result.fusion_confidence = Some(confidence);
        Ok(result)
    }

    /// Hybrid search returning stubs with per-signal score breakdowns.
    #[instrument(skip_all)]
    pub async fn search_with_scores(&self, params: &SearchParams<'_>) -> Result<SearchResult> {
        let (ranked, confidence) = self.search_ranked(params).await?;
        let ml_summaries = self.load_ml_summaries(&ranked)?;
        let mut result = pack_minimal(&ranked, params.budget_tokens, params.k, true, &ml_summaries);
        result.fusion_confidence = Some(confidence);
        Ok(result)
    }

    /// Batch-load ML summaries for a set of ranked results.
    fn load_ml_summaries(
        &self,
        ranked: &[crate::ranking::RankedResult],
    ) -> Result<HashMap<String, String>> {
        let chunk_ids: Vec<&str> = ranked.iter().map(|r| r.chunk_id.as_str()).collect();
        self.db.get_ml_summaries_for_chunks(&chunk_ids)
    }

    /// Core search logic: returns ranked results with fusion confidence and diagnostics.
    pub(crate) async fn search_ranked_with_diagnostics(
        &self,
        params: &SearchParams<'_>,
    ) -> Result<(
        Vec<crate::ranking::RankedResult>,
        FusionConfidence,
        PipelineDiagnostics,
    )> {
        let query = params.query;
        let query_tags = params.query_tags;
        let mode = params.mode;
        let graph_expand = params.graph_expand;
        let brain_ids = params.brain_ids;
        let profile = resolve_intent(params.intent);
        let weights = Weights::from_profile(profile);

        // 0. Read-time alias projection: build the per-brain (raw_tag →
        //    canonical_tag) map and pre-expand tags_require / tags_exclude
        //    through it. Empty `brain_id` (or empty alias table) collapses
        //    to today's literal-match behavior. Held in scope through the
        //    rank stage — `brn-83a.7.2.4.4` will pass it to `tag_match_score`.
        let alias_lookup = match params.brain_id {
            Some(b) if !b.is_empty() => self.db.alias_lookup_for_brain(b).unwrap_or_default(),
            _ => HashMap::new(),
        };
        // Per-input-tag cluster expansions for the require side. Each entry
        // is the full canonical-class membership of one input tag, lowercased.
        // Filter semantics: AND across these vectors, OR within each. Empty
        // alias table ⇒ each cluster is just `[input_lower]` (literal-only).
        let tags_require_clusters: Vec<Vec<String>> = params
            .tags_require
            .iter()
            .map(|t| expand_tags_via_aliases(std::slice::from_ref(t), &alias_lookup))
            .collect();
        // Exclude side: union of every cluster's members. Filter semantics:
        // chunk must contain NONE of these. Flat vector is correct here.
        let tags_exclude_expanded = expand_tags_via_aliases(params.tags_exclude, &alias_lookup);

        // 1. Embed query
        let vecs =
            crate::embedder::embed_batch_async(self.embedder, vec![query.to_string()]).await?;
        let query_vec = vecs
            .into_iter()
            .next()
            .ok_or_else(|| BrainCoreError::Embedding("Empty embedding result".into()))?;

        // 2. Vector search (top-50)
        let vector_results = self
            .store
            .query(&query_vec, CANDIDATE_LIMIT, DEFAULT_NPROBES, mode, params.brain_id)
            .await?;
        let vector_count = vector_results.len();

        // 3. FTS search (top-50, gracefully degrade on failure)
        let fts_results = match self.db.search_fts(query, CANDIDATE_LIMIT, brain_ids) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "FTS search failed, continuing with vector-only");
                self.metrics.query_errors.fetch_add(1, Ordering::Relaxed);
                Vec::new()
            }
        };
        let fts_count = fts_results.len();

        // 4. Union + deduplicate by chunk_id
        let mut candidates: HashMap<String, CandidateSignals> = HashMap::new();

        for vr in &vector_results {
            let sim = 1.0 - vr.score.unwrap_or(1.0) as f64;
            candidates.insert(
                vr.chunk_id.clone(),
                CandidateSignals {
                    chunk_id: vr.chunk_id.clone(),
                    sim_vector: sim.clamp(0.0, 1.0),
                    bm25: 0.0,
                    age_seconds: 0.0,
                    pagerank_score: 0.0,
                    tags: vec![],
                    importance: 1.0,
                    file_path: vr.file_path.clone(),
                    heading_path: String::new(),
                    content: vr.content.clone(),
                    token_estimate: estimate_tokens(&vr.content),
                    byte_start: 0,
                    byte_end: 0,
                    summary_kind: None,
                },
            );
        }

        for fr in &fts_results {
            if let Some(existing) = candidates.get_mut(&fr.chunk_id) {
                existing.bm25 = fr.score;
            } else {
                candidates.insert(
                    fr.chunk_id.clone(),
                    CandidateSignals {
                        chunk_id: fr.chunk_id.clone(),
                        sim_vector: 0.0,
                        bm25: fr.score,
                        age_seconds: 0.0,
                        pagerank_score: 0.0,
                        tags: vec![],
                        importance: 1.0,
                        file_path: String::new(),
                        heading_path: String::new(),
                        content: String::new(),
                        token_estimate: 0,
                        byte_start: 0,
                        byte_end: 0,
                        summary_kind: None,
                    },
                );
            }
        }

        // 4a. Compute fusion confidence
        let vector_ids: Vec<&str> = vector_results.iter().map(|r| r.chunk_id.as_str()).collect();
        let fts_ids: Vec<&str> = fts_results.iter().map(|r| r.chunk_id.as_str()).collect();
        let fusion_confidence =
            compute_fusion_confidence(&vector_ids, &fts_ids, self.reranker_policy.confidence_k);

        let union_count = candidates.len();

        if candidates.is_empty() {
            return Ok((
                vec![],
                fusion_confidence,
                PipelineDiagnostics {
                    vector_candidates: vector_count,
                    fts_candidates: fts_count,
                    union_size: 0,
                    reranked: false,
                },
            ));
        }

        // 5. Enrich from SQLite (single batched JOIN — pagerank_score comes from files table)
        let chunk_ids: Vec<String> = candidates.keys().cloned().collect();
        let enrichment = self.db.get_chunks_by_ids(&chunk_ids);
        let now = crate::utils::now_ts();

        if let Ok(rows) = enrichment {
            for row in &rows {
                if let Some(candidate) = candidates.get_mut(&row.chunk_id) {
                    candidate.file_path = row.file_path.clone();
                    candidate.heading_path = row.heading_path.clone();
                    candidate.content = row.content.clone();
                    candidate.token_estimate = row.token_estimate;
                    candidate.byte_start = row.byte_start;
                    candidate.byte_end = row.byte_end;
                    candidate.pagerank_score = row.pagerank_score;
                    // Combine frontmatter tags + heading-derived tags (deduplicated)
                    let mut tags: Vec<String> = row.tags.clone();
                    tags.extend(
                        row.heading_path
                            .split(" > ")
                            .map(|segment| segment.trim_start_matches('#').trim())
                            .filter(|segment| !segment.is_empty())
                            .map(|segment| segment.to_lowercase()),
                    );
                    tags.sort();
                    tags.dedup();
                    candidate.tags = tags;
                    candidate.importance = row.importance;

                    // Prefer disk mtime (real edit time) over last_indexed_at
                    let effective_ts = row.disk_modified_at.or(row.last_indexed_at);
                    if let Some(ts) = effective_ts {
                        candidate.age_seconds = (now - ts).max(0) as f64;
                    }
                }
            }
        }

        // 5a. Enrich summary metadata for sum:-prefixed candidates.
        //     Loads kind, tags, importance, and created_at from the summaries
        //     table so that derive_kind, tag filtering, and recency all work
        //     correctly for episodes/reflections/procedures.
        {
            let summary_ids: Vec<String> = candidates
                .keys()
                .filter(|id| id.starts_with("sum:"))
                .map(|id| id["sum:".len()..].to_string())
                .collect();

            if !summary_ids.is_empty()
                && let Ok(meta_map) = self.db.get_summary_metadata(&summary_ids)
            {
                for (chunk_id, candidate) in candidates.iter_mut() {
                    if let Some(raw_id) = chunk_id.strip_prefix("sum:")
                        && let Some(meta) = meta_map.get(raw_id)
                    {
                        candidate.summary_kind = Some(meta.kind.clone());
                        if !meta.tags.is_empty() {
                            candidate.tags = meta.tags.clone();
                        }
                        candidate.importance = meta.importance;
                        if meta.created_at > 0 {
                            candidate.age_seconds = (now - meta.created_at).max(0) as f64;
                        }
                    }
                }
            }
        }

        // Remove FTS-only candidates that weren't found in SQLite
        let mut candidate_vec: Vec<CandidateSignals> = candidates
            .into_values()
            .filter(|c| !c.content.is_empty())
            .collect();

        // 5c. Apply metadata filters (post-enrichment, pre-ranking)
        let has_filters = !params.kinds.is_empty()
            || params.time_after.is_some()
            || params.time_before.is_some()
            || !params.tags_require.is_empty()
            || !params.tags_exclude.is_empty();

        if has_filters {
            candidate_vec.retain(|c| {
                // Kind filter
                if !params.kinds.is_empty() {
                    let kind = derive_kind(&c.chunk_id, c.summary_kind.as_deref());
                    if !params.kinds.contains(&kind) {
                        return false;
                    }
                }
                // Time filters — skip candidates with unknown timestamps
                // (age_seconds == 0.0 means no timestamp was found during enrichment)
                if (params.time_after.is_some() || params.time_before.is_some())
                    && c.age_seconds > 0.0
                {
                    let effective_ts = now - c.age_seconds as i64;
                    if let Some(after) = params.time_after
                        && effective_ts < after
                    {
                        return false;
                    }
                    if let Some(before) = params.time_before
                        && effective_ts > before
                    {
                        return false;
                    }
                }
                // Tag filters — clusters / expanded vectors precomputed above.
                if !tags_require_clusters.is_empty() || !tags_exclude_expanded.is_empty() {
                    let lower_tags: Vec<String> = c.tags.iter().map(|t| t.to_lowercase()).collect();
                    // Require: AND across input tags, OR within each cluster.
                    if !tags_require_clusters
                        .iter()
                        .all(|cluster| cluster.iter().any(|m| lower_tags.iter().any(|t| t == m)))
                    {
                        return false;
                    }
                    // Exclude (NOR): none of the expanded set may be present.
                    if tags_exclude_expanded
                        .iter()
                        .any(|exc| lower_tags.iter().any(|t| t == exc))
                    {
                        return false;
                    }
                }
                true
            });
        }

        // 5d. 1-hop graph expansion: follow outgoing links from top-K candidates
        //     and inject (or boost) linked chunks in the candidate pool before ranking.
        let graph_only_chunk_ids: std::collections::HashSet<String> =
            if graph_expand && !candidate_vec.is_empty() {
                self.expand_graph_links(&mut candidate_vec, 10, 20)
            } else {
                std::collections::HashSet::new()
            };

        // 6. Rank — pass through the per-brain alias_lookup built in step 0
        //    so `tag_match_score` can reward same-cluster matches at the
        //    `Weights::alias_discount` rate (0.7× by default).
        let mut ranked = rank_candidates(&candidate_vec, &weights, query_tags, &alias_lookup);

        // Override the discovery reason for chunks that only entered the
        // candidate pool through graph expansion. Boost-only updates leave
        // the original vector/keyword classification intact.
        if !graph_only_chunk_ids.is_empty() {
            for r in ranked.iter_mut() {
                if graph_only_chunk_ids.contains(&r.chunk_id) {
                    r.expansion_reason = crate::ranking::ExpansionReason::GraphLink;
                }
            }
        }

        // 7. Adaptive reranking: if confidence is low and a reranker is attached,
        //    rerank the top-N fused candidates using the cross-encoder.
        let mut did_rerank = false;
        if let Some(reranker) = self.reranker
            && self.reranker_policy.should_rerank(&fusion_confidence)
        {
            let depth = self.reranker_policy.rerank_depth.min(ranked.len());
            let candidates: Vec<RerankCandidate> = ranked
                .iter()
                .take(depth)
                .map(|r| RerankCandidate {
                    chunk_id: r.chunk_id.clone(),
                    text: generate_stub_capsule(Some(&r.heading_path), &r.content),
                })
                .collect();

            match reranker.rerank(query, &candidates) {
                Ok(reranked) => {
                    did_rerank = true;
                    let score_map: std::collections::HashMap<&str, f64> = reranked
                        .iter()
                        .map(|r| (r.chunk_id.as_str(), r.score))
                        .collect();

                    for result in ranked.iter_mut().take(depth) {
                        if let Some(&score) = score_map.get(result.chunk_id.as_str()) {
                            result.hybrid_score = score;
                        }
                    }

                    ranked.sort_by(|a, b| {
                        b.hybrid_score
                            .partial_cmp(&a.hybrid_score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.chunk_id.cmp(&b.chunk_id))
                    });
                }
                Err(e) => {
                    warn!(error = %e, "Reranker failed, continuing with hybrid-only ranking");
                }
            }
        }

        Ok((
            ranked,
            fusion_confidence,
            PipelineDiagnostics {
                vector_candidates: vector_count,
                fts_candidates: fts_count,
                union_size: union_count,
                reranked: did_rerank,
            },
        ))
    }

    /// Core search logic: returns ranked results with fusion confidence.
    ///
    /// Thin wrapper around [`search_ranked_with_diagnostics`] that discards the
    /// diagnostic counters. Preserves the original call sites unchanged.
    pub(crate) async fn search_ranked(
        &self,
        params: &SearchParams<'_>,
    ) -> Result<(Vec<crate::ranking::RankedResult>, FusionConfidence)> {
        let (ranked, fusion, _diag) = self.search_ranked_with_diagnostics(params).await?;
        Ok((ranked, fusion))
    }

    /// 1-hop graph expansion: follow outgoing links from the top `seed_count` candidates
    /// (by composite score) and inject or boost linked chunks in `candidate_vec`.
    ///
    /// Expansion candidates receive `sim_vector = parent_score * 0.5` (graph penalty).
    /// At most `max_expansion` file IDs are fetched.
    ///
    /// Returns the set of chunk IDs that were *newly introduced* by graph
    /// expansion (i.e. not already in `candidate_vec` from vector/FTS).
    /// Boost-only updates to existing candidates are not included; their
    /// discovery reason stays whatever vector/FTS classification produced.
    fn expand_graph_links(
        &self,
        candidate_vec: &mut Vec<CandidateSignals>,
        seed_count: usize,
        max_expansion: usize,
    ) -> std::collections::HashSet<String> {
        let mut graph_only: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Sort by composite signal to pick top seed candidates.
        let mut seeds = candidate_vec.clone();
        seeds.sort_by(|a, b| {
            let sa = a.sim_vector + a.bm25 + a.pagerank_score;
            let sb = b.sim_vector + b.bm25 + b.pagerank_score;
            sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
        });

        // For each seed, get 1-hop outlink file_ids, carrying the parent's vector score.
        // Expansion candidates receive sim_vector = parent_score * 0.5 (graph penalty).
        let mut expansion_entries: Vec<(String, f64)> = Vec::new(); // (target_file_id, parent_sim)
        for seed in seeds.iter().take(seed_count) {
            // Derive file_id from chunk_id (format: "file_id:chunk_ord")
            let file_id = seed
                .chunk_id
                .rsplit_once(':')
                .map(|(prefix, _)| prefix.to_string())
                .unwrap_or_else(|| seed.chunk_id.clone());

            // Use the composite signal (vector + keyword) as the parent score
            // so that FTS-strong seeds also produce meaningful expansion boosts.
            let parent_sim = (seed.sim_vector + seed.bm25).min(1.0);
            match self.db.get_outlinks(&file_id) {
                Ok(outlinks) => {
                    for target_file_id in outlinks {
                        if !expansion_entries
                            .iter()
                            .any(|(fid, _)| fid == &target_file_id)
                        {
                            expansion_entries.push((target_file_id, parent_sim));
                        }
                    }
                }
                Err(e) => {
                    warn!(file_id = %file_id, error = %e, "graph expansion: get_outlinks failed");
                }
            }
        }

        // Cap at max_expansion file_ids.
        expansion_entries.truncate(max_expansion);

        if expansion_entries.is_empty() {
            return graph_only;
        }

        let expansion_file_ids: Vec<String> = expansion_entries
            .iter()
            .map(|(fid, _)| fid.clone())
            .collect();
        let parent_sim_map: std::collections::HashMap<String, f64> =
            expansion_entries.into_iter().collect();

        match self.db.get_chunks_by_file_ids(&expansion_file_ids) {
            Ok(expansion_chunks) => {
                let now = crate::utils::now_ts();
                for chunk in expansion_chunks {
                    let graph_sim =
                        parent_sim_map.get(&chunk.file_id).copied().unwrap_or(0.0) * 0.5;

                    if let Some(existing) = candidate_vec
                        .iter_mut()
                        .find(|c| c.chunk_id == chunk.chunk_id)
                    {
                        if graph_sim > existing.sim_vector {
                            existing.sim_vector = graph_sim;
                        }
                        continue;
                    }

                    let effective_ts = chunk.disk_modified_at.or(chunk.last_indexed_at);
                    let age_seconds = if let Some(ts) = effective_ts {
                        (now - ts).max(0) as f64
                    } else {
                        0.0
                    };
                    let chunk_id = chunk.chunk_id.clone();
                    candidate_vec.push(CandidateSignals {
                        chunk_id: chunk.chunk_id,
                        sim_vector: graph_sim,
                        bm25: 0.0,
                        age_seconds,
                        pagerank_score: chunk.pagerank_score,
                        tags: vec![],
                        importance: 1.0,
                        file_path: chunk.file_path,
                        heading_path: chunk.heading_path,
                        content: chunk.content.clone(),
                        token_estimate: estimate_tokens(&chunk.content),
                        byte_start: chunk.byte_start,
                        byte_end: chunk.byte_end,
                        summary_kind: None,
                    });
                    graph_only.insert(chunk_id);
                }
            }
            Err(e) => {
                warn!(error = %e, "graph expansion: get_chunks_by_file_ids failed");
            }
        }
        graph_only
    }

    /// Reflect: fetch recent episodes + search for related chunks, return combined result.
    #[instrument(skip_all)]
    pub async fn reflect(
        &self,
        topic: String,
        budget_tokens: usize,
        brain_id: &str,
    ) -> Result<ReflectResult> {
        let episodes = self.db.list_episodes(10, brain_id).unwrap_or_default();
        self.reflect_with_episodes(topic, budget_tokens, episodes)
            .await
    }

    /// Reflect with a caller-provided episode list.
    ///
    /// Callers use this variant when the episode scope has already been
    /// determined — e.g. multi-brain or "all" mode from `memory.reflect`
    /// prepare. The `episodes` slice is used as-is; no additional episode
    /// loading is performed.
    ///
    /// `brain_id` post-filter is intentionally absent here.
    ///
    /// Note/chunk retrieval remains workspace-global by design. Brain scoping
    /// applies to task/record domains and their related metadata.
    #[instrument(skip_all)]
    pub async fn reflect_with_episodes(
        &self,
        topic: String,
        budget_tokens: usize,
        episodes: Vec<SummaryRow>,
    ) -> Result<ReflectResult> {
        let empty_tags: Vec<String> = Vec::new();
        // TODO(spike): reflect may benefit from graph_expand — linked notes
        // often contain supporting evidence for the reflection topic
        let params = SearchParams::new(&topic, "reflection", budget_tokens / 2, 5, &empty_tags);
        let search_result = self.search(&params).await?;

        Ok(ReflectResult {
            topic,
            budget_tokens,
            episodes,
            search_result,
        })
    }
}

// ---------------------------------------------------------------------------
// FederatedPipeline
// ---------------------------------------------------------------------------

/// Federated search across multiple brain projects.
///
/// Task/record metadata queries use the single shared SQLite `db` (with
/// brain_id scoping where applicable). Vector search fans out concurrently
/// to each brain's per-brain LanceDB store.
///
/// The `S` type parameter is the store implementation for each brain, which
/// must implement [`ChunkSearcher`]. It defaults to [`StoreReader`] for
/// production use. Tests may substitute any type implementing `ChunkSearcher`.
///
/// The `D` type parameter is the SQLite-backed persistence implementation,
/// which must implement [`ChunkMetaReader`], [`FtsSearcher`], and
/// [`EpisodeReader`]. It defaults to [`Db`] for production use.
///
/// Construct via the builder or directly:
/// ```ignore
/// let pipeline = FederatedPipeline {
///     db: &ctx.db,
///     brains: vec![
///         ("local".into(), local_id, Some(local_store)),
///         ("remote".into(), remote_ctx.brain_id, remote_ctx.store),
///     ],
///     embedder: &embedder,
///     metrics: &metrics,
/// };
/// ```
pub struct FederatedPipeline<'a, S = StoreReader, D = Db>
where
    S: ChunkSearcher + Send + Sync,
    D: ChunkMetaReader
        + FtsSearcher
        + EpisodeReader
        + GraphLinkReader
        + TagAliasReader
        + Send
        + Sync,
{
    /// Shared unified SQLite database — abstracted via port traits.
    pub db: &'a D,
    /// Per-brain entries: `(brain_name, brain_id, store)`.
    ///
    /// `brain_id` is the brain's UUID — used by per-brain SearchParams so
    /// downstream projections (alias lookup, etc.) can scope correctly.
    /// `store` is `None` when the brain's LanceDB has not yet been
    /// initialised — that brain is skipped for vector search with a warning.
    pub brains: Vec<(String, String, Option<S>)>,
    /// Shared embedder — query is embedded once, used across all brains.
    pub embedder: &'a Arc<dyn crate::embedder::Embed>,
    /// Shared metrics handle.
    pub metrics: &'a Arc<crate::metrics::Metrics>,
}

impl<'a, S, D> FederatedPipeline<'a, S, D>
where
    S: ChunkSearcher + Send + Sync,
    D: ChunkMetaReader
        + FtsSearcher
        + EpisodeReader
        + GraphLinkReader
        + TagAliasReader
        + Send
        + Sync,
{
    /// Search across all configured brains, returning merged ranked results
    /// with brain attribution before packing into stubs.
    ///
    /// This is the core federated search logic. Callers that need pre-pack
    /// access to full `RankedResult` content (e.g., LOD resolution) should
    /// use this method directly.
    pub async fn search_ranked_federated(
        &self,
        params: &SearchParams<'_>,
    ) -> Result<FederatedRankedResult> {
        // ── 1. Build per-brain vector-search futures ──────────────────────────
        type BrainResult = (
            String,
            Vec<crate::ranking::RankedResult>,
            Option<crate::ranking::FusionConfidence>,
        );

        let mode = params.mode;
        let mut futs: Vec<
            std::pin::Pin<Box<dyn std::future::Future<Output = BrainResult> + Send + '_>>,
        > = Vec::new();

        for (brain_name, brain_id, store_opt) in &self.brains {
            let store = match store_opt {
                Some(s) => s,
                None => {
                    warn!(
                        brain = %brain_name,
                        "skipping brain: LanceDB not initialised"
                    );
                    continue;
                }
            };

            // Each brain gets a QueryPipeline backed by the SHARED db so that
            // FTS and chunk-enrichment queries run against the unified SQLite.
            let pipeline = QueryPipeline::new(self.db, store, self.embedder, self.metrics);
            let brain_name = brain_name.clone();
            let brain_id = brain_id.clone();
            let query = params.query.to_string();
            let intent = params.intent.to_string();
            let query_tags = params.query_tags.to_vec();
            let kinds = params.kinds.to_vec();
            let tags_require = params.tags_require.to_vec();
            let tags_exclude = params.tags_exclude.to_vec();
            let time_after = params.time_after;
            let time_before = params.time_before;
            futs.push(Box::pin(async move {
                // TODO(spike): federated search skips graph_expand — evaluate
                // whether cross-brain queries should follow links within each brain
                let sp = SearchParams {
                    query: &query,
                    intent: &intent,
                    budget_tokens: 0, // unused by search_ranked
                    k: 0,             // unused by search_ranked
                    query_tags: &query_tags,
                    mode,
                    graph_expand: false,
                    brain_ids: None,
                    brain_id: Some(brain_id.as_str()),
                    kinds: &kinds,
                    time_after,
                    time_before,
                    tags_require: &tags_require,
                    tags_exclude: &tags_exclude,
                };
                match pipeline.search_ranked(&sp).await {
                    Ok((ranked, confidence)) => (brain_name, ranked, Some(confidence)),
                    Err(e) => {
                        warn!(brain = %brain_name, error = %e, "brain search failed");
                        (brain_name, vec![], None)
                    }
                }
            }));
        }

        // ── 2. Fan out and collect ────────────────────────────────────────────
        let all_results: Vec<BrainResult> = join_all(futs).await;

        // ── 3. Merge: deduplicate by chunk_id, keep highest score ────────────
        //
        // Every brain runs FTS against the same shared DB, so the same chunks
        // can appear in multiple brains' results. We deduplicate by chunk_id,
        // keeping the entry with the highest hybrid_score. The brain
        // attribution goes to whichever brain produced the best score for
        // that chunk (typically the brain whose vector store actually
        // contains the chunk).
        let mut chunk_brain: HashMap<String, String> = HashMap::new();
        let mut best_by_chunk: HashMap<String, crate::ranking::RankedResult> = HashMap::new();

        let mut first_confidence: Option<crate::ranking::FusionConfidence> = None;
        for (brain_name, ranked, confidence) in all_results {
            if first_confidence.is_none() {
                first_confidence = confidence;
            }
            for result in ranked {
                // Determinism: when two brains report the same chunk with
                // identical scores, pick the lexicographically-smaller brain
                // name as the canonical attribution. Strict `>` lets ties
                // fall through to the explicit tiebreak below.
                let replace = match best_by_chunk.get(&result.chunk_id) {
                    None => true,
                    Some(existing) => {
                        if result.hybrid_score > existing.hybrid_score {
                            true
                        } else if result.hybrid_score < existing.hybrid_score {
                            false
                        } else {
                            // Tied scores: keep whichever brain name sorts
                            // lexicographically smaller.
                            let current_brain = chunk_brain
                                .get(&result.chunk_id)
                                .map(String::as_str)
                                .unwrap_or("");
                            brain_name.as_str() < current_brain
                        }
                    }
                };
                if replace {
                    chunk_brain.insert(result.chunk_id.clone(), brain_name.clone());
                    best_by_chunk.insert(result.chunk_id.clone(), result);
                }
            }
        }

        let mut merged: Vec<crate::ranking::RankedResult> = best_by_chunk.into_values().collect();

        // ── 4. Sort by hybrid_score descending, lex chunk_id as tiebreak ──────
        merged.sort_by(|a, b| {
            b.hybrid_score
                .partial_cmp(&a.hybrid_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.chunk_id.cmp(&b.chunk_id))
        });

        Ok(FederatedRankedResult {
            ranked: merged,
            chunk_brain,
            fusion_confidence: first_confidence,
        })
    }

    /// Search across all configured brains.
    ///
    /// The query is embedded once. Vector search fans out concurrently to
    /// each brain's LanceDB store. FTS and chunk enrichment run against the
    /// shared `db`. Results are merged by `hybrid_score` (descending) and
    /// packed into a single `SearchResult` within the token budget.
    ///
    /// Brains whose `store` is `None` (LanceDB not yet initialised) are
    /// skipped with a warning — the search continues with the remaining brains.
    pub async fn search(
        &self,
        params: &SearchParams<'_>,
        include_scores: bool,
    ) -> Result<crate::retrieval::SearchResult> {
        let federated = self.search_ranked_federated(params).await?;

        // Pack into SearchResult — ML summary preloading skipped for federated
        // search since results span multiple brain namespaces.
        let mut search_result = crate::retrieval::pack_minimal(
            &federated.ranked,
            params.budget_tokens,
            params.k,
            include_scores,
            &HashMap::new(),
        );

        // Annotate each stub with its source brain name.
        for stub in &mut search_result.results {
            stub.brain_name = federated.chunk_brain.get(&stub.memory_id).cloned();
        }

        // Attach fusion confidence from primary brain.
        search_result.fusion_confidence = federated.fusion_confidence;

        Ok(search_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| (*x).to_string()).collect()
    }

    fn alias_map(rows: &[(&str, &str)]) -> HashMap<String, String> {
        rows.iter()
            .map(|(raw, canonical)| (raw.to_string(), canonical.to_string()))
            .collect()
    }

    #[test]
    fn expand_tags_via_aliases_empty_alias_map_passthrough() {
        let out = expand_tags_via_aliases(&s(&["Bug", "perf"]), &HashMap::new());
        assert_eq!(out, s(&["bug", "perf"]));
    }

    #[test]
    fn expand_tags_via_aliases_empty_input_empty_output() {
        let out = expand_tags_via_aliases(&[], &HashMap::new());
        assert!(out.is_empty());
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let out2 = expand_tags_via_aliases(&[], &lookup);
        assert!(out2.is_empty());
    }

    #[test]
    fn expand_tags_via_aliases_single_cluster() {
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug"), ("defect", "bug")]);
        let out = expand_tags_via_aliases(&s(&["bug"]), &lookup);
        assert_eq!(out, s(&["bug", "bugs", "defect"]));
    }

    #[test]
    fn expand_tags_via_aliases_multi_cluster() {
        let lookup = alias_map(&[
            ("bug", "bug"),
            ("bugs", "bug"),
            ("perf", "perf"),
            ("performance", "perf"),
        ]);
        let out = expand_tags_via_aliases(&s(&["bug", "perf"]), &lookup);
        assert_eq!(out, s(&["bug", "bugs", "perf", "performance"]));
    }

    #[test]
    fn expand_tags_via_aliases_unknown_tag_passthrough() {
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let out = expand_tags_via_aliases(&s(&["bug", "novel"]), &lookup);
        assert_eq!(out, s(&["bug", "bugs", "novel"]));
    }

    #[test]
    fn expand_tags_via_aliases_lowercases_caller_input() {
        // The lookup is already lowercased (per `alias_lookup_for_brain`'s
        // contract), but caller-supplied tags may be mixed-case.
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug")]);
        let out = expand_tags_via_aliases(&s(&["BUG"]), &lookup);
        assert_eq!(out, s(&["bug", "bugs"]));
    }

    #[test]
    fn expand_tags_via_aliases_canonical_not_in_raw_keys() {
        // Recluster may pick a canonical tag that itself has no raw_tag row
        // — e.g. seed inserts ("Bug", "Bugs") meaning raw "bug" maps to
        // canonical "bugs", but "bugs" is not separately a raw_tag key in
        // the lookup. The cluster must still include the canonical.
        let lookup = alias_map(&[("bug", "bugs")]);
        let out = expand_tags_via_aliases(&s(&["bug"]), &lookup);
        assert_eq!(out, s(&["bug", "bugs"]));
    }

    #[test]
    fn expand_tags_via_aliases_idempotent() {
        let lookup = alias_map(&[("bug", "bug"), ("bugs", "bug"), ("defect", "bug")]);
        let once = expand_tags_via_aliases(&s(&["bug"]), &lookup);
        let twice = expand_tags_via_aliases(&once, &lookup);
        assert_eq!(once, twice);
    }

    // ───────────────────────────────────────────────────────────────────────
    // Filter integration tests — drive `search_ranked_with_diagnostics`
    // against an in-memory `Db` with a hand-stuffed file+chunk row, a
    // `MockChunkSearcher` returning that chunk_id, and seeded `tag_aliases`.
    // No LanceDB I/O. Exercises the actual filter wiring under the alias
    // expansion built above.
    // ───────────────────────────────────────────────────────────────────────

    use crate::embedder::{Embed, MockEmbedder};
    use crate::metrics::Metrics;
    use crate::ports::mock::MockChunkSearcher;
    use brain_persistence::db::Db;
    use brain_persistence::db::tag_aliases::seed_tag_aliases;
    use brain_persistence::store::QueryResult;
    use std::sync::Arc;

    /// Seed an in-memory `Db` with a single file (carrying `tags`) plus a
    /// single chunk. Returns `(db, chunk_id)`.
    fn seed_chunk_with_tags(brain_id: &str, file_tags: &[&str]) -> (Db, String) {
        let db = Db::open_in_memory().unwrap();
        let tags_json = serde_json::to_string(file_tags).unwrap();
        db.with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO files (file_id, path, indexing_state, brain_id, tags)
                 VALUES ('f1', '/test.md', 'idle', ?1, ?2)",
                rusqlite::params![brain_id, tags_json],
            )?;
            conn.execute(
                "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
                 VALUES ('f1:0', 'f1', 0, 'h0', 'arbitrary content for the test fixture')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        (db, "f1:0".to_string())
    }

    fn mock_searcher_for(chunk_id: &str) -> MockChunkSearcher {
        MockChunkSearcher::with_results(vec![QueryResult {
            chunk_id: chunk_id.to_string(),
            file_id: "f1".to_string(),
            file_path: "/test.md".to_string(),
            chunk_ord: 0,
            content: "arbitrary content for the test fixture".to_string(),
            score: Some(0.05),
            brain_id: String::new(),
        }])
    }

    /// Run `search_ranked_with_diagnostics` against the seeded db with the
    /// given require/exclude filters, returning the chunk_ids that survive.
    async fn run_filter(
        db: &Db,
        chunk_id: &str,
        brain_id: &str,
        tags_require: &[String],
        tags_exclude: &[String],
    ) -> Vec<String> {
        let store = mock_searcher_for(chunk_id);
        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let metrics = Arc::new(Metrics::new());
        let pipeline = QueryPipeline::new(db, &store, &embedder, &metrics);
        let sp = SearchParams::new("ignored-by-mock", "lookup", 0, 0, &[])
            .with_brain_id(Some(brain_id))
            .with_tags_require(tags_require)
            .with_tags_exclude(tags_exclude);
        let (ranked, _confidence, _diag) =
            pipeline.search_ranked_with_diagnostics(&sp).await.unwrap();
        ranked.into_iter().map(|r| r.chunk_id).collect()
    }

    #[tokio::test]
    async fn empty_tag_aliases_table_yields_today_behavior() {
        // Brain has never been reclustered: alias_lookup is empty. A query
        // for ["bug"] against a chunk tagged ["bugs"] must NOT match — same
        // as today's literal-equality filter.
        let brain_id = "brain-empty";
        let (db, chunk_id) = seed_chunk_with_tags(brain_id, &["bugs"]);
        // No tag_aliases rows seeded.
        let surviving = run_filter(&db, &chunk_id, brain_id, &[String::from("bug")], &[]).await;
        assert!(
            !surviving.contains(&chunk_id),
            "candidate must be filtered out when alias table is empty (literal-only)"
        );
    }

    #[tokio::test]
    async fn seeded_alias_table_expands_filter() {
        let brain_id = "brain-a";
        let (db, chunk_id) = seed_chunk_with_tags(brain_id, &["bugs"]);
        db.with_write_conn(|conn| {
            seed_tag_aliases(
                conn,
                brain_id,
                &[("bug", "bug", "c1"), ("bugs", "bug", "c1")],
            )?;
            Ok(())
        })
        .unwrap();
        let surviving = run_filter(&db, &chunk_id, brain_id, &[String::from("bug")], &[]).await;
        assert!(
            surviving.contains(&chunk_id),
            "candidate tagged 'bugs' must be retained for tags_require=['bug'] when bug↔bugs cluster"
        );
    }

    #[tokio::test]
    async fn seeded_alias_table_expands_exclude() {
        let brain_id = "brain-a";
        let (db, chunk_id) = seed_chunk_with_tags(brain_id, &["bugs"]);
        db.with_write_conn(|conn| {
            seed_tag_aliases(
                conn,
                brain_id,
                &[("bug", "bug", "c1"), ("bugs", "bug", "c1")],
            )?;
            Ok(())
        })
        .unwrap();
        let surviving = run_filter(&db, &chunk_id, brain_id, &[], &[String::from("bug")]).await;
        assert!(
            !surviving.contains(&chunk_id),
            "candidate tagged 'bugs' must be excluded by tags_exclude=['bug'] when bug↔bugs cluster"
        );
    }

    #[tokio::test]
    async fn filter_handles_mixed_case_alias_rows() {
        // alias_lookup_for_brain lowercases keys/values at the boundary,
        // so a row inserted as ("Bug", "Bugs") behaves like ("bug", "bugs")
        // and a query for ["BUG"] retains a chunk tagged ["bugs"].
        let brain_id = "brain-a";
        let (db, chunk_id) = seed_chunk_with_tags(brain_id, &["bugs"]);
        db.with_write_conn(|conn| {
            seed_tag_aliases(conn, brain_id, &[("Bug", "Bugs", "c1")])?;
            Ok(())
        })
        .unwrap();
        let surviving = run_filter(&db, &chunk_id, brain_id, &[String::from("BUG")], &[]).await;
        assert!(
            surviving.contains(&chunk_id),
            "mixed-case alias rows must lowercase end-to-end — ['BUG'] should retain chunk tagged ['bugs']"
        );
    }

    // ───────────────────────────────────────────────────────────────────────
    // Regression suite (`brn-83a.7.2.4.5`) — pin every contract from the
    // parent plan's "Success criteria":
    //   • read-only audit invariant (record_tags / task_labels counts)
    //   • federated per-brain alias isolation (no cluster leakage)
    //   • federated `["all"]` / `["*"]` over a mix of scanned + unscanned brains
    // ───────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn audit_invariant_record_tags_unchanged() {
        // The query path is supposed to be a pure read — `record_tags` and
        // `task_labels` row counts must NOT change as a side-effect of an
        // alias-expanding query. Pin the read-only contract.
        let brain_id = "brain-a";
        let (db, chunk_id) = seed_chunk_with_tags(brain_id, &["bugs"]);
        db.with_write_conn(|conn| {
            // Register the brain so records/tasks FK targets exist.
            conn.execute(
                "INSERT INTO brains (brain_id, name, prefix, created_at)
                 VALUES (?1, 'test-brain', 'TST', strftime('%s', 'now'))",
                rusqlite::params![brain_id],
            )?;
            seed_tag_aliases(
                conn,
                brain_id,
                &[("bug", "bug", "c1"), ("bugs", "bug", "c1")],
            )?;
            // Seed a record + task with tags so the count is non-zero and we
            // would notice an accidental delete/insert.
            brain_persistence::db::tag_aliases::seed_record_with_tags(
                conn,
                "rec-1",
                brain_id,
                1_700_000_000,
                &["bug", "bugs"],
            )?;
            brain_persistence::db::tag_aliases::seed_task_with_labels(
                conn,
                "task-1",
                brain_id,
                1_700_000_000,
                &["bug"],
            )?;
            Ok(())
        })
        .unwrap();

        // Content snapshot, not just count — protects against a
        // hypothetical bug that does `delete + insert` of a different row
        // (counts would balance and slip through). Each call returns a
        // deterministic string of `key:value` rows ordered by rowid.
        let snapshot =
            |table: &'static str, key_col: &'static str, val_col: &'static str| -> String {
                db.with_read_conn(|conn| {
                    let sql = format!(
                        "SELECT COALESCE(GROUP_CONCAT({key_col} || ':' || {val_col}, '|'
                                                  ORDER BY rowid), '')
                     FROM {table}"
                    );
                    let s: String = conn.query_row(&sql, [], |r| r.get(0))?;
                    Ok(s)
                })
                .unwrap()
            };
        let before_record_tags = snapshot("record_tags", "record_id", "tag");
        let before_task_labels = snapshot("task_labels", "task_id", "label");
        assert!(
            !before_record_tags.is_empty() && !before_task_labels.is_empty(),
            "fixture must seed non-empty rows so this test is meaningful"
        );

        // Run a query that triggers alias expansion.
        let _ = run_filter(&db, &chunk_id, brain_id, &[String::from("bug")], &[]).await;

        let after_record_tags = snapshot("record_tags", "record_id", "tag");
        let after_task_labels = snapshot("task_labels", "task_id", "label");
        assert_eq!(
            before_record_tags, after_record_tags,
            "record_tags content must be byte-identical across an alias-expanding query \
             (catches delete+insert that would balance row counts)"
        );
        assert_eq!(
            before_task_labels, after_task_labels,
            "task_labels content must be byte-identical across an alias-expanding query"
        );
    }

    /// Helper: seed a brain (file + chunk) inside an existing shared `Db`.
    /// Used by the federated regression tests where multiple brains share the
    /// same SQLite handle — the `brain_id` column on `files` is the
    /// per-brain partition.
    fn seed_brain_with_chunk(
        db: &Db,
        brain_id: &str,
        file_id: &str,
        path: &str,
        chunk_id: &str,
        file_tags: &[&str],
    ) {
        let tags_json = serde_json::to_string(file_tags).unwrap();
        db.with_write_conn(|conn| {
            conn.execute(
                "INSERT INTO files (file_id, path, indexing_state, brain_id, tags)
                 VALUES (?1, ?2, 'idle', ?3, ?4)",
                rusqlite::params![file_id, path, brain_id, tags_json],
            )?;
            conn.execute(
                "INSERT INTO chunks (chunk_id, file_id, chunk_ord, chunk_hash, content)
                 VALUES (?1, ?2, 0, 'h0', 'arbitrary content for the test fixture')",
                rusqlite::params![chunk_id, file_id],
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn mock_searcher_for_chunk(chunk_id: &str, file_id: &str, path: &str) -> MockChunkSearcher {
        MockChunkSearcher::with_results(vec![QueryResult {
            chunk_id: chunk_id.to_string(),
            file_id: file_id.to_string(),
            file_path: path.to_string(),
            chunk_ord: 0,
            content: "arbitrary content for the test fixture".to_string(),
            score: Some(0.05),
            brain_id: String::new(),
        }])
    }

    #[tokio::test]
    async fn federated_per_brain_alias_isolation() {
        // Per-brain alias provenance: brain A clusters bug↔bugs, brain B
        // clusters bug↔defect. Cross-brain query for ["bug"] must return
        // both chunks (each via its own cluster), and brain A's `bugs`
        // chunk must NOT match when querying brain B alone — i.e. clusters
        // do not leak across brains even on a shared `Db`.
        let db = Db::open_in_memory().unwrap();
        seed_brain_with_chunk(&db, "brain-a", "fa", "/a.md", "fa:0", &["bugs"]);
        seed_brain_with_chunk(&db, "brain-b", "fb", "/b.md", "fb:0", &["defect"]);
        db.with_write_conn(|conn| {
            seed_tag_aliases(
                conn,
                "brain-a",
                &[("bug", "bug", "ca"), ("bugs", "bug", "ca")],
            )?;
            seed_tag_aliases(
                conn,
                "brain-b",
                &[("bug", "bug", "cb"), ("defect", "bug", "cb")],
            )?;
            Ok(())
        })
        .unwrap();

        let store_a = mock_searcher_for_chunk("fa:0", "fa", "/a.md");
        let store_b = mock_searcher_for_chunk("fb:0", "fb", "/b.md");
        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let metrics = Arc::new(Metrics::new());

        let federated = FederatedPipeline {
            db: &db,
            brains: vec![
                ("brain-a".into(), "brain-a".into(), Some(store_a)),
                ("brain-b".into(), "brain-b".into(), Some(store_b)),
            ],
            embedder: &embedder,
            metrics: &metrics,
        };

        let require = vec![String::from("bug")];
        let sp = SearchParams::new("ignored", "lookup", 0, 0, &[]).with_tags_require(&require);
        let result = federated.search_ranked_federated(&sp).await.unwrap();

        let attribution: std::collections::HashMap<&str, &str> = result
            .chunk_brain
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            attribution.get("fa:0"),
            Some(&"brain-a"),
            "brain A's `bugs`-tagged chunk must come back attributed to brain A"
        );
        assert_eq!(
            attribution.get("fb:0"),
            Some(&"brain-b"),
            "brain B's `defect`-tagged chunk must come back attributed to brain B"
        );

        // Per-brain isolation: querying brain B in isolation must NOT match
        // brain A's `bugs` chunk through brain B's `defect` cluster.
        let surviving_b_only = run_filter(&db, "fa:0", "brain-b", &require, &[]).await;
        assert!(
            !surviving_b_only.contains(&"fa:0".to_string()),
            "brain B's alias map must not bleed into brain A's chunks"
        );
    }

    #[tokio::test]
    async fn federated_all_brains_includes_unscanned() {
        // `brains: ["all"]` over a mix of scanned (A, B) and unscanned (C)
        // brains. A and B return alias-expanded matches. C — which has no
        // `tag_aliases` rows — returns only literal-match chunks; nothing
        // leaks into C from A's or B's clusters. Pins the parent plan's
        // "all-brains expansion" success criterion.
        let db = Db::open_in_memory().unwrap();
        seed_brain_with_chunk(&db, "brain-a", "fa", "/a.md", "fa:0", &["bugs"]);
        seed_brain_with_chunk(&db, "brain-b", "fb", "/b.md", "fb:0", &["defect"]);
        seed_brain_with_chunk(&db, "brain-c", "fc", "/c.md", "fc:0", &["bug"]);
        db.with_write_conn(|conn| {
            seed_tag_aliases(
                conn,
                "brain-a",
                &[("bug", "bug", "ca"), ("bugs", "bug", "ca")],
            )?;
            seed_tag_aliases(
                conn,
                "brain-b",
                &[("bug", "bug", "cb"), ("defect", "bug", "cb")],
            )?;
            // brain-c: NO alias rows — degenerate "never-reclustered" brain.
            Ok(())
        })
        .unwrap();

        let store_a = mock_searcher_for_chunk("fa:0", "fa", "/a.md");
        let store_b = mock_searcher_for_chunk("fb:0", "fb", "/b.md");
        let store_c = mock_searcher_for_chunk("fc:0", "fc", "/c.md");
        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let metrics = Arc::new(Metrics::new());

        let federated = FederatedPipeline {
            db: &db,
            brains: vec![
                ("brain-a".into(), "brain-a".into(), Some(store_a)),
                ("brain-b".into(), "brain-b".into(), Some(store_b)),
                ("brain-c".into(), "brain-c".into(), Some(store_c)),
            ],
            embedder: &embedder,
            metrics: &metrics,
        };

        let require = vec![String::from("bug")];
        let sp = SearchParams::new("ignored", "lookup", 0, 0, &[]).with_tags_require(&require);
        let result = federated.search_ranked_federated(&sp).await.unwrap();

        let chunks: std::collections::HashSet<&str> =
            result.ranked.iter().map(|r| r.chunk_id.as_str()).collect();
        assert!(
            chunks.contains("fa:0"),
            "brain A's `bugs` chunk must be returned via alias expansion"
        );
        assert!(
            chunks.contains("fb:0"),
            "brain B's `defect` chunk must be returned via alias expansion"
        );
        assert!(
            chunks.contains("fc:0"),
            "brain C's literal `bug` chunk must be returned (literal match — no recluster needed)"
        );

        // brain-c's chunk must be attributed to brain-c (proves the literal
        // path went through the brain-c branch, not blended in from another).
        assert_eq!(
            result.chunk_brain.get("fc:0").map(|s| s.as_str()),
            Some("brain-c"),
            "brain C's chunk must be attributed to brain C, not blended from another brain"
        );
    }

    // ───────────────────────────────────────────────────────────────────────
    // Edge cases (`brn-83a.7.2.4.7`):
    //   • exclude-side cross-brain isolation (mirror of the require-side test)
    //   • `Some("")` brain_id collapses to no-alias semantics, matching the
    //     contract documented on `SearchParams.brain_id`
    // ───────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn federated_exclude_per_brain_isolation() {
        // Mirror of `federated_per_brain_alias_isolation` with `tags_exclude`.
        // Each brain must use its OWN cluster for exclude expansion — A's
        // `bug↔bugs` mapping must drop A's `bugs` chunk; B's `bug↔defect`
        // mapping must drop B's `defect` chunk; nothing leaks across.
        let db = Db::open_in_memory().unwrap();
        seed_brain_with_chunk(&db, "brain-a", "fa", "/a.md", "fa:0", &["bugs"]);
        seed_brain_with_chunk(&db, "brain-b", "fb", "/b.md", "fb:0", &["defect"]);
        db.with_write_conn(|conn| {
            seed_tag_aliases(
                conn,
                "brain-a",
                &[("bug", "bug", "ca"), ("bugs", "bug", "ca")],
            )?;
            seed_tag_aliases(
                conn,
                "brain-b",
                &[("bug", "bug", "cb"), ("defect", "bug", "cb")],
            )?;
            Ok(())
        })
        .unwrap();

        let store_a = mock_searcher_for_chunk("fa:0", "fa", "/a.md");
        let store_b = mock_searcher_for_chunk("fb:0", "fb", "/b.md");
        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let metrics = Arc::new(Metrics::new());

        let federated = FederatedPipeline {
            db: &db,
            brains: vec![
                ("brain-a".into(), "brain-a".into(), Some(store_a)),
                ("brain-b".into(), "brain-b".into(), Some(store_b)),
            ],
            embedder: &embedder,
            metrics: &metrics,
        };

        let exclude = vec![String::from("bug")];
        let sp = SearchParams::new("ignored", "lookup", 0, 0, &[]).with_tags_exclude(&exclude);
        let result = federated.search_ranked_federated(&sp).await.unwrap();

        let chunks: std::collections::HashSet<&str> =
            result.ranked.iter().map(|r| r.chunk_id.as_str()).collect();
        assert!(
            !chunks.contains("fa:0"),
            "brain A's `bugs` chunk must be excluded via brain A's bug↔bugs cluster"
        );
        assert!(
            !chunks.contains("fb:0"),
            "brain B's `defect` chunk must be excluded via brain B's bug↔defect cluster"
        );

        // Per-brain isolation: querying brain B alone with exclude=["bug"]
        // against brain A's `bugs`-tagged chunk must NOT drop it through
        // brain B's `defect` cluster — brain B's exclude expansion is
        // {bug, defect}, not {bug, bugs}. The chunk should pass through.
        let surviving_b_only = run_filter(&db, "fa:0", "brain-b", &[], &exclude).await;
        assert!(
            surviving_b_only.contains(&"fa:0".to_string()),
            "brain B's exclude expansion must NOT drop a chunk tagged `bugs` — \
             that mapping lives in brain A's cluster, not brain B's"
        );
    }

    #[tokio::test]
    async fn empty_brain_id_collapses_to_no_alias_expansion() {
        // The contract on `SearchParams.brain_id` says `Some("")` MUST be
        // treated as `None` — the empty string is reserved for "caller
        // could not resolve a brain", not "all brains". Pin this so a
        // future refactor that drops the `is_empty()` guard at
        // `query_pipeline.rs:371` cannot silently start passing the empty
        // string through to `alias_lookup_for_brain`.
        let brain_id = "brain-a";
        let (db, chunk_id) = seed_chunk_with_tags(brain_id, &["bugs"]);
        // Seed an alias map for the real brain. If the empty-string path
        // accidentally activated alias expansion, the chunk tagged `bugs`
        // would be retained for `tags_require=["bug"]` — exactly what the
        // assertion below rules out.
        db.with_write_conn(|conn| {
            seed_tag_aliases(
                conn,
                brain_id,
                &[("bug", "bug", "c1"), ("bugs", "bug", "c1")],
            )?;
            Ok(())
        })
        .unwrap();

        // brain_id="" — Some("") should behave like None: no alias path.
        let surviving_empty = run_filter(&db, &chunk_id, "", &[String::from("bug")], &[]).await;
        assert!(
            !surviving_empty.contains(&chunk_id),
            "Some(\"\") brain_id must NOT activate alias expansion — chunk tagged \
             `bugs` should be filtered out for tags_require=[\"bug\"]"
        );

        // Sanity: the SAME setup with brain_id="brain-a" DOES retain the
        // chunk via alias expansion. Proves the test fixture is meaningful
        // (i.e. the Some("") result above is from the empty-string path,
        // not from a degenerate setup).
        let surviving_real =
            run_filter(&db, &chunk_id, brain_id, &[String::from("bug")], &[]).await;
        assert!(
            surviving_real.contains(&chunk_id),
            "fixture sanity: with brain_id=\"brain-a\" the chunk must be retained \
             via alias expansion — otherwise the previous assertion is vacuous"
        );
    }

    // ───────────────────────────────────────────────────────────────────────
    // Regression: brain_id passthrough to vector search
    //
    // Verifies that `QueryPipeline` forwards `SearchParams::brain_id` to the
    // underlying `ChunkSearcher::query` call rather than hardcoding `None`.
    // Without the fix, the spy would record `None` regardless of the param.
    // ───────────────────────────────────────────────────────────────────────

    use crate::ports::ChunkSearcher;
    use brain_persistence::store::VectorSearchMode;
    use std::sync::Mutex;

    /// A `ChunkSearcher` spy that records the `brain_id` argument of every
    /// `query` call. Returns an empty result set so the pipeline runs to
    /// completion without requiring real LanceDB data.
    struct SpyChunkSearcher {
        /// The `brain_id` passed to the most recent `query` call.
        /// `None` means `query` has not been called yet.
        last_brain_id: Mutex<Option<Option<String>>>,
    }

    impl SpyChunkSearcher {
        fn new() -> Self {
            Self {
                last_brain_id: Mutex::new(None),
            }
        }

        /// Return the `brain_id` seen on the last `query` call, or `None`
        /// if `query` was never called.
        fn recorded_brain_id(&self) -> Option<Option<String>> {
            self.last_brain_id.lock().unwrap().clone()
        }
    }

    impl ChunkSearcher for SpyChunkSearcher {
        fn query<'a>(
            &'a self,
            _embedding: &'a [f32],
            _top_k: usize,
            _nprobes: usize,
            _mode: VectorSearchMode,
            brain_id: Option<&'a str>,
        ) -> impl std::future::Future<Output = crate::error::Result<Vec<brain_persistence::store::QueryResult>>> + Send + 'a
        {
            let captured = brain_id.map(str::to_owned);
            async move {
                *self.last_brain_id.lock().unwrap() = Some(captured);
                Ok(vec![])
            }
        }
    }

    #[tokio::test]
    async fn vector_search_receives_brain_id_from_params() {
        // Regression test: confirm `params.brain_id` reaches the store.
        // Seeds a minimal in-memory Db so `search_ranked_with_diagnostics`
        // can run to completion, then asserts the spy recorded the expected
        // brain_id value.
        let (db, _chunk_id) = seed_chunk_with_tags("brain-a", &[]);
        let spy = SpyChunkSearcher::new();
        let embedder: Arc<dyn Embed> = Arc::new(MockEmbedder);
        let metrics = Arc::new(Metrics::new());
        let pipeline = QueryPipeline::new(&db, &spy, &embedder, &metrics);

        // Query scoped to "brain-a".
        let sp = SearchParams::new("test query", "lookup", 0, 0, &[])
            .with_brain_id(Some("brain-a"));
        let _ = pipeline.search_ranked_with_diagnostics(&sp).await;

        assert_eq!(
            spy.recorded_brain_id(),
            Some(Some("brain-a".to_owned())),
            "store.query must receive Some(\"brain-a\") — not None — when \
             params.brain_id is Some(\"brain-a\")"
        );

        // Also verify that passing no brain_id forwards None to the store.
        let spy_none = SpyChunkSearcher::new();
        let pipeline_none = QueryPipeline::new(&db, &spy_none, &embedder, &metrics);
        let sp_none = SearchParams::new("test query", "lookup", 0, 0, &[]);
        let _ = pipeline_none.search_ranked_with_diagnostics(&sp_none).await;

        assert_eq!(
            spy_none.recorded_brain_id(),
            Some(None),
            "store.query must receive None when params.brain_id is None"
        );
    }

    #[test]
    fn with_brain_id_normalizes_empty_string_to_none() {
        let tags: Vec<String> = vec![];
        let params = SearchParams::new("q", "intent", 100, 10, &tags)
            .with_brain_id(Some(""));
        assert_eq!(
            params.brain_id,
            None,
            "with_brain_id(Some(\"\")) must normalize to None"
        );
    }
}
