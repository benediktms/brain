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
use crate::ports::{ChunkMetaReader, ChunkSearcher, EpisodeReader, FtsSearcher, GraphLinkReader};
use crate::ranking::{
    CandidateSignals, FusionConfidence, RerankCandidate, Reranker, RerankerPolicy, Weights,
    compute_fusion_confidence, rank_candidates, resolve_intent,
};
use crate::retrieval::{
    ExpandResult, ExpandableChunk, MemoryKind, SearchResult, derive_kind, expand_results,
    pack_minimal,
};
use crate::tokens::estimate_tokens;
use brain_persistence::db::Db;
use brain_persistence::db::summaries::SummaryRow;
use brain_persistence::store::VectorSearchMode;
use brain_persistence::store::{DEFAULT_NPROBES, StoreReader};

const CANDIDATE_LIMIT: usize = 50;

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
    D: ChunkMetaReader + FtsSearcher + EpisodeReader + GraphLinkReader + Send + Sync,
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
    D: ChunkMetaReader + FtsSearcher + EpisodeReader + GraphLinkReader + Send + Sync,
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
            .query(&query_vec, CANDIDATE_LIMIT, DEFAULT_NPROBES, mode, None)
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
                // Tag filters — compute lowercase tags once for both checks
                if !params.tags_require.is_empty() || !params.tags_exclude.is_empty() {
                    let lower_tags: Vec<String> = c.tags.iter().map(|t| t.to_lowercase()).collect();
                    // Require (AND): all must be present
                    if !params.tags_require.iter().all(|req| {
                        let req_lower = req.to_lowercase();
                        lower_tags.iter().any(|t| t == &req_lower)
                    }) {
                        return false;
                    }
                    // Exclude (NOR): none may be present
                    if params.tags_exclude.iter().any(|exc| {
                        let exc_lower = exc.to_lowercase();
                        lower_tags.iter().any(|t| t == &exc_lower)
                    }) {
                        return false;
                    }
                }
                true
            });
        }

        // 5d. 1-hop graph expansion: follow outgoing links from top-K candidates
        //     and inject (or boost) linked chunks in the candidate pool before ranking.
        if graph_expand && !candidate_vec.is_empty() {
            self.expand_graph_links(&mut candidate_vec, 10, 20);
        }

        // 6. Rank
        let mut ranked = rank_candidates(&candidate_vec, &weights, query_tags);

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
    fn expand_graph_links(
        &self,
        candidate_vec: &mut Vec<CandidateSignals>,
        seed_count: usize,
        max_expansion: usize,
    ) {
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
            return;
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
                }
            }
            Err(e) => {
                warn!(error = %e, "graph expansion: get_chunks_by_file_ids failed");
            }
        }
    }

    /// Expand: look up chunks by IDs, preserve order, return full content within budget.
    #[instrument(skip_all)]
    pub async fn expand(
        &self,
        memory_ids: &[String],
        budget_tokens: usize,
    ) -> Result<ExpandResult> {
        let rows = self.db.get_chunks_by_ids(memory_ids)?;

        // Preserve the requested order
        let row_map: HashMap<&str, _> = rows.iter().map(|r| (r.chunk_id.as_str(), r)).collect();
        let chunks: Vec<ExpandableChunk> = memory_ids
            .iter()
            .filter_map(|id| row_map.get(id.as_str()).copied())
            .map(|row| ExpandableChunk {
                chunk_id: row.chunk_id.clone(),
                content: row.content.clone(),
                file_path: row.file_path.clone(),
                heading_path: row.heading_path.clone(),
                token_estimate: row.token_estimate,
                byte_start: row.byte_start,
                byte_end: row.byte_end,
            })
            .collect();

        Ok(expand_results(&chunks, budget_tokens))
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
///         ("local".into(), Some(local_store)),
///         ("remote".into(), remote_ctx.store),
///     ],
///     embedder: &embedder,
///     metrics: &metrics,
/// };
/// ```
pub struct FederatedPipeline<'a, S = StoreReader, D = Db>
where
    S: ChunkSearcher + Send + Sync,
    D: ChunkMetaReader + FtsSearcher + EpisodeReader + GraphLinkReader + Send + Sync,
{
    /// Shared unified SQLite database — abstracted via port traits.
    pub db: &'a D,
    /// Per-brain entries: `(brain_name, store)`.
    ///
    /// `store` is `None` when the brain's LanceDB has not yet been
    /// initialised — that brain is skipped for vector search with a warning.
    pub brains: Vec<(String, Option<S>)>,
    /// Shared embedder — query is embedded once, used across all brains.
    pub embedder: &'a Arc<dyn crate::embedder::Embed>,
    /// Shared metrics handle.
    pub metrics: &'a Arc<crate::metrics::Metrics>,
}

impl<'a, S, D> FederatedPipeline<'a, S, D>
where
    S: ChunkSearcher + Send + Sync,
    D: ChunkMetaReader + FtsSearcher + EpisodeReader + GraphLinkReader + Send + Sync,
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

        for (brain_name, store_opt) in &self.brains {
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
                let dominated = best_by_chunk
                    .get(&result.chunk_id)
                    .is_some_and(|existing| existing.hybrid_score >= result.hybrid_score);
                if !dominated {
                    chunk_brain.insert(result.chunk_id.clone(), brain_name.clone());
                    best_by_chunk.insert(result.chunk_id.clone(), result);
                }
            }
        }

        let mut merged: Vec<crate::ranking::RankedResult> = best_by_chunk.into_values().collect();

        // ── 4. Sort by hybrid_score descending ────────────────────────────────
        merged.sort_by(|a, b| {
            b.hybrid_score
                .partial_cmp(&a.hybrid_score)
                .unwrap_or(std::cmp::Ordering::Equal)
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
