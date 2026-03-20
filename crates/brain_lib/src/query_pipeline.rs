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
use crate::db::Db;
use crate::db::summaries::SummaryRow;
use crate::embedder::Embed;
use crate::error::{BrainCoreError, Result};
use crate::metrics::Metrics;
use crate::ports::{ChunkMetaReader, ChunkSearcher, EpisodeReader, FtsSearcher, GraphLinkReader};
use crate::ranking::{
    CandidateSignals, FusionConfidence, RerankCandidate, Reranker, RerankerPolicy, Weights,
    compute_fusion_confidence, rank_candidates, resolve_intent,
};
use crate::retrieval::{ExpandResult, ExpandableChunk, SearchResult, expand_results, pack_minimal};
use crate::store::VectorSearchMode;
use crate::store::{DEFAULT_NPROBES, StoreReader};
use crate::tokens::estimate_tokens;

const CANDIDATE_LIMIT: usize = 50;

/// Result of a reflect call.
#[derive(Debug)]
pub struct ReflectResult {
    pub topic: String,
    pub budget_tokens: usize,
    pub episodes: Vec<SummaryRow>,
    pub search_result: SearchResult,
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
    /// Defaults to `false`. Feature is not yet implemented — this field exists
    /// to allow TDD test compilation before the expansion logic is wired in.
    pub graph_expand: bool,
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
        }
    }

    /// Override the vector search mode.
    pub fn with_mode(mut self, mode: VectorSearchMode) -> Self {
        self.mode = mode;
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
        let (ranked, confidence) = self
            .search_ranked(params.query, params.intent, params.query_tags, params.mode, params.graph_expand)
            .await?;
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
        let (ranked, confidence) = self
            .search_ranked(params.query, params.intent, params.query_tags, params.mode, params.graph_expand)
            .await?;
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

    /// Core search logic: returns ranked results with fusion confidence.
    pub(crate) async fn search_ranked(
        &self,
        query: &str,
        intent: &str,
        query_tags: &[String],
        mode: VectorSearchMode,
        graph_expand: bool,
    ) -> Result<(Vec<crate::ranking::RankedResult>, FusionConfidence)> {
        let profile = resolve_intent(intent);
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
            .query(&query_vec, CANDIDATE_LIMIT, DEFAULT_NPROBES, mode)
            .await?;

        // 3. FTS search (top-50, gracefully degrade on failure)
        let fts_results = match self.db.search_fts(query, CANDIDATE_LIMIT) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "FTS search failed, continuing with vector-only");
                self.metrics.query_errors.fetch_add(1, Ordering::Relaxed);
                Vec::new()
            }
        };

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

        if candidates.is_empty() {
            return Ok((vec![], fusion_confidence));
        }

        // 5. Enrich from SQLite (single batched JOIN — pagerank_score comes from files table)
        let chunk_ids: Vec<String> = candidates.keys().cloned().collect();
        let enrichment = self.db.get_chunks_by_ids(&chunk_ids);

        if let Ok(rows) = enrichment {
            let now = crate::utils::now_ts();

            for row in &rows {
                if let Some(candidate) = candidates.get_mut(&row.chunk_id) {
                    candidate.file_path = row.file_path.clone();
                    candidate.heading_path = row.heading_path.clone();
                    candidate.content = row.content.clone();
                    candidate.token_estimate = row.token_estimate;
                    candidate.byte_start = row.byte_start;
                    candidate.byte_end = row.byte_end;
                    candidate.pagerank_score = row.pagerank_score;

                    if let Some(indexed_at) = row.last_indexed_at {
                        candidate.age_seconds = (now - indexed_at).max(0) as f64;
                    }
                }
            }
        }

        // Remove FTS-only candidates that weren't found in SQLite
        let mut candidate_vec: Vec<CandidateSignals> = candidates
            .into_values()
            .filter(|c| !c.content.is_empty())
            .collect();

        // 5b. 1-hop graph expansion: follow outgoing links from top-K candidates
        //     and inject (or boost) linked chunks in the candidate pool before ranking.
        if graph_expand && !candidate_vec.is_empty() {
            // Sort by composite signal to pick top-10 seed candidates
            let mut seeds = candidate_vec.clone();
            seeds.sort_by(|a, b| {
                let sa = a.sim_vector + a.bm25 + a.pagerank_score;
                let sb = b.sim_vector + b.bm25 + b.pagerank_score;
                sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
            });

            // For each seed, get 1-hop outlink file_ids, carrying the parent's vector score.
            // Expansion candidates receive sim_vector = parent_score * 0.5 (graph penalty).
            let mut expansion_entries: Vec<(String, f64)> = Vec::new(); // (target_file_id, parent_sim)
            for seed in seeds.iter().take(10) {
                // Derive file_id from chunk_id (format: "file_id:chunk_ord")
                let file_id = seed.chunk_id.rsplit_once(':')
                    .map(|(prefix, _)| prefix.to_string())
                    .unwrap_or_else(|| seed.chunk_id.clone());

                // Use the composite signal (vector + keyword) as the parent score
                // so that FTS-strong seeds also produce meaningful expansion boosts.
                let parent_sim = (seed.sim_vector + seed.bm25).min(1.0);
                if let Ok(outlinks) = self.db.get_outlinks(&file_id) {
                    for target_file_id in outlinks {
                        if !expansion_entries.iter().any(|(fid, _)| fid == &target_file_id) {
                            expansion_entries.push((target_file_id, parent_sim));
                        }
                    }
                }
            }

            // Cap at 20 expansion file_ids
            expansion_entries.truncate(20);

            if !expansion_entries.is_empty() {
                let expansion_file_ids: Vec<String> =
                    expansion_entries.iter().map(|(fid, _)| fid.clone()).collect();
                let parent_sim_map: std::collections::HashMap<String, f64> =
                    expansion_entries.into_iter().collect();

                if let Ok(expansion_chunks) = self.db.get_chunks_by_file_ids(&expansion_file_ids) {
                    let now = crate::utils::now_ts();
                    for chunk in expansion_chunks {
                        let graph_sim = parent_sim_map
                            .get(&chunk.file_id)
                            .copied()
                            .unwrap_or(0.0) * 0.5;

                        // If this chunk is already in the candidate pool, boost its sim_vector
                        // if the graph signal is higher than its current vector score.
                        if let Some(existing) = candidate_vec
                            .iter_mut()
                            .find(|c| c.chunk_id == chunk.chunk_id)
                        {
                            if graph_sim > existing.sim_vector {
                                existing.sim_vector = graph_sim;
                            }
                            continue;
                        }

                        // Not in pool yet — add as a new expansion candidate.
                        let age_seconds = if let Some(indexed_at) = chunk.last_indexed_at {
                            (now - indexed_at).max(0) as f64
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
            }
        }

        // 6. Rank
        let mut ranked = rank_candidates(&candidate_vec, &weights, query_tags);

        // 7. Adaptive reranking: if confidence is low and a reranker is attached,
        //    rerank the top-N fused candidates using the cross-encoder.
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

        Ok((ranked, fusion_confidence))
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
    /// # Finding 7 — brain_id post-filter
    ///
    /// Once `brain_persistence` adds a `brain_id` column to the `summaries`
    /// table and updates `SummaryRow` accordingly, add a post-filter here:
    ///
    /// ```ignore
    /// let episodes: Vec<SummaryRow> = episodes
    ///     .into_iter()
    ///     .filter(|ep| ep.brain_id == target_brain_id)
    ///     .collect();
    /// ```
    ///
    /// For regular vector-search candidates enriched from the `chunks` table,
    /// a similar `brain_id` filter belongs in `search_ranked` after the SQLite
    /// enrichment step, following the same pattern as the vector-only filter.
    /// That change also requires `brain_id` on `ChunkRow` from `brain_persistence`.
    #[instrument(skip_all)]
    pub async fn reflect_with_episodes(
        &self,
        topic: String,
        budget_tokens: usize,
        episodes: Vec<SummaryRow>,
    ) -> Result<ReflectResult> {
        let empty_tags: Vec<String> = Vec::new();
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
    ) -> Result<crate::retrieval::SearchResult> {
        // ── 1. Build per-brain vector-search futures ──────────────────────────
        type BrainResult = (String, Vec<crate::ranking::RankedResult>);

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
            futs.push(Box::pin(async move {
                match pipeline
                    .search_ranked(&query, &intent, &query_tags, mode, false)
                    .await
                {
                    Ok((ranked, _confidence)) => (brain_name, ranked),
                    Err(e) => {
                        warn!(brain = %brain_name, error = %e, "brain search failed");
                        (brain_name, vec![])
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

        for (brain_name, ranked) in all_results {
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

        // ── 5. Pack into SearchResult ─────────────────────────────────────────
        // ML summary preloading skipped for federated search — results span
        // multiple brain namespaces.
        let mut search_result = crate::retrieval::pack_minimal(
            &merged,
            params.budget_tokens,
            params.k,
            false,
            &HashMap::new(),
        );

        // ── 6. Annotate each stub with its source brain name ──────────────────
        for stub in &mut search_result.results {
            stub.brain_name = chunk_brain.get(&stub.memory_id).cloned();
        }

        Ok(search_result)
    }
}
