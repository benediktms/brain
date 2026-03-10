/// Read-path orchestration pipeline.
///
/// Centralises hybrid search, expand, and reflect logic so that MCP handlers
/// and the CLI query command share a single implementation.
use std::collections::HashMap;
use std::sync::Arc;

use tracing::{instrument, warn};

use std::sync::atomic::Ordering;

use crate::capsule::generate_stub_capsule;
use crate::db::Db;
use crate::db::chunks::get_chunks_by_ids;
use crate::db::fts::search_fts;
use crate::db::links::count_backlinks;
use crate::db::summaries::{SummaryRow, list_episodes};
use crate::embedder::Embed;
use crate::error::{BrainCoreError, Result};
use crate::metrics::Metrics;
use crate::ranking::{
    CandidateSignals, FusionConfidence, RerankCandidate, Reranker, RerankerPolicy, Weights,
    compute_fusion_confidence, rank_candidates, resolve_intent,
};
use crate::retrieval::{ExpandResult, ExpandableChunk, SearchResult, expand_results, pack_minimal};
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

/// Orchestrates the read-path: hybrid search, expand, and reflect.
pub struct QueryPipeline<'a> {
    db: &'a Db,
    store: &'a StoreReader,
    embedder: &'a Arc<dyn Embed>,
    metrics: &'a Arc<Metrics>,
    reranker: Option<&'a dyn Reranker>,
    reranker_policy: RerankerPolicy,
}

impl<'a> QueryPipeline<'a> {
    pub fn new(
        db: &'a Db,
        store: &'a StoreReader,
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
    pub fn with_reranker(mut self, reranker: &'a dyn Reranker, policy: RerankerPolicy) -> Self {
        self.reranker = Some(reranker);
        self.reranker_policy = policy;
        self
    }

    /// Hybrid search: vector + FTS union, enriched, ranked, packed within budget.
    #[instrument(skip_all)]
    pub async fn search(
        &self,
        query: &str,
        intent: &str,
        budget_tokens: usize,
        k: usize,
        query_tags: &[String],
    ) -> Result<SearchResult> {
        let (ranked, confidence) = self.search_ranked(query, intent, query_tags).await?;
        let mut result = pack_minimal(&ranked, budget_tokens, k, false);
        result.fusion_confidence = Some(confidence);
        Ok(result)
    }

    /// Hybrid search returning stubs with per-signal score breakdowns.
    #[instrument(skip_all)]
    pub async fn search_with_scores(
        &self,
        query: &str,
        intent: &str,
        budget_tokens: usize,
        k: usize,
        query_tags: &[String],
    ) -> Result<SearchResult> {
        let (ranked, confidence) = self.search_ranked(query, intent, query_tags).await?;
        let mut result = pack_minimal(&ranked, budget_tokens, k, true);
        result.fusion_confidence = Some(confidence);
        Ok(result)
    }

    /// Core search logic: returns ranked results with fusion confidence.
    async fn search_ranked(
        &self,
        query: &str,
        intent: &str,
        query_tags: &[String],
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
            .query(&query_vec, CANDIDATE_LIMIT, DEFAULT_NPROBES)
            .await?;

        // 3. FTS search (top-50, gracefully degrade on failure)
        let fts_results = match self
            .db
            .with_read_conn(|conn| search_fts(conn, query, CANDIDATE_LIMIT))
        {
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
                    backlink_count: 0,
                    max_backlinks: 0,
                    tags: vec![],
                    importance: 1.0,
                    file_path: vr.file_path.clone(),
                    heading_path: String::new(),
                    content: vr.content.clone(),
                    token_estimate: estimate_tokens(&vr.content),
                    byte_start: 0,
                    byte_end: 0,
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
                        backlink_count: 0,
                        max_backlinks: 0,
                        tags: vec![],
                        importance: 1.0,
                        file_path: String::new(),
                        heading_path: String::new(),
                        content: String::new(),
                        token_estimate: 0,
                        byte_start: 0,
                        byte_end: 0,
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

        // 5. Enrich from SQLite
        let chunk_ids: Vec<String> = candidates.keys().cloned().collect();
        let enrichment = self.db.with_read_conn(|conn| {
            let rows = get_chunks_by_ids(conn, &chunk_ids)?;

            let file_ids: Vec<String> = rows.iter().map(|r| r.file_id.clone()).collect();
            let mut backlinks: HashMap<String, usize> = HashMap::new();
            for fid in &file_ids {
                if !backlinks.contains_key(fid) {
                    let path: Option<String> = conn
                        .query_row("SELECT path FROM files WHERE file_id = ?1", [fid], |row| {
                            row.get(0)
                        })
                        .ok();
                    if let Some(path) = path {
                        let count = count_backlinks(conn, &path).unwrap_or(0);
                        backlinks.insert(fid.clone(), count);
                    }
                }
            }

            Ok((rows, backlinks))
        });

        if let Ok((rows, backlinks)) = enrichment {
            let now = crate::utils::now_ts();
            let max_bl = backlinks.values().copied().max().unwrap_or(0);

            for row in &rows {
                if let Some(candidate) = candidates.get_mut(&row.chunk_id) {
                    candidate.file_path = row.file_path.clone();
                    candidate.heading_path = row.heading_path.clone();
                    candidate.content = row.content.clone();
                    candidate.token_estimate = row.token_estimate;
                    candidate.byte_start = row.byte_start;
                    candidate.byte_end = row.byte_end;
                    candidate.backlink_count = *backlinks.get(&row.file_id).unwrap_or(&0);
                    candidate.max_backlinks = max_bl;

                    if let Some(indexed_at) = row.last_indexed_at {
                        candidate.age_seconds = (now - indexed_at).max(0) as f64;
                    }
                }
            }
        }

        // Remove FTS-only candidates that weren't found in SQLite
        let candidate_vec: Vec<CandidateSignals> = candidates
            .into_values()
            .filter(|c| !c.content.is_empty())
            .collect();

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
        let rows = self
            .db
            .with_read_conn(|conn| get_chunks_by_ids(conn, memory_ids))?;

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
    pub async fn reflect(&self, topic: String, budget_tokens: usize) -> Result<ReflectResult> {
        let episodes = self
            .db
            .with_read_conn(|conn| list_episodes(conn, 10))
            .unwrap_or_default();

        let search_result = self
            .search(&topic, "reflection", budget_tokens / 2, 5, &[])
            .await?;

        Ok(ReflectResult {
            topic,
            budget_tokens,
            episodes,
            search_result,
        })
    }
}
