use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};
use tracing::{error, warn};

use crate::db::chunks::get_chunks_by_ids;
use crate::db::fts::search_fts;
use crate::db::links::count_backlinks;
use crate::mcp::McpContext;
use crate::mcp::protocol::ToolCallResult;
use crate::ranking::{CandidateSignals, Weights, rank_candidates, resolve_intent};
use crate::retrieval::pack_minimal;
use crate::tokens::estimate_tokens;

const CANDIDATE_LIMIT: usize = 50;

pub(super) async fn handle(params: &Value, ctx: &McpContext) -> ToolCallResult {
    let query = match params.get("query").and_then(|v| v.as_str()) {
        Some(q) => q,
        None => return ToolCallResult::error("Missing required parameter: query"),
    };

    let intent = params
        .get("intent")
        .and_then(|v| v.as_str())
        .unwrap_or("auto");
    let budget_tokens = params
        .get("budget_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(800) as usize;
    let k = params.get("k").and_then(|v| v.as_u64()).unwrap_or(10) as usize;

    let profile = resolve_intent(intent);
    let weights = Weights::from_profile(profile);

    // 1. Embed query
    let embedder = Arc::clone(&ctx.embedder);
    let query_owned = query.to_string();
    let query_vec =
        match tokio::task::spawn_blocking(move || embedder.embed_batch(&[&query_owned])).await {
            Ok(Ok(vecs)) if !vecs.is_empty() => vecs.into_iter().next().unwrap(),
            Ok(Err(e)) => {
                error!(error = %e, "embedding failed");
                return ToolCallResult::error(format!("Embedding failed: {e}"));
            }
            Err(e) => {
                error!(error = %e, "embedding task failed");
                return ToolCallResult::error(format!("Embedding task failed: {e}"));
            }
            _ => return ToolCallResult::error("Empty embedding result"),
        };

    // 2. Vector search (top-50)
    let vector_results = match ctx.store.query(&query_vec, CANDIDATE_LIMIT).await {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "vector search failed");
            return ToolCallResult::error(format!("Vector search failed: {e}"));
        }
    };

    // 3. FTS search (top-50)
    let fts_results = match ctx
        .db
        .with_conn(|conn| search_fts(conn, query, CANDIDATE_LIMIT))
    {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "FTS search failed, continuing with vector-only");
            Vec::new()
        }
    };

    // 4. Union + deduplicate by chunk_id
    let mut candidates: HashMap<String, CandidateSignals> = HashMap::new();

    // Add vector results
    for vr in &vector_results {
        // Dot-product distance: lower = more similar. Convert: sim = 1.0 - distance
        let sim = 1.0 - vr.score.unwrap_or(1.0) as f64;
        candidates.insert(
            vr.chunk_id.clone(),
            CandidateSignals {
                chunk_id: vr.chunk_id.clone(),
                sim_vector: sim.clamp(0.0, 1.0),
                bm25: 0.0,
                age_seconds: 0.0, // enriched below
                backlink_count: 0,
                max_backlinks: 0,
                tags: vec![],
                importance: 1.0,
                file_path: vr.file_path.clone(),
                heading_path: String::new(), // enriched below
                content: vr.content.clone(),
                token_estimate: estimate_tokens(&vr.content),
            },
        );
    }

    // Merge FTS results
    for fr in &fts_results {
        if let Some(existing) = candidates.get_mut(&fr.chunk_id) {
            existing.bm25 = fr.score;
        } else {
            // FTS-only candidate — need to look up content from SQLite
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
                    file_path: String::new(), // enriched below
                    heading_path: String::new(),
                    content: String::new(),
                    token_estimate: 0,
                },
            );
        }
    }

    if candidates.is_empty() {
        let response = json!({
            "budget_tokens": budget_tokens,
            "used_tokens_est": 0,
            "intent_resolved": format!("{profile:?}"),
            "result_count": 0,
            "total_available": 0,
            "results": []
        });
        return ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default());
    }

    // 5. Enrich candidates with SQLite metadata
    let chunk_ids: Vec<String> = candidates.keys().cloned().collect();
    let enrichment = ctx.db.with_conn(|conn| {
        let rows = get_chunks_by_ids(conn, &chunk_ids)?;

        // Get backlink counts for all unique file_ids
        let file_ids: Vec<String> = rows.iter().map(|r| r.file_id.clone()).collect();
        let mut backlinks: HashMap<String, usize> = HashMap::new();
        for fid in &file_ids {
            if !backlinks.contains_key(fid) {
                // Get file path for backlink lookup
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let max_bl = backlinks.values().copied().max().unwrap_or(0);

        for row in &rows {
            if let Some(candidate) = candidates.get_mut(&row.chunk_id) {
                candidate.file_path = row.file_path.clone();
                candidate.heading_path = row.heading_path.clone();
                candidate.content = row.content.clone();
                candidate.token_estimate = row.token_estimate;
                candidate.backlink_count = *backlinks.get(&row.file_id).unwrap_or(&0);
                candidate.max_backlinks = max_bl;

                if let Some(indexed_at) = row.last_indexed_at {
                    candidate.age_seconds = (now - indexed_at).max(0) as f64;
                }
            }
        }
    }

    // Remove candidates with no content (FTS-only candidates that weren't found in SQLite)
    let candidate_vec: Vec<CandidateSignals> = candidates
        .into_values()
        .filter(|c| !c.content.is_empty())
        .collect();

    // 6. Rank
    let ranked = rank_candidates(&candidate_vec, &weights, &[]);

    // 7. Pack within budget
    let search_result = pack_minimal(&ranked, budget_tokens, k);

    // 8. Serialize
    let results_json: Vec<Value> = search_result
        .results
        .iter()
        .map(|stub| {
            json!({
                "memory_id": stub.memory_id,
                "title": stub.title,
                "summary": stub.summary_2sent,
                "score": stub.hybrid_score,
                "file_path": stub.file_path,
                "heading_path": stub.heading_path,
            })
        })
        .collect();

    let response = json!({
        "budget_tokens": search_result.budget_tokens,
        "used_tokens_est": search_result.used_tokens_est,
        "intent_resolved": format!("{profile:?}"),
        "result_count": search_result.num_results,
        "total_available": search_result.total_available,
        "results": results_json
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::dispatch_tool_call;
    use super::super::tests::create_test_context;

    #[test]
    fn test_missing_query() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (_dir, ctx) = rt.block_on(async { create_test_context().await });
        let result = rt.block_on(dispatch_tool_call(
            "memory.search_minimal",
            &json!({}),
            &ctx,
        ));
        assert_eq!(result.is_error, Some(true));
    }
}
