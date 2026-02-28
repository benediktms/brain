/// MCP tool definitions and handlers.
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};
use tracing::{error, warn};

use crate::db::chunks::get_chunks_by_ids;
use crate::db::fts::search_fts;
use crate::db::links::count_backlinks;
use crate::db::summaries::{Episode, list_episodes, store_episode};
use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::ranking::{CandidateSignals, Weights, rank_candidates, resolve_intent};
use crate::retrieval::{expand_results, pack_minimal};
use crate::tokens::estimate_tokens;

/// Return all available tool definitions.
pub fn tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "memory.search_minimal".into(),
            description: "Search the knowledge base and return compact memory stubs within a token budget. Use this first to find relevant memories, then expand specific ones.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query"
                    },
                    "intent": {
                        "type": "string",
                        "enum": ["lookup", "planning", "reflection", "synthesis", "auto"],
                        "description": "Retrieval intent — controls ranking weight profile. Default: auto",
                        "default": "auto"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 800",
                        "default": 800
                    },
                    "k": {
                        "type": "integer",
                        "description": "Maximum number of results. Default: 10",
                        "default": 10
                    }
                },
                "required": ["query"]
            }),
        },
        ToolDefinition {
            name: "memory.expand".into(),
            description: "Expand memory stubs to full content. Pass memory_ids from search_minimal results.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "memory_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Memory IDs to expand (from search_minimal results)"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens in response. Default: 2000",
                        "default": 2000
                    }
                },
                "required": ["memory_ids"]
            }),
        },
        ToolDefinition {
            name: "memory.write_episode".into(),
            description: "Record an episode (goal, actions, outcome) to the knowledge base.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "goal": {
                        "type": "string",
                        "description": "What was the goal"
                    },
                    "actions": {
                        "type": "string",
                        "description": "What actions were taken"
                    },
                    "outcome": {
                        "type": "string",
                        "description": "What was the outcome"
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Tags for categorization"
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance score (0.0 to 1.0). Default: 1.0",
                        "default": 1.0
                    }
                },
                "required": ["goal", "actions", "outcome"]
            }),
        },
        ToolDefinition {
            name: "memory.reflect".into(),
            description: "Retrieve source material for reflection. Returns relevant memories that the LLM can synthesize into a reflection, then call back to store.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Topic to reflect on"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens for source material. Default: 2000",
                        "default": 2000
                    }
                },
                "required": ["topic"]
            }),
        },
    ]
}

/// Dispatch a tool call to the appropriate handler.
pub async fn dispatch_tool_call(name: &str, params: &Value, ctx: &McpContext) -> ToolCallResult {
    match name {
        "memory.search_minimal" => handle_search_minimal(params, ctx).await,
        "memory.expand" => handle_expand(params, ctx).await,
        "memory.write_episode" => handle_write_episode(params, ctx),
        "memory.reflect" => handle_reflect(params, ctx).await,
        _ => ToolCallResult::error(format!("Unknown tool: {name}")),
    }
}

const CANDIDATE_LIMIT: usize = 50;

async fn handle_search_minimal(params: &Value, ctx: &McpContext) -> ToolCallResult {
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

async fn handle_expand(params: &Value, ctx: &McpContext) -> ToolCallResult {
    let memory_ids: Vec<String> = match params.get("memory_ids").and_then(|v| v.as_array()) {
        Some(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        None => return ToolCallResult::error("Missing required parameter: memory_ids"),
    };

    let budget_tokens = params
        .get("budget_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(2000) as usize;

    // Look up chunks from SQLite
    let rows = match ctx
        .db
        .with_conn(|conn| get_chunks_by_ids(conn, &memory_ids))
    {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "chunk lookup failed");
            return ToolCallResult::error(format!("Chunk lookup failed: {e}"));
        }
    };

    // Preserve the requested order
    let row_map: HashMap<&str, _> = rows.iter().map(|r| (r.chunk_id.as_str(), r)).collect();
    let ordered_rows: Vec<_> = memory_ids
        .iter()
        .filter_map(|id| row_map.get(id.as_str()).copied())
        .collect();

    // Build ranked results for expand_results (scores don't matter here)
    let ranked: Vec<crate::ranking::RankedResult> = ordered_rows
        .iter()
        .map(|row| crate::ranking::RankedResult {
            chunk_id: row.chunk_id.clone(),
            hybrid_score: 0.0,
            scores: crate::ranking::SignalScores {
                vector: 0.0,
                keyword: 0.0,
                recency: 0.0,
                links: 0.0,
                tag_match: 0.0,
                importance: 0.0,
            },
            file_path: row.file_path.clone(),
            heading_path: row.heading_path.clone(),
            content: row.content.clone(),
            token_estimate: row.token_estimate,
        })
        .collect();

    let expand_result = expand_results(&ranked, budget_tokens);

    let memories_json: Vec<Value> = expand_result
        .memories
        .iter()
        .map(|m| {
            json!({
                "memory_id": m.memory_id,
                "content": m.content,
                "file_path": m.file_path,
                "heading_path": m.heading_path,
                "byte_start": m.byte_start,
                "byte_end": m.byte_end,
                "truncated": m.truncated,
            })
        })
        .collect();

    let response = json!({
        "budget_tokens": expand_result.budget_tokens,
        "used_tokens_est": expand_result.used_tokens_est,
        "memories": memories_json
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

fn handle_write_episode(params: &Value, ctx: &McpContext) -> ToolCallResult {
    let goal = match params.get("goal").and_then(|v| v.as_str()) {
        Some(g) => g,
        None => return ToolCallResult::error("Missing required parameter: goal"),
    };
    let actions = match params.get("actions").and_then(|v| v.as_str()) {
        Some(a) => a,
        None => return ToolCallResult::error("Missing required parameter: actions"),
    };
    let outcome = match params.get("outcome").and_then(|v| v.as_str()) {
        Some(o) => o,
        None => return ToolCallResult::error("Missing required parameter: outcome"),
    };

    let tags: Vec<String> = params
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let importance = params
        .get("importance")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);

    let episode = Episode {
        goal: goal.to_string(),
        actions: actions.to_string(),
        outcome: outcome.to_string(),
        tags: tags.clone(),
        importance,
    };

    match ctx.db.with_conn(|conn| store_episode(conn, &episode)) {
        Ok(summary_id) => {
            let response = json!({
                "status": "stored",
                "summary_id": summary_id,
                "goal": goal,
                "tags": tags,
                "importance": importance
            });
            ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
        }
        Err(e) => {
            error!(error = %e, "failed to store episode");
            ToolCallResult::error(format!("Failed to store episode: {e}"))
        }
    }
}

async fn handle_reflect(params: &Value, ctx: &McpContext) -> ToolCallResult {
    let topic = match params.get("topic").and_then(|v| v.as_str()) {
        Some(t) => t,
        None => return ToolCallResult::error("Missing required parameter: topic"),
    };

    let budget_tokens = params
        .get("budget_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(2000) as usize;

    // Gather source material: recent episodes + relevant chunks
    let episodes = ctx
        .db
        .with_conn(|conn| list_episodes(conn, 10))
        .unwrap_or_default();

    // Also search for relevant chunks via search_minimal logic
    let search_params = json!({
        "query": topic,
        "intent": "reflection",
        "budget_tokens": budget_tokens / 2,
        "k": 5
    });
    let search_result = handle_search_minimal(&search_params, ctx).await;

    // Build combined source material
    let episode_sources: Vec<Value> = episodes
        .iter()
        .map(|ep| {
            json!({
                "type": "episode",
                "summary_id": ep.summary_id,
                "title": ep.title,
                "content": ep.content,
                "tags": ep.tags,
                "importance": ep.importance,
            })
        })
        .collect();

    let response = json!({
        "topic": topic,
        "budget_tokens": budget_tokens,
        "source_count": episode_sources.len(),
        "episodes": episode_sources,
        "related_chunks": serde_json::from_str::<Value>(
            search_result.content.first().map(|c| c.text.as_str()).unwrap_or("{}")
        ).unwrap_or(json!({})),
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_definitions_valid() {
        let defs = tool_definitions();
        assert_eq!(defs.len(), 4);

        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"memory.search_minimal"));
        assert!(names.contains(&"memory.expand"));
        assert!(names.contains(&"memory.write_episode"));
        assert!(names.contains(&"memory.reflect"));

        // All should have valid JSON schemas
        for def in &defs {
            assert!(def.input_schema.is_object());
            assert!(def.input_schema.get("type").is_some());
        }
    }

    #[test]
    fn test_dispatch_unknown_tool() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        // Create a minimal context for the test
        let ctx = rt.block_on(async { create_test_context().await });
        let result = rt.block_on(dispatch_tool_call("nonexistent", &json!({}), &ctx));
        assert_eq!(result.is_error, Some(true));
    }

    #[test]
    fn test_dispatch_search_minimal_missing_query() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });
        let result = rt.block_on(dispatch_tool_call(
            "memory.search_minimal",
            &json!({}),
            &ctx,
        ));
        assert_eq!(result.is_error, Some(true));
    }

    #[test]
    fn test_dispatch_write_episode() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "goal": "Fix the bug",
            "actions": "Debugged and patched",
            "outcome": "Bug fixed",
            "tags": ["debugging"],
            "importance": 0.8
        });

        let result = rt.block_on(dispatch_tool_call("memory.write_episode", &params, &ctx));
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["status"], "stored");
        assert!(parsed["summary_id"].is_string());
    }

    #[test]
    fn test_dispatch_expand_missing_ids() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });
        let result = rt.block_on(dispatch_tool_call("memory.expand", &json!({}), &ctx));
        assert_eq!(result.is_error, Some(true));
    }

    #[test]
    fn test_dispatch_expand_empty_ids() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });
        let params = json!({ "memory_ids": [], "budget_tokens": 1000 });
        let result = rt.block_on(dispatch_tool_call("memory.expand", &params, &ctx));
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["memories"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_dispatch_reflect() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({ "topic": "project architecture" });
        let result = rt.block_on(dispatch_tool_call("memory.reflect", &params, &ctx));
        assert!(result.is_error.is_none());
    }

    async fn create_test_context() -> McpContext {
        let tmp = tempfile::TempDir::new().unwrap();
        let sqlite_path = tmp.path().join("test.db");
        let lance_path = tmp.path().join("test_lance");

        let db = crate::db::Db::open(&sqlite_path).unwrap();
        let store = crate::store::Store::open_or_create(&lance_path)
            .await
            .unwrap();
        let embedder = Arc::new(crate::embedder::MockEmbedder);

        // Leak the TempDir so it lives for the test duration
        std::mem::forget(tmp);

        McpContext {
            db,
            store,
            embedder,
        }
    }
}
