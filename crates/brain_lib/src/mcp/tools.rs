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
use crate::tasks::events::{EventType, TaskEvent, new_event_id, now_ts};
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
        ToolDefinition {
            name: "tasks.apply_event".into(),
            description: "Apply an event to the task system. Creates, updates, or changes tasks via event sourcing. Returns the resulting task state and any newly unblocked task IDs.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "event_type": {
                        "type": "string",
                        "enum": ["task_created", "task_updated", "status_changed",
                                 "dependency_added", "dependency_removed",
                                 "note_linked", "note_unlinked",
                                 "label_added", "label_removed", "comment_added"],
                        "description": "The type of task event to apply"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Task ID. Optional for task_created (auto-generates UUID v7), required for all other event types."
                    },
                    "actor": {
                        "type": "string",
                        "description": "Who is performing this action. Default: 'mcp'",
                        "default": "mcp"
                    },
                    "payload": {
                        "type": "object",
                        "description": "Event-type-specific fields. task_created: {title, description?, priority?, due_ts?, task_type?, assignee?, defer_until?}. task_updated: {title?, description?, priority?, due_ts?, blocked_reason?, task_type?, assignee?, defer_until?}. status_changed: {new_status}. dependency_added/removed: {depends_on_task_id}. note_linked/unlinked: {chunk_id}. label_added/removed: {label}. comment_added: {body}."
                    }
                },
                "required": ["event_type", "payload"]
            }),
        },
        ToolDefinition {
            name: "tasks.next".into(),
            description: "Get the next highest-priority ready task(s). Returns tasks with no unresolved dependencies, sorted by configurable policy. Includes dependency summary and linked notes for each task.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "policy": {
                        "type": "string",
                        "enum": ["priority", "due_date"],
                        "description": "Sorting policy. 'priority' (default): by priority then due date. 'due_date': by due date then priority.",
                        "default": "priority"
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of tasks to return. Default: 1",
                        "default": 1
                    }
                }
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
        "tasks.apply_event" => handle_tasks_apply_event(params, ctx),
        "tasks.next" => handle_tasks_next(params, ctx),
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

fn handle_tasks_apply_event(params: &Value, ctx: &McpContext) -> ToolCallResult {
    // Parse event_type
    let event_type_str = match params.get("event_type").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return ToolCallResult::error("Missing required parameter: event_type"),
    };

    let event_type: EventType = match serde_json::from_value(json!(event_type_str)) {
        Ok(et) => et,
        Err(_) => {
            return ToolCallResult::error(format!(
                "Invalid event_type: '{event_type_str}'. Must be one of: task_created, \
                 task_updated, status_changed, dependency_added, dependency_removed, \
                 note_linked, note_unlinked"
            ));
        }
    };

    // Parse payload
    let payload = match params.get("payload") {
        Some(p) if p.is_object() => p.clone(),
        Some(_) => return ToolCallResult::error("Parameter 'payload' must be an object"),
        None => return ToolCallResult::error("Missing required parameter: payload"),
    };

    // Parse task_id: auto-generate for task_created if not provided
    let task_id = match params.get("task_id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => {
            if event_type == EventType::TaskCreated {
                new_event_id() // UUID v7 as task ID
            } else {
                return ToolCallResult::error(
                    "Missing required parameter: task_id (required for all event types except task_created)",
                );
            }
        }
    };

    let actor = params
        .get("actor")
        .and_then(|v| v.as_str())
        .unwrap_or("mcp")
        .to_string();

    // For task_created, inject defaults if not provided
    let payload = if event_type == EventType::TaskCreated {
        let mut p = payload;
        if p.get("status").is_none() {
            p["status"] = json!("open");
        }
        if p.get("priority").is_none() {
            p["priority"] = json!(4);
        }
        if p.get("task_type").is_none() {
            p["task_type"] = json!("task");
        }
        p
    } else {
        payload
    };

    let event = TaskEvent {
        event_id: new_event_id(),
        task_id: task_id.clone(),
        timestamp: now_ts(),
        actor,
        event_type: event_type.clone(),
        payload,
    };

    // Append (validates + writes JSONL + applies projection)
    if let Err(e) = ctx.tasks.append(&event) {
        return ToolCallResult::error(format!("Task event failed: {e}"));
    }

    // Fetch resulting task state
    let task_json = match ctx.tasks.get_task(&task_id) {
        Ok(Some(row)) => {
            let labels = ctx.tasks.get_task_labels(&task_id).unwrap_or_default();
            json!({
                "task_id": row.task_id,
                "title": row.title,
                "description": row.description,
                "status": row.status,
                "priority": row.priority,
                "blocked_reason": row.blocked_reason,
                "due_ts": row.due_ts,
                "task_type": row.task_type,
                "assignee": row.assignee,
                "defer_until": row.defer_until,
                "labels": labels,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            })
        }
        Ok(None) => json!(null),
        Err(e) => {
            warn!(error = %e, "failed to fetch task after event");
            json!(null)
        }
    };

    // Detect newly unblocked tasks after status_changed to done/cancelled
    let unblocked_task_ids = if event_type == EventType::StatusChanged {
        let new_status = event
            .payload
            .get("new_status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if new_status == "done" || new_status == "cancelled" {
            ctx.tasks.list_newly_unblocked(&task_id).unwrap_or_default()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    let response = json!({
        "event_id": event.event_id,
        "task_id": task_id,
        "task": task_json,
        "unblocked_task_ids": unblocked_task_ids,
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

fn handle_tasks_next(params: &Value, ctx: &McpContext) -> ToolCallResult {
    let policy = params
        .get("policy")
        .and_then(|v| v.as_str())
        .unwrap_or("priority");

    let k = params.get("k").and_then(|v| v.as_u64()).unwrap_or(1) as usize;

    // Get ready tasks (already sorted by priority policy)
    let ready_tasks = match ctx.tasks.list_ready() {
        Ok(tasks) => tasks,
        Err(e) => {
            error!(error = %e, "failed to list ready tasks");
            return ToolCallResult::error(format!("Failed to list ready tasks: {e}"));
        }
    };

    // Re-sort if due_date policy requested
    let mut tasks = ready_tasks;
    if policy == "due_date" {
        tasks.sort_by(|a, b| {
            // due_ts ASC NULLS LAST, then priority ASC, then task_id ASC
            let due_cmp = match (a.due_ts, b.due_ts) {
                (Some(a_ts), Some(b_ts)) => a_ts.cmp(&b_ts),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            };
            due_cmp
                .then(a.priority.cmp(&b.priority))
                .then(a.task_id.cmp(&b.task_id))
        });
    }

    // Take top-k
    let selected: Vec<_> = tasks.into_iter().take(k).collect();

    // Build response with dependency summaries and note links
    let results_json: Vec<Value> = selected
        .iter()
        .map(|task| {
            let dep_summary = ctx
                .tasks
                .get_dependency_summary(&task.task_id)
                .unwrap_or_else(|_| crate::tasks::queries::DependencySummary {
                    total_deps: 0,
                    done_deps: 0,
                    blocking_task_ids: vec![],
                });

            let note_links = ctx
                .tasks
                .get_task_note_links(&task.task_id)
                .unwrap_or_default();

            let labels = ctx.tasks.get_task_labels(&task.task_id).unwrap_or_default();

            let linked_notes: Vec<Value> = note_links
                .iter()
                .map(|nl| {
                    json!({
                        "chunk_id": nl.chunk_id,
                        "file_path": nl.file_path,
                    })
                })
                .collect();

            json!({
                "task_id": task.task_id,
                "title": task.title,
                "description": task.description,
                "status": task.status,
                "priority": task.priority,
                "due_ts": task.due_ts,
                "task_type": task.task_type,
                "assignee": task.assignee,
                "defer_until": task.defer_until,
                "labels": labels,
                "dependency_summary": {
                    "total_deps": dep_summary.total_deps,
                    "done_deps": dep_summary.done_deps,
                    "blocking_tasks": dep_summary.blocking_task_ids,
                },
                "linked_notes": linked_notes,
            })
        })
        .collect();

    // Get aggregate counts
    let (ready_count, blocked_count) = ctx.tasks.count_ready_blocked().unwrap_or((0, 0));

    let response = json!({
        "results": results_json,
        "ready_count": ready_count,
        "blocked_count": blocked_count,
    });

    ToolCallResult::text(serde_json::to_string_pretty(&response).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_definitions_valid() {
        let defs = tool_definitions();
        assert_eq!(defs.len(), 6);

        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"memory.search_minimal"));
        assert!(names.contains(&"memory.expand"));
        assert!(names.contains(&"memory.write_episode"));
        assert!(names.contains(&"memory.reflect"));
        assert!(names.contains(&"tasks.apply_event"));
        assert!(names.contains(&"tasks.next"));

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

    #[test]
    fn test_tasks_apply_event_create() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "test-1",
            "payload": { "title": "My first task", "priority": 2 }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert!(result.is_error.is_none(), "should succeed");

        let text = &result.content[0].text;
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["task_id"], "test-1");
        assert!(parsed["event_id"].is_string());
        assert_eq!(parsed["task"]["title"], "My first task");
        assert_eq!(parsed["task"]["status"], "open");
        assert_eq!(parsed["task"]["priority"], 2);
        assert_eq!(parsed["unblocked_task_ids"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_tasks_apply_event_auto_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "payload": { "title": "Auto ID task" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert!(result.is_error.is_none());

        let text = &result.content[0].text;
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert!(parsed["task_id"].is_string());
        assert!(!parsed["task_id"].as_str().unwrap().is_empty());
        assert_eq!(parsed["task"]["title"], "Auto ID task");
        assert_eq!(parsed["task"]["priority"], 4); // default
    }

    #[test]
    fn test_tasks_apply_event_status_change() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create task first
        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Task" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &create, &ctx));

        // Change status
        let update = json!({
            "event_type": "status_changed",
            "task_id": "t1",
            "payload": { "new_status": "in_progress" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &update, &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["status"], "in_progress");
    }

    #[test]
    fn test_tasks_apply_event_unblocked() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create two tasks, t2 depends on t1
        for (id, title) in &[("t1", "Blocker"), ("t2", "Blocked")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            rt.block_on(dispatch_tool_call("tasks.apply_event", &p, &ctx));
        }

        let dep = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &dep, &ctx));

        // Complete t1 — t2 should be unblocked
        let done = json!({
            "event_type": "status_changed",
            "task_id": "t1",
            "payload": { "new_status": "done" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &done, &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let unblocked = parsed["unblocked_task_ids"].as_array().unwrap();
        assert_eq!(unblocked.len(), 1);
        assert_eq!(unblocked[0], "t2");
    }

    #[test]
    fn test_tasks_apply_event_cycle_rejected() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create two tasks
        for (id, title) in &[("t1", "Task 1"), ("t2", "Task 2")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            rt.block_on(dispatch_tool_call("tasks.apply_event", &p, &ctx));
        }

        // t1 depends on t2
        let dep1 = json!({
            "event_type": "dependency_added",
            "task_id": "t1",
            "payload": { "depends_on_task_id": "t2" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &dep1, &ctx));

        // t2 depends on t1 — cycle!
        let dep2 = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &dep2, &ctx));
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("cycle"));
    }

    #[test]
    fn test_tasks_apply_event_missing_event_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({ "payload": { "title": "No event type" } });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert_eq!(result.is_error, Some(true));
    }

    #[test]
    fn test_tasks_apply_event_invalid_event_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "bogus_event",
            "payload": { "title": "Bad type" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("Invalid event_type"));
    }

    #[test]
    fn test_tasks_apply_event_missing_task_id_for_update() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "status_changed",
            "payload": { "new_status": "done" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert_eq!(result.is_error, Some(true));
        assert!(result.content[0].text.contains("task_id"));
    }

    #[test]
    fn test_tasks_next_returns_highest_priority() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create tasks with different priorities
        for (id, title, priority) in &[("t1", "Low", 4), ("t2", "High", 1), ("t3", "Medium", 2)] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title, "priority": priority }
            });
            rt.block_on(dispatch_tool_call("tasks.apply_event", &p, &ctx));
        }

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let results = parsed["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["task_id"], "t2");
        assert_eq!(results[0]["priority"], 1);
        assert_eq!(parsed["ready_count"], 3);
        assert_eq!(parsed["blocked_count"], 0);
    }

    #[test]
    fn test_tasks_next_excludes_blocked() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // t1 (P2), t2 (P1) depends on t1
        let p1 = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Blocker", "priority": 2 }
        });
        let p2 = json!({
            "event_type": "task_created",
            "task_id": "t2",
            "payload": { "title": "Blocked", "priority": 1 }
        });
        let dep = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &p1, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &p2, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &dep, &ctx));

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let results = parsed["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["task_id"], "t1"); // t2 is blocked
        assert_eq!(parsed["ready_count"], 1);
        assert_eq!(parsed["blocked_count"], 1);
    }

    #[test]
    fn test_tasks_next_k_multiple() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        for (id, title) in &[("t1", "Task 1"), ("t2", "Task 2"), ("t3", "Task 3")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title, "priority": 2 }
            });
            rt.block_on(dispatch_tool_call("tasks.apply_event", &p, &ctx));
        }

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({ "k": 2 }), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["results"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_tasks_next_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["results"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["ready_count"], 0);
    }

    #[test]
    fn test_tasks_next_includes_dependency_summary() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create t1 (done), t2 depends on t1 (now ready)
        let p1 = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Done task", "priority": 2 }
        });
        let p2 = json!({
            "event_type": "task_created",
            "task_id": "t2",
            "payload": { "title": "Ready task", "priority": 1 }
        });
        let dep = json!({
            "event_type": "dependency_added",
            "task_id": "t2",
            "payload": { "depends_on_task_id": "t1" }
        });
        let done = json!({
            "event_type": "status_changed",
            "task_id": "t1",
            "payload": { "new_status": "done" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &p1, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &p2, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &dep, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &done, &ctx));

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let task = &parsed["results"][0];
        assert_eq!(task["task_id"], "t2");
        assert_eq!(task["dependency_summary"]["total_deps"], 1);
        assert_eq!(task["dependency_summary"]["done_deps"], 1);
        assert_eq!(
            task["dependency_summary"]["blocking_tasks"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn test_tasks_apply_event_with_type_and_assignee() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": {
                "title": "Bug fix",
                "task_type": "bug",
                "assignee": "alice",
                "priority": 1
            }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["task_type"], "bug");
        assert_eq!(parsed["task"]["assignee"], "alice");
    }

    #[test]
    fn test_tasks_apply_event_labels() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        // Create task
        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Labeled task" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &create, &ctx));

        // Add labels
        let add1 = json!({
            "event_type": "label_added",
            "task_id": "t1",
            "payload": { "label": "urgent" }
        });
        let add2 = json!({
            "event_type": "label_added",
            "task_id": "t1",
            "payload": { "label": "backend" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &add1, &ctx));
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &add2, &ctx));
        assert!(result.is_error.is_none());

        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let labels = parsed["task"]["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&json!("backend")));
        assert!(labels.contains(&json!("urgent")));

        // Remove a label
        let rm = json!({
            "event_type": "label_removed",
            "task_id": "t1",
            "payload": { "label": "urgent" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &rm, &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let labels = parsed["task"]["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], "backend");
    }

    #[test]
    fn test_tasks_apply_event_comment() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Commented task" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &create, &ctx));

        let comment = json!({
            "event_type": "comment_added",
            "task_id": "t1",
            "actor": "bob",
            "payload": { "body": "This needs review" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &comment, &ctx));
        assert!(result.is_error.is_none());

        // Verify comment stored by fetching via TaskStore
        let comments = ctx.tasks.get_task_comments("t1").unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].body, "This needs review");
        assert_eq!(comments[0].author, "bob");
    }

    #[test]
    fn test_tasks_apply_event_default_task_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let params = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "No explicit type" }
        });
        let result = rt.block_on(dispatch_tool_call("tasks.apply_event", &params, &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["task"]["task_type"], "task");
    }

    #[test]
    fn test_tasks_next_includes_labels() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ctx = rt.block_on(async { create_test_context().await });

        let create = json!({
            "event_type": "task_created",
            "task_id": "t1",
            "payload": { "title": "Labeled", "priority": 1 }
        });
        let label = json!({
            "event_type": "label_added",
            "task_id": "t1",
            "payload": { "label": "critical" }
        });
        rt.block_on(dispatch_tool_call("tasks.apply_event", &create, &ctx));
        rt.block_on(dispatch_tool_call("tasks.apply_event", &label, &ctx));

        let result = rt.block_on(dispatch_tool_call("tasks.next", &json!({}), &ctx));
        let parsed: Value = serde_json::from_str(&result.content[0].text).unwrap();
        let task = &parsed["results"][0];
        let labels = task["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], "critical");
    }

    async fn create_test_context() -> McpContext {
        let tmp = tempfile::TempDir::new().unwrap();
        let sqlite_path = tmp.path().join("test.db");
        let lance_path = tmp.path().join("test_lance");
        let tasks_dir = tmp.path().join("tasks");

        let db = crate::db::Db::open(&sqlite_path).unwrap();
        let store = crate::store::Store::open_or_create(&lance_path)
            .await
            .unwrap();
        let embedder = Arc::new(crate::embedder::MockEmbedder);
        let tasks_db = crate::db::Db::open(&sqlite_path).unwrap();
        let tasks = crate::tasks::TaskStore::new(&tasks_dir, tasks_db).unwrap();

        // Leak the TempDir so it lives for the test duration
        std::mem::forget(tmp);

        McpContext {
            db,
            store,
            embedder,
            tasks,
        }
    }
}
