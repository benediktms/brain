use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::ports::{EpisodeReader, ReflectionWriter};
use crate::query_pipeline::QueryPipeline;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    #[serde(default = "default_mode")]
    mode: String,
    // --- prepare fields ---
    #[serde(default)]
    topic: String,
    #[serde(default = "default_budget")]
    budget_tokens: u64,
    /// Brain names/IDs to include. Empty = current brain only.
    /// "all" = all brains.
    #[serde(default)]
    brains: Vec<String>,
    // --- commit fields ---
    #[serde(default)]
    title: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    source_ids: Vec<String>,
    #[serde(default)]
    tags: Vec<String>,
    importance: Option<f64>,
}

fn default_mode() -> String {
    "prepare".to_string()
}

fn default_budget() -> u64 {
    2000
}

pub(super) struct MemReflect;

impl MemReflect {
    /// Prepare mode: retrieve source material for reflection.
    async fn prepare(params: Params, ctx: &McpContext) -> ToolCallResult {
        let Some(store) = ctx.store() else {
            return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
        };
        let Some(embedder) = ctx.embedder() else {
            return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
        };

        if params.topic.is_empty() {
            return ToolCallResult::error("'topic' is required for prepare mode");
        }

        let pipeline = QueryPipeline::new(ctx.db(), store, embedder, &ctx.metrics);

        // Determine episode scope from `brains` parameter.
        // Note: the summaries table does not yet carry a brain_id column.
        // Until a schema migration adds it, episode listing is global.
        // The brains parameter is accepted but scoping is a no-op until
        // brain_persistence adds brain_id support to the summaries table.
        let episodes = if params.brains.is_empty() {
            ctx.db()
                .list_episodes(10, ctx.brain_id())
                .unwrap_or_default()
        } else if params.brains.iter().any(|b| b == "all") {
            ctx.db().list_episodes(10, "").unwrap_or_default()
        } else {
            let brain_ids: Vec<String> = params.brains.clone();
            ctx.db()
                .list_episodes_multi_brain(10, &brain_ids)
                .unwrap_or_default()
        };

        let reflect_result = match pipeline
            .reflect_with_episodes(
                params.topic.clone(),
                params.budget_tokens as usize,
                episodes,
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return ToolCallResult::error(format!("Reflect failed: {e}")),
        };

        let episode_sources: Vec<Value> = reflect_result
            .episodes
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

        let related_chunks_json: Vec<Value> = reflect_result
            .search_result
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
            "mode": "prepare",
            "topic": reflect_result.topic,
            "budget_tokens": reflect_result.budget_tokens,
            "source_count": episode_sources.len(),
            "episodes": episode_sources,
            "related_chunks": {
                "budget_tokens": reflect_result.search_result.budget_tokens,
                "used_tokens_est": reflect_result.search_result.used_tokens_est,
                "result_count": reflect_result.search_result.num_results,
                "total_available": reflect_result.search_result.total_available,
                "results": related_chunks_json,
            },
        });

        json_response(&response)
    }

    /// Commit mode: store a synthesized reflection linked to its source IDs.
    async fn commit(params: Params, ctx: &McpContext) -> ToolCallResult {
        if params.title.is_empty() {
            return ToolCallResult::error("'title' is required for commit mode");
        }
        if params.content.is_empty() {
            return ToolCallResult::error("'content' is required for commit mode");
        }
        if params.source_ids.is_empty() {
            return ToolCallResult::error("'source_ids' is required for commit mode");
        }

        // Finding 6: clamp importance to [0.0, 1.0].
        let importance = params.importance.unwrap_or(1.0).clamp(0.0, 1.0);

        // Finding 5: batch source_id validation — single round-trip.
        let source_ids = params.source_ids.clone();
        let found = match EpisodeReader::get_summaries_by_ids(ctx.db(), &source_ids) {
            Ok(rows) => rows,
            Err(e) => {
                return ToolCallResult::error(format!("Failed to validate source_ids: {e}"));
            }
        };
        let found_ids: HashSet<&str> = found.iter().map(|r| r.summary_id.as_str()).collect();
        for id in &source_ids {
            if !found_ids.contains(id.as_str()) {
                return ToolCallResult::error(format!("source_id not found: {id}"));
            }
        }

        // Store the reflection in SQLite.
        let summary_id = match ctx.db().store_reflection(
            &params.title,
            &params.content,
            &source_ids,
            &params.tags,
            importance,
            ctx.brain_id(),
        ) {
            Ok(id) => id,
            Err(e) => {
                return ToolCallResult::error(format!("Failed to store reflection: {e}"));
            }
        };

        // Finding 2: best-effort LanceDB embedding.
        if let (Some(embedder), Some(store)) = (ctx.embedder(), ctx.writable_store.as_ref()) {
            let embed_content = params.content.clone();
            match crate::embedder::embed_batch_async(embedder, vec![embed_content.clone()]).await {
                Ok(vecs) => {
                    if let Some(vec) = vecs.into_iter().next()
                        && let Err(e) = store
                            .upsert_summary(&summary_id, &embed_content, &vec)
                            .await
                    {
                        warn!(error = %e, summary_id, "failed to embed reflection (best-effort)");
                    }
                }
                Err(e) => {
                    warn!(error = %e, summary_id, "failed to generate embedding for reflection (best-effort)");
                }
            }
        }

        let response = json!({
            "mode": "commit",
            "status": "stored",
            "summary_id": summary_id,
            "title": params.title,
            "source_count": source_ids.len(),
            "importance": importance,
        });

        json_response(&response)
    }
}

impl McpTool for MemReflect {
    fn name(&self) -> &'static str {
        "memory.reflect"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: concat!(
                "Two-phase episodic reflection.\n\n",
                "**prepare** (default): Retrieve source material — recent episodes and related chunks — ",
                "that the LLM can synthesize into a reflection. Returns structured source material.\n\n",
                "**commit**: Store a completed reflection linked to its source episodes. ",
                "Requires title, content, and source_ids from a prior prepare call."
            ).into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["prepare", "commit"],
                        "description": "Operation mode. Default: 'prepare'",
                        "default": "prepare"
                    },
                    "topic": {
                        "type": "string",
                        "description": "(prepare) Topic to reflect on"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "(prepare) Maximum tokens for source material. Default: 2000",
                        "default": 2000
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "(prepare) Brain names/IDs to include. Empty = current brain. 'all' = all brains."
                    },
                    "title": {
                        "type": "string",
                        "description": "(commit) Title of the reflection"
                    },
                    "content": {
                        "type": "string",
                        "description": "(commit) Synthesized reflection content"
                    },
                    "source_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "(commit) summary_ids of source episodes used"
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "(commit) Tags for the reflection"
                    },
                    "importance": {
                        "type": "number",
                        "description": "(commit) Importance score (0.0–1.0). Default: 1.0",
                        "default": 1.0
                    }
                },
                "required": []
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(async move {
            let params: Params = match serde_json::from_value(params) {
                Ok(p) => p,
                Err(e) => return ToolCallResult::error(format!("Invalid parameters: {e}")),
            };

            // Finding 4: explicit mode dispatch with error on unknown values.
            match params.mode.as_str() {
                "prepare" => Self::prepare(params, ctx).await,
                "commit" => Self::commit(params, ctx).await,
                _ => ToolCallResult::error(format!(
                    "Invalid mode: '{}'. Must be 'prepare' or 'commit'",
                    params.mode
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    #[tokio::test]
    async fn test_reflect_prepare() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({ "topic": "project architecture" });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert!(result.is_error.is_none());
        let text = &result.content[0].text;
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["mode"], "prepare");
    }

    #[tokio::test]
    async fn test_reflect_invalid_mode_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({ "mode": "unknown" });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("Invalid mode"));
    }

    #[tokio::test]
    async fn test_reflect_commit_missing_source_id_returns_error() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({
            "mode": "commit",
            "title": "My Reflection",
            "content": "I learned that...",
            "source_ids": ["nonexistent-id"]
        });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert_eq!(result.is_error, Some(true));
        let text = &result.content[0].text;
        assert!(text.contains("source_id not found"));
    }

    #[tokio::test]
    async fn test_reflect_commit_clamps_importance() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // Store an episode first to use as source_id.
        let ep_result = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Learn Rust",
                    "actions": "Read the book",
                    "outcome": "Learned Rust"
                }),
                &ctx,
            )
            .await;
        assert!(ep_result.is_error.is_none());
        let ep_text = &ep_result.content[0].text;
        let ep_parsed: serde_json::Value = serde_json::from_str(ep_text).unwrap();
        let source_id = ep_parsed["summary_id"].as_str().unwrap().to_string();

        // Commit with out-of-range importance (2.5 should clamp to 1.0).
        let params = json!({
            "mode": "commit",
            "title": "Reflection on Rust",
            "content": "Rust is great",
            "source_ids": [source_id],
            "importance": 2.5
        });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert!(result.is_error.is_none(), "reflect commit failed: {}", result.content[0].text);
        let text = &result.content[0].text;
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["mode"], "commit");
        // Importance should be clamped to 1.0.
        assert!((parsed["importance"].as_f64().unwrap() - 1.0).abs() < f64::EPSILON);
    }
}
