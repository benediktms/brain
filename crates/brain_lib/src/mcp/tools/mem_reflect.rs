use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};
use crate::query_pipeline::QueryPipeline;

use super::{McpTool, json_response};

#[derive(Deserialize)]
struct Params {
    #[serde(default = "default_mode")]
    mode: String, // "prepare" or "commit"
    // prepare mode fields:
    topic: Option<String>,
    #[serde(default = "default_budget")]
    budget_tokens: u64,
    #[serde(default)]
    brains: Vec<String>, // cross-brain support (informational; single-DB, unscoped)
    // commit mode fields:
    title: Option<String>,
    content: Option<String>,
    source_ids: Option<Vec<String>>,
    tags: Option<Vec<String>>,
    importance: Option<f64>,
}

fn default_mode() -> String {
    "prepare".into()
}

fn default_budget() -> u64 {
    2000
}

pub(super) struct MemReflect;

impl McpTool for MemReflect {
    fn name(&self) -> &'static str {
        "memory.reflect"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Retrieve source material for reflection, or store a completed reflection. Use mode='prepare' (default) to gather episodes and related chunks. Use mode='commit' to persist a reflection linked to source episodes.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["prepare", "commit"],
                        "description": "Operation mode. 'prepare' (default) gathers source material. 'commit' stores a reflection.",
                        "default": "prepare"
                    },
                    "topic": {
                        "type": "string",
                        "description": "Topic to reflect on (required for prepare mode)"
                    },
                    "budget_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens for source material (prepare mode). Default: 2000",
                        "default": 2000
                    },
                    "brains": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Brain IDs or names to include in episode gathering (prepare mode). Pass as a JSON array, e.g. [\"brain-a\", \"brain-b\"]. All brains share a single DB."
                    },
                    "title": {
                        "type": "string",
                        "description": "Title for the reflection (required for commit mode)"
                    },
                    "content": {
                        "type": "string",
                        "description": "Content of the reflection (required for commit mode)"
                    },
                    "source_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "summary_ids of episodes/summaries this reflection is based on (required for commit mode). May reference episodes from any brain."
                    },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Tags for the reflection (commit mode). Pass as a JSON array, e.g. [\"learning\", \"architecture\"]"
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance score 0.0–1.0 (commit mode). Default: 1.0",
                        "default": 1.0
                    }
                }
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

            match params.mode.as_str() {
                "commit" => Self::commit(params, ctx).await,
                "prepare" | _ => Self::prepare(params, ctx).await,
            }
        })
    }
}

impl MemReflect {
    async fn prepare(params: Params, ctx: &McpContext) -> ToolCallResult {
        let Some(store) = ctx.store.as_ref() else {
            return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
        };
        let Some(embedder) = ctx.embedder.as_ref() else {
            return ToolCallResult::error(super::MEMORY_UNAVAILABLE);
        };

        let topic = match params.topic {
            Some(t) => t,
            None => return ToolCallResult::error("Invalid parameters: 'topic' is required for prepare mode"),
        };

        // Log cross-brain request. The shared SQLite DB is unscoped — all
        // episodes are accessible regardless of brain origin. The brains
        // parameter is accepted and recorded for forward compatibility.
        if !params.brains.is_empty() {
            warn!(
                brains = ?params.brains,
                "cross-brain prepare requested; single-DB episodes are unscoped"
            );
        }

        let pipeline = QueryPipeline::new(&ctx.db, store, embedder, &ctx.metrics);
        let reflect_result = match pipeline
            .reflect(topic.clone(), params.budget_tokens as usize, &ctx.brain_id)
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

    async fn commit(params: Params, ctx: &McpContext) -> ToolCallResult {
        let title = match params.title {
            Some(t) => t,
            None => return ToolCallResult::error("Invalid parameters: 'title' is required for commit mode"),
        };
        let content = match params.content {
            Some(c) => c,
            None => return ToolCallResult::error("Invalid parameters: 'content' is required for commit mode"),
        };
        let source_ids = match params.source_ids {
            Some(ids) if !ids.is_empty() => ids,
            Some(_) => return ToolCallResult::error("Invalid parameters: 'source_ids' must be non-empty for commit mode"),
            None => return ToolCallResult::error("Invalid parameters: 'source_ids' is required for commit mode"),
        };
        let tags = params.tags.unwrap_or_default();
        let importance = params.importance.unwrap_or(1.0);

        // Validate source_ids exist (PK lookup, no brain_id filter — cross-brain refs allowed)
        for source_id in &source_ids {
            let id = source_id.clone();
            let exists = ctx.db.with_read_conn(move |conn| {
                crate::db::summaries::get_summary(conn, &id)
            });
            match exists {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return ToolCallResult::error(format!(
                        "source_id not found: {source_id}"
                    ));
                }
                Err(e) => {
                    return ToolCallResult::error(format!(
                        "Failed to validate source_id {source_id}: {e}"
                    ));
                }
            }
        }

        // Store the reflection
        let title_c = title.clone();
        let content_c = content.clone();
        let source_ids_c = source_ids.clone();
        let tags_c = tags.clone();
        let brain_id_c = ctx.brain_id.clone();
        let summary_id = ctx.db.with_write_conn(move |conn| {
            crate::db::summaries::store_reflection(
                conn,
                &title_c,
                &content_c,
                &source_ids_c,
                &tags_c,
                importance,
                &brain_id_c,
            )
        });

        let summary_id = match summary_id {
            Ok(id) => id,
            Err(e) => return ToolCallResult::error(format!("Failed to store reflection: {e}")),
        };

        let response = json!({
            "status": "stored",
            "summary_id": summary_id,
            "title": title,
            "source_count": source_ids.len(),
            "tags": tags,
            "importance": importance,
        });

        json_response(&response)
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
    }

    #[tokio::test]
    async fn test_reflect_prepare_explicit_mode() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({ "mode": "prepare", "topic": "testing patterns" });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert!(result.is_error.is_none());
    }

    #[tokio::test]
    async fn test_reflect_commit_missing_title() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({
            "mode": "commit",
            "content": "Some reflection",
            "source_ids": []
        });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert!(result.is_error.is_some());
    }

    #[tokio::test]
    async fn test_reflect_commit_invalid_source() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        let params = json!({
            "mode": "commit",
            "title": "My Reflection",
            "content": "I learned that patterns emerge from practice.",
            "source_ids": ["NONEXISTENT_ID_12345"]
        });
        let result = registry.dispatch("memory.reflect", params, &ctx).await;
        assert!(result.is_error.is_some());
    }

    #[tokio::test]
    async fn test_reflect_commit_roundtrip() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        // First store an episode to reference
        let ep_result = registry
            .dispatch(
                "memory.write_episode",
                json!({
                    "goal": "Learn reflection",
                    "actions": "Practiced writing reflections",
                    "outcome": "Understood the pattern",
                    "tags": ["learning"],
                    "importance": 0.9
                }),
                &ctx,
            )
            .await;
        assert!(ep_result.is_error.is_none());
        let ep_json: serde_json::Value =
            serde_json::from_str(&ep_result.content[0].text).unwrap();
        let episode_id = ep_json["summary_id"].as_str().unwrap().to_string();

        // Now commit a reflection referencing that episode
        let result = registry
            .dispatch(
                "memory.reflect",
                json!({
                    "mode": "commit",
                    "title": "Reflection on Learning",
                    "content": "Consistent practice builds durable understanding.",
                    "source_ids": [episode_id],
                    "tags": ["learning", "meta"],
                    "importance": 0.8
                }),
                &ctx,
            )
            .await;
        assert!(result.is_error.is_none());
        let parsed: serde_json::Value =
            serde_json::from_str(&result.content[0].text).unwrap();
        assert_eq!(parsed["status"], "stored");
        assert!(parsed["summary_id"].is_string());
        assert_eq!(parsed["source_count"], 1);
    }
}
