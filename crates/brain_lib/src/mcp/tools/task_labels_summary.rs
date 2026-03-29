use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use crate::mcp::McpContext;
use crate::mcp::protocol::{ToolCallResult, ToolDefinition};

use super::{McpTool, Warning, inject_warnings, json_response, store_or_warn};

pub(super) struct TaskLabelsSummary;

impl TaskLabelsSummary {
    fn execute(&self, _params: Value, ctx: &McpContext) -> ToolCallResult {
        let summaries = match ctx.stores.tasks.label_summary() {
            Ok(s) => s,
            Err(e) => return ToolCallResult::error(format!("Failed to query labels: {e}")),
        };

        let mut warnings: Vec<Warning> = Vec::new();
        let prefixes = store_or_warn(ctx.stores.tasks.compact_ids(), "compact_ids", &mut warnings);

        let labels: Vec<Value> = summaries
            .into_iter()
            .map(|s| {
                let short_ids: Vec<&str> = s
                    .task_ids
                    .iter()
                    .map(|id| {
                        prefixes
                            .get(id.as_str())
                            .map(|s| s.as_str())
                            .unwrap_or(id.as_str())
                    })
                    .collect();
                json!({
                    "label": s.label,
                    "count": s.count,
                    "task_ids": short_ids,
                })
            })
            .collect();

        let mut response = json!({ "labels": labels });
        inject_warnings(&mut response, warnings);
        json_response(&response)
    }
}

impl McpTool for TaskLabelsSummary {
    fn name(&self) -> &'static str {
        "tasks.labels_summary"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "Get all unique labels with counts and associated task IDs (short prefixes). Returns labels sorted by count descending. Use for label discovery and taxonomy overview.".into(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn call<'a>(
        &'a self,
        params: Value,
        ctx: &'a McpContext,
    ) -> Pin<Box<dyn Future<Output = ToolCallResult> + Send + 'a>> {
        Box::pin(std::future::ready(self.execute(params, ctx)))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::ToolRegistry;
    use super::super::tests::create_test_context;

    async fn dispatch(
        registry: &ToolRegistry,
        name: &str,
        params: Value,
        ctx: &crate::mcp::McpContext,
    ) -> crate::mcp::protocol::ToolCallResult {
        registry.dispatch(name, params, ctx).await
    }

    async fn create_tasks_with_labels(registry: &ToolRegistry, ctx: &crate::mcp::McpContext) {
        for (id, title) in &[("t1", "Task 1"), ("t2", "Task 2"), ("t3", "Task 3")] {
            let p = json!({
                "event_type": "task_created",
                "task_id": id,
                "payload": { "title": title }
            });
            dispatch(registry, "tasks.apply_event", p, ctx).await;
        }

        // Add labels: "urgent" on t1, t2, t3; "backend" on t1, t2
        for tid in &["t1", "t2", "t3"] {
            let p = json!({
                "event_type": "label_added",
                "task_id": tid,
                "payload": { "label": "urgent" }
            });
            dispatch(registry, "tasks.apply_event", p, ctx).await;
        }
        for tid in &["t1", "t2"] {
            let p = json!({
                "event_type": "label_added",
                "task_id": tid,
                "payload": { "label": "backend" }
            });
            dispatch(registry, "tasks.apply_event", p, ctx).await;
        }
    }

    #[tokio::test]
    async fn test_labels_summary_basic() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();
        create_tasks_with_labels(&registry, &ctx).await;

        let result = dispatch(&registry, "tasks.labels_summary", json!({}), &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let labels = parsed["labels"]
            .as_array()
            .expect("checked in test assertions");
        assert_eq!(labels.len(), 2);

        // First label should be "urgent" (count=3), then "backend" (count=2)
        assert_eq!(labels[0]["label"], "urgent");
        assert_eq!(labels[0]["count"], 3);
        assert_eq!(
            labels[0]["task_ids"]
                .as_array()
                .expect("checked in test assertions")
                .len(),
            3
        );

        assert_eq!(labels[1]["label"], "backend");
        assert_eq!(labels[1]["count"], 2);
        assert_eq!(
            labels[1]["task_ids"]
                .as_array()
                .expect("checked in test assertions")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn test_labels_summary_empty() {
        let (_dir, ctx) = create_test_context().await;
        let registry = ToolRegistry::new();

        let result = dispatch(&registry, "tasks.labels_summary", json!({}), &ctx).await;
        assert!(result.is_error.is_none());

        let parsed: Value =
            serde_json::from_str(&result.content[0].text).expect("checked in test assertions");
        let labels = parsed["labels"]
            .as_array()
            .expect("checked in test assertions");
        assert!(labels.is_empty());
    }
}
