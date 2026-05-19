//! `brains.list` MCP tool — thin wrapper over `DaemonClient::brains_list`.
//!
//! The daemon's `BrainsList` handler already shapes per-brain roots /
//! aliases / extra_roots / prefix into wire types. This tool just
//! parses the `include_archived` flag, issues the wire call, and
//! shapes the response into the legacy JSON envelope.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use brain_rpc::BrainsListParams;

use super::{McpTool, json_response};
use crate::context::McpContext;
use crate::protocol::{ToolCallResult, ToolDefinition};

pub(super) struct BrainsList;

impl McpTool for BrainsList {
    fn name(&self) -> &'static str {
        "brains.list"
    }

    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name().into(),
            description: "List all registered brain projects from the global registry. Returns name, ID, root path, aliases, extra_roots, task prefix, and archived status for each brain. By default, archived brains are excluded. Pass include_archived: true to include them. Use this to discover available brains before cross-brain operations (federated search via memory.retrieve with brains parameter, or cross-brain task creation).".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "include_archived": {
                        "type": "boolean",
                        "description": "When true, include archived brains in the results. Defaults to false."
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
            let include_archived = params
                .get("include_archived")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let brains = match ctx
                .with_client(|c| c.brains_list(BrainsListParams { include_archived }))
                .await
            {
                Ok((b, _count)) => b,
                Err(err) => {
                    return ToolCallResult::error(format!("Failed to list brains: {err}"));
                }
            };

            // WireBrainSummary's serde shape (name/id/root/aliases/
            // extra_roots/prefix/archived) is byte-identical to the
            // legacy BrainInfo, so we emit the wire type directly.
            let count = brains.len();
            json_response(&json!({ "brains": brains, "count": count }))
        })
    }
}
