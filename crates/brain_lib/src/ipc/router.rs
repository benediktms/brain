//! Multi-brain JSON-RPC router.
//!
//! Routes JSON-RPC 2.0 `tools/call` requests received by [`IpcServer`]
//! to the appropriate brain by forwarding them over the
//! [`brain_rpc`] wire protocol via [`DaemonClient`].
//!
//! This is the last piece of the `brain_lib` ↔ `brain_mcp` coupling:
//! once this module no longer imports from `crate::mcp`, brain_lib has
//! zero knowledge of MCP tools or the tool registry.

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;

use brain_rpc::{DaemonClient, Request, RpcError, SagaDescriptionUpdate, UnixSocketTransport};

/// Routes JSON-RPC tool calls by forwarding them to the daemon via RPC.
///
/// Each incoming connection gets its own `DaemonClient` (cloned from the
/// shared handle) so that multiple IPC clients can be in-flight simultaneously.
#[derive(Clone)]
pub struct BrainRouter {
    /// Shared RPC client handle. Each `dispatch` call acquires a mutable
    /// reference for the duration of one RPC round-trip.
    client: Arc<Mutex<DaemonClient<UnixSocketTransport>>>,
    /// brain_id of the default brain (used when no `brain` param is supplied).
    default_brain_id: String,
}

impl BrainRouter {
    /// Create a new router from a socket path and default brain_id.
    ///
    /// Connects to the daemon's socket (creating the transport in the
    /// process) so multiple IPC clients can share one connection handle
    /// cloned from `self.client`.
    pub fn new(socket_path: &std::path::Path, default_brain_id: String) -> Arc<Self> {
        let transport = UnixSocketTransport::connect(socket_path)
            .expect("BrainRouter: failed to connect to daemon socket");
        let client = DaemonClient::connect(transport)
            .expect("BrainRouter: failed to hand off transport to DaemonClient");
        Arc::new(Self {
            client: Arc::new(Mutex::new(client)),
            default_brain_id,
        })
    }

    /// Dispatch a tool call by forwarding it to the daemon via RPC.
    ///
    /// Translates the IPC protocol (tool name + arguments) into the
    /// appropriate `Request` variant and calls `DaemonClient::call`.
    /// Returns a shape compatible with `ToolCallResult` so callers
    /// (`IpcServer`) can serialize it back as JSON-RPC 2.0.
    pub async fn dispatch(
        &self,
        brain: Option<&str>,
        tool_name: &str,
        params: Value,
    ) -> Result<serde_json::Value, String> {
        let resolved_brain = brain
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.default_brain_id.clone());

        let request = build_rpc_request(tool_name, &resolved_brain, params)?;
        let mut client = self.client.lock().await;
        let response = client
            .call(request)
            .map_err(|e| format!("RPC error: {e}"))?;

        // Convert the RPC Response into a shape that IpcServer can
        // serialize as a JSON-RPC response. brain_rpc Response carries
        // the result in its own variants — flatten to a JSON Value.
        let value = response_to_json(response).map_err(|e| e.to_string())?;
        Ok(value)
    }
}

/// Convert a brain_rpc Response to a JSON Value for IPC serialization.
fn response_to_json(response: brain_rpc::Response) -> Result<serde_json::Value, RpcError> {
    use brain_rpc::Response;
    match response {
        Response::Pong
        | Response::HandshakeOk { .. }
        | Response::TasksList { .. }
        | Response::TasksShow { .. }
        | Response::TasksNext { .. }
        | Response::TasksCreate { .. }
        | Response::TasksUpdate { .. }
        | Response::TasksMutate { .. }
        | Response::TasksDepAdded { .. }
        | Response::TasksDepRemoved { .. }
        | Response::TasksLabelAdded { .. }
        | Response::TasksLabelRemoved { .. }
        | Response::TasksTransfer { .. }
        | Response::RecordsVerify { .. }
        | Response::AnalysesList { .. }
        | Response::AnalysesShow { .. }
        | Response::AnalysesCreate { .. }
        | Response::ArtifactsList { .. }
        | Response::ArtifactsShow { .. }
        | Response::DocumentsList { .. }
        | Response::DocumentsShow { .. }
        | Response::DocumentsCreate { .. }
        | Response::PlansList { .. }
        | Response::PlansShow { .. }
        | Response::PlansCreate { .. }
        | Response::SnapshotsList { .. }
        | Response::SnapshotsShow { .. }
        | Response::SnapshotsCreate { .. }
        | Response::SagasList { .. }
        | Response::SagasGet { .. }
        | Response::SagasCreate { .. }
        | Response::SagasUpdate { .. }
        | Response::SagasAddTasks { .. }
        | Response::SagasRemoveTasks { .. }
        | Response::SagasFrontier { .. }
        | Response::SagasStart { .. }
        | Response::SagasClose { .. }
        | Response::SagasCancel { .. }
        | Response::SagasReopen { .. }
        | Response::SagasStats { .. }
        | Response::MemoryWriteEpisode { .. }
        | Response::MemoryWriteProcedure { .. }
        | Response::MemoryRetrieve { .. }
        | Response::MemoryConsolidate { .. }
        | Response::MemorySummarizeScope { .. }
        | Response::MemoryReflect { .. }
        | Response::TagsAliasesList { .. }
        | Response::TagsAliasesStatus { .. }
        | Response::JobsStatus { .. }
        | Response::BrainStatus { .. }
        | Response::ProviderList { .. }
        | Response::LinksAdd { .. }
        | Response::LinksRemove { .. }
        | Response::LinksForEntity { .. }
        | Response::RecordsArchive { .. }
        | Response::RecordsLinkAdd { .. }
        | Response::RecordsLinkRemove { .. }
        | Response::RecordsTagAdd { .. }
        | Response::RecordsTagRemove { .. }
        | Response::RecordsSearch { .. }
        | Response::RecordsFetchContent { .. }
        | Response::TasksApplyEvent { .. }
        | Response::TasksDepsBatch { .. }
        | Response::TasksLabelsBatch { .. }
        | Response::TasksLabelsSummary { .. }
        | Response::MemoryWalkThread { .. }
        | Response::TagsRecluster { .. }
        | Response::BrainsList { .. }
        | Response::WatchAdded { .. }
        | Response::WatchRemoved { .. }
        | Response::WatchList { .. } => {
            Ok(
                serde_json::to_value(response).map_err(|_| RpcError::Unknown {
                    message: "failed to serialize response".into(),
                })?,
            )
        }
    }
}

/// Build the appropriate `Request` from IPC tool name + params.
fn build_rpc_request(tool_name: &str, brain: &str, params: Value) -> Result<Request, String> {
    let params = params.as_object().cloned().unwrap_or_default();

    // Convenience: build a brains list for multi-brain queries
    let brains_list = if brain.is_empty() {
        vec![]
    } else {
        vec![brain.to_string()]
    };

    let req = match tool_name {
        // ── brains ────────────────────────────────────────────────────
        "brains.list" => Request::BrainsList {
            params: brain_rpc::BrainsListParams {
                include_archived: params
                    .get("include_archived")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            },
        },
        // ── tasks ────────────────────────────────────────────────────
        "tasks.list" => Request::TasksList {
            params: brain_rpc::TasksListParams {
                status: params
                    .get("status")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                priority: params
                    .get("priority")
                    .and_then(|v| v.as_u64().map(|n| n as u8)),
                limit: params
                    .get("limit")
                    .and_then(|v| v.as_u64().map(|n| n as u32)),
                search: params
                    .get("search")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                ..Default::default()
            },
        },
        "tasks.get" => Request::TasksShow {
            id: params
                .get("task_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        },
        "tasks.create" => Request::TasksCreate {
            params: brain_rpc::TasksCreateParams {
                title: params
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                description: params
                    .get("description")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                priority: params
                    .get("priority")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as u8)
                    .unwrap_or(4),
                task_type: params
                    .get("task_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("task")
                    .to_string(),
                assignee: params
                    .get("assignee")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                parent: params
                    .get("parent")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
            },
        },
        "tasks.next" => Request::TasksNext,
        "tasks.apply_event" => Request::TasksApplyEvent {
            params: brain_rpc::TasksApplyEventParams {
                event_json: serde_json::to_value(&params).unwrap_or_default(),
            },
        },
        // ── memory ───────────────────────────────────────────────────
        "memory.write_episode" => Request::MemoryWriteEpisode {
            params: brain_rpc::MemoryWriteEpisodeParams {
                goal: params
                    .get("goal")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                actions: params
                    .get("actions")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                outcome: params
                    .get("outcome")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                tags: params
                    .get("tags")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                importance_millis: params
                    .get("importance")
                    .and_then(|v| v.as_f64())
                    .map(|f| (f * 1000.0) as u32)
                    .unwrap_or(1000),
                continues: params
                    .get("continues")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
            },
        },
        "memory.retrieve" => Request::MemoryRetrieve {
            params: brain_rpc::MemoryRetrieveParams {
                query: params
                    .get("query")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                lod: params
                    .get("lod")
                    .and_then(|v| v.as_str())
                    .unwrap_or("L0")
                    .to_string(),
                count: params.get("count").and_then(|v| v.as_u64()).unwrap_or(10),
                strategy: params
                    .get("strategy")
                    .and_then(|v| v.as_str())
                    .unwrap_or("auto")
                    .to_string(),
                brains: brains_list,
                ..Default::default()
            },
        },
        "memory.walk_thread" => Request::MemoryWalkThread {
            params: brain_rpc::MemoryWalkThreadParams {
                params_json: serde_json::to_value(&params).unwrap_or_default(),
            },
        },
        "memory.consolidate" => Request::MemoryConsolidate {
            params: brain_rpc::MemoryConsolidateParams {
                limit: params.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize,
                gap_seconds: params
                    .get("gap_seconds")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(3600),
                auto_summarize: params
                    .get("auto_summarize")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            },
        },
        "memory.reflect" => Request::MemoryReflect {
            params: brain_rpc::MemoryReflectParams {
                commit: true,
                topic: params
                    .get("topic")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                budget: params
                    .get("budget_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(2000) as usize,
                brains: brains_list,
                title: params
                    .get("title")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                content: params
                    .get("content")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                source_ids: params
                    .get("source_ids")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                tags: params
                    .get("tags")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                importance_millis: params
                    .get("importance")
                    .and_then(|v| v.as_f64())
                    .map(|f| (f * 1000.0) as u32),
                ..Default::default()
            },
        },
        // ── links ─────────────────────────────────────────────────────
        "links.add" => Request::LinksAdd {
            params: brain_rpc::LinksAddParams {
                from: params
                    .get("from")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or(brain_rpc::WireEntityRef {
                        kind: "".into(),
                        id: "".into(),
                    }),
                to: params
                    .get("to")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or(brain_rpc::WireEntityRef {
                        kind: "".into(),
                        id: "".into(),
                    }),
                edge_kind: params
                    .get("edge_kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or("relates_to")
                    .to_string(),
            },
        },
        "links.remove" => Request::LinksRemove {
            params: brain_rpc::LinksRemoveParams {
                from: params
                    .get("from")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or(brain_rpc::WireEntityRef {
                        kind: "".into(),
                        id: "".into(),
                    }),
                to: params
                    .get("to")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or(brain_rpc::WireEntityRef {
                        kind: "".into(),
                        id: "".into(),
                    }),
                edge_kind: params
                    .get("edge_kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or("relates_to")
                    .to_string(),
            },
        },
        "links.for_entity" => Request::LinksForEntity {
            params: brain_rpc::LinksForEntityParams {
                entity: params
                    .get("entity")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or(brain_rpc::WireEntityRef {
                        kind: "".into(),
                        id: "".into(),
                    }),
                direction: params
                    .get("direction")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                limit: params
                    .get("limit")
                    .and_then(|v| v.as_u64().map(|n| n as u32)),
            },
        },
        // ── sagas ──────────────────────────────────────────────────────
        "sagas.list" => Request::SagasList {
            params: brain_rpc::SagasListParams {
                include_closed: params
                    .get("include_closed")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                include_cancelled: params
                    .get("include_cancelled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                containing_brain: params
                    .get("containing_brain")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
            },
        },
        "sagas.get" => Request::SagasGet {
            saga_id: params
                .get("saga_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        },
        "sagas.create" => Request::SagasCreate {
            params: brain_rpc::SagasCreateParams {
                title: params
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                description: params
                    .get("description")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
            },
        },
        "sagas.update" => Request::SagasUpdate {
            params: brain_rpc::SagasUpdateParams {
                saga_id: params
                    .get("saga_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                title: params
                    .get("title")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                description: params.get("description").and_then(|v| v.as_str()).map(|s| {
                    SagaDescriptionUpdate::Set {
                        value: s.to_string(),
                    }
                }),
            },
        },
        "sagas.start" => Request::SagasStart {
            saga_id: params
                .get("saga_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        },
        "sagas.close" => Request::SagasClose {
            saga_id: params
                .get("saga_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            cascade: params
                .get("cascade")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        },
        "sagas.cancel" => Request::SagasCancel {
            saga_id: params
                .get("saga_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            cascade: params
                .get("cascade")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        },
        "sagas.frontier" => Request::SagasFrontier {
            saga_id: params
                .get("saga_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        },
        "sagas.stats" => Request::SagasStats {
            saga_id: params
                .get("saga_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        },
        "sagas.add_tasks" => Request::SagasAddTasks {
            saga_id: params
                .get("saga_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            task_ids: params
                .get("task_ids")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default(),
            cascade: params
                .get("cascade")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        },
        "sagas.remove_tasks" => Request::SagasRemoveTasks {
            saga_id: params
                .get("saga_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            task_ids: params
                .get("task_ids")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default(),
            cascade: params
                .get("cascade")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        },
        // ── records ───────────────────────────────────────────────────
        "records.list" => Request::RecordsArchive {
            params: brain_rpc::RecordsArchiveParams {
                record_id: params
                    .get("record_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                reason: params
                    .get("reason")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
            },
        },
        "records.search" => Request::RecordsSearch {
            params: brain_rpc::RecordsSearchParams {
                query: params
                    .get("query")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                k: params.get("k").and_then(|v| v.as_u64()).unwrap_or(10),
                budget_tokens: params
                    .get("budget_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(800),
                tags: params
                    .get("tags")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                brains: brains_list,
            },
        },
        "records.fetch_content" => Request::RecordsFetchContent {
            params: brain_rpc::RecordsFetchContentParams {
                record_id: params
                    .get("record_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                brain: Some(brain.to_string()),
            },
        },
        // ── tags ──────────────────────────────────────────────────────
        "tags.aliases_list" => Request::TagsAliasesList {
            params: brain_rpc::TagsAliasesListParams {
                canonical: params
                    .get("canonical")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                cluster_id: params
                    .get("cluster_id")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                limit: params.get("limit").and_then(|v| v.as_i64()).unwrap_or(50),
                offset: params.get("offset").and_then(|v| v.as_i64()).unwrap_or(0),
            },
        },
        "tags.aliases_status" => Request::TagsAliasesStatus,
        "tags.recluster" => Request::TagsRecluster {
            params: brain_rpc::TagsReclusterParams {
                params_json: serde_json::to_value(&params).unwrap_or_default(),
            },
        },
        // ── status ───────────────────────────────────────────────────
        "status" => Request::BrainStatus,
        "jobs.status" => Request::JobsStatus {
            params: brain_rpc::JobsStatusParams {
                kind: params
                    .get("kind")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                status: params
                    .get("status")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                limit: params.get("limit").and_then(|v| v.as_u64()).unwrap_or(10),
            },
        },
        // Catch-all: return a clear error for tools not yet wired
        _ => {
            return Err(format!(
                "tool '{tool_name}' not yet routed through BrainRouter IPC bridge"
            ));
        }
    };
    Ok(req)
}
