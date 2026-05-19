//! Multi-brain JSON-RPC router.
//!
//! Routes JSON-RPC 2.0 `tools/call` requests received by [`IpcServer`]
//! to the appropriate brain by forwarding them over the
//! [`brain_rpc`] wire protocol via [`DaemonClient`].
//!
//! This is the last piece of the `brain_lib` ↔ `brain_mcp` coupling:
//! once this module no longer imports from `crate::mcp`, brain_lib has
//! zero knowledge of MCP tools or the tool registry.

use std::sync::{Arc, Mutex as StdMutex};

use serde_json::Value;

use brain_rpc::{DaemonClient, Request, RpcError, SagaDescriptionUpdate, UnixSocketTransport};

/// Shared RPC client handle.
///
/// Structure:
/// - `Arc` — cheap cloning so multiple concurrent dispatches each get their
///   own handle without moving the original
/// - `StdMutex` — one-time init only; only `set_client` holds the write guard
/// - `Option` — `None` until `set_client` is called after the socket is bound
/// - `Arc<StdMutex<DaemonClient>>` — cheap to clone; each cloned handle is the
///   token passed into `block_in_place`, and the inner `StdMutex` provides the
///   mutable borrow for `call(&mut self)`
type RpcClient = Arc<StdMutex<Option<Arc<StdMutex<DaemonClient<UnixSocketTransport>>>>>>;

/// Routes JSON-RPC tool calls by forwarding them to the daemon via RPC.
///
/// Each incoming connection gets its own `DaemonClient` (cloned from the
/// shared handle) so that multiple IPC clients can be in-flight simultaneously.
#[derive(Clone)]
pub struct BrainRouter {
    /// Shared RPC client handle. Set once via [`set_client`](Self::set_client)
    /// after the daemon socket is bound and listening.
    client: RpcClient,
    /// brain_id of the default brain (used when no `brain` param is supplied).
    default_brain_id: String,
}

impl BrainRouter {
    /// Create a new router with a placeholder client.
    ///
    /// Use [`set_client`](Self::set_client) to initialize the RPC client after
    /// the daemon socket is bound and listening. This ordering is required
    /// because `BrainRouter` acts as a client to its own socket — the socket
    /// must exist before `connect()` is called.
    pub fn new_disconnected(default_brain_id: String) -> Arc<Self> {
        Arc::new(Self {
            client: Arc::new(StdMutex::new(None)),
            default_brain_id,
        })
    }

    /// Set the RPC client after the daemon socket is listening.
    ///
    /// Call this after `IpcServer::bind` to avoid connecting to a socket
    /// that doesn't exist yet.
    pub fn set_client(self: &Arc<Self>, client: DaemonClient<UnixSocketTransport>) {
        *self.client.lock().unwrap() = Some(Arc::new(StdMutex::new(client)));
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
        let response = tokio::task::block_in_place(|| {
            // block_in_place moves this to the blocking thread pool so the
            // blocking I/O does not starve the async runtime.
            // Lock the Mutex, then clone the inner Arc so the MutexGuard is
            // released before we call block_in_place (which would otherwise
            // hold the guard across the blocking call).
            let inner_arc = {
                let guard = self.client.lock().unwrap();
                guard
                    .as_ref()
                    .cloned()
                    .expect("BrainRouter client not set — call set_client() after IpcServer::bind")
            };
            // inner_arc is Arc<DaemonClient>, owned; lock it to get &mut DaemonClient.
            let mut client = inner_arc.lock().unwrap();
            client.call(request)
        })
        .map_err(|e| format!("RPC error: {e}"))?;

        // Convert the RPC Response into a JSON Value for IPC serialization.
        let value = response_to_json(response).map_err(|e| e.to_string())?;
        Ok(value)
    }
}

/// Convert a brain_rpc Response to a JSON Value for IPC serialization.
fn response_to_json(response: brain_rpc::Response) -> Result<serde_json::Value, RpcError> {
    serde_json::to_value(&response).map_err(|_| RpcError::Unknown {
        message: "failed to serialize response".into(),
    })
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
                uri: params
                    .get("uri")
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
                time_scope: params
                    .get("time_scope")
                    .and_then(|v| v.as_str().map(|s| s.to_string())),
                time_after: params.get("time_after").and_then(|v| v.as_i64()),
                time_before: params.get("time_before").and_then(|v| v.as_i64()),
                tags: params
                    .get("tags")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                tags_require: params
                    .get("tags_require")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                tags_exclude: params
                    .get("tags_exclude")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                kinds: params
                    .get("kinds")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default(),
                explain: params
                    .get("explain")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
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
                brain_id: params
                    .get("brain_id")
                    .and_then(|v| v.as_str().filter(|s| !s.is_empty()).map(String::from)),
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
        "records.list" => Request::ArtifactsList {
            params: brain_rpc::ArtifactsListParams {
                kind: params
                    .get("kind")
                    .and_then(|v| v.as_str().map(String::from)),
                tag: params.get("tag").and_then(|v| v.as_str().map(String::from)),
                status: params
                    .get("status")
                    .and_then(|v| v.as_str().map(String::from)),
                limit: params
                    .get("limit")
                    .and_then(|v| v.as_u64().map(|n| n as u32)),
            },
        },
        "records.archive" => Request::RecordsArchive {
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
