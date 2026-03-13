/// MCP stdio JSON-RPC server.
///
/// Implements the Model Context Protocol over newline-delimited JSON-RPC
/// on stdin/stdout. All tracing goes to stderr.
pub mod protocol;
pub mod tools;

use std::path::Path;
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::config::resolve_brain_entry;
use crate::db::Db;
use crate::embedder::{Embed, Embedder};
use crate::ipc::client::IpcClient;
use crate::metrics::Metrics;
use crate::records::RecordStore;
use crate::records::objects::ObjectStore;
use crate::store::{Store, StoreReader};
use crate::tasks::TaskStore;

use protocol::{
    InitializeResult, JsonRpcError, JsonRpcRequest, JsonRpcResponse, ServerCapabilities,
    ServerInfo, ToolsCapability, ToolsListResult,
};
use tools::ToolRegistry;

/// Dispatch mode for MCP tool calls.
///
/// `Local` uses the in-process tool registry and McpContext directly.
/// `Daemon` forwards `tools/call` requests to the daemon via UDS.
///
/// `session_brain_name` is the per-session brain name resolved from the MCP
/// `initialize` request's `roots` array. Defaults to the startup-resolved
/// brain name; updated when the client sends roots pointing to a different
/// registered brain.
enum DispatchMode {
    Local {
        ctx: Arc<McpContext>,
        /// Per-session brain name (resolved from initialize roots).
        session_brain_name: Arc<RwLock<String>>,
    },
    Daemon {
        client: Mutex<IpcClient>,
        /// Per-session brain name (resolved from initialize roots).
        session_brain_name: Arc<RwLock<String>>,
        /// Kept for tools/list and metrics even in daemon mode.
        ctx: Arc<McpContext>,
    },
}

/// Shared context for MCP tool handlers.
///
/// `store` and `embedder` are optional — they require the embedding model to
/// be downloaded. When absent, task tools still work but memory/search tools
/// return an error asking the user to download the model via the HuggingFace CLI.
///
/// Two database handles are maintained:
/// - `db`: per-brain SQLite (files, chunks, summaries, brain_meta) used by the
///   indexing pipeline, search queries, and the status tool.
/// - `unified_db`: `~/.brain/brain.db` shared across all brains.  TaskStore
///   and RecordStore are always opened against this handle so that
///   brain_id-scoped queries work correctly across the full workspace.
pub struct McpContext {
    /// Per-brain SQLite — indexing tables (files, chunks, summaries, brain_meta).
    pub db: Db,
    /// Unified SQLite (`~/.brain/brain.db`) — tasks, records, record_events.
    /// Falls back to `db` (same handle) when the unified DB does not yet exist
    /// (pre-migration installations).
    pub unified_db: Db,
    pub store: Option<StoreReader>,
    pub writable_store: Option<Store>, // for task capsule embedding
    pub embedder: Option<Arc<dyn Embed>>,
    pub tasks: TaskStore,
    pub records: RecordStore,
    pub objects: ObjectStore,
    pub metrics: Arc<Metrics>,
    /// The brain home directory (`$BRAIN_HOME` or `~/.brain`).
    pub brain_home: std::path::PathBuf,
    /// Human-readable name of the current brain (from its data directory path).
    pub brain_name: String,
    /// Stable ID of the current brain (from the `.brain/brain_id` file).
    pub brain_id: String,
}

impl McpContext {
    /// Bootstrap an MCP context with layered initialization.
    ///
    /// Always opens SQLite and creates a TaskStore (lightweight, reliable).
    /// Then optionally attempts to open LanceDB and load the embedder — if
    /// either fails the server still starts in tasks-only mode without
    /// memory/search tool support.
    ///
    /// This avoids the old approach of going through `IndexPipeline::new()`
    /// which always loads all three components before falling back.
    pub async fn bootstrap(
        model_dir: &Path,
        lance_db: &Path,
        sqlite_db: &Path,
    ) -> crate::error::Result<Arc<Self>> {
        // Step 1: always open SQLite and build TaskStore (required).
        let db = tokio::task::spawn_blocking({
            let sqlite_db = sqlite_db.to_path_buf();
            move || Db::open(&sqlite_db)
        })
        .await
        .map_err(|e| crate::error::BrainCoreError::Database(format!("spawn_blocking: {e}")))??;

        // Derive brain_name and brain_id from sqlite_db path early so stores
        // can be scoped correctly.
        // Convention: sqlite_db = $BRAIN_HOME/brains/<name>/brain.db
        let brain_data_dir = sqlite_db.parent().unwrap_or(std::path::Path::new("."));
        let brain_name = brain_data_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();
        let brain_home = brain_data_dir
            .parent() // brains/
            .and_then(|p| p.parent()) // $BRAIN_HOME
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));

        // Open the unified DB (~/.brain/brain.db) for tasks and records.
        // Falls back to the per-brain DB for pre-migration installations where
        // the unified DB does not yet exist.
        let unified_db_path = brain_home.join("brain.db");
        let unified_db = if unified_db_path.exists() {
            let path = unified_db_path.clone();
            tokio::task::spawn_blocking(move || Db::open(&path))
                .await
                .map_err(|e| {
                    crate::error::BrainCoreError::Database(format!("spawn_blocking unified: {e}"))
                })??
        } else {
            db.clone()
        };

        // Resolve brain_id from the global config registry so stores are
        // scoped to this brain in multi-brain workspaces.
        let brain_id = resolve_brain_entry(&brain_name)
            .and_then(|(name, entry)| crate::config::resolve_brain_id(&entry, &name))
            .unwrap_or_default();

        if !brain_id.is_empty() {
            // Backfill only the per-brain DB (for indexing tables).
            // The unified DB is already backfilled by the workspace migration.
            Self::backfill_brain_id(&db, &brain_id)?;
        }

        let tasks_dir = brain_data_dir.join("tasks");
        let tasks = if brain_id.is_empty() {
            TaskStore::new(&tasks_dir, unified_db.clone())?
        } else {
            TaskStore::with_brain_id(&tasks_dir, unified_db.clone(), &brain_id)?
        };

        let records_dir = brain_data_dir.join("records");
        let records = if brain_id.is_empty() {
            RecordStore::new(&records_dir, unified_db.clone())?
        } else {
            RecordStore::with_brain_id(&records_dir, unified_db.clone(), &brain_id)?
        };

        // Use unified ~/.brain/objects/ when it exists; fall back to per-brain
        // path for pre-migration installations.
        let unified_objects_dir = brain_home.join("objects");
        let objects_dir = if unified_objects_dir.exists() {
            unified_objects_dir
        } else {
            brain_data_dir.join("objects")
        };
        let objects = ObjectStore::new(&objects_dir)?;

        // Step 2: optionally load LanceDB + embedder. Failures are logged and
        // result in tasks-only mode — no hard error.
        let (writable_store, store, embedder) =
            match Self::try_load_search_layer(model_dir, lance_db, &db).await {
                Ok((ws, s, e)) => (Some(ws), Some(s), Some(e)),
                Err(err) => {
                    info!("embedding model unavailable ({err}), starting in tasks-only mode");
                    (None, None, None)
                }
            };

        let metrics = Arc::new(Metrics::new());

        Ok(Arc::new(Self {
            db,
            unified_db,
            store,
            writable_store,
            embedder,
            tasks,
            records,
            objects,
            metrics,
            brain_home,
            brain_name,
            brain_id,
        }))
    }

    /// Build an McpContext from pre-opened stores.
    ///
    /// Used by the daemon to create per-brain MCP contexts without going
    /// through the full bootstrap sequence (which opens its own stores).
    ///
    /// `db` is the per-brain SQLite (indexing tables).
    /// `unified_db` is `~/.brain/brain.db` (tasks/records).  Pass the same
    /// handle as `db` for pre-migration installations.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stores(
        db: Db,
        unified_db: Db,
        store: Option<StoreReader>,
        writable_store: Option<Store>,
        embedder: Option<Arc<dyn Embed>>,
        tasks: TaskStore,
        records: RecordStore,
        objects: ObjectStore,
        metrics: Arc<Metrics>,
        brain_home: std::path::PathBuf,
        brain_name: String,
    ) -> Arc<Self> {
        let brain_id = tasks.brain_id.clone();
        Arc::new(Self {
            db,
            unified_db,
            store,
            writable_store,
            embedder,
            tasks,
            records,
            objects,
            metrics,
            brain_home,
            brain_name,
            brain_id,
        })
    }

    /// Backfill brain_id on rows where brain_id = '' in tasks, records, and record_events.
    ///
    /// Called during startup to self-heal databases migrated before brain_id scoping
    /// was enforced. Safe to call multiple times (idempotent).
    ///
    /// Returns the total number of rows updated across all tables.
    pub fn backfill_brain_id(db: &Db, brain_id: &str) -> crate::error::Result<usize> {
        db.with_write_conn(|conn| {
            let tasks_updated = conn.execute(
                "UPDATE tasks SET brain_id = ?1 WHERE brain_id = ''",
                rusqlite::params![brain_id],
            )?;

            let records_updated = conn.execute(
                "UPDATE records SET brain_id = ?1 WHERE brain_id = ''",
                rusqlite::params![brain_id],
            )?;

            // record_events gained brain_id in v18; guard against older partial schemas.
            let has_record_events_brain_id: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('record_events') WHERE name = 'brain_id'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0)
                > 0;

            let record_events_updated = if has_record_events_brain_id {
                conn.execute(
                    "UPDATE record_events SET brain_id = ?1 WHERE brain_id = ''",
                    rusqlite::params![brain_id],
                )?
            } else {
                0
            };

            let total = tasks_updated + records_updated + record_events_updated;
            if total > 0 {
                info!(
                    brain_id,
                    tasks = tasks_updated,
                    records = records_updated,
                    record_events = record_events_updated,
                    "backfilled brain_id on legacy rows"
                );
            }
            Ok(total)
        })
    }

    /// Attempt to open the LanceDB store and load the embedder.
    ///
    /// Returns both as a pair on success. Any error causes the entire search
    /// layer to be skipped — we don't want a partially-loaded state.
    async fn try_load_search_layer(
        model_dir: &Path,
        lance_db: &Path,
        db: &Db,
    ) -> crate::error::Result<(Store, StoreReader, Arc<dyn Embed>)> {
        let mut store = Store::open_or_create(lance_db).await?;

        // Perform schema version check (same logic as IndexPipeline::new).
        crate::pipeline::ensure_schema_version(db, &mut store).await?;

        let embedder: Arc<dyn Embed> = {
            let model_dir = model_dir.to_path_buf();
            let e = tokio::task::spawn_blocking(move || Embedder::load(&model_dir))
                .await
                .map_err(|e| {
                    crate::error::BrainCoreError::Embedding(format!("spawn_blocking: {e}"))
                })??;
            Arc::new(e)
        };

        let store_reader = StoreReader::from_store(&store);
        Ok((store, store_reader, embedder))
    }

    /// Resolve a brain name or ID to a `(brain_name, brain_id)` pair.
    ///
    /// Looks up the global config registry to find the brain entry, then
    /// resolves its stable ID.
    pub fn resolve_brain_id(&self, name_or_id: &str) -> crate::error::Result<(String, String)> {
        let (name, entry) = resolve_brain_entry(name_or_id)?;
        let bid = crate::config::resolve_brain_id(&entry, &name)?;
        Ok((name, bid))
    }

    /// Create a brain_id-scoped TaskStore sharing this context's unified DB.
    pub fn tasks_for_brain(&self, brain_id: &str) -> crate::error::Result<TaskStore> {
        let tasks_dir = self.brain_home.join("tasks");
        TaskStore::with_brain_id(&tasks_dir, self.unified_db.clone(), brain_id)
    }

    /// Create a brain_id-scoped RecordStore sharing this context's unified DB.
    pub fn records_for_brain(&self, brain_id: &str) -> crate::error::Result<RecordStore> {
        let records_dir = self.brain_home.join("records");
        RecordStore::with_brain_id(&records_dir, self.unified_db.clone(), brain_id)
    }

    /// Clone this context with a different brain_id.
    ///
    /// All shared resources (Db, embedder, metrics, LanceDB store) are
    /// re-used. TaskStore and RecordStore are re-created scoped to
    /// `brain_id`. ObjectStore is re-opened at the same root path.
    pub fn with_brain_id(
        &self,
        brain_id: &str,
        brain_name: &str,
    ) -> crate::error::Result<Arc<Self>> {
        let tasks = self.tasks_for_brain(brain_id)?;
        let records = self.records_for_brain(brain_id)?;
        let objects = ObjectStore::new(self.objects.root())?;
        Ok(Arc::new(Self {
            db: self.db.clone(),
            unified_db: self.unified_db.clone(),
            store: self.store.clone(),
            writable_store: None, // shared read-only via IPC
            embedder: self.embedder.clone(),
            tasks,
            records,
            objects,
            metrics: Arc::clone(&self.metrics),
            brain_home: self.brain_home.clone(),
            brain_name: brain_name.to_string(),
            brain_id: brain_id.to_string(),
        }))
    }
}

/// Run the MCP server, reading JSON-RPC from stdin and writing to stdout.
///
/// All logging goes to stderr (stdout is reserved for MCP protocol).
/// Returns when stdin is closed.
///
/// If the brain daemon is running at the default socket path, tool calls are
/// routed through it via UDS. Otherwise falls back to direct store access.
pub async fn run_server(ctx: Arc<McpContext>) -> crate::error::Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    let registry = ToolRegistry::new();

    // Attempt to connect to the daemon. Fall back to local dispatch on failure.
    let dispatch_mode = {
        let sock = IpcClient::default_socket_path();
        // session_brain_name starts as the startup-resolved brain; updated on
        // initialize when the client provides roots pointing at a different brain.
        let session_brain_name = Arc::new(RwLock::new(ctx.brain_name.clone()));
        match IpcClient::connect(&sock).await {
            Ok(client) => {
                info!("connected to daemon via UDS, routing tool calls through daemon");
                DispatchMode::Daemon {
                    client: Mutex::new(client),
                    session_brain_name,
                    ctx,
                }
            }
            Err(_) => {
                info!("daemon not available, using direct store access");
                DispatchMode::Local {
                    ctx,
                    session_brain_name,
                }
            }
        }
    };

    info!("MCP server starting");

    while let Some(line) = lines
        .next_line()
        .await
        .map_err(crate::error::BrainCoreError::Io)?
    {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        debug!(line = %line, "received request");

        let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(req) => handle_request(req, &dispatch_mode, &registry).await,
            Err(e) => {
                error!(error = %e, "invalid JSON-RPC request");
                r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#
                    .to_string()
            }
        };

        if !response.is_empty() {
            stdout
                .write_all(response.as_bytes())
                .await
                .map_err(crate::error::BrainCoreError::Io)?;
            stdout
                .write_all(b"\n")
                .await
                .map_err(crate::error::BrainCoreError::Io)?;
            stdout
                .flush()
                .await
                .map_err(crate::error::BrainCoreError::Io)?;
        }
    }

    info!("MCP server shutting down (stdin closed)");
    Ok(())
}

/// Resolve the brain name from MCP `initialize` roots.
///
/// Parses each root URI (strips the `file://` prefix), then matches against
/// all registered brain roots in the global config. Returns the name of the
/// first matching brain, or `None` if no match is found.
fn resolve_brain_from_roots(roots: &Value) -> Option<String> {
    let roots_arr = roots.as_array()?;
    if roots_arr.is_empty() {
        return None;
    }

    // Collect candidate paths from root URIs.
    let root_paths: Vec<std::path::PathBuf> = roots_arr
        .iter()
        .filter_map(|r| r.get("uri").and_then(|u| u.as_str()))
        .map(|uri| {
            let path = uri.strip_prefix("file://").unwrap_or(uri);
            std::path::PathBuf::from(path)
        })
        .collect();

    if root_paths.is_empty() {
        return None;
    }

    let config = crate::config::load_global_config().ok()?;

    // For each registered brain, check whether any root_path starts with (or
    // equals) any of the brain's registered root paths.
    for (name, entry) in &config.brains {
        for brain_root in &entry.roots {
            for client_root in &root_paths {
                if client_root.starts_with(brain_root) || client_root == brain_root {
                    return Some(name.clone());
                }
            }
        }
    }

    None
}

/// Handle a single JSON-RPC request and return the serialized response.
async fn handle_request(
    req: JsonRpcRequest,
    mode: &DispatchMode,
    registry: &ToolRegistry,
) -> String {
    let id = req.id.clone();

    // Obtain the McpContext reference for non-dispatch paths (initialize, tools/list, metrics).
    let ctx = match mode {
        DispatchMode::Local { ctx, .. } => ctx,
        DispatchMode::Daemon { ctx, .. } => ctx,
    };

    match req.method.as_str() {
        "initialize" => {
            // Extract roots from initialize params and resolve session brain.
            let session_brain_name = match mode {
                DispatchMode::Local {
                    session_brain_name, ..
                } => session_brain_name,
                DispatchMode::Daemon {
                    session_brain_name, ..
                } => session_brain_name,
            };
            if let Some(roots) = req.params.get("roots")
                && let Some(resolved) = resolve_brain_from_roots(roots)
            {
                info!(brain = %resolved, "session brain resolved from initialize roots");
                *session_brain_name.write().await = resolved;
            }

            let result = InitializeResult {
                protocol_version: "2024-11-05".into(),
                capabilities: ServerCapabilities {
                    tools: ToolsCapability {},
                },
                server_info: ServerInfo {
                    name: "brain".into(),
                    version: env!("CARGO_PKG_VERSION").into(),
                },
            };

            serialize_response(&JsonRpcResponse::new(
                id,
                serde_json::to_value(result).unwrap(),
            ))
        }
        "notifications/initialized" => {
            // No response for notifications
            info!("MCP client initialized");
            String::new()
        }
        "tools/list" => {
            let result = ToolsListResult {
                tools: registry.definitions(),
            };
            serialize_response(&JsonRpcResponse::new(
                id,
                serde_json::to_value(result).unwrap(),
            ))
        }
        "tools/call" => {
            let tool_name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| {
                    warn!("MCP request missing tool name");
                    ""
                });
            let arguments = req
                .params
                .get("arguments")
                .cloned()
                .unwrap_or(Value::Object(serde_json::Map::new()));

            let call_start = std::time::Instant::now();

            let result = match mode {
                DispatchMode::Daemon {
                    client,
                    session_brain_name,
                    ..
                } => {
                    // Forward to daemon via IPC using the per-session brain name.
                    // On connection-level errors, attempt a single reconnect
                    // (handles daemon restart).
                    let brain_name = session_brain_name.read().await.clone();
                    let mut guard = client.lock().await;
                    let args_backup = arguments.clone();
                    match guard.tools_call(tool_name, &brain_name, arguments).await {
                        Ok(value) => {
                            return serialize_response(&JsonRpcResponse::new(id, value));
                        }
                        Err(e) => {
                            use crate::ipc::client::IpcClientError;
                            let is_connection_error = matches!(
                                &e,
                                IpcClientError::Io(_)
                                    | IpcClientError::Protocol(_)
                                    | IpcClientError::DaemonUnavailable { .. }
                            );
                            if is_connection_error {
                                warn!(error = %e, "IPC connection lost, attempting reconnect");
                                match IpcClient::connect(&IpcClient::default_socket_path()).await {
                                    Ok(new_client) => {
                                        *guard = new_client;
                                        match guard
                                            .tools_call(tool_name, &brain_name, args_backup)
                                            .await
                                        {
                                            Ok(value) => {
                                                info!("IPC reconnect succeeded");
                                                return serialize_response(&JsonRpcResponse::new(
                                                    id, value,
                                                ));
                                            }
                                            Err(retry_err) => {
                                                error!(error = %retry_err, tool = tool_name, "IPC retry after reconnect failed");
                                                protocol::ToolCallResult::error(format!(
                                                    "daemon error after reconnect: {retry_err}"
                                                ))
                                            }
                                        }
                                    }
                                    Err(reconnect_err) => {
                                        error!(error = %reconnect_err, "IPC reconnect failed");
                                        protocol::ToolCallResult::error(format!(
                                            "daemon error: {e} (reconnect failed: {reconnect_err})"
                                        ))
                                    }
                                }
                            } else {
                                error!(error = %e, tool = tool_name, "IPC dispatch failed");
                                protocol::ToolCallResult::error(format!("daemon error: {e}"))
                            }
                        }
                    }
                }
                DispatchMode::Local {
                    session_brain_name, ..
                } => {
                    // For local dispatch, warn if the session brain differs from
                    // the startup brain. A full re-bootstrap is expensive; the
                    // unified DB fix (brain_id scoping) ensures correct data
                    // access for the common case. Re-bootstrap is a follow-up.
                    let session = session_brain_name.read().await.clone();
                    if session != ctx.brain_name {
                        warn!(
                            session_brain = %session,
                            startup_brain = %ctx.brain_name,
                            "session brain differs from startup brain in local dispatch mode; \
                             data access uses startup-brain context"
                        );
                    }
                    registry.dispatch(tool_name, arguments, ctx).await
                }
            };

            if matches!(
                tool_name,
                "memory.search_minimal" | "memory.expand" | "memory.reflect"
            ) {
                ctx.metrics.record_query_latency(call_start.elapsed());
            }
            serialize_response(&JsonRpcResponse::new(
                id,
                serde_json::to_value(result).unwrap(),
            ))
        }
        _ => serialize_error(&JsonRpcError::method_not_found(id, &req.method)),
    }
}

fn serialize_response(resp: &JsonRpcResponse) -> String {
    serde_json::to_string(resp).unwrap_or_else(|e| {
        error!("Failed to serialize MCP response: {e}");
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Internal: response serialization failed"}}"#.to_string()
    })
}

fn serialize_error(err: &JsonRpcError) -> String {
    serde_json::to_string(err).unwrap_or_else(|e| {
        error!("Failed to serialize MCP error: {e}");
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Internal: error serialization failed"}}"#.to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn call(method: &str, params: Value) -> String {
        let (_dir, ctx) = tools::tests::create_test_context().await;
        let registry = ToolRegistry::new();
        let session_brain_name = Arc::new(RwLock::new(ctx.brain_name.clone()));
        let mode = DispatchMode::Local {
            ctx: Arc::new(ctx),
            session_brain_name,
        };
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(1)),
            method: method.into(),
            params,
        };
        handle_request(req, &mode, &registry).await
    }

    #[tokio::test]
    async fn test_initialize() {
        let resp = call("initialize", json!({})).await;
        let parsed: Value = serde_json::from_str(&resp).unwrap();

        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(parsed["result"]["serverInfo"]["name"], "brain");
        assert!(parsed["result"]["capabilities"]["tools"].is_object());
    }

    #[tokio::test]
    async fn test_tools_list() {
        let resp = call("tools/list", json!({})).await;
        let parsed: Value = serde_json::from_str(&resp).unwrap();

        let tools = parsed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 25);

        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"memory.search_minimal"));
        assert!(names.contains(&"memory.expand"));
        assert!(names.contains(&"tasks.apply_event"));
        assert!(names.contains(&"tasks.create"));
        assert!(names.contains(&"tasks.labels_batch"));
        assert!(names.contains(&"tasks.deps_batch"));
        assert!(names.contains(&"tasks.get"));
        assert!(names.contains(&"tasks.list"));
        assert!(names.contains(&"tasks.next"));
        assert!(!names.contains(&"tasks.create_remote"));
    }

    #[tokio::test]
    async fn test_method_not_found() {
        let resp = call("unknown/method", json!({})).await;
        let parsed: Value = serde_json::from_str(&resp).unwrap();

        assert!(parsed["error"].is_object());
        assert_eq!(parsed["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn test_notification_no_response() {
        let resp = call("notifications/initialized", json!({})).await;
        assert!(resp.is_empty());
    }

    #[test]
    fn test_backfill_brain_id_stamps_empty_rows() {
        let db = Db::open_in_memory().expect("open_in_memory");

        // Insert rows with empty brain_id into tasks, records, and record_events.
        db.with_write_conn(|conn| {
            conn.execute_batch(
                "INSERT INTO tasks (task_id, title, status, priority, task_type, created_at, updated_at)
                 VALUES ('t1', 'Task One', 'open', 4, 'task', 0, 0);
                 INSERT INTO records (record_id, title, kind, content_hash, content_size, actor, created_at, updated_at)
                 VALUES ('r1', 'Rec One', 'snapshot', 'abc', 10, 'agent', 0, 0);
                 INSERT INTO record_events (event_id, record_id, event_type, timestamp, actor, payload)
                 VALUES ('e1', 'r1', 'created', 0, 'agent', '{}');"
            ).map_err(Into::into)
        })
        .expect("seed data");

        let total = McpContext::backfill_brain_id(&db, "brain-abc").expect("backfill");
        assert_eq!(
            total, 3,
            "expected 3 rows updated (tasks + records + record_events)"
        );

        db.with_write_conn(|conn| {
            let t_id: String = conn
                .query_row("SELECT brain_id FROM tasks WHERE task_id = 't1'", [], |r| {
                    r.get(0)
                })
                .map_err(crate::error::BrainCoreError::from)?;
            assert_eq!(t_id, "brain-abc");

            let r_id: String = conn
                .query_row(
                    "SELECT brain_id FROM records WHERE record_id = 'r1'",
                    [],
                    |r| r.get(0),
                )
                .map_err(crate::error::BrainCoreError::from)?;
            assert_eq!(r_id, "brain-abc");

            let e_id: String = conn
                .query_row(
                    "SELECT brain_id FROM record_events WHERE event_id = 'e1'",
                    [],
                    |r| r.get(0),
                )
                .map_err(crate::error::BrainCoreError::from)?;
            assert_eq!(e_id, "brain-abc");

            Ok(())
        })
        .expect("verify");
    }

    #[test]
    fn test_backfill_brain_id_idempotent() {
        let db = Db::open_in_memory().expect("open_in_memory");

        db.with_write_conn(|conn| {
            conn.execute_batch(
                "INSERT INTO tasks (task_id, title, status, priority, task_type, created_at, updated_at)
                 VALUES ('t1', 'Task One', 'open', 4, 'task', 0, 0);"
            ).map_err(Into::into)
        })
        .expect("seed data");

        let first = McpContext::backfill_brain_id(&db, "brain-xyz").expect("first backfill");
        assert_eq!(first, 1);

        let second = McpContext::backfill_brain_id(&db, "brain-xyz").expect("second backfill");
        assert_eq!(
            second, 0,
            "second call must update 0 rows (already stamped)"
        );
    }

    #[test]
    fn test_backfill_brain_id_skips_already_stamped_rows() {
        let db = Db::open_in_memory().expect("open_in_memory");

        db.with_write_conn(|conn| {
            conn.execute_batch(
                "INSERT INTO tasks (task_id, title, status, priority, task_type, created_at, updated_at, brain_id)
                 VALUES ('t1', 'Task One', 'open', 4, 'task', 0, 0, 'other-brain');
                 INSERT INTO tasks (task_id, title, status, priority, task_type, created_at, updated_at)
                 VALUES ('t2', 'Task Two', 'open', 4, 'task', 0, 0);"
            ).map_err(Into::into)
        })
        .expect("seed data");

        let total = McpContext::backfill_brain_id(&db, "brain-abc").expect("backfill");
        assert_eq!(total, 1, "only the empty-brain_id row should be updated");

        db.with_write_conn(|conn| {
            let t1_id: String = conn
                .query_row("SELECT brain_id FROM tasks WHERE task_id = 't1'", [], |r| {
                    r.get(0)
                })
                .map_err(crate::error::BrainCoreError::from)?;
            assert_eq!(
                t1_id, "other-brain",
                "pre-stamped row must not be overwritten"
            );

            let t2_id: String = conn
                .query_row("SELECT brain_id FROM tasks WHERE task_id = 't2'", [], |r| {
                    r.get(0)
                })
                .map_err(crate::error::BrainCoreError::from)?;
            assert_eq!(t2_id, "brain-abc");

            Ok(())
        })
        .expect("verify");
    }

    // --- resolve_brain_from_roots ---

    #[test]
    fn test_resolve_brain_from_roots_empty_array() {
        let roots = json!([]);
        assert_eq!(resolve_brain_from_roots(&roots), None);
    }

    #[test]
    fn test_resolve_brain_from_roots_null() {
        let roots = json!(null);
        assert_eq!(resolve_brain_from_roots(&roots), None);
    }

    #[test]
    fn test_resolve_brain_from_roots_no_uri_field() {
        let roots = json!([{"path": "/some/path"}]);
        // No "uri" field — should produce no candidate paths, so None.
        assert_eq!(resolve_brain_from_roots(&roots), None);
    }

    #[test]
    fn test_resolve_brain_from_roots_non_file_uri_ignored_gracefully() {
        // Even if a non-file URI is present, we should not panic.
        let roots = json!([{"uri": "https://example.com/project"}]);
        // No registered brains match this path, so None (may match depending
        // on local config, but we only assert it does not panic).
        let _ = resolve_brain_from_roots(&roots);
    }

    // --- initialize root extraction ---

    #[tokio::test]
    async fn test_initialize_without_roots_returns_ok() {
        // initialize with no roots field — fallback brain is used, no panic.
        let resp = call("initialize", json!({})).await;
        let parsed: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(parsed["result"]["protocolVersion"], "2024-11-05");
    }

    #[tokio::test]
    async fn test_initialize_with_empty_roots_returns_ok() {
        let resp = call("initialize", json!({"roots": []})).await;
        let parsed: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(parsed["result"]["protocolVersion"], "2024-11-05");
    }

    #[tokio::test]
    async fn test_initialize_with_unmatched_roots_returns_ok() {
        // Roots that do not match any registered brain — fallback is used.
        let resp = call(
            "initialize",
            json!({"roots": [{"uri": "file:///no/such/project"}]}),
        )
        .await;
        let parsed: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(parsed["result"]["protocolVersion"], "2024-11-05");
    }
}
