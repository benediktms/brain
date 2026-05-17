//! Real `Request` handlers backed by [`brain_lib::stores::BrainStores`].
//!
//! # Hexagonal role
//!
//! `BrainStoresDispatcher` is the **anti-corruption layer** between
//! the wire format and the internal storage. The trait it implements
//! ([`crate::dispatcher::Dispatcher`]) is framework-free; the impl
//! here is where wire types translate to / from internal domain types.
//!
//! Specifically: [`brain_rpc::TaskSummary`] is *defined locally* in
//! brain_rpc, and its fields are produced by mapping
//! [`brain_tasks::Task`] field-by-field. If `brain_tasks::Task` adds
//! a new field tomorrow, this file is where the decision is made
//! about whether to expose it on the wire.

use std::sync::{Arc, OnceLock};

use brain_lib::stores::BrainStores;
use brain_persistence::db::sagas::compact_saga_id;
use brain_persistence::db::summaries::Episode;
use brain_records::{
    CreateRecordParams, Record, RecordKind, RecordQuery, RecordStatus, RecordStore, integrity,
};
use brain_rpc::{
    AnalysisSummary, ArtifactSummary, ArtifactsListParams, BrainStatusReport, DocumentSummary,
    JobSummary, JobsStatusReport, MemoryConsolidateParams, MemoryReflectParams,
    MemoryRetrieveParams, MemorySummarizeScopeParams, MemoryWalkThreadParams,
    MemoryWriteEpisodeParams, MemoryWriteProcedureParams, PROTOCOL_VERSION, PlanSummary,
    ProviderSummary, RecordsCreateParams, RecordsListParams, RecordsVerifyReport, Request,
    Response, RpcError, SagaBrainSummary, SagaCascadeOutcome, SagaCascadeResult,
    SagaDescriptionUpdate, SagaFrontierTask, SagaLabelCount, SagaStatsReport, SagaSummary,
    SagasCreateParams, SagasListParams, SagasUpdateParams, SnapshotSummary, TagAliasSummary,
    TagAliasesStatusReport, TagsAliasesListParams, TagsReclusterParams, TaskSummary,
    TasksCreateParams, TasksDepsBatchParams, TasksLabelsBatchParams, TasksListParams,
    TasksMutateParams, TasksTransferParams, TasksUpdateParams,
};
use brain_sagas::{
    BrainSummary as SagaBrainDomain, CascadeOutcome, CascadeResult, LabelCount, Saga,
    SagaListFilter, SagaStats,
};
use brain_tasks::events::{
    DependencyPayload, EventType, LabelPayload, StatusChangedPayload, TaskCreatedPayload,
    TaskEvent, TaskStatus, TaskType, TaskUpdatedPayload,
};
use brain_tasks::{Task, TaskStore};
use chrono::{DateTime, Utc};

use crate::dispatcher::Dispatcher;

/// Production [`Dispatcher`] that handles real `Request` variants by
/// consulting a [`BrainStores`] instance.
///
/// Hold a `BrainStores`, share it across worker threads via `&self`.
/// `BrainStores` itself wraps `Db` (which is `Send + Sync`) so the
/// `D: Dispatcher + Send + Sync + 'static` bound on
/// [`crate::UnixSocketServer`] is satisfied.
pub struct BrainStoresDispatcher {
    stores: BrainStores,
    /// Tokio current-thread runtime for memory handlers that drive the
    /// async `brain_memory::retrieve` / `reflect` pipelines (async
    /// LanceDB + embedder). Created lazily on first use to keep daemon
    /// startup fast for callers that never touch semantic memory.
    runtime: OnceLock<tokio::runtime::Runtime>,
    /// Bootstrapped semantic-memory resource holder (BrainStores +
    /// SearchService + Metrics). Memory handlers borrow this into a
    /// `brain_memory::context::SemanticContext` for each call. Built
    /// lazily so a daemon launched without the embedder model on disk
    /// still answers everything else. Keeps its historical
    /// `McpContext` name until brain_lib::mcp leaves in PR2.
    mcp_ctx: OnceLock<Arc<brain_lib::mcp::McpContext>>,
    /// Handle to the file-watcher supervisor. `None` when the daemon was
    /// started without a supervisor (e.g. no `--watch-dir` flag). Used
    /// by the `WatchAdd` / `WatchRemove` / `WatchList` request handlers
    /// to send `ControlMessage`s over the mpsc channel.
    #[cfg(feature = "embed")]
    watcher: Option<Arc<crate::watcher::WatcherHandle>>,
}

impl BrainStoresDispatcher {
    pub fn new(
        stores: BrainStores,
        #[cfg(feature = "embed")] watcher: Option<Arc<crate::watcher::WatcherHandle>>,
    ) -> Self {
        Self {
            stores,
            runtime: OnceLock::new(),
            mcp_ctx: OnceLock::new(),
            #[cfg(feature = "embed")]
            watcher,
        }
    }

    /// Lazily build (or return) a tokio current-thread runtime. Used by
    /// memory handlers that need to `block_on` an async tool call.
    fn runtime(&self) -> Result<&tokio::runtime::Runtime, RpcError> {
        if let Some(rt) = self.runtime.get() {
            return Ok(rt);
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| RpcError::Unknown {
                message: format!("create tokio runtime: {e}"),
            })?;
        // OnceLock::set returns Err if another writer raced us — that's
        // fine, we just discard our build and use the winner's runtime.
        let _ = self.runtime.set(rt);
        Ok(self
            .runtime
            .get()
            .expect("runtime initialised above (or by racing writer)"))
    }

    /// Lazily build (or return) the shared `McpContext` for semantic
    /// memory tools. Returns `RpcError::Unknown` if the embedder /
    /// LanceDB cannot be loaded — callers (retrieve / reflect) surface
    /// the error verbatim to the wire.
    fn mcp_ctx(&self) -> Result<Arc<brain_lib::mcp::McpContext>, RpcError> {
        if let Some(ctx) = self.mcp_ctx.get() {
            return Ok(Arc::clone(ctx));
        }
        let brain_home = self.stores.brain_home.clone();
        let model_dir = brain_home.join("models").join("bge-small-en-v1.5");
        let lance_db = brain_home.join("lancedb");
        let sqlite_db = brain_home.join("brain.db");
        let ctx = self
            .runtime()?
            .block_on(async {
                brain_lib::mcp::McpContext::bootstrap(
                    &model_dir,
                    &lance_db,
                    &sqlite_db,
                    Some(self.stores.brain_name.as_str()),
                )
                .await
            })
            .map_err(|e| RpcError::Unknown {
                message: format!("bootstrap McpContext for memory tools: {e}"),
            })?;
        let _ = self.mcp_ctx.set(Arc::clone(&ctx));
        Ok(self
            .mcp_ctx
            .get()
            .map(Arc::clone)
            .expect("mcp_ctx initialised above (or by racing writer)"))
    }

    fn handle_tasks_list(&self, params: TasksListParams) -> Result<Response, RpcError> {
        // Status filter picks the right base query. Unknown values
        // surface as a Protocol error so the caller fixes the input,
        // not the daemon.
        let tasks: Vec<Task> = match params.status.as_deref() {
            None => self.stores.tasks.list_all(),
            Some("open") => self.stores.tasks.list_open(),
            Some("in_progress") => self.stores.tasks.list_in_progress(),
            Some("blocked") => self.stores.tasks.list_blocked(),
            Some("done") => self.stores.tasks.list_done(),
            Some("cancelled") => self.stores.tasks.list_cancelled(),
            Some(other) => {
                return Err(RpcError::Protocol {
                    message: format!(
                        "unknown status filter: {other:?} (expected open|in_progress|blocked|done|cancelled)"
                    ),
                });
            }
        }
        .map_err(|e| RpcError::Unknown {
            message: format!("list tasks: {e}"),
        })?;

        // Remaining filters (priority, search) and limit happen
        // in-memory because the brain_tasks TaskStore doesn't expose
        // a combined filter API yet. Acceptable for MVP — the daemon
        // is local and task lists are small.
        let summaries: Vec<TaskSummary> = tasks
            .into_iter()
            .filter(|t| {
                params
                    .priority
                    .map(|want| t.priority.as_i32() == i32::from(want))
                    .unwrap_or(true)
            })
            .filter(|t| match params.search.as_deref() {
                Some(needle) => {
                    let needle = needle.to_lowercase();
                    t.title.to_lowercase().contains(&needle)
                        || t.description
                            .as_deref()
                            .map(|d| d.to_lowercase().contains(&needle))
                            .unwrap_or(false)
                }
                None => true,
            })
            .take(params.limit.map(|n| n as usize).unwrap_or(usize::MAX))
            .map(|t| self.task_to_summary(&t))
            .collect();

        Ok(Response::TasksList { tasks: summaries })
    }

    fn handle_tasks_show(&self, id: String) -> Result<Response, RpcError> {
        // Best-effort prefix resolution. A bad / unknown id is treated as
        // "not found" (Option::None) rather than a Protocol error — the
        // wire-shape contract for TasksShow is "None when absent".
        let resolved = match self.stores.tasks.resolve_task_id(&id) {
            Ok(r) => r,
            Err(_) => return Ok(Response::TasksShow { task: None }),
        };
        let task = self
            .stores
            .tasks
            .get_task(&resolved)
            .map_err(|e| RpcError::Unknown {
                message: format!("get task: {e}"),
            })?;
        Ok(Response::TasksShow {
            task: task.as_ref().map(|t| self.task_to_summary(t)),
        })
    }

    fn handle_tasks_next(&self) -> Result<Response, RpcError> {
        let mut tasks =
            self.stores
                .tasks
                .list_ready_actionable()
                .map_err(|e| RpcError::Unknown {
                    message: format!("list ready actionable: {e}"),
                })?;

        // Same sort order as `brain tasks next`: in-progress first,
        // then priority ascending (0=critical), then earliest due_at.
        let status_ord =
            |s: &TaskStatus| -> u8 { if *s == TaskStatus::InProgress { 0 } else { 1 } };
        tasks.sort_by(|a, b| {
            status_ord(&a.status)
                .cmp(&status_ord(&b.status))
                .then(a.priority.cmp(&b.priority))
                .then_with(|| match (a.due_at, b.due_at) {
                    (Some(a_ts), Some(b_ts)) => a_ts.cmp(&b_ts),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                })
                .then(a.id.as_str().cmp(b.id.as_str()))
        });

        Ok(Response::TasksNext {
            task: tasks.first().map(|t| self.task_to_summary(t)),
        })
    }

    fn handle_tasks_create(&self, params: TasksCreateParams) -> Result<Response, RpcError> {
        if params.priority > 4 {
            return Err(RpcError::Protocol {
                message: format!("invalid priority: {} (must be 0..=4)", params.priority),
            });
        }

        let task_type: TaskType =
            params
                .task_type
                .parse()
                .map_err(|e: String| RpcError::Protocol {
                    message: format!("invalid task_type: {e}"),
                })?;

        let prefix = self
            .stores
            .tasks
            .get_project_prefix()
            .map_err(|e| RpcError::Unknown {
                message: format!("get project prefix: {e}"),
            })?;
        let task_id = brain_tasks::events::new_task_id(&prefix);

        // Resolve parent if provided.
        let parent = match params.parent.as_deref() {
            Some(p) => {
                Some(
                    self.stores
                        .tasks
                        .resolve_task_id(p)
                        .map_err(|e| RpcError::Protocol {
                            message: format!("resolve parent task id: {e}"),
                        })?,
                )
            }
            None => None,
        };

        let event = TaskEvent::from_payload(
            &task_id,
            "daemon",
            TaskCreatedPayload {
                title: params.title,
                description: params.description,
                priority: i32::from(params.priority),
                status: TaskStatus::Open,
                due_ts: None,
                task_type: Some(task_type),
                assignee: params.assignee,
                defer_until: None,
                parent_task_id: parent,
                display_id: None,
            },
        );
        let event_id = event.event_id.clone();

        self.stores
            .tasks
            .append(&event)
            .map_err(|e| RpcError::Unknown {
                message: format!("append create event: {e}"),
            })?;

        let task = self
            .stores
            .tasks
            .get_task(&task_id)
            .map_err(|e| RpcError::Unknown {
                message: format!("refetch created task: {e}"),
            })?
            .ok_or_else(|| RpcError::Unknown {
                message: format!("task vanished after create: {task_id}"),
            })?;

        Ok(Response::TasksCreate {
            task: self.task_to_summary(&task),
            event_id,
        })
    }

    fn handle_tasks_update(&self, params: TasksUpdateParams) -> Result<Response, RpcError> {
        if params.priority.is_some_and(|p| p > 4) {
            return Err(RpcError::Protocol {
                message: format!(
                    "invalid priority: {} (must be 0..=4)",
                    params.priority.unwrap()
                ),
            });
        }

        let resolved =
            self.stores
                .tasks
                .resolve_task_id(&params.id)
                .map_err(|_| RpcError::NotFound {
                    id: params.id.clone(),
                })?;

        let event = TaskEvent::from_payload(
            &resolved,
            "daemon",
            TaskUpdatedPayload {
                title: params.title,
                description: params.description,
                priority: params.priority.map(i32::from),
                due_ts: None,
                blocked_reason: None,
                task_type: None,
                assignee: params.assignee,
                defer_until: None,
            },
        );
        let event_id = event.event_id.clone();

        self.stores
            .tasks
            .append(&event)
            .map_err(|e| RpcError::Unknown {
                message: format!("append update event: {e}"),
            })?;

        let task = self
            .stores
            .tasks
            .get_task(&resolved)
            .map_err(|e| RpcError::Unknown {
                message: format!("refetch updated task: {e}"),
            })?
            .ok_or(RpcError::NotFound { id: resolved })?;

        Ok(Response::TasksUpdate {
            task: self.task_to_summary(&task),
            event_id,
        })
    }

    fn handle_tasks_mutate(&self, params: TasksMutateParams) -> Result<Response, RpcError> {
        let new_status = match params.action.as_str() {
            "close" => TaskStatus::Done,
            "open" => TaskStatus::Open,
            "block" => TaskStatus::Blocked,
            "in_progress" => TaskStatus::InProgress,
            "cancel" => TaskStatus::Cancelled,
            other => {
                return Err(RpcError::Protocol {
                    message: format!(
                        "unknown mutate action: {other:?} (expected close|open|block|in_progress|cancel)"
                    ),
                });
            }
        };

        let resolved =
            self.stores
                .tasks
                .resolve_task_id(&params.id)
                .map_err(|_| RpcError::NotFound {
                    id: params.id.clone(),
                })?;

        let event =
            TaskEvent::from_payload(&resolved, "daemon", StatusChangedPayload { new_status });
        let event_id = event.event_id.clone();

        self.stores
            .tasks
            .append(&event)
            .map_err(|e| RpcError::Unknown {
                message: format!("append status event: {e}"),
            })?;

        let task = self
            .stores
            .tasks
            .get_task(&resolved)
            .map_err(|e| RpcError::Unknown {
                message: format!("refetch mutated task: {e}"),
            })?
            .ok_or(RpcError::NotFound { id: resolved })?;

        Ok(Response::TasksMutate {
            task: self.task_to_summary(&task),
            event_id,
        })
    }

    fn handle_tasks_add_dep(
        &self,
        task_id: String,
        depends_on_task_id: String,
    ) -> Result<Response, RpcError> {
        let task_resolved =
            self.stores
                .tasks
                .resolve_task_id(&task_id)
                .map_err(|_| RpcError::NotFound {
                    id: task_id.clone(),
                })?;
        let dep_resolved = self
            .stores
            .tasks
            .resolve_task_id(&depends_on_task_id)
            .map_err(|_| RpcError::NotFound {
                id: depends_on_task_id.clone(),
            })?;

        let event = TaskEvent::new(
            &task_resolved,
            "daemon",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: dep_resolved,
            },
        );
        let event_id = event.event_id.clone();

        self.stores
            .tasks
            .append(&event)
            .map_err(|e| RpcError::Unknown {
                message: format!("append dep_added event: {e}"),
            })?;

        Ok(Response::TasksDepAdded { event_id })
    }

    fn handle_tasks_remove_dep(
        &self,
        task_id: String,
        depends_on_task_id: String,
    ) -> Result<Response, RpcError> {
        let task_resolved =
            self.stores
                .tasks
                .resolve_task_id(&task_id)
                .map_err(|_| RpcError::NotFound {
                    id: task_id.clone(),
                })?;
        let dep_resolved = self
            .stores
            .tasks
            .resolve_task_id(&depends_on_task_id)
            .map_err(|_| RpcError::NotFound {
                id: depends_on_task_id.clone(),
            })?;

        let event = TaskEvent::new(
            &task_resolved,
            "daemon",
            EventType::DependencyRemoved,
            &DependencyPayload {
                depends_on_task_id: dep_resolved,
            },
        );
        let event_id = event.event_id.clone();

        self.stores
            .tasks
            .append(&event)
            .map_err(|e| RpcError::Unknown {
                message: format!("append dep_removed event: {e}"),
            })?;

        Ok(Response::TasksDepRemoved { event_id })
    }

    fn handle_tasks_add_label(&self, task_id: String, label: String) -> Result<Response, RpcError> {
        let resolved =
            self.stores
                .tasks
                .resolve_task_id(&task_id)
                .map_err(|_| RpcError::NotFound {
                    id: task_id.clone(),
                })?;

        let event = TaskEvent::new(
            &resolved,
            "daemon",
            EventType::LabelAdded,
            &LabelPayload { label },
        );
        let event_id = event.event_id.clone();

        self.stores
            .tasks
            .append(&event)
            .map_err(|e| RpcError::Unknown {
                message: format!("append label_added event: {e}"),
            })?;

        Ok(Response::TasksLabelAdded { event_id })
    }

    fn handle_tasks_remove_label(
        &self,
        task_id: String,
        label: String,
    ) -> Result<Response, RpcError> {
        let resolved =
            self.stores
                .tasks
                .resolve_task_id(&task_id)
                .map_err(|_| RpcError::NotFound {
                    id: task_id.clone(),
                })?;

        let event = TaskEvent::new(
            &resolved,
            "daemon",
            EventType::LabelRemoved,
            &LabelPayload { label },
        );
        let event_id = event.event_id.clone();

        self.stores
            .tasks
            .append(&event)
            .map_err(|e| RpcError::Unknown {
                message: format!("append label_removed event: {e}"),
            })?;

        Ok(Response::TasksLabelRemoved { event_id })
    }

    fn handle_tasks_transfer(&self, params: TasksTransferParams) -> Result<Response, RpcError> {
        let resolved = self
            .stores
            .tasks
            .resolve_task_id(&params.task_id)
            .map_err(|_| RpcError::NotFound {
                id: params.task_id.clone(),
            })?;

        let (target_brain_id, target_brain_name) = self
            .stores
            .tasks
            .resolve_brain(&params.target_brain)
            .map_err(|e| RpcError::Protocol {
                message: format!("resolve target brain: {e}"),
            })?;

        // TaskStore::transfer_task is `async fn` only because the Lance
        // re-stamp branch (taken when `vector_store: Some(_)`) calls an
        // async store API. The MVP daemon passes `None`, so the returned
        // future is non-yielding — driving it once with a no-op waker
        // completes it without needing a tokio runtime. The brain_daemon
        // crate does not depend on tokio; this keeps it that way.
        let result = block_on_no_yield(self.stores.tasks.transfer_task(
            &resolved,
            &target_brain_id,
            None,
        ))
        .map_err(|e| RpcError::Unknown {
            message: format!("transfer task: {e}"),
        })?;

        // Re-fetch from a store scoped to the target brain. Required
        // because `self.stores.tasks` is scoped to the daemon's local
        // brain and a successful transfer moves the task OUT of that
        // scope.
        let target_store = self
            .stores
            .tasks
            .with_remote_brain_id(&target_brain_id, &target_brain_name)
            .map_err(|e| RpcError::Unknown {
                message: format!("open target brain store: {e}"),
            })?;
        let task = target_store
            .get_task(&resolved)
            .map_err(|e| RpcError::Unknown {
                message: format!("refetch transferred task: {e}"),
            })?
            .ok_or_else(|| RpcError::Unknown {
                message: format!("task vanished after transfer: {resolved}"),
            })?;

        // The summary's brain_id field must reflect the post-transfer
        // brain — `task_to_summary` reads `self.stores.brain_id` (the
        // daemon's local scope), so build the summary inline here.
        let summary = TaskSummary {
            task_id: task
                .display_id
                .clone()
                .unwrap_or_else(|| task.id.as_str().to_string()),
            title: task.title.clone(),
            status: status_to_wire_string(&task.status),
            priority: task.priority.as_i32().clamp(0, u8::MAX as i32) as u8,
            brain_id: result.to_brain_id.clone(),
        };

        // `transfer_task_inner` inserts the task_transferred event row
        // internally and does not return the row's event_id. Surface a
        // correlation key derived from the stable display_id pair so
        // callers can locate the matching row in the event log; this is
        // explicit in the wire contract for transfer (see brain_rpc
        // module rustdoc) rather than a synthesized "row id".
        let event_id = format!(
            "transfer:{}->{}",
            result.from_display_id, result.to_display_id
        );

        Ok(Response::TasksTransfer {
            task: summary,
            event_id,
        })
    }

    /// Map an internal [`Task`] into the wire-format [`TaskSummary`].
    /// This is the anti-corruption-layer translation point — if the
    /// internal type gains fields, this function decides whether to
    /// expose them on the wire.
    fn task_to_summary(&self, task: &Task) -> TaskSummary {
        TaskSummary {
            task_id: task
                .display_id
                .clone()
                .unwrap_or_else(|| task.id.as_str().to_string()),
            title: task.title.clone(),
            status: status_to_wire_string(&task.status),
            priority: task.priority.as_i32().clamp(0, u8::MAX as i32) as u8,
            brain_id: self.stores.brain_id.clone(),
        }
    }

    // ── records / sub-record handlers ──────────────────────────

    fn handle_records_verify(&self) -> Result<Response, RpcError> {
        let report = integrity::verify_integrity(&self.stores.records, &self.stores.objects)
            .map_err(|e| RpcError::Unknown {
                message: format!("verify integrity: {e}"),
            })?;
        Ok(Response::RecordsVerify {
            report: record_to_verify_report(&report),
        })
    }

    fn list_records_with_kind(
        &self,
        kind: &str,
        params: &RecordsListParams,
    ) -> Result<Vec<Record>, RpcError> {
        let status = parse_status_filter(params.status.as_deref())?;
        let query = RecordQuery {
            kind: Some(RecordKind::from(kind)),
            status: Some(status),
            tag: params.tag.clone(),
            task_id: params.task_id.clone(),
            limit: params.limit.map(|n| n as usize),
        };
        self.stores
            .records
            .list_records(&query)
            .map_err(|e| RpcError::Unknown {
                message: format!("list {kind} records: {e}"),
            })
    }

    fn handle_analyses_list(&self, params: RecordsListParams) -> Result<Response, RpcError> {
        let records = self.list_records_with_kind("analysis", &params)?;
        let records = records
            .iter()
            .map(|r| self.analysis_to_summary(r))
            .collect();
        Ok(Response::AnalysesList { records })
    }

    fn handle_analyses_show(&self, id: String) -> Result<Response, RpcError> {
        let record = self.lookup_record_of_kind(&id, Some("analysis"))?;
        Ok(Response::AnalysesShow {
            record: record.as_ref().map(|r| self.analysis_to_summary(r)),
        })
    }

    fn handle_analyses_create(&self, params: RecordsCreateParams) -> Result<Response, RpcError> {
        let brain = params.brain.clone();
        let resolved_store = self.scoped_record_store(brain.as_deref())?;
        let target_records: &RecordStore = resolved_store
            .as_ref()
            .map(|(rs, _)| rs)
            .unwrap_or(&self.stores.records);
        let brain_id: String = resolved_store
            .as_ref()
            .map(|(_, bid)| bid.clone())
            .unwrap_or_else(|| self.stores.brain_id.clone());
        let create_params = records_create_params_to_internal(params);
        let record = target_records
            .create_analysis(create_params, &self.stores.objects)
            .map_err(|e| RpcError::Unknown {
                message: format!("create analysis: {e}"),
            })?;
        let summary = analysis_to_summary_with_brain(&record, &brain_id);
        let content_hash = record.content_ref.hash.clone();
        let size = record.content_ref.size;
        Ok(Response::AnalysesCreate {
            record: summary,
            content_hash,
            size,
        })
    }

    fn handle_artifacts_list(&self, params: ArtifactsListParams) -> Result<Response, RpcError> {
        let status = parse_status_filter(params.status.as_deref())?;
        let query = RecordQuery {
            kind: params.kind.as_deref().map(RecordKind::from),
            status: Some(status),
            tag: params.tag.clone(),
            task_id: None,
            limit: params.limit.map(|n| n as usize),
        };
        let records: Vec<Record> =
            self.stores
                .records
                .list_records(&query)
                .map_err(|e| RpcError::Unknown {
                    message: format!("list artifact records: {e}"),
                })?;
        let records = records
            .iter()
            .map(|r| self.artifact_to_summary(r))
            .collect();
        Ok(Response::ArtifactsList { records })
    }

    fn handle_artifacts_show(&self, id: String) -> Result<Response, RpcError> {
        // Artifacts is a cross-kind read view — no kind filter on
        // lookup, just resolve and map.
        let record = self.lookup_record_of_kind(&id, None)?;
        Ok(Response::ArtifactsShow {
            record: record.as_ref().map(|r| self.artifact_to_summary(r)),
        })
    }

    fn handle_documents_list(&self, params: RecordsListParams) -> Result<Response, RpcError> {
        let records = self.list_records_with_kind("document", &params)?;
        let records = records
            .iter()
            .map(|r| self.document_to_summary(r))
            .collect();
        Ok(Response::DocumentsList { records })
    }

    fn handle_documents_show(&self, id: String) -> Result<Response, RpcError> {
        let record = self.lookup_record_of_kind(&id, Some("document"))?;
        Ok(Response::DocumentsShow {
            record: record.as_ref().map(|r| self.document_to_summary(r)),
        })
    }

    fn handle_documents_create(&self, params: RecordsCreateParams) -> Result<Response, RpcError> {
        let brain = params.brain.clone();
        let resolved_store = self.scoped_record_store(brain.as_deref())?;
        let target_records: &RecordStore = resolved_store
            .as_ref()
            .map(|(rs, _)| rs)
            .unwrap_or(&self.stores.records);
        let brain_id: String = resolved_store
            .as_ref()
            .map(|(_, bid)| bid.clone())
            .unwrap_or_else(|| self.stores.brain_id.clone());
        let create_params = records_create_params_to_internal(params);
        let record = target_records
            .create_document(create_params, &self.stores.objects)
            .map_err(|e| RpcError::Unknown {
                message: format!("create document: {e}"),
            })?;
        let summary = document_to_summary_with_brain(&record, &brain_id);
        let content_hash = record.content_ref.hash.clone();
        let size = record.content_ref.size;
        Ok(Response::DocumentsCreate {
            record: summary,
            content_hash,
            size,
        })
    }

    fn handle_plans_list(&self, params: RecordsListParams) -> Result<Response, RpcError> {
        let records = self.list_records_with_kind("plan", &params)?;
        let records = records.iter().map(|r| self.plan_to_summary(r)).collect();
        Ok(Response::PlansList { records })
    }

    fn handle_plans_show(&self, id: String) -> Result<Response, RpcError> {
        let record = self.lookup_record_of_kind(&id, Some("plan"))?;
        Ok(Response::PlansShow {
            record: record.as_ref().map(|r| self.plan_to_summary(r)),
        })
    }

    fn handle_plans_create(&self, params: RecordsCreateParams) -> Result<Response, RpcError> {
        let brain = params.brain.clone();
        let resolved_store = self.scoped_record_store(brain.as_deref())?;
        let target_records: &RecordStore = resolved_store
            .as_ref()
            .map(|(rs, _)| rs)
            .unwrap_or(&self.stores.records);
        let brain_id: String = resolved_store
            .as_ref()
            .map(|(_, bid)| bid.clone())
            .unwrap_or_else(|| self.stores.brain_id.clone());
        let create_params = records_create_params_to_internal(params);
        let record = target_records
            .create_plan(create_params, &self.stores.objects)
            .map_err(|e| RpcError::Unknown {
                message: format!("create plan: {e}"),
            })?;
        let summary = plan_to_summary_with_brain(&record, &brain_id);
        let content_hash = record.content_ref.hash.clone();
        let size = record.content_ref.size;
        Ok(Response::PlansCreate {
            record: summary,
            content_hash,
            size,
        })
    }

    fn handle_snapshots_list(&self, params: RecordsListParams) -> Result<Response, RpcError> {
        let records = self.list_records_with_kind("snapshot", &params)?;
        let records = records
            .iter()
            .map(|r| self.snapshot_to_summary(r))
            .collect();
        Ok(Response::SnapshotsList { records })
    }

    fn handle_snapshots_show(&self, id: String) -> Result<Response, RpcError> {
        let record = self.lookup_record_of_kind(&id, Some("snapshot"))?;
        Ok(Response::SnapshotsShow {
            record: record.as_ref().map(|r| self.snapshot_to_summary(r)),
        })
    }

    fn handle_snapshots_create(&self, params: RecordsCreateParams) -> Result<Response, RpcError> {
        let brain = params.brain.clone();
        let resolved_store = self.scoped_record_store(brain.as_deref())?;
        let target_records: &RecordStore = resolved_store
            .as_ref()
            .map(|(rs, _)| rs)
            .unwrap_or(&self.stores.records);
        let brain_id: String = resolved_store
            .as_ref()
            .map(|(_, bid)| bid.clone())
            .unwrap_or_else(|| self.stores.brain_id.clone());
        let create_params = records_create_params_to_internal(params);
        let record = target_records
            .create_snapshot(create_params, &self.stores.objects)
            .map_err(|e| RpcError::Unknown {
                message: format!("create snapshot: {e}"),
            })?;
        let summary = snapshot_to_summary_with_brain(&record, &brain_id);
        let content_hash = record.content_ref.hash.clone();
        let size = record.content_ref.size;
        Ok(Response::SnapshotsCreate {
            record: summary,
            content_hash,
            size,
        })
    }

    /// Resolve an optional remote-brain (record store, brain_id) pair
    /// for record-creation requests. `None` means "use the daemon's
    /// local stores"; `Some(name_or_id)` resolves to a `RecordStore`
    /// scoped to the requested brain on the same underlying DB,
    /// rejecting archived targets. The object store is always the
    /// daemon-local one because object storage is keyed by hash and
    /// shared across brains.
    fn scoped_record_store(
        &self,
        brain: Option<&str>,
    ) -> Result<Option<(RecordStore, String)>, RpcError> {
        let Some(name_or_id) = brain else {
            return Ok(None);
        };
        let (bid, bname) =
            self.stores
                .records
                .resolve_brain(name_or_id)
                .map_err(|e| RpcError::Protocol {
                    message: format!("resolve brain: {e}"),
                })?;
        if self
            .stores
            .records
            .is_brain_archived(&bid)
            .map_err(|e| RpcError::Unknown {
                message: format!("check brain archived: {e}"),
            })?
        {
            return Err(RpcError::Protocol {
                message: format!("target brain '{bname}' is archived"),
            });
        }
        let records = self
            .stores
            .records
            .with_remote_brain_id(&bid, &bname)
            .map_err(|e| RpcError::Unknown {
                message: format!("open target brain record store: {e}"),
            })?;
        Ok(Some((records, bid)))
    }

    /// Resolve a record by ID, optionally filtering on `expected_kind`.
    /// A mismatched kind is treated as "not found" so callers honour
    /// per-family Show semantics (returning `None`) rather than leaking
    /// records of the wrong kind.
    fn lookup_record_of_kind(
        &self,
        id: &str,
        expected_kind: Option<&str>,
    ) -> Result<Option<Record>, RpcError> {
        let resolved = match self.stores.records.resolve_record_id(id) {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };
        let record = self
            .stores
            .records
            .get_record(&resolved)
            .map_err(|e| RpcError::Unknown {
                message: format!("get record: {e}"),
            })?;
        Ok(record.filter(|r| match expected_kind {
            Some(kind) => r.kind.as_str() == kind,
            None => true,
        }))
    }

    fn analysis_to_summary(&self, record: &Record) -> AnalysisSummary {
        analysis_to_summary_with_brain(record, &self.stores.brain_id)
    }

    fn artifact_to_summary(&self, record: &Record) -> ArtifactSummary {
        ArtifactSummary {
            record_id: record.record_id.clone(),
            title: record.title.clone(),
            kind: record.kind.as_str().to_string(),
            status: record.status.as_str().to_string(),
            created_at: epoch_seconds_to_iso(record.created_at),
            brain_id: self.stores.brain_id.clone(),
        }
    }

    fn document_to_summary(&self, record: &Record) -> DocumentSummary {
        document_to_summary_with_brain(record, &self.stores.brain_id)
    }

    fn plan_to_summary(&self, record: &Record) -> PlanSummary {
        plan_to_summary_with_brain(record, &self.stores.brain_id)
    }

    fn snapshot_to_summary(&self, record: &Record) -> SnapshotSummary {
        snapshot_to_summary_with_brain(record, &self.stores.brain_id)
    }

    // ── sagas handlers ───────────────────────────────────────────────────────

    fn handle_sagas_list(&self, params: SagasListParams) -> Result<Response, RpcError> {
        let filter = SagaListFilter {
            include_closed: params.include_closed,
            include_cancelled: params.include_cancelled,
            containing_brain: params.containing_brain,
        };
        let sagas = self
            .stores
            .sagas
            .list(filter)
            .map_err(|e| RpcError::Unknown {
                message: format!("list sagas: {e}"),
            })?;
        let sagas: Vec<SagaSummary> = sagas.iter().map(saga_to_summary).collect();
        Ok(Response::SagasList { sagas })
    }

    fn handle_sagas_get(&self, saga_id: String) -> Result<Response, RpcError> {
        let saga = self
            .stores
            .sagas
            .get(&saga_id)
            .map_err(|e| RpcError::Unknown {
                message: format!("get saga: {e}"),
            })?;
        Ok(Response::SagasGet {
            saga: saga.as_ref().map(saga_to_summary),
        })
    }

    fn handle_sagas_create(&self, params: SagasCreateParams) -> Result<Response, RpcError> {
        let saga = self
            .stores
            .sagas
            .create(&params.title, params.description.as_deref(), "daemon")
            .map_err(|e| RpcError::Unknown {
                message: format!("create saga: {e}"),
            })?;
        Ok(Response::SagasCreate {
            saga: saga_to_summary(&saga),
        })
    }

    fn handle_sagas_update(&self, params: SagasUpdateParams) -> Result<Response, RpcError> {
        // Map the wire description-update enum into the SagaStore's
        // Option<Option<&str>> contract: None = don't touch,
        // Some(None) = clear, Some(Some(value)) = set.
        let description: Option<Option<&str>> = match params.description.as_ref() {
            None => None,
            Some(SagaDescriptionUpdate::Clear) => Some(None),
            Some(SagaDescriptionUpdate::Set { value }) => Some(Some(value.as_str())),
        };
        let saga = self
            .stores
            .sagas
            .update(
                &params.saga_id,
                params.title.as_deref(),
                description,
                "daemon",
            )
            .map_err(|e| RpcError::Unknown {
                message: format!("update saga: {e}"),
            })?;
        Ok(Response::SagasUpdate {
            saga: saga_to_summary(&saga),
        })
    }

    fn handle_sagas_add_tasks(
        &self,
        saga_id: String,
        task_ids: Vec<String>,
        cascade: bool,
    ) -> Result<Response, RpcError> {
        let (canonical, saga_id_short) =
            self.stores
                .sagas
                .resolve_short(&saga_id)
                .map_err(|e| RpcError::Unknown {
                    message: format!("resolve saga: {e}"),
                })?;
        let added = self
            .stores
            .sagas
            .add_tasks(&canonical, &task_ids, cascade, "daemon")
            .map_err(|e| RpcError::Unknown {
                message: format!("add tasks: {e}"),
            })?;
        let added_task_ids: Vec<String> = added
            .iter()
            .map(|id| self.stores.tasks.compact_id_or_raw(id))
            .collect();
        Ok(Response::SagasAddTasks {
            saga_id: saga_id_short,
            added: added_task_ids.len() as u32,
            added_task_ids,
        })
    }

    fn handle_sagas_remove_tasks(
        &self,
        saga_id: String,
        task_ids: Vec<String>,
        cascade: bool,
    ) -> Result<Response, RpcError> {
        let (canonical, saga_id_short) =
            self.stores
                .sagas
                .resolve_short(&saga_id)
                .map_err(|e| RpcError::Unknown {
                    message: format!("resolve saga: {e}"),
                })?;
        let removed = self
            .stores
            .sagas
            .remove_tasks(&canonical, task_ids, cascade, "daemon")
            .map_err(|e| RpcError::Unknown {
                message: format!("remove tasks: {e}"),
            })?;
        let removed_task_ids: Vec<String> = removed
            .iter()
            .map(|id| self.stores.tasks.compact_id_or_raw(id))
            .collect();
        Ok(Response::SagasRemoveTasks {
            saga_id: saga_id_short,
            removed: removed_task_ids.len() as u32,
            removed_task_ids,
        })
    }

    fn handle_sagas_frontier(&self, saga_id: String) -> Result<Response, RpcError> {
        let (canonical, saga_id_short) =
            self.stores
                .sagas
                .resolve_short(&saga_id)
                .map_err(|e| RpcError::Unknown {
                    message: format!("resolve saga: {e}"),
                })?;
        let frontier = self
            .stores
            .sagas
            .frontier(&canonical)
            .map_err(|e| RpcError::Unknown {
                message: format!("saga frontier: {e}"),
            })?;
        let tasks: Vec<SagaFrontierTask> = frontier
            .tasks
            .iter()
            .map(|t| self.frontier_task_to_summary(t))
            .collect();
        let brains: Vec<SagaBrainSummary> =
            frontier.brains.iter().map(saga_brain_to_summary).collect();
        Ok(Response::SagasFrontier {
            saga_id: saga_id_short,
            saga_status: frontier.status.to_string(),
            tasks,
            brains,
        })
    }

    fn handle_sagas_start(&self, saga_id: String) -> Result<Response, RpcError> {
        let saga = self
            .stores
            .sagas
            .start(&saga_id, "daemon")
            .map_err(|e| RpcError::Unknown {
                message: format!("start saga: {e}"),
            })?;
        Ok(Response::SagasStart {
            saga: saga_to_summary(&saga),
        })
    }

    fn handle_sagas_close(&self, saga_id: String, cascade: bool) -> Result<Response, RpcError> {
        let (saga, cascade_results) = self
            .stores
            .sagas
            .close(&saga_id, cascade, "daemon")
            .map_err(|e| RpcError::Unknown {
                message: format!("close saga: {e}"),
            })?;
        let cascade_results: Vec<SagaCascadeResult> = cascade_results
            .iter()
            .map(|r| self.cascade_result_to_wire(r))
            .collect();
        Ok(Response::SagasClose {
            saga: saga_to_summary(&saga),
            cascade,
            cascade_results,
        })
    }

    fn handle_sagas_cancel(&self, saga_id: String, cascade: bool) -> Result<Response, RpcError> {
        let (saga, cascade_results) = self
            .stores
            .sagas
            .cancel(&saga_id, cascade, "daemon")
            .map_err(|e| RpcError::Unknown {
                message: format!("cancel saga: {e}"),
            })?;
        let cascade_results: Vec<SagaCascadeResult> = cascade_results
            .iter()
            .map(|r| self.cascade_result_to_wire(r))
            .collect();
        Ok(Response::SagasCancel {
            saga: saga_to_summary(&saga),
            cascade,
            cascade_results,
        })
    }

    fn handle_sagas_reopen(&self, saga_id: String) -> Result<Response, RpcError> {
        let saga = self
            .stores
            .sagas
            .reopen(&saga_id, "daemon")
            .map_err(|e| RpcError::Unknown {
                message: format!("reopen saga: {e}"),
            })?;
        Ok(Response::SagasReopen {
            saga: saga_to_summary(&saga),
        })
    }

    fn handle_sagas_stats(&self, saga_id: String) -> Result<Response, RpcError> {
        let (canonical, saga_id_short) =
            self.stores
                .sagas
                .resolve_short(&saga_id)
                .map_err(|e| RpcError::Unknown {
                    message: format!("resolve saga: {e}"),
                })?;
        let stats = self
            .stores
            .sagas
            .stats(&canonical)
            .map_err(|e| RpcError::Unknown {
                message: format!("saga stats: {e}"),
            })?;
        Ok(Response::SagasStats {
            saga_id: saga_id_short,
            stats: saga_stats_to_report(&stats),
            label_histogram: stats
                .label_histogram
                .iter()
                .map(label_count_to_wire)
                .collect(),
            brains: stats.brains.iter().map(saga_brain_to_summary).collect(),
        })
    }

    /// Anti-corruption mapper for one frontier task — uses the
    /// daemon's task store to render the compact display ID.
    fn frontier_task_to_summary(&self, task: &Task) -> SagaFrontierTask {
        SagaFrontierTask {
            task_id: self.stores.tasks.compact_id_or_raw(task.id.as_str()),
            title: task.title.clone(),
            status: status_to_wire_string(&task.status),
            priority: task.priority.as_i32(),
            task_type: task.task_type.as_str().to_string(),
        }
    }

    /// Anti-corruption mapper for one cascade result — renders the
    /// member task ID via the daemon's task store so the wire string
    /// matches the local CLI's compact form.
    fn cascade_result_to_wire(&self, result: &CascadeResult) -> SagaCascadeResult {
        SagaCascadeResult {
            task_id: self.stores.tasks.compact_id_or_raw(result.task_id.as_str()),
            outcome: match &result.outcome {
                CascadeOutcome::Closed => SagaCascadeOutcome::Closed,
                CascadeOutcome::Cancelled => SagaCascadeOutcome::Cancelled,
                CascadeOutcome::Skipped { reason } => SagaCascadeOutcome::Skipped {
                    reason: reason.clone(),
                },
                CascadeOutcome::Failed { error } => SagaCascadeOutcome::Failed {
                    error: error.clone(),
                },
            },
        }
    }

    // ── memory handlers ─────────────────────────────────────────────────────

    fn handle_memory_write_episode(
        &self,
        params: MemoryWriteEpisodeParams,
    ) -> Result<Response, RpcError> {
        let importance = millis_to_unit(params.importance_millis);
        let episode = Episode {
            brain_id: self.stores.brain_id.clone(),
            goal: params.goal,
            actions: params.actions,
            outcome: params.outcome,
            tags: params.tags,
            importance,
        };
        let summary_id = self
            .stores
            .store_episode(&episode)
            .map_err(|e| RpcError::Unknown {
                message: format!("store episode: {e}"),
            })?;
        let uri = brain_lib::uri::SynapseUri::for_episode(&self.stores.brain_name, &summary_id)
            .to_string();
        Ok(Response::MemoryWriteEpisode { summary_id, uri })
    }

    fn handle_memory_write_procedure(
        &self,
        params: MemoryWriteProcedureParams,
    ) -> Result<Response, RpcError> {
        let importance = millis_to_unit(params.importance_millis);
        let summary_id = self
            .stores
            .store_procedure(
                &params.title,
                &params.steps,
                &params.tags,
                importance,
                &self.stores.brain_id,
            )
            .map_err(|e| RpcError::Unknown {
                message: format!("store procedure: {e}"),
            })?;
        let uri = brain_lib::uri::SynapseUri::for_procedure(&self.stores.brain_name, &summary_id)
            .to_string();
        Ok(Response::MemoryWriteProcedure { summary_id, uri })
    }

    fn handle_memory_consolidate(
        &self,
        params: MemoryConsolidateParams,
    ) -> Result<Response, RpcError> {
        use brain_lib::consolidation::{consolidate_episodes, enqueue_cluster_summarization};

        let episodes = self
            .stores
            .list_episodes(params.limit, &self.stores.brain_id)
            .map_err(|e| RpcError::Unknown {
                message: format!("list_episodes: {e}"),
            })?;

        let result = consolidate_episodes(episodes, params.gap_seconds);
        let jobs_enqueued = if params.auto_summarize {
            enqueue_cluster_summarization(&self.stores, &result.clusters, &self.stores.brain_id)
                .map_err(|e| RpcError::Unknown {
                    message: format!("enqueue cluster summarization: {e}"),
                })?
        } else {
            0
        };

        let clusters_json: Vec<serde_json::Value> = result
            .clusters
            .iter()
            .map(|c| {
                serde_json::json!({
                    "episode_ids": c.episode_ids,
                    "episode_count": c.episodes.len(),
                    "suggested_title": c.suggested_title,
                    "summary": c.summary,
                    "oldest_ts": c.episodes.iter().map(|e| e.created_at).min(),
                    "newest_ts": c.episodes.iter().map(|e| e.created_at).max(),
                })
            })
            .collect();
        let out = serde_json::json!({
            "cluster_count": clusters_json.len(),
            "jobs_enqueued": jobs_enqueued,
            "clusters": clusters_json,
        });
        let result_json = serde_json::to_string(&out).map_err(|e| RpcError::Protocol {
            message: format!("serialize consolidate report: {e}"),
        })?;
        Ok(Response::MemoryConsolidate { result_json })
    }

    fn handle_memory_summarize_scope(
        &self,
        params: MemorySummarizeScopeParams,
    ) -> Result<Response, RpcError> {
        use brain_lib::hierarchy::{
            DerivedSummary, ScopeType, generate_scope_summary_with_options, get_scope_summary,
        };

        let st = match params.scope_type.as_str() {
            "directory" => ScopeType::Directory,
            "tag" => ScopeType::Tag,
            other => {
                return Err(RpcError::Protocol {
                    message: format!(
                        "invalid scope_type {other:?} (expected \"directory\" or \"tag\")"
                    ),
                });
            }
        };

        let mut llm_pending = false;
        let summary: DerivedSummary = if params.regenerate {
            let generation = generate_scope_summary_with_options(
                &self.stores,
                &st,
                &params.scope_value,
                params.async_llm,
            )
            .map_err(|e| RpcError::Unknown {
                message: format!("generate scope summary: {e}"),
            })?;
            llm_pending = generation.llm_pending;
            get_scope_summary(&self.stores, &st, &params.scope_value)
                .map_err(|e| RpcError::Unknown {
                    message: format!("load scope summary: {e}"),
                })?
                .ok_or_else(|| RpcError::Unknown {
                    message: format!("generated summary {} not found after insert", generation.id),
                })?
        } else {
            match get_scope_summary(&self.stores, &st, &params.scope_value).map_err(|e| {
                RpcError::Unknown {
                    message: format!("load scope summary: {e}"),
                }
            })? {
                Some(s) => s,
                None => {
                    let generation = generate_scope_summary_with_options(
                        &self.stores,
                        &st,
                        &params.scope_value,
                        params.async_llm,
                    )
                    .map_err(|e| RpcError::Unknown {
                        message: format!("generate scope summary: {e}"),
                    })?;
                    llm_pending = generation.llm_pending;
                    get_scope_summary(&self.stores, &st, &params.scope_value)
                        .map_err(|e| RpcError::Unknown {
                            message: format!("load scope summary: {e}"),
                        })?
                        .ok_or_else(|| RpcError::Unknown {
                            message: format!(
                                "generated summary {} not found after insert",
                                generation.id
                            ),
                        })?
                }
            }
        };

        let out = serde_json::json!({
            "scope_type": summary.scope_type,
            "scope_value": summary.scope_value,
            "content": summary.content,
            "stale": summary.stale,
            "llm_pending": llm_pending,
            "generated_at": summary.generated_at,
        });
        let result_json = serde_json::to_string(&out).map_err(|e| RpcError::Protocol {
            message: format!("serialize summarize report: {e}"),
        })?;
        Ok(Response::MemorySummarizeScope { result_json })
    }

    fn handle_memory_retrieve(&self, params: MemoryRetrieveParams) -> Result<Response, RpcError> {
        let mcp_ctx = self.mcp_ctx()?;
        let semantic_ctx = brain_memory::context::SemanticContext {
            db: mcp_ctx.stores.inner_db(),
            brain_id: mcp_ctx.brain_id(),
            brain_name: mcp_ctx.brain_name(),
            store: mcp_ctx.store(),
            embedder: mcp_ctx.embedder(),
            metrics: &mcp_ctx.metrics,
        };

        let kinds: Vec<brain_lib::retrieval::MemoryKind> = params
            .kinds
            .iter()
            .filter_map(|k| serde_json::from_value(serde_json::Value::String(k.clone())).ok())
            .collect();

        let retrieve_params = brain_memory::retrieve::RetrieveParams {
            query: params.query,
            uri: params.uri.clone(),
            lod: params.lod.clone(),
            count: params.count,
            strategy: params.strategy,
            brain: None,
            brains: params.brains.clone(),
            time_scope: params.time_scope,
            tags: params.tags,
            kinds,
            time_after: params.time_after,
            time_before: params.time_before,
            tags_require: params.tags_require,
            tags_exclude: params.tags_exclude,
            explain: params.explain,
            vector_search_mode: None,
        };

        let lod = brain_lib::lod::LodLevel::parse(&retrieve_params.lod.to_uppercase()).ok_or_else(
            || RpcError::Protocol {
                message: format!(
                    "Invalid lod value {:?}: must be one of L0, L1, L2",
                    retrieve_params.lod
                ),
            },
        )?;

        // URI mode is sync and skips the search layer entirely.
        if let Some(uri_str) = retrieve_params.uri.as_deref() {
            let value = brain_memory::retrieve::run_uri_mode_as_json(
                &semantic_ctx,
                uri_str,
                lod,
                retrieve_params.explain,
            )
            .map_err(|e| RpcError::Unknown {
                message: format!("memory.retrieve: {e}"),
            })?;
            return wrap_memory_retrieve_value(value);
        }

        // Query mode runs the async pipeline on the daemon's runtime.
        let runtime = self.runtime()?;
        let brains_pre = if retrieve_params.brains.is_empty() {
            Vec::new()
        } else {
            self.build_federated_brain_list(&mcp_ctx, &retrieve_params.brains, runtime)?
        };

        let value = runtime
            .block_on(brain_memory::retrieve::run_query_as_json(
                &semantic_ctx,
                retrieve_params,
                brains_pre,
            ))
            .map_err(|e| RpcError::Unknown {
                message: format!("memory.retrieve: {e}"),
            })?;
        wrap_memory_retrieve_value(value)
    }

    fn handle_memory_reflect(&self, params: MemoryReflectParams) -> Result<Response, RpcError> {
        let mcp_ctx = self.mcp_ctx()?;
        let semantic_ctx = brain_memory::context::SemanticContext {
            db: mcp_ctx.stores.inner_db(),
            brain_id: mcp_ctx.brain_id(),
            brain_name: mcp_ctx.brain_name(),
            store: mcp_ctx.store(),
            embedder: mcp_ctx.embedder(),
            metrics: &mcp_ctx.metrics,
        };

        let reflect_params = brain_memory::reflect::ReflectParams {
            mode: if params.commit { "commit" } else { "prepare" }.to_string(),
            topic: params.topic.unwrap_or_default(),
            budget_tokens: params.budget as u64,
            brains: params.brains,
            title: params.title.unwrap_or_default(),
            content: params.content.unwrap_or_default(),
            source_ids: params.source_ids,
            tags: params.tags,
            importance: params.importance_millis.map(millis_to_unit),
        };

        let runtime = self.runtime()?;
        let value = runtime
            .block_on(brain_memory::reflect::run_as_json(
                &semantic_ctx,
                reflect_params,
            ))
            .map_err(|e| RpcError::Unknown {
                message: format!("memory.reflect: {e}"),
            })?;
        let result_json = serde_json::to_string(&value).map_err(|e| RpcError::Protocol {
            message: format!("serialize reflect result: {e}"),
        })?;
        Ok(Response::MemoryReflect { result_json })
    }

    fn handle_memory_walk_thread(
        &self,
        params: MemoryWalkThreadParams,
    ) -> Result<Response, RpcError> {
        let walk_params: brain_memory::walk_thread::WalkThreadParams =
            serde_json::from_value(params.params_json).map_err(|e| RpcError::Protocol {
                message: format!("invalid memory.walk_thread params: {e}"),
            })?;
        let value = brain_memory::walk_thread::run_as_json(
            self.stores.inner_db(),
            &self.stores.brain_id,
            &self.stores.brain_name,
            walk_params,
        )
        .map_err(|e| RpcError::Unknown {
            message: format!("memory.walk_thread: {e}"),
        })?;
        let result_json = serde_json::to_string(&value).map_err(|e| RpcError::Protocol {
            message: format!("serialize walk_thread result: {e}"),
        })?;
        Ok(Response::MemoryWalkThread { result_json })
    }

    // ── Wave-1 mutation/read handlers (links / records / brains / labels)

    fn handle_links_add(&self, params: brain_rpc::LinksAddParams) -> Result<Response, RpcError> {
        use brain_persistence::db::links::{
            EntityRef, LinkError, add_link_checked, edge_kind_from_str, entity_type_from_str,
        };

        let from_kind =
            entity_type_from_str(&params.from.kind).ok_or_else(|| RpcError::Protocol {
                message: format!("unknown from.kind: {}", params.from.kind),
            })?;
        let to_kind = entity_type_from_str(&params.to.kind).ok_or_else(|| RpcError::Protocol {
            message: format!("unknown to.kind: {}", params.to.kind),
        })?;
        let edge_kind =
            edge_kind_from_str(&params.edge_kind).ok_or_else(|| RpcError::Protocol {
                message: format!("unknown edge_kind: {}", params.edge_kind),
            })?;
        let from = EntityRef {
            kind: from_kind,
            id: params.from.id,
        };
        let to = EntityRef {
            kind: to_kind,
            id: params.to.id,
        };

        let outcome = self.stores.inner_db().with_write_conn(move |conn| {
            match add_link_checked(conn, from, to, edge_kind) {
                // Re-wrap LinkError so the closure's SqlError signature is satisfied
                // while preserving the LinkError discriminant for the outer match.
                Err(LinkError::Cycle(ek)) => Err(brain_persistence::sql::SqlError::Domain(
                    brain_persistence::error::BrainCoreError::Database(format!("cycle: {ek:?}")),
                )),
                Err(e) => Err(brain_persistence::sql::SqlError::Domain(
                    brain_persistence::error::BrainCoreError::Database(e.to_string()),
                )),
                Ok(()) => Ok(()),
            }
        });

        match outcome {
            Ok(()) => Ok(Response::LinksAdd { created: true }),
            Err(e) => Err(RpcError::Unknown {
                message: format!("links.add: {e}"),
            }),
        }
    }

    fn handle_links_remove(
        &self,
        params: brain_rpc::LinksRemoveParams,
    ) -> Result<Response, RpcError> {
        use brain_persistence::db::links::{
            EntityRef, edge_kind_from_str, entity_type_from_str, remove_link,
        };

        let from_kind =
            entity_type_from_str(&params.from.kind).ok_or_else(|| RpcError::Protocol {
                message: format!("unknown from.kind: {}", params.from.kind),
            })?;
        let to_kind = entity_type_from_str(&params.to.kind).ok_or_else(|| RpcError::Protocol {
            message: format!("unknown to.kind: {}", params.to.kind),
        })?;
        let edge_kind =
            edge_kind_from_str(&params.edge_kind).ok_or_else(|| RpcError::Protocol {
                message: format!("unknown edge_kind: {}", params.edge_kind),
            })?;
        let from = EntityRef {
            kind: from_kind,
            id: params.from.id,
        };
        let to = EntityRef {
            kind: to_kind,
            id: params.to.id,
        };

        let removed = self
            .stores
            .inner_db()
            .with_write_conn(move |conn| {
                remove_link(conn, from, to, edge_kind).map_err(|e| {
                    brain_persistence::sql::SqlError::Domain(
                        brain_persistence::error::BrainCoreError::Database(e.to_string()),
                    )
                })
            })
            .map_err(|e| RpcError::Unknown {
                message: format!("links.remove: {e}"),
            })?;
        Ok(Response::LinksRemove { removed })
    }

    fn handle_links_for_entity(
        &self,
        params: brain_rpc::LinksForEntityParams,
    ) -> Result<Response, RpcError> {
        use brain_persistence::db::links::{EntityRef, edge_kind_str, entity_type_str, for_entity};

        let kind = brain_persistence::db::links::entity_type_from_str(&params.entity.kind)
            .ok_or_else(|| RpcError::Protocol {
                message: format!("unknown entity.kind: {}", params.entity.kind),
            })?;
        let entity = EntityRef {
            kind,
            id: params.entity.id.clone(),
        };
        let entity_id = params.entity.id.clone();

        let edges = self
            .stores
            .inner_db()
            .with_read_conn(move |conn| {
                for_entity(conn, &entity, None).map_err(|e| {
                    brain_persistence::sql::SqlError::Domain(
                        brain_persistence::error::BrainCoreError::Database(e.to_string()),
                    )
                })
            })
            .map_err(|e| RpcError::Unknown {
                message: format!("links.for_entity: {e}"),
            })?;

        // Apply direction filter daemon-side. "both" / unrecognised fall through unfiltered.
        let direction = params.direction.as_str();
        let entity_kind_str = params.entity.kind.as_str();
        let filtered: Vec<_> = edges
            .into_iter()
            .filter(|e| {
                let is_outgoing =
                    entity_type_str(e.from.kind) == entity_kind_str && e.from.id == entity_id;
                let is_incoming =
                    entity_type_str(e.to.kind) == entity_kind_str && e.to.id == entity_id;
                match direction {
                    "out" | "outgoing" => is_outgoing,
                    "in" | "incoming" => is_incoming,
                    _ => is_outgoing || is_incoming,
                }
            })
            .collect();

        let limit = params.limit.unwrap_or(u32::MAX) as usize;
        let links: Vec<brain_rpc::WireLinkSummary> = filtered
            .into_iter()
            .take(limit)
            .map(|e| brain_rpc::WireLinkSummary {
                from: brain_rpc::WireEntityRef {
                    kind: entity_type_str(e.from.kind).to_string(),
                    id: e.from.id,
                },
                to: brain_rpc::WireEntityRef {
                    kind: entity_type_str(e.to.kind).to_string(),
                    id: e.to.id,
                },
                edge_kind: edge_kind_str(e.edge_kind).to_string(),
                // for_entity doesn't surface created_at — wire field stays empty
                // until the read API is extended. CodeRabbit may flag this; the
                // alternative is a parallel query for timestamps which doubles I/O.
                created_at: String::new(),
            })
            .collect();
        Ok(Response::LinksForEntity { links })
    }

    fn handle_records_archive(
        &self,
        params: brain_rpc::RecordsArchiveParams,
    ) -> Result<Response, RpcError> {
        let record_id = self
            .stores
            .records
            .resolve_record_id(&params.record_id)
            .map_err(|e| RpcError::Protocol {
                message: format!("resolve record_id: {e}"),
            })?;
        if self
            .stores
            .records
            .get_record(&record_id)
            .map_err(|e| RpcError::Unknown {
                message: format!("get record: {e}"),
            })?
            .is_none()
        {
            return Err(RpcError::Unknown {
                message: format!("record not found: {record_id}"),
            });
        }
        self.stores
            .records
            .archive_record(&record_id, params.reason, "mcp")
            .map_err(|e| RpcError::Unknown {
                message: format!("archive record: {e}"),
            })?;
        let compact = self
            .stores
            .records
            .compact_record_id(&record_id)
            .unwrap_or_else(|_| record_id.clone());
        let uri =
            brain_lib::uri::SynapseUri::for_record(&self.stores.brain_name, &compact).to_string();
        Ok(Response::RecordsArchive {
            record_id: compact,
            uri,
            status: "archived".into(),
        })
    }

    fn handle_records_link_add(
        &self,
        params: brain_rpc::RecordsLinkParams,
    ) -> Result<Response, RpcError> {
        let record_id = self
            .stores
            .records
            .resolve_record_id(&params.record_id)
            .map_err(|e| RpcError::Protocol {
                message: format!("resolve record_id: {e}"),
            })?;
        match params.target.kind.to_lowercase().as_str() {
            "task" => self
                .stores
                .records
                .link_task(&record_id, &params.target.id, "mcp")
                .map_err(|e| RpcError::Unknown {
                    message: format!("records.link_add(task): {e}"),
                })?,
            "chunk" => self
                .stores
                .records
                .link_chunk(&record_id, &params.target.id, "mcp")
                .map_err(|e| RpcError::Unknown {
                    message: format!("records.link_add(chunk): {e}"),
                })?,
            other => {
                return Err(RpcError::Protocol {
                    message: format!("records.link_add: unsupported target kind {other:?}"),
                });
            }
        }
        Ok(Response::RecordsLinkAdd { created: true })
    }

    fn handle_records_link_remove(
        &self,
        params: brain_rpc::RecordsLinkParams,
    ) -> Result<Response, RpcError> {
        let record_id = self
            .stores
            .records
            .resolve_record_id(&params.record_id)
            .map_err(|e| RpcError::Protocol {
                message: format!("resolve record_id: {e}"),
            })?;
        match params.target.kind.to_lowercase().as_str() {
            "task" => self
                .stores
                .records
                .unlink_task(&record_id, &params.target.id, "mcp")
                .map_err(|e| RpcError::Unknown {
                    message: format!("records.link_remove(task): {e}"),
                })?,
            "chunk" => self
                .stores
                .records
                .unlink_chunk(&record_id, &params.target.id, "mcp")
                .map_err(|e| RpcError::Unknown {
                    message: format!("records.link_remove(chunk): {e}"),
                })?,
            other => {
                return Err(RpcError::Protocol {
                    message: format!("records.link_remove: unsupported target kind {other:?}"),
                });
            }
        }
        Ok(Response::RecordsLinkRemove { removed: true })
    }

    fn handle_records_tag_add(&self, record_id: String, tag: String) -> Result<Response, RpcError> {
        let resolved = self
            .stores
            .records
            .resolve_record_id(&record_id)
            .map_err(|e| RpcError::Protocol {
                message: format!("resolve record_id: {e}"),
            })?;
        self.stores
            .records
            .add_tag(&resolved, &tag, "mcp")
            .map_err(|e| RpcError::Unknown {
                message: format!("records.tag_add: {e}"),
            })?;
        Ok(Response::RecordsTagAdd { tag })
    }

    fn handle_records_tag_remove(
        &self,
        record_id: String,
        tag: String,
    ) -> Result<Response, RpcError> {
        let resolved = self
            .stores
            .records
            .resolve_record_id(&record_id)
            .map_err(|e| RpcError::Protocol {
                message: format!("resolve record_id: {e}"),
            })?;
        self.stores
            .records
            .remove_tag(&resolved, &tag, "mcp")
            .map_err(|e| RpcError::Unknown {
                message: format!("records.tag_remove: {e}"),
            })?;
        Ok(Response::RecordsTagRemove { removed: true })
    }

    fn handle_tasks_labels_summary(&self) -> Result<Response, RpcError> {
        let summaries = self
            .stores
            .tasks
            .label_summary()
            .map_err(|e| RpcError::Unknown {
                message: format!("tasks.labels_summary: {e}"),
            })?;
        let prefixes = self.stores.tasks.compact_ids().unwrap_or_default();
        let labels: Vec<brain_rpc::WireTaskLabelSummary> = summaries
            .into_iter()
            .map(|s| brain_rpc::WireTaskLabelSummary {
                count: s.count as u32,
                task_ids: s
                    .task_ids
                    .iter()
                    .map(|id| {
                        prefixes
                            .get(id.as_str())
                            .cloned()
                            .unwrap_or_else(|| id.clone())
                    })
                    .collect(),
                label: s.label,
            })
            .collect();
        Ok(Response::TasksLabelsSummary { labels })
    }

    fn handle_brains_list(
        &self,
        params: brain_rpc::BrainsListParams,
    ) -> Result<Response, RpcError> {
        let active_only = !params.include_archived;
        let rows = self
            .stores
            .list_brains(active_only)
            .map_err(|e| RpcError::Unknown {
                message: format!("brains.list: {e}"),
            })?;
        let brains: Vec<brain_rpc::WireBrainSummary> = rows
            .into_iter()
            .map(|row| {
                let roots: Vec<String> = row
                    .roots_json
                    .as_deref()
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or_default();
                let (root, extra_roots) = match roots.split_first() {
                    Some((head, tail)) => (head.clone(), tail.to_vec()),
                    None => (String::new(), Vec::new()),
                };
                let aliases: Vec<String> = row
                    .aliases_json
                    .as_deref()
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or_default();
                brain_rpc::WireBrainSummary {
                    name: row.name,
                    id: Some(row.brain_id),
                    root,
                    aliases,
                    extra_roots,
                    prefix: row.prefix,
                    archived: row.archived,
                }
            })
            .collect();
        let count = brains.len() as u32;
        Ok(Response::BrainsList { brains, count })
    }

    fn handle_tasks_deps_batch(&self, params: TasksDepsBatchParams) -> Result<Response, RpcError> {
        // Mirrors `brain_lib::mcp::tools::task_deps_batch::{Params, DepPair}`.
        // Action union: add / remove / chain / fan / clear.
        #[derive(serde::Deserialize)]
        struct DepPair {
            task_id: String,
            depends_on_task_id: String,
        }
        #[derive(serde::Deserialize)]
        struct Inner {
            action: String,
            #[serde(default)]
            pairs: Option<Vec<DepPair>>,
            #[serde(default)]
            task_ids: Option<Vec<String>>,
            #[serde(default)]
            source_task_id: Option<String>,
            #[serde(default)]
            dependent_task_ids: Option<Vec<String>>,
            #[serde(default)]
            task_id: Option<String>,
        }

        let parsed: Inner =
            serde_json::from_value(params.params_json).map_err(|e| RpcError::Protocol {
                message: format!("Invalid parameters: {e}"),
            })?;

        let store = &self.stores.tasks;
        let brain_name = self.stores.brain_name.as_str();

        let value = match parsed.action.as_str() {
            "add" => deps_pairs_response(
                store,
                parsed.pairs.as_deref().map(|p| {
                    p.iter()
                        .map(|d| (d.task_id.as_str(), d.depends_on_task_id.as_str()))
                        .collect::<Vec<_>>()
                }),
                EventType::DependencyAdded,
                brain_name,
            )?,
            "remove" => deps_pairs_response(
                store,
                parsed.pairs.as_deref().map(|p| {
                    p.iter()
                        .map(|d| (d.task_id.as_str(), d.depends_on_task_id.as_str()))
                        .collect::<Vec<_>>()
                }),
                EventType::DependencyRemoved,
                brain_name,
            )?,
            "chain" => deps_chain_response(store, parsed.task_ids.as_deref(), brain_name)?,
            "fan" => deps_fan_response(
                store,
                parsed.source_task_id.as_deref(),
                parsed.dependent_task_ids.as_deref(),
                brain_name,
            )?,
            "clear" => deps_clear_response(store, parsed.task_id.as_deref(), brain_name)?,
            other => {
                return Err(RpcError::Protocol {
                    message: format!(
                        "Invalid action: '{other}'. Must be one of: add, remove, chain, fan, clear"
                    ),
                });
            }
        };

        let result_json = serde_json::to_string(&value).map_err(|e| RpcError::Protocol {
            message: format!("serialize deps_batch response: {e}"),
        })?;

        Ok(Response::TasksDepsBatch { result_json })
    }

    fn handle_tasks_labels_batch(
        &self,
        params: TasksLabelsBatchParams,
    ) -> Result<Response, RpcError> {
        // Mirrors `brain_lib::mcp::tools::task_labels_batch::Params`.
        // Action union: add / remove / rename / purge.
        #[derive(serde::Deserialize)]
        struct Inner {
            action: String,
            #[serde(default)]
            label: Option<String>,
            #[serde(default)]
            task_ids: Option<Vec<String>>,
            #[serde(default)]
            old_label: Option<String>,
            #[serde(default)]
            new_label: Option<String>,
            #[serde(default)]
            brain: Option<String>,
        }

        let parsed: Inner =
            serde_json::from_value(params.params_json).map_err(|e| RpcError::Protocol {
                message: format!("Invalid parameters: {e}"),
            })?;

        // Resolve target TaskStore. With `brain` set we open a remote
        // rescoped store; otherwise we use the daemon's local one. The
        // remote store is owned through this scope; the local store is
        // borrowed from `self`. `target_tasks` ends up as `&TaskStore`
        // regardless, so the helper signatures stay uniform.
        let (remote_store, target_brain_name) = if let Some(b) = &parsed.brain {
            let (bid, bname) = self
                .stores
                .resolve_brain(b)
                .map_err(|e| RpcError::Unknown {
                    message: format!("Failed to resolve brain: {e}"),
                })?;
            let tasks = self
                .stores
                .tasks
                .with_remote_brain_id(&bid, &bname)
                .map_err(|e| RpcError::Unknown {
                    message: format!("Failed to open brain stores: {e}"),
                })?;
            (Some(tasks), bname)
        } else {
            (None, self.stores.brain_name.clone())
        };
        let target_tasks: &TaskStore = remote_store.as_ref().unwrap_or(&self.stores.tasks);

        let value = match parsed.action.as_str() {
            "add" => label_add_remove_response(
                target_tasks,
                parsed.label.as_deref(),
                parsed.task_ids.as_deref(),
                EventType::LabelAdded,
                &target_brain_name,
            )?,
            "remove" => label_add_remove_response(
                target_tasks,
                parsed.label.as_deref(),
                parsed.task_ids.as_deref(),
                EventType::LabelRemoved,
                &target_brain_name,
            )?,
            "rename" => label_rename_response(
                target_tasks,
                parsed.old_label.as_deref(),
                parsed.new_label.as_deref(),
                &target_brain_name,
            )?,
            "purge" => {
                label_purge_response(target_tasks, parsed.label.as_deref(), &target_brain_name)?
            }
            other => {
                return Err(RpcError::Protocol {
                    message: format!(
                        "Invalid action: '{other}'. Must be one of: add, remove, rename, purge"
                    ),
                });
            }
        };

        let result_json = serde_json::to_string(&value).map_err(|e| RpcError::Protocol {
            message: format!("serialize labels_batch response: {e}"),
        })?;

        Ok(Response::TasksLabelsBatch { result_json })
    }

    fn handle_tags_recluster(&self, params: TagsReclusterParams) -> Result<Response, RpcError> {
        // Mirrors `brain_lib::mcp::tools::tags_recluster::Params` — accepts
        // an opaque JSON body to avoid mirroring the MCP tool's input
        // schema in the wire protocol.
        #[derive(serde::Deserialize, Default)]
        struct Inner {
            threshold: Option<f32>,
        }

        let parsed: Inner =
            serde_json::from_value(params.params_json).map_err(|e| RpcError::Protocol {
                message: format!("Invalid parameters: {e}"),
            })?;

        let mut cluster_params = brain_lib::ClusterParams::default();
        if let Some(threshold) = parsed.threshold {
            if !(0.0..=1.0).contains(&threshold) {
                return Err(RpcError::Unknown {
                    message: format!(
                        "threshold must be between 0.0 and 1.0 (got {threshold}); \
                         values outside this range produce all-singleton clusters \
                         and write a misleading 'successful' run row"
                    ),
                });
            }
            cluster_params.cosine_threshold = threshold;
        }

        let mcp_ctx = self.mcp_ctx()?;
        // Verbatim of `brain_lib::mcp::tools::MEMORY_UNAVAILABLE`; kept in
        // sync until brain_lib::mcp leaves and the daemon owns the wire
        // message outright.
        let embedder = mcp_ctx.embedder().ok_or_else(|| RpcError::Unknown {
            message: "Memory tools are unavailable: embedding model not found.\n\
                To download the model, either run the setup script:\n  \
                curl -sSL https://raw.githubusercontent.com/benediktms/brain/master/scripts/setup-model.sh | bash\n\
                Or install the HuggingFace CLI manually:\n  \
                pip install huggingface_hub\n  \
                hf download BAAI/bge-small-en-v1.5 config.json tokenizer.json model.safetensors --local-dir ~/.brain/models/bge-small-en-v1.5"
                .to_string(),
        })?;

        let runtime = self.runtime()?;
        let report = runtime
            .block_on(brain_lib::run_recluster(
                mcp_ctx.stores.inner_db(),
                &mcp_ctx.stores.brain_id,
                embedder,
                cluster_params,
            ))
            .map_err(|e| RpcError::Unknown {
                message: format!("recluster failed: {e}"),
            })?;

        let result_json = serde_json::to_string(&report).map_err(|e| RpcError::Protocol {
            message: format!("serialize ReclusterReport: {e}"),
        })?;

        Ok(Response::TagsRecluster { result_json })
    }

    /// Resolve federated brain keys to a `(name, id, Option<StoreReader>)`
    /// list suitable for `brain_memory::retrieve::run_query_as_json`.
    ///
    /// Mirrors the brain_lib `build_federated_brains` helper but lives
    /// here so the daemon doesn't reach into the about-to-be-deleted
    /// `brain_lib::mcp::tools` module.
    fn build_federated_brain_list(
        &self,
        mcp_ctx: &brain_lib::mcp::McpContext,
        brain_keys_input: &[String],
        runtime: &tokio::runtime::Runtime,
    ) -> Result<
        Vec<(
            String,
            String,
            Option<brain_persistence::store::StoreReader>,
        )>,
        RpcError,
    > {
        let store = mcp_ctx.store().ok_or_else(|| RpcError::Unknown {
            message: "memory.retrieve: search layer unavailable".into(),
        })?;
        let embedder = mcp_ctx.embedder().ok_or_else(|| RpcError::Unknown {
            message: "memory.retrieve: embedder unavailable".into(),
        })?;

        let brain_keys: Vec<String> = if brain_keys_input.iter().any(|b| b == "all") {
            self.stores
                .list_brain_keys()
                .map_err(|e| RpcError::Unknown {
                    message: format!("list brains: {e}"),
                })?
                .into_iter()
                .map(|(name, _id)| name)
                .collect()
        } else {
            brain_keys_input.to_vec()
        };

        let mut brains: Vec<(
            String,
            String,
            Option<brain_persistence::store::StoreReader>,
        )> = Vec::new();
        brains.push((
            mcp_ctx.brain_name().to_string(),
            mcp_ctx.brain_id().to_string(),
            Some(store.clone()),
        ));

        for key in &brain_keys {
            if key == mcp_ctx.brain_name() {
                continue;
            }
            let remote_result = runtime.block_on(brain_lib::config::open_remote_search_context(
                mcp_ctx.brain_home(),
                key,
                std::path::Path::new(""),
                embedder,
            ));
            match remote_result {
                Ok(Some(remote)) => {
                    brains.push((remote.brain_name, remote.brain_id, remote.store));
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }

        Ok(brains)
    }

    // ── tags handlers ────────────────────────────────────────────────────────

    fn handle_tags_aliases_list(
        &self,
        params: TagsAliasesListParams,
    ) -> Result<Response, RpcError> {
        let rows = self
            .stores
            .list_tag_aliases(
                params.canonical.as_deref(),
                params.cluster_id.as_deref(),
                params.limit,
                params.offset,
            )
            .map_err(|e| RpcError::Unknown {
                message: format!("list_tag_aliases failed: {e}"),
            })?;

        let wire_rows: Vec<TagAliasSummary> = rows
            .into_iter()
            .map(|r| TagAliasSummary {
                raw_tag: r.raw_tag,
                canonical_tag: r.canonical_tag,
                cluster_id: r.cluster_id,
                updated_at: r.updated_at,
            })
            .collect();

        Ok(Response::TagsAliasesList { rows: wire_rows })
    }

    fn handle_tags_aliases_status(&self) -> Result<Response, RpcError> {
        let last_run = self
            .stores
            .latest_tag_cluster_run()
            .map_err(|e| RpcError::Unknown {
                message: format!("latest_tag_cluster_run failed: {e}"),
            })?;
        let counts = self
            .stores
            .count_tag_aliases()
            .map_err(|e| RpcError::Unknown {
                message: format!("count_tag_aliases failed: {e}"),
            })?;

        let report = TagAliasesStatusReport {
            total_aliases: counts.raw_count as u64,
            total_clusters: counts.cluster_count as u64,
            canonical_count: counts.canonical_count as u64,
            last_run_id: last_run.as_ref().map(|r| r.run_id.clone()),
            last_run_started_at: last_run.as_ref().map(|r| r.started_at.clone()),
            last_run_embedder_version: last_run.as_ref().map(|r| r.embedder_version.clone()),
        };

        Ok(Response::TagsAliasesStatus { report })
    }

    // ── jobs handlers ────────────────────────────────────────────────────────

    fn handle_jobs_status(&self) -> Result<Response, RpcError> {
        use brain_persistence::db::job::JobStatus;

        let pending = self
            .stores
            .count_jobs_by_status(&JobStatus::Pending)
            .map_err(|e| RpcError::Unknown {
                message: format!("count_jobs_by_status(Pending) failed: {e}"),
            })?;
        let running = self
            .stores
            .count_jobs_by_status(&JobStatus::InProgress)
            .map_err(|e| RpcError::Unknown {
                message: format!("count_jobs_by_status(InProgress) failed: {e}"),
            })?;
        let done = self
            .stores
            .count_jobs_by_status(&JobStatus::Done)
            .map_err(|e| RpcError::Unknown {
                message: format!("count_jobs_by_status(Done) failed: {e}"),
            })?;
        let failed = self
            .stores
            .count_jobs_by_status(&JobStatus::Failed)
            .map_err(|e| RpcError::Unknown {
                message: format!("count_jobs_by_status(Failed) failed: {e}"),
            })?;
        let ready = self
            .stores
            .count_jobs_by_status(&JobStatus::Ready)
            .map_err(|e| RpcError::Unknown {
                message: format!("count_jobs_by_status(Ready) failed: {e}"),
            })?;

        let recent_jobs = self
            .stores
            .list_jobs_by_status(&JobStatus::Failed, 10)
            .map_err(|e| RpcError::Unknown {
                message: format!("list_jobs_by_status(Failed) failed: {e}"),
            })?;
        let stuck_jobs = self
            .stores
            .list_stuck_jobs()
            .map_err(|e| RpcError::Unknown {
                message: format!("list_stuck_jobs failed: {e}"),
            })?;

        let to_wire = |j: &brain_persistence::db::job::Job| JobSummary {
            job_id: j.job_id.clone(),
            kind: j.kind().to_string(),
            ref_id: j.payload.ref_id().to_string(),
            attempts: j.attempts,
            last_error: j.last_error.clone(),
            updated_at: epoch_seconds_to_iso(j.updated_at),
        };

        let report = JobsStatusReport {
            pending: pending as u64,
            running: running as u64,
            ready: ready as u64,
            done: done as u64,
            failed: failed as u64,
            recent_failures: recent_jobs.iter().map(to_wire).collect(),
            stuck_jobs: stuck_jobs.iter().map(to_wire).collect(),
        };

        Ok(Response::JobsStatus { report })
    }

    // ── status handler ───────────────────────────────────────────────────────

    fn handle_brain_status(&self) -> Result<Response, RpcError> {
        let open = self
            .stores
            .tasks
            .list_open()
            .map_err(|e| RpcError::Unknown {
                message: format!("list_open failed: {e}"),
            })?
            .len() as u64;
        let in_progress = self
            .stores
            .tasks
            .list_in_progress()
            .map_err(|e| RpcError::Unknown {
                message: format!("list_in_progress failed: {e}"),
            })?
            .len() as u64;
        let blocked = self
            .stores
            .tasks
            .list_blocked()
            .map_err(|e| RpcError::Unknown {
                message: format!("list_blocked failed: {e}"),
            })?
            .len() as u64;
        let done = self
            .stores
            .tasks
            .list_done()
            .map_err(|e| RpcError::Unknown {
                message: format!("list_done failed: {e}"),
            })?
            .len() as u64;
        let stuck_files = self
            .stores
            .count_stuck_files()
            .map_err(|e| RpcError::Unknown {
                message: format!("count_stuck_files failed: {e}"),
            })?;
        let stale_hashes_prevented =
            self.stores
                .stale_hashes_prevented()
                .map_err(|e| RpcError::Unknown {
                    message: format!("stale_hashes_prevented failed: {e}"),
                })?;

        let report = BrainStatusReport {
            brain_name: self.stores.brain_name.clone(),
            brain_id: self.stores.brain_id.clone(),
            tasks_open: open,
            tasks_in_progress: in_progress,
            tasks_blocked: blocked,
            tasks_done: done,
            stuck_files,
            stale_hashes_prevented,
        };

        Ok(Response::BrainStatus { report })
    }

    // ── provider handler ─────────────────────────────────────────────────────

    fn handle_provider_list(&self) -> Result<Response, RpcError> {
        use brain_lib::ports::ProviderStore;

        let providers = self
            .stores
            .list_providers()
            .map_err(|e| RpcError::Unknown {
                message: format!("list_providers failed: {e}"),
            })?;

        let wire: Vec<ProviderSummary> = providers
            .into_iter()
            .map(|p| ProviderSummary {
                id: p.id.clone(),
                name: p.name.clone(),
                key_hash_prefix: p.api_key_hash.chars().take(8).collect(),
            })
            .collect();

        Ok(Response::ProviderList { providers: wire })
    }

    // ── file-watcher handlers ────────────────────────────────────────────────

    #[cfg(feature = "embed")]
    fn handle_watch_add(&self, path: String) -> Result<Response, RpcError> {
        let handle = self
            .watcher
            .as_ref()
            .ok_or_else(|| RpcError::Unknown {
                message: "watcher not running in this daemon".into(),
            })?
            .clone();
        let runtime = self.runtime()?;
        let outcome =
            runtime
                .block_on(handle.add(path.clone()))
                .map_err(|e| RpcError::Unknown {
                    message: format!("watch add: {e}"),
                })?;
        Ok(Response::WatchAdded {
            path,
            brain_name: outcome.brain_name,
        })
    }

    #[cfg(not(feature = "embed"))]
    fn handle_watch_add(&self, _path: String) -> Result<Response, RpcError> {
        Err(RpcError::Unknown {
            message: "this daemon was built without the embed feature".into(),
        })
    }

    #[cfg(feature = "embed")]
    fn handle_watch_remove(&self, path: String) -> Result<Response, RpcError> {
        let handle = self
            .watcher
            .as_ref()
            .ok_or_else(|| RpcError::Unknown {
                message: "watcher not running in this daemon".into(),
            })?
            .clone();
        let runtime = self.runtime()?;
        runtime
            .block_on(handle.remove(path.clone()))
            .map_err(|e| RpcError::Unknown {
                message: format!("watch remove: {e}"),
            })?;
        Ok(Response::WatchRemoved { path })
    }

    #[cfg(not(feature = "embed"))]
    fn handle_watch_remove(&self, _path: String) -> Result<Response, RpcError> {
        Err(RpcError::Unknown {
            message: "this daemon was built without the embed feature".into(),
        })
    }

    #[cfg(feature = "embed")]
    fn handle_watch_list(&self) -> Result<Response, RpcError> {
        let handle = self
            .watcher
            .as_ref()
            .ok_or_else(|| RpcError::Unknown {
                message: "watcher not running in this daemon".into(),
            })?
            .clone();
        let runtime = self.runtime()?;
        let entries = runtime
            .block_on(handle.list())
            .map_err(|e| RpcError::Unknown {
                message: format!("watch list: {e}"),
            })?;
        let watches = entries.into_iter().map(Self::watch_to_summary).collect();
        Ok(Response::WatchList { watches })
    }

    #[cfg(not(feature = "embed"))]
    fn handle_watch_list(&self) -> Result<Response, RpcError> {
        Err(RpcError::Unknown {
            message: "this daemon was built without the embed feature".into(),
        })
    }

    /// Map a supervisor-internal [`crate::watcher::control::WatchEntry`]
    /// onto the wire-format [`brain_rpc::WatchSummary`]. Mirrors the
    /// `task_to_summary` pattern — if the internal type gains fields,
    /// this is the explicit decision point for what reaches the wire.
    #[cfg(feature = "embed")]
    fn watch_to_summary(entry: crate::watcher::control::WatchEntry) -> brain_rpc::WatchSummary {
        brain_rpc::WatchSummary {
            brain_name: entry.brain_name,
            brain_id: entry.brain_id,
            note_dir: entry.note_dir,
            watching: entry.watching,
        }
    }
}

/// Convert the wire-format `importance_millis` (0..=1000) into the
/// internal 0.0..=1.0 float. Free function (not on `BrainStoresDispatcher`)
/// because the conversion is pure and shared across memory handlers.
fn millis_to_unit(millis: u32) -> f64 {
    (millis.min(1000) as f64) / 1000.0
}

impl Dispatcher for BrainStoresDispatcher {
    fn dispatch(&self, req: Request) -> Result<Response, RpcError> {
        match req {
            Request::Ping => Ok(Response::Pong),
            Request::Handshake { .. } => Ok(Response::HandshakeOk {
                server_version: PROTOCOL_VERSION,
            }),
            Request::TasksList { params } => self.handle_tasks_list(params),
            Request::TasksShow { id } => self.handle_tasks_show(id),
            Request::TasksNext => self.handle_tasks_next(),
            Request::TasksCreate { params } => self.handle_tasks_create(params),
            Request::TasksUpdate { params } => self.handle_tasks_update(params),
            Request::TasksMutate { params } => self.handle_tasks_mutate(params),
            Request::TasksAddDep {
                task_id,
                depends_on_task_id,
            } => self.handle_tasks_add_dep(task_id, depends_on_task_id),
            Request::TasksRemoveDep {
                task_id,
                depends_on_task_id,
            } => self.handle_tasks_remove_dep(task_id, depends_on_task_id),
            Request::TasksAddLabel { task_id, label } => {
                self.handle_tasks_add_label(task_id, label)
            }
            Request::TasksRemoveLabel { task_id, label } => {
                self.handle_tasks_remove_label(task_id, label)
            }
            Request::TasksTransfer { params } => self.handle_tasks_transfer(params),
            Request::RecordsVerify => self.handle_records_verify(),
            Request::AnalysesList { params } => self.handle_analyses_list(params),
            Request::AnalysesShow { id } => self.handle_analyses_show(id),
            Request::AnalysesCreate { params } => self.handle_analyses_create(params),
            Request::ArtifactsList { params } => self.handle_artifacts_list(params),
            Request::ArtifactsShow { id } => self.handle_artifacts_show(id),
            Request::DocumentsList { params } => self.handle_documents_list(params),
            Request::DocumentsShow { id } => self.handle_documents_show(id),
            Request::DocumentsCreate { params } => self.handle_documents_create(params),
            Request::PlansList { params } => self.handle_plans_list(params),
            Request::PlansShow { id } => self.handle_plans_show(id),
            Request::PlansCreate { params } => self.handle_plans_create(params),
            Request::SnapshotsList { params } => self.handle_snapshots_list(params),
            Request::SnapshotsShow { id } => self.handle_snapshots_show(id),
            Request::SnapshotsCreate { params } => self.handle_snapshots_create(params),
            Request::SagasList { params } => self.handle_sagas_list(params),
            Request::SagasGet { saga_id } => self.handle_sagas_get(saga_id),
            Request::SagasCreate { params } => self.handle_sagas_create(params),
            Request::SagasUpdate { params } => self.handle_sagas_update(params),
            Request::SagasAddTasks {
                saga_id,
                task_ids,
                cascade,
            } => self.handle_sagas_add_tasks(saga_id, task_ids, cascade),
            Request::SagasRemoveTasks {
                saga_id,
                task_ids,
                cascade,
            } => self.handle_sagas_remove_tasks(saga_id, task_ids, cascade),
            Request::SagasFrontier { saga_id } => self.handle_sagas_frontier(saga_id),
            Request::SagasStart { saga_id } => self.handle_sagas_start(saga_id),
            Request::SagasClose { saga_id, cascade } => self.handle_sagas_close(saga_id, cascade),
            Request::SagasCancel { saga_id, cascade } => self.handle_sagas_cancel(saga_id, cascade),
            Request::SagasReopen { saga_id } => self.handle_sagas_reopen(saga_id),
            Request::SagasStats { saga_id } => self.handle_sagas_stats(saga_id),
            Request::MemoryWriteEpisode { params } => self.handle_memory_write_episode(params),
            Request::MemoryWriteProcedure { params } => self.handle_memory_write_procedure(params),
            Request::MemoryRetrieve { params } => self.handle_memory_retrieve(params),
            Request::MemoryConsolidate { params } => self.handle_memory_consolidate(params),
            Request::MemorySummarizeScope { params } => self.handle_memory_summarize_scope(params),
            Request::MemoryReflect { params } => self.handle_memory_reflect(params),
            Request::TagsAliasesList { params } => self.handle_tags_aliases_list(params),
            Request::TagsAliasesStatus => self.handle_tags_aliases_status(),
            Request::JobsStatus => self.handle_jobs_status(),
            Request::BrainStatus => self.handle_brain_status(),
            Request::ProviderList => self.handle_provider_list(),
            Request::WatchAdd { path } => self.handle_watch_add(path),
            Request::WatchRemove { path } => self.handle_watch_remove(path),
            Request::WatchList => self.handle_watch_list(),

            // ── New wire variants — first wave ───────────────────────────
            // Links / records-mutation / brains-list / labels-summary /
            // memory.walk_thread are real handlers. The four heavier arms
            // (TasksApplyEvent, TasksDepsBatch, TasksLabelsBatch,
            // TagsRecluster) remain stubs pending dedicated commits.
            Request::LinksAdd { params } => self.handle_links_add(params),
            Request::LinksRemove { params } => self.handle_links_remove(params),
            Request::LinksForEntity { params } => self.handle_links_for_entity(params),
            Request::RecordsArchive { params } => self.handle_records_archive(params),
            Request::RecordsLinkAdd { params } => self.handle_records_link_add(params),
            Request::RecordsLinkRemove { params } => self.handle_records_link_remove(params),
            Request::RecordsTagAdd { record_id, tag } => {
                self.handle_records_tag_add(record_id, tag)
            }
            Request::RecordsTagRemove { record_id, tag } => {
                self.handle_records_tag_remove(record_id, tag)
            }
            Request::TasksApplyEvent { .. } => Err(RpcError::Unknown {
                message: "TasksApplyEvent: handler pending wire-up".into(),
            }),
            Request::TasksDepsBatch { params } => self.handle_tasks_deps_batch(params),
            Request::TasksLabelsBatch { params } => self.handle_tasks_labels_batch(params),
            Request::TasksLabelsSummary => self.handle_tasks_labels_summary(),
            Request::MemoryWalkThread { params } => self.handle_memory_walk_thread(params),
            Request::TagsRecluster { params } => self.handle_tags_recluster(params),
            Request::BrainsList { params } => self.handle_brains_list(params),
        }
    }
}

/// Translate the internal status enum into the wire string. Kept as a
/// free function (not a `Display` impl on `TaskStatus`) so the wire
/// strings live next to the wire types, not next to the domain types.
fn status_to_wire_string(s: &TaskStatus) -> String {
    match s {
        TaskStatus::Open => "open",
        TaskStatus::InProgress => "in_progress",
        TaskStatus::Blocked => "blocked",
        TaskStatus::Done => "done",
        TaskStatus::Cancelled => "cancelled",
    }
    .to_string()
}

/// Wrap a `memory.retrieve` JSON value into the wire `Response::MemoryRetrieve`
/// envelope. Mirrors the `result_json: String` shape the MCP layer
/// expects (the daemon serialises; the wire echoes the JSON string).
fn wrap_memory_retrieve_value(value: serde_json::Value) -> Result<Response, RpcError> {
    let result_json = serde_json::to_string(&value).map_err(|e| RpcError::Protocol {
        message: format!("serialize retrieve result: {e}"),
    })?;
    Ok(Response::MemoryRetrieve { result_json })
}

/// Map [`integrity::IntegrityReport`] onto the wire-format
/// [`RecordsVerifyReport`]. Per-finding detail lists are flattened to
/// counts — the wire surface mirrors the JSON the CLI's
/// `--json` path produces, where verbose findings are local-only.
fn record_to_verify_report(report: &integrity::IntegrityReport) -> RecordsVerifyReport {
    RecordsVerifyReport {
        clean: report.is_clean(),
        records_checked: report.records_checked as u64,
        blobs_checked: report.blobs_checked as u64,
        missing: report.missing.len() as u64,
        corrupt: report.corrupt.len() as u64,
        orphans: report.orphans.len() as u64,
        stale_flags: report.stale_flags.len() as u64,
    }
}

/// Convert a wire [`RecordsCreateParams`] into the internal
/// [`CreateRecordParams`]. Field-for-field translation point that
/// keeps the persistence type behind the daemon boundary.
fn records_create_params_to_internal(p: RecordsCreateParams) -> CreateRecordParams {
    CreateRecordParams {
        title: p.title,
        description: p.description,
        body: p.body,
        media_type: p.media_type,
        task_id: p.task_id,
        tags: p.tags,
        scope_type: None,
        scope_id: None,
        retention_class: None,
        producer: None,
        actor: "daemon".to_string(),
    }
}

/// Parse a wire status filter ("active" / "archived" / arbitrary
/// string) into [`RecordStatus`]. `None` resolves to `Active` —
/// matches the CLI default. `RecordStatus::from_str` is infallible
/// (unrecognised strings become `Unknown(s)`), so this function
/// cannot fail; the `Result` envelope is kept for symmetry with the
/// other dispatcher helpers and forward-compatibility.
fn parse_status_filter(status: Option<&str>) -> Result<RecordStatus, RpcError> {
    Ok(status
        .map(|s| s.parse::<RecordStatus>().unwrap_or(RecordStatus::Active))
        .unwrap_or(RecordStatus::Active))
}

/// Format an epoch-seconds timestamp as an RFC 3339 / ISO 8601 string.
/// The wire format uses strings (not raw i64) for timestamps per
/// project convention — see iso_timestamps feedback.
fn epoch_seconds_to_iso(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| ts.to_string())
}

// ── per-family anti-corruption mappers ─────────────────────────

fn analysis_to_summary_with_brain(record: &Record, brain_id: &str) -> AnalysisSummary {
    AnalysisSummary {
        record_id: record.record_id.clone(),
        title: record.title.clone(),
        created_at: epoch_seconds_to_iso(record.created_at),
        brain_id: brain_id.to_string(),
    }
}

fn document_to_summary_with_brain(record: &Record, brain_id: &str) -> DocumentSummary {
    DocumentSummary {
        record_id: record.record_id.clone(),
        title: record.title.clone(),
        created_at: epoch_seconds_to_iso(record.created_at),
        brain_id: brain_id.to_string(),
    }
}

fn plan_to_summary_with_brain(record: &Record, brain_id: &str) -> PlanSummary {
    PlanSummary {
        record_id: record.record_id.clone(),
        title: record.title.clone(),
        created_at: epoch_seconds_to_iso(record.created_at),
        brain_id: brain_id.to_string(),
    }
}

fn snapshot_to_summary_with_brain(record: &Record, brain_id: &str) -> SnapshotSummary {
    SnapshotSummary {
        record_id: record.record_id.clone(),
        title: record.title.clone(),
        created_at: epoch_seconds_to_iso(record.created_at),
        brain_id: brain_id.to_string(),
    }
}

// ── saga anti-corruption mappers ─────────────────────────────────

/// Map a [`Saga`] into the wire-format [`SagaSummary`]. Free function
/// because the mapping needs no daemon state — the brain context lives
/// on the saga itself via its membership table, not on the saga row.
/// The `saga_id` returned is the short user-facing form (`saga-<hex>`)
/// that mirrors the local CLI's JSON output.
fn saga_to_summary(saga: &Saga) -> SagaSummary {
    SagaSummary {
        saga_id: compact_saga_id(&saga.display_id),
        title: saga.title.clone(),
        description: saga.description.clone(),
        status: saga.status.to_string(),
        created_at: saga
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        updated_at: saga
            .updated_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        closed_at: saga
            .closed_at
            .map(|ts| ts.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
    }
}

/// Map a saga brain summary onto the wire format.
fn saga_brain_to_summary(brain: &SagaBrainDomain) -> SagaBrainSummary {
    SagaBrainSummary {
        brain_id: brain.brain_id.clone(),
        name: brain.name.clone(),
        prefix: brain.prefix.clone(),
    }
}

/// Map a saga stats aggregate onto the wire format.
fn saga_stats_to_report(stats: &SagaStats) -> SagaStatsReport {
    let c = &stats.counts;
    SagaStatsReport {
        total: c.total,
        open: c.open,
        in_progress: c.in_progress,
        blocked: c.blocked,
        done: c.done,
        cancelled: c.cancelled,
        orphan: c.orphan,
        completion_pct: c.completion_pct,
    }
}

/// Map a label-count pair onto the wire format.
fn label_count_to_wire(label: &LabelCount) -> SagaLabelCount {
    SagaLabelCount {
        label: label.label.clone(),
        count: label.count,
    }
}

// ───────── tasks.labels_batch helpers ───────────────────────────────
// Pure data transforms used by `handle_tasks_labels_batch`. Free
// functions (not methods) so the dispatcher impl block stays focused
// on request routing.

fn label_add_remove_response(
    store: &TaskStore,
    label: Option<&str>,
    task_ids: Option<&[String]>,
    event_type: EventType,
    brain_name: &str,
) -> Result<serde_json::Value, RpcError> {
    let label = label.filter(|s| !s.is_empty()).ok_or(RpcError::Protocol {
        message: "Missing required parameter: label".into(),
    })?;
    let task_ids = task_ids.ok_or(RpcError::Protocol {
        message: "Missing required parameter: task_ids".into(),
    })?;

    if task_ids.is_empty() {
        return Ok(batch_response_value(Vec::new(), Vec::new()));
    }

    let mut events = Vec::with_capacity(task_ids.len());
    let mut failed: Vec<serde_json::Value> = Vec::new();

    for raw_id in task_ids {
        let resolved_input = brain_lib::uri::resolve_id(raw_id);
        match store.resolve_task_id(&resolved_input) {
            Ok(resolved) => events.push(TaskEvent::new(
                &resolved,
                "mcp",
                event_type.clone(),
                &LabelPayload {
                    label: label.to_string(),
                },
            )),
            Err(e) => failed.push(serde_json::json!({
                "task_id": raw_id,
                "error": format!("{e}"),
            })),
        }
    }

    let results = store.append_batch(&events);
    let mut succeeded: Vec<serde_json::Value> = Vec::new();
    for (i, result) in results.into_iter().enumerate() {
        let tid = &events[i].task_id;
        let short_id = store.compact_id_or_raw(tid);
        match result {
            Ok(()) => {
                let uri = brain_lib::uri::SynapseUri::for_task(brain_name, &short_id).to_string();
                succeeded.push(serde_json::json!({ "task_id": short_id, "uri": uri }));
            }
            Err(e) => failed.push(serde_json::json!({
                "task_id": short_id,
                "error": format!("{e}"),
            })),
        }
    }

    Ok(batch_response_value(succeeded, failed))
}

fn label_rename_response(
    store: &TaskStore,
    old_label: Option<&str>,
    new_label: Option<&str>,
    brain_name: &str,
) -> Result<serde_json::Value, RpcError> {
    let old_label = old_label
        .filter(|s| !s.is_empty())
        .ok_or(RpcError::Protocol {
            message: "Missing required parameter: old_label".into(),
        })?;
    let new_label = new_label
        .filter(|s| !s.is_empty())
        .ok_or(RpcError::Protocol {
            message: "Missing required parameter: new_label".into(),
        })?;

    let task_ids = store
        .get_task_ids_with_label(old_label)
        .map_err(|e| RpcError::Unknown {
            message: format!("Failed to query tasks: {e}"),
        })?;

    if task_ids.is_empty() {
        return Ok(batch_response_value(Vec::new(), Vec::new()));
    }

    let mut events = Vec::with_capacity(task_ids.len() * 2);
    for tid in &task_ids {
        events.push(TaskEvent::new(
            tid,
            "mcp",
            EventType::LabelRemoved,
            &LabelPayload {
                label: old_label.to_string(),
            },
        ));
        events.push(TaskEvent::new(
            tid,
            "mcp",
            EventType::LabelAdded,
            &LabelPayload {
                label: new_label.to_string(),
            },
        ));
    }

    let results = store.append_batch(&events);
    let mut succeeded: Vec<serde_json::Value> = Vec::new();
    let mut failed: Vec<serde_json::Value> = Vec::new();

    // Results come in (remove, add) pairs per task — mirror the
    // original tool's pair-failure-reporting semantics.
    for (i, tid) in task_ids.iter().enumerate() {
        let remove_idx = i * 2;
        let add_idx = i * 2 + 1;
        let short_id = store.compact_id_or_raw(tid);
        let remove_ok = results[remove_idx].is_ok();
        let add_ok = results[add_idx].is_ok();

        if remove_ok && add_ok {
            let uri = brain_lib::uri::SynapseUri::for_task(brain_name, &short_id).to_string();
            succeeded.push(serde_json::json!({ "task_id": short_id, "uri": uri }));
        } else {
            let mut errors = Vec::new();
            if let Err(e) = &results[remove_idx] {
                errors.push(format!("remove: {e}"));
            }
            if let Err(e) = &results[add_idx] {
                errors.push(format!("add: {e}"));
            }
            failed.push(serde_json::json!({
                "task_id": short_id,
                "error": errors.join("; "),
            }));
        }
    }

    Ok(batch_response_value(succeeded, failed))
}

fn label_purge_response(
    store: &TaskStore,
    label: Option<&str>,
    brain_name: &str,
) -> Result<serde_json::Value, RpcError> {
    let label = label.filter(|s| !s.is_empty()).ok_or(RpcError::Protocol {
        message: "Missing required parameter: label".into(),
    })?;

    let task_ids = store
        .get_task_ids_with_label(label)
        .map_err(|e| RpcError::Unknown {
            message: format!("Failed to query tasks: {e}"),
        })?;

    if task_ids.is_empty() {
        return Ok(batch_response_value(Vec::new(), Vec::new()));
    }

    let events: Vec<TaskEvent> = task_ids
        .iter()
        .map(|tid| {
            TaskEvent::new(
                tid,
                "mcp",
                EventType::LabelRemoved,
                &LabelPayload {
                    label: label.to_string(),
                },
            )
        })
        .collect();

    let results = store.append_batch(&events);
    let mut succeeded: Vec<serde_json::Value> = Vec::new();
    let mut failed: Vec<serde_json::Value> = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        let tid = &task_ids[i];
        let short_id = store.compact_id_or_raw(tid);
        match result {
            Ok(()) => {
                let uri = brain_lib::uri::SynapseUri::for_task(brain_name, &short_id).to_string();
                succeeded.push(serde_json::json!({ "task_id": short_id, "uri": uri }));
            }
            Err(e) => failed.push(serde_json::json!({
                "task_id": short_id,
                "error": format!("{e}"),
            })),
        }
    }

    Ok(batch_response_value(succeeded, failed))
}

fn batch_response_value(
    succeeded: Vec<serde_json::Value>,
    failed: Vec<serde_json::Value>,
) -> serde_json::Value {
    let succeeded_count = succeeded.len();
    let failed_count = failed.len();
    serde_json::json!({
        "succeeded": succeeded,
        "failed": failed,
        "summary": {
            "succeeded": succeeded_count,
            "failed": failed_count,
        },
    })
}

// ───────── tasks.deps_batch helpers ─────────────────────────────────
// Action sub-handlers for `handle_tasks_deps_batch`. Mirrors the
// original tool body's add/remove/chain/fan/clear split. Sequential
// `append` per event is intentional — cycle detection in
// `TaskStore::append` reads accumulated state.

fn deps_pairs_response(
    store: &TaskStore,
    pairs: Option<Vec<(&str, &str)>>,
    event_type: EventType,
    brain_name: &str,
) -> Result<serde_json::Value, RpcError> {
    let pairs = pairs.ok_or(RpcError::Protocol {
        message: "Missing required parameter: pairs".into(),
    })?;

    if pairs.is_empty() {
        return Ok(deps_batch_response_value(
            Vec::new(),
            Vec::new(),
            brain_name,
        ));
    }

    let mut succeeded: Vec<serde_json::Value> = Vec::new();
    let mut failed: Vec<serde_json::Value> = Vec::new();

    for (raw_task, raw_depends_on) in pairs {
        let task_input = brain_lib::uri::resolve_id(raw_task);
        let task_id = match store.resolve_task_id(&task_input) {
            Ok(id) => id,
            Err(e) => {
                failed.push(serde_json::json!({
                    "task_id": raw_task,
                    "depends_on_task_id": raw_depends_on,
                    "error": format!("{e}"),
                }));
                continue;
            }
        };
        let depends_input = brain_lib::uri::resolve_id(raw_depends_on);
        let depends_on = match store.resolve_task_id(&depends_input) {
            Ok(id) => id,
            Err(e) => {
                failed.push(serde_json::json!({
                    "task_id": raw_task,
                    "depends_on_task_id": raw_depends_on,
                    "error": format!("{e}"),
                }));
                continue;
            }
        };

        let event = TaskEvent::new(
            &task_id,
            "mcp",
            event_type.clone(),
            &DependencyPayload {
                depends_on_task_id: depends_on.clone(),
            },
        );

        let short_task = store.compact_id_or_raw(&task_id);
        let short_depends = store.compact_id_or_raw(&depends_on);
        match store.append(&event) {
            Ok(()) => succeeded.push(serde_json::json!({
                "task_id": short_task,
                "depends_on_task_id": short_depends,
            })),
            Err(e) => failed.push(serde_json::json!({
                "task_id": short_task,
                "depends_on_task_id": short_depends,
                "error": format!("{e}"),
            })),
        }
    }

    Ok(deps_batch_response_value(succeeded, failed, brain_name))
}

fn deps_chain_response(
    store: &TaskStore,
    task_ids: Option<&[String]>,
    brain_name: &str,
) -> Result<serde_json::Value, RpcError> {
    let task_ids = task_ids.ok_or(RpcError::Protocol {
        message: "Missing required parameter: task_ids".into(),
    })?;

    if task_ids.len() < 2 {
        return Err(RpcError::Protocol {
            message: "chain requires at least 2 task IDs".into(),
        });
    }

    let mut resolved: Vec<String> = Vec::with_capacity(task_ids.len());
    let mut failed: Vec<serde_json::Value> = Vec::new();
    for raw_id in task_ids {
        let input = brain_lib::uri::resolve_id(raw_id);
        match store.resolve_task_id(&input) {
            Ok(id) => resolved.push(id),
            Err(e) => failed.push(serde_json::json!({
                "task_id": raw_id,
                "error": format!("{e}"),
            })),
        }
    }

    // Can't build a chain with missing tasks — report all as failed.
    if !failed.is_empty() {
        return Ok(deps_batch_response_value(Vec::new(), failed, brain_name));
    }

    let mut succeeded: Vec<serde_json::Value> = Vec::new();
    for i in 1..resolved.len() {
        let task_id = &resolved[i];
        let depends_on = &resolved[i - 1];

        let event = TaskEvent::new(
            task_id,
            "mcp",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: depends_on.clone(),
            },
        );

        let short_task = store.compact_id_or_raw(task_id);
        let short_depends = store.compact_id_or_raw(depends_on);
        match store.append(&event) {
            Ok(()) => succeeded.push(serde_json::json!({
                "task_id": short_task,
                "depends_on_task_id": short_depends,
            })),
            Err(e) => failed.push(serde_json::json!({
                "task_id": short_task,
                "depends_on_task_id": short_depends,
                "error": format!("{e}"),
            })),
        }
    }

    Ok(deps_batch_response_value(succeeded, failed, brain_name))
}

fn deps_fan_response(
    store: &TaskStore,
    source: Option<&str>,
    dependents: Option<&[String]>,
    brain_name: &str,
) -> Result<serde_json::Value, RpcError> {
    let source = source.filter(|s| !s.is_empty()).ok_or(RpcError::Protocol {
        message: "Missing required parameter: source_task_id".into(),
    })?;
    let dependents = dependents.ok_or(RpcError::Protocol {
        message: "Missing required parameter: dependent_task_ids".into(),
    })?;

    if dependents.is_empty() {
        return Ok(deps_batch_response_value(
            Vec::new(),
            Vec::new(),
            brain_name,
        ));
    }

    let source_input = brain_lib::uri::resolve_id(source);
    let source_resolved = store
        .resolve_task_id(&source_input)
        .map_err(|e| RpcError::Unknown {
            message: format!("Failed to resolve source_task_id: {e}"),
        })?;
    let source_compact = store.compact_id_or_raw(&source_resolved);

    let mut succeeded: Vec<serde_json::Value> = Vec::new();
    let mut failed: Vec<serde_json::Value> = Vec::new();

    for raw_id in dependents {
        let dep_input = brain_lib::uri::resolve_id(raw_id);
        let dep_id = match store.resolve_task_id(&dep_input) {
            Ok(id) => id,
            Err(e) => {
                failed.push(serde_json::json!({
                    "task_id": raw_id,
                    "depends_on_task_id": source_compact,
                    "error": format!("{e}"),
                }));
                continue;
            }
        };

        let event = TaskEvent::new(
            &dep_id,
            "mcp",
            EventType::DependencyAdded,
            &DependencyPayload {
                depends_on_task_id: source_resolved.clone(),
            },
        );

        let short_dep = store.compact_id_or_raw(&dep_id);
        match store.append(&event) {
            Ok(()) => succeeded.push(serde_json::json!({
                "task_id": short_dep,
                "depends_on_task_id": source_compact,
            })),
            Err(e) => failed.push(serde_json::json!({
                "task_id": short_dep,
                "depends_on_task_id": source_compact,
                "error": format!("{e}"),
            })),
        }
    }

    Ok(deps_batch_response_value(succeeded, failed, brain_name))
}

fn deps_clear_response(
    store: &TaskStore,
    task_id: Option<&str>,
    brain_name: &str,
) -> Result<serde_json::Value, RpcError> {
    let task_id = task_id
        .filter(|s| !s.is_empty())
        .ok_or(RpcError::Protocol {
            message: "Missing required parameter: task_id".into(),
        })?;

    let input = brain_lib::uri::resolve_id(task_id);
    let resolved = store
        .resolve_task_id(&input)
        .map_err(|e| RpcError::Unknown {
            message: format!("Failed to resolve task_id: {e}"),
        })?;

    let deps = store
        .get_deps_for_task(&resolved)
        .map_err(|e| RpcError::Unknown {
            message: format!("Failed to query dependencies: {e}"),
        })?;

    if deps.is_empty() {
        return Ok(deps_batch_response_value(
            Vec::new(),
            Vec::new(),
            brain_name,
        ));
    }

    let events: Vec<TaskEvent> = deps
        .iter()
        .map(|dep| {
            TaskEvent::new(
                &resolved,
                "mcp",
                EventType::DependencyRemoved,
                &DependencyPayload {
                    depends_on_task_id: dep.clone(),
                },
            )
        })
        .collect();

    let results = store.append_batch(&events);
    let mut succeeded: Vec<serde_json::Value> = Vec::new();
    let mut failed: Vec<serde_json::Value> = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        let short_resolved = store.compact_id_or_raw(&resolved);
        let short_dep = store.compact_id_or_raw(&deps[i]);
        match result {
            Ok(()) => succeeded.push(serde_json::json!({
                "task_id": short_resolved,
                "depends_on_task_id": short_dep,
            })),
            Err(e) => failed.push(serde_json::json!({
                "task_id": short_resolved,
                "depends_on_task_id": short_dep,
                "error": format!("{e}"),
            })),
        }
    }

    Ok(deps_batch_response_value(succeeded, failed, brain_name))
}

fn deps_batch_response_value(
    mut succeeded: Vec<serde_json::Value>,
    failed: Vec<serde_json::Value>,
    brain_name: &str,
) -> serde_json::Value {
    // Inject URIs into every succeeded entry that carries a task_id.
    // Mirrors the original tool's post-process behaviour exactly so
    // the wire-shape stays byte-identical.
    for item in &mut succeeded {
        if let Some(obj) = item.as_object_mut()
            && let Some(task_id) = obj
                .get("task_id")
                .and_then(|v| v.as_str())
                .map(String::from)
        {
            let uri = brain_lib::uri::SynapseUri::for_task(brain_name, &task_id).to_string();
            obj.insert("uri".into(), serde_json::json!(uri));
        }
    }
    let succeeded_count = succeeded.len();
    let failed_count = failed.len();
    serde_json::json!({
        "succeeded": succeeded,
        "failed": failed,
        "summary": {
            "succeeded": succeeded_count,
            "failed": failed_count,
        },
    })
}

/// Drive a future to completion under the assumption that it never
/// yields. Used by [`BrainStoresDispatcher::handle_tasks_transfer`] for
/// `TaskStore::transfer_task` with `vector_store: None` — that path
/// only does synchronous `with_write_conn` work and never awaits
/// anything that could pend.
///
/// Panics if the future returns `Pending` — that means an assumption
/// upstream broke, and we'd rather fail loudly than hang the daemon.
fn block_on_no_yield<F: std::future::Future>(future: F) -> F::Output {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    // SAFETY: the no-op vtable holds no state; `clone` returns the same
    // singleton, `wake` / `wake_by_ref` / `drop` are inert. Standard
    // pattern for a "we promise this future never yields" executor.
    const VTABLE: &RawWakerVTable = &RawWakerVTable::new(
        |_| RawWaker::new(std::ptr::null(), VTABLE),
        |_| {},
        |_| {},
        |_| {},
    );
    let raw = RawWaker::new(std::ptr::null(), VTABLE);
    let waker = unsafe { Waker::from_raw(raw) };
    let mut cx = Context::from_waker(&waker);

    let mut future = Box::pin(future);
    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(out) => out,
        Poll::Pending => {
            panic!(
                "block_on_no_yield: future yielded — this call site assumes \
                 a synchronous path (TaskStore::transfer_task with vector_store=None). \
                 An async branch crept in; replace this helper with a real runtime."
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dispatcher_with_empty_store() -> (tempfile::TempDir, BrainStoresDispatcher) {
        let (tmp, stores) = BrainStores::in_memory().expect("in_memory stores");
        #[cfg(feature = "embed")]
        let dispatcher = BrainStoresDispatcher::new(stores, None);
        #[cfg(not(feature = "embed"))]
        let dispatcher = BrainStoresDispatcher::new(stores);
        (tmp, dispatcher)
    }

    #[test]
    fn dispatch_ping_returns_pong() {
        let (_tmp, d) = dispatcher_with_empty_store();
        assert_eq!(d.dispatch(Request::Ping).unwrap(), Response::Pong);
    }

    #[test]
    fn dispatch_handshake_returns_handshake_ok() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d
            .dispatch(Request::Handshake {
                version: PROTOCOL_VERSION,
            })
            .unwrap();
        match res {
            Response::HandshakeOk { server_version } => {
                assert_eq!(server_version, PROTOCOL_VERSION);
            }
            other => panic!("expected HandshakeOk, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_list_empty_store() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d
            .dispatch(Request::TasksList {
                params: TasksListParams::default(),
            })
            .unwrap();
        match res {
            Response::TasksList { tasks } => assert!(tasks.is_empty()),
            other => panic!("expected TasksList, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_list_rejects_unknown_status_filter() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d.dispatch(Request::TasksList {
            params: TasksListParams {
                status: Some("bogus".into()),
                ..TasksListParams::default()
            },
        });
        match res {
            Err(RpcError::Protocol { message }) => {
                assert!(
                    message.contains("bogus"),
                    "error should mention the bad value, got: {message}"
                );
            }
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_show_returns_none_for_missing_task() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d
            .dispatch(Request::TasksShow {
                id: "no-such-task".into(),
            })
            .unwrap();
        match res {
            Response::TasksShow { task } => assert!(task.is_none()),
            other => panic!("expected TasksShow, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_next_returns_none_on_empty_store() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d.dispatch(Request::TasksNext).unwrap();
        match res {
            Response::TasksNext { task } => assert!(task.is_none()),
            other => panic!("expected TasksNext, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_mutate_rejects_unknown_action() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d.dispatch(Request::TasksMutate {
            params: TasksMutateParams {
                id: "x".into(),
                action: "bogus".into(),
            },
        });
        match res {
            Err(RpcError::Protocol { message }) => {
                assert!(
                    message.contains("bogus"),
                    "error should mention the bad action, got: {message}"
                );
            }
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_create_rejects_invalid_task_type() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d.dispatch(Request::TasksCreate {
            params: TasksCreateParams {
                title: "t".into(),
                description: None,
                priority: 2,
                task_type: "bogus".into(),
                assignee: None,
                parent: None,
            },
        });
        match res {
            Err(RpcError::Protocol { message }) => {
                assert!(message.contains("task_type"));
            }
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_update_returns_not_found_for_missing_task() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d.dispatch(Request::TasksUpdate {
            params: TasksUpdateParams {
                id: "no-such-task".into(),
                title: Some("renamed".into()),
                description: None,
                priority: None,
                assignee: None,
            },
        });
        match res {
            Err(RpcError::NotFound { id }) => assert_eq!(id, "no-such-task"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_add_dep_returns_not_found_for_missing_task() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d.dispatch(Request::TasksAddDep {
            task_id: "missing-a".into(),
            depends_on_task_id: "missing-b".into(),
        });
        match res {
            Err(RpcError::NotFound { .. }) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_add_label_returns_not_found_for_missing_task() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d.dispatch(Request::TasksAddLabel {
            task_id: "missing".into(),
            label: "blocked".into(),
        });
        match res {
            Err(RpcError::NotFound { .. }) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_remove_label_returns_not_found_for_missing_task() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d.dispatch(Request::TasksRemoveLabel {
            task_id: "missing".into(),
            label: "blocked".into(),
        });
        match res {
            Err(RpcError::NotFound { .. }) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_tasks_transfer_returns_not_found_for_missing_task() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d.dispatch(Request::TasksTransfer {
            params: TasksTransferParams {
                task_id: "missing".into(),
                target_brain: "other".into(),
            },
        });
        match res {
            Err(RpcError::NotFound { .. }) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn status_to_wire_string_covers_all_variants() {
        // Compile-time-ish check: if a new TaskStatus variant lands,
        // status_to_wire_string's match becomes non-exhaustive and
        // this test fails to compile.
        assert_eq!(status_to_wire_string(&TaskStatus::Open), "open");
        assert_eq!(
            status_to_wire_string(&TaskStatus::InProgress),
            "in_progress"
        );
        assert_eq!(status_to_wire_string(&TaskStatus::Blocked), "blocked");
        assert_eq!(status_to_wire_string(&TaskStatus::Done), "done");
        assert_eq!(status_to_wire_string(&TaskStatus::Cancelled), "cancelled");
    }

    // ── sagas dispatcher tests ─────────────────────────────────

    #[test]
    fn dispatch_sagas_list_empty_store() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d
            .dispatch(Request::SagasList {
                params: SagasListParams::default(),
            })
            .unwrap();
        match res {
            Response::SagasList { sagas } => assert!(sagas.is_empty()),
            other => panic!("expected SagasList, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_sagas_get_returns_none_for_missing_saga() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let res = d
            .dispatch(Request::SagasGet {
                saga_id: "no-such-saga".into(),
            })
            .unwrap();
        match res {
            Response::SagasGet { saga } => assert!(saga.is_none()),
            other => panic!("expected SagasGet, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_sagas_create_then_get_roundtrips() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let created = d
            .dispatch(Request::SagasCreate {
                params: SagasCreateParams {
                    title: "wire saga".into(),
                    description: Some("desc".into()),
                },
            })
            .unwrap();
        let summary = match created {
            Response::SagasCreate { saga } => saga,
            other => panic!("expected SagasCreate, got {other:?}"),
        };
        assert_eq!(summary.title, "wire saga");
        assert_eq!(summary.status, "planning");
        assert!(summary.saga_id.starts_with("saga-"));

        // Round-trip lookup.
        let got = d
            .dispatch(Request::SagasGet {
                saga_id: summary.saga_id.clone(),
            })
            .unwrap();
        match got {
            Response::SagasGet { saga: Some(s) } => {
                assert_eq!(s.saga_id, summary.saga_id);
                assert_eq!(s.title, "wire saga");
            }
            other => panic!("expected SagasGet Some, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_sagas_update_clear_description() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let created = d
            .dispatch(Request::SagasCreate {
                params: SagasCreateParams {
                    title: "t".into(),
                    description: Some("desc".into()),
                },
            })
            .unwrap();
        let saga_id = match created {
            Response::SagasCreate { saga } => saga.saga_id,
            other => panic!("expected SagasCreate, got {other:?}"),
        };
        let updated = d
            .dispatch(Request::SagasUpdate {
                params: SagasUpdateParams {
                    saga_id,
                    title: None,
                    description: Some(SagaDescriptionUpdate::Clear),
                },
            })
            .unwrap();
        match updated {
            Response::SagasUpdate { saga } => assert!(saga.description.is_none()),
            other => panic!("expected SagasUpdate, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_sagas_start_transitions_to_open() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let created = d
            .dispatch(Request::SagasCreate {
                params: SagasCreateParams {
                    title: "t".into(),
                    description: None,
                },
            })
            .unwrap();
        let saga_id = match created {
            Response::SagasCreate { saga } => saga.saga_id,
            other => panic!("expected SagasCreate, got {other:?}"),
        };
        let started = d
            .dispatch(Request::SagasStart {
                saga_id: saga_id.clone(),
            })
            .unwrap();
        match started {
            Response::SagasStart { saga } => {
                assert_eq!(saga.status, "open");
                assert_eq!(saga.saga_id, saga_id);
            }
            other => panic!("expected SagasStart, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_sagas_close_then_reopen() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let created = d
            .dispatch(Request::SagasCreate {
                params: SagasCreateParams {
                    title: "t".into(),
                    description: None,
                },
            })
            .unwrap();
        let saga_id = match created {
            Response::SagasCreate { saga } => saga.saga_id,
            other => panic!("expected SagasCreate, got {other:?}"),
        };
        d.dispatch(Request::SagasStart {
            saga_id: saga_id.clone(),
        })
        .unwrap();
        let closed = d
            .dispatch(Request::SagasClose {
                saga_id: saga_id.clone(),
                cascade: false,
            })
            .unwrap();
        match closed {
            Response::SagasClose {
                saga,
                cascade,
                cascade_results,
            } => {
                assert_eq!(saga.status, "closed");
                assert!(!cascade);
                assert!(cascade_results.is_empty());
            }
            other => panic!("expected SagasClose, got {other:?}"),
        }
        let reopened = d
            .dispatch(Request::SagasReopen {
                saga_id: saga_id.clone(),
            })
            .unwrap();
        match reopened {
            Response::SagasReopen { saga } => assert_eq!(saga.status, "open"),
            other => panic!("expected SagasReopen, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_sagas_cancel_returns_summary() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let created = d
            .dispatch(Request::SagasCreate {
                params: SagasCreateParams {
                    title: "t".into(),
                    description: None,
                },
            })
            .unwrap();
        let saga_id = match created {
            Response::SagasCreate { saga } => saga.saga_id,
            other => panic!("expected SagasCreate, got {other:?}"),
        };
        d.dispatch(Request::SagasStart {
            saga_id: saga_id.clone(),
        })
        .unwrap();
        let cancelled = d
            .dispatch(Request::SagasCancel {
                saga_id,
                cascade: false,
            })
            .unwrap();
        match cancelled {
            Response::SagasCancel { saga, .. } => assert_eq!(saga.status, "cancelled"),
            other => panic!("expected SagasCancel, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_sagas_stats_empty_saga() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let created = d
            .dispatch(Request::SagasCreate {
                params: SagasCreateParams {
                    title: "t".into(),
                    description: None,
                },
            })
            .unwrap();
        let saga_id = match created {
            Response::SagasCreate { saga } => saga.saga_id,
            other => panic!("expected SagasCreate, got {other:?}"),
        };
        let stats = d.dispatch(Request::SagasStats { saga_id }).unwrap();
        match stats {
            Response::SagasStats {
                stats,
                label_histogram,
                brains,
                ..
            } => {
                assert_eq!(stats.total, 0);
                assert!(label_histogram.is_empty());
                assert!(brains.is_empty());
            }
            other => panic!("expected SagasStats, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_sagas_frontier_planning_returns_empty() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let created = d
            .dispatch(Request::SagasCreate {
                params: SagasCreateParams {
                    title: "t".into(),
                    description: None,
                },
            })
            .unwrap();
        let saga_id = match created {
            Response::SagasCreate { saga } => saga.saga_id,
            other => panic!("expected SagasCreate, got {other:?}"),
        };
        // Saga is in `planning` — frontier must be empty by contract.
        let res = d.dispatch(Request::SagasFrontier { saga_id }).unwrap();
        match res {
            Response::SagasFrontier {
                saga_status,
                tasks,
                brains,
                ..
            } => {
                assert_eq!(saga_status, "planning");
                assert!(tasks.is_empty());
                assert!(brains.is_empty());
            }
            other => panic!("expected SagasFrontier, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_sagas_add_tasks_empty_batch_is_noop() {
        let (_tmp, d) = dispatcher_with_empty_store();
        let created = d
            .dispatch(Request::SagasCreate {
                params: SagasCreateParams {
                    title: "t".into(),
                    description: None,
                },
            })
            .unwrap();
        let saga_id = match created {
            Response::SagasCreate { saga } => saga.saga_id,
            other => panic!("expected SagasCreate, got {other:?}"),
        };
        let res = d
            .dispatch(Request::SagasAddTasks {
                saga_id,
                task_ids: vec![],
                cascade: false,
            })
            .unwrap();
        match res {
            Response::SagasAddTasks {
                added,
                added_task_ids,
                ..
            } => {
                assert_eq!(added, 0);
                assert!(added_task_ids.is_empty());
            }
            other => panic!("expected SagasAddTasks, got {other:?}"),
        }
    }
}
