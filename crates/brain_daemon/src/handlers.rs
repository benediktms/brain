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

use brain_lib::stores::BrainStores;
use brain_records::{
    CreateRecordParams, Record, RecordKind, RecordQuery, RecordStatus, RecordStore, integrity,
};
use brain_rpc::{
    AnalysisSummary, ArtifactSummary, ArtifactsListParams, DocumentSummary, PROTOCOL_VERSION,
    PlanSummary, RecordsCreateParams, RecordsListParams, RecordsVerifyReport, Request, Response,
    RpcError, SnapshotSummary, TaskSummary, TasksCreateParams, TasksListParams, TasksMutateParams,
    TasksTransferParams, TasksUpdateParams,
};
use brain_tasks::Task;
use brain_tasks::events::{
    DependencyPayload, EventType, LabelPayload, StatusChangedPayload, TaskCreatedPayload,
    TaskEvent, TaskStatus, TaskType, TaskUpdatedPayload,
};
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
}

impl BrainStoresDispatcher {
    pub fn new(stores: BrainStores) -> Self {
        Self { stores }
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
        (tmp, BrainStoresDispatcher::new(stores))
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
}
