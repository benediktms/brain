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
use brain_rpc::{PROTOCOL_VERSION, Request, Response, RpcError, TaskSummary, TasksListParams};
use brain_tasks::Task;
use brain_tasks::events::TaskStatus;

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
}

impl Dispatcher for BrainStoresDispatcher {
    fn dispatch(&self, req: Request) -> Result<Response, RpcError> {
        match req {
            Request::Ping => Ok(Response::Pong),
            Request::Handshake { .. } => Ok(Response::HandshakeOk {
                server_version: PROTOCOL_VERSION,
            }),
            Request::TasksList { params } => self.handle_tasks_list(params),
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
