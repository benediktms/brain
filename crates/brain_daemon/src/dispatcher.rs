//! `Dispatcher` port + `DefaultDispatcher` adapter (logic-only —
//! still port-layer pure).
//!
//! # Hexagonal role
//!
//! `Dispatcher` is the server-side mirror of [`brain_rpc::Transport`].
//! The trait's signature deliberately contains zero I/O types: it maps
//! a [`Request`] to a [`Response`] (or an [`RpcError`]). Concrete I/O
//! — accept loop, framing, socket lifecycle — lives in
//! [`crate::server::UnixSocketServer`], which holds a `D: Dispatcher`
//! and invokes it per incoming request.
//!
//! Because the trait surface is I/O-free, the production dispatcher
//! (and any future variants — recording, rate-limiting, auth wrapper)
//! can be unit-tested by calling [`Dispatcher::dispatch`] directly with
//! synthetic requests.
//!
//! # Why `&self`, not `&mut self`
//!
//! Multiple incoming connections must be allowed to dispatch in
//! parallel. `&self` keeps dispatchers `Sync`-friendly so the server
//! can share one `D: Dispatcher` across worker threads. Any state a
//! specific dispatcher needs (counters, caches) goes behind interior
//! mutability (`AtomicX`, `Mutex<…>`) inside the impl, not as a
//! method-level `&mut self` bound.

use brain_rpc::{PROTOCOL_VERSION, Request, Response, RpcError};

/// Server-side handler port: given a [`Request`], produce a
/// [`Response`] (or an [`RpcError`]).
///
/// Implementations are I/O-free by contract — they receive an
/// already-deserialized `Request` and return a logical `Response`.
/// All framing, socket lifecycle, and concurrency happen in the
/// transport adapter that wraps the dispatcher.
pub trait Dispatcher {
    /// Dispatch one request and return the response. Errors surface
    /// as [`RpcError`] — the wire-format error type that gets
    /// serialized back to the client by the transport adapter.
    fn dispatch(&self, req: Request) -> Result<Response, RpcError>;
}

/// The MVP production dispatcher. Handles only the two `Request`
/// variants the wire protocol currently defines:
///
/// - [`Request::Ping`] → [`Response::Pong`]
/// - [`Request::Handshake`] → [`Response::HandshakeOk`] with the
///   server's [`brain_rpc::PROTOCOL_VERSION`].
///
/// The match is intentionally exhaustive (no `_ => …` arm) so adding
/// a new `Request` variant later forces a compile error here — the
/// right pressure to extend the dispatcher in lockstep with the
/// protocol surface.
pub struct DefaultDispatcher;

impl Dispatcher for DefaultDispatcher {
    fn dispatch(&self, req: Request) -> Result<Response, RpcError> {
        match req {
            Request::Ping => Ok(Response::Pong),
            Request::Handshake { .. } => Ok(Response::HandshakeOk {
                server_version: PROTOCOL_VERSION,
            }),
            // DefaultDispatcher deliberately doesn't reach into BrainStores
            // (no brain_lib dep) — that's `BrainStoresDispatcher`'s job in
            // crate::handlers. Surface a clear error for every tasks_* op
            // so a misconfigured daemon (started without DB args) fails
            // loudly rather than silently dropping requests.
            Request::TasksList { .. }
            | Request::TasksShow { .. }
            | Request::TasksNext
            | Request::TasksCreate { .. }
            | Request::TasksUpdate { .. }
            | Request::TasksMutate { .. }
            | Request::TasksAddDep { .. }
            | Request::TasksRemoveDep { .. }
            | Request::TasksAddLabel { .. }
            | Request::TasksRemoveLabel { .. }
            | Request::TasksTransfer { .. }
            | Request::RecordsVerify
            | Request::AnalysesList { .. }
            | Request::AnalysesShow { .. }
            | Request::AnalysesCreate { .. }
            | Request::ArtifactsList { .. }
            | Request::ArtifactsShow { .. }
            | Request::DocumentsList { .. }
            | Request::DocumentsShow { .. }
            | Request::DocumentsCreate { .. }
            | Request::PlansList { .. }
            | Request::PlansShow { .. }
            | Request::PlansCreate { .. }
            | Request::SnapshotsList { .. }
            | Request::SnapshotsShow { .. }
            | Request::SnapshotsCreate { .. }
            | Request::SagasList { .. }
            | Request::SagasGet { .. }
            | Request::SagasCreate { .. }
            | Request::SagasUpdate { .. }
            | Request::SagasAddTasks { .. }
            | Request::SagasRemoveTasks { .. }
            | Request::SagasFrontier { .. }
            | Request::SagasStart { .. }
            | Request::SagasClose { .. }
            | Request::SagasCancel { .. }
            | Request::SagasReopen { .. }
            | Request::SagasStats { .. }
            | Request::MemoryWriteEpisode { .. }
            | Request::MemoryWriteProcedure { .. }
            | Request::MemoryRetrieve { .. }
            | Request::MemoryConsolidate { .. }
            | Request::MemorySummarizeScope { .. }
            | Request::MemoryReflect { .. }
            | Request::TagsAliasesList { .. }
            | Request::TagsAliasesStatus
            | Request::JobsStatus { .. }
            | Request::BrainStatus
            | Request::ProviderList
            | Request::LinksAdd { .. }
            | Request::LinksRemove { .. }
            | Request::LinksForEntity { .. }
            | Request::RecordsArchive { .. }
            | Request::RecordsLinkAdd { .. }
            | Request::RecordsLinkRemove { .. }
            | Request::RecordsTagAdd { .. }
            | Request::RecordsTagRemove { .. }
            | Request::RecordsSearch { .. }
            | Request::RecordsFetchContent { .. }
            | Request::TasksApplyEvent { .. }
            | Request::TasksDepsBatch { .. }
            | Request::TasksLabelsBatch { .. }
            | Request::TasksLabelsSummary
            | Request::MemoryWalkThread { .. }
            | Request::TagsRecluster { .. }
            | Request::BrainsList { .. }
            | Request::JobsRetry { .. }
            | Request::JobsGc { .. }
            | Request::ProviderSet { .. }
            | Request::ProviderRemove { .. } => Err(RpcError::Unknown {
                message: "tasks_* / records_* / <kind>_* / sagas_* / memory_* / tags_* / \
                          jobs_* / status / provider_* / links_* / brains_* requests not \
                          handled by DefaultDispatcher — start brain-daemon with \
                          --sqlite-db and --lance-db to use the BrainStores-backed \
                          dispatcher"
                    .into(),
            }),
            Request::WatchAdd { path } => Err(RpcError::Unknown {
                message: format!("DefaultDispatcher does not handle WatchAdd: path={path}"),
            }),
            Request::WatchRemove { path } => Err(RpcError::Unknown {
                message: format!("DefaultDispatcher does not handle WatchRemove: path={path}"),
            }),
            Request::WatchList => Err(RpcError::Unknown {
                message: "DefaultDispatcher does not handle WatchList".into(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_handles_ping() {
        let d = DefaultDispatcher;
        assert_eq!(d.dispatch(Request::Ping).unwrap(), Response::Pong);
    }

    #[test]
    fn dispatch_handles_handshake() {
        let d = DefaultDispatcher;
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
    fn dispatch_handshake_ignores_client_version_field() {
        // The handshake response carries the *server's* version, not
        // an echo of the client's. This locks that contract.
        let d = DefaultDispatcher;
        let res = d
            .dispatch(Request::Handshake {
                version: 999, // client claims a future version
            })
            .unwrap();
        match res {
            Response::HandshakeOk { server_version } => {
                assert_eq!(server_version, PROTOCOL_VERSION);
                assert_ne!(server_version, 999);
            }
            other => panic!("expected HandshakeOk, got {other:?}"),
        }
    }

    #[test]
    fn default_dispatcher_implements_dispatcher_via_dyn() {
        // Compile-time check: Dispatcher is dyn-compatible. Important
        // for future code that wants Box<dyn Dispatcher> (e.g., a
        // server that swaps the handler at runtime via config).
        let d: Box<dyn Dispatcher> = Box::new(DefaultDispatcher);
        assert_eq!(d.dispatch(Request::Ping).unwrap(), Response::Pong);
    }
}
