//! Test-only mock implementations of [`Transport`] (and, later,
//! [`crate::spawner::DaemonSpawner`]).
//!
//! Compiled only when `cfg(test)` (for in-crate unit tests) or the
//! `test-utils` feature is enabled (for downstream integration tests). The
//! crate's `[dev-dependencies]` self-reference activates the feature for
//! integration tests in `tests/*.rs`; downstream consumers (`brain_daemon`,
//! `cli`, `brain_mcp`) can opt in the same way once they exist.
//!
//! Mocks live here — not in production modules — so the architectural
//! ratchet stays sharp. The forbidden-import grep gate runs against
//! `src/` as a whole, but mocks that pretend to be a socket would still
//! tempt someone to add an `io::Error` field. Putting them under
//! `#[cfg(...)]` and giving them their own file isolates the temptation.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use crate::domain::{PROTOCOL_VERSION, Request, Response, RpcError};
#[cfg(unix)]
use crate::spawner::DaemonSpawner;
use crate::transport::Transport;

/// In-memory [`Transport`] that dispatches requests to a user-provided
/// closure. No sockets, no serialization, no framing — the closure receives
/// a typed `Request` and returns a typed `Response`.
///
/// # Why a closure instead of a fixed response queue
///
/// A closure handler lets a single test cover branchy server logic:
/// "respond Pong to Ping, return NotFound for any other request". A
/// pre-canned `Vec<Response>` queue forces every test to enumerate the call
/// sequence, which makes refactors of the client code painful.
pub struct InMemoryTransport {
    handler: Box<dyn FnMut(Request) -> Result<Response, RpcError>>,
}

impl InMemoryTransport {
    /// Construct an `InMemoryTransport` whose `call` invocations are
    /// dispatched to `handler`. The handler is `FnMut`, so tests can keep
    /// mutable state (a counter, a request log) across calls.
    pub fn new<F>(handler: F) -> Self
    where
        F: FnMut(Request) -> Result<Response, RpcError> + 'static,
    {
        Self {
            handler: Box::new(handler),
        }
    }

    /// Convenience constructor: a transport that responds like a healthy
    /// daemon at [`PROTOCOL_VERSION`]. Ping → Pong, Handshake → HandshakeOk.
    /// Useful for tests that want to exercise client code without writing a
    /// handler by hand.
    pub fn echo() -> Self {
        Self::new(|req| match req {
            Request::Ping => Ok(Response::Pong),
            Request::Handshake { .. } => Ok(Response::HandshakeOk {
                server_version: PROTOCOL_VERSION,
            }),
            // echo is a minimal mock for liveness + handshake tests.
            // Tests that need tasks_* behavior should build their
            // own InMemoryTransport with a custom closure rather
            // than rely on this convenience constructor.
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
            | Request::WatchAdd { .. }
            | Request::WatchRemove { .. }
            | Request::WatchList
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
            | Request::BrainsList { .. } => Err(RpcError::Unknown {
                message: "InMemoryTransport::echo does not handle tasks_* / records_* / \
                          sagas_* / memory_* / <kind>_* / watch_* / links_* / brains_* / \
                          tags_recluster requests — use \
                          InMemoryTransport::new with a custom handler"
                    .into(),
            }),
        })
    }
}

impl Transport for InMemoryTransport {
    fn call(&mut self, req: Request) -> Result<Response, RpcError> {
        (self.handler)(req)
    }
}

/// Test-only `DaemonSpawner`. Records the number of times `spawn` is
/// called and dispatches to a user-provided behavior closure that can
/// (a) simulate "daemon binds the socket" by binding a real
/// `UnixListener`, or (b) return [`RpcError`] to exercise the
/// spawn-failure path.
///
/// # Why a closure
///
/// Tests want to do different things on spawn — bind a listener, sleep
/// before binding, fail with a specific error, count invocations across
/// multiple spawns. A single closure handler covers every case with no
/// API surface bloat.
/// Erased closure type for [`FakeSpawner`]'s behavior slot.
///
/// Extracted as a `type` alias because clippy's `type_complexity`
/// lint flags inline trait-object types of this shape under
/// `--all-features` (which activates the `test-utils` feature and
/// compiles this module).
#[cfg(unix)]
pub type FakeSpawnBehavior = Box<dyn Fn(&Path) -> Result<(), RpcError> + Send + Sync>;

#[cfg(unix)]
pub struct FakeSpawner {
    binary: PathBuf,
    spawn_calls: AtomicU32,
    behavior: FakeSpawnBehavior,
}

#[cfg(unix)]
impl FakeSpawner {
    /// Construct a `FakeSpawner` that reports `binary` as its resolved
    /// binary path and runs `behavior` on every `spawn` call.
    pub fn new<F>(binary: impl Into<PathBuf>, behavior: F) -> Self
    where
        F: Fn(&Path) -> Result<(), RpcError> + Send + Sync + 'static,
    {
        Self {
            binary: binary.into(),
            spawn_calls: AtomicU32::new(0),
            behavior: Box::new(behavior),
        }
    }

    /// Number of times `spawn` has been called since construction.
    pub fn spawn_calls(&self) -> u32 {
        self.spawn_calls.load(Ordering::SeqCst)
    }
}

#[cfg(unix)]
impl DaemonSpawner for FakeSpawner {
    fn spawn(&self, socket_path: &Path) -> Result<(), RpcError> {
        self.spawn_calls.fetch_add(1, Ordering::SeqCst);
        (self.behavior)(socket_path)
    }

    fn binary_path(&self) -> Result<PathBuf, RpcError> {
        Ok(self.binary.clone())
    }
}
