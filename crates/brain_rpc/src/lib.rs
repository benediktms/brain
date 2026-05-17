//! brain_rpc — wire contract between thin clients and the daemon.
//!
//! # Role
//!
//! `brain_rpc` defines the wire protocol used by `brain` (CLI), `brain-mcp`,
//! and any future client to talk to `brain-daemon`. It is the **only** crate
//! that both sides of that boundary depend on.
//!
//! # Architectural ratchet
//!
//! This crate MUST NEVER take a transitive dependency on any of:
//! `rusqlite`, `lancedb`, `candle`, `brain_persistence`, `brain_lib`,
//! `brain_tasks`, `brain_sagas`, `brain_records`, `brain_tags`,
//! `brain_retrieval`, `brain_embedder`.
//!
//! Rationale: if the wire format becomes coupled to internal storage shapes,
//! any future refactor breaks the protocol. The ratchet is enforced two ways:
//!
//! 1. The `[dependencies]` set in Cargo.toml is restricted to
//!    `serde + serde_json + anyhow + thiserror`. DB and domain crates are
//!    physically unreachable.
//! 2. The integration test in `tests/architecture.rs` (added in US-008) walks
//!    the source tree and asserts no forbidden import appears.
//!
//! # Hexagonal shape
//!
//! Following the brain_tasks / brain_sagas crate style, but leaning further
//! into ports & adapters because the Transport abstraction is genuinely useful
//! for testing without sockets:
//!
//! - [`domain`] — pure protocol data: `Request`, `Response`, `RpcError`,
//!   `PROTOCOL_VERSION`. No I/O types.
//! - `transport` (US-003) — the `Transport` port: framework-free trait
//!   abstracting send/receive.
//! - `client` (US-004) — `DaemonClient<T: Transport>` — generic over transport,
//!   so unit tests can plug in `InMemoryTransport` (no real socket).
//! - `spawner` (US-007) — the `DaemonSpawner` port + `StdProcessSpawner` adapter.
//! - `unix` (US-006) — the `UnixSocketTransport` adapter (concrete I/O).
//! - `testing` — mocks (`InMemoryTransport`, `FakeSpawner`) gated behind the
//!   `test-utils` feature for downstream consumers.

pub mod client;
pub mod domain;
pub mod transport;

#[cfg(unix)]
pub mod spawner;
#[cfg(unix)]
pub mod unix;

#[cfg(any(test, feature = "test-utils"))]
pub mod testing;

pub use client::DaemonClient;
pub use domain::{
    AnalysisSummary, ArtifactSummary, ArtifactsListParams, BrainStatusReport, DocumentSummary,
    JobSummary, JobsStatusReport, MemoryConsolidateParams, MemoryReflectParams,
    MemoryRetrieveParams, MemorySummarizeScopeParams, MemoryWriteEpisodeParams,
    MemoryWriteProcedureParams, PROTOCOL_VERSION, PlanSummary, ProviderSummary,
    RecordsCreateParams, RecordsListParams, RecordsVerifyReport, Request, Response, RpcError,
    SagaBrainSummary, SagaCascadeOutcome, SagaCascadeResult, SagaDescriptionUpdate,
    SagaFrontierTask, SagaLabelCount, SagaStatsReport, SagaSummary, SagasCreateParams,
    SagasListParams, SagasUpdateParams, SnapshotSummary, TagAliasSummary, TagAliasesStatusReport,
    TagsAliasesListParams, TaskSummary, TasksCreateParams, TasksListParams, TasksMutateParams,
    TasksTransferParams, TasksUpdateParams, WatchSummary,
};
pub use transport::Transport;

#[cfg(unix)]
pub use spawner::{DaemonSpawner, StdProcessSpawner, connect_or_spawn};
#[cfg(unix)]
pub use unix::{UnixSocketTransport, read_frame, write_frame};
