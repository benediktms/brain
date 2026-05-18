//! End-to-end integration test: `DaemonClient` driving
//! [`UnixSocketTransport`] against a real (temp-directory) Unix socket.
//!
//! This is the first test in the suite that exercises *actual* I/O —
//! framing, serde, socket lifecycle, the works. It validates the
//! adapter layer of the hexagon. The client implementation under test
//! is byte-identical to the one exercised in `client_in_memory.rs`,
//! which is the whole point of generic `DaemonClient<T: Transport>`:
//! one client, two backends, same code path.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::thread;

use brain_rpc::{DaemonClient, PROTOCOL_VERSION, Request, Response, RpcError, UnixSocketTransport};
use tempfile::TempDir;

/// Run `server_fn` on a freshly-bound Unix socket in a temp dir, give
/// the caller the socket path, and join the server thread when the
/// returned guard drops. Keeps each test self-contained.
fn spawn_server<F>(server_fn: F) -> (TempDir, std::path::PathBuf, ServerGuard)
where
    F: FnOnce(UnixListener) + Send + 'static,
{
    let tmp = TempDir::new().expect("tempdir");
    let sock_path = tmp.path().join("brain.sock");
    let listener = UnixListener::bind(&sock_path).expect("bind");
    let handle = thread::spawn(move || server_fn(listener));
    (tmp, sock_path, ServerGuard(Some(handle)))
}

struct ServerGuard(Option<thread::JoinHandle<()>>);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            // Best-effort: don't panic on thread cleanup. The test
            // itself asserts on the response, so any server-side
            // panic surfaces via test stdout.
            let _ = h.join();
        }
    }
}

/// Echo server that handles exactly one client, replies to
/// Handshake/Ping correctly, then closes.
fn echo_once(listener: UnixListener) {
    let (stream, _addr) = listener.accept().expect("accept");
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut writer = stream;

    for _ in 0..2 {
        let mut frame = Vec::new();
        let n = reader.read_until(b'\n', &mut frame).expect("read");
        if n == 0 {
            return; // client hung up
        }
        if frame.last() == Some(&b'\n') {
            frame.pop();
        }
        let req: Request = serde_json::from_slice(&frame).expect("parse request");
        let resp = match req {
            Request::Handshake { .. } => Response::HandshakeOk {
                server_version: PROTOCOL_VERSION,
            },
            Request::Ping => Response::Pong,
            // This test echo server only exercises the wire framing for
            // Handshake + Ping. If we ever wire a tasks_* test through
            // here, the relevant arm should be expanded.
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
            | Request::TasksApplyEvent { .. }
            | Request::TasksDepsBatch { .. }
            | Request::TasksLabelsBatch { .. }
            | Request::TasksLabelsSummary
            | Request::MemoryWalkThread { .. }
            | Request::TagsRecluster { .. }
            | Request::BrainsList { .. } => {
                unreachable!(
                    "echo_once test server is not configured to respond to tasks_* / records_* / \
                     sagas_* / memory_* / tags_* / jobs_* / status / provider_* / watch_* / \
                     links_* / brains_* requests"
                )
            }
        };
        let mut payload = serde_json::to_vec(&resp).expect("serialize response");
        payload.push(b'\n');
        writer.write_all(&payload).expect("write");
        writer.flush().expect("flush");
    }
}

#[test]
fn connect_and_ping_pong_over_real_socket() {
    let (_tmp, sock_path, _guard) = spawn_server(echo_once);

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect transport");
    let mut client = DaemonClient::connect(transport).expect("connect client");

    assert_eq!(client.call(Request::Ping).expect("ping"), Response::Pong);
}

#[test]
fn connect_returns_transport_error_when_socket_missing() {
    let tmp = TempDir::new().expect("tempdir");
    let sock_path = tmp.path().join("nonexistent.sock");

    match UnixSocketTransport::connect(&sock_path) {
        Ok(_) => panic!("expected error connecting to nonexistent socket"),
        Err(RpcError::Transport { message }) => {
            assert!(
                message.contains(&sock_path.display().to_string()),
                "error message should mention socket path; got: {message}"
            );
        }
        Err(other) => panic!("expected Transport error, got {other:?}"),
    }
}

#[test]
fn connect_rejects_mismatched_server_version_over_socket() {
    // Server that responds with the wrong protocol version.
    let (_tmp, sock_path, _guard) = spawn_server(|listener| {
        let (stream, _) = listener.accept().expect("accept");
        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
        let mut writer = stream;

        let mut frame = Vec::new();
        reader.read_until(b'\n', &mut frame).expect("read");
        // Don't even bother parsing the handshake — just lie back.
        let resp = Response::HandshakeOk { server_version: 99 };
        let mut payload = serde_json::to_vec(&resp).expect("serialize");
        payload.push(b'\n');
        writer.write_all(&payload).expect("write");
        writer.flush().expect("flush");
    });

    let transport = UnixSocketTransport::connect(&sock_path).expect("connect transport");
    match DaemonClient::connect(transport) {
        Ok(_) => panic!("expected VersionMismatch, got Ok"),
        Err(RpcError::VersionMismatch { client, server }) => {
            assert_eq!(client, PROTOCOL_VERSION);
            assert_eq!(server, 99);
        }
        Err(other) => panic!("expected VersionMismatch, got Err({other:?})"),
    }
}
