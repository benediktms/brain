//! Library-level entry point for the `brain-daemon` binary.
//!
//! Lives in `brain_daemon` (not in `main.rs`) so the `brain` cli crate
//! can host a `[[bin]] name = "brain-daemon"` target that forwards to
//! the same function. That's how `cargo-dist` ends up shipping both
//! `brain` and `brain-daemon` from a single Homebrew formula — see
//! `cli/src/bin/brain-daemon.rs` for the forwarder, and the
//! `[package.metadata.dist] dist = false` line on this crate's
//! `Cargo.toml` for the opt-out that prevents a duplicate release.
//!
//! All arg parsing is hand-written (`std::env::args`) — keeps
//! brain_daemon's dep tree at exactly `{brain-rpc, serde, serde_json,
//! anyhow, thiserror}`. clap can join later if the surface grows.

#[cfg(unix)]
pub fn run_cli() -> std::process::ExitCode {
    use std::path::PathBuf;
    use std::process::ExitCode;

    use crate::{BrainStoresDispatcher, DaemonConfig, DefaultDispatcher};

    let mut args = std::env::args().skip(1);
    let mut socket_path: Option<PathBuf> = None;
    let mut pid_file: Option<PathBuf> = None;
    let mut sqlite_db: Option<PathBuf> = None;
    let mut lance_db: Option<PathBuf> = None;
    #[cfg(feature = "embed")]
    let mut no_watcher = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket-path" => {
                socket_path = args.next().map(PathBuf::from);
                if socket_path.is_none() {
                    eprintln!("brain-daemon: --socket-path requires a value");
                    return ExitCode::from(2);
                }
            }
            "--pid-file" => {
                pid_file = args.next().map(PathBuf::from);
                if pid_file.is_none() {
                    eprintln!("brain-daemon: --pid-file requires a value");
                    return ExitCode::from(2);
                }
            }
            "--sqlite-db" => {
                sqlite_db = args.next().map(PathBuf::from);
                if sqlite_db.is_none() {
                    eprintln!("brain-daemon: --sqlite-db requires a value");
                    return ExitCode::from(2);
                }
            }
            "--lance-db" => {
                lance_db = args.next().map(PathBuf::from);
                if lance_db.is_none() {
                    eprintln!("brain-daemon: --lance-db requires a value");
                    return ExitCode::from(2);
                }
            }
            #[cfg(feature = "embed")]
            "--no-watcher" => {
                no_watcher = true;
            }
            "--help" | "-h" => {
                print_usage();
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("brain-daemon: unknown argument: {other}");
                print_usage();
                return ExitCode::from(2);
            }
        }
    }

    let Some(socket_path) = socket_path else {
        eprintln!("brain-daemon: --socket-path is required");
        print_usage();
        return ExitCode::from(2);
    };

    // If --sqlite-db was not supplied, derive a default from $BRAIN_HOME
    // (or $HOME/.brain/ if BRAIN_HOME is unset) — but only when the
    // resolved file actually exists. This lets `connect_or_spawn` auto-
    // start the daemon against a real install without forwarding DB
    // paths through the spawner, while leaving test sandboxes and
    // fresh-install setups in DefaultDispatcher mode (which never
    // touches the DB).
    let brain_home = resolve_brain_home();
    let sqlite_db = sqlite_db.or_else(|| {
        brain_home
            .as_ref()
            .map(|h| h.join("brain.db"))
            .filter(|p| p.exists())
    });
    let lance_db = lance_db.or_else(|| brain_home.as_ref().map(|h| h.join("lancedb")));

    let mut config = DaemonConfig::new(&socket_path);
    if let Some(p) = pid_file {
        config = config.with_pid_file(p);
    }

    // Pre-create the watcher control channel so we can wire its sender
    // into the dispatcher before constructing the server. The supervisor
    // thread itself is spawned inside `run_with_server` once the server
    // is bound, because the supervisor needs the server's ShutdownHandle
    // — its Phase 1 shutdown flips the accept loop's atomic, which is
    // the only path that bridges SIGTERM (caught only by the supervisor's
    // tokio runtime) into clean termination of the synchronous accept loop.
    #[cfg(feature = "embed")]
    let (watcher_handle, supervisor_rx) = if sqlite_db.is_some() && !no_watcher {
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::watcher::ControlMessage>(64);
        let handle = std::sync::Arc::new(crate::watcher::WatcherHandle::new(tx));
        (Some(handle), Some(rx))
    } else {
        (None, None)
    };

    // Shared flag the supervisor thread sets if its tokio runtime fails to
    // build or `bootstrap_and_run` returns Err. After the accept loop exits
    // and the watcher thread joins, we check this and force a non-zero exit
    // so the process surfaces "daemon running degraded" instead of looking
    // healthy with a silently-dead watcher.
    #[cfg(feature = "embed")]
    let supervisor_failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Two monomorphizations of `run_with_server` chosen at runtime by which
    // args were present. Keeps UnixSocketServer's generic Dispatcher
    // bound (`D: Dispatcher + Send + Sync + 'static`) clean and avoids
    // a `Box<dyn Dispatcher>` indirection in the hot path.
    let (exit_code, supervisor_thread) = if let Some(sqlite_path) = sqlite_db {
        let stores =
            match brain_lib::stores::BrainStores::from_path(&sqlite_path, lance_db.as_deref()) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("brain-daemon: failed to open stores: {e}");
                    return ExitCode::from(1);
                }
            };
        #[cfg(feature = "embed")]
        let dispatcher = BrainStoresDispatcher::new(stores, watcher_handle.clone());
        #[cfg(not(feature = "embed"))]
        let dispatcher = BrainStoresDispatcher::new(stores);
        run_with_server(
            &config,
            dispatcher,
            &socket_path,
            "BrainStoresDispatcher",
            #[cfg(feature = "embed")]
            supervisor_rx,
            #[cfg(feature = "embed")]
            std::sync::Arc::clone(&supervisor_failed),
        )
    } else {
        run_with_server(
            &config,
            DefaultDispatcher,
            &socket_path,
            "DefaultDispatcher (Ping + Handshake only — pass --sqlite-db to enable real handlers)",
            #[cfg(feature = "embed")]
            None,
            #[cfg(feature = "embed")]
            std::sync::Arc::clone(&supervisor_failed),
        )
    };

    // Coordinated shutdown: drop the only remaining `WatcherHandle` so
    // the supervisor's `control_rx.recv()` returns `None`, which its
    // event loop treats as a shutdown trigger. If the supervisor
    // already exited (its own SIGTERM/Ctrl+C handler fired first) this
    // is a no-op. Either way we then join the thread; the supervisor's
    // Phase 1-5 shutdown — WAL checkpoint, LanceDB optimize — can take
    // tens of seconds on a large brain, so the join is unbounded.
    #[cfg(feature = "embed")]
    drop(watcher_handle);

    if let Some(thread) = supervisor_thread
        && let Err(e) = thread.join()
    {
        eprintln!("brain-daemon: watcher thread join failed: {e:?}");
    }

    #[cfg(feature = "embed")]
    if supervisor_failed.load(std::sync::atomic::Ordering::SeqCst) {
        // Override whatever the accept loop reported: the watcher dying is
        // a daemon-wide failure regardless of how the RPC server exited.
        return ExitCode::from(1);
    }

    exit_code
}

/// Bind the socket, optionally spawn the watcher supervisor (handing it the
/// server's shutdown handle), then run the accept loop on the calling thread.
///
/// Returns the resulting exit code together with the supervisor's `JoinHandle`
/// (when spawned) so the caller can drop the watcher handle and join the
/// thread after the accept loop exits.
///
/// The supervisor owns its own tokio current-thread runtime in that thread,
/// mirroring the per-request runtime pattern memory handlers use. Keeping it
/// off the RPC server's accept-loop thread means a hung watcher can never
/// wedge accept(). The supervisor receives the server's [`ShutdownHandle`] so
/// its Phase 1 shutdown can flip the accept-loop's atomic — the only path
/// that bridges SIGTERM (caught by the supervisor's tokio runtime) into
/// termination of the synchronous accept loop.
#[cfg(unix)]
fn run_with_server<D>(
    config: &crate::DaemonConfig,
    dispatcher: D,
    socket_path: &std::path::Path,
    dispatcher_label: &str,
    #[cfg(feature = "embed")] supervisor_rx: Option<
        tokio::sync::mpsc::Receiver<crate::watcher::ControlMessage>,
    >,
    #[cfg(feature = "embed")] supervisor_failed: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> (std::process::ExitCode, Option<std::thread::JoinHandle<()>>)
where
    D: crate::Dispatcher + Send + Sync + 'static,
{
    use std::process::ExitCode;

    let server = match crate::UnixSocketServer::bind(config, dispatcher) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("brain-daemon: failed to bind socket: {e}");
            return (ExitCode::from(1), None);
        }
    };

    #[cfg(feature = "embed")]
    let supervisor_thread = supervisor_rx.and_then(|rx| {
        let rpc_shutdown = server.shutdown_handle();
        let failed_flag = std::sync::Arc::clone(&supervisor_failed);
        match std::thread::Builder::new()
            .name("brain-watcher".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!("brain-daemon: failed to build watcher runtime: {e}");
                        // Signal failure and wake the accept loop so the
                        // daemon doesn't sit healthy with a dead watcher.
                        failed_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        rpc_shutdown.request();
                        return;
                    }
                };
                match rt.block_on(crate::watcher::Supervisor::bootstrap_and_run(
                    rx,
                    rpc_shutdown.clone(),
                )) {
                    Ok(outcome) => {
                        eprintln!(
                            "brain-daemon: watcher exited cleanly (dropped_items={})",
                            outcome.dropped_items
                        );
                    }
                    Err(e) => {
                        eprintln!("brain-daemon: watcher exited with error: {e}");
                        failed_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        rpc_shutdown.request();
                    }
                }
            }) {
            Ok(t) => Some(t),
            Err(e) => {
                eprintln!("brain-daemon: failed to spawn watcher thread: {e}");
                // Spawn failure also counts as a degraded daemon when a
                // watcher was expected; flip the flag so the caller knows.
                supervisor_failed.store(true, std::sync::atomic::Ordering::SeqCst);
                None
            }
        }
    });
    #[cfg(not(feature = "embed"))]
    let supervisor_thread: Option<std::thread::JoinHandle<()>> = None;

    println!(
        "brain-daemon listening on {} (dispatcher: {dispatcher_label})",
        socket_path.display()
    );

    if let Err(e) = server.run() {
        eprintln!("brain-daemon: server error: {e}");
        return (ExitCode::from(1), supervisor_thread);
    }

    (ExitCode::SUCCESS, supervisor_thread)
}

#[cfg(unix)]
fn print_usage() {
    #[cfg(feature = "embed")]
    eprintln!(
        "Usage: brain-daemon --socket-path <PATH> \\\n\
         \x20            [--pid-file <PATH>] \\\n\
         \x20            [--sqlite-db <PATH>] [--lance-db <PATH>] \\\n\
         \x20            [--no-watcher]\n\
         \n\
         When --sqlite-db is omitted, brain-daemon resolves it from\n\
         $BRAIN_HOME/brain.db (or $HOME/.brain/brain.db). Similarly,\n\
         --lance-db defaults to $BRAIN_HOME/lancedb (or\n\
         $HOME/.brain/lancedb). If neither $BRAIN_HOME nor $HOME is\n\
         set, the daemon falls back to a minimal dispatcher that only\n\
         answers Ping + Handshake.\n\
         \n\
         When a real DB is resolved, brain-daemon spawns the file\n\
         watcher and job scheduler on a dedicated thread alongside the\n\
         RPC accept loop, and handles SIGTERM / SIGHUP / SIGUSR1 /\n\
         Ctrl+C through the watcher supervisor's own signal handlers.\n\
         \n\
         --no-watcher disables the watcher and job scheduler, leaving\n\
         only the RPC dispatcher running. Used by `brain mcp` so each\n\
         per-session subprocess stays lightweight."
    );
    #[cfg(not(feature = "embed"))]
    eprintln!(
        "Usage: brain-daemon --socket-path <PATH> \\\n\
         \x20            [--pid-file <PATH>] \\\n\
         \x20            [--sqlite-db <PATH>] [--lance-db <PATH>]\n\
         \n\
         When --sqlite-db is omitted, brain-daemon resolves it from\n\
         $BRAIN_HOME/brain.db (or $HOME/.brain/brain.db). Similarly,\n\
         --lance-db defaults to $BRAIN_HOME/lancedb (or\n\
         $HOME/.brain/lancedb). If neither $BRAIN_HOME nor $HOME is\n\
         set, the daemon falls back to a minimal dispatcher that only\n\
         answers Ping + Handshake.\n\
         \n\
         This binary was built without the `embed` feature, so the\n\
         file watcher and job scheduler are unavailable; the RPC\n\
         dispatcher serves typed requests only."
    );
}

/// Resolve `$BRAIN_HOME`, falling back to `$HOME/.brain/`.
///
/// Returns `None` only when neither env var is set — callers treat
/// `None` as "no default available, use the minimal dispatcher".
#[cfg(unix)]
pub(crate) fn resolve_brain_home() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("BRAIN_HOME")
        && !p.is_empty()
    {
        return Some(std::path::PathBuf::from(p));
    }
    std::env::var("HOME")
        .ok()
        .filter(|h| !h.is_empty())
        .map(|h| std::path::PathBuf::from(h).join(".brain"))
}

#[cfg(unix)]
#[cfg(test)]
mod tests {
    use super::resolve_brain_home;
    use std::path::PathBuf;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Helper: run a closure with specific env vars set, restoring
    /// originals afterwards. Uses a mutex guard from `std::sync` to
    /// serialise env mutations across tests in the same process — env
    /// state is global and cargo-nextest runs tests in parallel.
    fn with_env<F>(vars: &[(&str, Option<&str>)], f: F)
    where
        F: FnOnce() + std::panic::UnwindSafe,
    {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // Save and set.
        let saved: Vec<(&str, Option<String>)> = vars
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            // SAFETY: test-only env mutation. Rust 2024 marks set_var /
            // remove_var unsafe because they race with concurrent reads
            // from other threads. These tests run single-threaded (no
            // env reads happen on other threads during the with_env
            // closure), and cargo-nextest isolates each test binary in
            // its own process — so the global env mutation is bounded
            // to this test's scope.
            unsafe {
                match v {
                    Some(val) => std::env::set_var(k, val),
                    None => std::env::remove_var(k),
                }
            }
        }

        let result = std::panic::catch_unwind(f);
        // Restore.
        for (k, v) in &saved {
            // SAFETY: see the env-mutation block above — same conditions
            // apply on restore.
            unsafe {
                match v {
                    Some(val) => std::env::set_var(k, val),
                    None => std::env::remove_var(k),
                }
            }
        }
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }

    #[test]
    fn brain_home_env_takes_priority() {
        with_env(
            &[
                ("BRAIN_HOME", Some("/custom/brain")),
                ("HOME", Some("/home/user")),
            ],
            || {
                assert_eq!(resolve_brain_home(), Some(PathBuf::from("/custom/brain")));
            },
        );
    }

    #[test]
    fn falls_back_to_home_dot_brain() {
        with_env(
            &[("BRAIN_HOME", None), ("HOME", Some("/home/testuser"))],
            || {
                assert_eq!(
                    resolve_brain_home(),
                    Some(PathBuf::from("/home/testuser/.brain"))
                );
            },
        );
    }

    #[test]
    fn returns_none_when_neither_var_set() {
        with_env(&[("BRAIN_HOME", None), ("HOME", None)], || {
            assert_eq!(resolve_brain_home(), None);
        });
    }

    #[test]
    fn ignores_empty_brain_home_falls_back_to_home() {
        with_env(
            &[("BRAIN_HOME", Some("")), ("HOME", Some("/home/fallback"))],
            || {
                assert_eq!(
                    resolve_brain_home(),
                    Some(PathBuf::from("/home/fallback/.brain"))
                );
            },
        );
    }
}

#[cfg(not(unix))]
pub fn run_cli() -> std::process::ExitCode {
    eprintln!("brain-daemon: this build target does not support Unix sockets");
    std::process::ExitCode::from(2)
}
