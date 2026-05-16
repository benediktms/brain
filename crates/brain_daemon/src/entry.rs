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

    let mut config = DaemonConfig::new(&socket_path);
    if let Some(p) = pid_file {
        config = config.with_pid_file(p);
    }

    // Two monomorphizations of `run_server` chosen at runtime by which
    // args were present. Keeps UnixSocketServer's generic Dispatcher
    // bound (`D: Dispatcher + Send + Sync + 'static`) clean and avoids
    // a `Box<dyn Dispatcher>` indirection in the hot path.
    if let Some(sqlite_path) = sqlite_db {
        let stores =
            match brain_lib::stores::BrainStores::from_path(&sqlite_path, lance_db.as_deref()) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("brain-daemon: failed to open stores: {e}");
                    return ExitCode::from(1);
                }
            };
        run_server(
            &config,
            BrainStoresDispatcher::new(stores),
            &socket_path,
            "BrainStoresDispatcher",
        )
    } else {
        run_server(
            &config,
            DefaultDispatcher,
            &socket_path,
            "DefaultDispatcher (Ping + Handshake only — pass --sqlite-db to enable real handlers)",
        )
    }
}

#[cfg(unix)]
fn run_server<D>(
    config: &crate::DaemonConfig,
    dispatcher: D,
    socket_path: &std::path::Path,
    dispatcher_label: &str,
) -> std::process::ExitCode
where
    D: crate::Dispatcher + Send + Sync + 'static,
{
    use std::process::ExitCode;

    let server = match crate::UnixSocketServer::bind(config, dispatcher) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("brain-daemon: failed to bind socket: {e}");
            return ExitCode::from(1);
        }
    };

    println!(
        "brain-daemon listening on {} (dispatcher: {dispatcher_label})",
        socket_path.display()
    );

    if let Err(e) = server.run() {
        eprintln!("brain-daemon: server error: {e}");
        return ExitCode::from(1);
    }

    ExitCode::SUCCESS
}

#[cfg(unix)]
fn print_usage() {
    eprintln!(
        "Usage: brain-daemon --socket-path <PATH> \\\n\
         \x20            [--pid-file <PATH>] \\\n\
         \x20            [--sqlite-db <PATH>] [--lance-db <PATH>]\n\
         \n\
         When --sqlite-db is provided, brain-daemon opens the database\n\
         and serves real Request variants (TasksList, …) via the\n\
         BrainStoresDispatcher. Without it, the daemon falls back to a\n\
         minimal dispatcher that only answers Ping + Handshake — useful\n\
         for smoke tests but rejects every other Request with NotFound.\n\
         \n\
         Signal handling, file watcher, and job scheduler are deferred\n\
         to follow-up tickets — see `brain_daemon::NOT_YET_IMPLEMENTED`\n\
         in the crate docs."
    );
}

#[cfg(not(unix))]
pub fn run_cli() -> std::process::ExitCode {
    eprintln!("brain-daemon: this build target does not support Unix sockets");
    std::process::ExitCode::from(2)
}
