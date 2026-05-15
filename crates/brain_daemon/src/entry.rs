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

    use crate::{DaemonConfig, DefaultDispatcher, UnixSocketServer};

    let mut args = std::env::args().skip(1);
    let mut socket_path: Option<PathBuf> = None;
    let mut pid_file: Option<PathBuf> = None;

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

    let server = match UnixSocketServer::bind(&config, DefaultDispatcher) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("brain-daemon: failed to bind socket: {e}");
            return ExitCode::from(1);
        }
    };

    println!("brain-daemon listening on {}", socket_path.display());

    if let Err(e) = server.run() {
        eprintln!("brain-daemon: server error: {e}");
        return ExitCode::from(1);
    }

    ExitCode::SUCCESS
}

#[cfg(unix)]
fn print_usage() {
    eprintln!(
        "Usage: brain-daemon --socket-path <PATH> [--pid-file <PATH>]\n\
         \n\
         The MVP daemon binds a Unix socket and answers Ping + Handshake\n\
         requests. Signal handling, real request handlers, file watcher,\n\
         and job scheduler are deferred to follow-up tickets — see\n\
         `brain_daemon::NOT_YET_IMPLEMENTED` for the full list."
    );
}

#[cfg(not(unix))]
pub fn run_cli() -> std::process::ExitCode {
    eprintln!("brain-daemon: this build target does not support Unix sockets");
    std::process::ExitCode::from(2)
}
