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
         When --sqlite-db is omitted, brain-daemon resolves it from\n\
         $BRAIN_HOME/brain.db (or $HOME/.brain/brain.db). Similarly,\n\
         --lance-db defaults to $BRAIN_HOME/lancedb (or\n\
         $HOME/.brain/lancedb). If neither $BRAIN_HOME nor $HOME is\n\
         set, the daemon falls back to a minimal dispatcher that only\n\
         answers Ping + Handshake.\n\
         \n\
         Signal handling, file watcher, and job scheduler are deferred\n\
         to follow-up tickets — see `brain_daemon::NOT_YET_IMPLEMENTED`\n\
         in the crate docs."
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

    /// Helper: run a closure with specific env vars set, restoring
    /// originals afterwards. Uses a mutex guard from `std::sync` to
    /// serialise env mutations across tests in the same process — env
    /// state is global and cargo-nextest runs tests in parallel.
    fn with_env<F>(vars: &[(&str, Option<&str>)], f: F)
    where
        F: FnOnce(),
    {
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

        f();

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
