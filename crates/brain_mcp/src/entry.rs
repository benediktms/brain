//! Library entry point for the `brain-mcp` binary.
//!
//! Hand-rolled argv parsing (matches the `brain-daemon` style — no
//! clap dep), tracing setup, daemon connect via
//! [`brain_rpc::connect_or_spawn`], and a tokio current-thread runtime
//! for the stdio server loop. A future `crates/brain_mcp/src/main.rs`
//! (Phase F) is a one-liner that calls this function.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use brain_rpc::{BrainsListParams, DaemonClient, StdProcessSpawner, connect_or_spawn};
use tracing_subscriber::EnvFilter;

use crate::context::McpContext;
use crate::server;

const USAGE: &str = "\
brain-mcp — MCP stdio server (forwards tool calls to brain-daemon)

USAGE:
    brain-mcp [--socket-path <path>] [--log-level <level>]

OPTIONS:
    --socket-path <path>    Unix socket path for brain-daemon
                            (default: $BRAIN_SOCKET_PATH, then
                                      $BRAIN_HOME/brain-rpc.sock, then
                                      $HOME/.brain/brain-rpc.sock)
    --log-level <level>     tracing level (default: info; honors RUST_LOG when set)
    -h, --help              Print this help and exit
";

#[derive(Default)]
struct Args {
    socket_path: Option<PathBuf>,
    log_level: Option<String>,
}

enum ParseOutcome {
    Ok(Args),
    Help,
    Error(String),
}

fn parse_argv<I: IntoIterator<Item = String>>(argv: I) -> ParseOutcome {
    let mut args = Args::default();
    let mut it = argv.into_iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--socket-path" => match it.next() {
                Some(v) => args.socket_path = Some(PathBuf::from(v)),
                None => return ParseOutcome::Error("--socket-path requires a value".into()),
            },
            "--log-level" => match it.next() {
                Some(v) => args.log_level = Some(v),
                None => return ParseOutcome::Error("--log-level requires a value".into()),
            },
            "-h" | "--help" => return ParseOutcome::Help,
            other => return ParseOutcome::Error(format!("unknown argument: {other}")),
        }
    }
    ParseOutcome::Ok(args)
}

/// Default Unix socket path mirroring `crates/cli/src/commands/rpc_client.rs`.
fn default_socket_path() -> Result<PathBuf, String> {
    if let Ok(p) = std::env::var("BRAIN_SOCKET_PATH") {
        return Ok(PathBuf::from(p));
    }
    let home = std::env::var("BRAIN_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|h| PathBuf::from(h).join(".brain")))
        .map_err(|_| "neither BRAIN_HOME nor HOME is set".to_string())?;
    Ok(home.join("brain-rpc.sock"))
}

/// Initialise `tracing-subscriber` writing to stderr. `RUST_LOG` wins
/// over an explicit `--log-level` only when the latter is absent — the
/// usual `tracing-subscriber` convention.
fn init_tracing(level: Option<&str>) {
    let filter = if let Some(lvl) = level {
        EnvFilter::try_new(lvl).unwrap_or_else(|_| EnvFilter::new("info"))
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// Library entry: parse argv, connect to the daemon, and run the
/// stdio server loop until stdin closes.
pub fn run_cli() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_argv(argv) {
        ParseOutcome::Ok(a) => a,
        ParseOutcome::Help => {
            eprint!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        ParseOutcome::Error(msg) => {
            eprintln!("brain-mcp: {msg}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    init_tracing(args.log_level.as_deref());

    let socket_path = match args.socket_path {
        Some(p) => p,
        None => match default_socket_path() {
            Ok(p) => p,
            Err(e) => {
                eprintln!("brain-mcp: {e}");
                return ExitCode::FAILURE;
            }
        },
    };

    let spawner = StdProcessSpawner::new();
    let transport = match connect_or_spawn(&socket_path, &spawner) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "brain-mcp: failed to connect to brain-daemon at {}: {e}",
                socket_path.display()
            );
            return ExitCode::FAILURE;
        }
    };

    let mut client = match DaemonClient::connect(transport) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("brain-mcp: daemon handshake failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Best-effort startup brain name: pick the first non-archived
    // brain the daemon reports. Falls back to "brain" so an empty
    // registry still produces a usable session.
    let startup_brain = match client.brains_list(BrainsListParams::default()) {
        Ok((brains, _)) => brains
            .into_iter()
            .find(|b| !b.archived)
            .map(|b| b.name)
            .unwrap_or_else(|| "brain".to_string()),
        Err(_) => "brain".to_string(),
    };

    let ctx = Arc::new(McpContext::new(client, startup_brain));

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("brain-mcp: failed to build tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    match rt.block_on(server::run_server(ctx)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("brain-mcp: server error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(input: &[&str]) -> ParseOutcome {
        parse_argv(input.iter().map(|s| s.to_string()))
    }

    #[test]
    fn parse_default_no_args() {
        match args(&[]) {
            ParseOutcome::Ok(a) => {
                assert!(a.socket_path.is_none());
                assert!(a.log_level.is_none());
            }
            _ => panic!("expected Ok on empty argv"),
        }
    }

    #[test]
    fn parse_socket_path() {
        match args(&["--socket-path", "/tmp/x.sock"]) {
            ParseOutcome::Ok(a) => {
                assert_eq!(a.socket_path, Some(PathBuf::from("/tmp/x.sock")));
            }
            _ => panic!("expected Ok with socket_path"),
        }
    }

    #[test]
    fn parse_log_level() {
        match args(&["--log-level", "debug"]) {
            ParseOutcome::Ok(a) => assert_eq!(a.log_level.as_deref(), Some("debug")),
            _ => panic!("expected Ok with log_level"),
        }
    }

    #[test]
    fn parse_help_short_and_long() {
        assert!(matches!(args(&["-h"]), ParseOutcome::Help));
        assert!(matches!(args(&["--help"]), ParseOutcome::Help));
    }

    #[test]
    fn parse_unknown_flag_errors() {
        assert!(matches!(args(&["--what"]), ParseOutcome::Error(_)));
    }

    #[test]
    fn parse_missing_value_errors() {
        assert!(matches!(args(&["--socket-path"]), ParseOutcome::Error(_)));
        assert!(matches!(args(&["--log-level"]), ParseOutcome::Error(_)));
    }
}
