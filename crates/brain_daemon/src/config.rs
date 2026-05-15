//! `DaemonConfig` — typed daemon configuration.
//!
//! Pure data. No I/O. Adapter code (`server.rs`, `main.rs`) consumes a
//! `&DaemonConfig` to know which socket to bind, which pidfile to
//! write, etc. Keeping config off the adapter side lets tests inject a
//! per-test temp socket path without going near `std::env` or argument
//! parsing.

use std::path::PathBuf;

/// Daemon configuration values consumed by `UnixSocketServer` and the
/// `brain-daemon` binary entry point.
///
/// Construct via [`Self::new`] (required socket path) and chain
/// optional fields with the builder methods.
///
/// # Fields
///
/// - `socket_path` — Where the daemon binds its Unix listener. Clients
///   (`brain_rpc::UnixSocketTransport`) connect to this path.
/// - `pid_file` — Optional pidfile path. The MVP server does not write
///   the pidfile yet; the field exists so later signal-handling
///   tickets (graceful SIGTERM via `kill $(cat brain.pid)`) can be
///   added without changing the public config shape.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub socket_path: PathBuf,
    pub pid_file: Option<PathBuf>,
}

impl DaemonConfig {
    /// Construct a config with the given socket path and no pidfile.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            pid_file: None,
        }
    }

    /// Builder: set the optional pidfile path.
    pub fn with_pid_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.pid_file = Some(path.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sets_socket_path_and_defaults_pid_file_to_none() {
        let cfg = DaemonConfig::new("/tmp/brain.sock");
        assert_eq!(cfg.socket_path, PathBuf::from("/tmp/brain.sock"));
        assert!(cfg.pid_file.is_none());
    }

    #[test]
    fn with_pid_file_sets_optional_field() {
        let cfg = DaemonConfig::new("/tmp/brain.sock").with_pid_file("/tmp/brain.pid");
        assert_eq!(cfg.pid_file, Some(PathBuf::from("/tmp/brain.pid")));
    }

    #[test]
    fn config_is_clone() {
        // Compile-time assertion that DaemonConfig: Clone. Servers need
        // to hold a copy alongside whatever derived state they build,
        // so cloning the input must work.
        let cfg = DaemonConfig::new("/tmp/brain.sock").with_pid_file("/tmp/brain.pid");
        let cloned = cfg.clone();
        assert_eq!(cfg.socket_path, cloned.socket_path);
        assert_eq!(cfg.pid_file, cloned.pid_file);
    }
}
