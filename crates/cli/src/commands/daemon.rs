use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

pub struct Daemon {
    pid_path: PathBuf,
    log_dir: PathBuf,
    lock_path: PathBuf,
}

/// CLI-level overrides for the log sink. Fields shadow config-file values when
/// `Some`. Applied before `init_tracing_for_daemon` in the forked child.
#[derive(Debug, Default)]
pub(crate) struct LogOverrides {
    pub(crate) log_filter: Option<String>,
    pub(crate) log_max_files: Option<u32>,
    pub(crate) log_max_size_mb: Option<u64>,
    /// True when `--log-max-size-mb` was explicitly supplied on the CLI.
    /// Used to emit a warning that size-based rotation is not yet implemented.
    pub(crate) user_set_max_size_mb: bool,
    pub(crate) log_format: Option<String>,
}

impl Daemon {
    pub fn new() -> Result<Self> {
        let home = dirs::home_dir()
            .context("could not determine home directory")?
            .join(".brain");
        brain_lib::fs_permissions::ensure_private_dir(&home).map_err(|e| anyhow::anyhow!("{e}"))?;
        let pid_path = home.join("brain.pid");
        let log_dir = home.join("logs");
        let lock_path = home.join("brain.lock");
        Ok(Self {
            pid_path,
            log_dir,
            lock_path,
        })
    }

    /// Fork, setsid, redirect fds, write PID. Parent exits; child returns.
    pub fn start(&self, overrides: LogOverrides) -> Result<()> {
        // One-time cleanup of legacy log paths superseded by ~/.brain/logs/.
        let brain_home = self
            .log_dir
            .parent()
            .expect("log_dir always has a parent (~/.brain)");
        let _ = fs::remove_file(brain_home.join("brain-launchd.log"));
        let _ = fs::remove_file(brain_home.join("brain-launchd.err"));
        let _ = fs::remove_file(brain_home.join("brain.log"));

        // Acquire an exclusive, non-blocking lock to prevent concurrent starts.
        // The child inherits the open FD (and thus the lock) after fork.
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.lock_path)?;
        let lock_fd = lock_file.as_raw_fd();
        let lock_ret = unsafe { libc::flock(lock_fd, libc::LOCK_EX | libc::LOCK_NB) };
        if lock_ret != 0 {
            bail!(
                "Another daemon start is in progress (lock held on {})",
                self.lock_path.display()
            );
        }
        // lock_file stays alive through start() — protects the startup
        // sequence from concurrent `brain daemon start` invocations.  The
        // lock is released when start() returns in the child, which is fine:
        // by then the PID file is written and the daemon is running.

        if let Some((pid, stored_mtime)) = self.read_pid_file()? {
            if self.is_alive(pid) {
                let cur_mtime = current_exe_mtime().ok();
                let is_stale = match (stored_mtime, cur_mtime) {
                    (Some(stored), Some(cur)) => stored != cur,
                    _ => false,
                };
                if is_stale {
                    println!("Replacing stale daemon (PID: {pid}, binary changed)");
                    if !kill_and_wait(pid) {
                        bail!(
                            "Failed to stop stale daemon (PID: {pid}). Kill manually: kill -9 {pid}"
                        );
                    }
                    let _ = fs::remove_file(&self.pid_path);
                } else {
                    bail!("Daemon already running (PID: {pid})");
                }
            } else {
                eprintln!("Removing stale PID file (PID {pid} is not running)");
                fs::remove_file(&self.pid_path)?;
            }
        }

        // Ensure log directory exists before fork so the child can write to it.
        fs::create_dir_all(&self.log_dir)
            .with_context(|| format!("failed to create log dir {}", self.log_dir.display()))?;

        // Surface the deprecation warning to the user immediately, before
        // forking.  The in-child tracing::warn! still fires for the daemon log.
        if overrides.user_set_max_size_mb {
            eprintln!(
                "warning: --log-max-size-mb is reserved for future use; current rotation is daily"
            );
        }

        let pid = unsafe { libc::fork() };
        match pid {
            -1 => bail!("fork failed: {}", std::io::Error::last_os_error()),
            0 => {
                // Child: new session. Redirect stdin to /dev/null; stdout/stderr
                // are left untouched — tracing-appender writes directly to the
                // log file, bypassing fd 1/2 entirely.
                if unsafe { libc::setsid() } == -1 {
                    bail!("setsid failed: {}", std::io::Error::last_os_error());
                }
                // Redirect stdin to /dev/null only.
                let devnull = fs::File::open("/dev/null")?;
                unsafe { libc::dup2(devnull.as_raw_fd() as libc::c_int, 0) };
                drop(devnull);

                // Initialize the log sink now that we are the daemon child.
                // This MUST happen after fork — non_blocking spawns a thread.
                // CLI flags override config-file values; RUST_LOG still wins.
                let user_set_max_size_mb = overrides.user_set_max_size_mb;
                let mut config = brain_lib::config::load_global_config().unwrap_or_default();
                if let Some(v) = overrides.log_filter {
                    config.log_filter = Some(v);
                }
                if let Some(v) = overrides.log_max_files {
                    config.log_max_files = Some(v);
                }
                if let Some(v) = overrides.log_max_size_mb {
                    config.log_max_size_mb = Some(v);
                }
                if let Some(v) = overrides.log_format {
                    config.log_format = Some(v);
                }
                crate::dispatch::init_tracing_for_daemon(
                    &config,
                    self.log_dir.clone(),
                    user_set_max_size_mb,
                );

                // Write PID from child (getpid is accurate post-setsid)
                let child_pid = unsafe { libc::getpid() };
                let mtime_line = current_exe_mtime()
                    .map(|m| format!("\n{m}"))
                    .unwrap_or_default();
                fs::write(&self.pid_path, format!("{child_pid}{mtime_line}"))?;
                Ok(())
            }
            _parent => {
                // Parent: print info and exit
                println!("Daemon started (PID: {pid})");
                println!("Logs: {}", self.log_dir.display());
                std::process::exit(0);
            }
        }
    }

    pub fn stop(&self) -> Result<()> {
        let pid = match self.read_pid_file()? {
            Some((pid, _)) => pid,
            None => {
                println!("Daemon is not running");
                return Ok(());
            }
        };
        if !self.is_alive(pid) {
            let _ = fs::remove_file(&self.pid_path);
            println!("Daemon is not running (stale PID file removed)");
            return Ok(());
        }

        println!("Stopping daemon (PID: {pid})...");
        if kill_and_wait(pid) {
            let _ = fs::remove_file(&self.pid_path);
            // NOTE: We do NOT delete the socket file here.  The daemon's own
            // shutdown sequence removes it (phase 1 in watch.rs).  For
            // SIGKILL, the stale-socket detection in IpcServer::bind() handles
            // cleanup on next start.
            println!("Daemon stopped");
        } else {
            eprintln!("Daemon did not exit. Kill manually: kill -9 {pid}");
        }
        Ok(())
    }

    pub fn status(&self) -> Result<()> {
        match self.read_pid_file()? {
            Some((pid, stored_mtime)) if self.is_alive(pid) => {
                let cur_mtime = current_exe_mtime().ok();
                let is_stale = match (stored_mtime, cur_mtime) {
                    (Some(stored), Some(cur)) => stored != cur,
                    _ => false,
                };
                if is_stale {
                    println!("Daemon is running (PID: {pid}, binary STALE)");
                } else {
                    println!("Daemon is running (PID: {pid})");
                }
            }
            Some((pid, _)) => {
                let _ = fs::remove_file(&self.pid_path);
                println!("Daemon is not running (stale PID file for {pid})");
            }
            None => println!("Daemon is not running"),
        }
        Ok(())
    }

    fn read_pid_file(&self) -> Result<Option<(u32, Option<u64>)>> {
        match fs::read_to_string(&self.pid_path) {
            Ok(c) => {
                let mut lines = c.trim().lines();
                let pid: u32 = lines
                    .next()
                    .context("empty PID file")?
                    .trim()
                    .parse()
                    .context("invalid PID in PID file")?;
                let exe_mtime: Option<u64> = lines.next().and_then(|l| l.trim().parse().ok());
                Ok(Some((pid, exe_mtime)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Send SIGHUP to the running daemon so it reloads the registry.
    /// Silently succeeds if no daemon is running.
    pub fn signal_reload(&self) -> Result<()> {
        let pid = match self.read_pid_file()? {
            Some((pid, _)) => pid,
            None => return Ok(()),
        };
        if !self.is_alive(pid) {
            return Ok(());
        }
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGHUP) };
        println!("Signaled daemon to reload registry");
        Ok(())
    }

    fn is_alive(&self, pid: u32) -> bool {
        let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if ret == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}

/// Send SIGTERM, wait up to 5s, escalate to SIGKILL, wait 2s more.
///
/// Returns `true` if the process is no longer alive after the sequence.
pub(crate) fn kill_and_wait(pid: u32) -> bool {
    fn is_alive(pid: u32) -> bool {
        let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if ret != 0 {
            // ESRCH = no such process → dead
            return std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH);
        }
        // kill(0) succeeded — process exists, but might be a zombie child.
        // Try non-blocking waitpid to reap it.  If it's our child and has
        // exited, this reaps it and we know it's dead.  If it's not our
        // child, waitpid returns 0 or -1 and we treat it as alive.
        let mut status: libc::c_int = 0;
        let w = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) };
        if w == pid as libc::pid_t {
            return false; // reaped zombie — process is dead
        }
        true
    }

    // Phase 1: SIGTERM
    unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    for _ in 0..10 {
        std::thread::sleep(std::time::Duration::from_millis(500));
        if !is_alive(pid) {
            return true;
        }
    }

    // Phase 2: Escalate to SIGKILL
    eprintln!("Daemon did not exit after 5s SIGTERM, sending SIGKILL (PID: {pid})");
    unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    for _ in 0..4 {
        std::thread::sleep(std::time::Duration::from_millis(500));
        if !is_alive(pid) {
            return true;
        }
    }

    false
}

fn current_exe_mtime() -> Result<u64> {
    let exe = std::env::current_exe().context("cannot determine executable path")?;
    let meta = fs::metadata(&exe).context("cannot stat executable")?;
    let mtime = meta.modified().context("cannot read executable mtime")?;
    Ok(mtime
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_pid_file_content(content: &str) -> Option<(u32, Option<u64>)> {
        let mut lines = content.trim().lines();
        let pid: u32 = lines.next()?.trim().parse().ok()?;
        let exe_mtime: Option<u64> = lines.next().and_then(|l| l.trim().parse().ok());
        Some((pid, exe_mtime))
    }

    #[test]
    fn test_parse_old_format_pid_file() {
        let content = "12345\n";
        let result = parse_pid_file_content(content);
        assert_eq!(result, Some((12345, None)));
    }

    #[test]
    fn test_parse_extended_pid_file_format() {
        let content = "12345\n1700000000\n";
        let result = parse_pid_file_content(content);
        assert_eq!(result, Some((12345, Some(1700000000))));
    }

    #[test]
    fn test_parse_extended_pid_file_no_trailing_newline() {
        let content = "42\n1234567890";
        let result = parse_pid_file_content(content);
        assert_eq!(result, Some((42, Some(1234567890))));
    }

    // ── flock tests ─────────────────────────────────────────────────

    // TODO: this test is flaky on macOS — flock behavior after drop is
    // non-deterministic when fd2 was opened before the first lock released.
    // Need to find a way to run it more deterministically.
    #[test]
    #[ignore]
    fn test_flock_prevents_concurrent_lock() {
        let tmp = tempfile::TempDir::new().unwrap();
        let lock_path = tmp.path().join("brain.lock");

        // Acquire the lock.
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .unwrap();
        let fd = lock_file.as_raw_fd();
        let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(ret, 0, "first flock should succeed");

        // Second attempt should fail (EWOULDBLOCK).
        let lock_file2 = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .unwrap();
        let fd2 = lock_file2.as_raw_fd();
        let ret2 = unsafe { libc::flock(fd2, libc::LOCK_EX | libc::LOCK_NB) };
        assert_ne!(ret2, 0, "second flock should fail while first is held");

        // Drop first lock → second should now succeed.
        drop(lock_file);
        let ret3 = unsafe { libc::flock(fd2, libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(ret3, 0, "flock should succeed after first lock released");
    }

    // ── kill_and_wait tests ──────────────────────────────────────────

    #[test]
    #[allow(clippy::zombie_processes)] // kill_and_wait reaps the process via libc
    fn test_kill_and_wait_kills_normal_process() {
        use std::process::Command;
        // Spawn a process that sleeps forever but responds to SIGTERM.
        let child = Command::new("sleep")
            .arg("120")
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();

        assert!(
            kill_and_wait(pid),
            "kill_and_wait should succeed for a normal process"
        );

        // Verify process is gone.
        let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
        assert_ne!(ret, 0, "process should be dead after kill_and_wait");
    }

    #[test]
    fn test_kill_and_wait_escalates_to_sigkill() {
        use std::process::Command;
        // Spawn a process that traps SIGTERM (ignores it).
        let mut child = Command::new("bash")
            .args(["-c", "trap '' TERM; sleep 120"])
            .spawn()
            .expect("failed to spawn bash");
        let pid = child.id();

        // Give the trap a moment to be installed.
        std::thread::sleep(std::time::Duration::from_millis(100));

        assert!(
            kill_and_wait(pid),
            "kill_and_wait should escalate to SIGKILL and succeed"
        );

        // Verify process is gone.
        let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
        assert_ne!(ret, 0, "process should be dead after SIGKILL escalation");
        let _ = child.wait();
    }

    #[test]
    fn test_kill_and_wait_returns_true_for_already_dead_process() {
        use std::process::Command;
        let mut child = Command::new("true").spawn().expect("failed to spawn true");
        let pid = child.id();
        child.wait().unwrap(); // wait for it to exit

        // Process is already dead — kill_and_wait should handle gracefully.
        assert!(
            kill_and_wait(pid),
            "kill_and_wait should return true for already-dead process"
        );
    }

    // ── stop() socket behavior tests ───────────────────────────────

    #[test]
    fn test_stop_does_not_delete_socket_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let pid_path = tmp.path().join("brain.pid");
        let log_dir = tmp.path().join("logs");
        let sock_path = tmp.path().join("brain.sock");

        // Create a dummy socket file.
        std::fs::write(&sock_path, "dummy").unwrap();

        // Spawn a process we can stop.
        let mut child = std::process::Command::new("sleep")
            .arg("120")
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id();
        std::fs::write(&pid_path, format!("{pid}\n1700000000")).unwrap();

        let daemon = Daemon {
            pid_path,
            log_dir,
            lock_path: tmp.path().join("brain.lock"),
        };
        daemon.stop().unwrap();

        assert!(
            sock_path.exists(),
            "stop() must NOT delete the socket file — the daemon's own shutdown handles it"
        );
        let _ = child.wait();
    }

    #[test]
    fn test_current_exe_mtime_returns_reasonable_value() {
        let mtime = current_exe_mtime().expect("should get exe mtime");
        // The mtime should be after 2020-01-01 (Unix timestamp 1577836800)
        assert!(
            mtime > 1_577_836_800,
            "mtime {mtime} looks unreasonably old"
        );
        // And before some far future date (year 2100 = Unix timestamp ~4102444800)
        assert!(
            mtime < 4_102_444_800,
            "mtime {mtime} looks unreasonably far in the future"
        );
    }
}
