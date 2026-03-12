use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

pub struct Daemon {
    pid_path: PathBuf,
    log_path: PathBuf,
    sock_path: PathBuf,
}

impl Daemon {
    pub fn new() -> Result<Self> {
        let home = dirs::home_dir()
            .context("could not determine home directory")?
            .join(".brain");
        brain_lib::fs_permissions::ensure_private_dir(&home).map_err(|e| anyhow::anyhow!("{e}"))?;
        let pid_path = home.join("brain.pid");
        let log_path = home.join("brain.log");
        let sock_path = home.join("brain.sock");
        Ok(Self {
            pid_path,
            log_path,
            sock_path,
        })
    }

    /// Fork, setsid, redirect fds, write PID. Parent exits; child returns.
    pub fn start(&self) -> Result<()> {
        if let Some((pid, stored_mtime)) = self.read_pid_file()? {
            if self.is_alive(pid) {
                let cur_mtime = current_exe_mtime().ok();
                let is_stale = match (stored_mtime, cur_mtime) {
                    (Some(stored), Some(cur)) => stored != cur,
                    _ => false,
                };
                if is_stale {
                    println!("Replacing stale daemon (PID: {pid}, binary changed)");
                    unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
                    for _ in 0..10 {
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        if !self.is_alive(pid) {
                            break;
                        }
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

        let log_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        let pid = unsafe { libc::fork() };
        match pid {
            -1 => bail!("fork failed: {}", std::io::Error::last_os_error()),
            0 => {
                // Child: new session, redirect fds
                if unsafe { libc::setsid() } == -1 {
                    bail!("setsid failed: {}", std::io::Error::last_os_error());
                }
                self.redirect_fds(&log_file)?;
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
                println!("Logs: {}", self.log_path.display());
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

        unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        println!("Sent SIGTERM to daemon (PID: {pid})");

        for _ in 0..10 {
            std::thread::sleep(std::time::Duration::from_millis(500));
            if !self.is_alive(pid) {
                let _ = fs::remove_file(&self.pid_path);
                let _ = fs::remove_file(&self.sock_path);
                println!("Daemon stopped");
                return Ok(());
            }
        }
        eprintln!("Daemon did not exit within 5s. Kill manually: kill -9 {pid}");
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

    fn redirect_fds(&self, log_file: &fs::File) -> Result<()> {
        let devnull = fs::File::open("/dev/null")?;
        let log_fd = log_file.as_raw_fd();
        unsafe {
            libc::dup2(devnull.as_raw_fd(), 0);
            libc::dup2(log_fd, 1);
            libc::dup2(log_fd, 2);
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
