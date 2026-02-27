use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};

pub struct Daemon {
    pid_path: PathBuf,
    log_path: PathBuf,
}

impl Daemon {
    pub fn new() -> Result<Self> {
        let home = dirs::home_dir()
            .context("could not determine home directory")?
            .join(".brain");
        fs::create_dir_all(&home)?;
        let pid_path = home.join("brain.pid");
        let log_path = home.join("brain.log");
        Ok(Self { pid_path, log_path })
    }

    /// Fork, setsid, redirect fds, write PID. Parent exits; child returns.
    pub fn start(&self) -> Result<()> {
        if let Some(pid) = self.read_pid()? {
            if self.is_alive(pid) {
                bail!("Daemon already running (PID: {pid})");
            }
            eprintln!("Removing stale PID file (PID {pid} is not running)");
            fs::remove_file(&self.pid_path)?;
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
                fs::write(&self.pid_path, child_pid.to_string())?;
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
        let pid = match self.read_pid()? {
            Some(pid) => pid,
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
                println!("Daemon stopped");
                return Ok(());
            }
        }
        eprintln!("Daemon did not exit within 5s. Kill manually: kill -9 {pid}");
        Ok(())
    }

    pub fn status(&self) -> Result<()> {
        match self.read_pid()? {
            Some(pid) if self.is_alive(pid) => println!("Daemon is running (PID: {pid})"),
            Some(pid) => {
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

    fn read_pid(&self) -> Result<Option<u32>> {
        match fs::read_to_string(&self.pid_path) {
            Ok(c) => Ok(Some(c.trim().parse().context("invalid PID file")?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn is_alive(&self, pid: u32) -> bool {
        let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if ret == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }
}
