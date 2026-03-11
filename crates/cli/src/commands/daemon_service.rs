//! Platform-native service installation for the brain daemon.
//!
//! Generates and manages launchd plists (macOS) or systemd user units (Linux)
//! so `brain daemon start` starts automatically on login.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// Resolved parameters for service installation.
struct ServiceParams {
    /// Absolute path to the `brain` binary.
    brain_bin: PathBuf,
}

impl ServiceParams {
    fn resolve() -> Result<Self> {
        // Resolve binary path from current executable
        let brain_bin = std::env::current_exe()
            .context("cannot determine the brain binary path. Ensure brain is installed.")?;
        let brain_bin = std::fs::canonicalize(&brain_bin).unwrap_or(brain_bin);

        Ok(Self { brain_bin })
    }
}

// ─── macOS (launchd) ─────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
mod platform {
    use super::*;

    const SERVICE_LABEL: &str = "com.brain.daemon";

    fn plist_path() -> Result<PathBuf> {
        let home = dirs::home_dir().context("cannot determine home directory")?;
        let dir = home.join("Library").join("LaunchAgents");
        std::fs::create_dir_all(&dir)?;
        Ok(dir.join(format!("{SERVICE_LABEL}.plist")))
    }

    pub fn generate_service(params: &ServiceParams) -> String {
        let bin = params.brain_bin.display();
        let home = dirs::home_dir()
            .map(|h| h.display().to_string())
            .unwrap_or_default();
        let log_path = format!("{home}/.brain/brain-launchd.log");
        let err_path = format!("{home}/.brain/brain-launchd.err");

        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{SERVICE_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{bin}</string>
        <string>daemon</string>
        <string>start</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>{log_path}</string>
    <key>StandardErrorPath</key>
    <string>{err_path}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:{home}/bin</string>
    </dict>
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>ProcessType</key>
    <string>Background</string>
</dict>
</plist>
"#
        )
    }

    pub fn install(params: &ServiceParams) -> Result<()> {
        let path = plist_path()?;
        let content = generate_service(params);

        // Unload first if already installed (ignore errors)
        if path.exists() {
            let _ = std::process::Command::new("launchctl")
                .args(["unload", &path.display().to_string()])
                .output();
        }

        std::fs::write(&path, &content)
            .with_context(|| format!("failed to write plist to {}", path.display()))?;

        let output = std::process::Command::new("launchctl")
            .args(["load", &path.display().to_string()])
            .output()
            .context("failed to run launchctl load")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("launchctl load failed: {stderr}");
        }

        println!("Service installed and started:");
        println!("  Plist:  {}", path.display());
        println!("  Label:  {SERVICE_LABEL}");
        println!("\nThe daemon will start automatically on login.");
        println!("Use `brain daemon uninstall` to remove.");
        Ok(())
    }

    pub fn uninstall() -> Result<()> {
        let path = plist_path()?;

        if !path.exists() {
            println!("No service installed (expected plist: {})", path.display());
            return Ok(());
        }

        let output = std::process::Command::new("launchctl")
            .args(["unload", &path.display().to_string()])
            .output()
            .context("failed to run launchctl unload")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("Warning: launchctl unload returned: {stderr}");
        }

        std::fs::remove_file(&path)
            .with_context(|| format!("failed to remove {}", path.display()))?;

        println!("Service uninstalled:");
        println!("  Removed: {}", path.display());
        Ok(())
    }

    pub fn status() -> Result<()> {
        let path = plist_path()?;

        if !path.exists() {
            println!("No service installed (expected plist: {})", path.display());
            return Ok(());
        }

        println!("Service installed:");
        println!("  Plist: {}", path.display());
        println!("  Label: {SERVICE_LABEL}");

        let output = std::process::Command::new("launchctl")
            .args(["list", SERVICE_LABEL])
            .output();

        match output {
            Ok(o) if o.status.success() => {
                let stdout = String::from_utf8_lossy(&o.stdout);
                // Parse PID from launchctl list output
                if let Some(line) = stdout.lines().find(|l| l.contains("PID")) {
                    println!("  {}", line.trim());
                } else {
                    println!(
                        "  Status: loaded (check `launchctl list {SERVICE_LABEL}` for details)"
                    );
                }
            }
            _ => {
                println!("  Status: not loaded (plist exists but service is not active)");
            }
        }
        Ok(())
    }
}

// ─── Linux (systemd) ─────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
mod platform {
    use super::*;

    const UNIT_NAME: &str = "brain-daemon.service";

    fn unit_path() -> Result<PathBuf> {
        let home = dirs::home_dir().context("cannot determine home directory")?;
        let dir = home.join(".config").join("systemd").join("user");
        std::fs::create_dir_all(&dir)?;
        Ok(dir.join(UNIT_NAME))
    }

    pub fn generate_service(params: &ServiceParams) -> String {
        let bin = params.brain_bin.display();

        format!(
            r#"[Unit]
Description=Brain daemon
After=default.target

[Service]
Type=exec
ExecStart={bin} daemon start
Restart=on-failure
RestartSec=10

# Logging goes to journald by default
StandardOutput=journal
StandardError=journal
SyslogIdentifier=brain-daemon

[Install]
WantedBy=default.target
"#,
        )
    }

    pub fn install(params: &ServiceParams) -> Result<()> {
        let path = unit_path()?;
        let content = generate_service(params);

        std::fs::write(&path, &content)
            .with_context(|| format!("failed to write unit to {}", path.display()))?;

        // Reload systemd user daemon
        let reload = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .output()
            .context("failed to run systemctl daemon-reload")?;
        if !reload.status.success() {
            let stderr = String::from_utf8_lossy(&reload.stderr);
            bail!("systemctl daemon-reload failed: {stderr}");
        }

        // Enable and start
        let enable = std::process::Command::new("systemctl")
            .args(["--user", "enable", "--now", UNIT_NAME])
            .output()
            .context("failed to run systemctl enable")?;
        if !enable.status.success() {
            let stderr = String::from_utf8_lossy(&enable.stderr);
            bail!("systemctl enable --now failed: {stderr}");
        }

        println!("Service installed and started:");
        println!("  Unit:   {}", path.display());
        println!("  Name:   {UNIT_NAME}");
        println!("\nThe daemon will start automatically on login.");
        println!("Use `brain daemon uninstall` to remove.");
        println!("View logs: journalctl --user -u {UNIT_NAME} -f");
        Ok(())
    }

    pub fn uninstall() -> Result<()> {
        let path = unit_path()?;

        if !path.exists() {
            println!("No service installed (expected unit: {})", path.display());
            return Ok(());
        }

        // Stop and disable
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "disable", "--now", UNIT_NAME])
            .output();

        std::fs::remove_file(&path)
            .with_context(|| format!("failed to remove {}", path.display()))?;

        // Reload daemon
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .output();

        println!("Service uninstalled:");
        println!("  Removed: {}", path.display());
        Ok(())
    }

    pub fn status() -> Result<()> {
        let path = unit_path()?;

        if !path.exists() {
            println!("No service installed (expected unit: {})", path.display());
            return Ok(());
        }

        println!("Service installed:");
        println!("  Unit: {}", path.display());
        println!("  Name: {UNIT_NAME}");

        let output = std::process::Command::new("systemctl")
            .args(["--user", "is-active", UNIT_NAME])
            .output();

        match output {
            Ok(o) => {
                let state = String::from_utf8_lossy(&o.stdout).trim().to_string();
                println!("  Status: {state}");
            }
            Err(e) => {
                println!("  Status: unknown (systemctl error: {e})");
            }
        }
        Ok(())
    }
}

// ─── Unsupported platforms ───────────────────────────────────────────────

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod platform {
    use super::*;

    pub fn generate_service(_params: &ServiceParams) -> String {
        String::from("# Service generation is not supported on this platform")
    }

    pub fn install(_params: &ServiceParams) -> Result<()> {
        bail!("Auto-start is only supported on macOS (launchd) and Linux (systemd)")
    }

    pub fn uninstall() -> Result<()> {
        bail!("Auto-start is only supported on macOS (launchd) and Linux (systemd)")
    }

    pub fn status() -> Result<()> {
        bail!("Auto-start is only supported on macOS (launchd) and Linux (systemd)")
    }
}

// ─── Public API ──────────────────────────────────────────────────────────

/// Install the platform-native service for auto-start on login.
///
/// Generates a service that runs `brain daemon start` (which reads the
/// registry itself). The `brain_root` parameter is accepted for backwards
/// compatibility with existing callers but is not used — the service is
/// a single global service, not per-brain.
pub fn install(_brain_root: &Path, dry_run: bool) -> Result<()> {
    let params = ServiceParams::resolve()?;

    if dry_run {
        println!("# Generated service definition for brain daemon");
        println!("# Binary: {}", params.brain_bin.display());
        println!();
        print!("{}", platform::generate_service(&params));
        return Ok(());
    }

    platform::install(&params)
}

/// Uninstall the platform-native service.
///
/// The `brain_root` parameter is accepted for backwards compatibility but
/// is not used — the service is a single global `com.brain.daemon` service.
pub fn uninstall(_brain_root: &Path) -> Result<()> {
    platform::uninstall()
}

/// Show service installation status.
///
/// The `brain_root` parameter is accepted for backwards compatibility but
/// is not used — the service is a single global `com.brain.daemon` service.
pub fn status(_brain_root: &Path) -> Result<()> {
    platform::status()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_service_runs_daemon_start() {
        let params = ServiceParams {
            brain_bin: PathBuf::from("/usr/local/bin/brain"),
        };
        let content = platform::generate_service(&params);
        assert!(content.contains("brain"), "should reference brain binary");
        assert!(content.contains("daemon"), "should use daemon subcommand");
        assert!(content.contains("start"), "should use start argument");
    }

    #[test]
    fn test_generate_service_has_no_path_arguments() {
        let params = ServiceParams {
            brain_bin: PathBuf::from("/usr/local/bin/brain"),
        };
        let content = platform::generate_service(&params);
        // The service should not hardcode any notes path
        assert!(
            !content.contains("/home/user/project"),
            "should not hardcode notes path"
        );
    }
}
