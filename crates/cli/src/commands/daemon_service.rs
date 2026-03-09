//! Platform-native service installation for the brain daemon.
//!
//! Generates and manages launchd plists (macOS) or systemd user units (Linux)
//! so `brain watch` starts automatically on login.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// Resolved parameters for service installation.
struct ServiceParams {
    /// Absolute path to the `brain` binary.
    brain_bin: PathBuf,
    /// Absolute path to the brain project root (where `.brain/brain.toml` lives).
    brain_root: PathBuf,
    /// Brain name (from brain.toml).
    brain_name: String,
    /// Note directories to watch (absolute paths).
    note_dirs: Vec<PathBuf>,
}

impl ServiceParams {
    fn resolve(brain_root: &Path) -> Result<Self> {
        let brain_root = std::fs::canonicalize(brain_root)
            .with_context(|| format!("cannot resolve brain root: {}", brain_root.display()))?;

        let brain_dir = brain_root.join(".brain");
        let brain_toml =
            brain_lib::config::load_brain_toml(&brain_dir).map_err(|e| anyhow::anyhow!("{e}"))?;

        let note_dirs: Vec<PathBuf> = if brain_toml.notes.is_empty() {
            vec![brain_root.clone()]
        } else {
            brain_toml
                .notes
                .iter()
                .map(|p| {
                    if p.is_absolute() {
                        p.clone()
                    } else {
                        brain_root.join(p)
                    }
                })
                .collect()
        };

        // Resolve binary path from current executable
        let brain_bin = std::env::current_exe()
            .context("cannot determine the brain binary path. Ensure brain is installed.")?;
        let brain_bin = std::fs::canonicalize(&brain_bin).unwrap_or(brain_bin);

        Ok(Self {
            brain_bin,
            brain_root,
            brain_name: brain_toml.name,
            note_dirs,
        })
    }
}

// ─── macOS (launchd) ─────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
mod platform {
    use super::*;

    fn plist_label(brain_name: &str) -> String {
        format!("com.brain.watcher.{brain_name}")
    }

    fn plist_path(brain_name: &str) -> Result<PathBuf> {
        let home = dirs::home_dir().context("cannot determine home directory")?;
        let dir = home.join("Library").join("LaunchAgents");
        std::fs::create_dir_all(&dir)?;
        Ok(dir.join(format!("{}.plist", plist_label(brain_name))))
    }

    pub fn generate_service(params: &ServiceParams) -> String {
        let label = plist_label(&params.brain_name);
        let bin = params.brain_bin.display();
        // First note dir as the watch target (watch command takes one path)
        let notes_path = params
            .note_dirs
            .first()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| params.brain_root.display().to_string());
        let working_dir = params.brain_root.display();
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
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{bin}</string>
        <string>watch</string>
        <string>{notes_path}</string>
    </array>
    <key>WorkingDirectory</key>
    <string>{working_dir}</string>
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
        let path = plist_path(&params.brain_name)?;
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
        println!("  Label:  {}", plist_label(&params.brain_name));
        println!(
            "  Brain:  {} ({})",
            params.brain_name,
            params.brain_root.display()
        );
        println!("\nThe watcher will start automatically on login.");
        println!("Use `brain daemon uninstall` to remove.");
        Ok(())
    }

    pub fn uninstall(brain_root: &Path) -> Result<()> {
        let brain_dir = brain_root.join(".brain");
        let brain_toml =
            brain_lib::config::load_brain_toml(&brain_dir).map_err(|e| anyhow::anyhow!("{e}"))?;
        let path = plist_path(&brain_toml.name)?;

        if !path.exists() {
            println!("No service installed for brain '{}'", brain_toml.name);
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
        println!("  Brain:   {}", brain_toml.name);
        Ok(())
    }

    pub fn status(brain_root: &Path) -> Result<()> {
        let brain_dir = brain_root.join(".brain");
        let brain_toml =
            brain_lib::config::load_brain_toml(&brain_dir).map_err(|e| anyhow::anyhow!("{e}"))?;
        let label = plist_label(&brain_toml.name);
        let path = plist_path(&brain_toml.name)?;

        if !path.exists() {
            println!("No service installed for brain '{}'", brain_toml.name);
            return Ok(());
        }

        println!("Service installed:");
        println!("  Plist: {}", path.display());
        println!("  Label: {label}");

        let output = std::process::Command::new("launchctl")
            .args(["list", &label])
            .output();

        match output {
            Ok(o) if o.status.success() => {
                let stdout = String::from_utf8_lossy(&o.stdout);
                // Parse PID from launchctl list output
                if let Some(line) = stdout.lines().find(|l| l.contains("PID")) {
                    println!("  {}", line.trim());
                } else {
                    println!("  Status: loaded (check `launchctl list {label}` for details)");
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

    fn unit_name(brain_name: &str) -> String {
        format!("brain-watcher-{brain_name}.service")
    }

    fn unit_path(brain_name: &str) -> Result<PathBuf> {
        let home = dirs::home_dir().context("cannot determine home directory")?;
        let dir = home.join(".config").join("systemd").join("user");
        std::fs::create_dir_all(&dir)?;
        Ok(dir.join(unit_name(brain_name)))
    }

    pub fn generate_service(params: &ServiceParams) -> String {
        let bin = params.brain_bin.display();
        let notes_path = params
            .note_dirs
            .first()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| params.brain_root.display().to_string());
        let working_dir = params.brain_root.display();

        format!(
            r#"[Unit]
Description=Brain watcher for {name}
After=default.target

[Service]
Type=exec
ExecStart={bin} watch {notes_path}
WorkingDirectory={working_dir}
Restart=on-failure
RestartSec=10

# Logging goes to journald by default
StandardOutput=journal
StandardError=journal
SyslogIdentifier=brain-{name}

[Install]
WantedBy=default.target
"#,
            name = params.brain_name,
        )
    }

    pub fn install(params: &ServiceParams) -> Result<()> {
        let path = unit_path(&params.brain_name)?;
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
            .args(["--user", "enable", "--now", &unit_name(&params.brain_name)])
            .output()
            .context("failed to run systemctl enable")?;
        if !enable.status.success() {
            let stderr = String::from_utf8_lossy(&enable.stderr);
            bail!("systemctl enable --now failed: {stderr}");
        }

        println!("Service installed and started:");
        println!("  Unit:   {}", path.display());
        println!("  Name:   {}", unit_name(&params.brain_name));
        println!(
            "  Brain:  {} ({})",
            params.brain_name,
            params.brain_root.display()
        );
        println!("\nThe watcher will start automatically on login.");
        println!("Use `brain daemon uninstall` to remove.");
        println!(
            "View logs: journalctl --user -u {} -f",
            unit_name(&params.brain_name)
        );
        Ok(())
    }

    pub fn uninstall(brain_root: &Path) -> Result<()> {
        let brain_dir = brain_root.join(".brain");
        let brain_toml =
            brain_lib::config::load_brain_toml(&brain_dir).map_err(|e| anyhow::anyhow!("{e}"))?;
        let name = unit_name(&brain_toml.name);
        let path = unit_path(&brain_toml.name)?;

        if !path.exists() {
            println!("No service installed for brain '{}'", brain_toml.name);
            return Ok(());
        }

        // Stop and disable
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "disable", "--now", &name])
            .output();

        std::fs::remove_file(&path)
            .with_context(|| format!("failed to remove {}", path.display()))?;

        // Reload daemon
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .output();

        println!("Service uninstalled:");
        println!("  Removed: {}", path.display());
        println!("  Brain:   {}", brain_toml.name);
        Ok(())
    }

    pub fn status(brain_root: &Path) -> Result<()> {
        let brain_dir = brain_root.join(".brain");
        let brain_toml =
            brain_lib::config::load_brain_toml(&brain_dir).map_err(|e| anyhow::anyhow!("{e}"))?;
        let name = unit_name(&brain_toml.name);
        let path = unit_path(&brain_toml.name)?;

        if !path.exists() {
            println!("No service installed for brain '{}'", brain_toml.name);
            return Ok(());
        }

        println!("Service installed:");
        println!("  Unit: {}", path.display());
        println!("  Name: {name}");

        let output = std::process::Command::new("systemctl")
            .args(["--user", "is-active", &name])
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

    pub fn uninstall(_brain_root: &Path) -> Result<()> {
        bail!("Auto-start is only supported on macOS (launchd) and Linux (systemd)")
    }

    pub fn status(_brain_root: &Path) -> Result<()> {
        bail!("Auto-start is only supported on macOS (launchd) and Linux (systemd)")
    }
}

// ─── Public API ──────────────────────────────────────────────────────────

/// Install the platform-native service for auto-start on login.
///
/// Resolves the brain root from the given path (or cwd), generates the
/// appropriate service definition, and installs it.
pub fn install(brain_root: &Path, dry_run: bool) -> Result<()> {
    let params = ServiceParams::resolve(brain_root)?;

    if dry_run {
        println!(
            "# Generated service definition for brain '{}'",
            params.brain_name
        );
        println!("# Binary: {}", params.brain_bin.display());
        println!("# Root:   {}", params.brain_root.display());
        println!(
            "# Notes:  {:?}",
            params
                .note_dirs
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
        );
        println!();
        print!("{}", platform::generate_service(&params));
        return Ok(());
    }

    platform::install(&params)
}

/// Uninstall the platform-native service.
pub fn uninstall(brain_root: &Path) -> Result<()> {
    let brain_root = std::fs::canonicalize(brain_root)
        .with_context(|| format!("cannot resolve brain root: {}", brain_root.display()))?;
    platform::uninstall(&brain_root)
}

/// Show service installation status.
pub fn status(brain_root: &Path) -> Result<()> {
    let brain_root = std::fs::canonicalize(brain_root)
        .with_context(|| format!("cannot resolve brain root: {}", brain_root.display()))?;
    platform::status(&brain_root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_service_contains_brain_watch() {
        let params = ServiceParams {
            brain_bin: PathBuf::from("/usr/local/bin/brain"),
            brain_root: PathBuf::from("/home/user/project"),
            brain_name: "myproject".into(),
            note_dirs: vec![PathBuf::from("/home/user/project/notes")],
        };
        let content = platform::generate_service(&params);
        assert!(content.contains("brain"), "should reference brain binary");
        assert!(content.contains("watch"), "should use watch subcommand");
        assert!(
            content.contains("/home/user/project/notes"),
            "should include notes path"
        );
        assert!(content.contains("myproject"), "should include brain name");
    }

    #[test]
    fn test_generate_service_uses_root_as_fallback() {
        let params = ServiceParams {
            brain_bin: PathBuf::from("/usr/local/bin/brain"),
            brain_root: PathBuf::from("/home/user/project"),
            brain_name: "test".into(),
            note_dirs: vec![],
        };
        let content = platform::generate_service(&params);
        assert!(
            content.contains("/home/user/project"),
            "should fall back to brain root when no note dirs"
        );
    }
}
