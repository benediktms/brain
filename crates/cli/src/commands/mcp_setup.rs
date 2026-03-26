use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::cli::McpTarget;

/// Resolve the installed brain binary path.
///
/// Checks `~/bin/brain` first (the canonical install location from `just install`),
/// then falls back to `current_exe()`, then the bare name `"brain"`.
///
/// This ensures MCP server configs always point to the stable installed binary,
/// not a transient debug/worktree build that may have incompatible schema versions.
pub fn installed_brain_bin() -> String {
    let canonical = dirs::home_dir().map(|h| h.join("bin").join("brain"));
    let current = std::env::current_exe().ok();
    resolve_brain_bin(canonical.as_deref(), current.as_deref())
}

/// Pure resolution logic, testable without filesystem side effects.
///
/// `canonical` is the known install path (`~/bin/brain`).
/// `current_exe` is the path of the currently running binary.
/// Prefers `canonical` when it exists on disk, otherwise falls back to `current_exe`,
/// then the bare name `"brain"`.
fn resolve_brain_bin(
    canonical: Option<&std::path::Path>,
    current_exe: Option<&std::path::Path>,
) -> String {
    // Prefer the canonical install location if it exists on disk.
    if let Some(path) = canonical.filter(|p| p.exists()) {
        return path.to_string_lossy().into_owned();
    }

    // Fallback: current executable path.
    if let Some(path) = current_exe {
        return path.to_string_lossy().into_owned();
    }

    "brain".into()
}

/// Register brain as a Claude Code MCP server (user scope).
///
/// Calls `claude mcp remove brain` then `claude mcp add` so the entry is
/// always up-to-date. Used by both `brain init` and `brain mcp setup claude`.
pub fn register_claude(brain_bin: &str, dry_run: bool) -> Result<()> {
    if dry_run {
        println!("Would run: claude mcp remove brain --scope user");
        println!("Would run: claude mcp add --scope user brain -- {brain_bin} mcp");
        return Ok(());
    }

    // Remove first in case it already exists (claude mcp add rejects duplicates).
    let _ = Command::new("claude")
        .args(["mcp", "remove", "brain", "--scope", "user"])
        .output();

    let status = Command::new("claude")
        .args([
            "mcp", "add", "--scope", "user", "brain", "--", brain_bin, "mcp",
        ])
        .status();

    match status {
        Ok(s) if s.success() => println!("Registered brain MCP server in Claude Code"),
        Ok(_) => anyhow::bail!("claude mcp add failed"),
        Err(_) => anyhow::bail!(
            "'claude' CLI not found. Install it from https://docs.anthropic.com/en/docs/claude-code"
        ),
    }

    Ok(())
}

/// Set up brain as an MCP server for Cursor (~/.cursor/mcp.json).
fn setup_cursor(brain_bin: &str, dry_run: bool) -> Result<()> {
    let home = dirs::home_dir().context("cannot determine home directory")?;
    let config_path = home.join(".cursor").join("mcp.json");

    let entry = json!({
        "mcpServers": {
            "brain": {
                "command": brain_bin,
                "args": ["mcp"]
            }
        }
    });

    if dry_run {
        println!("Would write to {}:\n", config_path.display());
        println!("{}", serde_json::to_string_pretty(&entry)?);
        return Ok(());
    }

    let merged = merge_json_file(&config_path, &entry)?;
    write_json_file(&config_path, &merged)?;

    println!("Configured brain MCP server in {}", config_path.display());
    Ok(())
}

/// Set up brain as an MCP server for VS Code (.vscode/settings.json in the
/// project root). Walks up from CWD to find a git repo root; falls back to CWD.
fn setup_vscode(brain_bin: &str, dry_run: bool) -> Result<()> {
    let project_root = find_git_root().unwrap_or_else(|| {
        std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
    });
    let config_path = project_root.join(".vscode").join("settings.json");

    let entry = json!({
        "mcp": {
            "servers": {
                "brain": {
                    "command": brain_bin,
                    "args": ["mcp"]
                }
            }
        }
    });

    if dry_run {
        println!("Would write to {}:\n", config_path.display());
        println!("{}", serde_json::to_string_pretty(&entry)?);
        return Ok(());
    }

    let merged = merge_json_file(&config_path, &entry)?;
    write_json_file(&config_path, &merged)?;

    println!("Configured brain MCP server in {}", config_path.display());
    Ok(())
}

/// Read an existing JSON file (or start from `{}`), deep-merge `overlay` into
/// it, and return the result. Preserves all existing keys.
fn merge_json_file(path: &Path, overlay: &Value) -> Result<Value> {
    let mut base: Value = if path.exists() {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        serde_json::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))?
    } else {
        json!({})
    };

    deep_merge(&mut base, overlay);
    Ok(base)
}

/// Write a JSON value to a file, creating parent directories as needed.
fn write_json_file(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let output = serde_json::to_string_pretty(value)?;
    fs::write(path, format!("{output}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

/// Recursively merge `overlay` into `base`. Object keys in overlay overwrite
/// or extend base; non-object values replace outright.
fn deep_merge(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, overlay_val) in overlay_map {
                let entry = base_map.entry(key.clone()).or_insert(json!(null));
                deep_merge(entry, overlay_val);
            }
        }
        (base, overlay) => {
            *base = overlay.clone();
        }
    }
}

/// Walk up from CWD to find the nearest directory containing `.git`.
fn find_git_root() -> Option<std::path::PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        if dir.join(".git").exists() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Entry point for `brain mcp setup <target>`.
pub fn run(target: McpTarget, dry_run: bool) -> Result<()> {
    let brain_bin = installed_brain_bin();

    // Check for brain project root (informational only).
    let cwd = std::env::current_dir().context("cannot determine current directory")?;
    if brain_lib::config::find_brain_root(&cwd).is_none() {
        eprintln!("Warning: no brain project found in current directory tree");
        eprintln!(
            "  The MCP server will start but memory tools won't work without a brain project."
        );
        eprintln!("  Run `brain init` to create one.\n");
    }

    // Check for embedding model (informational only).
    if let Ok(home) = brain_lib::config::brain_home() {
        let model_dir = home.join("models").join("bge-small-en-v1.5");
        if !model_dir.exists() {
            eprintln!(
                "Warning: embedding model not found at {}",
                model_dir.display()
            );
            eprintln!("  The MCP server will work in tasks-only mode.");
            eprintln!("  To download it, run the setup script:");
            eprintln!(
                "    curl -sSL https://raw.githubusercontent.com/benediktms/brain/master/scripts/setup-model.sh | bash"
            );
            eprintln!("  Or install the HuggingFace CLI manually:");
            eprintln!("    pip install huggingface_hub");
            eprintln!(
                "    hf download BAAI/bge-small-en-v1.5 config.json tokenizer.json model.safetensors --local-dir ~/.brain/models/bge-small-en-v1.5\n"
            );
        }
    }

    match target {
        McpTarget::Claude => register_claude(&brain_bin, dry_run),
        McpTarget::Cursor => setup_cursor(&brain_bin, dry_run),
        McpTarget::Vscode => setup_vscode(&brain_bin, dry_run),
    }
}

struct TemplateFile {
    content: &'static str,
    output_path: &'static str,
}

fn render(template: &str) -> String {
    let result = template.replace("{{version}}", env!("CARGO_PKG_VERSION"));
    debug_assert!(
        !result.contains("{{"),
        "unresolved template placeholder in output"
    );
    result
}

fn plugin_root() -> Result<std::path::PathBuf> {
    let home = dirs::home_dir().context("cannot determine home directory")?;
    Ok(home
        .join(".claude")
        .join("plugins")
        .join("marketplaces")
        .join("brain-marketplace")
        .join("claude-plugin"))
}

pub fn install_claude_plugin(dry_run: bool) -> Result<()> {
    let plugin_root = plugin_root()?;

    let templates: &[TemplateFile] = &[
        TemplateFile {
            content: include_str!("../templates/plugin/plugin.json"),
            output_path: ".claude-plugin/plugin.json",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/ready.md"),
            output_path: "commands/ready.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/create.md"),
            output_path: "commands/create.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/show.md"),
            output_path: "commands/show.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/list.md"),
            output_path: "commands/list.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/close.md"),
            output_path: "commands/close.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/update.md"),
            output_path: "commands/update.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/stats.md"),
            output_path: "commands/stats.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/blocked.md"),
            output_path: "commands/blocked.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/dep.md"),
            output_path: "commands/dep.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/label.md"),
            output_path: "commands/label.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/search.md"),
            output_path: "commands/search.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/expand.md"),
            output_path: "commands/expand.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/write-episode.md"),
            output_path: "commands/write-episode.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/reflect.md"),
            output_path: "commands/reflect.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/artifact.md"),
            output_path: "commands/artifact.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/snapshot.md"),
            output_path: "commands/snapshot.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/records.md"),
            output_path: "commands/records.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/status.md"),
            output_path: "commands/status.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/brains.md"),
            output_path: "commands/brains.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/procedure.md"),
            output_path: "commands/procedure.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/consolidate.md"),
            output_path: "commands/consolidate.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/summarize.md"),
            output_path: "commands/summarize.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/commands/jobs.md"),
            output_path: "commands/jobs.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/agents/task-agent.md"),
            output_path: "agents/task-agent.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/skills/brain/SKILL.md"),
            output_path: "skills/brain/SKILL.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/skills/brain/resources/TASK_WORKFLOW.md"),
            output_path: "skills/brain/resources/TASK_WORKFLOW.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/skills/brain/resources/MEMORY_PATTERNS.md"),
            output_path: "skills/brain/resources/MEMORY_PATTERNS.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/skills/brain/resources/RECORDS_GUIDE.md"),
            output_path: "skills/brain/resources/RECORDS_GUIDE.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugin/skills/brain/resources/TROUBLESHOOTING.md"),
            output_path: "skills/brain/resources/TROUBLESHOOTING.md",
        },
    ];

    if dry_run {
        println!(
            "Would write {} files to {}",
            templates.len(),
            plugin_root.display()
        );
        for t in templates {
            println!("  {}", t.output_path);
        }
        return Ok(());
    }

    for t in templates {
        let target = plugin_root.join(t.output_path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
        let rendered = render(t.content);
        fs::write(&target, rendered)
            .with_context(|| format!("failed to write {}", target.display()))?;
    }

    println!("Installed brain Claude Code plugin ({} files)", templates.len());
    println!("  Location: {}", plugin_root.display());
    println!("  To uninstall: rm -rf {}", plugin_root.display());
    println!("\nRestart Claude Code to load the new plugin.");
    Ok(())
}

pub fn uninstall_claude_plugin() -> Result<()> {
    let plugin_root = plugin_root()?;

    if !plugin_root.exists() {
        println!("No brain plugin found at {}", plugin_root.display());
        return Ok(());
    }

    fs::remove_dir_all(&plugin_root)
        .with_context(|| format!("failed to remove {}", plugin_root.display()))?;

    println!("Removed brain Claude Code plugin");
    println!("  Was at: {}", plugin_root.display());
    println!("\nRestart Claude Code to unload the plugin.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Before the fix, MCP config used `current_exe()` directly. When running
    /// from a worktree debug build (e.g. `target/debug/brain`), the config
    /// would point to that transient binary — which may auto-migrate the shared
    /// database with an incompatible schema version.
    ///
    /// This test reproduces the bugged behavior: when no canonical install
    /// exists, `resolve_brain_bin` falls back to whatever `current_exe` is,
    /// which could be a worktree build path.
    #[test]
    fn old_behavior_uses_worktree_binary_when_no_canonical_install() {
        let worktree_bin = PathBuf::from("/tmp/worktree/target/debug/brain");
        // No canonical path exists → falls back to current_exe (the worktree binary).
        let result = resolve_brain_bin(None, Some(&worktree_bin));
        assert_eq!(result, "/tmp/worktree/target/debug/brain");
    }

    /// With the fix: when the canonical install path exists on disk,
    /// `resolve_brain_bin` returns it regardless of what `current_exe` is.
    #[test]
    fn prefers_canonical_install_over_current_exe() {
        let tmp = TempDir::new().unwrap();
        let canonical = tmp.path().join("bin").join("brain");
        std::fs::create_dir_all(canonical.parent().unwrap()).unwrap();
        std::fs::write(&canonical, b"fake-binary").unwrap();

        let worktree_bin = PathBuf::from("/tmp/worktree/target/debug/brain");
        let result = resolve_brain_bin(Some(&canonical), Some(&worktree_bin));
        assert_eq!(result, canonical.to_string_lossy());
    }

    /// When the canonical path is provided but doesn't exist on disk,
    /// falls back to current_exe.
    #[test]
    fn falls_back_to_current_exe_when_canonical_missing() {
        let missing = PathBuf::from("/nonexistent/bin/brain");
        let current = PathBuf::from("/usr/local/bin/brain");
        let result = resolve_brain_bin(Some(&missing), Some(&current));
        assert_eq!(result, "/usr/local/bin/brain");
    }

    /// When neither canonical nor current_exe is available, returns bare "brain".
    #[test]
    fn falls_back_to_bare_name_when_nothing_available() {
        let result = resolve_brain_bin(None, None);
        assert_eq!(result, "brain");
    }

    #[test]
    fn render_replaces_version_placeholder() {
        let result = render("brain v{{version}} is great");
        assert!(result.contains(env!("CARGO_PKG_VERSION")));
        assert!(!result.contains("{{"));
    }

    #[test]
    fn render_passes_through_text_without_placeholders() {
        let result = render("no placeholders here");
        assert_eq!(result, "no placeholders here");
    }

    #[test]
    fn install_claude_plugin_dry_run_succeeds() {
        assert!(install_claude_plugin(true).is_ok());
    }

    #[test]
    fn deep_merge_preserves_existing_keys() {
        let mut base = json!({"a": 1, "b": {"c": 2, "d": 3}});
        let overlay = json!({"b": {"c": 99, "e": 4}, "f": 5});
        deep_merge(&mut base, &overlay);
        assert_eq!(
            base,
            json!({"a": 1, "b": {"c": 99, "d": 3, "e": 4}, "f": 5})
        );
    }
}
