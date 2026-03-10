use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::cli::McpTarget;

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
    let brain_bin = std::env::current_exe()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "brain".into());

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
