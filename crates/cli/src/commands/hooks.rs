use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::{Map, Value, json};

/// The hook entries brain installs into `.claude/settings.json`.
fn brain_hooks() -> Value {
    json!({
        "UserPromptSubmit": [
            {
                "hooks": [
                    {
                        "type": "command",
                        "command": "brain tasks list --ready --json 2>/dev/null | head -c 2000"
                    }
                ]
            }
        ],
        "SessionStart": [
            {
                "hooks": [
                    {
                        "type": "command",
                        "command": "brain tasks stats --json 2>/dev/null"
                    }
                ]
            }
        ]
    })
}

/// Marker we use to detect brain-managed hook entries.
const BRAIN_COMMAND_PREFIX: &str = "brain tasks";

fn is_brain_hook(entry: &Value) -> bool {
    // New format: { "matcher": {}, "hooks": [{ "command": "brain tasks ..." }] }
    if let Some(hooks_arr) = entry.get("hooks").and_then(|h| h.as_array()) {
        return hooks_arr.iter().any(|hook| {
            hook.get("command")
                .and_then(|c| c.as_str())
                .is_some_and(|cmd| cmd.starts_with(BRAIN_COMMAND_PREFIX))
        });
    }
    // Legacy format: { "command": "brain tasks ..." }
    entry
        .get("command")
        .and_then(|c| c.as_str())
        .is_some_and(|cmd| cmd.starts_with(BRAIN_COMMAND_PREFIX))
}

/// Merge brain hooks into an existing hooks object, preserving non-brain hooks.
fn merge_hooks(existing: &Value) -> Value {
    let brain = brain_hooks();
    let brain_obj = brain.as_object().unwrap();

    let mut merged = existing.as_object().cloned().unwrap_or_else(Map::new);

    for (event_name, brain_entries) in brain_obj {
        let brain_arr = brain_entries.as_array().unwrap();

        let existing_arr = merged
            .get(event_name)
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        // Keep non-brain hooks, then append brain hooks
        let mut new_arr: Vec<Value> = existing_arr
            .into_iter()
            .filter(|entry| !is_brain_hook(entry))
            .collect();
        new_arr.extend(brain_arr.clone());

        merged.insert(event_name.clone(), Value::Array(new_arr));
    }

    Value::Object(merged)
}

pub fn install(dry_run: bool) -> Result<()> {
    let hooks = brain_hooks();

    if dry_run {
        println!("Hook configuration that would be added to .claude/settings.json:\n");
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({ "hooks": hooks }))?
        );
        return Ok(());
    }

    let claude_dir = Path::new(".claude");
    let settings_path = claude_dir.join("settings.json");

    // Ensure .claude/ directory exists
    fs::create_dir_all(claude_dir).context("Failed to create .claude/ directory")?;

    // Read existing settings or start with empty object
    let mut settings: Value = if settings_path.exists() {
        let content =
            fs::read_to_string(&settings_path).context("Failed to read .claude/settings.json")?;
        serde_json::from_str(&content).context("Failed to parse .claude/settings.json")?
    } else {
        json!({})
    };

    // Merge hooks
    let existing_hooks = settings.get("hooks").cloned().unwrap_or(json!({}));
    let merged = merge_hooks(&existing_hooks);
    settings
        .as_object_mut()
        .unwrap()
        .insert("hooks".to_string(), merged);

    // Write back
    let output = serde_json::to_string_pretty(&settings)?;
    fs::write(&settings_path, format!("{output}\n"))
        .context("Failed to write .claude/settings.json")?;

    println!("Installed brain hooks into .claude/settings.json");
    println!();
    println!("Hooks added:");
    println!("  SessionStart  -> brain tasks stats --json");
    println!("  UserPromptSubmit -> brain tasks list --ready --json");

    Ok(())
}

pub fn status() -> Result<()> {
    let settings_path = Path::new(".claude/settings.json");

    if !settings_path.exists() {
        println!("Status: not installed");
        println!("  .claude/settings.json does not exist");
        println!("  Run `brain hooks install` to set up hooks");
        return Ok(());
    }

    let content =
        fs::read_to_string(settings_path).context("Failed to read .claude/settings.json")?;
    let settings: Value =
        serde_json::from_str(&content).context("Failed to parse .claude/settings.json")?;

    let hooks = settings.get("hooks");

    let has_session_start = hooks
        .and_then(|h| h.get("SessionStart"))
        .and_then(|v| v.as_array())
        .is_some_and(|arr| arr.iter().any(is_brain_hook));

    let has_prompt_submit = hooks
        .and_then(|h| h.get("UserPromptSubmit"))
        .and_then(|v| v.as_array())
        .is_some_and(|arr| arr.iter().any(is_brain_hook));

    if has_session_start && has_prompt_submit {
        println!("Status: installed");
        println!("  SessionStart:      active");
        println!("  UserPromptSubmit:  active");
    } else if has_session_start || has_prompt_submit {
        println!("Status: partially installed");
        println!(
            "  SessionStart:      {}",
            if has_session_start {
                "active"
            } else {
                "missing"
            }
        );
        println!(
            "  UserPromptSubmit:  {}",
            if has_prompt_submit {
                "active"
            } else {
                "missing"
            }
        );
        println!("  Run `brain hooks install` to fix");
    } else {
        println!("Status: not installed");
        println!("  .claude/settings.json exists but has no brain hooks");
        println!("  Run `brain hooks install` to set up hooks");
    }

    Ok(())
}
