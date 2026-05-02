//! Install / uninstall brain's AI-agent plugins.
//!
//! For Claude Code, four separate marketplace directories are created, each containing one
//! plugin with domain-scoped skills:
//!
//!   brain-tasks   → /tasks:next, /tasks:create, …
//!   brain-mem     → /mem:search, /mem:write, …
//!   brain-records → /records:artifact, …
//!   brain-brain   → /brain:status, /brain:list, /brain:jobs, /brain:guide
//!
//! After writing files, the installer registers each marketplace and plugin
//! via the `claude` CLI so they're discovered on the next restart.
//!
//! For Codex, the same skills are materialized into one home-local plugin at
//! `~/.agents/plugins/brain` and registered in `~/.agents/plugins/marketplace.json`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::cli::PluginTarget;

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

fn marketplaces_dir() -> Result<std::path::PathBuf> {
    let home = dirs::home_dir().context("cannot determine home directory")?;
    Ok(home.join(".claude").join("plugins").join("marketplaces"))
}

fn codex_agents_root() -> Result<PathBuf> {
    let home = dirs::home_dir().context("cannot determine home directory")?;
    Ok(home.join(".agents"))
}

/// Parse a `plugin.json` string and return the hooks it declares.
///
/// Returns one `(event_name, command_summary)` pair per hook entry found under
/// the top-level `"hooks"` object. Entries that carry only the `_brain_managed`
/// sentinel (i.e. no nested `"hooks"` commands) are skipped. Event names are
/// sorted alphabetically for deterministic output.
///
/// Returns an empty [`Vec`] when `"hooks"` is absent, empty, or the input is
/// not valid JSON.
fn summarize_hooks(json_str: &str) -> Vec<(String, String)> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str) else {
        return vec![];
    };
    let Some(hooks_map) = value.get("hooks").and_then(|v| v.as_object()) else {
        return vec![];
    };

    let mut pairs: Vec<(String, String)> = Vec::new();

    let mut events: Vec<&String> = hooks_map.keys().collect();
    events.sort();

    for event in events {
        let Some(entries) = hooks_map[event].as_array() else {
            continue;
        };
        for entry in entries {
            let Some(inner_hooks) = entry.get("hooks").and_then(|v| v.as_array()) else {
                continue;
            };
            for hook in inner_hooks {
                let command = hook
                    .get("command")
                    .and_then(|v| v.as_str())
                    .unwrap_or("<command>");
                let summary = if command.len() > 80 {
                    format!("{}…", &command[..80])
                } else {
                    command.to_owned()
                };
                pairs.push((event.clone(), summary));
            }
        }
    }

    pairs
}

/// (marketplace_dir_name, plugin_name, templates)
struct PluginSpec {
    /// Directory name under ~/.claude/plugins/marketplaces/
    marketplace: &'static str,
    /// Plugin name as registered with Claude Code (e.g. "tasks")
    plugin: &'static str,
    templates: &'static [TemplateFile],
}

const PLUGINS: &[PluginSpec] = &[
    // ─── tasks plugin (/tasks:next, /tasks:create, …) ────────
    PluginSpec {
        marketplace: "brain-tasks",
        plugin: "tasks",
        templates: &[
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/marketplace.json"),
                output_path: ".claude-plugin/marketplace.json",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/plugin.json"),
                output_path: ".claude-plugin/plugin.json",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/next/SKILL.md"),
                output_path: "skills/next/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/create/SKILL.md"),
                output_path: "skills/create/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/show/SKILL.md"),
                output_path: "skills/show/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/list/SKILL.md"),
                output_path: "skills/list/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/close/SKILL.md"),
                output_path: "skills/close/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/update/SKILL.md"),
                output_path: "skills/update/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/blocked/SKILL.md"),
                output_path: "skills/blocked/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/stats/SKILL.md"),
                output_path: "skills/stats/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/dep/SKILL.md"),
                output_path: "skills/dep/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/tasks/skills/label/SKILL.md"),
                output_path: "skills/label/SKILL.md",
            },
        ],
    },
    // ─── mem plugin (/mem:search, /mem:write, …) ─────────────
    PluginSpec {
        marketplace: "brain-mem",
        plugin: "mem",
        templates: &[
            TemplateFile {
                content: include_str!("../templates/plugins/mem/marketplace.json"),
                output_path: ".claude-plugin/marketplace.json",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/mem/plugin.json"),
                output_path: ".claude-plugin/plugin.json",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/mem/skills/search/SKILL.md"),
                output_path: "skills/search/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/mem/skills/write/SKILL.md"),
                output_path: "skills/write/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/mem/skills/reflect/SKILL.md"),
                output_path: "skills/reflect/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/mem/skills/consolidate/SKILL.md"),
                output_path: "skills/consolidate/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/mem/skills/summarize/SKILL.md"),
                output_path: "skills/summarize/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/mem/skills/procedure/SKILL.md"),
                output_path: "skills/procedure/SKILL.md",
            },
        ],
    },
    // ─── records plugin (/records:artifact, …) ───────────────
    PluginSpec {
        marketplace: "brain-records",
        plugin: "records",
        templates: &[
            TemplateFile {
                content: include_str!("../templates/plugins/records/marketplace.json"),
                output_path: ".claude-plugin/marketplace.json",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/records/plugin.json"),
                output_path: ".claude-plugin/plugin.json",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/records/skills/artifact/SKILL.md"),
                output_path: "skills/artifact/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/records/skills/snapshot/SKILL.md"),
                output_path: "skills/snapshot/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/records/skills/search/SKILL.md"),
                output_path: "skills/search/SKILL.md",
            },
        ],
    },
    // ─── brain plugin (/brain:status, …, guide, agent) ───────
    PluginSpec {
        marketplace: "brain-brain",
        plugin: "brain",
        templates: &[
            TemplateFile {
                content: include_str!("../templates/plugins/brain/marketplace.json"),
                output_path: ".claude-plugin/marketplace.json",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/brain/plugin.json"),
                output_path: ".claude-plugin/plugin.json",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/brain/skills/status/SKILL.md"),
                output_path: "skills/status/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/brain/skills/list/SKILL.md"),
                output_path: "skills/list/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/brain/skills/jobs/SKILL.md"),
                output_path: "skills/jobs/SKILL.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/brain/skills/guide/SKILL.md"),
                output_path: "skills/guide/SKILL.md",
            },
            TemplateFile {
                content: include_str!(
                    "../templates/plugins/brain/skills/guide/resources/TASK_WORKFLOW.md"
                ),
                output_path: "skills/guide/resources/TASK_WORKFLOW.md",
            },
            TemplateFile {
                content: include_str!(
                    "../templates/plugins/brain/skills/guide/resources/MEMORY_PATTERNS.md"
                ),
                output_path: "skills/guide/resources/MEMORY_PATTERNS.md",
            },
            TemplateFile {
                content: include_str!(
                    "../templates/plugins/brain/skills/guide/resources/RECORDS_GUIDE.md"
                ),
                output_path: "skills/guide/resources/RECORDS_GUIDE.md",
            },
            TemplateFile {
                content: include_str!(
                    "../templates/plugins/brain/skills/guide/resources/TROUBLESHOOTING.md"
                ),
                output_path: "skills/guide/resources/TROUBLESHOOTING.md",
            },
            TemplateFile {
                content: include_str!("../templates/plugins/brain/agents/task-agent.md"),
                output_path: "agents/task-agent.md",
            },
        ],
    },
];

const CODEX_PLUGIN_JSON: &str = r##"{
  "name": "brain",
  "description": "Brain task, memory, records, and administration skills for Codex.",
  "version": "{{version}}",
  "author": { "name": "Benedikt Schnatterbeck" },
  "license": "MIT",
  "keywords": ["brain", "tasks", "memory", "records", "mcp"],
  "skills": "./skills/",
  "interface": {
    "displayName": "Brain",
    "shortDescription": "Persistent memory, tasks, and records for local agent work.",
    "longDescription": "Brain provides Codex skills for task tracking, semantic memory retrieval, episode writing, typed records, snapshots, and project health checks through the brain MCP server.",
    "developerName": "Benedikt Schnatterbeck",
    "category": "Productivity",
    "capabilities": ["Interactive", "Read", "Write"],
    "defaultPrompt": [
      "Show my next ready Brain task.",
      "Search Brain memory for project context.",
      "Create a Brain record for this plan."
    ],
    "brandColor": "#275DAD"
  }
}
"##;

const CODEX_PLUGIN_FILES: &[TemplateFile] = &[
    TemplateFile {
        content: CODEX_PLUGIN_JSON,
        output_path: ".codex-plugin/plugin.json",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/next/SKILL.md"),
        output_path: "skills/tasks-next/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/create/SKILL.md"),
        output_path: "skills/tasks-create/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/show/SKILL.md"),
        output_path: "skills/tasks-show/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/list/SKILL.md"),
        output_path: "skills/tasks-list/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/close/SKILL.md"),
        output_path: "skills/tasks-close/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/update/SKILL.md"),
        output_path: "skills/tasks-update/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/blocked/SKILL.md"),
        output_path: "skills/tasks-blocked/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/stats/SKILL.md"),
        output_path: "skills/tasks-stats/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/dep/SKILL.md"),
        output_path: "skills/tasks-dep/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/tasks/skills/label/SKILL.md"),
        output_path: "skills/tasks-label/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/mem/skills/search/SKILL.md"),
        output_path: "skills/mem-search/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/mem/skills/write/SKILL.md"),
        output_path: "skills/mem-write/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/mem/skills/reflect/SKILL.md"),
        output_path: "skills/mem-reflect/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/mem/skills/consolidate/SKILL.md"),
        output_path: "skills/mem-consolidate/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/mem/skills/summarize/SKILL.md"),
        output_path: "skills/mem-summarize/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/mem/skills/procedure/SKILL.md"),
        output_path: "skills/mem-procedure/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/records/skills/artifact/SKILL.md"),
        output_path: "skills/records-artifact/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/records/skills/snapshot/SKILL.md"),
        output_path: "skills/records-snapshot/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/records/skills/search/SKILL.md"),
        output_path: "skills/records-search/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/brain/skills/status/SKILL.md"),
        output_path: "skills/brain-status/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/brain/skills/list/SKILL.md"),
        output_path: "skills/brain-list/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/brain/skills/jobs/SKILL.md"),
        output_path: "skills/brain-jobs/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/brain/skills/guide/SKILL.md"),
        output_path: "skills/guide/SKILL.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/brain/skills/guide/resources/TASK_WORKFLOW.md"),
        output_path: "skills/guide/resources/TASK_WORKFLOW.md",
    },
    TemplateFile {
        content: include_str!(
            "../templates/plugins/brain/skills/guide/resources/MEMORY_PATTERNS.md"
        ),
        output_path: "skills/guide/resources/MEMORY_PATTERNS.md",
    },
    TemplateFile {
        content: include_str!("../templates/plugins/brain/skills/guide/resources/RECORDS_GUIDE.md"),
        output_path: "skills/guide/resources/RECORDS_GUIDE.md",
    },
    TemplateFile {
        content: include_str!(
            "../templates/plugins/brain/skills/guide/resources/TROUBLESHOOTING.md"
        ),
        output_path: "skills/guide/resources/TROUBLESHOOTING.md",
    },
];

pub fn install(target: PluginTarget, dry_run: bool) -> Result<()> {
    match target {
        PluginTarget::Claude => install_claude(dry_run),
        PluginTarget::Codex => install_codex(dry_run),
    }
}

pub fn uninstall(target: PluginTarget) -> Result<()> {
    match target {
        PluginTarget::Claude => uninstall_claude(),
        PluginTarget::Codex => uninstall_codex(),
    }
}

fn write_template_files(root: &Path, templates: &[TemplateFile]) -> Result<()> {
    for t in templates {
        let target = root.join(t.output_path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
        fs::write(&target, render(t.content))
            .with_context(|| format!("failed to write {}", target.display()))?;
    }
    Ok(())
}

fn install_claude(dry_run: bool) -> Result<()> {
    let base = marketplaces_dir()?;
    let total_files: usize = PLUGINS.iter().map(|p| p.templates.len()).sum();

    if dry_run {
        println!(
            "Would write {} files across {} plugins to {}",
            total_files,
            PLUGINS.len(),
            base.display()
        );
        for spec in PLUGINS {
            println!("  {}/", spec.marketplace);
            for t in spec.templates {
                if t.output_path == ".claude-plugin/plugin.json" {
                    let hooks = summarize_hooks(t.content);
                    if hooks.is_empty() {
                        println!("    {} (no hooks)", t.output_path);
                    } else {
                        println!("    {} ({} hooks)", t.output_path, hooks.len());
                        for (event, cmd) in &hooks {
                            println!("      {} → {}", event, cmd);
                        }
                    }
                } else {
                    println!("    {}", t.output_path);
                }
            }
        }
        println!("\nWould register each marketplace and install each plugin via `claude` CLI.");
        return Ok(());
    }

    // 1. Write template files.
    for spec in PLUGINS {
        let root = base.join(spec.marketplace);
        write_template_files(&root, spec.templates)?;
    }

    // 2. Register marketplaces and install plugins via `claude` CLI.
    let has_claude = Command::new("claude").arg("--version").output().is_ok();
    if has_claude {
        for spec in PLUGINS {
            let mp_path = base.join(spec.marketplace);
            let plugin_ref = format!("{}@{}", spec.plugin, spec.marketplace);

            // Register marketplace (idempotent — re-adding an existing one is fine).
            let _ = Command::new("claude")
                .args(["plugin", "marketplace", "add", &mp_path.to_string_lossy()])
                .output();

            // Install plugin (idempotent — already-installed is fine).
            let _ = Command::new("claude")
                .args(["plugin", "install", &plugin_ref])
                .output();
        }
        println!(
            "Installed brain Claude Code plugins ({total_files} files across {} plugins)",
            PLUGINS.len()
        );
        println!("  Plugins: tasks, mem, records, brain");
        println!("  Commands: /tasks:next, /mem:search, /records:artifact, /brain:status, ...");
        println!("\nRestart Claude Code to load the new plugins.");
    } else {
        println!(
            "Wrote {total_files} plugin files across {} marketplaces",
            PLUGINS.len()
        );
        println!("  Location: {}", base.display());
        println!("\n'claude' CLI not found — register manually with:");
        for spec in PLUGINS {
            let mp_path = base.join(spec.marketplace);
            println!("  claude plugin marketplace add {}", mp_path.display());
            println!(
                "  claude plugin install {}@{}",
                spec.plugin, spec.marketplace
            );
        }
    }

    // 3. Clean up legacy brain-marketplace if it exists.
    let legacy = base.join("brain-marketplace");
    if legacy.exists() {
        let _ = fs::remove_dir_all(&legacy);
        println!("  Removed legacy brain-marketplace directory.");
    }

    Ok(())
}

fn uninstall_claude() -> Result<()> {
    let base = marketplaces_dir()?;
    let has_claude = Command::new("claude").arg("--version").output().is_ok();

    let mut removed = 0;
    for spec in PLUGINS {
        let path = base.join(spec.marketplace);
        if !path.exists() {
            continue;
        }

        // Unregister from Claude Code.
        if has_claude {
            let plugin_ref = format!("{}@{}", spec.plugin, spec.marketplace);
            let _ = Command::new("claude")
                .args(["plugin", "uninstall", &plugin_ref])
                .output();
            let _ = Command::new("claude")
                .args(["plugin", "marketplace", "remove", spec.marketplace])
                .output();
        }

        // Remove files.
        fs::remove_dir_all(&path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
        removed += 1;
    }

    if removed == 0 {
        println!("No brain plugins found.");
    } else {
        println!("Removed {removed} brain Claude Code plugins.");
        println!("\nRestart Claude Code to unload the plugins.");
    }
    Ok(())
}

fn codex_plugin_dir(agents_root: &Path) -> PathBuf {
    agents_root.join("plugins").join("brain")
}

fn codex_marketplace_path(agents_root: &Path) -> PathBuf {
    agents_root.join("plugins").join("marketplace.json")
}

fn codex_marketplace_entry() -> Value {
    json!({
        "name": "brain",
        "source": {
            "source": "local",
            "path": "./plugins/brain"
        },
        "policy": {
            "installation": "AVAILABLE",
            "authentication": "ON_INSTALL"
        },
        "category": "Productivity"
    })
}

fn new_codex_marketplace() -> Value {
    json!({
        "name": "local",
        "interface": {
            "displayName": "Local Plugins"
        },
        "plugins": []
    })
}

fn read_codex_marketplace(path: &Path) -> Result<Value> {
    if !path.exists() {
        return Ok(new_codex_marketplace());
    }

    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&content).with_context(|| format!("failed to parse {}", path.display()))
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let mut content = serde_json::to_string_pretty(value)?;
    content.push('\n');
    fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))
}

fn upsert_codex_marketplace_entry(marketplace: &mut Value) -> Result<()> {
    let root = marketplace
        .as_object_mut()
        .context("Codex marketplace JSON must be an object")?;

    root.entry("name".to_string())
        .or_insert_with(|| json!("local"));

    let interface = root
        .entry("interface".to_string())
        .or_insert_with(|| json!({ "displayName": "Local Plugins" }));
    let interface_obj = interface
        .as_object_mut()
        .context("Codex marketplace `interface` must be an object")?;
    interface_obj
        .entry("displayName".to_string())
        .or_insert_with(|| json!("Local Plugins"));

    let plugins = root
        .entry("plugins".to_string())
        .or_insert_with(|| json!([]));
    let plugins = plugins
        .as_array_mut()
        .context("Codex marketplace `plugins` must be an array")?;
    let entry = codex_marketplace_entry();

    if let Some(existing) = plugins
        .iter_mut()
        .find(|plugin| plugin.get("name").and_then(Value::as_str) == Some("brain"))
    {
        *existing = entry;
    } else {
        plugins.push(entry);
    }

    Ok(())
}

fn remove_codex_marketplace_entry(marketplace: &mut Value) -> Result<bool> {
    let root = marketplace
        .as_object_mut()
        .context("Codex marketplace JSON must be an object")?;
    let Some(plugins) = root.get_mut("plugins") else {
        return Ok(false);
    };
    let plugins = plugins
        .as_array_mut()
        .context("Codex marketplace `plugins` must be an array")?;
    let before = plugins.len();
    plugins.retain(|plugin| plugin.get("name").and_then(Value::as_str) != Some("brain"));
    Ok(plugins.len() != before)
}

fn install_codex(dry_run: bool) -> Result<()> {
    let agents_root = codex_agents_root()?;
    install_codex_to_agents_root(&agents_root, dry_run)
}

fn install_codex_to_agents_root(agents_root: &Path, dry_run: bool) -> Result<()> {
    let plugin_dir = codex_plugin_dir(agents_root);
    let marketplace_path = codex_marketplace_path(agents_root);

    if dry_run {
        println!(
            "Would write {} files to {}",
            CODEX_PLUGIN_FILES.len(),
            plugin_dir.display()
        );
        for t in CODEX_PLUGIN_FILES {
            println!("  {}", t.output_path);
        }
        println!(
            "\nWould upsert Codex marketplace entry `brain` in {}",
            marketplace_path.display()
        );
        println!("  source.path: ./plugins/brain");
        return Ok(());
    }

    write_template_files(&plugin_dir, CODEX_PLUGIN_FILES)?;

    let mut marketplace = read_codex_marketplace(&marketplace_path)?;
    upsert_codex_marketplace_entry(&mut marketplace)?;
    write_json(&marketplace_path, &marketplace)?;

    println!(
        "Installed brain Codex plugin ({} files)",
        CODEX_PLUGIN_FILES.len()
    );
    println!("  Plugin: {}", plugin_dir.display());
    println!("  Marketplace: {}", marketplace_path.display());
    println!(
        "  Skills: brain:tasks-next, brain:mem-search, brain:records-artifact, brain:guide, ..."
    );
    println!("\nRestart Codex to load the new plugin.");
    Ok(())
}

fn uninstall_codex() -> Result<()> {
    let agents_root = codex_agents_root()?;
    uninstall_codex_from_agents_root(&agents_root)
}

fn uninstall_codex_from_agents_root(agents_root: &Path) -> Result<()> {
    let plugin_dir = codex_plugin_dir(agents_root);
    let marketplace_path = codex_marketplace_path(agents_root);

    let removed_plugin = if plugin_dir.exists() {
        fs::remove_dir_all(&plugin_dir)
            .with_context(|| format!("failed to remove {}", plugin_dir.display()))?;
        true
    } else {
        false
    };

    let mut removed_entry = false;
    if marketplace_path.exists() {
        let mut marketplace = read_codex_marketplace(&marketplace_path)?;
        removed_entry = remove_codex_marketplace_entry(&mut marketplace)?;
        write_json(&marketplace_path, &marketplace)?;
    }

    if removed_plugin || removed_entry {
        println!("Removed brain Codex plugin.");
        println!("\nRestart Codex to unload the plugin.");
    } else {
        println!("No brain Codex plugin found.");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
    fn install_dry_run_succeeds() {
        assert!(install(PluginTarget::Claude, true).is_ok());
        assert!(install(PluginTarget::Codex, true).is_ok());
    }

    #[test]
    fn codex_install_writes_plugin_and_marketplace() {
        let tmp = TempDir::new().unwrap();

        install_codex_to_agents_root(tmp.path(), false).unwrap();
        install_codex_to_agents_root(tmp.path(), false).unwrap();

        let plugin_dir = tmp.path().join("plugins").join("brain");
        assert!(plugin_dir.join(".codex-plugin/plugin.json").exists());
        assert!(plugin_dir.join("skills/tasks-next/SKILL.md").exists());
        assert!(plugin_dir.join("skills/mem-search/SKILL.md").exists());
        assert!(plugin_dir.join("skills/records-artifact/SKILL.md").exists());
        assert!(
            plugin_dir
                .join("skills/guide/resources/TASK_WORKFLOW.md")
                .exists()
        );

        let manifest: Value = serde_json::from_str(
            &fs::read_to_string(plugin_dir.join(".codex-plugin/plugin.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest["name"], "brain");
        assert_eq!(manifest["skills"], "./skills/");

        let marketplace: Value = serde_json::from_str(
            &fs::read_to_string(tmp.path().join("plugins/marketplace.json")).unwrap(),
        )
        .unwrap();
        let plugins = marketplace["plugins"].as_array().unwrap();
        assert_eq!(
            plugins
                .iter()
                .filter(|plugin| plugin["name"] == "brain")
                .count(),
            1
        );
        let brain = plugins
            .iter()
            .find(|plugin| plugin["name"] == "brain")
            .unwrap();
        assert_eq!(brain["source"]["source"], "local");
        assert_eq!(brain["source"]["path"], "./plugins/brain");
        assert_eq!(brain["policy"]["installation"], "AVAILABLE");
        assert_eq!(brain["policy"]["authentication"], "ON_INSTALL");
        assert_eq!(brain["category"], "Productivity");
    }

    #[test]
    fn codex_marketplace_upsert_preserves_unrelated_metadata_and_entries() {
        let mut marketplace = json!({
            "name": "custom-marketplace",
            "interface": {
                "displayName": "Custom Local",
                "theme": "kept"
            },
            "custom": {
                "preserved": true
            },
            "plugins": [
                {
                    "name": "brain",
                    "source": {
                        "source": "local",
                        "path": "./old"
                    },
                    "policy": {
                        "installation": "NOT_AVAILABLE",
                        "authentication": "ON_USE"
                    },
                    "category": "Old"
                },
                {
                    "name": "other",
                    "source": {
                        "source": "local",
                        "path": "./plugins/other"
                    },
                    "policy": {
                        "installation": "AVAILABLE",
                        "authentication": "ON_INSTALL"
                    },
                    "category": "Productivity"
                }
            ]
        });

        upsert_codex_marketplace_entry(&mut marketplace).unwrap();

        assert_eq!(marketplace["name"], "custom-marketplace");
        assert_eq!(marketplace["interface"]["displayName"], "Custom Local");
        assert_eq!(marketplace["interface"]["theme"], "kept");
        assert_eq!(marketplace["custom"]["preserved"], true);

        let plugins = marketplace["plugins"].as_array().unwrap();
        assert_eq!(plugins.len(), 2);
        assert_eq!(plugins[0]["name"], "brain");
        assert_eq!(plugins[0]["source"]["path"], "./plugins/brain");
        assert_eq!(plugins[1]["name"], "other");
        assert_eq!(plugins[1]["source"]["path"], "./plugins/other");
    }

    #[test]
    fn codex_uninstall_removes_only_brain_plugin_and_entry() {
        let tmp = TempDir::new().unwrap();
        install_codex_to_agents_root(tmp.path(), false).unwrap();

        let marketplace_path = tmp.path().join("plugins/marketplace.json");
        let mut marketplace: Value =
            serde_json::from_str(&fs::read_to_string(&marketplace_path).unwrap()).unwrap();
        marketplace["plugins"].as_array_mut().unwrap().push(json!({
            "name": "other",
            "source": {
                "source": "local",
                "path": "./plugins/other"
            },
            "policy": {
                "installation": "AVAILABLE",
                "authentication": "ON_INSTALL"
            },
            "category": "Productivity"
        }));
        write_json(&marketplace_path, &marketplace).unwrap();

        uninstall_codex_from_agents_root(tmp.path()).unwrap();
        uninstall_codex_from_agents_root(tmp.path()).unwrap();

        assert!(!tmp.path().join("plugins/brain").exists());
        let marketplace: Value =
            serde_json::from_str(&fs::read_to_string(&marketplace_path).unwrap()).unwrap();
        let plugins = marketplace["plugins"].as_array().unwrap();
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0]["name"], "other");
    }

    #[test]
    fn summarize_hooks_multiple_events() {
        let json = r#"{
            "hooks": {
                "UserPromptSubmit": [
                    {"_brain_managed": true, "hooks": [{"type": "command", "command": "brain tasks list --json"}]}
                ],
                "SessionStart": [
                    {"_brain_managed": true, "hooks": [{"type": "command", "command": "brain tasks stats --json"}]}
                ]
            }
        }"#;
        let pairs = summarize_hooks(json);
        // Events sorted alphabetically: SessionStart before UserPromptSubmit
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, "SessionStart");
        assert_eq!(pairs[0].1, "brain tasks stats --json");
        assert_eq!(pairs[1].0, "UserPromptSubmit");
        assert_eq!(pairs[1].1, "brain tasks list --json");
    }

    #[test]
    fn summarize_hooks_empty_hooks_object() {
        let json = r#"{"name": "tasks", "hooks": {}}"#;
        let pairs = summarize_hooks(json);
        assert!(pairs.is_empty());
    }

    #[test]
    fn summarize_hooks_missing_hooks_field() {
        let json = r#"{"name": "records", "version": "1.0.0"}"#;
        let pairs = summarize_hooks(json);
        assert!(pairs.is_empty());
    }

    #[test]
    fn summarize_hooks_malformed_json_returns_empty() {
        let pairs = summarize_hooks("this is not { valid json");
        assert!(pairs.is_empty());
    }
}
