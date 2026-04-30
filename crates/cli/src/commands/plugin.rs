//! Install / uninstall brain's Claude Code plugins.
//!
//! Four separate marketplace directories are created, each containing one
//! plugin with domain-scoped skills:
//!
//!   brain-tasks   → /tasks:next, /tasks:create, …
//!   brain-mem     → /mem:search, /mem:write, …
//!   brain-records → /records:artifact, …
//!   brain-brain   → /brain:status, /brain:list, /brain:jobs, /brain:guide
//!
//! After writing files, the installer registers each marketplace and plugin
//! via the `claude` CLI so they're discovered on the next restart.

use std::fs;
use std::process::Command;

use anyhow::{Context, Result};

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

pub fn install(dry_run: bool) -> Result<()> {
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
        for t in spec.templates {
            let target = root.join(t.output_path);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create directory {}", parent.display()))?;
            }
            fs::write(&target, render(t.content))
                .with_context(|| format!("failed to write {}", target.display()))?;
        }
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

pub fn uninstall() -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(install(true).is_ok());
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
