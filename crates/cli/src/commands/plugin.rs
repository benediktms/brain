//! Install / uninstall brain's Claude Code plugins.
//!
//! Four plugins are installed into the brain-marketplace directory, each
//! providing domain-scoped skills:
//!
//!   tasks-plugin → /tasks:next, /tasks:create, …
//!   mem-plugin   → /mem:search, /mem:write, …
//!   records-plugin → /records:artifact, …
//!   brain-plugin → /brain:status, /brain:list, /brain:jobs, /brain:guide

use std::fs;

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

fn marketplace_root() -> Result<std::path::PathBuf> {
    let home = dirs::home_dir().context("cannot determine home directory")?;
    Ok(home
        .join(".claude")
        .join("plugins")
        .join("marketplaces")
        .join("brain-marketplace"))
}

/// All plugin subdirectory names within the brain marketplace.
const PLUGIN_DIRS: &[&str] = &[
    "tasks-plugin",
    "mem-plugin",
    "records-plugin",
    "brain-plugin",
];

pub fn install(dry_run: bool) -> Result<()> {
    let marketplace = marketplace_root()?;

    // ─── marketplace manifest ────────────────────────────────
    let marketplace_manifest = TemplateFile {
        content: include_str!("../templates/plugins/marketplace.json"),
        output_path: ".claude-plugin/marketplace.json",
    };

    // ─── tasks plugin (/tasks:next, /tasks:create, …) ────────
    let tasks_templates: &[TemplateFile] = &[
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
    ];

    // ─── mem plugin (/mem:search, /mem:write, …) ─────────────
    let mem_templates: &[TemplateFile] = &[
        TemplateFile {
            content: include_str!("../templates/plugins/mem/plugin.json"),
            output_path: ".claude-plugin/plugin.json",
        },
        TemplateFile {
            content: include_str!("../templates/plugins/mem/skills/search/SKILL.md"),
            output_path: "skills/search/SKILL.md",
        },
        TemplateFile {
            content: include_str!("../templates/plugins/mem/skills/expand/SKILL.md"),
            output_path: "skills/expand/SKILL.md",
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
    ];

    // ─── records plugin (/records:artifact, …) ───────────────
    let records_templates: &[TemplateFile] = &[
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
    ];

    // ─── brain plugin (/brain:status, …, guide, agent) ───────
    let brain_templates: &[TemplateFile] = &[
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
    ];

    let plugins: &[(&str, &[TemplateFile])] = &[
        ("tasks-plugin", tasks_templates),
        ("mem-plugin", mem_templates),
        ("records-plugin", records_templates),
        ("brain-plugin", brain_templates),
    ];

    let total_files: usize = plugins.iter().map(|(_, ts)| ts.len()).sum();

    if dry_run {
        println!(
            "Would write {} files across {} plugins to {}",
            total_files + 1, // +1 for marketplace.json
            plugins.len(),
            marketplace.display()
        );
        println!("  {}", marketplace_manifest.output_path);
        for (dir, templates) in plugins {
            println!("  {dir}/");
            for t in *templates {
                println!("    {}", t.output_path);
            }
        }
        return Ok(());
    }

    // Write marketplace manifest.
    let manifest_target = marketplace.join(marketplace_manifest.output_path);
    if let Some(parent) = manifest_target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    fs::write(&manifest_target, render(marketplace_manifest.content))
        .with_context(|| format!("failed to write {}", manifest_target.display()))?;

    // Write plugin files.
    for (dir, templates) in plugins {
        let plugin_root = marketplace.join(dir);
        for t in *templates {
            let target = plugin_root.join(t.output_path);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create directory {}", parent.display()))?;
            }
            let rendered = render(t.content);
            fs::write(&target, rendered)
                .with_context(|| format!("failed to write {}", target.display()))?;
        }
    }

    println!(
        "Installed brain Claude Code plugins ({} files across {} plugins)",
        total_files,
        plugins.len()
    );
    println!("  Location: {}", marketplace.display());
    println!("  Plugins: tasks, mem, records, brain");
    println!("  Commands: /tasks:next, /mem:search, /records:artifact, /brain:status, ...");
    println!("\nRestart Claude Code to load the new plugins.");
    Ok(())
}

pub fn uninstall() -> Result<()> {
    let marketplace = marketplace_root()?;

    let mut removed = 0;
    for dir in PLUGIN_DIRS {
        let path = marketplace.join(dir);
        if path.exists() {
            fs::remove_dir_all(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
            removed += 1;
        }
    }

    if removed == 0 {
        println!("No brain plugins found at {}", marketplace.display());
    } else {
        println!("Removed {removed} brain Claude Code plugins");
        println!("  Was at: {}", marketplace.display());
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
}
