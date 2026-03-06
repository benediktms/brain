use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use brain_lib::config::{
    BrainEntry, BrainToml, brain_home, load_global_config, save_brain_toml, save_global_config,
};

/// Initialize a new brain in the current (or given) directory.
pub fn run(name: Option<String>, notes: Vec<PathBuf>, no_claude_md: bool) -> Result<()> {
    let cwd = std::env::current_dir().context("cannot determine current directory")?;
    let brain_dir = cwd.join(".brain");
    let marker_path = brain_dir.join("brain.toml");

    if marker_path.exists() {
        bail!(
            "Brain already initialized: {} exists",
            marker_path.display()
        );
    }

    // Derive brain name from explicit flag or directory name.
    let brain_name = name.unwrap_or_else(|| {
        cwd.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "brain".into())
    });

    // Default note dirs to cwd if none specified.
    let note_dirs: Vec<PathBuf> = if notes.is_empty() {
        vec![PathBuf::from(".")]
    } else {
        notes
    };

    // 1. Create .brain/ in the project root.
    fs::create_dir_all(&brain_dir)?;

    // 2. Write .brain/brain.toml
    let brain_toml = BrainToml {
        name: brain_name.clone(),
        notes: note_dirs.clone(),
    };
    save_brain_toml(&brain_dir, &brain_toml)?;

    // 3. Write .brain/.gitignore
    let gitignore_path = brain_dir.join(".gitignore");
    fs::write(
        &gitignore_path,
        "# Derived data — do not commit\nbrain.db*\nlancedb/\nmodels/\n",
    )?;

    // 4. Register in global config (~/.brain/config.toml)
    let mut global = load_global_config()?;

    let abs_notes: Vec<PathBuf> = note_dirs
        .iter()
        .map(|p| {
            if p.is_absolute() {
                p.clone()
            } else {
                cwd.join(p)
            }
        })
        .collect();

    global.brains.insert(
        brain_name.clone(),
        BrainEntry {
            root: cwd.clone(),
            notes: abs_notes,
        },
    );
    save_global_config(&global)?;

    // 5. Create ~/.brain/brains/<name>/
    let home = brain_home()?;
    let brains_dir = home.join("brains").join(&brain_name);
    fs::create_dir_all(&brains_dir)?;

    // 6. Generate CLAUDE.md (unless --no-claude-md)
    if !no_claude_md {
        let claude_md_path = cwd.join("CLAUDE.md");
        if !claude_md_path.exists() {
            let build_section = detect_build_section(&cwd);
            let content = CLAUDE_MD_TEMPLATE
                .replace("{brain_name}", &brain_name)
                .replace("{build_section}", &build_section);
            fs::write(&claude_md_path, content)?;
            println!("Generated CLAUDE.md");
        }
    }

    // 7. Print success
    let display_notes: Vec<String> = note_dirs.iter().map(|p| p.display().to_string()).collect();
    println!(
        "Brain \"{brain_name}\" initialized. Note directories: {:?}",
        display_notes
    );

    Ok(())
}

fn detect_build_section(cwd: &std::path::Path) -> String {
    if cwd.join("Cargo.toml").exists() {
        return r#"## Build & Test

```bash
cargo build    # Build
cargo test     # Test
cargo clippy   # Lint
```

"#
        .to_string();
    }
    if cwd.join("package.json").exists() {
        return r#"## Build & Test

```bash
npm install    # Install dependencies
npm test       # Test
npm run build  # Build
```

"#
        .to_string();
    }
    if cwd.join("justfile").exists() {
        return r#"## Build & Test

```bash
just           # Run default recipe
just test      # Test
just build     # Build
```

"#
        .to_string();
    }
    if cwd.join("Makefile").exists() {
        return r#"## Build & Test

```bash
make           # Build
make test      # Test
```

"#
        .to_string();
    }
    String::new()
}

const CLAUDE_MD_TEMPLATE: &str = r#"# {brain_name}

{build_section}## Task Management

This project uses `brain` for task tracking. Use the CLI or MCP tools.

### CLI Commands

```bash
# Finding work
brain tasks ready              # Show tasks with no blockers
brain tasks list               # List all tasks
brain tasks list --status=open # Filter by status
brain tasks show <id>          # Detailed task view

# Creating & updating
brain tasks create --title="..." --description="..." --type=task --priority=2
brain tasks update <id> --status=in_progress
brain tasks comment <id> "comment text"

# Dependencies
brain tasks dep add <task> <depends-on>

# Completing work
brain tasks close <id1> <id2>  # Close one or more tasks
brain tasks stats              # Project statistics
```

### MCP Tools

When running as an MCP server (`brain mcp`), these tools are available:
- `tasks_apply_event` — Create or update tasks via event sourcing
- `tasks_list` — List tasks with filters
- `tasks_get` — Get task details
- `tasks_next` — Get next highest-priority ready tasks
- `memory_search_minimal` — Search notes
- `memory_expand` — Expand memory stubs to full content
- `memory_write_episode` — Record episodes
- `memory_reflect` — Retrieve source material for reflection

### Conventions

- **Priority scale**: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
- **Task types**: task, bug, feature, epic
- **Statuses**: open, in_progress, blocked, done, cancelled
"#;
