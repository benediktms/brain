# brain

## Build & Test

```bash
cargo build    # Build
cargo test     # Test
cargo clippy   # Lint
```

## Task Management

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

### Finding Work

When the user asks what to work on next (e.g., "what's next?", "what should I work on?", "next task", "any work?"), always check brain tasks first:
1. Run `brain tasks ready` to show unblocked tasks sorted by priority
2. Present the top candidates with their ID, title, priority, and type
3. If a task has dependencies, briefly note what's blocking it

### Workflow

When working on tasks:
1. **Before starting**: Mark the task `in_progress` via `tasks_apply_event` (status_changed) or `brain tasks update <id> --status=in_progress`
2. **While working**: Add comments for significant decisions or blockers
3. **On completion**: Close the task via `tasks_apply_event` (status_changed to `done`) or `brain tasks close <id>`

### Conventions

- **Priority scale**: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
- **Task types**: task, bug, feature, epic
- **Statuses**: open, in_progress, blocked, done, cancelled
