# brain

## Task Runner

This project uses [just](https://github.com/casey/just) as its task runner. Always prefer `just` recipes over raw commands (e.g., `just build` instead of `cargo build`).

Run `just` with no arguments to list all available recipes.

### Common recipes

```bash
just build        # Build
just test         # Test (pass args: just test -- --nocapture)
just lint         # Lint (fmt check + clippy)
just fmt          # Format code
just check        # cargo check
just install      # Build release binary and symlink to ~/bin/brain
just clean        # cargo clean
```

## Task Management

This project uses `brain` for task tracking. **Always use MCP tools for task operations** — they provide structured responses and are the canonical interface for AI agents. CLI commands exist for human terminal use only.

### MCP Tools (preferred for AI agents)

When running as an MCP server (`brain mcp`), these tools are available:

**Task tools:**

- `tasks_apply_event` — Single tool for all task mutations. Event types: `task_created`, `task_updated`, `status_changed`, `dependency_added`, `dependency_removed`, `comment_added`, `label_added`, `label_removed`, `note_linked`, `note_unlinked`, `parent_set`. Accepts task ID as full ID or unique prefix (e.g. `BRN-01JPH`).
- `tasks_list` — List tasks filtered by status: `open` (default, excludes done), `ready` (no unresolved deps), `blocked` (has unresolved deps), `done`. Supports `task_ids` array for batch lookup, `limit` for pagination, `include_description` flag, and per-field filters: `priority` (0-4), `task_type`, `assignee`, `label`, `search` (FTS5 full-text search on title+description).
- `tasks_get` — Get full task details including relationships, comments, labels, and linked notes. Use `expand` parameter (`parent`, `children`, `blocked_by`, `blocks`) to inline related task objects.
- `tasks_next` — Get highest-priority ready tasks sorted by priority then due date. Use for "what should I work on?" queries.
- `tasks_close` — Close one or more tasks by ID/prefix. Accepts a single string or array of task IDs. Returns closed tasks and newly unblocked task IDs.
- `tasks_labels_summary` — Get all unique labels with counts and associated task IDs (short prefixes). No parameters. Use for label discovery and taxonomy overview.
- `tasks_labels_batch` — Batch label operations. Actions: `add` (label + task_ids), `remove` (label + task_ids), `rename` (old_label + new_label), `purge` (label). Returns succeeded/failed/summary.
- `tasks_deps_batch` — Batch dependency operations. Actions: `add`/`remove` (pairs of task_id + depends_on_task_id), `chain` (ordered task_ids), `fan` (source_task_id + dependent_task_ids), `clear` (task_id). Returns succeeded/failed/summary.

**Memory tools:**

- `memory_search_minimal` — Semantic search across indexed notes. Returns compact stubs (title, summary, score). Use `intent` parameter to control ranking: `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy). Optional `tags` array boosts results matching the given tags via Jaccard similarity (e.g. `["rust", "memory"]`).
- `memory_expand` — Expand stubs from `search_minimal` to full content by chunk ID. Use `budget` to control token limit. Returns `byte_start`/`byte_end` offsets within the source file for each chunk.
- `memory_write_episode` — Record structured episodes (goal, actions, outcome) with tags and importance score.
- `memory_reflect` — Retrieve source material for a topic, suitable for reflection and synthesis.

### CLI Commands (for human terminal use)

```bash
# Finding work
brain tasks ready              # Show tasks with no blockers
brain tasks list               # List all tasks
brain tasks list --status=open # Filter by status
brain tasks list --search "query" # Full-text search
brain tasks list --priority 1 --label urgent # Combined filters
brain tasks show <id>          # Detailed task view

# Creating & updating
brain tasks create --title="..." --description="..." --type=task --priority=2
brain tasks update <id> --status=in_progress
brain tasks comment <id> "comment text"

# Dependencies
brain tasks dep add <task> <depends-on>
brain tasks dep add-chain BRN-01 BRN-02 BRN-03  # Sequential chain
brain tasks dep add-fan BRN-01 BRN-02,BRN-03    # Fan-out from source
brain tasks dep clear BRN-01                      # Remove all deps

# Batch labels
brain tasks label batch-add --tasks BRN-01,BRN-02 my-label
brain tasks label batch-remove --tasks BRN-01,BRN-02 old-label
brain tasks label rename old-label new-label
brain tasks label purge old-label

# Labels
brain tasks labels                    # List all labels with counts
brain tasks list --group-by label     # List tasks grouped by label

# Completing work
brain tasks close <id1> <id2>  # Close one or more tasks
brain tasks stats              # Project statistics

# Agent docs
brain docs                     # Regenerate AGENTS.md + bridge CLAUDE.md
```

### Finding Work

When the user asks what to work on next (e.g., "what's next?", "what should I work on?", "next task", "any work?"), always check brain tasks first:

1. Use `tasks_next` MCP tool to get unblocked tasks sorted by priority
2. Present the top candidates with their ID, title, priority, and type
3. If a task has dependencies, briefly note what's blocking it

### Workflow

When working on tasks:

1. **Before starting**: Mark the task `in_progress` via `tasks_apply_event` (status_changed)
2. **While working**: Add comments via `tasks_apply_event` (comment_added) for significant decisions or blockers
3. **On completion**: Close the task via `tasks_apply_event` (status_changed to `done`)

**Important**: Always close tasks when work is complete. If the brain MCP server is unavailable, fall back to the CLI: `brain tasks close <id>`

**Cross-task insights**: If you discover during work on one task that something affects or should be captured on a different task, immediately add a comment to that task with the relevant context. Don't defer — the insight is freshest now and costs seconds to capture vs. minutes to reconstruct later.

**Planning references**: When planning work, always reference the task ID(s) being planned for and any related tasks that may be affected. This creates a traceable link between plans and the work they address, and helps future agents (or humans) understand why decisions were made.

### Conventions

- **Priority scale**: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
- **Task types**: task, bug, feature, epic, spike
- **Statuses**: open, in_progress, blocked, done, cancelled

## Keeping AGENTS.md in sync

This file is the canonical reference for all AI agents working on this codebase (Claude Code, Cursor, Zed, Copilot, Windsurf). CLAUDE.md is a thin bridge that points here. When making changes that affect the documented surface area, **update this file as part of the same commit**:

- **MCP tool changes** (new tools, renamed tools, new/changed parameters, changed return shapes) → update the **MCP Tools** section above
- **CLI command changes** (new subcommands, changed flags, removed commands) → update the **CLI Commands** section above
- **Task runner changes** (new/renamed `just` recipes) → update the **Common recipes** section above
- **Workflow or convention changes** (new statuses, priority scale changes, new task types) → update the **Conventions** / **Workflow** sections above

If unsure whether a change warrants a docs update, err on the side of updating — stale docs cause more harm than verbose docs.

<!-- brain:start -->

## Build & Test

```bash
cargo build    # Build
cargo test     # Test
cargo clippy   # Lint
```

## Task Management

This project uses `brain` for task tracking. **Always use MCP tools for task operations** — they provide structured responses and are the canonical interface for AI agents. CLI commands exist for human terminal use only.

### MCP Tools (preferred for AI agents)

When running as an MCP server (`brain mcp`), these tools are available:

**Task tools:**

- `tasks_apply_event` — Single tool for all task mutations. Event types: `task_created`, `task_updated`, `status_changed`, `dependency_added`, `dependency_removed`, `comment_added`, `label_added`, `label_removed`, `note_linked`, `note_unlinked`, `parent_set`. Accepts task ID as full ID or unique prefix (e.g. `BRN-01JPH`).
- `tasks_list` — List tasks filtered by status: `open` (default, excludes done), `ready` (no unresolved deps), `blocked` (has unresolved deps), `done`. Supports `task_ids` array for batch lookup, `limit` for pagination, `include_description` flag, and per-field filters: `priority` (0-4), `task_type`, `assignee`, `label`, `search` (FTS5 full-text search on title+description).
- `tasks_get` — Get full task details including relationships, comments, labels, and linked notes. Use `expand` parameter (`parent`, `children`, `blocked_by`, `blocks`) to inline related task objects.
- `tasks_next` — Get highest-priority ready tasks sorted by priority then due date. Use for "what should I work on?" queries.
- `tasks_close` — Close one or more tasks by ID/prefix. Accepts a single string or array of task IDs. Returns closed tasks and newly unblocked task IDs.
- `tasks_labels_summary` — Get all unique labels with counts and associated task IDs (short prefixes). No parameters. Use for label discovery and taxonomy overview.
- `tasks_labels_batch` — Batch label operations. Actions: `add` (label + task_ids), `remove` (label + task_ids), `rename` (old_label + new_label), `purge` (label). Returns succeeded/failed/summary.
- `tasks_deps_batch` — Batch dependency operations. Actions: `add`/`remove` (pairs of task_id + depends_on_task_id), `chain` (ordered task_ids), `fan` (source_task_id + dependent_task_ids), `clear` (task_id). Returns succeeded/failed/summary.

**Memory tools:**

- `memory_search_minimal` — Semantic search across indexed notes. Returns compact stubs (title, summary, score). Use `intent` parameter to control ranking: `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy). Optional `tags` array boosts results matching the given tags via Jaccard similarity (e.g. `["rust", "memory"]`).
- `memory_expand` — Expand stubs from `search_minimal` to full content by chunk ID. Use `budget` to control token limit. Returns `byte_start`/`byte_end` offsets within the source file for each chunk.
- `memory_write_episode` — Record structured episodes (goal, actions, outcome) with tags and importance score.
- `memory_reflect` — Retrieve source material for a topic, suitable for reflection and synthesis.

### CLI Commands (for human terminal use)

```bash
# Finding work
brain tasks ready              # Show tasks with no blockers
brain tasks list               # List all tasks
brain tasks list --status=open # Filter by status
brain tasks list --search "query" # Full-text search
brain tasks list --priority 1 --label urgent # Combined filters
brain tasks show <id>          # Detailed task view

# Creating & updating
brain tasks create --title="..." --description="..." --type=task --priority=2
brain tasks update <id> --status=in_progress
brain tasks comment <id> "comment text"

# Dependencies
brain tasks dep add <task> <depends-on>
brain tasks dep add-chain BRN-01 BRN-02 BRN-03  # Sequential chain
brain tasks dep add-fan BRN-01 BRN-02,BRN-03    # Fan-out from source
brain tasks dep clear BRN-01                      # Remove all deps

# Labels
brain tasks labels                    # List all labels with counts
brain tasks list --group-by label     # List tasks grouped by label
brain tasks label batch-add --tasks BRN-01,BRN-02 my-label
brain tasks label rename old-label new-label
brain tasks label purge old-label

# Completing work
brain tasks close <id1> <id2>  # Close one or more tasks
brain tasks stats              # Project statistics

# Agent docs
brain docs                     # Regenerate AGENTS.md + bridge CLAUDE.md
```

### Finding Work

When the user asks what to work on next (e.g., "what's next?", "what should I work on?", "next task", "any work?"), always check brain tasks first:

1. Use `tasks_next` MCP tool to get unblocked tasks sorted by priority
2. Present the top candidates with their ID, title, priority, and type
3. If a task has dependencies, briefly note what's blocking it

### Workflow

When working on tasks:

1. **Before starting**: Mark the task `in_progress` via `tasks_apply_event` (status_changed)
2. **While working**: Add comments via `tasks_apply_event` (comment_added) for significant decisions or blockers
3. **On completion**: Close the task via `tasks_close` (or `tasks_apply_event` with status_changed to `done`)

**When creating tasks**, always add appropriate labels:

- Start with an `area:` label (see Label Schema below)
- Add `type:` label if the work type is distinct (e.g., `type:test`, `type:perf`, `type:refactor`)
- Use `phase:polish` for cleanup/final-touch tasks
- Review existing labels with `brain tasks labels` to avoid creating duplicates
- Maximum 3 labels per task; if you need more, the task may be too broad

### Conventions

- **Priority scale**: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
- **Task types**: task, bug, feature, epic, spike
- **Statuses**: open, in_progress, blocked, done, cancelled
<!-- brain:end -->

## Project Conventions

### Label Schema (3-Dimensional Taxonomy)

Labels are organized into three orthogonal dimensions. Each task should have at most 3 labels (typically 1-2). Use `brain tasks labels` to see available labels.

#### 1. Area (Component/Domain) — What part of the system?

| Label          | Use For                                                          | Examples                                               |
| -------------- | ---------------------------------------------------------------- | ------------------------------------------------------ |
| `area:memory`  | Semantic search, retrieval, ranking, embeddings, hybrid search   | Query pipeline, ranking engine, embeddings             |
| `area:tasks`   | Task system, events, dependencies, projections                   | Task creation, dependency management, event sourcing   |
| `area:records` | Records domain (artifacts, snapshots, content-addressed storage) | Artifact storage, snapshot management, content hashing |
| `area:cli`     | Command-line interface, user-facing commands                     | Command parsing, output formatting, subcommands        |
| `area:mcp`     | MCP server, tools, JSON-RPC protocol                             | MCP tool handlers, protocol implementation             |
| `area:index`   | Indexing pipeline, file scanning, chunking, parsing              | Scanner, chunker, parser, pipeline orchestration       |
| `area:infra`   | CI/CD, build tooling, developer experience, documentation        | GitHub Actions, build scripts, AGENTS.md               |
| `area:core`    | Database, schema, storage primitives, utilities                  | SQLite schema, LanceDB store, shared utilities         |

#### 2. Type (Work Category) — What kind of work?

| Label           | Use For                                                  |
| --------------- | -------------------------------------------------------- |
| `type:feature`  | New functionality or capability                          |
| `type:refactor` | Code restructuring, cleanup, improving maintainability   |
| `type:bugfix`   | Fixing incorrect or broken behavior                      |
| `type:test`     | Adding tests, improving testability, test infrastructure |
| `type:perf`     | Performance optimization, benchmarking                   |
| `type:docs`     | Documentation, comments, README updates                  |

#### 3. Phase (Lifecycle) — Where in development?

| Label          | Use For                                                      |
| -------------- | ------------------------------------------------------------ |
| `phase:design` | Architecture, RFC, planning, research spikes                 |
| `phase:polish` | Final touches, edge cases, cleanup after main implementation |

#### Special Labels

| Label         | Use For                                                         |
| ------------- | --------------------------------------------------------------- |
| `cross-brain` | Multi-brain features (federated search, cross-brain references) |

#### Labeling Guidelines

- **Start with area**: Every task should have an `area:` label
- **Add type when helpful**: Use `type:` to indicate work nature (especially `type:refactor`, `type:test`, `type:perf`)
- **Use phase sparingly**: Only for design-phase or polish-phase tasks
- **Maximum 3 labels**: If you need more, the task may be too broad
- **Prefer specific over vague**: `area:memory` is better than `performance` + `retrieval` + `ranking`

#### Common Label Combinations

- `area:memory` — Most memory/search tasks
- `area:memory,type:perf` — Search performance optimization
- `area:cli,type:test` — Adding CLI tests
- `area:index,phase:polish` — Indexing pipeline cleanup
- `area:records` — Records domain work
- `area:core,type:refactor` — Core library refactoring
