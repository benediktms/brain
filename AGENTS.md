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

- `tasks_apply_event` — Single tool for all task mutations. Event types: `task_created`, `task_updated`, `status_changed`, `dependency_added`, `dependency_removed`, `comment_added`, `label_added`, `label_removed`, `note_linked`, `note_unlinked`, `parent_set`, `cross_brain_ref_added`, `cross_brain_ref_removed`. Accepts task ID as full ID or unique prefix (e.g. `BRN-01JPH`).
- `tasks_list` — List tasks filtered by status: `open` (default, excludes done), `ready` (no unresolved deps), `blocked` (has unresolved deps), `done`. Supports `task_ids` array for batch lookup, `limit` for pagination, `include_description` flag, and per-field filters: `priority` (0-4), `task_type`, `assignee`, `label`, `search` (FTS5 full-text search on title+description).
- `tasks_get` — Get full task details including relationships, comments, labels, linked notes, and cross-brain references (`cross_refs`). Use `expand` parameter (`parent`, `children`, `blocked_by`, `blocks`) to inline related task objects.
- `tasks_next` — Get highest-priority ready tasks sorted by priority then due date. Use for "what should I work on?" queries.
- `tasks_close` — Close one or more tasks by ID/prefix. Accepts a single string or array of task IDs. Returns closed tasks and newly unblocked task IDs.
- `tasks_labels_summary` — Get all unique labels with counts and associated task IDs (short prefixes). No parameters. Use for label discovery and taxonomy overview.
- `tasks_labels_batch` — Batch label operations. Actions: `add` (label + task_ids), `remove` (label + task_ids), `rename` (old_label + new_label), `purge` (label). Returns succeeded/failed/summary.
- `tasks_deps_batch` — Batch dependency operations. Actions: `add`/`remove` (pairs of task_id + depends_on_task_id), `chain` (ordered task_ids), `fan` (source_task_id + dependent_task_ids), `clear` (task_id). Returns succeeded/failed/summary.

**Note:** `tasks_apply_event` and `tasks_close` automatically generate and embed searchable capsules into LanceDB on every task create, update, or completion. Done/cancelled tasks get both a task capsule and an outcome capsule. Tasks become discoverable via `memory_search_minimal` without any extra steps. For tasks created before this feature, run `brain backfill-tasks` to index them.

**Tip:** Tasks live in the same vector store as notes. Use `memory_search_minimal` to find tasks by semantic meaning (e.g. "what was done about search ranking?"), or `tasks_list` with `search` for keyword matching. Task results have `kind: "task"` or `kind: "task-outcome"` — outcome capsules capture what was done and what was learned.

**Cross-brain tools:**

- `brains.list` — List all brain projects registered in `~/.brain/config.toml`. Returns `name`, `id`, `root` (filesystem path), and `prefix` (task ID prefix, e.g. `BRN`) for each brain. Use this to discover available targets before calling `tasks.create_remote`. Also callable as `brains_list`.
- `tasks.create_remote` — Create a task in another registered brain project. Required params: `brain` (registry name or 8-char brain ID) and `title`. Brain resolution tries the registry name first, then falls back to scanning by ID. The task is written directly into the remote brain's event store and gets the remote brain's task ID prefix. Optional params: `description`, `priority` (0–4, default 4), `task_type` (task|bug|feature|epic|spike, default task), `assignee`, `parent` (remote task ID). When `link_from` is provided (a local task ID), a `cross_brain_ref_added` event is appended to that local task, linking it to the newly created remote task. `link_type` controls the ref direction (depends_on|blocks|related, default related). Returns `remote_task_id`, `remote_brain_name`, `remote_brain_id`, and `local_ref_created`. Also callable as `tasks_create_remote`.

**Cross-brain workflow:**

1. Call `brains.list` to discover registered brains and their prefixes.
2. Call `tasks.create_remote` with the target brain name and task details.
3. Optionally pass `link_from` (a local task ID) to auto-create a cross-brain reference on the local task.

**Federated search:**

- Search across multiple brains in a single query via `--brain` (CLI) or `brains` parameter (MCP).
- Results are merged by hybrid score and labeled with source brain name (`brain_name` field in stubs).
- Architecture: `FederatedPipeline` in `query_pipeline.rs` fans out to each brain's `QueryPipeline`, merging results by `hybrid_score`. Each brain's `Db`/`StoreReader` is opened on demand using a shared embedder. `RemoteSearchContext` in `config/mod.rs` holds the per-brain context. `brain_name: Option<String>` on `MemoryStub` carries the source attribution.
- Single-brain queries remain the default and have no performance impact.

**Memory tools:**

- `memory_search_minimal` — Semantic search across indexed notes and tasks. Returns compact stubs (title, summary, score, kind). The `kind` field is `"note"` for indexed documents, `"task"` for active task capsules, or `"task-outcome"` for completed task outcomes. Use `intent` parameter to control ranking: `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy). Optional `tags` array boosts results matching the given tags via Jaccard similarity (e.g. `["rust", "memory"]`). Optional `brains` array to search across multiple brain projects (e.g. `["work", "personal"]`); use `["all"]` to search all registered brains. Results include a `brain_name` field indicating the source brain. Omitting `brains` defaults to single-brain search (backward compatible).
- `memory_expand` — Expand stubs from `search_minimal` to full content by chunk ID. Use `budget` to control token limit. Returns `byte_start`/`byte_end` offsets within the source file for each chunk.
- `memory_write_episode` — Record structured episodes (goal, actions, outcome) with tags and importance score.
- `memory_reflect` — Retrieve source material for a topic, suitable for reflection and synthesis.

**Records tools:**

| Tool | Description |
| --- | --- |
| `records.create_artifact` | Create a new artifact record with base64-encoded content |
| `records.save_snapshot` | Save an opaque state bundle as a snapshot record |
| `records.get` | Get a record by ID with full metadata, tags, and links (supports prefix resolution) |
| `records.list` | List records with optional filters (kind, status, tag, task_id) |
| `records.fetch_content` | Fetch raw content of a record as base64-encoded data |
| `records.archive` | Archive a record (metadata-only, payload preserved) |
| `records.tag_add` | Add a tag to a record (idempotent) |
| `records.tag_remove` | Remove a tag from a record (idempotent) |
| `records.link_add` | Link a record to a task or note chunk |
| `records.link_remove` | Remove a link from a record |

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

# Cross-brain task creation
brain tasks create --title="..." --brain=<NAME_OR_ID>          # Create in another brain
brain tasks create --title="..." --brain=infra --link-from=BRN-01JPHABC --link-type=related
                                                                # Create remote + auto-link local task

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

# Registry
brain list                     # List registered brains
brain list --json              # List as JSON (name, id, root, prefix)

# Records
brain artifacts <subcommand>   # Artifact management (alias: art)
brain snapshots <subcommand>   # Snapshot management (alias: snap)
brain records <subcommand>     # Records maintenance (verify, gc, evict, pin, unpin)

# Indexing
brain backfill-tasks           # Embed all tasks into the vector store
brain backfill-tasks --dry-run # Preview without writing

# Federated search (query across brains)
brain query "term"                              # Search current brain
brain query "term" --brain work --brain personal  # Search specific brains
brain query "term" --brain all                  # Search all registered brains

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
- `tasks_apply_event` — Single tool for all task mutations. Event types: `task_created`, `task_updated`, `status_changed`, `dependency_added`, `dependency_removed`, `comment_added`, `label_added`, `label_removed`, `note_linked`, `note_unlinked`, `parent_set`, `cross_brain_ref_added`, `cross_brain_ref_removed`. Accepts task ID as full ID or unique prefix (e.g. `BRN-01JPH`).
- `tasks_list` — List tasks filtered by status: `open` (default, excludes done), `ready` (no unresolved deps), `blocked` (has unresolved deps), `done`. Supports `task_ids` array for batch lookup, `limit` for pagination, `include_description` flag, and per-field filters: `priority` (0-4), `task_type`, `assignee`, `label`, `search` (FTS5 full-text search on title+description).
- `tasks_get` — Get full task details including relationships, comments, labels, linked notes, and cross-brain references (`cross_refs`). Use `expand` parameter (`parent`, `children`, `blocked_by`, `blocks`) to inline related task objects.
- `tasks_next` — Get highest-priority ready tasks sorted by priority then due date. Use for "what should I work on?" queries.
- `tasks_close` — Close one or more tasks by ID/prefix. Accepts a single string or array of task IDs. Returns closed tasks and newly unblocked task IDs.
- `tasks_labels_summary` — Get all unique labels with counts and associated task IDs (short prefixes). No parameters. Use for label discovery and taxonomy overview.
- `tasks_labels_batch` — Batch label operations. Actions: `add` (label + task_ids), `remove` (label + task_ids), `rename` (old_label + new_label), `purge` (label). Returns succeeded/failed/summary.
- `tasks_deps_batch` — Batch dependency operations. Actions: `add`/`remove` (pairs of task_id + depends_on_task_id), `chain` (ordered task_ids), `fan` (source_task_id + dependent_task_ids), `clear` (task_id). Returns succeeded/failed/summary.

**Note:** `tasks_apply_event` and `tasks_close` automatically generate and embed searchable capsules into LanceDB on every task create, update, or completion. Done/cancelled tasks get both a task capsule and an outcome capsule. Tasks become discoverable via `memory_search_minimal` without any extra steps. For tasks created before this feature, run `brain backfill-tasks` to index them.

**Tip:** Tasks live in the same vector store as notes. Use `memory_search_minimal` to find tasks by semantic meaning (e.g. "what was done about search ranking?"), or `tasks_list` with `search` for keyword matching. Task results have `kind: "task"` or `kind: "task-outcome"` — outcome capsules capture what was done and what was learned.

**Cross-brain tools:**
- `brains.list` — List all brain projects registered in `~/.brain/config.toml`. Returns `name`, `id`, `root` (filesystem path), and `prefix` (task ID prefix) for each brain. Use this to discover available targets before calling `tasks.create_remote`.
- `tasks.create_remote` — Create a task in another registered brain project. Required params: `brain` (registry name or 8-char brain ID) and `title`. Brain resolution tries the registry name first, then falls back to scanning by ID. Optional params: `description`, `priority` (0–4, default 4), `task_type`, `assignee`, `parent` (remote task ID). When `link_from` is provided (a local task ID), a cross-brain ref is added to that local task. `link_type` controls the ref direction (depends_on|blocks|related, default related). Returns `remote_task_id`, `remote_brain_name`, `remote_brain_id`, and `local_ref_created`.

**Cross-brain workflow:**
1. Call `brains.list` to discover registered brains and their prefixes.
2. Call `tasks.create_remote` with the target brain name and task details.
3. Optionally pass `link_from` (a local task ID) to auto-create a cross-brain reference on the local task.

**Federated search:**
- Search across multiple brains in a single query via `--brain` (CLI) or `brains` parameter (MCP).
- Results are merged by hybrid score and labeled with source brain name (`brain_name` field in stubs).
- Architecture: `FederatedPipeline` in `query_pipeline.rs` fans out to each brain's `QueryPipeline`, merging results by `hybrid_score`. Each brain's `Db`/`StoreReader` is opened on demand using a shared embedder. `RemoteSearchContext` in `config/mod.rs` holds the per-brain context. `brain_name: Option<String>` on `MemoryStub` carries the source attribution.
- Single-brain queries remain the default and have no performance impact.

**Memory tools:**
- `memory_search_minimal` — Semantic search across indexed notes and tasks. Returns compact stubs (title, summary, score, kind). The `kind` field is `"note"` for indexed documents, `"task"` for active task capsules, or `"task-outcome"` for completed task outcomes. Use `intent` parameter to control ranking: `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy). Optional `tags` array boosts results matching the given tags via Jaccard similarity (e.g. `["rust", "memory"]`). Optional `brains` array to search across multiple brain projects (e.g. `["work", "personal"]`); use `["all"]` to search all registered brains. Results include a `brain_name` field indicating the source brain. Omitting `brains` defaults to single-brain search (backward compatible).
- `memory_expand` — Expand stubs from `search_minimal` to full content by chunk ID. Use `budget` to control token limit. Returns `byte_start`/`byte_end` offsets within the source file for each chunk.
- `memory_write_episode` — Record structured episodes (goal, actions, outcome) with tags and importance score.
- `memory_reflect` — Retrieve source material for a topic, suitable for reflection and synthesis.

**Records tools:**
- `records.create_artifact` — Create a new artifact record with base64-encoded content.
- `records.save_snapshot` — Save an opaque state bundle as a snapshot record.
- `records.get` — Get a record by ID with full metadata, tags, and links (supports prefix resolution).
- `records.list` — List records with optional filters (kind, status, tag, task_id).
- `records.fetch_content` — Fetch raw content of a record as base64-encoded data.
- `records.archive` — Archive a record (metadata-only, payload preserved).
- `records.tag_add` — Add a tag to a record (idempotent).
- `records.tag_remove` — Remove a tag from a record (idempotent).
- `records.link_add` — Link a record to a task or note chunk.
- `records.link_remove` — Remove a link from a record.

**Other tools:**
- `status` — Health/status probe. Returns project name, brain ID, task counts, and index stats.

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

# Cross-brain task creation
brain tasks create --title="..." --brain=<NAME_OR_ID>          # Create in another brain
brain tasks create --title="..." --brain=infra --link-from=BRN-01X --link-type=related  # Create + auto-link

# Registry
brain list                     # List registered brains
brain list --json              # List as JSON (name, id, root, prefix)

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

# Federated search (query across brains)
brain query "term"                              # Search current brain
brain query "term" --brain work --brain personal  # Search specific brains
brain query "term" --brain all                  # Search all registered brains

# Agent docs
brain docs                     # Regenerate AGENTS.md + bridge CLAUDE.md
brain agent schema             # Output JSON Schema for all MCP tools
brain agent schema --pretty    # Pretty-printed output
brain agent schema --tool tasks.apply_event --pretty  # Single tool
```

> **Tip:** Run `brain agent schema --pretty` to get the full JSON Schema for all MCP tools, including exact per-event-type payload definitions for `tasks_apply_event`. This is useful for validating payloads before sending them.

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

**Cross-task insights**: If you discover during work on one task that something affects or should be captured on a different task, immediately add a comment to that task with the relevant context. Don't defer — the insight is freshest now and costs seconds to capture vs. minutes to reconstruct later.

**Planning references**: When planning work, always reference the task ID(s) being planned for and any related tasks that may be affected. This creates a traceable link between plans and the work they address, and helps future agents (or humans) understand why decisions were made.

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
