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
```

<!-- brain:start:e4bd3468 -->
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
- `tasks_apply_event` — Single tool for all task mutations. Event types: `task_created`, `task_updated`, `status_changed`, `dependency_added`, `dependency_removed`, `comment_added`, `label_added`, `label_removed`, `note_linked`, `note_unlinked`, `parent_set`, `external_id_added`, `external_id_removed`. Accepts task ID as full ID or unique prefix (e.g. `BRN-01JPH`).
- `tasks_create` — Create a task with a flat schema (no event envelope). Required param: `title`. Optional: `description`, `priority` (0-4, default 4), `task_type` (task|bug|feature|epic|spike), `assignee`, `parent` (task ID prefix), `due_ts` (ISO 8601), `defer_until` (ISO 8601), `actor` (default: mcp). For remote creation: add `brain` (target brain name or ID from registry); optionally `link_from` (local task ID) and `link_type` (depends_on|blocks|related, default: related). Returns `{task_id, task, unblocked_task_ids}` for local creation, or `{remote_task_id, remote_brain_name, remote_brain_id, local_ref_created}` for remote creation.
- `tasks_list` — List tasks filtered by status: `open` (default, excludes done), `ready` (no unresolved deps), `blocked` (has unresolved deps), `done`, `in_progress` (exact match), `cancelled` (exact match). Supports `task_ids` array for batch lookup, `limit` for pagination, `include_description` flag, and per-field filters: `priority` (0-4), `task_type`, `assignee`, `label`, `search` (FTS5 full-text search on title+description). Optional `brains` array to query across multiple brain projects (e.g. `["work", "personal"]`); use `["all"]` (or `["*"]`) to query every registered brain. Each task is tagged with its source `brain`; federated responses include a top-level `brains` array. Singular `brain` is accepted as a deprecated alias for `brains: [name]`.
- `tasks_get` — Get full task details including relationships, comments, labels, linked notes, and external IDs (`external_ids`). Use `expand` parameter (`parent`, `children`, `blocked_by`, `blocks`) to inline related task objects.
- `tasks_next` — Get highest-priority ready tasks sorted by status (in-progress first), then priority, then due date. Use for "what should I work on?" queries. Optional `brains` array to query across multiple brains (e.g. `["work", "personal"]`); use `["all"]` to merge ready tasks from every registered brain and sort by priority globally. Each task is tagged with its source `brain`; federated responses include a top-level `brains` array.
- `tasks_close` — Close one or more tasks by ID/prefix. Accepts a single string or array of task IDs. Returns closed tasks and newly unblocked task IDs.
- `tasks_labels_summary` — Get all unique labels with counts and associated task IDs (short prefixes). No parameters. Use for label discovery and taxonomy overview.
- `tasks_labels_batch` — Batch label operations. Actions: `add` (label + task_ids), `remove` (label + task_ids), `rename` (old_label + new_label), `purge` (label). Supports `brain` param for cross-brain label management. Returns succeeded/failed/summary.
- `tasks_deps_batch` — Batch dependency operations. Actions: `add`/`remove` (pairs of task_id + depends_on_task_id), `chain` (ordered task_ids), `fan` (source_task_id + dependent_task_ids), `clear` (task_id). Returns succeeded/failed/summary.

**Note:** `tasks_apply_event` and `tasks_close` automatically generate and embed searchable capsules into LanceDB on every task create, update, or completion. Tasks become discoverable via `memory_search_minimal` without any extra steps.

**Brain tools:**
- `brains.list` — List all brain projects registered in `~/.brain/state_projection.toml`. Returns `name`, `id`, `root` (filesystem path), and `prefix` (task ID prefix) for each brain. Also callable as `brains_list`.

**Memory tools:**
- `memory_search_minimal` — Semantic search across indexed notes and tasks. Returns compact stubs (title, summary, score, kind). The `kind` field is one of: `"note"`, `"episode"`, `"reflection"`, `"procedure"`, `"task"`, `"task-outcome"`, `"record"`. Use `intent` parameter to control ranking: `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy). Optional `tags` array boosts results matching the given tags via Jaccard similarity (e.g. `["rust", "memory"]`). Optional `brains` array to search across multiple brain projects (e.g. `["work", "personal"]`); use `["all"]` to search all registered brains. Results include a `brain_name` field indicating the source brain. Supports metadata filters: `kinds` (array of kind strings to include), `time_after`/`time_before` (Unix timestamps), `tags_require` (AND — all must match), `tags_exclude` (NOR — any match excludes).
- `memory_expand` — Expand stubs from `search_minimal` to full content by chunk ID. Use `budget` to control token limit. Returns `byte_start`/`byte_end` offsets within the source file for each chunk.
- `memory_write_episode` — Record structured episodes (goal, actions, outcome) with tags and importance score.
- `memory_reflect` — Retrieve source material for a topic, suitable for reflection and synthesis.

**Records tools:**
- `records.create_document` — Create a document record with `text` (plain) or `data` (base64) content. Supports `brain` param for cross-brain writes.
- `records.create_analysis` — Create an analysis record with `text` (plain) or `data` (base64) content. Supports `brain` param for cross-brain writes.
- `records.create_plan` — Create a plan record with `text` (plain) or `data` (base64) content. Supports `brain` param for cross-brain writes.
- `records.save_snapshot` — Save a snapshot record with `text` (plain) or `data` (base64) content. Supports `brain` param for cross-brain writes.
- Per-kind policy: document/analysis/plan/summary are embedded and summarized; implementation/review/custom are embedded and searchable; snapshots are stored without embedding or summarization.
- `records.get` — Get a record by ID with full metadata, tags, and links (supports prefix resolution). Supports `brain` param for cross-brain access.
- `records.list` — List records with optional filters (kind, status, tag, task_id). Optional `brains` array to query across multiple brains (e.g. `["work", "personal"]`); use `["all"]` to query every registered brain. Each record is tagged with its source `brain`; federated responses include a top-level `brains` array. Singular `brain` is accepted as a deprecated alias for `brains: [name]`.
- `records.fetch_content` — Fetch raw content of a record. Text content (text/*, application/json, application/toml, application/yaml) is auto-decoded as UTF-8 and returned in a `text` field; binary content is returned as base64 in `data`. Response includes `encoding` ('utf-8' or 'base64'), `title`, and `kind` metadata. Supports `brain` param for cross-brain access.
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
brain tasks next               # Top 10 actionable tasks (priority-sorted)
brain tasks next -k 3          # Top 3 actionable tasks

# Creating & updating
brain tasks create --title="..." --description="..." --task-type=task --priority=2
brain tasks update <id> --status=in_progress
brain tasks comment <id> "comment text"

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

# Memory (semantic search & episodes)
brain memory search "query"                  # Search notes/tasks (compact stubs)
brain memory search -i lookup "exact term"   # Keyword-heavy search
brain memory search --brain all "patterns"   # Search all registered brains
brain memory expand <id1> <id2>              # Expand stubs to full content
brain memory write-episode --goal "..." --actions "..." --outcome "..."
brain memory reflect --topic "architecture"  # Prepare: get source material
brain memory reflect --commit --title "..." --content "..." --source-ids ep1,ep2

# Records (cross-brain writes supported via --brain)
brain documents create --title "Report" --file report.md
brain analyses create --title "Investigation" --brain other-brain --stdin
brain plans create --title "Rollout" --file rollout.md
brain snapshots save --title "State" --file state.json
brain snapshots save --title "State" --brain other-brain --stdin
brain artifacts restore <id>          # Print artifact content to stdout
brain artifacts restore <id> -o file  # Write artifact content to file
brain snapshots restore <id>          # Print snapshot content to stdout

# Status
brain status                  # Brain health check (task counts, index stats)
brain status --json           # Machine-readable JSON output

# Setup & management
brain init                     # Initialize a new brain in cwd
brain link <name>              # Link cwd as additional root for brain
brain alias add <alias> <name> # Add alias for a brain
brain alias remove <alias>     # Remove alias
brain alias list               # List aliases
brain config set <key> <val>   # Set brain config value
brain config get <key>         # Get brain config value
brain remove <name>            # Remove a brain from registry (alias: rm)
brain id                       # Show brain ID for current directory

# Daemon
brain daemon start [notes]     # Start background daemon
brain daemon stop              # Stop daemon
brain daemon status            # Check daemon status
brain daemon install           # Install launchd/systemd service
brain daemon uninstall         # Uninstall service

# Indexing & maintenance
brain reindex --full <path>    # Full reindex of notes
brain reindex --file <file>    # Reindex single file
brain vacuum                   # Clean stale data (default: >30 days)

# MCP server
brain mcp                      # Start MCP server (stdio)
brain mcp setup claude         # Auto-configure Claude Code MCP
brain mcp setup cursor         # Auto-configure Cursor MCP
brain mcp setup vscode         # Auto-configure VS Code MCP
brain hooks install            # Install git hooks

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

### Recording Context as Memory

When the user shares critical context that is not derivable from the current codebase, **proactively record it** using `memory_write_episode`. This preserves knowledge that would otherwise be lost between conversations.

**Record an episode when the user shares:**
- How an external API or service behaves (rate limits, quirks, undocumented behavior)
- Architecture or conventions of a different codebase that this project interacts with
- Business logic, domain rules, or constraints not captured in code
- Deployment topology, infrastructure details, or environment-specific behavior
- Historical context about why something was built a certain way
- Gotchas, workarounds, or lessons learned from past incidents

**How to record:** Use `memory_write_episode` with:
- `goal`: What the user was explaining or what prompted the context
- `actions`: The key facts, rules, or details shared
- `outcome`: How this knowledge should influence future work
- `tags`: Relevant topic tags for later retrieval (e.g. `["external-api", "payments"]`)

**Do not record:** Information already in the codebase, git history, or existing notes. Check `memory_search_minimal` first to avoid duplicates.

### Conventions

- **Priority scale**: 0=critical, 1=high, 2=medium, 3=low, 4=backlog
- **Task types**: task, bug, feature, epic, spike
- **Statuses**: open, in_progress, blocked, done, cancelled
<!-- brain:end -->

## Storage Architecture

Brain uses a **unified single-database model** where all brains share a centralized SQLite instance alongside per-brain vector indexes and a shared object store.

### Directory Layout

```
~/.brain/                                  # Central registry
  state_projection.toml                              # Global config + registered brains
  brain.db                                 # Unified SQLite (all brains, sole source of truth)
  brain.sock                               # Daemon Unix socket
  brain.pid                                # Daemon PID file
  brain.log                                # Daemon log file
  models/                                  # Shared embedding models
    bge-small-en-v1.5/                     # Default embedding model
  objects/                                 # Shared content-addressed object store (BLAKE3-keyed)
    <2-char prefix>/
      <full 64-char BLAKE3 hex>            # Immutable payload bytes
  brains/
    <brain-name>/
      state_projection.toml                          # Per-brain config (overrides global)
      lancedb/                             # Per-brain vector index (semantic space is distinct)
```

### Core Principles

1. **Unified SQLite (`~/.brain/brain.db`)**: Single database instance shared by all brains. Tasks and records tables include a `brain_id` column for partitioning. Queries filter by `brain_id` to isolate results per brain.

2. **Per-Brain Vector Store (`~/.brain/brains/<name>/lancedb/`)**: Each brain maintains separate LanceDB indexes. Semantic spaces are distinct — vectors from different brains are not comparable.

3. **Unified Object Store (`~/.brain/objects/`)**: Content-addressed blobs shared across all brains. Deduplication is global: two brains creating identical artifacts point to the same object on disk.

4. **`brain` Parameter = `brain_id` Filter**: When MCP tools or CLI commands receive a `brain` parameter (name or ID), it resolves to a `brain_id` and filters all queries. No per-brain database routing is needed.

5. **SQLite is Sole Source of Truth**: No JSONL event logs. All task and record mutations write directly to SQLite. Legacy JSONL files are read during `brain migrate` and `brain init` for one-time import only.

6. **External IDs for Cross-Brain References**: Cross-brain task references use the `external_ids` system (`external_id_added`/`external_id_removed` events). The source field encodes provenance as `brain:<name>:<link_type>`.

### Migration Path

Existing single-brain users upgrading from per-brain storage to unified storage run:

```bash
brain migrate
```

This command:
- Merges all per-brain `brain.db` databases into the central `~/.brain/brain.db`
- Migrates object stores from per-brain to unified `~/.brain/objects/`
- Preserves all task/record metadata
- Registers all brains in the central `~/.brain/state_projection.toml`

### Relationship to Notes, Tasks, and Records

Three parallel domains with decoupled lifecycles:

| Domain | Source of Truth | Derived State | Purpose |
|--------|---|---|---|
| Notes | Markdown files in repo | SQLite metadata + LanceDB embeddings | Semantic search, indexing |
| Tasks | SQLite (`brain.db`) | LanceDB capsules (searchable via `memory_search_minimal`) | Intent, execution state, dependencies |
| Records | SQLite (`brain.db`) + object store (`~/.brain/objects/`) | — | Work products, artifacts, snapshots |

All three domains live in the unified SQLite instance, partitioned by `brain_id`. See [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) and [docs/RECORDS.md](./docs/RECORDS.md) for detailed design.

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

<!-- neural_link:start:66ac518d -->
## neural_link — Multi-Agent Coordination

neural_link provides real-time coordination between agents via an MCP server.
All tools below are MCP tool calls prefixed with `neural_link` (e.g., `mcp__neural_link__room_open`).

### When to use neural_link

**Always use neural_link when 2 or more subagents are dispatched.** No exceptions.
The lead opens a room before dispatching, every subagent joins the room, and the lead participates alongside them.

### Roles

| Role | Who | Responsibilities |
|------|-----|------------------|
| **Lead** | The session that dispatches subagents | Opens the room before dispatch. Joins the room after dispatch. Monitors messages, answers questions, asks questions, unblocks agents. Closes the room when all work is done. Persists the summary. |
| **Subagent** | Each dispatched agent | Joins the room on activation. Communicates findings, blockers, and questions. Reads inbox periodically. Sends `handoff` before returning. |

### Coordination flow

#### Lead (dispatcher)

1. **Open a room** — call `room_open` with a descriptive `title` and `purpose` BEFORE dispatching any subagents. If working with brain-tracked projects, pass `brains` to enable artifact persistence on close.
2. **Dispatch subagents** — include the `room_id` in every subagent's prompt along with the instruction to join the room and communicate.
3. **Join the room** — after dispatching, the lead calls `room_join` to become a participant (use a stable identifier like `lead` or your session designation as `participant_id`).
4. **Monitor and participate** — while subagents work, the lead periodically reads the room via `inbox_read`. Answer questions from subagents. Ask questions if something is unclear. Send `decision` messages to resolve ambiguities. Unblock agents that report `blocker` messages.
5. **Close the room** — once all subagents have sent `handoff` and their work is complete, call `room_close` with a resolution. The server persists the full conversation as a brain artifact (if brains were declared) and returns structured extraction data: decisions, open questions, blockers, participant list, message count, and artifact record ID.
6. **Persist and present** — use the structured extraction from `room_close` to compose a narrative summary for the user. The artifact record ID links to the full conversation for future reference.

#### Subagent (participant)

1. **Join** — on activation, if a `room_id` was provided in your prompt, call `room_join` with the room_id, your designation as `participant_id` and `display_name`, and role `member`.
2. **Communicate** — send typed messages via `message_send` as you work. Share findings, flag blockers, ask questions. Use the appropriate message `kind` (see below).
3. **Read inbox** — call `inbox_read` periodically (after completing logical units of work). Process and `message_ack` all messages promptly. If the lead or another subagent asks a question, answer it. If a blocker is raised that you can resolve, respond.
4. **Wait when blocked** — if you cannot proceed until another agent provides something, use `wait_for` to block until the matching message arrives. Do not poll.
5. **Handoff** — when your work is complete, send a `handoff` message summarizing what you accomplished and any open items. This is mandatory — silent completion causes deadlocks.

### Message kinds

Every message has a `kind` that signals its intent. Use the right kind — other agents filter on it.

| Kind | When to use |
|------|-------------|
| `finding` | You discovered something another agent needs to know |
| `handoff` | Your part is done — summarize results and hand over |
| `blocker` | You cannot proceed until something is resolved |
| `decision` | Recording a choice that affects other agents |
| `question` | Asking another agent (or the lead) for information |
| `answer` | Responding to a question |
| `review_request` | Asking another agent to review your work |
| `review_result` | Delivering review feedback |
| `artifact_ref` | Pointing to a file, commit, or output another agent should consume |
| `summary` | Summarizing progress or conclusions |

### Waiting for other agents

`wait_for` is a blocking call. Your tool call is held open on the server until a matching message arrives or the timeout expires (default: 30s, max: 120s). You are effectively paused.

- **Use `wait_for` when you have nothing else to do** until a specific message arrives (e.g., waiting for a handoff, a review result, or an answer to your question)
- **Do not use `wait_for` if you have other work to do** — use `inbox_read` periodically instead
- **Filter precisely** — use the `kinds` and `from` params to match only what you need, avoiding false wakeups
- **Set reasonable timeouts** — a stuck `wait_for` blocks you for up to 120 seconds

### Tools reference

- **`room_open`** — Create a coordination room. Params: title (required), purpose, external_ref, tags, brains
- **`room_join`** — Join a room as a participant. Params: room_id (required), participant_id (required), display_name (required), role
- **`message_send`** — Send a typed message to a room. Params: room_id (required), from (required), kind (required), summary (required), to, body, thread_id, persist_hint
- **`inbox_read`** — Read your pending messages in a room. Params: room_id (required), participant_id (required)
- **`message_ack`** — Acknowledge messages you have processed. Params: room_id (required), participant_id (required), message_ids (required)
- **`wait_for`** — Block until a matching message arrives (long-poll). Params: room_id (required), participant_id (required), since_sequence, kinds, from, timeout_ms
- **`thread_summarize`** — Get structured coordination status (decisions, open questions, blockers) — read-only, no persistence. Params: room_id (required), thread_id
- **`room_close`** — Close a room. Persists full conversation as brain artifact, returns structured extraction. Params: room_id (required), resolution (required: completed|cancelled|superseded|failed)

### Rules

1. **Always acknowledge messages you have read.** Call `message_ack` after processing inbox messages. Unacknowledged messages reappear in your inbox.
2. **One room per coordination concern.** Do not multiplex unrelated work into a single room.
3. **Close rooms when done.** Always call `room_close` with a resolution (`completed`, `cancelled`, `superseded`, `failed`). Unclosed rooms leak state.
4. **Send `handoff` before returning.** If you are a subagent and your work is done, send a handoff message. Silent completion causes deadlocks.
5. **Never ignore a `blocker`.** If you receive a blocker message, respond to it or escalate. Dropping blockers stalls the entire coordination.
6. **Use `thread_id` in multi-topic rooms.** If a room covers multiple sub-topics, tag messages with a thread ID to keep conversations separable.
7. **Do not use neural_link as a logging system.** Rooms are for agent-to-agent communication. Use brain records for persistence.
8. **Do not send messages to yourself.** Use the appropriate persistence tool instead.
9. **Do not poll `inbox_read` in a loop.** Use `wait_for` to block until a message arrives. Polling wastes resources.
10. **The lead persists the summary.** After `room_close`, the lead uses the structured extraction (decisions, open questions, blockers, artifact record ID) to compose and present a summary to the user.
<!-- neural_link:end -->
