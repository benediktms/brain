---
name: guide
description: >
  Local-first personal knowledge base with semantic memory, task management,
  and records. Use when work needs persistent context, task tracking with
  dependencies, or semantic search across notes and episodes.
allowed-tools: "Read,mcp__brain__*"
version: "{{version}}"
author: "Benedikt Schnatterbeck"
license: "MIT"
---

# Brain — Persistent Knowledge Base for AI Agents

Local-first knowledge base combining semantic search, task management, and records. Provides persistent memory that survives conversation compaction.

## Brain vs TodoWrite

| Brain (persistent) | TodoWrite (ephemeral) |
|--------------------|-----------------------|
| Multi-session work | Single-session tasks |
| Complex dependencies | Linear execution |
| Survives compaction | Conversation-scoped |
| Semantic search | No search |
| Cross-brain queries | Local to session |

**Decision test**: "Will I need this context in 2 weeks?" → YES = Brain

## Prerequisites

- **brain CLI** installed and in PATH (`brain --version`)
- **MCP server** configured (`brain mcp setup claude`)
- **Brain project** initialized (`brain init` in your project)

## Session Protocol

1. `tasks_next` — Find unblocked work
2. `tasks_get` — Get full context
3. `tasks_apply_event` (status_changed → in_progress) — Claim task
4. Work on the task, add comments as needed
5. `tasks_close` — Complete task
6. `memory_write_episode` — Record learnings if any

## Tool Domains

| Domain | Key Tools | Use For |
|--------|-----------|---------|
| Tasks | `tasks_next`, `tasks_create`, `tasks_get`, `tasks_close` | Work tracking, dependencies, priorities |
| Memory | `memory_retrieve`, `memory_write_episode` | LOD-aware retrieval with search and expansion in one call, episode recording |
| Records | `records_create_document`, `records_create_analysis`, `records_create_plan`, `records_save_snapshot`, `records_list` | Typed work products, snapshots, and per-kind retrieval policy |
| Brain | `status`, `brains_list`, `jobs_status` | Health checks, brain registry, job queue |

## Resources

| Resource | Content |
|----------|---------|
| [TASK_WORKFLOW.md](resources/TASK_WORKFLOW.md) | Task lifecycle, dependencies, labels |
| [MEMORY_PATTERNS.md](resources/MEMORY_PATTERNS.md) | Search intents, episodes, cross-brain |
| [RECORDS_GUIDE.md](resources/RECORDS_GUIDE.md) | Typed records vs snapshots, linking, policy |
| [TROUBLESHOOTING.md](resources/TROUBLESHOOTING.md) | Common issues and fixes |
