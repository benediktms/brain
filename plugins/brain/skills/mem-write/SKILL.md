---
description: Record a structured episode to memory
allowed-tools: "mcp__brain__*"
---

Record a structured episode in brain's memory.

## Write the episode

Call `memory_write_episode`:
- `goal` (required): What was the objective.
- `actions` (required): What was done. Free-form prose; rationale belongs here.
- `outcome` (required): What happened as a result.
- `tags`: Topic tags for retrieval, as a JSON array (e.g. `["debugging", "auth"]`).
- `importance`: 0.0–1.0 (default 1.0).
- `continues` (optional, **preferred for thread extension**): The `summary_id` of a prior episode this one extends. Internally lowered to a `links` entry with `edge_kind: continues` (DAG-validated). See "Continuing a thread" below.
- `links` (optional): Inline links to attach the new episode to existing entities in one round-trip. JSON array of `{to: {type, id}, edge_kind}` objects.

The response returns `{summary_id, uri, ...}`. When `continues` or `links` were provided the response also carries a `links` block with `{succeeded: [...], failed: [...], summary: {succeeded, failed}}` so per-link outcomes are observable.

## Continuing a thread

To extend a prior episode — the common case for multi-step workflows — pass `continues: <prior_summary_id>` at the top level:

```
memory_write_episode {
  goal:    "Apply the fix the audit recommended",
  actions: "...",
  outcome: "...",
  continues: "<summary_id of the audit episode>"
}
```

This lowers internally to a `links` entry of `{to: {type: EPISODE, id: <prev>}, edge_kind: continues}` and is DAG-validated (a `continues` cycle is rejected — episodes are time-ordered). The synthesized entry appears first in the response's `links` block, ahead of any explicit entries from `links`.

Walk the resulting thread later with `/brain:mem-thread`.

### Inline link examples

- **To the task it served** —
  `{to: {type: "TASK", id: <task_id>}, edge_kind: "relates_to"}`
- **To a procedure that distilled it** —
  `{to: {type: "PROCEDURE", id: <proc_id>}, edge_kind: "relates_to"}`
- **To a prior episode it corrects** —
  `{to: {type: "EPISODE", id: <ep_id>}, edge_kind: "supersedes"}` (DAG-checked)
- **To a record (analysis, plan, snapshot) it informs** —
  `{to: {type: "RECORD", id: <record_id>}, edge_kind: "covers"}` or `"relates_to"`

Edge kinds: `parent_of`, `blocks`, `covers`, `relates_to`, `see_also`, `supersedes`, `contradicts`, `continues`.

Inline links are **partial-failure tolerant** — the episode persists even if some links fail. Successes and failures are reported per-link in the response's `links` block.

## If you need more links after the write

If you discover additional linking targets only after seeing the response, call `links_add` with the returned `summary_id`:

```
links_add {
  from: { type: EPISODE, id: <summary_id> },
  to:   { type: TASK,    id: <task_id> },
  edge_kind: "relates_to"
}
```

If arguments are missing for the initial write, ask for each field interactively.

Episodes are indexed for semantic search and findable via `/brain:mem-search`.
