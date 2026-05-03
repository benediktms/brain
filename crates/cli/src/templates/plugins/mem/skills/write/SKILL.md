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
- `links` (optional, **preferred**): Inline links to attach the new episode to existing entities in one round-trip. JSON array of `{to: {type, id}, edge_kind}` objects.

### Inline link examples

- **To the task it served** —
  `{to: {type: "TASK", id: <task_id>}, edge_kind: "relates_to"}`
- **To a procedure that distilled it** —
  `{to: {type: "PROCEDURE", id: <proc_id>}, edge_kind: "relates_to"}`
- **To a prior episode it corrects** —
  `{to: {type: "EPISODE", id: <ep_id>}, edge_kind: "supersedes"}` (DAG-checked)
- **To a record (analysis, plan, snapshot) it informs** —
  `{to: {type: "RECORD", id: <record_id>}, edge_kind: "covers"}` or `"relates_to"`

Edge kinds: `parent_of`, `blocks`, `covers`, `relates_to`, `see_also`, `supersedes`, `contradicts`.

Inline links are **partial-failure tolerant** — the episode persists even if some links fail. Successes and failures are reported per-link in the response.

## If you need more links after the write

The response returns `summary_id`. If you discover additional linking targets only after seeing the response, call `links_add`:

```
links_add {
  from: { type: EPISODE, id: <summary_id> },
  to:   { type: TASK,    id: <task_id> },
  edge_kind: "relates_to"
}
```

If arguments are missing for the initial write, ask for each field interactively.

Episodes are indexed for semantic search and findable via `/mem:search`.
