---
description: Record a reusable procedure (how-to) to memory
allowed-tools: "mcp__brain__*"
---

Record a reusable procedure for future retrieval.

## Write the procedure

Call `memory_write_procedure`:
- `title` (required): Short name (e.g. "Deploy to production", "Set up dev environment").
- `steps` (required): A **single markdown string** — not an array. Use ordered/unordered lists, code blocks, headings, whatever reads best.
- `tags`: Topic tags for retrieval, as a JSON array.
- `importance`: 0.0–1.0 (default 0.9).
- `links` (optional, **preferred**): Inline links to attach the new procedure to existing entities in one round-trip. JSON array of `{to: {type, id}, edge_kind}` objects.

### Inline link examples

- **To the episode that taught it** (lesson-learned arc) —
  `{to: {type: "EPISODE", id: <ep_id>}, edge_kind: "relates_to"}`
- **To a record it documents** (analysis, plan) —
  `{to: {type: "RECORD", id: <record_id>}, edge_kind: "covers"}` or `"relates_to"`
- **To a task type it applies to** —
  `{to: {type: "TASK", id: <task_id>}, edge_kind: "relates_to"}`
- **To a prior procedure it replaces** —
  `{to: {type: "PROCEDURE", id: <prior_proc_id>}, edge_kind: "supersedes"}` (DAG-checked)

Edge kinds: `parent_of`, `blocks`, `covers`, `relates_to`, `see_also`, `supersedes`, `contradicts`.

Inline links are **partial-failure tolerant** — the procedure persists even if some links fail. Successes and failures are reported per-link in the response.

## If you need more links after the write

The response returns `summary_id`. If you discover additional linking targets only after seeing the response, call `links_add`:

```
links_add {
  from: { type: PROCEDURE, id: <summary_id> },
  to:   { type: EPISODE,   id: <ep_id> },
  edge_kind: "relates_to"
}
```

If arguments are missing for the initial write, ask for: title, then steps (as one markdown blob), then tags, then importance.

Procedures are indexed for semantic search and findable via `/mem:search`. Use procedures for repeatable workflows — use `/mem:write` for one-time events.
