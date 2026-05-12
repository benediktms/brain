---
description: List records with optional filters; supports cross-brain queries
allowed-tools: "mcp__brain__*"
---

List brain records with optional filters. Defaults to active records in the current brain, capped at 50 results.

If arguments are provided:
- --kind: Filter by record kind (e.g. `artifact`, `snapshot`, `document`, `analysis`, `plan`)
- --status: `active` (default) or `archived`
- --tag: Filter by tag
- --task-id: Filter by associated task ID
- --limit: Max results (default 50)
- --brains: Comma-separated brain names or IDs for federated queries (omit for current brain only)

Use the `records_list` tool with whichever filters were provided. Present:
- A short summary line: total count, scope (local or federated), filters applied
- A compact table per record: `record_id`, `kind`, `title`, `status`, `media_type`, `created_at`
- Any `warnings` from the tool

If the result is empty under default filters, suggest the user pass `--status archived` or broaden via `--brains`. For semantic search, point at `/brain:records-search` instead.
