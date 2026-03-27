---
description: Record a reusable procedure (how-to) to memory
allowed-tools: "mcp__brain__*"
---

Record a reusable procedure in brain's memory for future retrieval.

Use the `memory_write_procedure` tool with:
- `title` (required): Procedure name (e.g., "Deploy to production", "Set up dev environment")
- `steps` (required): Array of step descriptions in order
- `tags`: Topic tags for later retrieval
- `importance`: Score from 0.0 to 1.0

If arguments are missing, ask the user for:
1. Procedure title
2. Steps (collect one at a time until the user signals completion)

Procedures are indexed for semantic search and can be found via `/mem:search`. Use procedures for repeatable workflows — use `/mem:write` for one-time events.
