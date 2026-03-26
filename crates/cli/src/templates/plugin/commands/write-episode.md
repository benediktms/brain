---
description: Record a structured episode to memory
argument-hint: [--goal] [--actions] [--outcome]
---

Record a structured episode in brain's memory.

Use the `memory_write_episode` tool with:
- `goal` (required): What was the objective
- `actions` (required): What was done
- `outcome` (required): What happened as a result
- `tags`: Array of topic tags for later retrieval
- `importance`: Score from 0.0 to 1.0

If arguments are missing, ask the user for each field interactively.

Episodes are indexed for semantic search and can be found via `/search`.
