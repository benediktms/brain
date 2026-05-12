---
description: Saga progress and member task rollup
allowed-tools: "mcp__brain__*"
---

Aggregate statistics for a saga's member tasks.

Use `mcp__brain__sagas_stats` with:
- `saga_id` (required): bare 26-char ULID — no prefix

Returns:
- counts by status (open, in_progress, blocked, done, cancelled)
- completion percentage: done / (total − cancelled), `null` if denominator is zero
- label histogram across member tasks
- contributing brains

Use this skill when you need to answer "how close to done is this saga?" or want a quick health check across all member tasks. For the actionable subset use `/brain:sagas-frontier`; for the full membership list use `/brain:sagas-get`.
