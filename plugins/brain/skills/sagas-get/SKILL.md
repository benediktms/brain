---
description: Fetch a saga and its member tasks
allowed-tools: "mcp__brain__*"
---

Fetch a single saga and its member task stubs.

Use `mcp__brain__sagas_get` with:
- `saga_id` (required): bare 26-char ULID — no prefix (e.g. `01KR16ZJRDVNF5D463QMVD9PH0`)

Returns the saga row (title, description, status, timestamps), the `brains` array of brains contributing member tasks, and stubs for each member task. Member stubs are empty until tasks have been added via `/brain:sagas-add-tasks`.

Use this skill to inspect what tasks belong to a saga and what brain each one lives in. For aggregate counts by status use `/brain:sagas-stats`; for only the actionable subset use `/brain:sagas-frontier`.
