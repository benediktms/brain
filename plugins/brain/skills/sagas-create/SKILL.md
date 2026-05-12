---
description: Create a new saga
allowed-tools: "mcp__brain__*"
---

Create a new saga in `planning` status.

Use `mcp__brain__sagas_create` with:
- `title` (required): saga title
- `description` (optional): longer description of the saga's scope or goal
- `actor` (optional): who is creating it, defaults to `mcp`

Sagas are registry-level — they are not scoped to any brain. The returned `saga_id` is a bare 26-char ULID (e.g. `01KR16ZJRDVNF5D463QMVD9PH0`), not prefixed.

New sagas start in `planning` status. Use this status to shape the work and add member tasks before committing. Call `/brain:sagas-start` when the saga is ready to move into active execution.

Show the returned saga ID and status after creation.
