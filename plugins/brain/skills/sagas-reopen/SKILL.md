---
description: Reopen a closed or cancelled saga
allowed-tools: "mcp__brain__*"
---

Reopen a closed or cancelled saga, restoring it to `open` status.

Use `mcp__brain__sagas_reopen` with:
- `saga_id` (required): bare 26-char ULID — no prefix
- `actor` (optional): who is reopening the saga, defaults to `mcp`

Only `closed` or `cancelled` sagas can be reopened. Calling on a `planning` or already `open` saga returns an error. Member tasks are unaffected by the reopen — their statuses are not changed.

After reopening, confirm the saga is now `open` and use `/brain:sagas-frontier` to find the next actionable tasks.
