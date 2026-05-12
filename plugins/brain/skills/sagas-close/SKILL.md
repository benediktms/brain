---
description: Close a completed saga
allowed-tools: "mcp__brain__*"
---

Close a saga when its work is done.

Use `mcp__brain__sagas_close` with:
- `saga_id` (required): `saga-<hex>` short form (3+ lowercase hex chars) or bare 26-char ULID
- `cascade` (optional): if `true`, transitions all member tasks to `done` (best-effort — already-done and already-cancelled tasks are skipped); defaults to `false`
- `actor` (optional): who is closing the saga, defaults to `mcp`

Only `open` sagas can be closed. To recover a closed saga use `/brain:sagas-reopen`.

Use `cascade: true` only if all member tasks should also transition to `done`. Without it, member tasks are left untouched. Recommended: check `/brain:sagas-stats` first to confirm the completion percentage meets expectations before closing.
