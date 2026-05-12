---
description: Cancel a saga
allowed-tools: "mcp__brain__*"
---

Cancel a saga that will not be completed.

Use `mcp__brain__sagas_cancel` with:
- `saga_id` (required): bare 26-char ULID — no prefix
- `cascade` (optional): if `true`, cancels all non-terminal member tasks; defaults to `false`
- `actor` (optional): who is cancelling the saga, defaults to `mcp`

Allowed from `planning` or `open` status. Closed sagas must be reopened first via `/brain:sagas-reopen` before cancelling.

Use `cascade: true` only if all non-terminal member tasks should also transition to `cancelled`. Without it, member tasks are left untouched. To undo a cancellation use `/brain:sagas-reopen`.
