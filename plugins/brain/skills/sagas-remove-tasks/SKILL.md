---
description: Remove member tasks from a saga
allowed-tools: "mcp__brain__*"
---

Remove one or more tasks from a saga.

Use `mcp__brain__sagas_remove_tasks` with:
- `saga_id` (required): `saga-<hex>` short form (3+ lowercase hex chars) or bare 26-char ULID
- `task_ids` (required): array of task IDs to remove; task IDs not currently in the saga are silently ignored (idempotent). Empty array is a valid no-op. Max 500 per call
- `cascade` (optional, default `false`): when `true`, also remove every transitive descendant of each input task (via the `parent_of` graph) that is currently a member of the saga. Lets you strip an entire epic subtree out of the saga in one call
- `actor` (optional): who is performing the removal, defaults to `mcp`

Returns the count of tasks actually removed. Allowed in any saga status including closed and cancelled. With `cascade` the same idempotency applies — descendants not currently members of the saga are skipped silently; only the intersection of the subtree with current membership is removed.

Use this skill to trim scope from a saga. Removing a task from a saga does not change the task's status — it only detaches the membership. To add tasks use `/brain:sagas-add-tasks`.
