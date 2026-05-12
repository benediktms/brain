---
description: Remove member tasks from a saga
allowed-tools: "mcp__brain__*"
---

Remove one or more tasks from a saga.

Use `mcp__brain__sagas_remove_tasks` with:
- `saga_id` (required): bare 26-char ULID — no prefix
- `task_ids` (required): array of task IDs to remove; task IDs not currently in the saga are silently ignored (idempotent). Empty array is a valid no-op. Max 500 per call
- `actor` (optional): who is performing the removal, defaults to `mcp`

Returns the count of tasks actually removed. Allowed in any saga status including closed and cancelled.

Use this skill to trim scope from a saga. Removing a task from a saga does not change the task's status — it only detaches the membership. To add tasks use `/brain:sagas-add-tasks`.
