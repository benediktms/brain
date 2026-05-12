---
description: Add member tasks to a saga
allowed-tools: "mcp__brain__*"
---

Add one or more tasks to a saga.

Use `mcp__brain__sagas_add_tasks` with:
- `saga_id` (required): bare 26-char ULID — no prefix
- `task_ids` (required): array of task IDs to add; full IDs or short hashes are accepted, cross-brain aware. Min 1, max 500 per call
- `actor` (optional): who is adding the tasks, defaults to `mcp`

The operation is atomic and idempotent — tasks already in the saga and intra-batch duplicates are silently skipped. If any task ID cannot be resolved the entire batch fails; verify IDs before calling.

The saga must not be closed or cancelled. Use on a `planning` saga to shape membership before starting, or on an `open` saga to add work mid-flight. To remove tasks use `/brain:sagas-remove-tasks`.
