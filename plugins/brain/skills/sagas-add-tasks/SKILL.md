---
description: Add member tasks to a saga
allowed-tools: "mcp__brain__*"
---

Add one or more tasks to a saga.

Use `mcp__brain__sagas_add_tasks` with:
- `saga_id` (required): `saga-<hex>` short form (3+ lowercase hex chars) or bare 26-char ULID
- `task_ids` (required): array of task IDs to add; full IDs or short hashes are accepted, cross-brain aware. Min 1, max 500 per call
- `cascade` (optional, default `false`): when `true`, also add every transitive descendant of each input task (via the `parent_of` graph). Lets you pull an entire epic and all its subtasks into the saga in one call
- `actor` (optional): who is adding the tasks, defaults to `mcp`

The operation is atomic and idempotent — tasks already in the saga and intra-batch duplicates are silently skipped. If any task ID cannot be resolved the entire batch fails; verify IDs before calling. When `cascade` is true the same idempotency applies to the expanded set — descendants already in the saga drop out silently.

The saga must not be closed or cancelled. Use on a `planning` saga to shape membership before starting, or on an `open` saga to add work mid-flight. To remove tasks use `/brain:sagas-remove-tasks`.

The response carries `added` (count) and `added_task_ids` (the exact compact task IDs that were inserted). Use `added_task_ids` to see what `cascade=true` pulled in — without it the count alone hides which descendants were attached.

**Subtree cap**: cascade expansion is bounded by `MAX_EXPANDED_BATCH = 2000` tasks. A call that would expand beyond that limit errors before any writes with a clear message naming the cap. The MCP `task_ids` array is separately capped at 500 input entries.
