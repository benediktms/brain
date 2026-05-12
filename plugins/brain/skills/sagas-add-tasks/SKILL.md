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

**Large subtrees**: the `task_ids` array is capped at 500 entries on input, but `cascade` expansion can produce many more memberships. There is no hard cap on the expanded set — assume the runtime cost scales with the total number of descendants. At current repo scales (epics with <100 descendants) this is well under a second; if you are about to cascade across a very large tree, prefer a planning pass first.
