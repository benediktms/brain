---
description: Transfer a task from the current brain to a target brain
allowed-tools: "mcp__brain__*"
---

Move a brain task to a different brain, preserving the underlying task ID but recomputing its display ID in the target brain. Useful when a task was filed in the wrong brain or when scope shifts to another domain.

If arguments are provided:
- $1: Task ID (full or short prefix)
- --to: Target brain (name, brain_id, or alias)

If either is missing, ask the user. Use `/brain:list` to surface available brains when the user is unsure.

Call the `tasks_transfer` tool. Present:
- `task_id` (unchanged)
- `from_display_id` → `to_display_id`
- `from_brain_id` → `to_brain_id`
- Note if `was_no_op: true` (source == target) so the user knows nothing changed.
