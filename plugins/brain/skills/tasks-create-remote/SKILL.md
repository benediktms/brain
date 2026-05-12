---
description: Create a task, optionally in a different brain, with auto-link to a local task
allowed-tools: "mcp__brain__*"
---

Create a brain task using the cross-brain-aware path of `tasks_create`. When `--brain` is set, the task lands in the target brain; when omitted, falls back to the current brain (same as `/brain:tasks-create`). Use this skill when you want the option of routing the task to another brain or auto-linking it to a local task.

If arguments are provided:
- $1: Task title
- --brain: Target brain (name, brain_id, or alias). Optional — defaults to the current brain.
- --type: Task type (task, bug, feature, epic, spike; default: task)
- --priority: Priority (0=critical, 1=high, 2=medium, 3=low, 4=backlog; default: 4)
- --link-from: Local task ID to auto-link to the new task (works for both local and cross-brain creation)
- --link-type: Link type when --link-from is set (depends_on, blocks, related; default: related)

If required arguments are missing, ask the user for:
1. Task title (required)
2. Target brain (optional) — offer `/brain:list` if they want to pick a different brain
3. Task type (default: task)
4. Priority (default: 4)
5. Description (optional)
6. Whether to link a local task via --link-from / --link-type (optional)

Use the `tasks_create` tool, passing `brain` only when the user supplied one. Show:
- For local creation: `task_id`, status, plus any `unblocked_task_ids`
- For cross-brain creation: `remote_task_id`, `remote_brain_name`, and `local_ref_created` (true if --link-from was honoured)
