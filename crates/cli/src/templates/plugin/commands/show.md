---
description: Show detailed information about a task
argument-hint: <task-id>
---

Display detailed information about a brain task.

If a task ID is provided as $1, use it. Otherwise, ask the user for the task ID.

Use the `tasks_get` tool with `expand` set to `["parent", "children", "blocked_by", "blocks"]` to retrieve full details. Present clearly:
- Task ID, title, and description
- Status, priority, and type
- Assignee and timestamps
- Dependencies (what this task blocks or is blocked by)
- Children tasks (if epic)
- Labels and comments
