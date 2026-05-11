---
description: Update task fields (status, priority, assignee, etc.)
allowed-tools: "mcp__brain__*"
---

Update a brain task's fields.

Use `tasks_apply_event` with the appropriate event type:
- `status_changed`: Change status (open, in_progress, blocked, done, cancelled)
- `task_updated`: Change title, description, priority, task_type, assignee, due_ts, defer_until
- `comment_added`: Add a comment

If no specific fields are provided, ask the user what they'd like to update.
