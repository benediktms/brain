---
description: List and filter tasks
argument-hint: [--status] [--priority] [--label] [--search]
---

List brain tasks with optional filters.

Use the `tasks_list` tool with these filters:
- `status`: open (default), ready, blocked, done, in_progress, cancelled
- `priority`: 0-4
- `task_type`: task, bug, feature, epic, spike
- `assignee`: filter by assignee
- `label`: filter by label (exact match)
- `search`: full-text search on title and description

Present results in a table showing: ID, Title, Status, Priority, Type, Labels.

If there are many results, note the total count and suggest narrowing filters.
