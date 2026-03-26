---
description: Manage task labels
argument-hint: <action> [label] [task-ids]
---

Manage labels on brain tasks.

Use `tasks_labels_summary` to list all labels with counts.
Use `tasks_labels_batch` for operations:
- `add`: Add a label to tasks
- `remove`: Remove a label from tasks
- `rename`: Rename a label across all tasks
- `purge`: Remove a label from all tasks

Label conventions: `area:memory`, `area:tasks`, `area:cli`, `type:feature`, `type:bugfix`, `phase:design`.
