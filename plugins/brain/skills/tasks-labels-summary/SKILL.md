---
description: Show all labels with counts and associated task IDs
allowed-tools: "mcp__brain__*"
---

Surface the full label taxonomy for the current brain — useful for discovering existing tags before applying labels, or auditing label sprawl.

Call the `tasks_labels_summary` tool (no parameters). Present results sorted by count descending:
- Label name
- Count of tasks
- Short task ID prefixes (truncate if more than ~8 per label)

If the list is large, suggest the user follow up with `/brain:tasks-label` to apply or rename labels, or `/brain:tasks-list` filtered by a specific label.
