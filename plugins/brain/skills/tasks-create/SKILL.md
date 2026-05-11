---
description: Create a new task interactively
allowed-tools: "mcp__brain__*"
---

Create a new brain task. If arguments are provided:
- $1: Task title
- --type: Task type (task, bug, feature, epic, spike)
- --priority: Priority (0=critical, 1=high, 2=medium, 3=low, 4=backlog)

If arguments are missing, ask the user for:
1. Task title (required)
2. Task type (default: task)
3. Priority (default: 4)
4. Description (optional)
5. Parent task (optional)

Use the `tasks_create` tool. Show the created task ID and details.

Optionally ask if this task should be linked to another task via dependencies.
