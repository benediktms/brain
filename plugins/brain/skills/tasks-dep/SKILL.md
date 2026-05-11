---
description: Manage task dependencies
allowed-tools: "mcp__brain__*"
---

Manage dependencies between brain tasks.

Use the `tasks_deps_batch` tool with one of these actions:
- `add`: Add dependency pairs (task depends on another)
- `remove`: Remove dependency pairs
- `chain`: Create sequential dependencies (A -> B -> C)
- `fan`: Multiple tasks depend on one source task
- `clear`: Remove all dependencies from a task

If no arguments provided, ask the user what they want to do.

Show the result and any tasks that became unblocked or blocked.
