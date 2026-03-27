---
description: Find ready-to-work tasks with no blockers
allowed-tools: "mcp__brain__*"
---

Use the brain MCP server to find tasks that are ready to work on.

Call the `tasks_next` tool to get unblocked tasks sorted by priority. Present them showing:
- Task ID
- Title
- Priority
- Task type

If there are ready tasks, ask the user which one they'd like to work on. If they choose one, use `tasks_apply_event` with event_type `status_changed` to set status to `in_progress`.

If there are no ready tasks, suggest checking blocked tasks with `/tasks:blocked` or creating a new one with `/tasks:create`.
