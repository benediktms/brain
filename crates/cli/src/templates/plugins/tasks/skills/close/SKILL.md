---
description: Close one or more tasks
allowed-tools: "mcp__brain__*"
---

Close brain tasks by marking them as done.

Use the `tasks_close` tool with the provided task IDs. Accepts one or multiple IDs.

Show which tasks were closed and any tasks that became unblocked as a result.

## Optional — link a wrap-up episode

If you wrote an episode capturing what was learned during this task (`mem:write`),
attach it to the entity graph before closing so it surfaces alongside the task
in future retrievals:

```
links_add {
  from: { type: EPISODE, id: <summary_id> },
  to:   { type: TASK,    id: <task_id> },
  edge_kind: "relates_to"
}
```

This costs one extra MCP call but turns the episode from semantic-search-only
into entity-graph-discoverable from the task it served.
