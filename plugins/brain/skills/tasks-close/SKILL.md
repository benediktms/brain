---
description: Close one or more tasks
allowed-tools: "mcp__brain__*"
---

Close brain tasks by marking them as done.

Use the `tasks_close` tool with the provided task IDs. Accepts one or multiple IDs.

Show which tasks were closed and any tasks that became unblocked as a result.

### Optional — link a wrap-up episode

**Skip this section unless an episode was written for this task during this session** (via `mem:write` or `memory.write_episode`). Otherwise:

```
links_add {
  from: { type: EPISODE, id: <summary_id> },
  to:   { type: TASK,    id: <task_id> },
  edge_kind: "relates_to"
}
```

The link is recorded in the entity graph for future retrieval surfaces to use. Today only `links_for_entity` reads these edges; broader retrieval/ranking integration is a planned follow-up. Linking now costs one extra MCP call and pays off when those integrations land.
