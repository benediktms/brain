---
description: List sagas with status filters
allowed-tools: "mcp__brain__*"
---

List sagas across the registry.

Use `mcp__brain__sagas_list` to retrieve sagas. By default only `planning` and `open` sagas are returned — use the widening flags to include terminal states:
- `include_closed`: also return closed sagas
- `include_cancelled`: also return cancelled sagas
- `all`: include both closed and cancelled regardless of the other flags
- `containing_brain`: filter to sagas that have at least one **live** member task in the given brain (pass the brain_id, not the brain name — use `mcp__brain__brains_list` first if you only have the name)

Present results showing: saga ID, title, status, and member task count if available.

Use this skill for an overview of active work or when searching for a saga by status. To inspect a specific saga's members, follow up with `/brain:sagas-get`.
