---
description: Show ready-to-work tasks across all brains in a saga
allowed-tools: "mcp__brain__*"
---

Return the actionable tasks in a saga — the orchestration sweet spot.

Use `mcp__brain__sagas_frontier` with:
- `saga_id` (required): `saga-<hex>` short form (3+ lowercase hex chars) or bare 26-char ULID

Applies the same readiness filter as `tasks_next`: tasks must be `open` or `in_progress`, have no unresolved dependencies, no `blocked_reason`, not be deferred, and not be epics. The result spans all brains in the saga, making this the primary tool for cross-brain orchestration.

Planning, closed, and cancelled sagas return an empty task list (the response still includes the contributing brains).

Present each ready task showing: task ID, brain, title, priority, and type. If the list is empty on an open saga, suggest checking `/brain:sagas-stats` to understand whether tasks are blocked or already done. For a full membership view (including non-ready tasks) use `/brain:sagas-get`.
