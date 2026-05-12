---
description: Walk an episode thread forward and backward via continues edges
allowed-tools: "mcp__brain__*"
---

Walk a full episode thread by following `continues` edges from any member episode.

Use the `memory_walk_thread` tool. Key parameters:
- `seed_summary_id` (required): The `summary_id` of any episode in the thread — the walk recovers all predecessors and successors regardless of which member is passed.
- `max_depth`: BFS depth bound (default: 32). The visited set is capped at 1024 episodes; when the cap is hit, the response sets `truncated: true`.

The response shape is `{ seed_summary_id, count, truncated, thread: [{ summary_id, uri, kind, title, content, tags, importance, created_at }] }`, ordered by `created_at` ASC.

**walk vs. mem-search**: Use `mem-thread` for **ordered episode histories** — "show me the full chain of episodes connected by `continues` edges", "what led up to this outcome". The walk follows explicit `continues` edges in the graph, so it returns the ordered narrative of a multi-part episode sequence. Use `/brain:mem-search` instead when you want **semantic neighbourhood** — things thematically similar to a query, ranked by embedding similarity rather than structural linkage.

**Edge kind in scope**: `memory_walk_thread` exclusively traverses `continues` edges. For edges like `parent_of`, `supersedes`, `relates_to`, `covers`, `see_also`, `contradicts`, or `blocks`, call the `links_for_entity` tool directly to enumerate them.

**Brain scope**: Threads are scoped to the current brain — episodes from other brains are filtered out defensively even if `continues` edges cross brain boundaries (which should not normally occur via the standard write path).
