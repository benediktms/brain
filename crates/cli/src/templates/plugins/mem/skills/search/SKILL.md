---
description: Semantic search across notes and tasks
allowed-tools: "mcp__brain__*"
---

Search brain's semantic memory.

Use the `memory_search_minimal` tool. Parameters:
- `query` (required): Search query text
- `intent`: Ranking profile -- `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy)
- `tags`: Array of tags to boost matching results
- `brains`: Array of brain names to search across (use `["all"]` for all brains)
- `kinds`: Filter by result kind -- `["note", "episode", "reflection", "procedure", "task", "task-outcome", "record"]`
- `time_after`: Only results modified/created after this Unix timestamp
- `time_before`: Only results modified/created before this Unix timestamp
- `tags_require`: Require ALL of these tags (AND logic, case-insensitive)
- `tags_exclude`: Exclude results matching ANY of these tags (NOR logic, case-insensitive)

Results include stubs with title, summary, score, and kind.

To see full content, use `/mem:expand` with the returned memory IDs.
