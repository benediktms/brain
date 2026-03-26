---
description: Semantic search across notes and tasks
argument-hint: <query> [--intent] [--tags] [--brains]
---

Search brain's semantic memory.

Use the `memory_search_minimal` tool. Parameters:
- `query` (required): Search query text
- `intent`: Ranking profile -- `lookup` (keyword-heavy), `planning` (recency + links), `reflection` (recency-heavy), `synthesis` (vector-heavy)
- `tags`: Array of tags to boost matching results
- `brains`: Array of brain names to search across (use `["all"]` for all brains)

Results include stubs with title, summary, score, and kind (note/task/task-outcome).

To see full content, use `/expand` with the returned memory IDs.
