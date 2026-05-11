---
description: Retrieve semantic memory at requested detail level
allowed-tools: "mcp__brain__*"
---

Retrieve memory chunks via semantic search at a requested level of detail (LOD).

Use the `memory_retrieve` tool. Key parameters:
- `query` (required): Natural language search query
- `lod`: Level of detail for returned content
  - `L0`: Extractive abstract (~100 tokens) — fastest, suitable for scanning
  - `L1`: LLM-summarized content (~2000 tokens) — balanced
  - `L2`: Full source passthrough — complete content
- `count`: Maximum number of results (default: 10)
- `strategy`: Retrieval strategy controlling ranking
  - `lookup`: Keyword-heavy, for exact terms and names
  - `planning`: Recency + links, for "what's related to X?"
  - `reflection`: Recency-heavy, for recent learnings
  - `synthesis`: Vector-heavy, for semantic similarity
  - `auto`: Equal weights (default)
- `brains`: Array of brain names to search (use `["all"]` for all registered brains)
- `kinds`: Filter by result kind -- `["note", "episode", "reflection", "procedure", "task", "task-outcome", "record"]`
- `time_after` / `time_before`: Unix timestamps for time-scoped results
- `tags`: Tags to boost via Jaccard similarity
- `tags_require`: Require ALL tags (AND logic)
- `tags_exclude`: Exclude results matching ANY tags (NOR logic)
- `explain`: When true, include per-signal score breakdowns

Results include URI, kind, LOD-adjusted content, and hybrid search score.

**One-shot retrieval**: `memory_retrieve` returns LOD-adjusted content in a single call.
