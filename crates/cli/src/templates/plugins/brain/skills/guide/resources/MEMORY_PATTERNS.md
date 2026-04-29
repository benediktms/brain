# Memory Patterns

## Canonical Retrieval Pattern

**One-shot retrieval**: Use `memory_retrieve` to search and expand in a single call.

```
Call memory_retrieve with:
- query: Natural language search text
- lod: Desired level of detail (L0, L1, or L2)
- count: Number of results
- [optional] strategy, kinds, time filters, tags, brains
```

No follow-up expand call needed. Results include full LOD-adjusted content.

## Level of Detail (LOD)

Choose based on your use case:

| LOD | Size | Speed | Use For |
|-----|------|-------|---------|
| `L0` | ~100 tokens | Fast | Scanning/filtering (many results) |
| `L1` | ~2000 tokens | Balanced | Most common — balanced summary + detail |
| `L2` | Full source | Slow | Complete content, no truncation |

LOD is "best-effort": if L1 is not yet generated, `memory_retrieve` falls back to L0 or L2 automatically.

## Strategy Parameter

Control ranking weight profile:

| Strategy | Weighting | Best For |
|----------|-----------|----------|
| `auto` | Equal weights | General queries (default) |
| `lookup` | Keyword-heavy | Exact terms, names, task IDs |
| `planning` | Recency + links | "What's related to X?" — project-scoped questions |
| `reflection` | Recency-heavy | Recent entries, journal-style queries |
| `synthesis` | Vector-heavy | Semantic similarity, "things like X" |

## Result Kinds

Results are classified by kind, filterable via `kinds` parameter:

- **note**: Indexed markdown documents from the notes directory
- **episode**: Recorded episodes (goal, actions, outcome)
- **reflection**: Synthesized reflections from episodes
- **procedure**: Stored procedures and workflows
- **task**: Active task capsules (auto-generated from task events)
- **task-outcome**: Completed task outcomes
- **record**: Artifacts, snapshots, and other stored records

## Metadata Filtering

Filter search results post-retrieval using:

- `kinds`: Array of kind strings to include. Empty = all kinds.
- `time_after`: Unix timestamp — only results created/modified after this time
- `time_before`: Unix timestamp — only results created/modified before this time
- `tags_require`: Array of tags — ALL must match (AND logic, case-insensitive)
- `tags_exclude`: Array of tags — results matching ANY are excluded (NOR logic, case-insensitive)

Note: recency is based on file modification time on disk, not indexing time.

## Cross-Brain Retrieval

Search across multiple brains by passing `brains` parameter:
- `["work", "personal"]` — specific brain names/IDs
- `["all"]` — all registered brains

Results include `brain_name` field indicating the source brain.

## Tag Boosting

Pass `tags` array to boost results matching those tags via Jaccard similarity:

```
tags: ["rust", "memory"] — boosts results tagged with one or both terms
```

Useful for domain-scoped searches (e.g., "architecture" + `tags: ["database"]`).

## Episode Recording

Use `memory_write_episode` to capture knowledge that would be lost between conversations:

- **External API behavior** (rate limits, quirks, undocumented features)
- **Architecture of other codebases** this project interacts with
- **Business rules** not captured in code
- **Deployment topology** and environment-specific behavior
- **Historical context** about why things were built a certain way
- **Lessons learned** from incidents

Structure: `goal` (what prompted it), `actions` (key facts), `outcome` (how to use this knowledge).

Always call `memory_retrieve` first to check for existing coverage and avoid duplicates.
