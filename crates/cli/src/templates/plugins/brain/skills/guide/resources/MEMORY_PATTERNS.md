# Memory Patterns

## Search Intent Profiles

Use the `intent` parameter on `memory_search_minimal` to control ranking:

| Intent | Weighting | Best For |
|--------|-----------|----------|
| `auto` | Equal weights | General queries |
| `lookup` | 40% BM25 (keyword) | Exact terms, names, IDs |
| `planning` | Recency + links | Project planning, "what's related to X?" |
| `reflection` | Recency-heavy | Journal entries, recent learnings |
| `synthesis` | 40% vector | Semantic similarity, "things like X" |

## Search → Expand Workflow

1. `memory_search_minimal` returns compact stubs (title, summary, score, kind)
2. Identify relevant results by score and kind
3. `memory_expand` with selected memory IDs to get full content
4. Use `budget` parameter to control token usage

## Result Kinds

- **note**: Indexed markdown documents from the notes directory
- **episode**: Recorded episodes (goal, actions, outcome)
- **reflection**: Synthesized reflections from episodes
- **procedure**: Stored procedures and workflows
- **task**: Active task capsules (auto-generated from task events)
- **task-outcome**: Completed task outcomes
- **record**: Artifacts, snapshots, and other stored records

## Metadata Filters

Filter search results using metadata facets:

- `kinds`: Array of kind strings to include (e.g. `["episode", "reflection"]`). Empty = all kinds.
- `time_after`: Unix timestamp — only results modified/created after this time
- `time_before`: Unix timestamp — only results modified/created before this time
- `tags_require`: Array of tags — ALL must be present (AND logic, case-insensitive)
- `tags_exclude`: Array of tags — results matching ANY are excluded (NOR logic, case-insensitive)

Filters are applied post-retrieval before ranking. Note: recency is based on actual file modification time on disk (not indexing time).

## Cross-Brain Search

Pass `brains` parameter to search across multiple brain projects:
- `["work", "personal"]` — specific brains
- `["all"]` — all registered brains

Results include `brain_name` field indicating the source.

## Episode Recording

Use `memory_write_episode` to capture knowledge that would be lost between conversations:

- **External API behavior** (rate limits, quirks, undocumented features)
- **Architecture of other codebases** this project interacts with
- **Business rules** not captured in code
- **Deployment topology** and environment-specific behavior
- **Historical context** about why things were built a certain way
- **Lessons learned** from incidents

Structure: `goal` (what prompted it), `actions` (key facts), `outcome` (how to use this knowledge).

Always check `memory_search_minimal` first to avoid recording duplicates.

## Tag Boosting

Pass `tags` array to `memory_search_minimal` to boost results matching specific tags via Jaccard similarity. Example: `tags: ["rust", "memory"]` boosts results tagged with those terms.
