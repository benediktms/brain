# ADR 002: Episodic Memory Foundation

**Status:** Accepted (implemented)

**Date:** 2026-03-17

---

## Context

The episodic memory system was partially implemented. Episodes could be written via `memory.write_episode` but never surfaced through the main retrieval path (`memory.search_minimal`). Reflections were unreachable from MCP entirely — `store_reflection()` existed in `db/summaries.rs` but had no callable tool path.

Several structural gaps blocked the write-retrieve-reflect-retrieve loop:

1. `summaries` was the only domain table without `brain_id` — tasks, records, and record_events all had it. This broke multi-brain isolation.
2. `store_episode()` and `store_reflection()` had no `brain_id` parameter, writing unscoped rows.
3. `list_episodes()` had no `brain_id` filter — it returned episodes from all brains.
4. `derive_kind()` in `retrieval.rs` only recognized `task:`, `task-outcome:`, and `note` prefixes — `sum:` IDs were misclassified.
5. `valid_from` / `valid_to` columns were dormant since their introduction.
6. `reflection_sources` rows were written but never read at runtime.

These gaps blocked later roadmap phases: URI-addressed summaries, hierarchy summaries, consolidation, playbooks, and explainability all depend on summaries being first-class retrievable objects.

---

## Decision

Eight design decisions close the loop.

### 1. Migration v24 to v25

Schema migration adds `brain_id TEXT NOT NULL DEFAULT ''` to the `summaries` table and bumps `SCHEMA_VERSION` to 25. A backfill via `ensure_brain_registered()` (replacing the stale `backfill_brain_id()` approach) populates `brain_id` on existing rows.

Phase 4 foundations are also included in this migration as inert columns:

- `parent_id TEXT` — links reflections to their predecessor (versioned learnings)
- `source_hash TEXT` — hash of source content at reflection time (staleness detection)
- `confidence REAL` — decay-weighted relevance score
- `valid_from` — activated from `created_at` (was dormant since introduction)

These columns carry no runtime logic in Phase 3. They are placeholders for the consolidation pipeline.

### 2. Per-summary chunk ID with synthetic SQLite rows

LanceDB chunk ID format: `sum:{summary_id}`. Synthetic file_id `__summaries__` is used to prevent the file-scoped orphan cleanup in `upsert_chunks()` from deleting summary embeddings. This is the task capsule pattern applied to summaries.

Episode vs. reflection kind is determined from SQLite enrichment, not from the chunk ID prefix. A single `sum:` prefix is used uniformly; the kind field on the enriched `SummaryRow` propagates to `make_stub()`.

### 3. Mode parameter on existing reflect tool

`memory.reflect` gains a `mode` parameter: `prepare` (existing behavior) and `commit` (new). A separate tool was not added — tool count stays at 25 and the two-phase workflow matches the existing tool description.

`commit` mode requires `title`, `content`, and `source_ids`. It calls `store_reflection()` with `ctx.brain_id` and validates source IDs via `get_summary()` (PK lookup with no `brain_id` filter — cross-brain references are intentional).

### 4. FTS5 with porter stemming for summaries

A new `fts_summaries` virtual table uses `tokenize='porter unicode61'`. The existing `fts_chunks` table uses no stemming tokenizer. Episodes and reflections are prose; porter stemming improves recall. Insert, delete, and update triggers maintain the index automatically via `ensure_fts5()`.

### 5. Best-effort embedding on write

`mem_write_episode.rs` and `mem_reflect.rs` (commit mode) embed and upsert into LanceDB after the SQLite write. Embedding failure is logged but does not fail the episode or reflection write. Existing summaries are backfilled via `brain reindex --summaries` (CLI-only, no auto-startup cost).

### 6. Cross-brain reflection via globally unique ULID source references

Reflections are stored in the current brain. `source_ids` in a commit may reference episode `summary_id` values from any brain — `reflection_sources` stores `(reflection_id, source_id)` pairs without brain scoping. `get_summary()` is a PK lookup; no `brain_id` filter is applied during source validation. This enables agents working across multiple brains to synthesize cross-brain knowledge into a single reflection.

`reflect(mode="prepare")` accepts a `brains` parameter (same pattern as `search_minimal`) to gather episodes from specified brains. When omitted, the default is the current brain.

### 7. Episode linking via `related_ids` on write_episode

`memory.write_episode` accepts an optional `related_ids` list. These are written to `reflection_sources` at episode creation time, enabling explicit episode-to-episode linking without requiring a separate reflect call.

### 8. Phase 4 schema foundations

The v24→v25 migration adds four inert columns to `summaries`:

| Column | Purpose |
|---|---|
| `parent_id` | Links a reflection to its predecessor (versioned learning chains) |
| `source_hash` | Hash of source content at write time (staleness detection) |
| `confidence` | Decay-weighted relevance score |
| `valid_from` | Activation timestamp; populated from `created_at` |

Consolidation (daemon-detected, agent-executed sleep replay) will activate these columns in Phase 4.

---

## CLS Architecture

The implementation maps to Complementary Learning Systems (CLS) theory:

| CLS Layer | Brain Component | Properties |
|---|---|---|
| Hippocampus | Episodes | Fast-write, immutable, specific to a context |
| Neocortex | Reflections | Slow-integrate, versioned via `parent_id`, generalizing patterns |
| Sleep replay / consolidation | Phase 4 daemon | Agent-nudge trigger; `parent_id` version chains; local SQL+embeddings for detection, remote LLM for execution |

Three consolidation trigger modes are defined (Phase 4):
1. Agent-invoked — explicit `memory.consolidate()` call
2. Daemon-detected — local heuristics flag candidate clusters
3. Agent-nudge — daemon finds candidates, surfaces them to the agent for execution

---

## Consequences

### Positive

- **Loop closed** — episodes written via `write_episode` are retrievable via `search_minimal` with `kind="episode"`. Reflections committed via `reflect(mode="commit")` are retrievable with `kind="reflection"`.
- **Multi-brain isolation** — `brain_id` on summaries matches the pattern on every other domain table.
- **Cross-brain knowledge synthesis** — reflections can reference episodes from any brain; cross-brain `prepare` mode gathers material from multiple brains.
- **Phase 4 ready** — `parent_id`, `source_hash`, `confidence`, and `valid_from` are in schema. Consolidation pipeline can activate them without a schema migration.

### Neutral

- **Embedding backfill required** — episodes written before this migration are not vector-indexed. A one-time `brain reindex --summaries` is required for full recall on historical episodes.
- **PageRank not extended to summaries** — summary candidates default to `pagerank_score = 0.0`. Extending the PageRank graph to include reflections (via `reflection_sources` edges) is deferred.

### Negative

- **Phase 4 columns are inert** — `parent_id`, `source_hash`, `confidence` carry no runtime logic. They are schema debt until consolidation is implemented.

---

## Discrepancies Resolved During Implementation

| Stale Plan Item | Resolution |
|---|---|
| Migration target v21→v22 | Corrected to v24→v25 (actual current schema version) |
| `backfill_brain_id()` for summaries | Replaced by `ensure_brain_registered()` — more correct registration path |
| `validate_file_id` underscore issue | Avoided entirely via per-summary `sum:{summary_id}` chunk IDs |
| `EpisodeWriter` / `EpisodeReader` port traits needing `brain_id` + `related_ids` | Parameters added directly; port trait extraction deferred (same rationale as `TaskPersistence` in ADR-001) |
| Synthetic file_id special-casing | Task capsule pattern applied uniformly — no special-casing in expand |
| Source-group dedup | Post-ranking filter (not pre-filter) — preserves ranking signal integrity |

---

## References

- CLS theory: McClelland, McNaughton, O'Reilly (1995) — *Why there are complementary learning systems in the hippocampus and neocortex*
- Stanford Generative Agents: Park et al. (2023) — *Generative agents: Interactive simulacra of human behavior*
- Brain snapshot: BRN-01KKXMB1FJW77T0WRWPVA9DZA4
