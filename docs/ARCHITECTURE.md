# Architecture Overview

A local-first "Personal Second Brain" that indexes Markdown notes into a dual-store system (SQLite + LanceDB) and exposes token-budgeted retrieval tools to AI agents over MCP stdio JSON-RPC.

## Concepts

A **brain** is a named knowledge container with its own notes, tasks, indexes, and configuration. Multiple brains can coexist (e.g., `personal`, `work-project`, `research`), managed by a central **registry** at `~/.brain/`.

**Core invariant**: Each domain has exactly one source of truth, and sync is always unidirectional:

- **Notes**: Markdown files are the source of truth. SQLite metadata, LanceDB embeddings, and FTS indexes are derived projections, rebuildable from source.
- **Tasks**: The append-only event log (`.brain/tasks/events.jsonl`, git-tracked) is the source of truth. SQLite task tables are derived projections, rebuildable by replaying the log.

Notes and tasks are parallel subsystems that can cross-reference each other (tasks link to note chunks, notes can mention task IDs) but have decoupled lifecycles and mutation patterns.

## Directory Structure

```
~/.brain/                                  # Central registry
  config.toml                              # Global config + registered brains
  brains/
    <brain-name>/                          # Per-brain derived data
      config.toml                          # Per-brain config (overrides global)
      brain.db                             # SQLite projections (notes + tasks)
      lancedb/                             # Vector indexes
~/code/my-project/                         # A project with brain notes
  .brain/
    brain.toml                             # Brain marker: name + note paths
    tasks/
      events.jsonl                         # Task event log (source of truth, git-tracked)
  docs/
    architecture.md                        # Indexed as notes
  notes/
    decisions.md                           # Indexed as notes
```

**Brain marker** (`.brain/brain.toml` in a project):
```toml
name = "my-project"
notes = ["docs", "notes"]                  # Relative paths to index
```

**Central registry** (`~/.brain/config.toml`):
```toml
[brains.my-project]
root = "~/code/my-project"
notes = ["~/code/my-project/docs", "~/code/my-project/notes"]

[brains.personal]
root = "~/notes"
notes = ["~/notes"]
```

**Key design decisions:**
- `brain init` in a project creates `.brain/brain.toml` and registers the brain centrally
- All derived data (SQLite, LanceDB) lives in `~/.brain/brains/<name>/`, not in the project
- The task event log lives in-repo at `.brain/tasks/events.jsonl` (git-tracked source of truth)
- A brain can index multiple note directories (e.g., `docs/` and `notes/` from one project)
- Moving a project just means updating the path in the registry
- No symlinks — just paths in config files

## System Architecture

```mermaid
graph TB
    subgraph Notes["Markdown Notes (Source of Truth)"]
        MD[("*.md files<br/>from registered note directories")]
    end

    subgraph Daemon["brain daemon (long-lived Rust process)"]
        subgraph Ingest["Ingest Pipeline"]
            FW[File Watcher<br/>notify-debouncer-full]
            HG[Hash Gate<br/>BLAKE3]
            MP[Markdown Parser<br/>pulldown-cmark]
            CH[Chunker<br/>heading-aware]
            EM[Embedder<br/>Candle BGE-small]
        end

        subgraph Query["Query Engine"]
            HR[Hybrid Ranker<br/>vector + FTS + graph expansion<br/>+ recency + links]
            IP[Intent Profile<br/>weight selection]
            TB[Token Budget<br/>Manager]
            TE[Token Estimator]
        end

        subgraph Server["MCP Server (stdio JSON-RPC)"]
            SM["memory.search_minimal"]
            EX["memory.expand"]
            WE["memory.write_episode"]
            RF["memory.reflect"]
            TA["tasks.apply_event"]
            TN["tasks.next"]
        end
    end

    subgraph Storage["Dual-Store Architecture"]
        subgraph SQLite["SQLite (Control Plane)"]
            FT[files table<br/>identity + hash gate]
            CT[chunks table<br/>metadata + FTS5]
            LT[links table<br/>wiki-links + backlinks]
            TT[tasks table<br/>projected from event log]
            TD[task_deps table<br/>dependency graph]
            TL[task_note_links table<br/>cross-references]
            ST[summaries table<br/>episodes + reflections]
        end

        subgraph LanceDB["LanceDB (Data Plane)"]
            VT[chunks table<br/>content + 384-dim embeddings]
        end

        subgraph TaskLog["Task Event Log (Source of Truth)"]
            EL[".brain/tasks/events.jsonl<br/>append-only UUID v7-ordered"]
        end
    end

    subgraph Agent["AI Agent (Orchestrator LLM)"]
        LLM["Claude / GPT / etc."]
    end

    MD -->|fs events| FW
    FW -->|debounced paths| HG
    HG -->|changed files| MP
    MP --> CH
    CH --> EM
    CH -->|metadata| SQLite
    EM -->|vectors| LanceDB

    LLM <-->|stdio JSON-RPC| Server
    Server --> Query
    Server -->|task events| EL
    EL -->|replay/project| SQLite
    Query --> SQLite
    Query --> LanceDB
```

## Storage Role Separation

| Concern | SQLite (Control Plane) | LanceDB (Data Plane) | Task Event Log |
|---------|----------------------|---------------------|----------------|
| **Role** | Transactional bookkeeping | Vector similarity search | Task source of truth |
| **Stores** | File identity, content hashes, chunk metadata, links, task projections, FTS5 index, summaries, schema versions | Chunk text, 384-dim embeddings, tags, timestamps, scores | Append-only task events (ULID-ordered JSONL) |
| **Access pattern** | Joins, filters, exact lookups, FTS5 BM25 | kNN vector search, batch upserts | Sequential append, full replay for rebuild |
| **Concurrency** | WAL mode (concurrent readers, single writer) | Arc\<Table\> shared across threads | Single writer (append-only) |
| **Consistency anchor** | content_hash gates note re-indexing | Derived from SQLite state | Log is authoritative; SQLite task tables are derived projections |

## Sequence Diagrams

### 1. Daemon Startup

```mermaid
sequenceDiagram
    participant Main as main()
    participant Config as Config
    participant Model as Candle Embedder
    participant DB as SQLite
    participant Vec as LanceDB
    participant Scanner as Vault Scanner
    participant Watcher as File Watcher
    participant MCP as MCP Server

    Main->>Config: Load brain.toml / env vars / defaults
    Main->>DB: Open SQLite (WAL mode, foreign keys)
    Main->>DB: Run schema migrations (check schema_version)
    Main->>Vec: Connect LanceDB (open/create chunks table)
    Main->>Vec: Check lancedb_schema_version vs expected
    alt Schema mismatch
        Vec-->>Main: Trigger full rebuild
        Main->>DB: Clear all content_hash values
    end
    Main->>Model: Load BGE-small weights (mmap safetensors)
    Main->>Model: Load tokenizer.json
    Main->>Model: Validate hidden_size == 384
    Note over Model: Model kept hot in RAM for session lifetime

    Main->>Scanner: Full vault scan (catch offline changes)
    loop Each *.md file
        Scanner->>DB: Check content_hash
        alt Hash changed or new file
            Scanner->>Scanner: Queue for indexing
        end
        alt File in DB but not on disk
            Scanner->>DB: Soft-delete (set deleted_at)
            Scanner->>Vec: Delete chunks for file_id
        end
    end
    Scanner->>Main: Scan complete (X changed, Y deleted)

    par Start concurrent services
        Main->>Watcher: Start watching vault (recursive, 250ms debounce)
        Main->>MCP: Start stdio JSON-RPC listener
    end

    Note over Main: tokio::select! multiplexes:<br/>- MCP stdin messages<br/>- Watcher file events<br/>- Shutdown signals (SIGTERM/SIGINT)
```

### 2. Indexing Pipeline (File Change to Indexed)

```mermaid
sequenceDiagram
    participant Editor as Editor / Git
    participant FW as File Watcher
    participant HG as Hash Gate
    participant Parser as Markdown Parser
    participant Chunker as Chunker
    participant Emb as Candle Embedder
    participant DB as SQLite
    participant Vec as LanceDB

    Editor->>FW: File saved (possibly multiple events)
    FW->>FW: Debounce (250ms window, coalesce by path)
    FW->>HG: FileChanged(path)

    HG->>HG: Read file, normalize whitespace
    HG->>HG: Compute BLAKE3 hash
    HG->>DB: SELECT content_hash FROM files WHERE path = ?
    alt Hash unchanged
        HG-->>HG: Skip (log at debug level)
    else Hash changed or new file
        HG->>DB: SET indexing_state = 'indexing_started'

        HG->>Parser: Parse markdown content
        Parser->>Parser: Extract YAML frontmatter
        Parser->>Parser: Build heading hierarchy
        Parser->>Parser: Identify block types (paragraph, code, list)

        Parser->>Chunker: Structured AST
        Chunker->>Chunker: Split on heading boundaries
        Chunker->>Chunker: Enforce max ~400 tokens per chunk
        Chunker->>Chunker: Track byte_start/byte_end offsets
        Chunker->>Chunker: Compute chunk_hash per chunk

        par SQLite metadata update
            Chunker->>DB: BEGIN TRANSACTION
            Chunker->>DB: Upsert files row (file_id, path, hash)
            Chunker->>DB: Delete old chunks for file_id
            Chunker->>DB: Insert new chunk metadata
            Chunker->>DB: Delete old links, insert new links
            Chunker->>DB: Delete old tasks, insert new tasks
            Chunker->>DB: COMMIT
            Note over DB: FTS5 triggers auto-update fts_chunks
        and Embedding
            Chunker->>Emb: embed_batch(chunk_contents)
            Emb->>Emb: Tokenize (padding, attention masks)
            Emb->>Emb: Forward pass: BertModel
            Emb->>Emb: CLS pooling (first token hidden state)
            Emb->>Emb: L2 normalize
            Emb-->>Chunker: Vec<Vec<f32>> (batch_size x 384)
        end

        Chunker->>Vec: merge_insert(chunks + embeddings)
        Note over Vec: on chunk_id:<br/>matched → update_all<br/>not matched → insert_all<br/>not matched by source<br/>  WHERE file_id = ? → delete

        alt merge_insert fails (CommitConflict)
            Vec-->>Chunker: Retry (3x, exponential backoff)
        end

        Chunker->>DB: SET indexing_state = 'indexed'
        Chunker->>DB: UPDATE content_hash, last_indexed_at
    end

    Note over Vec: Periodic: optimize()<br/>(compaction + pruning + index update)<br/>Triggered by N upserts or T elapsed
```

### 3. Agent Retrieval Flow (search_minimal + expand)

```mermaid
sequenceDiagram
    participant LLM as Agent (LLM)
    participant MCP as MCP Server
    participant HR as Hybrid Ranker
    participant DB as SQLite
    participant Vec as LanceDB
    participant TE as Token Estimator

    LLM->>MCP: tools/call: memory.search_minimal
    Note over LLM,MCP: { query, intent: "lookup", filters,<br/>budget_tokens: 600, k: 12 }

    MCP->>HR: search_minimal(query, intent, filters, budget)
    HR->>HR: Resolve intent to weight profile

    par Vector retrieval
        HR->>Vec: vector_search(embed(query), limit=50, metric=dot)
        Vec-->>HR: top-50 by vector similarity (chunk_id, sim_v)
    and Keyword retrieval
        HR->>DB: FTS5 query (BM25 ranking, limit=50)
        DB-->>HR: top-50 by BM25 (chunk_id, bm25_score)
    end

    HR->>HR: Union candidates, deduplicate by chunk_id

    opt Graph expansion (1-hop, Phase 3+)
        HR->>HR: Take top-10 seeds from initial fusion
        HR->>DB: SELECT linked chunk_ids FROM links<br/>WHERE src_chunk_id IN (seeds)<br/>OR dst_chunk_id IN (seeds)
        DB-->>HR: Neighbor chunk_ids (capped at 100)
        HR->>HR: Add neighbors to candidate pool, deduplicate
    end

    HR->>DB: Batch enrich: SELECT backlink_count, tags,<br/>updated_at, importance<br/>WHERE chunk_id IN (...)
    DB-->>HR: Metadata for all candidates

    loop Each candidate
        HR->>HR: Compute hybrid score (weights from intent profile)
        Note over HR: S = w_v*sim_v + w_k*bm25<br/>  + w_r*exp(-dt/tau)<br/>  + w_l*log(1+backlinks)<br/>  + w_t*tag_match<br/>  + w_i*importance
    end

    HR->>HR: Sort by hybrid score, take top-k
    HR->>TE: Estimate tokens for each stub

    loop Pack stubs within budget
        HR->>HR: Add stub if within budget_tokens
        Note over HR: Stub = { memory_id, kind, title,<br/>summary_2sent, scores, provenance,<br/>expand_hint }
    end

    HR-->>MCP: { budget_tokens, used_tokens_est, results[] }
    MCP-->>LLM: JSON-RPC response

    Note over LLM: Agent decides which stubs to expand

    LLM->>MCP: tools/call: memory.expand
    Note over LLM,MCP: { memory_ids: [...], budget_tokens: 2000 }

    MCP->>Vec: Fetch full chunk content by IDs
    Vec-->>MCP: Full text + byte offsets

    MCP->>TE: Estimate tokens per chunk
    loop Pack chunks within budget
        MCP->>MCP: Add chunk if within budget
        alt Last chunk exceeds budget
            MCP->>MCP: Truncate with [truncated] marker
        end
    end

    MCP-->>LLM: { budget_tokens, used_tokens_est,<br/>results[{ content, provenance: { file_path,<br/>byte_start, byte_end } }] }
```

### 4. Agent Memory Loop (Write + Reflect)

```mermaid
sequenceDiagram
    participant LLM as Agent (LLM)
    participant MCP as MCP Server
    participant DB as SQLite
    participant Vec as LanceDB
    participant Emb as Candle Embedder

    Note over LLM: Agent completes a task and wants to record what happened

    LLM->>MCP: tools/call: memory.write_episode
    Note over LLM,MCP: { goal: "Investigate indexing latency",<br/>actions: ["profiled embed_batch", "found bottleneck"],<br/>outcome: "Batch size 32 is optimal for CPU",<br/>tags: ["performance", "embedding"],<br/>importance: 0.8 }

    MCP->>MCP: Generate episode_id (UUID v7)
    MCP->>MCP: Auto-extract additional tags from content
    MCP->>DB: INSERT INTO summaries (kind='episode', ...)
    MCP->>Emb: embed(goal + " " + outcome)
    Emb-->>MCP: 384-dim vector
    MCP->>Vec: Insert episode embedding row
    MCP-->>LLM: { episode_id, created_at, tags, importance }

    Note over LLM: Later, after accumulating several episodes...<br/>Agent decides to consolidate

    LLM->>MCP: tools/call: memory.reflect
    Note over LLM,MCP: { memory_ids: [ep_1, ep_2, ep_3],<br/>reflection_prompt: "Summarize perf findings",<br/>budget_tokens: 500 }

    MCP->>DB: Fetch episodes by IDs from summaries table
    MCP->>Vec: Fetch episode content
    MCP-->>LLM: Source material formatted for synthesis
    Note over MCP,LLM: { sources: [{ id, goal, outcome, ... }],<br/>prompt: "Synthesize into a summary" }

    Note over LLM: LLM generates the reflection summary<br/>(brain does NOT run a generative model)

    LLM->>MCP: tools/call: memory.reflect (store result)
    Note over LLM,MCP: { summary: "Embedding batch size 32 is optimal...",<br/>source_ids: [ep_1, ep_2, ep_3] }

    MCP->>MCP: Generate reflection_id (UUID v7)
    MCP->>DB: INSERT INTO summaries (kind='reflection', ...)
    MCP->>DB: INSERT INTO reflection_sources (reflection_id, source_id) x3
    MCP->>Emb: embed(summary)
    Emb-->>MCP: 384-dim vector
    MCP->>Vec: Insert reflection embedding row
    MCP-->>LLM: { reflection_id, created_at }

    Note over LLM: Future searches return reflections<br/>as low-token, high-signal results
```

### 5. Dual-Store Consistency (Indexing State Machine)

Partial failures across SQLite and LanceDB are the hardest correctness problem. The indexing state machine ensures recovery.

```mermaid
stateDiagram-v2
    [*] --> idle: File exists, hash stored

    idle --> indexing_started: Hash changed / new file
    note right of indexing_started
        SQLite: state = 'indexing_started'
        Hash NOT updated yet
    end note

    indexing_started --> sqlite_written: SQLite transaction committed
    note right of sqlite_written
        chunks, links, tasks updated
        FTS5 triggers fired
    end note

    sqlite_written --> indexed: LanceDB merge_insert succeeded
    note right of indexed
        SQLite: state = 'indexed'
        content_hash updated
        last_indexed_at updated
    end note

    sqlite_written --> indexing_started: LanceDB write failed
    note left of indexing_started
        Hash NOT updated
        On restart: detected as
        'indexing_started' → re-index
    end note

    indexing_started --> indexing_started: SQLite write failed
    note left of indexing_started
        Transaction rolled back
        Nothing changed
        Hash gate will retry
    end note

    indexed --> idle: Ready for next change
    indexed --> indexing_started: File changed again
```

### 6. Graceful Shutdown

```mermaid
sequenceDiagram
    participant Signal as OS Signal
    participant Main as Main Loop
    participant Watcher as File Watcher
    participant Queue as Index Queue
    participant DB as SQLite
    participant Vec as LanceDB

    Signal->>Main: SIGTERM or SIGINT (first)
    Main->>Main: Set shutdown flag

    Main->>Watcher: Stop accepting new events
    Main->>Queue: Drain remaining items (10s timeout)

    alt Queue drained in time
        Queue-->>Main: All items processed
    else Timeout exceeded
        Queue-->>Main: N items dropped (log warning)
    end

    Main->>DB: PRAGMA wal_checkpoint(TRUNCATE)
    Note over DB: Flush WAL to main database

    Main->>Vec: optimize() (if pending unoptimized rows)
    Main->>Vec: Close table handles
    Main->>DB: Close connections
    Main-->>Signal: Exit code 0 (clean)

    Note over Signal: Second SIGINT = force shutdown<br/>(skip drain, close immediately, exit 1)
```

### 7. Embedding Pipeline Detail

```mermaid
sequenceDiagram
    participant Text as Chunk Text
    participant Tok as Tokenizer
    participant Bert as BertModel
    participant Pool as CLS Pooling
    participant Norm as L2 Normalize
    participant Out as 384-dim Vector

    Text->>Tok: encode_batch(chunks, padding=true)
    Tok-->>Bert: token_ids [B, T], attention_mask [B, T]

    Note over Bert: token_type_ids = zeros [B, T]

    Bert->>Bert: forward(token_ids, token_type_ids, attention_mask)
    Bert-->>Pool: hidden_states [B, T, 384]

    Pool->>Pool: Select [:, 0, :] (CLS token)
    Pool-->>Norm: cls_embeddings [B, 384]

    Norm->>Norm: v / ||v||_2 per row
    Note over Norm: sqr → sum_keepdim(1) → sqrt<br/>→ clamp(1e-12) → broadcast_div

    Norm-->>Out: normalized [B, 384]
    Note over Out: L2 norm = 1.0 (within 1e-5)<br/>Pairs with LanceDB dot product
```

**Numerical stability**: The L2 normalization must clamp the magnitude to `max(||v||_2, 1e-12)` before dividing. Degenerate all-padding inputs can produce zero-magnitude vectors, and dividing by zero silently produces NaN that poisons the LanceDB index with no runtime error. Add a debug assertion that all output vectors satisfy `|1.0 - ||v||_2| < 1e-5`.

## Memory Architecture

```
                    Token Cost
                    High ──────────────────── Low
                    │                          │
    Tier 1          │  Raw Chunks              │
    (Episodic)      │  Full markdown text       │
                    │  384-dim embeddings       │
                    │                          │
    Tier 2          │         Structured Meta   │
    (Semantic)      │         Tags, backlinks   │
                    │         Tasks, timestamps  │
                    │                          │
    Tier 3          │              Summaries    │
    (Procedural)    │              Reflections  │
                    │              2-sent stubs │
                    │                          │
                    High ──────────────────── Low
                    Recall
```

The retrieval policy is **progressive and budget-first**:

1. **search_minimal** returns compact stubs (Tier 2/3 cost) — covers both notes and task capsules
2. **expand** fetches raw chunks on demand (Tier 1 cost)
3. **write_episode** stores structured events (creates Tier 1)
4. **reflect** consolidates into summaries (creates Tier 3 from Tier 1)
5. **tasks.apply_event** creates/mutates tasks via event log (task subsystem)
6. **tasks.next** returns highest-priority ready task (deterministic selection)

## Hybrid Scoring

All retrieval combines six signals into a single relevance score:

```
S = w_v * sim_v + w_k * bm25 + w_r * f(dt) + w_l * g(links) + w_t * tag_match + w_i * importance
```

| Signal | Source | Computation |
|--------|--------|-------------|
| `sim_v` | LanceDB | Dot product similarity (normalized vectors) |
| `bm25` | SQLite FTS5 | BM25 rank, normalized to [0,1] |
| `f(dt)` | SQLite | `exp(-dt/tau)`, tau=30 days default |
| `g(links)` | SQLite | `log(1 + backlinks) / log(1 + max_backlinks)` |
| `tag_match` | SQLite | Jaccard coefficient (query tags vs chunk tags) |
| `importance` | SQLite/LanceDB | Pre-computed at write time |

### Intent-Driven Weight Profiles

The `intent` parameter on `memory.search_minimal` selects a weight profile that adjusts signal priorities:

| Intent | Description | Upweighted signals | Downweighted signals |
|--------|-------------|-------------------|---------------------|
| `lookup` | Fact finding, direct answers | `bm25`, `tag_match` | `importance` |
| `planning` | What to do next | `f(dt)`, `g(links)`, `importance` | `bm25` |
| `reflection` | What happened, how we decided | `f(dt)`, `importance` | `tag_match` |
| `synthesis` | Write or design something | `sim_v`, `g(links)` | `f(dt)` |
| `auto` | Default, no adjustment | Equal weights (1/6 each) | — |

Weight profiles are stored as a lookup table and are configurable via `brain.toml`.

**Invariant**: All weight profiles MUST sum to 1.0. Validate at load time; normalize by dividing each weight by the sum if needed. Hand-tuned profiles can silently drift, biasing retrieval without runtime errors.

**Edge cases in signal computation**:
- `bm25` normalization: divide by `max(max_bm25_in_result_set, 1e-12)` — zero FTS matches means all BM25 scores should be 0.0, not NaN.
- `tag_match` (Jaccard coefficient): `J(∅, ∅) = 0.0` by convention, not division-by-zero.
- `g(links)`: `log(1 + 0) / log(1 + max_L)` is well-defined (= 0.0), but guard `max_L = 0` with a denominator of 1.0.

### Candidate Sources

Retrieval draws candidates from three sources, fused before scoring:

| Source | Store | Phase | Description |
|--------|-------|-------|-------------|
| Vector search | LanceDB | Phase 2 | Top-50 by dot product similarity |
| Keyword search | SQLite FTS5 | Phase 2 | Top-50 by BM25 |
| Graph expansion | SQLite links | Phase 3+ | 1-hop neighbors of top-10 seeds (capped at 100) |

Graph expansion captures transitively relevant content in interlinked vaults: a query matching note A will also surface note B if A links to B, even when B has low direct similarity. Candidates from all sources are unioned, deduplicated, then scored through the hybrid formula.

## Key Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Rust | Performance, safety, single-binary deployment |
| Async runtime | tokio | Required by LanceDB async API |
| Embedding model | BGE-small-en-v1.5 (384-dim) | Small, fast, well-benchmarked for CPU |
| ML framework | Candle | Rust-native, safetensors mmap, no Python dependency |
| Vector store | LanceDB | Arrow-native, merge_insert upsert, disk-based |
| Metadata store | SQLite (WAL mode) | Transactional, FTS5, concurrent reads |
| Content hashing | BLAKE3 | 3-4x faster than SHA-256 |
| File watching | notify-debouncer-full | Editor-agnostic event coalescing |
| File identity | UUID v7 | Time-ordered, survives renames |
| Agent protocol | MCP stdio JSON-RPC | Standard for AI tool integration |
| Distance metric | Dot product | Optimal for L2-normalized embeddings |
| Pooling | CLS (first token) | BGE-recommended, fastest |
| Reranker (post-v1) | ONNX Runtime via `ort` crate | Cross-encoder reranking on top-N fused candidates |

## Performance Design

The system is designed around one core insight: **indexing is the expensive part; querying is cheap once warm**. All performance decisions flow from keeping the daemon responsive during normal use while deferring heavy computation to idle or scheduled windows.

### Design Decisions

#### 1. Capsule Generation Strategy

Every chunk gets a **deterministic capsule** at ingest time — zero ML cost:

| Capsule field | Source | Cost |
|---------------|--------|------|
| `title` | Heading hierarchy from Markdown AST | Negligible |
| `summary_2sent` | First meaningful sentence + heading outline | Negligible |
| `tags` | Frontmatter + auto-extracted | Negligible |

ML-quality summarization runs only during **consolidation** (idle/scheduled), never on the hot ingest path. This prevents the biggest laptop killer: eager summarization burning CPU on every file save.

#### 2. Reranker Policy

Cross-encoder reranking is **opt-in**, not default:

- Triggered per-query when the caller requests it, OR when fusion confidence is below threshold
- Applied to the **top 10–30 fused candidates** only, never the full candidate pool
- Operates on **capsules/snippets**, not full chunks, to minimize compute
- Latency budget: 200–500ms on CPU for top-20 candidates
- The reranker model is **loaded lazily** (not at startup) and unloaded after idle timeout

#### 3. Work Queue and Backpressure

The indexing pipeline is protected against watcher storms (git pulls, branch switches, mass edits):

```
File events → Debounce (250ms) → Bounded work queue → Indexer
                                      ↓
                              Overflow policy:
                              • Last-write-wins per file_id
                              • Drop oldest if queue full
                              • Batch SQLite writes (single writer lane)
                              • Batch LanceDB upserts
```

Key invariant: the queue is bounded. If it fills up, dropped files are caught on the next periodic scan.

#### 4. LanceDB Compaction Strategy

Without compaction, queries fall back to brute-force scan on unindexed fragments. The `optimize()` schedule uses a **dual trigger**:

| Trigger | Threshold | Rationale |
|---------|-----------|-----------|
| Upsert count | ~100–500 upserts since last optimize | Keeps unindexed fragment count bounded |
| Elapsed time | 5–10 minutes since last optimize | Catches quiet periods after bursts |

Whichever fires first triggers compaction. Runs on a background task to avoid blocking indexer or query paths.

#### 5. Model Loading Strategy

| Model | Loading | Memory | Rationale |
|-------|---------|--------|-----------|
| BGE-small embedder | **Always hot** | ~130MB | Needed for every ingest and query |
| Cross-encoder reranker | **Lazy** (on first use) | Variable | Rarely needed; idle-unloaded |
| Summarizer | **Lazy** (consolidation only) | Variable | Only runs during idle/scheduled jobs |

Target daemon baseline RSS: **300–400MB** (embedder + SQLite + LanceDB structures).

### Performance Expectations

Assumes a "medium" vault: 2k–10k Markdown files, 20k–200k chunks, 384-dim embeddings.

| Operation | Expected Latency | Notes |
|-----------|-----------------|-------|
| SQLite FTS5 query | 5–30ms | Scales with vault size |
| LanceDB vector search | 10–50ms | After compaction; warm indexes |
| Hybrid fusion + scoring | 1–10ms | Lightweight arithmetic |
| `search_minimal` end-to-end | 20–80ms | FTS + vector + fusion + stub packing |
| `expand` (fetch chunks) | 5–20ms | Direct ID lookup |
| Optional rerank (top 20) | 200–500ms | Cross-encoder on CPU |
| Incremental index (1 file) | Sub-second | Hash gate + embed 1–10 chunks |
| Initial full index (100k chunks) | ~10–17 min | CPU, batch size 32, no acceleration |

### Storage Footprint

| Component | Size (100k chunks) | Notes |
|-----------|-------------------|-------|
| Embeddings (raw float32) | ~147MB | 100k × 384 × 4 bytes |
| LanceDB with indexes | ~200–400MB | Overhead from indexes + metadata |
| SQLite (metadata + FTS) | ~50–150MB | Depends on content length |
| BGE-small model weights | ~130MB | Loaded in RAM via mmap |
| **Total disk** | **~400MB–800MB** | Comfortable for laptop |
| **Daemon RSS (baseline)** | **~300–400MB** | Embedder + stores, no optional models |

## Mathematical Foundations

The system relies on concepts from several mathematical and computer science domains. This section summarizes the key foundations; see RESEARCH.md § Mathematical Foundations for detailed formulas, numerical verification, and implementation guidance.

### Linear Algebra & Embeddings

Every chunk is a point in R^384. The embedding pipeline transforms text through a BERT forward pass (`[B, T] → [B, T, 384]`), CLS pooling (`[:, 0, :]`), and L2 normalization onto the unit hypersphere S^383. Dot product on unit vectors equals cosine similarity — this is why LanceDB uses `dot` metric after normalization, saving ~2x compute vs explicit cosine.

### Information Retrieval & Scoring

The hybrid scoring formula is a weighted linear combination of 6 orthogonal signals. BM25 (from SQLite FTS5) provides term-frequency scoring with document-length normalization. Min-max normalization maps BM25 to [0,1]. The Jaccard coefficient measures tag overlap as `|A∩B| / |A∪B|`. All signals are combined with intent-driven weight profiles that must sum to 1.0.

### Exponential Decay & Recency

The recency signal uses exponential decay: `f(dt) = exp(-dt/τ)` with τ=30 days (configurable). The half-life is `τ × ln(2) ≈ 20.8 days`. The backlink signal uses logarithmic scaling: `g(L) = log(1+L) / log(1+max_L)`, which compresses the dynamic range of link counts.

### Graph Theory

The links table is a directed graph (adjacency list). Graph expansion performs 1-hop BFS from seed nodes. The task dependency graph is a DAG — cycle detection (DFS-based, O(V+E)) is required when adding edges. In-degree counting provides the backlink score; future work may upgrade to iterative PageRank for importance propagation.

### Probability & Hashing

BLAKE3 (256-bit) provides collision probability `< 10^(-70)` for 10k files. UUID v7 provides time-ordered identity with a birthday-problem safe threshold of ~2^64 IDs. ULID provides monotonic event ordering with same-millisecond disambiguation via incrementing counter. IVF-PQ indexing (Product Quantization in Voronoi cells) enables sub-5ms ANN search at 100x vector compression with >95% recall.

### Concurrency & Async

The daemon uses tokio for async concurrency: file watcher events, MCP request handling, and indexing run as concurrent tasks multiplexed via `tokio::select!`. SQLite WAL mode enables concurrent readers with a single writer. The bounded work queue with file_id coalescing provides backpressure against watcher storms.
