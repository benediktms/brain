# brain

A local-first personal second brain — indexes Markdown notes into a dual-store system and exposes token-budgeted retrieval tools to AI agents over MCP.

## Overview

brain is a Rust daemon that turns a directory of Markdown files into a queryable knowledge base accessible to AI agents. It watches your notes for changes, incrementally indexes them into a hybrid store (SQLite for full-text search and metadata, LanceDB for vector embeddings), and exposes structured retrieval tools over MCP stdio JSON-RPC.

Retrieval is designed around token budgets rather than raw result counts. The `memory.search_minimal` tool returns lightweight stubs that fit within a narrow budget; the `memory.expand` tool fetches full chunk content only for the items that matter. This two-phase pattern lets agents orient themselves cheaply before committing tokens to detailed reading.

Hybrid scoring combines six signals — vector similarity, BM25, recency decay, link count, tag match, and importance — with intent-driven weight profiles (lookup, planning, reflection, synthesis). All computation runs locally: no network calls, no external services, no ongoing cost.

## Features

- Local-first: zero network calls, zero ongoing cost after initial model download
- Dual-store indexing: SQLite (FTS5, metadata, wiki-links, tasks) + LanceDB (384-dim BGE embeddings)
- Hybrid retrieval combining vector similarity, BM25, recency decay, link count, tag match, and importance
- Intent-driven weight profiles: lookup, planning, reflection, synthesis, auto
- Progressive token-budgeted retrieval: search_minimal returns stubs, expand fetches full content
- Memory tiers: Episodic (raw chunks), Semantic (metadata), Procedural (summaries and reflections)
- MCP stdio JSON-RPC with 6 tools for search, writing, reflection, and task management
- Incremental indexing via file watcher with BLAKE3 hash gating (only changed files are reprocessed)
- BGE-small-en-v1.5 embeddings via Candle (Rust-native, no Python dependency)
- Multiple named brains can coexist (personal, work-project, research)

## Quick Start

### Prerequisites

- Rust toolchain (stable, edition 2024)
- [just](https://github.com/casey/just) task runner

### Setup

Clone the repository and download the embedding model weights:

```sh
git clone <repo-url>
cd brain-02
just setup-model
```

This downloads BGE-small-en-v1.5 weights into the expected local path.

### Initialize a brain

Create a brain marker in a directory of Markdown notes:

```toml
# ~/notes/.brain/brain.toml
name = "personal"
notes = ["docs", "notes"]
```

Register and index:

```sh
brain index
```

### Connect to an AI agent

Run the MCP daemon and point your agent at it:

```sh
brain daemon
```

The daemon listens on stdio and speaks MCP JSON-RPC. Configure your agent to spawn `brain daemon` as a stdio tool server.

## Usage

### Indexing

Index all notes in the configured paths:

```sh
brain index
```

Watch for changes and index incrementally (250ms debounce):

```sh
brain watch
```

### Querying

Search from the command line:

```sh
brain query "weekly review template"
```

### Daemon (MCP server)

Start the MCP stdio server for agent integration:

```sh
brain daemon
```

The daemon exposes the MCP tools described below. Agents send JSON-RPC requests over stdin and receive responses over stdout.

### Multiple brains

Brains are named containers stored in the central registry at `~/.brain/`. Each brain has its own notes, indexes, and configuration. Switch between brains by running commands from different directories or by specifying the brain name explicitly.

```
~/.brain/
  config.toml                   # Global config + registered brains
  brains/<brain-name>/
    config.toml                 # Per-brain config
    brain.db                    # SQLite projections
    lancedb/                    # Vector indexes
    tasks/events.jsonl          # Task event log

~/notes/.brain/
  brain.toml                    # name + note paths
```

## Architecture

brain uses a unidirectional sync pipeline:

```
file watcher -> hash gate (BLAKE3) -> parser -> chunker -> embedder -> dual store
```

The dual store separates concerns: SQLite holds structured projections (FTS5 search, wiki-links, task state, metadata), while LanceDB holds the vector index for semantic similarity. Retrieval merges results from both using a weighted scoring formula. Weight profiles shift emphasis depending on the declared intent of the query.

Memory is organized into three tiers:

- **Episodic**: raw note chunks with timestamps, used for direct recall
- **Semantic**: metadata and link structure, used for navigation and context
- **Procedural**: summaries and reflections synthesized from episodic content

Performance targets for a medium vault (2,000-10,000 files):

| Operation | Target |
|---|---|
| `search_minimal` end-to-end | 20-80ms |
| Incremental index (1 file) | sub-second |
| Daemon RSS baseline | ~300-400MB |

For full architecture details, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). For background on retrieval design decisions, see [docs/RESEARCH.md](docs/RESEARCH.md).

## MCP Tools

The daemon exposes 6 tools over MCP stdio JSON-RPC:

| Tool | Description |
|---|---|
| `memory.search_minimal` | Search notes and return lightweight stubs within a token budget. Use this first to orient. |
| `memory.expand` | Fetch full chunk content for specific chunk IDs returned by search_minimal. |
| `memory.write_episode` | Append a new episodic memory (note or observation) to the knowledge base. |
| `memory.reflect` | Synthesize a procedural summary from a set of episodic chunks. |
| `tasks.apply_event` | Append an event to the append-only task log (create, update, complete, etc.). |
| `tasks.next` | Query the task list and return the highest-priority actionable items. |

Task events are stored as ULID-ordered JSONL, making the log append-only and replay-safe.

## Development

Build and check:

```sh
just build
just check
```

Run tests:

```sh
just test
```

Lint and format:

```sh
just lint
just fmt
just fmt-check
```

Clean build artifacts and database:

```sh
just clean
just clean-db
```

### Workspace layout

The project is a Cargo workspace with two crates:

- `brain_lib` — core library: indexing, retrieval, embedding, MCP protocol, task subsystem
- `cli` — thin binary crate that wires commands to library functions

### Key dependencies

- `tokio` — async runtime
- `candle` (0.9) — Rust-native tensor operations for BGE embeddings
- `lancedb` (0.26) — vector store
- `rusqlite` — SQLite with FTS5
- `blake3` — content hashing for the hash gate
- `pulldown-cmark` — Markdown parsing
- `clap` — CLI argument parsing
- `notify-debouncer-full` — file system watching

## Release

Create a new release using the tag command:

```sh
just tag patch    # 0.1.0 -> 0.1.1
just tag minor    # 0.1.0 -> 0.2.0
just tag major    # 0.1.0 -> 1.0.0
```

Generate or update the changelog:

```sh
just changelog
just changelog-update
```

## License

MIT
