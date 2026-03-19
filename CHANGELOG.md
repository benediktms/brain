# Changelog

All notable changes to this project will be documented in this file.

## [0.3.2] - 2026-03-19

### Bug Fixes

- Deduplicate brain sections in AGENTS.md and fix upsert logic

### Features

- Add archived-brain guards and improve CLI output
- Add --brain flag to brain snapshots save
- Add --brain flag to artifacts create
- Implement cross-brain task creation in CLI tasks create
- Add optional brain param to records.save_snapshot tool
- Add optional brain param to records.create_artifact

## [0.3.1] - 2026-03-19

### Bug Fixes

- Remove brain-specific crate architecture from AGENTS.md template
- Prefix generation — strip noise words and preserve reading order

### Documentation

- Remove phase status table from README

### Features

- CLI parity with MCP tools — 7 new subcommands (#87)
- Add VectorSearchMode knob for determinism control

### Refactoring

- Unify McpContext behind BrainStores + SearchService

## [0.3.0] - 2026-03-18

### Bug Fixes

- Resolve clippy warnings and mermaid parse error
- Byte-offset correctness tests + CRLF paragraph splitting (#84)
- Isolate fd-heavy perf tests behind #[ignore] + just test-perf (#82)
- Single daemon model — IPC fallback error discrimination + stale docstring (#79)
- Task creation architecture remediation — remaining subtasks (#77)
- Use canonical install path for MCP server binary (#75)
- Sanitize FTS5 search queries to prevent column-operator errors
- Route CLI task/record commands through unified DB
- Route prefix lookups to per-brain DB to prevent collision in unified DB
- Self-healing pipeline — truncation guard, KV cache fix, repair-on-corrupt
- MCP brain_id scoping — unified DB + per-session resolution
- Correct CLI docs --type→--task-type, remove AGENTS.md from git
- Close single-daemon gaps (BRN-01KKFR) (#69)
- Resolve clippy doc-overindented-list-items warnings in v18→v19 migration
- Wire v18→v19, preserve event columns, fix brain_id propagation
- Eliminate SQL injection via parameterized bindings in migrate_workspace
- Use __ipc_brain routing key to prevent brain param collision in daemon IPC (#66)
- Wire up Flan-T5 summarizer at daemon startup (#57)

### Documentation

- Sync ARCHITECTURE.md with JSONL removal and full MCP tool surface
- Sync AGENTS.md and init.rs template with current implementation
- Add "Recording Context as Memory" section to AGENTS.md template
- Update OPERATIONS.md schema version (13→24), remove stale cross-brain label from AGENTS.md
- Merge semantic search, episodic memory, and roadmap plans
- Correct factual errors in plan documents
- Add episodic memory foundation plan
- Add OpenViking-inspired development roadmap
- Add semantic search records plan
- Update AGENTS.md file
- Document records.task_id denormalization vs record_links
- Uddate AGENTS.md file
- Reflect B2 storage refactor — SQLite as runtime source of truth

### Features

- Prefix single source of truth + brain registry DB projection
- Episodic memory foundation — close write→retrieve→reflect→retrieve loop (#86)
- Blake3-versioned brain:start markers + pre-commit hook for AGENTS.md sync
- Extract TaskStore/RecordStore SQL to brain_persistence (#85)
- Task listing improvements — in-progress priority + exact status filters (#83)
- Cross-brain task creation via tasks.create MCP tool (#81)
- Stale root pruning + brain archival (#80)
- Add brain tasks ext-link CLI subcommand (#78)
- V23 migration — per-brain prefix column + ambiguous ID UX (#76)
- Add cargo fmt pre-commit hook
- Add FK constraint brain_id → brains(brain_id) (#73)
- JSONL-based migration & command consolidation
- Daemon-driven embedding with hybrid embedded_at self-heal (#70)
- Add v19→v20 self-healing migration for corrupted event schemas
- Add v18→v19 schema hardening migration
- Workspace unified single-DB storage (#68.2)
- B2 — SQLite as runtime source of truth (#68)
- Add brain link command (#67)
- Single daemon UDS IPC layer (#65)
- Brain aliases, multi-path roots, and init-as-reference (#64)
- Cross-brain expansion — bug fix + Tier 1 MCP/CLI support (#63)
- Cross-brain task fetch and close (#62)
- Add text param to snapshot and artifact MCP tools (#61)
- PageRank-inspired link scoring for hybrid ranking (#58)
- ML summarization pipeline with Flan-T5 integration (#56)
- Unified tasks.create MCP tool and auto-decode fetch_content (#55)

### Refactoring

- Purge all "vault" references — replace with "brain"
- Remove JSONL audit trail — SQLite is sole source of truth
- Brain init uses unified DB path ~/.brain/brain.db
- Config.rs accepts explicit brain_name — no longer derives from DB path
- Decouple brain_data_dir from sqlite_db path — use explicit per-brain dirs
- Resolve_paths sqlite_db → unified ~/.brain/brain.db
- Extract persistence layer and introduce trait-based ports (#74)
- BrainStores unified store access abstraction

### Testing

- Update integration tests for unified DB path

## [0.2.1] - 2026-03-11

### Bug Fixes

- Collapse nested if statements to satisfy clippy collapsible_if
- Make assignee filter case-insensitive (#51)

### Documentation

- Document task capsule embedding in AGENTS.md

### Features

- Federated search across multiple brain projects (#54)
- Add backfill-tasks CLI command and document task vector search
- Hook capsule embedding into task_apply_event
- Add kind field to MemoryStub and search_minimal responses
- Embed outcome capsule after task close
- Add capsule module and SQLite task chunk storage
- Add writable Store to McpContext for task capsule embedding
- Cross-brain task creation (CLI + MCP) (#53)
- Add cross-brain task references (#52)
- Unified daemon watches all registered brain projects (#50)
- Add records domain with event-sourced storage (#49)

### Refactoring

- Fix too_many_arguments clippy warning on embed_task_capsule

### Testing

- Add integration tests for task capsule embedding and search

## [0.2.0] - 2026-03-10

### Bug Fixes

- Eliminate error swallowing across all tool handlers and enrichment layer
- Rename cli package to brain for correct cargo-dist artifact naming (#41)
- Retrieval polish wiring (#36)
- Propagate ancestor blocked state to child tasks in ready/blocked queries
- Hooks seup on init (#29)

### Documentation

- Add cross-task insights and planning references conventions (#40)
- Update README status table — phases 3-4 done, add phase 5

### Features

- Auto-replace stale daemon when binary changes (#47)
- Hide event_id and short_id from MCP/CLI output (#44)
- Add label schema documentation and task creation guidelines
- Add brain agent schema command and discriminated MCP inputSchema (#42)
- Add fusion confidence metric for adaptive reranking (#39)
- Add `brain docs` command to regenerate AGENTS.md (#38)
- Add strict TaskType enum with spike variant (#37)
- Batch label and dependency operations (#35)
- Rc hardening and optimization (#34)
- Initial task retrieval optimization (#32)
- Add IVF-PQ vector index with auto-creation and nprobes support
- Reduce tasks list mcp output (#31)
- Determenistic chunking contract (#30)

### Refactoring

- Robustness, testability & idiomatic Rust (batch 1) (#48)
- Extract shared task-listing helpers and eliminate panic paths
- Add error-handling helpers and harden protocol serialization
- Always use dot notation for child task IDs (#46)
- Break up god files into role-focused modules (#45)
- Migrate CLAUDE.md to AGENTS.md bridge pattern

## [0.1.2] - 2026-03-06

### Features

- Improve init setup and MCP server resilience (#28)

## [0.1.1] - 2026-03-06

### Bug Fixes

- Scan blocking and error metrics (#19)
- Eager delete orphaned chunks
- TOCTOU race condition for eager and redundant file reads
- Fall-through case for 0 row tables in index store
- Make rename_file async so it updates both SQLite an LanceDB
- Return proper error to avoid daemon panics
- Added content hash check to the resurrect indexing

### Documentation

- Update readme (#18)
- Update retrieval policy graph in architecture reference
- Document decision to decouple notes and tasks architecture
- Add math and compsci references
- Describe inital considerations for performance strategies
- Add initial project scope documentation

### Features

- Resolve config path from cwd (#27)
- Add brain inti command (#26)
- Dogfooding prep (#25)
- Implement lanceDB schema versioning (#22)
- Add reindex, vacuum, and doctor CLI commands
- Wire cli hybrid ranking (#21)
- Add bounded work queue with file_id coalescing (#20)
- Add runtime observability metrics and structured logging (#16)
- Separate read/write SQLite connections and add StoreReader
- Lancedb dual trigger optimize scheduler (#13)
- Migrate task id to prefixed ulid (#12)
- Implement get and list tools for tasks (#8)
- Task related cli tooling (#7)
- Implement batched embedding with backpressure (#6)
- Add initial MCP task tooling (#4)
- Event sourced task system (#2)
- Hybrid retrieval for agentic memory (#1)
- Process file modification events
- Add proper file ID validation for store operations
- Dual-store-arch-impl
- Workspace-setup

### Performance

- Use candle accelerate for macos (#15)
- Check for debug target when invoking query command from justfile

### Refactoring

- Codebase cleanup and reconsolidating (#11)

### Testing

- Add byte-offset correctness tests for chunker and parser (#24)
- Add migration test harness with schema snapshots (#23)
- Perf and concurrency test (#17)
- Add smoke tests for current chunk, embed and hash-gate pipeline
- Add-testing-fixtures

