# Changelog

All notable changes to this project will be documented in this file.

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

