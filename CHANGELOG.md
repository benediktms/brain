# Changelog

All notable changes to this project will be documented in this file.

## [0.3.8] - 2026-05-01

### Bug Fixes

- Make BRAIN_HOME authoritative for path resolution (#104)
- Retrieval includes vetted band, not just trusted
- Handle nested transcript format and write vetted trust
- Emit minimal ack for Stop/PreCompact (no hookSpecificOutput)

### Features

- Cwd-based brain inference + show unscoped brain in list (#107)
- Cross-brain task transfer (preserve-ID) (#106)
- Auto-attach git worktrees to existing brain (#103)
- Deepen Claude Code hook integration
- PreToolUse:Edit|Write injects file-scoped memory (opt-in)
- Stop hook captures session episode from transcript
- Replace PreCompact stub with edited-files snapshot
- Add --output=hook-envelope and --frame=safety output flags
- Summarize hooks in dry-run output (#102)
- Hook-injection sanitization (brn-5da.9) (#101)
- Trust/provenance schema for hook-ingested records (brn-5da.8) (#100)
- Canonicalize plugin templates (brn-5da.6) (#99)

### Refactoring

- Remove legacy CLI surface and clean up dead code (#98)
- Remove legacy search_minimal and expand tools (#97)

## [0.3.7] - 2026-04-29

### Features

- Cross-brain deps + first-class external blockers gate readiness (#96)
- CLI plumbing for memory.retrieve (brn-83a.6.4) (#95)
- Explainability fields + deterministic packing (brn-83a.6.3) (#94)

## [0.3.6] - 2026-04-29

### Bug Fixes

- Hardening from review of brn-83a.7.2.5/.8
- Brain-scoping helpers + federated reads (brn-a3e) (#93)
- Address code review — :0 suffix, dedup helpers, pub(crate) visibility
- Address code review findings for federated LOD (brn-83a.5.4)
- Address code review findings for explainability and frontmatter
- Address code review findings for frontmatter signals
- Wire search layer into daemon McpContext for IPC dispatch (brn-3d9)
- Harden test_auto_index_on_optimize for CI
- Harden flaky perf tests for CI reliability
- Prevent dotfile directory names from becoming brain names
- Resolve clippy warnings and doctest failure on master

### Documentation

- Clarify ordering invariant test bound (brn-83a.7.2.4.4)
- Update AGENTS.md and init.rs MCP docs
- Update README and RECORDS.md for typed taxonomy
- Update SKILL.md templates for typed tools

### Features

- MCP + CLI surface for manual recluster and alias inspection (brn-83a.7.2.5)
- Observability instrumentation + end-to-end integration test (brn-83a.7.2.6)
- Case-insensitive tag_match_score (brn-83a.7.2.4.6)
- Alias-aware tag_match_score + alias_discount (brn-83a.7.2.4.4)
- Expand_tags_via_aliases + filter rewiring (brn-83a.7.2.4.3)
- FederatedPipeline brain_id plumbing (brn-83a.7.2.4.2)
- A
- TagAliasReader::alias_lookup_for_brain + MockTagAliasReader (brn-83a.7.2.4.1)
- Alias_lookup_for_brain reader + seed_tag_aliases fixture (brn-83a.7.2.4.1)
- Include summarizable records in tag-scope summaries
- Per-kind embedding policy in create tools
- [**breaking**] Replace 'artifacts create' with typed CLI subcommands
- [**breaking**] Remove records.create_artifact tool
- Scaffold documents/analyses/plans subcommands
- Add records.create_plan tool
- Add records.create_analysis tool
- Add records.create_document tool
- Migration v41->v42 normalize free-form kinds
- Add LinkWriter, EmbeddingOps, BrainManager port traits
- Use content-aware L0 abstract for immediate record creation path
- Unify record L0 generation for immediate and poll paths
- L0 generation and lod_chunks for reflections
- L0 generation + lod_chunks for episodes
- Wire record capsules into lod_chunks as L0 entries (brn-83a.5.5.2)
- Wire task capsules into lod_chunks as L0 entries (brn-83a.5.5.1)
- Cross-brain LOD resolution in federated retrieve (brn-83a.5.4)
- Frontmatter signals & retrieve explainability (brn-83a.6.3)
- Wire explain through federated search and expose fusion_confidence (brn-83a.6.3.2)
- Extract YAML frontmatter tags/importance for independent ranking signals (brn-83a.6.3.1)
- Implement Retrieve+ LOD storage and unified retrieve tool (#92)

### Refactoring

- Lift EMBEDDER_VERSION onto Embed trait (brn-83a.7.2.8)
- Derive searchable flag from KindPolicy in projections
- Redesign RecordKind enum with KindPolicy
- Address branch review feedback
- Remove public db() from RecordStore and TaskStore
- Remove public db() accessor from IndexPipeline
- Remove public db() accessor from BrainStores
- Parameterize upsert_domain_lod_l0 over LodChunkStore
- Pass &ctx.stores to enqueue_cluster_summarization
- Impl JobQueue and DerivedSummaryStore for BrainStores
- Add query_pipeline helper to BrainStores
- Add resolve_brain delegation and BrainRegistry impl
- Parameterize IndexPipeline over port traits
- Fix boundary violations — use BrainStores delegation

### Testing

- Pin federated exclude isolation + empty brain_id (brn-83a.7.2.4.7)
- Strengthen audit invariant from count to content (brn-83a.7.2.4.5)
- Regression suite — audit invariant + federated isolation (brn-83a.7.2.4.5)
- Migration v41->v42 fixture DB tests
- Integration tests for typed creation + policy

## [0.3.5] - 2026-03-31

### Bug Fixes

- Prevent daemon restart race from deleting new daemon's socket
- Fall back to BRAIN_HOME when MCP server can't resolve brain from cwd or registry
- Resolve brain name from registry for MCP server
- Raise optimize threshold to 5k rows / 10min — prevent OOM during bulk re-index
- Raise MIN_ROWS_FOR_INDEX to 100k — prevent OOM during auto-index
- Disable startup compaction — too memory-heavy for 28 brains
- Defer LanceDB cleanup to vacuum, avoid memory spike during scan
- Scope full_scan deletion by brain_id + batch LanceDB deletes
- Full_scan now soft-deletes files absent from scan results
- Add startup compaction to reduce LanceDB fragment memory bloat
- Scope embed_poll_sweep by brain_id + cap concurrent jobs
- Remediate reflection_sources FK references pointing to stale summaries_v27
- Address clippy warnings from just lint (-D warnings)
- Address code review findings from brain_id scoping
- Remove module aliases and rusqlite params from production code
- Remove invalid disallowed-methods entry
- Remove needless borrows flagged by clippy
- Replace silent error swallowing with explicit handling across MCP tools
- Versioned hooks/ dir with core.hooksPath for worktree support
- Handle custom merge format without 'branch' keyword

### Documentation

- Update search_minimal docs with new filter params and result kinds
- Update scoping model and workspace layout for brain_id on files/chunks
- Rewrite README and ARCHITECTURE for task-management-first framing

### Features

- Canonical metadata facets & time-scope filters (brn-83a.7.1)
- Unified LanceDB vector store — single store with brain_id scoping
- Thread brain_id through indexing pipeline + fix CLI boundary imports
- Add brain_id filtering to FTS search queries
- Add brain_id to files/chunks tables (v36→v37)

### Refactoring

- Enforce port-trait boundary in MCP handlers
- Remove direct db access, add clippy.toml enforcement
- Extract SQL helpers, add test harness, audit unwraps
- Wire ranking signals, async summary embedding, scoping docs, port traits
- Delete dead inline consolidation scheduler

### Testing

- Add handler tests and remove all unwraps

## [0.3.4] - 2026-03-28

### Bug Fixes

- Use per-marketplace layout for Claude Code discovery
- DB is source of truth, rename config.toml → state_projection.toml (#brn-990)

### Features

- Domain-scoped Claude Code plugins with skills format (#91)
- DB source of truth + state_projection.toml (#brn-990)

## [0.3.3] - 2026-03-27

### Bug Fixes

- Cross-brain prefix resolution at TaskStore level
- Brain-scoped task ID resolution — prevent cross-brain collisions (#brn-889)
- Update migration harness for v35 — add snapshot, tables, indexes
- Wrap scope summary upsert + lineage in single transaction
- 5 issues from adversarial review — guard gap, stuck jobs, hash CAS
- 4 bugs from deep review — hash bypass, transaction atomicity, dedup guard, ID reuse
- Persist LLM results + per-brain episode tracking
- In-memory lock set + status guards to prevent race conditions
- Prevent duplicate consolidation by checking active jobs
- Graceful handling of brain entries with empty roots
- Remove blocking full_scan from multi-brain init
- Remove redundant embed poll + fix parser panic on dual frontmatter
- Non-blocking embed poll (brn-e18)
- Non-blocking embed poll prevents starving select loop
- Ensure all MCP tools and CLI commands emit compact task IDs
- DB as source of truth, prefix preservation (#89)
- Normalize tasks.close inputs and parent IDs
- Resolve all clippy warnings for CI compliance
- Use remote brain name for URI in cross-brain record_get
- Restore missing partial unique index in v27→v28 + wire migration harness
- Address compliance matrix findings (C1, M1, M9, m2, m3, m8, n1)
- Align write_procedure embed format + add federated explain TODO
- Address compliance matrix minor findings
- Flip hierarchy tests from stub assertions to green-phase
- Update tool count assertion 28→29 for summarize_scope
- Use safe UTF-8 truncation in episode summary
- Handle FTS5 + reflection_sources FK in v28 migration
- Add tracing::warn for graph expansion errors
- Align signal field names with MCP (sim_vector/bm25)
- Update tool count assertion for records.search addition

### Documentation

- Add compact parent ID invariant and regression tests
- Clean up docs — consolidate ADRs, remove implemented plans, rename roadmap
- Mark hierarchy summaries + consolidation as WIP

### Features

- Non-blocking daemon + staleness detection (#brn-d7c, #brn-8d1, #brn-642)
- Content hash optimization + source lineage in scope summaries
- Propagate staleness to directory scopes on file re-index
- V35 migration — source lineage + staleness detection
- Per-kind reschedule delays + parser panic fix
- Job-based daemon architecture with per-brain scheduling
- Spawn startup scan as background task
- .gitignore support + non-blocking daemon prep (brn-45e, brn-d7c)
- Respect .gitignore during file scanning
- Stale scope sweep and consolidation sweep recurring jobs
- Add stale scope sweep and consolidation sweep recurring jobs
- Recurring job scheduling with singleton concurrency safety (#f0a)
- Recurring job scheduling with singleton concurrency safety
- Embed git SHA as version for local just installs
- Stable BLAKE3 hash-based short IDs for tasks (#88)
- L0 extractive abstract for record embeddings
- Hierarchy summaries — derived_summaries table + summarize_scope
- Brain:// URI output on 16 tools + input on 11 tools
- BrainUri module + v29 object_links migration + URI MCP tests
- Consolidation + dedup implementation + CLI
- Add brain memory write-procedure subcommand
- Memory.write_procedure MCP tool + summary_kind enrichment
- V28 migration + store_procedure + TDD tests
- 1-hop graph expansion + set_db lifecycle fix
- Add --explain flag to memory search + records search subcommand
- Implement explain mode + records.search MCP tool

### Refactoring

- Use named SQL params in ensure_singleton_job
- Rename tasks.id column to display_id for clarity
- Centralize compact parent ID formatting across MCP and CLI
- Rename brain:// → synapse:// + extend URI coverage to memory tools + CLI
- Address compliance matrix major findings

### Testing

- Add full migration harness test
- Integration tests for reaper-vs-active-lock invariant
- TDD for episode consolidation + vector dedup
- TDD for PageRank lifecycle + 1-hop graph expansion
- TDD integration tests for expand record:*, explain mode, records.search

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

