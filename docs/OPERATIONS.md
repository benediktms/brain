# Operations Guide

This document covers upgrading, backup, recovery, troubleshooting, performance tuning, and model management for brain.

---

## 1. Upgrading

Brain handles most upgrade tasks automatically on the next startup or index cycle.

### Schema Migrations

When brain opens the SQLite database, `init_schema` checks the stored `user_version` against the compiled `SCHEMA_VERSION` (currently **13**). Version 12 added records domain tables (`records`, `record_tags`, `record_links`, `record_events`). Version 13 added storage lifecycle columns (`retention_class`, `pinned`, `payload_available`, `content_encoding`, `original_size`) for compression, retention classes, and payload eviction. If the stored version is behind, a version-aware migration dispatch loop runs each migration sequentially in its own transaction — from the current version up to the target. If the stored version is *ahead* of the binary (e.g. you downgraded), brain rejects the database with an error rather than silently corrupting it.

### FTS5 Tables

The full-text search tables (`fts_chunks` and `fts_tasks`) and their sync triggers are rebuilt idempotently on **every** startup via `ensure_fts5()`. No manual action is needed.

### Chunker Version

The chunker algorithm is versioned (currently **`CHUNKER_VERSION = 2`**). When the version changes between releases, the `HashGate` detects the mismatch during the next index or watch cycle and re-indexes affected files — even if the file content hasn't changed. This happens lazily: files are re-chunked as they are encountered, not all at once.

### LanceDB

Vector database schema changes may require a full re-embed. After a major upgrade, if search results seem off, run:

```sh
brain reindex --full .
```

### Upgrade Checklist

1. (Optional) Back up `.brain/brain.db` and `.brain/lancedb/` — see [Backup](#2-backup)
2. Install the new binary (`just install`)
3. Run any brain command — migrations and FTS5 rebuild happen automatically
4. If the changelog mentions a chunker or embedding change, run `brain reindex --full .`

---

## 2. Backup

### Source of Truth

Markdown files are the durable source of truth. Brain **never writes to your notes** — the sync is strictly unidirectional (notes → brain). This means the index is always rebuildable.

### Recommended Backup

Before a major upgrade, copy the derived data:

```sh
cp -r .brain/brain.db .brain/brain.db.bak
cp -r .brain/lancedb/ .brain/lancedb.bak/
```

For registered brains, the derived data lives at `~/.brain/brains/<name>/` instead.

### Records Data

The records event log at `~/.brain/brains/<name>/records/events.jsonl` is the source of truth for record metadata and should be backed up. The object store at `~/.brain/brains/<name>/objects/` contains immutable content-addressed blobs that can be large — back up if payload content is irreplaceable.

```sh
cp ~/.brain/brains/<name>/records/events.jsonl ~/.brain/brains/<name>/records/events.jsonl.bak
cp -r ~/.brain/brains/<name>/objects/ ~/.brain/brains/<name>/objects.bak/
```

### Lightweight Approach

Skip backups entirely. If anything goes wrong, delete the derived data and rebuild:

```sh
brain reindex --full .
```

This re-scans all Markdown files, re-chunks, re-embeds, and repopulates both SQLite and LanceDB from scratch.

### What NOT to Back Up

- **`.brain/models/`** — Downloaded separately (~130MB). Re-download with `just setup-model` if needed.

### Global Config

The brain registry at `~/.brain/config.toml` is small and worth backing up. It stores registered brain names, root paths, and note directories.

---

## 3. Recovery

| Scenario | Recovery |
|----------|----------|
| SQLite corrupted | Delete `.brain/brain.db` (and any `.brain/brain.db-wal`, `.brain/brain.db-shm`), then run `brain reindex --full .` |
| LanceDB corrupted | Delete `.brain/lancedb/`, then run `brain reindex --full .` |
| Both corrupted | Delete both directories, then run `brain reindex --full .` |
| Stuck indexing files | `brain doctor .` detects stuck files; `brain reindex --full .` clears state |
| FTS out of sync | Automatic — `ensure_fts5()` rebuilds tables and triggers on every startup |
| Stale chunks after upgrade | Automatic — chunker version bump triggers lazy re-index on next cycle |
| Model files missing/corrupt | Run `just setup-model` or `scripts/setup-model.sh` |
| Records projection corrupted | The SQLite record tables are derived from `records/events.jsonl`. Delete `brain.db` and restart — the daemon rebuilds record projections from the event log automatically. |
| Object store corrupted | Run `brain records verify` to identify missing or corrupt blobs. Corrupt blobs can be removed manually. Missing blobs for evicted records are expected (`payload_available = 0`). |
| Orphan blobs accumulating | Run `brain records gc` to scan for unreferenced objects. Use `--dry-run` first to preview. |
| Record payload accidentally evicted | Eviction removes the blob but preserves metadata. If the original content is available, re-create the record with the same bytes — content-addressing stores it at the same hash. |
| Task data lost | Tasks live in `brain.db` — if SQLite is corrupted, task history is lost (no Markdown source). Back up `brain.db` if tasks are important. |

### Full Reset

To start completely fresh:

```sh
rm -rf .brain/brain.db .brain/brain.db-wal .brain/brain.db-shm .brain/lancedb/
brain reindex --full .
```

For registered brains:

```sh
rm -rf ~/.brain/brains/<name>/
brain reindex --full .
```

### Records Maintenance

```sh
# Verify integrity of the object store
brain records verify
brain records verify --verbose    # Show detailed findings

# Remove orphan blobs (unreferenced by any record)
brain records gc --dry-run        # Preview what would be removed
brain records gc                  # Actually remove orphans

# Evict a record's payload (free disk space, keep metadata)
brain records evict <record_id> --reason "no longer needed"

# Pin/unpin records to control eviction eligibility
brain records pin <record_id>
brain records unpin <record_id>
```

All commands support `--json` for machine-readable output. `brain records verify` exits with code 1 if issues are found, making it suitable for scripting and CI checks.

---

## 4. Troubleshooting

### "embedding model not found"

The embedding model is missing or incomplete.

```sh
# Preferred
just setup-model

# Or manually
scripts/setup-model.sh
```

This downloads `BAAI/bge-small-en-v1.5` (~130MB) to `~/.brain/models/bge-small-en-v1.5/`.

### "database schema version X is newer than supported version Y"

The database was created by a newer version of brain than the binary you're running. Either:
- **Upgrade the binary** to match the database version, or
- **Delete the database** and rebuild: `rm .brain/brain.db && brain reindex --full .`

### Stale or Missing Search Results

```sh
# Diagnose
brain doctor .

# Fix
brain reindex --full .
```

`brain doctor` checks for orphan chunks, stuck files, and index inconsistencies. A full reindex clears all content hashes and re-processes every file.

### Permission Errors on `~/.brain/`

```sh
chmod -R u+rw ~/.brain
```

### Daemon Won't Start

1. Check for a stale PID file:
   ```sh
   cat ~/.brain/brain.pid
   # If the process isn't running, remove it:
   rm ~/.brain/brain.pid
   ```
2. Check logs for errors:
   ```sh
   cat ~/.brain/brain.log
   ```
3. Try running the watcher in the foreground to see errors directly:
   ```sh
   brain watch .
   ```

### Disk Full During Indexing

Reclaim space by purging soft-deleted files, then retry:

```sh
brain vacuum --older-than 7
brain reindex --full .
```

### BLAKE3 Checksum Mismatch

Brain verifies model file integrity at startup using BLAKE3 checksums. If you see a checksum mismatch error with expected and actual hashes, the model files are corrupted. Re-download:

```sh
rm -rf ~/.brain/models/bge-small-en-v1.5/
just setup-model
```

### "Record not found" after database rebuild

Record projections are rebuilt from the records event log (`records/events.jsonl`). If the event log is intact, restart the daemon to trigger a projection rebuild. If the event log is missing, record metadata is lost — only the raw object store blobs remain.

### Object store growing unexpectedly

Run `brain records verify` to check for orphan blobs (objects not referenced by any record). Then `brain records gc` to remove them. Also check for duplicate large artifacts that could be archived.

---

## 5. Performance Tuning

### Query Parameters

| Parameter | Flag | Default | Description |
|-----------|------|---------|-------------|
| Result count | `-k` | 5 | Maximum number of results returned |
| Token budget | `--budget` | 800 | Token limit for result packing |
| Intent profile | `-i, --intent` | auto | Ranking signal weights |

**Intent profiles** shift how the six ranking signals (vector similarity, BM25 keyword, recency, backlinks, tags, importance) are weighted:

| Intent | Best for | Primary signal |
|--------|----------|----------------|
| `auto` | General queries | Equal weights |
| `lookup` | Exact keyword matches | BM25 (40%) |
| `planning` | Project planning | Recency + link structure |
| `reflection` | Journal/reflection | Recency-heavy |
| `synthesis` | Semantic similarity | Vector (40%) |

### Vacuum (Periodic Maintenance)

`brain vacuum` compacts SQLite (VACUUM), optimizes LanceDB, and purges soft-deleted files:

```sh
# Default: purge files deleted more than 30 days ago
brain vacuum

# Aggressive: purge files deleted more than 7 days ago
brain vacuum --older-than 7
```

Run periodically (e.g. monthly) to reclaim disk space and keep query performance stable.

### Watcher Debounce

File system events are coalesced before processing to avoid redundant re-indexing during rapid edits. This is an internal implementation detail and is not currently user-configurable.

---

## 6. Model Management

### Default Model

Brain uses [BAAI/bge-small-en-v1.5](https://huggingface.co/BAAI/bge-small-en-v1.5) — a 384-dimension BERT-based embedding model optimized for retrieval tasks.

### Directory Structure

```
~/.brain/models/bge-small-en-v1.5/
├── config.json        # BERT configuration (hidden_size=384)
├── tokenizer.json     # WordPiece tokenizer
└── model.safetensors  # Model weights (~130MB, memory-mapped at runtime)
```

### Prerequisites

The model setup script requires the **HuggingFace CLI** to download weights. The script will attempt to install it automatically, but you can install it manually:

| Platform | Install command | CLI binary |
|----------|----------------|------------|
| macOS | `brew install huggingface-cli` | `hf` |
| Linux (Debian/Ubuntu) | `pipx install huggingface-hub` | `huggingface-cli` |

**Optional:** Install [`b3sum`](https://github.com/BLAKE3-team/BLAKE3) for BLAKE3 checksum verification during download:

| Platform | Install command |
|----------|----------------|
| macOS | `brew install b3sum` |
| Linux (Debian/Ubuntu) | `apt install b3sum` |
| Any (via Cargo) | `cargo install b3sum` |

> **Note:** The Linux install commands above are for Debian/Ubuntu. Other distributions may use different package managers — substitute the appropriate command for your system (e.g. `dnf`, `pacman`, `zypper`). The package names are generally the same. Alternatively, `cargo install` works on any platform with a Rust toolchain.

Brain verifies model integrity at startup using built-in BLAKE3 hashing regardless — `b3sum` is only used to print checksums during the download step for manual verification.

### Setup Methods

1. **Just recipe** (preferred — auto-installs HuggingFace CLI if missing):
   ```sh
   just setup-model
   ```

2. **Setup script** (manual — also auto-installs HuggingFace CLI):
   ```sh
   scripts/setup-model.sh
   ```

3. **HuggingFace CLI** (direct — requires manual install of `hf`/`huggingface-cli`):
   ```sh
   hf download BAAI/bge-small-en-v1.5 config.json tokenizer.json model.safetensors \
     --local-dir ~/.brain/models/bge-small-en-v1.5
   ```

### Environment Overrides

| Variable | Description | Default |
|----------|-------------|---------|
| `BRAIN_MODEL_DIR` | Path to model directory | `./.brain/models/bge-small-en-v1.5` |
| `BRAIN_HOME` | Brain home directory | `~/.brain` |

Example — use a model from a custom location:

```sh
BRAIN_MODEL_DIR=/opt/models/bge-small brain query "async patterns"
```

### Integrity Verification

Brain verifies model files using BLAKE3 checksums at startup. If `b3sum` was available during download, checksums are printed for manual verification. If a file is corrupted or swapped, brain reports a checksum mismatch with the expected and actual hashes — re-download the model to fix.
