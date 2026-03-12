# Records Domain

The records domain introduces durable work products and opaque state bundles as first-class citizens in Brain. This document describes the domain model, storage architecture, event types, projection tables, CLI and MCP surfaces, and operational guidance.

## Overview

Brain has two existing durable domains:

- **Notes** — Markdown files are the source of truth. SQLite and LanceDB are derived projections.
- **Tasks** — SQLite is the runtime source of truth. The append-only JSONL event log is a best-effort audit trail. `rebuild_projections()` can reconstruct SQLite from the log as a recovery mechanism.

Records is a third parallel domain serving a different purpose: **capturing the outputs of work**, not the work itself.

### Why a new domain rather than extending tasks?

Tasks track intent and execution state (what needs to be done, who owns it, is it blocked). Records track the *products* of completed work: generated documents, analysis reports, exported data, diff summaries, and saved state bundles. These are fundamentally different lifecycles:

- A task can be re-opened, re-assigned, or cancelled. A record is immutable once created.
- A task's value is in its workflow state. A record's value is in its payload content.
- Tasks need cycle detection, dependency graphs, and status machines. Records need content addressing, deduplication, and archival.

Extending tasks to carry payloads would conflate two orthogonal concerns and add payload storage complexity to a subsystem designed for lightweight metadata events.

### Why a new domain rather than extending notes?

Notes are Markdown files managed by humans and indexed by Brain. Records are machine-generated artifacts managed programmatically by agents and Brain tools. Notes are mutable (content changes trigger re-indexing). Record payloads are immutable once written (content-addressed by hash). Mixing the two would require special-casing machine artifacts throughout the notes indexing pipeline.

### Relationship to tasks and notes

Records and tasks can cross-reference each other. A `RecordCreated` event can carry a `task_id` linking the record to the task that produced it. A record can also hold a `chunk_id` referencing the note that it summarizes or documents. These are soft references — no foreign-key validation at write time, consistent with how `NoteLinked` events work in the tasks domain.

Records are standalone-valid when Brain is used without any orchestrator runtime. A snapshot stored in the object store is an opaque byte bundle — Brain makes no assumptions about its internal format. An artifact is a structured document — Brain stores metadata and content references but does not interpret content beyond what is needed for retrieval.

---

## Domain Model

### Artifact

A durable work product with known structure and semantics. Artifacts are things an agent or tool explicitly creates as outputs: reports, diffs, exports, structured analyses, generated documents.

Key properties:
- Has a `RecordKind` that describes its category (e.g., `report`, `diff`, `export`, `analysis`)
- Has a `title` and optional `description` for human-readable identification
- Has a `ContentRef` pointing to the payload in the object store
- Has a `RecordStatus` tracking its lifecycle (active, archived)
- Can carry tags and links to tasks or note chunks
- Immutable after creation (updates only affect metadata, never the payload)

### Snapshot

A generic saved state bundle. Snapshots are opaque to Brain core — the internal format is defined by the creator (an agent, a CLI command, or an external tool). Brain stores the bytes, the hash, and the metadata, but does not parse or interpret the content.

Key properties:
- Same metadata structure as Artifact (id, title, tags, links, status)
- `RecordKind::Snapshot` always
- Content is opaque — Brain does not assume any internal format
- NOT a Brain-native workflow checkpoint. Snapshots are not coupled to any orchestrator's execution model. They are generic state bundles that any caller can write and retrieve.

### RecordId

A ULID string, prefixed with the brain's project prefix for human readability. Example: `BRN-01KK7XXXXXXXXXXXXXXXXXXXX`. Generated at write time. Prefix resolution is supported: short prefixes (minimum 4 ULID characters after the project prefix) are accepted by CLI and MCP tools and resolved to the full ID.

### ContentRef

A reference to an object in the content-addressed object store. Contains:
- `hash`: hex-encoded BLAKE3 digest of the raw payload bytes (64 hex chars, 256 bits)
- `size`: byte length of the stored object (u64) — may be less than original if compressed
- `media_type`: optional MIME type hint (e.g., `text/plain`, `application/json`)

The `hash` field doubles as the storage key. Two records with identical payloads share one object on disk.

### RecordKind

```
report      — Structured analysis or summary produced by an agent
diff        — A patch or change set (text or structured)
export      — A serialized data export (JSON, CSV, etc.)
analysis    — Quantitative or qualitative analysis result
document    — A generated prose document
snapshot    — Opaque saved state bundle (see Snapshot above)
```

This list is extensible. The kind is stored as a string in both the event log and the SQLite projection.

### RecordStatus

```
active      — The record is current and accessible
archived    — The record has been superseded or explicitly archived
```

Archived records remain in the event log and object store. Archival is a metadata-only operation.

---

## Event Types

All record events are stored in a separate JSONL event log at `.brain/records/events.jsonl`. They are never mixed with task events.

Each event has the envelope structure:

```json
{
  "event_id": "<ULID>",
  "record_id": "<RecordId>",
  "timestamp": <unix_seconds>,
  "actor": "<string>",
  "event_type": "<snake_case>",
  "event_version": 1,
  "payload": { ... }
}
```

### RecordCreated

Creates a new record (artifact or snapshot). Required fields:

```json
{
  "title": "Q1 performance analysis",
  "kind": "analysis",
  "content_ref": {
    "hash": "abc123...",
    "size": 4096,
    "media_type": "application/json"
  },
  "description": "Optional human-readable description",
  "task_id": "BRN-01KK...",
  "tags": ["performance", "q1-2026"],
  "retention_class": "permanent"
}
```

The payload bytes must be written to the object store before this event is appended. Write-then-append ordering ensures the object always exists when the event is visible.

### RecordUpdated

Updates mutable metadata fields (title, description). The `content_ref` cannot be changed — records are immutable once created.

```json
{
  "title": "Updated title",
  "description": "Clarified description"
}
```

### RecordArchived

Transitions the record status to `archived`.

```json
{
  "reason": "Superseded by BRN-01KK7YY..."
}
```

### TagAdded / TagRemoved

Adds or removes a single tag from the record's tag set.

```json
{
  "tag": "performance"
}
```

### LinkAdded / LinkRemoved

Adds or removes a cross-reference link. Links can target tasks (by `task_id`) or note chunks (by `chunk_id`). Both fields are optional but at least one must be present.

```json
{
  "task_id": "BRN-01KK...",
  "chunk_id": null
}
```

### PayloadEvicted

Records that a record's blob has been deleted from the object store. Sets `payload_available = false` in the projection. Metadata is preserved.

```json
{
  "content_hash": "e3b0c44...",
  "reason": "manual eviction"
}
```

### RetentionClassSet

Updates the retention class of a record. Retention classes: `ephemeral`, `standard`, `permanent` (or null to clear).

```json
{
  "retention_class": "permanent"
}
```

### RecordPinned / RecordUnpinned

Pins or unpins a record. Pinned records are exempt from payload eviction. Payload is empty (`{}`).

---

## Storage Layout

### Event Log (Source of Truth)

```
~/.brain/brains/<brain-name>/records/events.jsonl
```

Append-only JSONL, one event per line. ULID event IDs provide monotonic time ordering. SQLite is the runtime source of truth; writes go to SQLite first, then the event log is appended as a best-effort audit trail. If the JSONL append fails, the operation still succeeds. `rebuild_projections()` can reconstruct SQLite from this file as a recovery mechanism.

The records event log is kept separate from the tasks event log (`.brain/tasks/events.jsonl`) to preserve domain isolation and allow independent rebuild operations.

### Object Store (Payload Storage)

```
~/.brain/brains/<brain-name>/objects/
  <2-char prefix>/
    <full 64-char BLAKE3 hex>
```

Example: a payload with BLAKE3 hash `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` would be stored at:

```
~/.brain/brains/my-project/objects/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

The 2-character prefix sharding limits directory entry count in filesystems that degrade with large flat directories. This scheme is identical to the Git object store layout and provides good distribution for BLAKE3 hashes.

**Write protocol:**
1. Compute BLAKE3 hash of the payload bytes (always on the raw, pre-compression bytes).
2. Check if the object already exists at the expected path (deduplication).
3. If absent, optionally compress with zstd (level 3) if the payload exceeds the compression threshold.
4. Write to a temp file in the same directory, then `rename()` into place. The atomic rename prevents partial writes from being visible.
5. Return the `ContentRef` (hash + stored size + optional media type) to the caller.

**Read protocol:**
1. Derive the path from the hash in the `ContentRef`.
2. Read the file bytes.
3. Auto-detect zstd compression by checking for the zstd magic number (`0xFD2FB528` in the first 4 bytes). Decompress if detected.
4. Return the raw (decompressed) bytes to the caller.

**Deduplication:** Two records with identical payloads share one object on disk. A blob is deleted only when no other active record references its hash (enforced by `evict_payload`).

### SQLite Projection

Record metadata is projected into the existing per-brain `brain.db` SQLite database, alongside the tasks and notes tables. The projection is derived from the event log and fully rebuildable.

---

## Projection Tables

### records

Primary record metadata table.

```sql
CREATE TABLE records (
    record_id         TEXT PRIMARY KEY,
    title             TEXT NOT NULL,
    kind              TEXT NOT NULL,                  -- RecordKind value
    status            TEXT NOT NULL DEFAULT 'active'
                      CHECK (status IN ('active', 'archived')),
    description       TEXT,
    content_hash      TEXT NOT NULL,                  -- BLAKE3 hex (64 chars)
    content_size      INTEGER NOT NULL,               -- stored size in bytes
    media_type        TEXT,                           -- optional MIME hint
    task_id           TEXT,                           -- soft ref to tasks.task_id
    actor             TEXT NOT NULL,                  -- creator
    created_at        INTEGER NOT NULL,               -- unix seconds
    updated_at        INTEGER NOT NULL,               -- unix seconds
    retention_class   TEXT,                           -- "ephemeral" | "standard" | "permanent" | NULL
    pinned            INTEGER NOT NULL DEFAULT 0,     -- 1 = pinned (exempt from eviction)
    payload_available INTEGER NOT NULL DEFAULT 1,     -- 0 = blob evicted
    content_encoding  TEXT NOT NULL DEFAULT 'identity', -- "identity" | "zstd"
    original_size     INTEGER                         -- pre-compression byte length
);
```

### record_tags

Tag set for each record (many-to-one).

```sql
CREATE TABLE record_tags (
    record_id  TEXT NOT NULL REFERENCES records(record_id),
    tag        TEXT NOT NULL,
    PRIMARY KEY (record_id, tag)
);
```

### record_links

Cross-reference links to tasks or note chunks.

```sql
CREATE TABLE record_links (
    record_id  TEXT NOT NULL REFERENCES records(record_id),
    task_id    TEXT,                              -- soft ref; nullable
    chunk_id   TEXT,                              -- soft ref; nullable
    created_at INTEGER NOT NULL,
    CHECK (task_id IS NOT NULL OR chunk_id IS NOT NULL)
);
CREATE INDEX record_links_task_id ON record_links(task_id) WHERE task_id IS NOT NULL;
CREATE INDEX record_links_chunk_id ON record_links(chunk_id) WHERE chunk_id IS NOT NULL;
```

### record_events

Full event audit log for the records domain, mirroring the JSONL log in queryable form.

```sql
CREATE TABLE record_events (
    event_id    TEXT PRIMARY KEY,
    record_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    timestamp   INTEGER NOT NULL,
    actor       TEXT NOT NULL,
    payload     TEXT NOT NULL                     -- JSON blob
);
CREATE INDEX record_events_record_id ON record_events(record_id);
```

---

## Rebuild Strategy

### Deterministic Rebuild

The SQLite projection is fully rebuildable from the event log at any time. The rebuild procedure:

1. Open a single SQLite transaction.
2. DELETE all rows from `record_events`, `record_links`, `record_tags`, `records` (in FK-safe order).
3. Replay every event from the JSONL file in order, applying each event to the cleared tables.
4. Commit the transaction.

This is identical to the tasks domain rebuild pattern.

### Invariants

- SQLite is the runtime source of truth for record metadata. Writes go to SQLite first, then the event log as a best-effort audit trail. If JSONL append fails, the operation still succeeds (a `tracing::warn!` is emitted).
- Object payloads are written before the SQLite insert. If the process crashes between payload write and SQLite insert, the payload exists but no record references it — this is safe (the object is an orphan blob, not a corrupted record). `brain records gc` will clean it up.
- `rebuild_projections()` can reconstruct SQLite from the event log as a recovery mechanism if the database is lost or corrupted.
- Object store entries are immutable. Never overwrite an existing object at a given hash path.

### Crash Recovery

On startup, if the daemon detects that the SQLite `records` table is absent or the schema version is below the records migration version, it triggers a full projection rebuild from the event log.

If the event log contains events for records whose objects are missing from the object store (e.g., the `objects/` directory was partially deleted), the projection rebuilds successfully but object reads for those records will fail at query time. This is surfaced as a retrieval error, not a startup failure. `brain records verify` will identify the missing blobs.

---

## Content-Addressed Object Store

### BLAKE3 Hashing

BLAKE3 is already used in Brain for content hashing in the notes indexing pipeline. The records object store reuses this choice for consistency. BLAKE3 properties:

- 256-bit output (64 hex chars): collision probability < 10^-70 for any realistic corpus
- 3–4x faster than SHA-256 on modern CPUs
- Streaming and parallelizable

The hash is always computed over the raw (pre-compression) bytes. This ensures deduplication works correctly: two identical payloads produce the same hash regardless of whether compression was applied to one or both.

### Compression

Object payloads are transparently compressed with zstd (level 3) when the payload size exceeds a configurable threshold. The BLAKE3 hash is always computed on the raw bytes, so deduplication is preserved across compressed and uncompressed writes.

On read, zstd compression is auto-detected by checking the first 4 bytes for the zstd magic number (`[0x28, 0xB5, 0x2F, 0xFD]`). If detected, the blob is decompressed before being returned to the caller. This is transparent — callers always receive the original bytes.

The `content_encoding` column in the projection records whether the stored blob is `"identity"` or `"zstd"`. The `original_size` column records the pre-compression byte length.

---

## CLI Reference

### brain artifacts (alias: art)

```
brain artifacts create --title <title> [--kind <kind>] [--file <path>|--stdin]
    Create a new artifact. Reads payload from file or stdin.
    Options: --description <text>, --task <task_id>, --tag <tag>..., --media-type <mime>
    Default kind: document

brain artifacts list [--kind <kind>] [--tag <tag>] [--status active|archived] [--limit <n>]
    List artifacts, filtered by kind, tag, or status. Default status: active. Default limit: 50.

brain artifacts get <record_id>
    Show full metadata for an artifact (tags, links, hash, size, actor, timestamps).

brain artifacts archive <record_id> [--reason <text>]
    Mark an artifact as archived.

brain artifacts tag add <record_id> <tag>
    Add a tag to an artifact.

brain artifacts tag remove <record_id> <tag>
    Remove a tag from an artifact.

brain artifacts link add <record_id> [--task <task_id>] [--chunk <chunk_id>]
    Add a link from an artifact to a task or note chunk.

brain artifacts link remove <record_id> [--task <task_id>] [--chunk <chunk_id>]
    Remove a link from an artifact to a task or note chunk.
```

### brain snapshots (alias: snap)

```
brain snapshots save --title <title> [--file <path>|--stdin]
    Save an opaque state bundle as a snapshot. Reads bytes from file or stdin.
    Options: --description <text>, --task <task_id>, --tag <tag>..., --media-type <mime>
    Default media type: application/octet-stream

brain snapshots list [--tag <tag>] [--status active|archived] [--limit <n>]
    List snapshots. Default status: active. Default limit: 50.

brain snapshots get <record_id>
    Show metadata for a snapshot (tags, links, hash, size, timestamps).

brain snapshots restore <record_id> [--output <path>]
    Write the snapshot bytes to a file or stdout (default: stdout).

brain snapshots archive <record_id> [--reason <text>]
    Mark a snapshot as archived.

brain snapshots tag add <record_id> <tag>
    Add a tag to a snapshot.

brain snapshots tag remove <record_id> <tag>
    Remove a tag from a snapshot.

brain snapshots link add <record_id> [--task <task_id>] [--chunk <chunk_id>]
    Add a link from a snapshot to a task or note chunk.

brain snapshots link remove <record_id> [--task <task_id>] [--chunk <chunk_id>]
    Remove a link from a snapshot to a task or note chunk.
```

### brain records

Maintenance commands for the records object store.

```
brain records verify [--verbose]
    Verify integrity of the records object store.
    Checks: missing blobs (referenced but absent), corrupt blobs (hash mismatch),
    orphan blobs (present but unreferenced), stale flags (payload_available=false but blob exists).
    Exits 0 if clean, 1 if issues found. Use --verbose for per-issue details.

brain records gc [--dry-run]
    Remove orphan blobs from the object store.
    Orphans are blobs not referenced by any record in the event log.
    Use --dry-run to preview without deleting.

brain records evict <record_id> [--reason <text>]
    Evict a record's payload from the object store.
    Deletes the blob if no other record shares its hash.
    Appends a PayloadEvicted event. Metadata is preserved.

brain records pin <record_id>
    Pin a record, marking it exempt from future eviction.
    Appends a RecordPinned event.

brain records unpin <record_id>
    Unpin a record, allowing its payload to be evicted.
    Appends a RecordUnpinned event.
```

All commands support `--json` for machine-readable output.

### Usage Examples

```bash
# Create a report artifact from a file
brain artifacts create --title "Q1 analysis" --kind report --file report.json

# Create a document from stdin
echo "Summary text" | brain artifacts create --title "Meeting notes" --stdin

# List active artifacts
brain artifacts list

# Show full details with tags and links
brain artifacts get BRN-01KK

# Save a binary state bundle as a snapshot
brain snapshots save --title "Agent state v3" --file state.bin

# Restore snapshot bytes to stdout
brain snapshots restore BRN-01KK7 | gunzip > restored.json

# Restore to a file
brain snapshots restore BRN-01KK7 --output state.bin

# Archive an artifact with a reason
brain artifacts archive BRN-01KK --reason "Superseded by BRN-01KK7YY"

# Tag operations
brain artifacts tag add BRN-01KK performance
brain artifacts tag remove BRN-01KK performance

# Link an artifact to a task
brain artifacts link add BRN-01KK --task BRN-01KKBV

# Verify store integrity
brain records verify

# Preview what gc would remove
brain records gc --dry-run

# Remove orphan blobs
brain records gc

# Evict a payload (metadata is kept)
brain records evict BRN-01KK --reason "storage reclamation"

# Pin a record to prevent eviction
brain records pin BRN-01KK
```

---

## MCP Tools Reference

The records domain exposes 10 tools via the MCP stdio JSON-RPC interface.

### records.create_artifact

Creates a new artifact record. Writes data to the object store and appends a `RecordCreated` event.

```
Input:
  title       string    (required) — Human-readable title
  kind        string    (optional, default "document") — report|diff|export|analysis|document|custom
  data        string    (optional) — Base64-encoded content bytes. Provide 'data' or 'text', not both. Omit both for metadata-only record.
  text        string    (optional) — Plain-text content (server encodes internally). Provide 'text' or 'data', not both.
  description string    (optional) — Free-text description
  task_id     string    (optional) — Soft link to originating task
  tags        string[]  (optional) — Initial tags
  media_type  string    (optional) — MIME type hint. Defaults to 'text/plain' for text, 'application/octet-stream' for data.

Output:
  record_id, content_hash, size
```

### records.save_snapshot

Saves a new snapshot record. Kind is always `"snapshot"`.

```
Input:
  title       string    (required) — Human-readable title
  data        string    (optional) — Base64-encoded snapshot bytes. Provide 'data' or 'text', not both.
  text        string    (optional) — Plain-text content (server encodes internally). Provide 'text' or 'data', not both.
  description string    (optional) — Free-text description
  task_id     string    (optional) — Soft link to originating task
  tags        string[]  (optional) — Initial tags
  media_type  string    (optional) — MIME type hint. Defaults to 'text/plain' for text, 'application/octet-stream' for data.

Output:
  record_id, content_hash, size
```

### records.get

Retrieves full metadata for a record by ID. Supports prefix resolution.

```
Input:
  record_id   string    (required) — Full ID or unique prefix

Output:
  record_id, title, kind, status, description, content_hash, content_size,
  media_type, task_id, actor, created_at, updated_at, tags[], links[]
```

### records.list

Lists records with optional filters. Returns compact (shortest-unique-prefix) IDs.

```
Input:
  kind        string    (optional) — Filter by kind
  status      string    (optional, default "active") — "active" | "archived"
  tag         string    (optional) — Filter by tag
  task_id     string    (optional) — Filter by associated task
  limit       integer   (optional, default 50)

Output:
  records[]  — Array of metadata stubs (no payloads), count
```

### records.fetch_content

Retrieves the raw content of a record. Returns base64-encoded data.

```
Input:
  record_id   string    (required) — Full ID or unique prefix

Output:
  record_id, content_hash, size, media_type, data (base64)
```

### records.archive

Archives a record by appending a `RecordArchived` event.

```
Input:
  record_id   string    (required) — Full ID or unique prefix
  reason      string    (optional) — Reason for archiving

Output:
  record_id, status: "archived"
```

### records.tag_add

Adds a tag to a record (artifact or snapshot). Idempotent.

```
Input:
  record_id   string    (required) — Full ID or unique prefix
  tag         string    (required) — Tag to add

Output:
  record_id, tag, action: "added"
```

### records.tag_remove

Removes a tag from a record. Idempotent.

```
Input:
  record_id   string    (required) — Full ID or unique prefix
  tag         string    (required) — Tag to remove

Output:
  record_id, tag, action: "removed"
```

### records.link_add

Adds a link from a record to a task or note chunk. At least one of `task_id` or `chunk_id` must be provided.

```
Input:
  record_id   string    (required) — Full ID or unique prefix
  task_id     string    (optional) — Task to link to
  chunk_id    string    (optional) — Note chunk to link to

Output:
  record_id, task_id, chunk_id, action: "linked"
```

### records.link_remove

Removes a link from a record to a task or note chunk. At least one of `task_id` or `chunk_id` must be provided. Idempotent.

```
Input:
  record_id   string    (required) — Full ID or unique prefix
  task_id     string    (optional) — Task to unlink
  chunk_id    string    (optional) — Note chunk to unlink

Output:
  record_id, task_id, chunk_id, action: "unlinked"
```

---

## Payload Lifecycle

### Retention Classes

Records can be assigned a retention class that signals intended lifetime to tooling:

- `ephemeral` — Short-lived; expected to be evicted when no longer needed
- `standard` — Default; retained until explicitly archived or evicted
- `permanent` — Long-lived; should not be evicted without operator action

The retention class is a hint stored in the projection. It does not automatically trigger eviction — eviction is always an explicit operator action.

### Eviction

Eviction deletes a record's blob from the object store while preserving the record's metadata in the event log and projection. After eviction:

- `payload_available` is set to `false` in the projection
- The blob is deleted from the object store only if no other record references the same hash
- Attempts to read the content will return an error
- The record remains queryable by metadata (title, tags, links, kind, status)

Pinned records cannot be evicted. `brain records evict` will refuse to evict a pinned record.

### Pinning

Pinning marks a record as exempt from eviction. A `RecordPinned` event is appended; `pinned = 1` is set in the projection. `brain records unpin` appends `RecordUnpinned` and clears the flag.

---

## Operational Guidance

### Integrity Verification

Run `brain records verify` to check the health of the object store:

- **Missing blobs**: Referenced by a record but absent from disk. May indicate disk corruption or accidental deletion. The record's metadata is intact but content reads will fail.
- **Corrupt blobs**: On disk but BLAKE3 hash does not match the filename. Indicates bit-rot or overwrites. The blob file is untrustworthy and should be treated as missing.
- **Orphan blobs**: Present on disk but not referenced by any record. Harmless but wasteful. Clean up with `brain records gc`.
- **Stale flags**: `payload_available = false` in the projection but the blob still exists on disk. Usually indicates a crash between eviction event commit and blob deletion (see below). Harmless — the blob is just taking disk space. Can be cleaned by re-running eviction or gc.

### Crash Recovery

The records domain uses an event-first design. The correct state is always the event log; the SQLite projection is a derived view.

**Crash between payload write and event append:** The blob exists in the object store but no `RecordCreated` event references it. The record was never created. The orphan blob will be detected by `brain records verify` and cleaned by `brain records gc`.

**Crash between event append and projection update:** The event log is ahead of the projection. On the next startup (or on-demand via rebuild), the projection is reconstructed from the event log. No data is lost.

**Crash between eviction event commit and blob deletion:** The `PayloadEvicted` event exists in the log, so the projection correctly shows `payload_available = false`. The blob still exists on disk — this is flagged as a `stale_flag` by `brain records verify`. The blob is not an orphan (the record still references the hash), so `brain records gc` will not remove it automatically. A subsequent eviction call or manual delete will resolve the stale flag. Future gc improvements may handle this case automatically.

### Routine Maintenance

For long-running deployments, schedule periodic maintenance:

```bash
# Weekly: verify store health
brain records verify

# After verification: clean orphans if any
brain records gc

# Archive old artifacts no longer actively used
brain artifacts list --status active | grep old-prefix | xargs -I{} brain artifacts archive {}
```
