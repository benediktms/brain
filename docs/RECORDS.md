# Records Domain

The records domain introduces durable work products and opaque state bundles as first-class citizens in Brain. This document describes the domain model, storage architecture, event types, projection tables, and planned CLI/MCP surfaces.

## Overview

Brain has two existing durable domains:

- **Notes** — Markdown files are the source of truth. SQLite and LanceDB are derived projections.
- **Tasks** — An append-only JSONL event log is the source of truth. SQLite tables are derived projections.

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

A ULID string, prefixed with the brain's project prefix for human readability. Example: `BRN-01KK7XXXXXXXXXXXXXXXXXXXX`. Generated at write time.

### ContentRef

A reference to an object in the content-addressed object store. Contains:
- `hash`: hex-encoded BLAKE3 digest of the raw payload bytes (64 hex chars, 256 bits)
- `size`: byte length of the payload (u64)
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
  "tags": ["performance", "q1-2026"]
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

---

## Storage Layout

### Event Log (Source of Truth)

```
~/.brain/brains/<brain-name>/records/events.jsonl
```

Append-only JSONL, one event per line. ULID event IDs provide monotonic time ordering. This file is the authoritative source of truth for all record metadata. The SQLite projection is always rebuildable from this file.

The records event log is kept separate from the tasks event log (`.brain/tasks/events.jsonl`) to preserve domain isolation and allow independent rebuild operations.

### Object Store (Payload Storage)

```
~/.brain/brains/<brain-name>/objects/
  <2-char prefix>/
    <remaining 62 chars of BLAKE3 hex>/
      <full 64-char BLAKE3 hex>
```

Example: a payload with BLAKE3 hash `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` would be stored at:

```
~/.brain/brains/my-project/objects/e3/b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

The 2-character prefix sharding limits directory entry count in filesystems that degrade with large flat directories. This scheme is identical to the Git object store layout and provides good distribution for BLAKE3 hashes.

**Write protocol:**
1. Compute BLAKE3 hash of the payload bytes.
2. Check if the object already exists at the expected path (deduplication).
3. If absent, write to a temp file in the same directory, then `rename()` into place. The atomic rename prevents partial writes from being visible.
4. Return the `ContentRef` (hash + size + optional media type) to the caller.

**Read protocol:**
1. Derive the path from the hash in the `ContentRef`.
2. Read the file bytes.
3. Optionally verify the BLAKE3 hash of the read bytes against the stored hash (integrity check).

**Deduplication:** Two records with identical payloads share one object on disk. The reference count is implicit — an object is retained as long as any record in the event log references its hash. Garbage collection (removing unreferenced objects) is a future maintenance operation.

### SQLite Projection

Record metadata is projected into the existing per-brain `brain.db` SQLite database, alongside the tasks and notes tables. The projection is derived from the event log and fully rebuildable.

---

## Projection Tables

### records

Primary record metadata table.

```sql
CREATE TABLE records (
    record_id     TEXT PRIMARY KEY,
    title         TEXT NOT NULL,
    kind          TEXT NOT NULL,                  -- RecordKind value
    status        TEXT NOT NULL DEFAULT 'active'
                  CHECK (status IN ('active', 'archived')),
    description   TEXT,
    content_hash  TEXT NOT NULL,                  -- BLAKE3 hex (64 chars)
    content_size  INTEGER NOT NULL,               -- payload bytes
    media_type    TEXT,                           -- optional MIME hint
    task_id       TEXT,                           -- soft ref to tasks.task_id
    actor         TEXT NOT NULL,                  -- creator
    created_at    INTEGER NOT NULL,               -- unix seconds
    updated_at    INTEGER NOT NULL                -- unix seconds
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

1. Drop all FTS triggers on records tables (if any exist in future).
2. Open a single SQLite transaction.
3. DELETE all rows from `record_events`, `record_links`, `record_tags`, `records` (in FK-safe order).
4. Replay every event from the JSONL file in order, applying each event to the cleared tables.
5. Commit the transaction.
6. Rebuild any FTS indexes.
7. Re-create FTS triggers.

This is identical to the tasks domain rebuild pattern in `projections::rebuild()`.

### Invariants

- The event log is the only source of truth for record metadata. Never write directly to SQLite projection tables.
- Object payloads are written before their `RecordCreated` event is appended. If the process crashes between payload write and event append, the payload exists but no record references it — this is safe (the object is unreferenced, not corrupted).
- If the process crashes after event append but before the SQLite projection is updated, the rebuild from the event log recovers the correct state.
- Object store entries are immutable. Never overwrite an existing object at a given hash path.

### Crash Recovery

On startup, if the daemon detects that the SQLite `records` table is absent or the `schema_version` is below the records migration version, it triggers a full projection rebuild from the event log.

If the event log contains events for records whose objects are missing from the object store (e.g., the `objects/` directory was partially deleted), the projection rebuilds successfully but object reads for those records will fail at query time. This is surfaced as a retrieval error, not a startup failure.

---

## Content-Addressed Object Store

### BLAKE3 Hashing

BLAKE3 is already used in Brain for content hashing in the notes indexing pipeline. The records object store reuses this choice for consistency. BLAKE3 properties:

- 256-bit output (64 hex chars): collision probability < 10^-70 for any realistic corpus
- 3–4x faster than SHA-256 on modern CPUs
- Streaming and parallelizable

### Write Path

```
fn write_object(objects_dir: &Path, bytes: &[u8]) -> ContentRef {
    let hash = blake3::hash(bytes);
    let hex = hash.to_hex().to_string();            // 64 hex chars
    let prefix = &hex[..2];                          // e.g. "e3"
    let dir = objects_dir.join(prefix);
    fs::create_dir_all(&dir);
    let path = dir.join(&hex);
    if path.exists() {
        return ContentRef { hash: hex, size: bytes.len() as u64, media_type: None };
    }
    // Write to temp, rename for atomicity
    let tmp = dir.join(format!("{hex}.tmp"));
    fs::write(&tmp, bytes);
    fs::rename(&tmp, &path);
    ContentRef { hash: hex, size: bytes.len() as u64, media_type: None }
}
```

### Read Path

```
fn read_object(objects_dir: &Path, content_ref: &ContentRef) -> Result<Vec<u8>> {
    let hex = &content_ref.hash;
    let path = objects_dir.join(&hex[..2]).join(hex);
    let bytes = fs::read(&path)?;
    Ok(bytes)
}
```

### Optional Integrity Verification

For reads where integrity is critical (export, display to user), callers can verify:

```
let actual_hash = blake3::hash(&bytes).to_hex().to_string();
assert_eq!(actual_hash, content_ref.hash, "object integrity check failed");
```

This is opt-in to avoid slowing down bulk operations.

---

## CLI Surface (Planned)

The records domain will expose commands under `brain artifact` and `brain snapshot`.

### artifact subcommands

```
brain artifact create --title <title> --kind <kind> [--file <path>] [--stdin]
    Create a new artifact. Reads payload from file or stdin.
    Flags: --task <task_id>, --tag <tag>..., --description <text>

brain artifact list [--kind <kind>] [--tag <tag>] [--status active|archived]
    List records, filtered by kind, tag, or status.

brain artifact show <record_id>
    Show full metadata for a record.

brain artifact get <record_id> [--output <path>]
    Download the object payload to stdout or a file.

brain artifact archive <record_id> [--reason <text>]
    Mark a record as archived.

brain artifact tag <record_id> --add <tag>... --remove <tag>...
    Manage tags on an existing record.
```

### snapshot subcommands

```
brain snapshot save --title <title> [--file <path>] [--stdin]
    Save an opaque state bundle as a snapshot.
    Flags: --task <task_id>, --tag <tag>..., --description <text>

brain snapshot list [--tag <tag>] [--status active|archived]
    List snapshots.

brain snapshot show <record_id>
    Show metadata for a snapshot.

brain snapshot restore <record_id> [--output <path>]
    Write the snapshot bytes to stdout or a file.

brain snapshot archive <record_id>
    Mark a snapshot as archived.
```

---

## MCP Surface (Planned)

The records domain will expose tools via the MCP stdio JSON-RPC interface.

### records.create_artifact

Creates a new artifact with an inline or base64-encoded payload.

```
Tool: records.create_artifact
Input:
  title        string  (required)
  kind         string  (required) — report|diff|export|analysis|document
  content      string  (required) — raw text content or base64-encoded bytes
  encoding     string  (optional, default "text") — "text" | "base64"
  media_type   string  (optional)
  description  string  (optional)
  task_id      string  (optional) — link to originating task
  tags         string[] (optional)
Output:
  record_id, content_ref, created_at
```

### records.save_snapshot

Saves an opaque state bundle.

```
Tool: records.save_snapshot
Input:
  title      string   (required)
  content    string   (required) — base64-encoded bytes
  description string  (optional)
  task_id    string   (optional)
  tags       string[] (optional)
Output:
  record_id, content_ref, created_at
```

### records.get

Retrieves record metadata (without payload).

```
Tool: records.get
Input:
  record_id  string  (required)
Output:
  record metadata, content_ref, tags, links
```

### records.fetch_content

Retrieves the object payload for a record.

```
Tool: records.fetch_content
Input:
  record_id  string  (required)
  encoding   string  (optional, default "text") — "text" | "base64"
Output:
  content, content_ref, encoding
```

### records.list

Lists records with optional filters.

```
Tool: records.list
Input:
  kind    string   (optional)
  tag     string   (optional)
  status  string   (optional, default "active") — "active" | "archived" | "all"
  limit   integer  (optional, default 20)
Output:
  records[]  — array of metadata stubs (no payloads)
```

### records.archive

Archives a record.

```
Tool: records.archive
Input:
  record_id  string  (required)
  reason     string  (optional)
Output:
  record_id, status: "archived", updated_at
```

---

## Open Questions and Future Work

### Compression

Object store entries are currently stored uncompressed. For large text artifacts (reports, exports), gzip or zstd compression could significantly reduce disk footprint. If compression is added, the `ContentRef` should gain an `encoding` field (`none | gzip | zstd`) so readers know how to decompress. The hash should be computed over the raw (pre-compression) bytes to preserve deduplication semantics.

### Retention and Garbage Collection

When records are archived, their objects remain in the object store. Over time, unreferenced or archived objects may accumulate. A `brain records gc` command could scan the object store, identify objects not referenced by any active record in the event log, and optionally remove them. This requires care: the event log is the authoritative reference set, so GC must scan the full log (or a derived index of `content_hash` values) before deleting anything.

### Integrity Verification

A `brain records verify` command could walk the object store and check each object's BLAKE3 hash against its filename, detecting bit-rot or accidental corruption. For large stores, this should run incrementally (with resume support via a cursor file).

### Record Search

Records are not currently indexed in the LanceDB vector store. Future work could embed artifact content (text artifacts only) using the same BGE-small embedder pipeline used for notes, making artifacts retrievable via `memory.search_minimal`. This would require a `records` source in the hybrid ranker candidate pool and metadata fields compatible with the existing chunk stub format.

### Cross-Brain References

Following the same pattern as task cross-brain references, record links could gain an optional `brain` field to support cross-brain soft references to tasks or chunks in other brains. `NULL` = local (zero-cost common case), non-NULL = cross-brain soft reference.

### Schema Migration

The records tables will be introduced in a new schema migration (v12 or later). The migration adds the four tables (`records`, `record_tags`, `record_links`, `record_events`) and indexes. On first open after upgrade, the daemon runs a projection rebuild from the records event log (which will be empty for existing installations, making the rebuild a no-op).
