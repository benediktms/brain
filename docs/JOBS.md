# Generic Jobs Table

## Overview

The `jobs` table is a generic, priority-aware background job queue backed by SQLite. It replaces the implicit "poll for `embedded_at IS NULL`" pattern with an explicit, observable system that supports deduplication, retry with backoff, crash recovery, and priority ordering.

## Motivation

The previous approach polled for stale rows every 10 seconds (`WHERE embedded_at IS NULL OR updated_at > embedded_at`). This worked but had limitations:

- **No observability** — answering "how many items are pending?" required running the stale-row query
- **No priority** — self-heal re-embeds competed equally with fresh writes
- **No retry tracking** — failures were invisible; items just stayed NULL and got retried silently forever
- **Scales linearly with object types** — each new embeddable type (episodes, reflections, procedures) would require a new `poll_stale_X()` function

## Schema

```sql
CREATE TABLE jobs (
    job_id        TEXT PRIMARY KEY,        -- ULID, generated at enqueue time
    kind          TEXT NOT NULL,            -- e.g. 'embed_task', 'embed_chunk'
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'running', 'completed', 'failed', 'dead')),
    brain_id      TEXT NOT NULL DEFAULT '',  -- matches convention in tasks/records tables
    ref_id        TEXT,                      -- the object this job acts on (task_id, chunk_id, etc.)
    ref_kind      TEXT,                      -- type hint for ref_id ('task', 'chunk', etc.)
    priority      INTEGER NOT NULL DEFAULT 100,  -- lower = higher priority
    payload       TEXT NOT NULL DEFAULT '{}',    -- JSON blob for job-specific parameters
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 3,
    last_error    TEXT,                      -- most recent error message on failure
    created_at    INTEGER NOT NULL,          -- unix timestamp
    scheduled_at  INTEGER NOT NULL,          -- earliest time this job may run
    started_at    INTEGER,                   -- when the worker claimed this job
    completed_at  INTEGER,                   -- when it finished
    updated_at    INTEGER NOT NULL           -- last status change
);
```

### Indexes

| Index | Purpose |
|-------|---------|
| `idx_jobs_poll` | Primary claim query: `(status, priority, scheduled_at)` |
| `idx_jobs_brain_status` | Brain-scoped queries: `(brain_id, status)` |
| `idx_jobs_dedup` | Partial unique index: `(kind, ref_id) WHERE status IN ('pending', 'running')` |

### Priority Levels

| Value | Name | Use Case |
|-------|------|----------|
| 0 | Critical | Emergency / system operations |
| 50 | Self-heal | LanceDB recovery re-embed |
| 100 | Normal | Regular embedding from mutations |
| 200 | Background | Maintenance, optimization |

## State Machine

```
pending ──[claim]──> running ──[success]──> completed
   ^                    |
   |              [failure, retries left]
   +────────────────────+
                        |
                  [failure, no retries]
                        |
                        v
                     failed ──[gc sweep]──> dead
```

### Transitions

| From | To | Trigger |
|------|----|---------|
| `pending` | `running` | Atomic claim query; increments `attempts`, sets `started_at` |
| `running` | `completed` | Worker success; sets `completed_at` |
| `running` | `pending` | Transient failure + retries remaining; sets `scheduled_at = now + backoff` |
| `running` | `failed` | Permanent failure or `attempts >= max_attempts` |
| `running` | `pending` | Reaper: stuck > 5min (crash recovery) |
| `completed`/`dead` | _(deleted)_ | GC sweep after 7 days |

### Retry Backoff

`min(30 * 2^(attempts-1), 3600)` seconds — 30s, 60s, 120s, 240s, ... capped at 1 hour.

## Claim Query

Uses atomic `UPDATE ... RETURNING` (SQLite 3.35+) under the existing single-writer mutex:

```sql
UPDATE jobs
SET status = 'running', started_at = ?1, attempts = attempts + 1, updated_at = ?1
WHERE job_id IN (
    SELECT job_id FROM jobs
    WHERE status = 'pending' AND scheduled_at <= ?1 AND kind = ?2
    ORDER BY priority ASC, scheduled_at ASC
    LIMIT ?3
)
RETURNING job_id, kind, brain_id, ref_id, ref_kind, payload, attempts;
```

## Deduplication

The partial unique index prevents duplicate active jobs:

```sql
CREATE UNIQUE INDEX idx_jobs_dedup ON jobs(kind, ref_id)
    WHERE status IN ('pending', 'running');
```

Enqueue uses `ON CONFLICT ... DO UPDATE SET priority = MIN(...)` to upgrade priority if a more urgent request arrives for the same object. Once a job completes/fails, a new job for the same `(kind, ref_id)` can be created.

## Integration

### Daemon Loop (`watch.rs`)

The `embed_poll_interval` tick (every 10s) now:
1. **Reaps stuck jobs** — resets `running` jobs with `started_at > 5min ago` back to `pending`
2. **Enqueues stale items** — scans for `embedded_at IS NULL` rows and creates jobs (bridge from old pattern)
3. **Claims and processes** — claims up to 256 jobs per kind, batch-embeds, upserts to LanceDB

### Self-Heal

When LanceDB is detected as missing/inaccessible, the system bulk-enqueues re-embed jobs at priority 50 (above normal 100) using `INSERT ... SELECT ... ON CONFLICT DO NOTHING`. This is idempotent — existing active jobs are not duplicated.

### Backward Compatibility

- `embedded_at` columns remain on `tasks` and `chunks` as materialized read-path flags
- The `poll_stale_tasks()` / `poll_stale_chunks()` APIs are preserved as wrappers that auto-enqueue jobs for stale items, then delegate to the job-based processor
- No changes to MCP handlers or external APIs

## Observability

```sql
-- Pending job counts by kind
SELECT kind, COUNT(*) FROM jobs WHERE status = 'pending' GROUP BY kind;

-- Failed jobs with errors
SELECT job_id, kind, ref_id, attempts, last_error, updated_at
FROM jobs WHERE status = 'failed' ORDER BY updated_at DESC LIMIT 50;

-- Stuck jobs (reaper candidates)
SELECT job_id, kind, ref_id, started_at
FROM jobs WHERE status = 'running' AND started_at < strftime('%s', 'now') - 300;
```

## Rust Module Structure

| File | Purpose |
|------|---------|
| `crates/brain_lib/src/db/jobs.rs` | CRUD: `enqueue_job()`, `claim_jobs()`, `complete_job()`, `fail_job()`, `reap_stuck_jobs()`, `gc_old_jobs()` |
| `crates/brain_lib/src/db/migrations/v21_to_v22.rs` | Migration DDL creating the `jobs` table and indexes |
| `crates/brain_lib/src/pipeline/embed_poll.rs` | Job-based embedding pipeline (claims jobs, processes, completes) |

## GC / Retention

Completed and dead jobs are deleted after 7 days:

```sql
DELETE FROM jobs WHERE status IN ('completed', 'dead') AND completed_at < now - 604800;
```
