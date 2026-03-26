---
description: Cluster and summarize recent episodes by temporal proximity
argument-hint: [--limit] [--gap-seconds]
---

Consolidate recent memory episodes into clusters based on temporal proximity.

Use the `memory_consolidate` tool with:
- `limit`: Maximum number of episodes to consider (default: 50)
- `gap_seconds`: Time gap threshold for clustering (default: 3600 = 1 hour)
- `auto_summarize`: Whether to auto-generate cluster summaries (default: false)

Episodes recorded close together in time are grouped into clusters. This helps reduce noise and surface patterns across multiple related episodes.

Use this periodically on long-running projects to keep memory organized and discoverable.
