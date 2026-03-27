---
description: Two-phase reflection — retrieve source material then store synthesis
allowed-tools: "mcp__brain__*"
---

Two-phase episodic reflection workflow.

**Prepare mode** (default): Retrieve source material for a topic.

Use `memory_reflect` with:
- `topic` (required): The subject to reflect on
- `budget_tokens`: Token budget for source material (default: 4000)
- `brains`: Array of brain names to search across

Returns episodes and related chunks suitable for synthesis.

**Commit mode**: Store a synthesized reflection linked to its sources.

Use `memory_reflect` with `mode: "commit"` and:
- `title` (required): Reflection title
- `content` (required): The synthesized reflection text
- `source_ids` (required): Array of episode/summary IDs used as sources
- `tags`: Optional topic tags
- `importance`: Score from 0.0 to 1.0

**Typical workflow:**
1. `/mem:reflect <topic>` — retrieve source material (prepare mode)
2. Synthesize the material into insights
3. `/mem:reflect --commit` — store the synthesis with source links (commit mode)
