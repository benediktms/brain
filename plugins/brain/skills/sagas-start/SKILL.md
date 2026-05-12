---
description: Transition a saga from planning to open
allowed-tools: "mcp__brain__*"
---

Start a saga, moving it from `planning` to `open`.

Use `mcp__brain__sagas_start` with:
- `saga_id` (required): `saga-<hex>` short form (3+ lowercase hex chars) or bare 26-char ULID
- `actor` (optional): who is starting the saga, defaults to `mcp`

Only `planning` sagas can be started. Calling on an already open, closed, or cancelled saga returns an error.

Use this skill when the scope is finalised and execution should begin. The `planning` status exists precisely to allow shaping membership (via `/brain:sagas-add-tasks`) before committing to active work. Once open, use `/brain:sagas-frontier` to find the first actionable tasks.
