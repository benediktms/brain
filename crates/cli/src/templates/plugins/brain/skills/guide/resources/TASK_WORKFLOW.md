# Task Workflow

## Task Lifecycle

```
open → in_progress → done
  ↓        ↓
blocked  cancelled
```

## Status Transitions

- **open**: Task is created, not yet started
- **in_progress**: Actively being worked on (claim with `tasks_apply_event`)
- **blocked**: Cannot proceed — set `blocked_reason` or add blocking dependency
- **done**: Work complete (use `tasks_close`)
- **cancelled**: Abandoned (use `tasks_apply_event` with status_changed)

## Dependencies

Use `tasks_deps_batch` for dependency management:
- **add**: Task A depends on Task B (A is blocked until B is done)
- **chain**: Sequential: A → B → C (each depends on the previous)
- **fan**: Multiple tasks depend on one: B,C,D all depend on A
- **clear**: Remove all dependencies from a task

When a dependency is closed, dependent tasks automatically become unblocked.

## Task Types

- **task**: General work item
- **bug**: Defect to fix
- **feature**: New functionality
- **epic**: Large initiative with subtasks (use `parent` field)
- **spike**: Research/investigation with time-box

## Priority Scale

| Priority | Level | Use For |
|----------|-------|---------|
| 0 | Critical | Production incidents, blockers |
| 1 | High | Current sprint priorities |
| 2 | Medium | Important but not urgent |
| 3 | Low | Nice to have |
| 4 | Backlog | Future consideration |

## Label Taxonomy

Three dimensions, max 3 labels per task:

**Area** (what part): `area:memory`, `area:tasks`, `area:records`, `area:cli`, `area:mcp`, `area:index`, `area:infra`, `area:core`

**Type** (what kind): `type:feature`, `type:refactor`, `type:bugfix`, `type:test`, `type:perf`, `type:docs`

**Phase** (where in lifecycle): `phase:design`, `phase:polish`
