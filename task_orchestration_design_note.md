# Task Orchestration Design Note

The previously documented rollout and maintenance deviations from
`task_orchestration_plan.md` have been resolved.

## Current operational model

1. `scheduler_v1` rollout gate

`Task Orchestration` is now disabled by default through a durable scheduler control row in
`shared/scheduler/scheduler.db`. Operators enable it explicitly with
`codex-switch scheduler enable` and can disable new dispatches again with
`codex-switch scheduler disable`.

2. Independent maintenance cadence

The long-lived scheduler loop now runs dispatch on `scheduler_poll_interval`, quota refresh on
`quota_refresh_interval`, and workspace cleanup on `gc_interval`. Quota refresh and cleanup status
are persisted for operator diagnostics and surfaced through `scheduler health`.

3. Git-aware worktree cleanup

Scheduler GC now routes cleanup through `WorktreeManager`, which removes Git-backed worktrees with
`git worktree remove --force` and removes copied workspaces with direct directory deletion.
