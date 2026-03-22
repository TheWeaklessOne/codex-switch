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

4. Dispatch and continuity model

The scheduler is designed for parallel work across multiple projects and multiple independent task
lineages.

- New unrelated tasks prefer free identities first, then rank eligible identities by quota
  headroom and configured priority.
- Identity occupancy is determined by durable account leases in SQLite, not by the current manual
  selection state.
- Follow-up and retry runs prefer affinity to the prior identity for the task lineage so they can
  resume the same thread when possible.
- Cross-identity follow-up uses explicit thread handoff when safe and falls back to checkpoint
  continuation when same-thread continuity is not safe.
- Active runs hold exclusive worktree leases so scheduler-managed worktrees are never shared across
  concurrent runs.
- `jobs run` is a scheduler-backed ad-hoc entry point that resolves the current directory or an
  explicit `--workspace` into a deterministic workspace project, so operators can use orchestration
  without manually registering a project first.
