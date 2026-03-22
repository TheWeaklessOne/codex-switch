# codex-switch

`codex-switch` is a production-oriented CLI for running multiple isolated Codex identities on one machine, choosing the best available account for new work, and safely handing a shared thread from one identity to another.

It is designed around isolated `CODEX_HOME` roots per identity, not around rewriting one global `~/.codex` in place.

## What It Does

- keeps one managed `CODEX_HOME` per identity
- shares `sessions/` when same-thread continuity is needed
- persists quota state, selection policy, health penalties, and selection history
- supports manual selection and automatic quota-aware selection
- supports safe thread leases, handoff, and checkpoint fallback
- can wrap `codex exec` and `codex app-server`
- can optionally retry `exec` on retryable auth/rate-limit failures with another healthy identity
- supports probe-gated ChatGPT workspace forcing for identities that have been validated locally
- supports durable task orchestration across multiple projects with SQLite-backed scheduler state
- spreads unrelated scheduler-managed tasks across free identities when possible to reduce quota coupling
- supports ad-hoc scheduler-backed jobs from the current workspace without manually registering a project first
- tracks account leases and worktree leases independently from operator-facing selection state
- runs scheduler-managed tasks through the App Server runtime and preserves thread continuity when possible

## Requirements

- Rust toolchain
- `codex` CLI installed locally
- validated primarily against `codex-cli 0.115.0`
- macOS or Linux-style filesystem semantics are assumed for managed home layout and atomic file replacement

## Install

Build and install globally:

```bash
cargo install --path . --force
```

Then use it from anywhere:

```bash
codex-switch --help
```

## Quick Start

Register and log in a ChatGPT-backed identity:

```bash
codex-switch identities add chatgpt --name "Account 1" --login
```

Register and log in an API-key-backed identity:

```bash
codex-switch identities add api --name "API Fallback" --env-var OPENAI_API_KEY --login --no-verify
```

Inspect current accounts and quotas:

```bash
codex-switch accounts
```

Pick the best currently eligible identity:

```bash
codex-switch select --auto
```

Launch Codex under the current best identity:

```bash
codex-switch exec -- --full-auto
```

Retry `exec` with another identity when a launch fails with a retryable auth or rate-limit error:

```bash
codex-switch exec --auto-failover -- --full-auto
```

## Core Commands

Identity lifecycle:

```bash
codex-switch identities add chatgpt --name "Personal Plus" --login
codex-switch identities add api --name "API Fallback" --env-var OPENAI_API_KEY --login --no-verify
codex-switch identities list
codex-switch identities login "Personal Plus"
codex-switch identities verify "Personal Plus"
codex-switch identities remove "Personal Plus"
codex-switch identities disable "Personal Plus"
codex-switch identities enable "Personal Plus"
codex-switch identities health show
codex-switch identities health clear "Personal Plus"
```

Selection and status:

```bash
codex-switch accounts
codex-switch status
codex-switch select
codex-switch select "Personal Plus"
codex-switch select --auto
codex-switch policy show
codex-switch policy set --warning 85 --avoid 95 --hard-stop 100 --rate-limit-cooldown 1800 --auth-failure-cooldown 21600
```

Execution:

```bash
codex-switch exec -- --full-auto
codex-switch exec --auto-failover -- --full-auto
codex-switch app-server --identity "Personal Plus" -- --listen stdio://
```

Thread continuation and handoff:

```bash
codex-switch continue --thread <thread-id> --to "Backup Workspace"
codex-switch continue --thread <thread-id> --auto
codex-switch threads inspect <thread-id> --identity "Personal Plus"
codex-switch threads lease acquire <thread-id> --identity "Personal Plus"
codex-switch threads lease show <thread-id>
codex-switch threads handoff prepare <thread-id> --from "Source" --to "Target" --lease-token <token> --reason quota
codex-switch threads handoff accept <thread-id> --to "Target" --lease-token <pending-token>
codex-switch threads handoff confirm <thread-id> --to "Target" --lease-token <active-token> --observed-turn-id <turn-id>
codex-switch threads state <thread-id>
```

Workspace forcing:

```bash
codex-switch identities workspace-force show "Personal Plus"
codex-switch identities workspace-force probe "Personal Plus"
codex-switch identities workspace-force set "Personal Plus" --status passed --notes "Operator override"
```

Task orchestration:

```bash
codex-switch projects add --name repo-a --repo-root /path/to/repo-a --execution-mode git-worktree
codex-switch projects add --name repo-b --repo-root /path/to/repo-b --execution-mode copy
codex-switch scheduler enable
codex-switch jobs run --title "Quick refactor" --prompt "Clean up the retry path"
codex-switch tasks submit --project repo-a --title "Refactor scheduler" --prompt "Implement durable leases"
codex-switch tasks submit --project repo-b --title "Fix flaky tests" --prompt "Stabilize the suite"
codex-switch jobs follow-up <task-id> --prompt "Continue from the last review comments"
codex-switch tasks follow-up <task-id> --prompt "Address the failing integration test"
codex-switch tasks list
codex-switch tasks show <task-id>
codex-switch tasks explain <task-id>
codex-switch scheduler tick --once
codex-switch scheduler health
codex-switch scheduler run
codex-switch scheduler gc
codex-switch scheduler reset-state
codex-switch scheduler disable
```

Task orchestration is built for the multi-project, multi-task workflow where several unrelated jobs can run in parallel and should consume quota independently when possible.

- New independent tasks are ranked toward free identities first, then by remaining quota headroom and identity priority.
- Account occupancy is tracked with durable SQLite account leases, not with the current manual selection.
- By default an identity gets at most one active scheduler-managed task at a time, so new unrelated tasks spread across available accounts before sharing one.
- Follow-up and retry runs prefer the same identity as the previous successful run so they can resume the same thread directly.
- If the preferred identity is not available, follow-up work can move to another identity through safe thread handoff.
- If same-thread continuity is unsafe across identities, the scheduler falls back to checkpoint-based continuation instead of forcing an unsafe resume.
- Every active run gets its own leased worktree, and scheduler-managed worktrees are never shared concurrently.
- `jobs run` is the projectless entry point: it resolves the current directory or `--workspace` to a deterministic workspace project, reuses it when one already exists, and then submits the job through the same scheduler/task machinery.

Example workflow:

1. You register three projects and submit three unrelated tasks.
2. The scheduler dispatches them to three different free identities when capacity exists, for example accounts 1, 2, and 5.
3. One task finishes, and a brand-new unrelated task is submitted later. The scheduler treats it as a fresh lineage and can send it to a different free identity, for example account 4.
4. Another task finishes, and you submit a follow-up. The scheduler first tries to keep it on the same identity and same thread. If that identity is unavailable, it can route the follow-up to another free identity through handoff or checkpoint fallback.
5. A third task gets a follow-up while its previous identity is free, so the scheduler reuses that identity and resumes in the same thread.

If you do not want to register a named project first, you can stay in your repository directory and use:

```bash
codex-switch scheduler enable
codex-switch jobs run --title "Investigate CI flake" --prompt "Reproduce and fix the intermittent failure"
codex-switch jobs list
codex-switch jobs follow-up <task-id> --prompt "Address review feedback"
```

`jobs run` detects whether the workspace is inside a Git repository. Git workspaces default to `git-worktree`; non-Git directories default to copied workspaces.

The scheduler rollout gate `scheduler_v1` is disabled by default. Use `codex-switch scheduler enable` before submitting or retrying scheduler-managed tasks. Read-only inspection commands remain available when the rollout gate is off.

`codex-switch tasks cancel <task-id>` is a hard cancel for scheduler-managed work: it durably marks active runs as canceled, releases account/worktree leases, and then best-effort interrupts the worker process group. Use `codex-switch scheduler reset-state` only as an operator recovery step when you intentionally want to clear the task orchestration bounded context.

## Runtime Layout

By default, state is stored under `~/.telex-codex-switcher` for backward compatibility with the existing managed runtime layout:

- `registry.json`: registered identities
- `homes/<identity-id>/`: isolated `CODEX_HOME` roots
- `shared/sessions/`: shared session store used for same-thread continuity
- `shared/quota-status.json`: cached quota state
- `shared/selection-policy.json`: threshold and cooldown policy
- `shared/identity-health.json`: manual-disable and penalty state
- `shared/selection-state.json`: current manual or automatic selection
- `shared/selection-events/`: append-only automatic decision log
- `shared/thread-leases/`: single-writer thread lease files
- `shared/turn-states/`: tracked handoff state
- `shared/task-checkpoints/`: checkpoint fallback artifacts
- `shared/scheduler/scheduler.db`: SQLite WAL-backed source of truth for projects, tasks, runs, leases, and dispatch decisions
- `shared/task-artifacts/`: per-run prompts, event streams, and thread snapshots
- `shared/task-worktrees/`: scheduler-managed per-run workspaces and reusable follow-up worktrees

## Safety Model

`codex-switch` is intentionally conservative:

- every identity gets its own isolated managed `CODEX_HOME`
- same-thread continuity shares only `sessions/`, not a full home
- one active writer per thread is enforced with lease files
- scheduler account occupancy is enforced with durable SQLite account leases, not with the current manual selection
- handoff confirmation requires persisted thread history to advance
- scheduler-managed worktrees are never shared across concurrent active runs
- Git-backed scheduler worktrees are cleaned up through `git worktree remove --force`, not by deleting the directory directly
- workspace forcing is only enabled automatically after a recorded successful probe
- state mutations prefer atomic file replacement and rollback on partial failure

## Helper Script

`tools/codex_identity.py` is included as a compatibility and operator utility for the same managed runtime layout. The Rust CLI is the primary implementation; the Python helper is retained because it can still be useful for debugging and migration workflows.

## Development

Run the main checks locally:

```bash
cargo fmt
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under [Apache-2.0](./LICENSE).
