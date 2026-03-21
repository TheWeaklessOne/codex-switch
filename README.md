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

## Safety Model

`codex-switch` is intentionally conservative:

- every identity gets its own isolated managed `CODEX_HOME`
- same-thread continuity shares only `sessions/`, not a full home
- one active writer per thread is enforced with lease files
- handoff confirmation requires persisted thread history to advance
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
