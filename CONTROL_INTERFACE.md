# Control Interface

This document defines the supported external control surface for `codex-switch`.

For the target contract needed by a full external session orchestrator, see [SESSION_CONTROL_INTERFACE.md](./SESSION_CONTROL_INTERFACE.md).

The goal is to give another application a clear answer to three questions:

- which entry points are intended for automation
- which parts of the runtime layout are internal implementation details
- which outputs and state names can be treated as stable today

## Scope

`codex-switch` currently exposes three practical integration modes:

1. Async task orchestration through `projects`, `jobs`, `tasks`, and `scheduler`
2. Identity-scoped passthrough to `codex app-server` through `codex-switch app-server`
3. Machine-facing session orchestration through `sessions`, `turns`, and `handoffs`

The following are not public APIs and must not be read or written directly by external applications:

- the managed runtime root, including `registry.json`
- `shared/*.json`
- `shared/scheduler/scheduler.db`
- `shared/task-artifacts/`
- internal Rust modules, structs, or SQLite schema details

Those files may change for implementation reasons without preserving machine-consumer compatibility.

## Compatibility

This control surface is validated primarily against `codex-cli 0.115.0`.

Current compatibility commitments for machine consumers:

- documented command names and flag meanings are the public surface
- exit code `0` means success; any non-zero exit code means failure
- unless a command section below says otherwise, stdout is operator-oriented and is not a structured protocol
- stderr and human-readable error text are not parse-stable
- internal runtime storage versions are not the public interface version

The machine-facing `sessions`, `turns`, and `handoffs` families now define a versioned `--json`
contract. Existing operator-oriented commands remain human-facing unless explicitly documented
otherwise.

## Recommended Integration Mode A: Async Task Orchestration

Use this mode when another application wants to:

- submit repository work
- poll for status
- inspect logs or dispatch decisions
- issue follow-up, retry, or cancel operations

### Supported commands

- `codex-switch projects add`
- `codex-switch projects list`
- `codex-switch projects show`
- `codex-switch jobs run`
- `codex-switch jobs list`
- `codex-switch jobs status`
- `codex-switch jobs show`
- `codex-switch jobs logs`
- `codex-switch jobs explain`
- `codex-switch jobs follow-up`
- `codex-switch jobs cancel`
- `codex-switch jobs retry`
- `codex-switch tasks submit`
- `codex-switch tasks list`
- `codex-switch tasks status`
- `codex-switch tasks show`
- `codex-switch tasks logs`
- `codex-switch tasks explain`
- `codex-switch tasks follow-up`
- `codex-switch tasks cancel`
- `codex-switch tasks retry`
- `codex-switch scheduler enable`
- `codex-switch scheduler disable`
- `codex-switch scheduler health`
- `codex-switch scheduler tick --once`
- `codex-switch scheduler run`
- `codex-switch scheduler gc`

### Rollout gate

Scheduler-backed submission and retry operations are gated by `scheduler_v1`.

- `codex-switch scheduler enable` enables the rollout gate
- `jobs run`, `tasks submit`, `tasks follow-up`, and `tasks retry` require the gate to be enabled
- read-only inspection commands remain available when the gate is disabled

### Stable stdout fields

Because `--json` is not available yet, only the labeled identifier and status lines below are parse-stable for machine consumers on the task surface.

`projects add`

- `project id: <project-id>`

`jobs run`

- `project id: <project-id>`
- `task id: <task-id>`
- `run id: <run-id>`

`tasks submit`

- `task id: <task-id>`
- `run id: <run-id>`

`tasks follow-up`

- `run id: <run-id>`

`tasks retry`

- `run id: <run-id>`

`tasks status`

- `task id: <task-id>`
- `status: <task-status>`
- `current thread: <thread-id|none>`
- `last identity: <identity-id|none>`
- `run <run-id> seq=<n> kind=<run-kind> status=<task-run-status>`

`tasks show`

- same contract as `tasks status`

`jobs status` and `jobs show`

- same contract as `tasks status`

`jobs follow-up`

- same contract as `tasks follow-up`

`jobs retry`

- same contract as `tasks retry`

All other stdout content on the task surface should be treated as human-readable diagnostics.

### Task lifecycle vocabulary

Task statuses:

- `queued`
- `running`
- `awaiting_followup`
- `completed`
- `failed_retryable`
- `failed_terminal`
- `canceled`
- `orphaned`

Run kinds:

- `initial`
- `follow_up`
- `retry`

Run statuses:

- `pending_assignment`
- `assigned`
- `launching`
- `running`
- `completed`
- `failed`
- `timed_out`
- `handoff_pending`
- `abandoned`
- `canceled`
- `orphaned`

Launch modes:

- `new_thread`
- `resume_same_identity`
- `resume_handoff`
- `resume_checkpoint`

Failure kinds that may appear in run records and diagnostics:

- `launch`
- `runtime`
- `retryable_auth`
- `retryable_rate_limit`
- `handoff`
- `checkpoint`
- `timeout`
- `worker_exited`
- `worker_spawn`
- `canceled`
- `validation`

### Polling model

Recommended flow for an external application:

1. Enable the scheduler rollout gate with `codex-switch scheduler enable`.
2. Submit a workspace-scoped job with `codex-switch jobs run ...`, or submit to a named project with `codex-switch tasks submit ...`.
3. Capture `task id` and initial `run id` from stdout.
4. Poll `codex-switch tasks status <task-id>` until the task reaches a non-active state relevant to your workflow.
5. Use `codex-switch tasks logs <task-id>` for opaque diagnostics or `codex-switch tasks explain <task-id>` for operator-facing dispatch reasoning.
6. Use `codex-switch tasks follow-up <task-id>`, `codex-switch tasks retry <task-id>`, or `codex-switch tasks cancel <task-id>` to continue or control the lineage.

### Jobs vs tasks

Use `jobs` when the caller only has a workspace path and does not want to register a named project up front.

Use `tasks` when the caller already manages named projects and wants explicit project identifiers.

### Logs and explain output

`tasks logs` and `jobs logs` are intended for diagnostics, not structured parsing.

Current behavior:

- scheduler events are printed as plain text lines
- if a per-run artifact stream exists, the command also prints the path and then the raw event stream content

External applications should display or store this output as opaque text rather than parse it as a stable protocol.

`tasks explain` and `jobs explain` are also diagnostic surfaces and should be treated as human-readable only.

## Recommended Integration Mode B: App Server Passthrough

Use this mode when another application already speaks the Codex App Server JSON-RPC protocol and needs `codex-switch` only for identity management.

Command form:

```bash
codex-switch app-server --identity "Personal Plus" -- --listen stdio://
```

Automatic selection is also supported:

```bash
codex-switch app-server --auto -- --listen stdio://
```

Behavior:

- `codex-switch` resolves the target identity
- `codex-switch` sets `CODEX_HOME` to that managed identity home
- `codex-switch` launches `codex app-server ...`
- `codex-switch` does not add a new RPC schema on top of the underlying App Server protocol

This is the right entry point when the integrator needs:

- thread and turn control
- live event streaming
- direct use of the underlying Codex App Server protocol

This is not the right entry point when the integrator wants:

- queued asynchronous jobs
- task lineage management

## Recommended Integration Mode C: Machine-Facing Session Orchestration

Use this mode when another application needs a durable logical conversation mapped to a canonical
Codex thread and wants structured turn, handoff, and continuity state without parsing operator
text.

Supported commands:

- `codex-switch sessions start --json`
- `codex-switch sessions resume --json`
- `codex-switch sessions show --json`
- `codex-switch sessions list --json`
- `codex-switch sessions stream --json`
- `codex-switch sessions cancel --json`
- `codex-switch turns start --json`
- `codex-switch turns show --json`
- `codex-switch turns wait --json`
- `codex-switch turns cancel --json`
- `codex-switch handoffs prepare --json`
- `codex-switch handoffs accept --json`
- `codex-switch handoffs confirm --json`
- `codex-switch handoffs show --json`

Success responses use the envelope:

```json
{
  "interface_version": "1",
  "ok": true,
  "command": "sessions.start",
  "data": {}
}
```

Failures use the envelope:

```json
{
  "interface_version": "1",
  "ok": false,
  "command": "turns.start",
  "error": {
    "code": "unsafe_same_thread_resume",
    "message": "human readable message",
    "retryable": false,
    "details": {}
  }
}
```

Structured streaming:

- `codex-switch sessions stream --json` emits NDJSON, one event per line
- each line includes `interface_version`, `event`, `session_id`, `thread_id`, `turn_id` when
  applicable, `timestamp`, and `payload`
- stream output is not wrapped in the success envelope because it is an event stream, not a single
  response document

Stable error codes exposed by this surface:

- `identity_not_found`
- `no_selectable_identity`
- `session_not_found`
- `turn_not_found`
- `handoff_not_found`
- `thread_lease_conflict`
- `turn_already_active`
- `handoff_pending`
- `unsafe_same_thread_resume`
- `checkpoint_fallback_required`
- `runtime_unavailable`
- `rpc_timeout`
- `rpc_server_error`
- `validation_error`
- `scheduler_disabled`
- `workspace_unavailable`

For the resource model, state vocabulary, and recommended flows on this machine-facing surface, see
[SESSION_CONTROL_INTERFACE.md](./SESSION_CONTROL_INTERFACE.md).
- scheduler-managed follow-up and retry behavior

## Operator-Oriented Commands

The following commands remain supported for operators, but their stdout should be treated as human-readable and not as the primary machine interface:

- `codex-switch accounts`
- `codex-switch status`
- `codex-switch select`
- `codex-switch continue`
- `codex-switch threads inspect`
- `codex-switch threads lease ...`
- `codex-switch threads handoff ...`
- `codex-switch threads state`

These commands are useful for manual intervention and debugging, but they are not the recommended control plane for a separate application.

## Error Contract

The only stable transport-level error contract today is:

- success returns exit code `0`
- failure returns a non-zero exit code

Machine consumers must not branch on exact English error messages.

Callers should expect these high-level failure classes:

- scheduler rollout disabled for submission or retry operations
- missing or conflicting prompt input
- unknown project, task, or run identifier
- ambiguous workspace-to-project mapping for workspace-scoped job commands
- underlying Codex launch or runtime failures

For task submission and state mutations, durable scheduler state is recorded through SQLite transactions before the command reports success.

## Versioning and Change Management

This file is the normative definition of the public control interface for this repository.

Breaking changes to any of the following require updating this file in the same change:

- command names documented here
- flag meanings documented here
- parse-stable stdout fields documented here
- lifecycle vocabulary documented here

Future structured `--json` modes may extend this interface. When they are added, they should be documented here instead of requiring integrators to infer behavior from implementation details.
