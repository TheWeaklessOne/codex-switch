# Session Control Interface

This document specifies the target machine-readable control contract for external applications that need full session orchestration on top of `codex-switch`.

It is intentionally broader than the currently supported interface in [CONTROL_INTERFACE.md](./CONTROL_INTERFACE.md).

Status of this document:

- shipped interface version `1`
- implemented by `codex-switch sessions|turns|handoffs`
- intended for integrations that require same-thread continuity, safe handoff, and non-fragile bot control

## Why This Exists

The current task-oriented surface is sufficient for queued async work.

It is not sufficient for an external system that needs all of the following:

- one logical conversation mapped to one long-lived Codex session
- reliable capture of `thread_id` and `turn_id`
- explicit `turn` lifecycle tracking
- safe same-thread resume across time
- controlled identity switching and handoff
- stable `/status` and `/continue` semantics
- machine-readable state that does not depend on parsing operator text

For that use case, the external application is no longer just a relay. It becomes a session orchestrator.

## Design Goals

- expose a stable control layer for session, thread, turn, and handoff operations
- separate human-readable streaming from machine-readable control state
- preserve backward compatibility for operator commands such as `select`, `continue`, and `threads`
- align the public contract with the existing internal App Server runtime model
- make same-thread continuity safe by default, not best-effort text parsing

## Non-Goals

- replacing the underlying Codex App Server protocol
- exposing internal SQLite schema as a supported interface
- making every current operator command parse-stable
- freezing every diagnostic string or event payload emitted by internal runtime components

## Recommended Public Model

External applications should treat `codex-switch` as exposing the following resources:

- `identity`
- `session`
- `turn`
- `handoff`
- `lease`

Recommended meanings:

- `identity`: a managed Codex identity that can own work
- `session`: the external application's logical conversation or topic
- `thread`: the canonical Codex thread bound to that session
- `turn`: one unit of model work within a session
- `handoff`: an explicit identity transition for continued work on the same session
- `lease`: the single-writer guarantee that protects same-thread safety

## Public Resource Semantics

### Session

A session is the stable external handle.

A session should be able to answer:

- which identity currently owns the session
- which Codex thread backs the session
- whether a turn is active right now
- whether same-thread continuation is safe
- whether a handoff is pending or active
- what fallback mode is required if same-thread continuation is unsafe

Recommended fields:

- `session_id`
- `topic_key`
- `thread_id`
- `current_identity_id`
- `current_identity_name`
- `status`
- `last_turn_id`
- `active_turn_id`
- `continuity_mode`
- `safe_to_continue`
- `pending_handoff`
- `last_checkpoint_id`
- `updated_at`

Recommended session statuses:

- `idle`
- `running`
- `waiting_for_followup`
- `handoff_pending`
- `handoff_ready`
- `blocked`
- `failed`
- `canceled`

Recommended continuity modes:

- `same_thread`
- `handoff`
- `checkpoint_fallback`

### Turn

A turn is the machine-visible execution unit within a session.

Recommended fields:

- `turn_id`
- `session_id`
- `thread_id`
- `identity_id`
- `status`
- `started_at`
- `finished_at`
- `failure_kind`
- `failure_message`

Recommended turn statuses:

- `queued`
- `starting`
- `running`
- `completed`
- `failed`
- `timed_out`
- `canceled`

### Handoff

A handoff is a controlled ownership transition for the same session.

Recommended fields:

- `handoff_id`
- `session_id`
- `thread_id`
- `from_identity_id`
- `to_identity_id`
- `status`
- `lease_token`
- `reason`
- `baseline_turn_id`
- `observed_turn_id`
- `fallback_mode`
- `created_at`
- `updated_at`

Recommended handoff statuses:

- `prepared`
- `accepted`
- `confirmed`
- `expired`
- `aborted`
- `fallback_required`

## Recommended CLI Contract

The cleanest public control layer is a new machine-facing command family rather than overloading operator commands.

Recommended new command groups:

- `codex-switch sessions ...`
- `codex-switch turns ...`
- `codex-switch handoffs ...`

All commands in this family should support `--json`.

### Sessions Commands

Implemented commands:

- `codex-switch sessions start`
- `codex-switch sessions resume`
- `codex-switch sessions show`
- `codex-switch sessions list`
- `codex-switch sessions stream`
- `codex-switch sessions cancel`

Examples:

```bash
codex-switch sessions start \
  --topic-key telegram:chat-123:topic-9 \
  --prompt "Investigate failing tests" \
  --auto \
  --json
```

```bash
codex-switch sessions resume \
  --session <session-id> \
  --prompt "Continue from the last checkpoint" \
  --auto \
  --json
```

```bash
codex-switch sessions show --session <session-id> --json
```

### Turns Commands

Implemented commands:

- `codex-switch turns start`
- `codex-switch turns wait`
- `codex-switch turns cancel`
- `codex-switch turns show`

Recommended examples:

```bash
codex-switch turns start \
  --session <session-id> \
  --prompt "Address review comments" \
  --json
```

```bash
codex-switch turns wait --turn <turn-id> --json
```

### Handoffs Commands

Implemented commands:

- `codex-switch handoffs prepare`
- `codex-switch handoffs accept`
- `codex-switch handoffs confirm`
- `codex-switch handoffs show`

Recommended examples:

```bash
codex-switch handoffs prepare \
  --session <session-id> \
  --to-identity "Backup Workspace" \
  --reason quota \
  --json
```

```bash
codex-switch handoffs confirm \
  --handoff <handoff-id> \
  --observed-turn-id <turn-id> \
  --json
```

## JSON Envelope

All machine-facing commands return a versioned envelope in `--json` mode.

Recommended response shape:

```json
{
  "interface_version": "1",
  "ok": true,
  "command": "sessions.start",
  "data": {}
}
```

Recommended failure shape:

```json
{
  "interface_version": "1",
  "ok": false,
  "command": "sessions.start",
  "error": {
    "code": "no_selectable_identity",
    "message": "no selectable identity is currently available",
    "retryable": true,
    "details": {}
  }
}
```

The `message` field is for operators.

Machine consumers should branch on:

- `ok`
- `error.code`
- explicit fields in `data`

They should not branch on free-form English text.

## Streaming Contract

Streaming and control must be separated.

The human-facing stream can still be used for Telegram updates, but it should be emitted as structured events so the bot can remain stateful without guessing.

Implemented format:

- newline-delimited JSON
- one event per line
- monotonically ordered within a single session stream

Recommended event shape:

```json
{
  "interface_version": "1",
  "event": "turn.output.delta",
  "session_id": "session-123",
  "thread_id": "thread-456",
  "turn_id": "turn-789",
  "timestamp": 1710000000,
  "payload": {}
}
```

`sessions stream --json` emits these event records directly. It does not wrap the stream in the
single-response success envelope.

## Current CLI Notes

- `sessions start` creates the durable session record, starts the canonical thread immediately, and
  can optionally launch the first turn from `--prompt` or `--prompt-file`
- `sessions resume` is the machine-facing "resume with more work" entry point; in the current
  implementation it requires a prompt and returns the started turn plus the continuity mode used
- `sessions show --json` is the status entry point for external `/status` commands
- `turns start` rejects duplicate active turns for the same session and supports caller-supplied
  `--idempotency-key`
- `turns wait --json` blocks until the target turn reaches a terminal state or times out
- `handoffs prepare` records either a prepared handoff or a `fallback_required` handoff when safe
  same-thread continuation cannot be established
- `handoffs accept` persists the accepted lease token so the target identity can safely write
- `handoffs confirm` accepts either `--observed-turn-id` for normal confirmation or
  `--fallback checkpoint_fallback` for explicit fallback decisions

## Implemented Safety Invariants

- at most one active turn per session is allowed at a time
- same-thread resume across identities is rejected with `unsafe_same_thread_resume` unless an
  explicit handoff or checkpoint fallback state exists
- a prepared handoff blocks new turns until it is accepted or resolved as fallback
- accepted handoffs keep the target lease active until confirmation or explicit fallback
- `safe_to_continue` is explicit machine state on the session record; callers do not need to infer
  it from operator text

Recommended event types:

- `session.started`
- `session.resumed`
- `session.status.changed`
- `turn.started`
- `turn.output.delta`
- `turn.completed`
- `turn.failed`
- `turn.timed_out`
- `turn.canceled`
- `handoff.prepared`
- `handoff.accepted`
- `handoff.confirmed`
- `handoff.fallback_required`
- `lease.lost`
- `runtime.warning`

For Telegram rendering:

- `turn.output.delta` may carry plain text fragments
- status events may carry short human summaries

For machine control:

- every event must include stable identifiers and event names

## Required Current-State Queries

A full orchestrator needs machine-readable answers to these questions:

- which identity is active for the session right now
- what canonical `thread_id` backs the session
- is there an active `turn_id`
- is same-thread continuation currently safe
- is a handoff required, prepared, accepted, or confirmed
- if same-thread continuation is unsafe, is checkpoint fallback required

Recommended single call:

- `codex-switch sessions show --session <session-id> --json`

This call should be sufficient to drive `/status` in the external application.

## Idempotency

Machine-facing session commands should support caller-supplied idempotency keys.

Recommended fields:

- `request_id`
- `idempotency_key`

Recommended guarantees:

- repeating `sessions.start` with the same idempotency key returns the same session metadata rather than creating a new session
- repeating `turns.start` with the same idempotency key does not create duplicate active turns
- repeating `handoffs.prepare` with the same idempotency key returns the existing pending handoff when one exists

## Safety Rules

For a full orchestrator, the public contract must preserve these safety guarantees:

- at most one active writer per session thread
- same-thread continuation must not proceed while lease ownership is ambiguous
- handoff confirmation must observe thread advancement or an explicit fallback decision
- fallback to checkpoint mode must be explicit in machine state
- session identity changes must be visible to callers as first-class state transitions

## Error Codes

Recommended stable error codes:

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

Recommended retryability guidance:

- `rpc_timeout`: usually retryable
- `runtime_unavailable`: retryable with backoff
- `no_selectable_identity`: retryable after capacity or quota change
- `thread_lease_conflict`: retryable after ownership changes
- `unsafe_same_thread_resume`: not retryable without changed session state
- `validation_error`: not retryable without changing input

## Relation to Existing Commands

Existing commands should remain for operators:

- `select`
- `continue`
- `threads ...`
- `accounts`

But they should not be the primary integration path for a full orchestrator.

Recommended positioning:

- operator commands remain human-oriented
- new `sessions` / `turns` / `handoffs` commands become the machine-facing control layer
- `jobs` / `tasks` remain the queue-oriented async workflow
- `app-server` passthrough remains available for low-level direct protocol clients

## Recommended Implementation Path

The intended implementation path is to expose the runtime model that already exists internally, rather than trying to stretch `exec`.

Recommended order:

1. Add versioned `--json` envelopes for the new machine-facing command family.
2. Persist a durable external `session` record that maps `topic_key` to canonical `thread_id`, identity ownership, and continuity metadata.
3. Expose explicit turn lifecycle state from the App Server runtime.
4. Add structured event streaming for session and turn events.
5. Expose handoff state as a first-class public resource instead of only operator flows.
6. Keep `jobs/tasks` for queued work and use them only when the external application truly wants task orchestration rather than session orchestration.

## Public Contract Boundary

Until the machine-facing session contract above is implemented, external applications that require full session continuity should treat current operator-oriented commands as insufficient for hard orchestration guarantees.

That is the core distinction:

- current supported interface: safe for operators and async queued task integrations
- target session interface: required for a full external orchestrator
