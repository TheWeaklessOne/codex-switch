use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use serde_json::Value;
use tempfile::tempdir;

use codex_switch::bootstrap::BootstrapIdentityRequest;
use codex_switch::domain::identity::AuthMode;
use codex_switch::identity_registry::IdentityRegistryService;
use codex_switch::storage::registry_store::JsonRegistryStore;

#[test]
fn machine_facing_session_flow_works_end_to_end() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("runtime");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_codex = fake_bin_dir.join("codex");
    write_fake_codex(&fake_codex);
    register_identity(&base_root, "Source");
    register_identity(&base_root, "Target");

    let started = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "start",
            "--topic-key",
            "telegram:chat-123:topic-9",
            "--workspace",
            workspace.to_str().unwrap(),
            "--identity",
            "Source",
            "--json",
        ],
    );
    assert!(started["ok"].as_bool().unwrap());
    let session_id = started["data"]["session"]["session_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        started["data"]["session"]["continuity_mode"].as_str(),
        Some("same_thread")
    );

    let shown = run_json(
        &base_root,
        &fake_bin_dir,
        &["sessions", "show", "--session", &session_id, "--json"],
    );
    assert!(shown["ok"].as_bool().unwrap());
    assert_eq!(
        shown["data"]["session"]["thread_id"].as_str(),
        started["data"]["session"]["thread_id"].as_str()
    );

    let first_turn = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "Investigate the failure",
            "--identity",
            "Source",
            "--json",
        ],
    );
    assert!(first_turn["ok"].as_bool().unwrap());
    let first_turn_id = first_turn["data"]["turn"]["turn_id"]
        .as_str()
        .unwrap()
        .to_string();

    let streamed = run_lines(
        &base_root,
        &fake_bin_dir,
        &["sessions", "stream", "--session", &session_id, "--json"],
    );
    assert!(streamed
        .iter()
        .any(|event| event["event"] == "turn.started" || event["event"] == "turn.output.delta"));

    let waited = run_json(
        &base_root,
        &fake_bin_dir,
        &["turns", "wait", "--turn", &first_turn_id, "--json"],
    );
    assert!(waited["ok"].as_bool().unwrap());
    assert_eq!(waited["data"]["turn"]["status"].as_str(), Some("completed"));

    let resumed = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "resume",
            "--session",
            &session_id,
            "--prompt",
            "Continue with the follow-up",
            "--identity",
            "Source",
            "--json",
        ],
    );
    assert!(resumed["ok"].as_bool().unwrap());
    assert_eq!(
        resumed["data"]["continuity_mode"].as_str(),
        Some("same_thread")
    );
    let resumed_turn_id = resumed["data"]["turn"]["turn_id"]
        .as_str()
        .unwrap()
        .to_string();
    let resumed_wait = run_json(
        &base_root,
        &fake_bin_dir,
        &["turns", "wait", "--turn", &resumed_turn_id, "--json"],
    );
    assert_eq!(
        resumed_wait["data"]["turn"]["status"].as_str(),
        Some("completed")
    );

    let prepared = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "handoffs",
            "prepare",
            "--session",
            &session_id,
            "--to-identity",
            "Target",
            "--reason",
            "quota",
            "--json",
        ],
    );
    assert!(prepared["ok"].as_bool().unwrap());
    let handoff_id = prepared["data"]["handoff"]["handoff_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        prepared["data"]["handoff"]["status"].as_str(),
        Some("prepared")
    );

    let handoff_shown = run_json(
        &base_root,
        &fake_bin_dir,
        &["handoffs", "show", "--handoff", &handoff_id, "--json"],
    );
    assert_eq!(
        handoff_shown["data"]["handoff"]["status"].as_str(),
        Some("prepared")
    );

    let accepted = run_json(
        &base_root,
        &fake_bin_dir,
        &["handoffs", "accept", "--handoff", &handoff_id, "--json"],
    );
    assert_eq!(
        accepted["data"]["handoff"]["status"].as_str(),
        Some("accepted")
    );

    let handoff_turn = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "Continue on the target identity",
            "--identity",
            "Target",
            "--json",
        ],
    );
    assert!(handoff_turn["ok"].as_bool().unwrap());
    assert_eq!(
        handoff_turn["data"]["continuity_mode"].as_str(),
        Some("handoff")
    );
    let handoff_turn_id = handoff_turn["data"]["turn"]["turn_id"]
        .as_str()
        .unwrap()
        .to_string();
    let handoff_wait = run_json(
        &base_root,
        &fake_bin_dir,
        &["turns", "wait", "--turn", &handoff_turn_id, "--json"],
    );
    let observed_turn_id = handoff_wait["data"]["turn"]["runtime_turn_id"]
        .as_str()
        .unwrap()
        .to_string();

    let confirmed = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "handoffs",
            "confirm",
            "--handoff",
            &handoff_id,
            "--observed-turn-id",
            &observed_turn_id,
            "--json",
        ],
    );
    assert_eq!(
        confirmed["data"]["handoff"]["status"].as_str(),
        Some("confirmed")
    );

    let followup = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "Same thread follow-up after confirm",
            "--identity",
            "Target",
            "--json",
        ],
    );
    assert_eq!(
        followup["data"]["continuity_mode"].as_str(),
        Some("same_thread")
    );
}

#[test]
fn rejects_unsafe_same_thread_resume_without_handoff() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("runtime");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_codex = fake_bin_dir.join("codex");
    write_fake_codex(&fake_codex);
    register_identity(&base_root, "Source");
    register_identity(&base_root, "Target");

    let started = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "start",
            "--topic-key",
            "topic-unsafe",
            "--workspace",
            workspace.to_str().unwrap(),
            "--identity",
            "Source",
            "--json",
        ],
    );
    let session_id = started["data"]["session"]["session_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (_, failure) = run_failure_json(
        &base_root,
        &fake_bin_dir,
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "try to switch unsafely",
            "--identity",
            "Target",
            "--json",
        ],
    );
    assert!(!failure["ok"].as_bool().unwrap());
    assert_eq!(
        failure["error"]["code"].as_str(),
        Some("unsafe_same_thread_resume")
    );
}

#[test]
fn rejects_duplicate_active_turns() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("runtime");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_codex = fake_bin_dir.join("codex");
    write_fake_codex(&fake_codex);
    register_identity(&base_root, "Source");

    let started = run_json_with_env(
        &base_root,
        &fake_bin_dir,
        &[("FAKE_CODEX_TURN_DELAY_MS", "600")],
        &[
            "sessions",
            "start",
            "--topic-key",
            "topic-busy",
            "--workspace",
            workspace.to_str().unwrap(),
            "--identity",
            "Source",
            "--json",
        ],
    );
    let session_id = started["data"]["session"]["session_id"]
        .as_str()
        .unwrap()
        .to_string();

    let first = run_json_with_env(
        &base_root,
        &fake_bin_dir,
        &[("FAKE_CODEX_TURN_DELAY_MS", "600")],
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "first turn",
            "--identity",
            "Source",
            "--json",
        ],
    );
    assert!(first["ok"].as_bool().unwrap());

    let (_, duplicate) = run_failure_json_with_env(
        &base_root,
        &fake_bin_dir,
        &[("FAKE_CODEX_TURN_DELAY_MS", "600")],
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "duplicate turn",
            "--identity",
            "Source",
            "--json",
        ],
    );
    assert!(!duplicate["ok"].as_bool().unwrap());
    assert_eq!(
        duplicate["error"]["code"].as_str(),
        Some("turn_already_active")
    );
}

#[test]
fn json_failures_exit_non_zero_with_machine_readable_envelope() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("runtime");
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_codex = fake_bin_dir.join("codex");
    write_fake_codex(&fake_codex);

    let (status, failure) = run_failure_json(
        &base_root,
        &fake_bin_dir,
        &["sessions", "show", "--session", "missing-session", "--json"],
    );
    assert_eq!(status, 2);
    assert_eq!(failure["interface_version"].as_str(), Some("1"));
    assert_eq!(failure["ok"].as_bool(), Some(false));
    assert_eq!(failure["command"].as_str(), Some("sessions.show"));
    assert_eq!(failure["error"]["code"].as_str(), Some("session_not_found"));
}

#[test]
fn sessions_start_is_idempotent() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("runtime");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_codex = fake_bin_dir.join("codex");
    write_fake_codex(&fake_codex);
    register_identity(&base_root, "Source");

    let first = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "start",
            "--topic-key",
            "topic-idempotent",
            "--workspace",
            workspace.to_str().unwrap(),
            "--identity",
            "Source",
            "--idempotency-key",
            "session-start-1",
            "--json",
        ],
    );
    let second = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "start",
            "--topic-key",
            "topic-idempotent",
            "--workspace",
            workspace.to_str().unwrap(),
            "--identity",
            "Source",
            "--idempotency-key",
            "session-start-1",
            "--json",
        ],
    );

    assert_eq!(
        first["data"]["session"]["session_id"].as_str(),
        second["data"]["session"]["session_id"].as_str()
    );
    assert_eq!(
        first["data"]["session"]["thread_id"].as_str(),
        second["data"]["session"]["thread_id"].as_str()
    );
}

#[test]
fn handoffs_prepare_is_idempotent() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("runtime");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_codex = fake_bin_dir.join("codex");
    write_fake_codex(&fake_codex);
    register_identity(&base_root, "Source");
    register_identity(&base_root, "Target");

    let started = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "start",
            "--topic-key",
            "topic-handoff-idempotent",
            "--workspace",
            workspace.to_str().unwrap(),
            "--identity",
            "Source",
            "--json",
        ],
    );
    let session_id = started["data"]["session"]["session_id"]
        .as_str()
        .unwrap()
        .to_string();

    let first = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "handoffs",
            "prepare",
            "--session",
            &session_id,
            "--to-identity",
            "Target",
            "--reason",
            "quota",
            "--idempotency-key",
            "handoff-1",
            "--json",
        ],
    );
    let second = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "handoffs",
            "prepare",
            "--session",
            &session_id,
            "--to-identity",
            "Target",
            "--reason",
            "quota",
            "--idempotency-key",
            "handoff-1",
            "--json",
        ],
    );

    assert_eq!(
        first["data"]["handoff"]["handoff_id"].as_str(),
        second["data"]["handoff"]["handoff_id"].as_str()
    );
}

#[test]
fn session_stream_is_monotonic_and_replayable() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("runtime");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_codex = fake_bin_dir.join("codex");
    write_fake_codex(&fake_codex);
    register_identity(&base_root, "Source");

    let started = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "start",
            "--topic-key",
            "topic-stream",
            "--workspace",
            workspace.to_str().unwrap(),
            "--identity",
            "Source",
            "--json",
        ],
    );
    let session_id = started["data"]["session"]["session_id"]
        .as_str()
        .unwrap()
        .to_string();

    let turn = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "stream this turn",
            "--identity",
            "Source",
            "--json",
        ],
    );
    let turn_id = turn["data"]["turn"]["turn_id"]
        .as_str()
        .unwrap()
        .to_string();
    let _ = run_json(
        &base_root,
        &fake_bin_dir,
        &["turns", "wait", "--turn", &turn_id, "--json"],
    );

    let events = run_lines(
        &base_root,
        &fake_bin_dir,
        &["sessions", "stream", "--session", &session_id, "--json"],
    );
    assert!(events.len() >= 3);
    let mut previous = 0_i64;
    for event in &events {
        assert_eq!(event["interface_version"].as_str(), Some("1"));
        assert_eq!(event["session_id"].as_str(), Some(session_id.as_str()));
        let sequence = event["sequence_no"].as_i64().unwrap();
        assert!(sequence > previous);
        previous = sequence;
    }

    let replay_after = events[1]["sequence_no"].as_i64().unwrap();
    let replay = run_lines(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "stream",
            "--session",
            &session_id,
            "--after-sequence",
            &replay_after.to_string(),
            "--json",
        ],
    );
    let expected: Vec<i64> = events
        .iter()
        .filter_map(|event| {
            let sequence = event["sequence_no"].as_i64().unwrap();
            (sequence > replay_after).then_some(sequence)
        })
        .collect();
    let actual: Vec<i64> = replay
        .iter()
        .map(|event| event["sequence_no"].as_i64().unwrap())
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn explicit_checkpoint_fallback_transitions_back_to_same_thread_after_success() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("runtime");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_codex = fake_bin_dir.join("codex");
    write_fake_codex(&fake_codex);
    register_identity(&base_root, "Source");
    register_identity(&base_root, "Target");

    let started = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "start",
            "--topic-key",
            "topic-fallback",
            "--workspace",
            workspace.to_str().unwrap(),
            "--identity",
            "Source",
            "--json",
        ],
    );
    let session_id = started["data"]["session"]["session_id"]
        .as_str()
        .unwrap()
        .to_string();

    let prepared = run_json_with_env(
        &base_root,
        &fake_bin_dir,
        &[("FAKE_CODEX_FAIL_THREAD_RESUME_FOR_SLUG", "target")],
        &[
            "handoffs",
            "prepare",
            "--session",
            &session_id,
            "--to-identity",
            "Target",
            "--reason",
            "quota",
            "--json",
        ],
    );
    let handoff_id = prepared["data"]["handoff"]["handoff_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        prepared["data"]["handoff"]["status"].as_str(),
        Some("fallback_required")
    );

    let confirmed = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "handoffs",
            "confirm",
            "--handoff",
            &handoff_id,
            "--fallback",
            "checkpoint-fallback",
            "--json",
        ],
    );
    assert_eq!(
        confirmed["data"]["handoff"]["status"].as_str(),
        Some("fallback_required")
    );

    let resumed = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "Recover via checkpoint fallback",
            "--identity",
            "Target",
            "--json",
        ],
    );
    assert_eq!(
        resumed["data"]["continuity_mode"].as_str(),
        Some("checkpoint_fallback")
    );
    let turn_id = resumed["data"]["turn"]["turn_id"]
        .as_str()
        .unwrap()
        .to_string();
    let _ = run_json(
        &base_root,
        &fake_bin_dir,
        &["turns", "wait", "--turn", &turn_id, "--json"],
    );

    let shown = run_json(
        &base_root,
        &fake_bin_dir,
        &["sessions", "show", "--session", &session_id, "--json"],
    );
    assert_eq!(
        shown["data"]["session"]["continuity_mode"].as_str(),
        Some("same_thread")
    );
    assert_eq!(
        shown["data"]["session"]["safe_to_continue"].as_bool(),
        Some(true)
    );
    assert!(shown["data"]["pending_handoff"].is_null());
}

#[test]
fn startup_failures_do_not_leave_active_turns_or_leases_behind() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("runtime");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_codex = fake_bin_dir.join("codex");
    write_fake_codex(&fake_codex);
    register_identity(&base_root, "Source");

    let started = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "sessions",
            "start",
            "--topic-key",
            "topic-startup-failure",
            "--workspace",
            workspace.to_str().unwrap(),
            "--identity",
            "Source",
            "--json",
        ],
    );
    let session_id = started["data"]["session"]["session_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, failure) = run_failure_json_with_env(
        &base_root,
        &fake_bin_dir,
        &[(
            "CODEX_SWITCH_TEST_WORKER_PROGRAM",
            "/definitely/missing-codex-switch-worker",
        )],
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "this worker never starts",
            "--identity",
            "Source",
            "--json",
        ],
    );
    assert_eq!(status, 2);
    assert_eq!(
        failure["error"]["code"].as_str(),
        Some("runtime_unavailable")
    );

    let shown = run_json(
        &base_root,
        &fake_bin_dir,
        &["sessions", "show", "--session", &session_id, "--json"],
    );
    assert!(shown["data"]["active_turn"].is_null());
    assert_eq!(
        shown["data"]["session"]["safe_to_continue"].as_bool(),
        Some(true)
    );

    let (_, retry) = run_failure_json_with_env(
        &base_root,
        &fake_bin_dir,
        &[("FAKE_CODEX_FAIL_THREAD_RESUME_FOR_SLUG", "source")],
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "resume will fail once",
            "--identity",
            "Source",
            "--json",
        ],
    );
    assert!(!retry["ok"].as_bool().unwrap());
    assert_eq!(retry["error"]["code"].as_str(), Some("runtime_unavailable"));

    let post_failure = run_json(
        &base_root,
        &fake_bin_dir,
        &["sessions", "show", "--session", &session_id, "--json"],
    );
    assert!(post_failure["data"]["active_turn"].is_null());
    assert_eq!(
        post_failure["data"]["session"]["safe_to_continue"].as_bool(),
        Some(true)
    );

    let recovered = run_json(
        &base_root,
        &fake_bin_dir,
        &[
            "turns",
            "start",
            "--session",
            &session_id,
            "--prompt",
            "retry after cleanup",
            "--identity",
            "Source",
            "--json",
        ],
    );
    assert!(recovered["ok"].as_bool().unwrap());
}

fn register_identity(base_root: &Path, name: &str) {
    let registry = JsonRegistryStore::new(base_root);
    let service = IdentityRegistryService::new(registry);
    service
        .register_identity(BootstrapIdentityRequest {
            display_name: name.to_string(),
            base_root: base_root.to_path_buf(),
            auth_mode: AuthMode::Chatgpt,
            home_override: None,
            import_auth_from_home: None,
            overwrite_config: false,
            api_key_env_var: None,
            forced_chatgpt_workspace_id: None,
        })
        .unwrap();
}

fn run_json(base_root: &Path, fake_bin_dir: &Path, args: &[&str]) -> Value {
    run_json_with_env(base_root, fake_bin_dir, &[], args)
}

fn run_json_with_env(
    base_root: &Path,
    fake_bin_dir: &Path,
    envs: &[(&str, &str)],
    args: &[&str],
) -> Value {
    let output = command(base_root, fake_bin_dir, envs, args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&output).unwrap()
}

fn run_lines(base_root: &Path, fake_bin_dir: &Path, args: &[&str]) -> Vec<Value> {
    let output = command(base_root, fake_bin_dir, &[], args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    String::from_utf8(output)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect()
}

fn run_failure_json(base_root: &Path, fake_bin_dir: &Path, args: &[&str]) -> (i32, Value) {
    run_failure_json_with_env(base_root, fake_bin_dir, &[], args)
}

fn run_failure_json_with_env(
    base_root: &Path,
    fake_bin_dir: &Path,
    envs: &[(&str, &str)],
    args: &[&str],
) -> (i32, Value) {
    let output = command(base_root, fake_bin_dir, envs, args)
        .assert()
        .failure()
        .get_output()
        .clone();
    let status = output.status.code().unwrap();
    let value = serde_json::from_slice(&output.stdout).unwrap();
    (status, value)
}

fn command(base_root: &Path, fake_bin_dir: &Path, envs: &[(&str, &str)], args: &[&str]) -> Command {
    let mut command = Command::cargo_bin("codex-switch").unwrap();
    command.env("PATH", prepend_path(fake_bin_dir));
    for (key, value) in envs {
        command.env(key, value);
    }
    command.args(args);
    command.arg("--base-root");
    command.arg(base_root);
    command
}

fn prepend_path(fake_bin_dir: &Path) -> String {
    let existing = std::env::var("PATH").unwrap_or_default();
    format!("{}:{}", fake_bin_dir.display(), existing)
}

fn write_fake_codex(path: &PathBuf) {
    fs::write(
        path,
        r#"#!/usr/bin/env python3
import json
import os
import sys
import time
from pathlib import Path

if len(sys.argv) < 2 or sys.argv[1] != "app-server":
    print("unexpected invocation", file=sys.stderr)
    sys.exit(1)

codex_home = Path(os.environ["CODEX_HOME"])
base_root = codex_home.parent.parent
state_path = base_root / "fake-app-server-state.json"
state_path.parent.mkdir(parents=True, exist_ok=True)
codex_home_lower = str(codex_home).lower()

def matches_slug_env(name):
    slug = os.environ.get(name, "").strip().lower()
    return bool(slug) and slug in codex_home_lower

def load_state():
    if state_path.exists():
        return json.loads(state_path.read_text())
    return {"counter": 0, "threads": {}}

def save_state(state):
    state_path.write_text(json.dumps(state))

def next_counter(state):
    state["counter"] += 1
    return state["counter"]

for raw in sys.stdin:
    message = json.loads(raw)
    method = message.get("method")
    if method == "initialized":
        continue
    if "id" not in message:
        continue
    state = load_state()
    if method == "initialize":
        response = {"jsonrpc": "2.0", "id": message["id"], "result": {"protocolVersion": "2"}}
        print(json.dumps(response), flush=True)
    elif method == "thread/start":
        counter = next_counter(state)
        thread_id = f"thread-{counter}"
        state["threads"][thread_id] = {
            "createdAt": 1700000000 + counter,
            "updatedAt": 1700000000 + counter,
            "turns": [],
            "status": "idle",
        }
        save_state(state)
        print(json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": {"thread": {"id": thread_id}}}), flush=True)
    elif method == "thread/read" or method == "thread/resume":
        if method == "thread/read" and matches_slug_env("FAKE_CODEX_FAIL_THREAD_READ_FOR_SLUG"):
            print(json.dumps({"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32010, "message": "forced thread/read failure"}}), flush=True)
            continue
        if method == "thread/resume" and matches_slug_env("FAKE_CODEX_FAIL_THREAD_RESUME_FOR_SLUG"):
            print(json.dumps({"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32011, "message": "forced thread/resume failure"}}), flush=True)
            continue
        thread_id = message["params"]["threadId"]
        thread = state["threads"][thread_id]
        response = {
            "jsonrpc": "2.0",
            "id": message["id"],
            "result": {
                "thread": {
                    "id": thread_id,
                    "createdAt": thread["createdAt"],
                    "updatedAt": thread["updatedAt"],
                    "status": {"type": thread["status"]},
                    "path": str(base_root / "shared" / "sessions" / thread_id),
                    "turns": [
                        {"id": turn["id"], "status": turn["status"]}
                        for turn in thread["turns"]
                    ],
                }
            },
        }
        print(json.dumps(response), flush=True)
    elif method == "turn/start":
        thread_id = message["params"]["threadId"]
        thread = state["threads"][thread_id]
        counter = next_counter(state)
        runtime_turn_id = f"runtime-turn-{counter}"
        thread["turns"].append({"id": runtime_turn_id, "status": "inProgress"})
        thread["updatedAt"] += 1
        thread["status"] = "active"
        save_state(state)
        print(json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": {"turn": {"id": runtime_turn_id}}}), flush=True)
        delay_ms = int(os.environ.get("FAKE_CODEX_TURN_DELAY_MS", "150"))
        time.sleep(delay_ms / 1000.0)
        print(json.dumps({"jsonrpc": "2.0", "method": "turn/output", "params": {"turn": {"id": runtime_turn_id}, "delta": "working"}}), flush=True)
        state = load_state()
        thread = state["threads"][thread_id]
        thread["turns"][-1]["status"] = "completed"
        thread["updatedAt"] += 1
        thread["status"] = "idle"
        save_state(state)
        time.sleep(0.05)
        print(json.dumps({"jsonrpc": "2.0", "method": "turn/completed", "params": {"turn": {"id": runtime_turn_id}}}), flush=True)
    else:
        print(json.dumps({"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32601, "message": f"unknown method {method}"}}), flush=True)
"#,
    )
    .unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}
