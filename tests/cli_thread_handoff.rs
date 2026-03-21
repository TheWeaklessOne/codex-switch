use std::fs;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn cli_handoff_flow_confirms_persisted_history() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let response_path = temp.path().join("thread-response.json");
    fs::write(&response_path, baseline_thread_response()).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
    case "$line" in
      *'"method":"thread/read"'*|*'"method":"thread/resume"'*)
        payload=$(tr -d '\n' < "$FAKE_THREAD_RESPONSE_FILE")
        printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$payload"
        ;;
      *)
        printf '{"jsonrpc":"2.0","id":"%s","error":{"code":-32601,"message":"unknown"}}\n' "$id"
        ;;
    esac
  done
  exit 0
fi
echo "unexpected invocation: $@" >&2
exit 1
"#,
    )
    .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake_codex_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_codex_path, permissions).unwrap();
    }

    let path = format!(
        "{}:{}",
        fake_bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    for name in ["Source", "Target"] {
        Command::cargo_bin("codex-switch")
            .unwrap()
            .args([
                "identities",
                "add",
                "chatgpt",
                "--name",
                name,
                "--base-root",
                &base_root_string,
            ])
            .assert()
            .success();
    }

    let acquire_output = run_command(
        &path,
        &response_path,
        &[
            "threads",
            "lease",
            "acquire",
            "thread-1",
            "--identity",
            "Source",
            "--base-root",
            &base_root_string,
        ],
    );
    let active_token = extract_value(&acquire_output, "lease token:");

    let prepare_output = run_command(
        &path,
        &response_path,
        &[
            "threads",
            "handoff",
            "prepare",
            "thread-1",
            "--from",
            "Source",
            "--to",
            "Target",
            "--lease-token",
            &active_token,
            "--reason",
            "quota",
            "--base-root",
            &base_root_string,
        ],
    );
    let pending_token = extract_value(&prepare_output, "lease token:");
    assert!(prepare_output.contains("handoff prepared"));

    let accept_output = run_command(
        &path,
        &response_path,
        &[
            "threads",
            "handoff",
            "accept",
            "thread-1",
            "--to",
            "Target",
            "--lease-token",
            &pending_token,
            "--base-root",
            &base_root_string,
        ],
    );
    let accepted_token = extract_value(&accept_output, "lease token:");
    assert!(accept_output.contains("handoff accepted"));
    assert!(accept_output.contains("tracked state: handoff_accepted"));

    fs::write(&response_path, advanced_thread_response()).unwrap();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_THREAD_RESPONSE_FILE", &response_path)
        .args([
            "threads",
            "handoff",
            "confirm",
            "thread-1",
            "--to",
            "Target",
            "--lease-token",
            &accepted_token,
            "--observed-turn-id",
            "turn-b",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("handoff confirmed"))
        .stdout(predicate::str::contains("latest turn id: turn-b"))
        .stdout(predicate::str::contains("matched turn id: turn-b"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "threads",
            "state",
            "thread-1",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tracked state: handoff_confirmed"))
        .stdout(predicate::str::contains(
            "handoff confirmed turn id: turn-b",
        ));
}

#[test]
fn cli_handoff_confirm_rejects_pending_handoff_token() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let response_path = temp.path().join("thread-response.json");
    fs::write(&response_path, advanced_thread_response()).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
    case "$line" in
      *'"method":"thread/read"'*|*'"method":"thread/resume"'*)
        payload=$(tr -d '\n' < "$FAKE_THREAD_RESPONSE_FILE")
        printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$payload"
        ;;
      *)
        printf '{"jsonrpc":"2.0","id":"%s","error":{"code":-32601,"message":"unknown"}}\n' "$id"
        ;;
    esac
  done
  exit 0
fi
echo "unexpected invocation: $@" >&2
exit 1
"#,
    )
    .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&fake_codex_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_codex_path, permissions).unwrap();
    }

    let path = format!(
        "{}:{}",
        fake_bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    for name in ["Source", "Target"] {
        Command::cargo_bin("codex-switch")
            .unwrap()
            .args([
                "identities",
                "add",
                "chatgpt",
                "--name",
                name,
                "--base-root",
                &base_root_string,
            ])
            .assert()
            .success();
    }

    let acquire_output = run_command(
        &path,
        &response_path,
        &[
            "threads",
            "lease",
            "acquire",
            "thread-1",
            "--identity",
            "Source",
            "--base-root",
            &base_root_string,
        ],
    );
    let active_token = extract_value(&acquire_output, "lease token:");

    let prepare_output = run_command(
        &path,
        &response_path,
        &[
            "threads",
            "handoff",
            "prepare",
            "thread-1",
            "--from",
            "Source",
            "--to",
            "Target",
            "--lease-token",
            &active_token,
            "--reason",
            "quota",
            "--base-root",
            &base_root_string,
        ],
    );
    let pending_token = extract_value(&prepare_output, "lease token:");

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_THREAD_RESPONSE_FILE", &response_path)
        .args([
            "threads",
            "handoff",
            "confirm",
            "thread-1",
            "--to",
            "Target",
            "--lease-token",
            &pending_token,
            "--observed-turn-id",
            "turn-b",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "expected lease state active but found handoff_pending",
        ));
}

fn run_command(path: &str, response_path: &std::path::Path, args: &[&str]) -> String {
    let output = Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", path)
        .env("FAKE_THREAD_RESPONSE_FILE", response_path)
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "command failed: {:?}", output);
    String::from_utf8(output.stdout).unwrap()
}

fn extract_value(output: &str, prefix: &str) -> String {
    output
        .lines()
        .find_map(|line| line.strip_prefix(prefix).map(str::trim))
        .unwrap()
        .to_string()
}

fn baseline_thread_response() -> &'static str {
    r#"{"thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000001,"status":{"type":"idle"},"path":"/tmp/thread-1","turns":[{"id":"turn-a","status":"completed"}]}}"#
}

fn advanced_thread_response() -> &'static str {
    r#"{"thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000002,"status":{"type":"idle"},"path":"/tmp/thread-1","turns":[{"id":"turn-a","status":"completed"},{"id":"turn-b","status":"completed"}]}}"#
}
