use std::fs;
use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn manual_select_persists_identity_for_exec_and_app_server_wrappers() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("launch-log.txt");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
printf '%s|%s\n' "${CODEX_HOME##*/}" "$*" >> "$FAKE_LAUNCH_LOG"
exit 0
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

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Target",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "Target", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("selected Target"))
        .stdout(predicate::str::contains("mode: manual"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LAUNCH_LOG", &log_path)
        .args([
            "exec",
            "--base-root",
            &base_root_string,
            "--",
            "exec",
            "hello world",
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LAUNCH_LOG", &log_path)
        .args([
            "app-server",
            "--base-root",
            &base_root_string,
            "--",
            "--listen",
            "stdio://",
        ])
        .assert()
        .success();

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("target|exec hello world"));
    assert!(log.contains("target|app-server --listen stdio://"));
}

#[test]
fn continue_switches_identity_with_shared_session_resume() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("resume-log.txt");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
identity="${CODEX_HOME##*/}"

baseline='{"thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000001,"status":{"type":"idle"},"path":"/tmp/thread-1","turns":[{"id":"turn-a","status":"completed"}]}}'
missing='{"thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000001,"status":{"type":"idle"},"path":"/tmp/thread-1","turns":[]}}'

if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
    case "$line" in
      *'"method":"thread/read"'*)
        printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$baseline"
        ;;
      *'"method":"thread/resume"'*)
        if [ "$identity" = "target" ] && [ "$FAKE_RESUME_MODE" = "mismatch" ]; then
          printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$missing"
        else
          printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$baseline"
        fi
        ;;
      *)
        printf '{"jsonrpc":"2.0","id":"%s","error":{"code":-32601,"message":"unknown"}}\n' "$id"
        ;;
    esac
  done
  exit 0
fi

printf '%s|%s\n' "$identity" "$*" >> "$FAKE_LAUNCH_LOG"
exit 0
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

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "Source", "--base-root", &base_root_string])
        .assert()
        .success();

    let success_output = Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LAUNCH_LOG", &log_path)
        .env("FAKE_RESUME_MODE", "shared")
        .args([
            "continue",
            "--thread",
            "thread-1",
            "--to",
            "Target",
            "--base-root",
            &base_root_string,
        ])
        .output()
        .unwrap();
    assert!(success_output.status.success(), "{:?}", success_output);
    let success_stdout = String::from_utf8(success_output.stdout).unwrap();
    assert!(success_stdout.contains("mode: resume_same_thread"));
    assert!(success_stdout.contains("checkpoint: "));

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("target|resume thread-1"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("selected Target"));
}

#[test]
fn continue_falls_back_to_checkpoint_when_shared_resume_is_unavailable() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("resume-log.txt");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
identity="${CODEX_HOME##*/}"

baseline='{"thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000001,"status":{"type":"idle"},"path":"/tmp/thread-1","turns":[{"id":"turn-a","status":"completed"}]}}'
missing='{"thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000001,"status":{"type":"idle"},"path":"/tmp/thread-1","turns":[]}}'

if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
    case "$line" in
      *'"method":"thread/read"'*)
        printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$baseline"
        ;;
      *'"method":"thread/resume"'*)
        printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$missing"
        ;;
      *)
        printf '{"jsonrpc":"2.0","id":"%s","error":{"code":-32601,"message":"unknown"}}\n' "$id"
        ;;
    esac
  done
  exit 0
fi

printf '%s|%s\n' "$identity" "$*" >> "$FAKE_LAUNCH_LOG"
exit 0
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

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "Source", "--base-root", &base_root_string])
        .assert()
        .success();

    let fallback_output = Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LAUNCH_LOG", &log_path)
        .args([
            "continue",
            "--thread",
            "thread-1",
            "--to",
            "Target",
            "--base-root",
            &base_root_string,
            "--no-launch",
        ])
        .output()
        .unwrap();
    assert!(fallback_output.status.success(), "{:?}", fallback_output);
    let fallback_stdout = String::from_utf8(fallback_output.stdout).unwrap();
    assert!(fallback_stdout.contains("mode: resume_via_checkpoint"));
    assert!(fallback_stdout.contains("fallback reason:"));
    assert!(fallback_stdout.contains("resume prompt:"));

    let checkpoint_path = extract_value(&fallback_stdout, "checkpoint:");
    let checkpoint_path = Path::new(&checkpoint_path);
    let checkpoint = fs::read_to_string(checkpoint_path).unwrap();
    assert!(checkpoint.contains("\"mode\": \"resume_via_checkpoint\""));
    assert!(checkpoint.contains("\"target_identity_id\": \"target\""));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("selected Target"));
}

fn extract_value(output: &str, prefix: &str) -> String {
    output
        .lines()
        .find_map(|line| line.strip_prefix(prefix).map(str::trim))
        .unwrap()
        .to_string()
}
