use std::fs;
use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn exec_auto_selects_a_healthy_identity_when_no_manual_pin_exists() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let log_path = temp.path().join("launch-log.txt");
    let path = install_fake_codex(temp.path());

    for name in ["Primary", "Backup"] {
        add_identity(name, &base_root_string);
    }

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

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("backup|exec hello world"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("selected Backup"))
        .stdout(predicate::str::contains("mode: automatic"));
}

#[test]
fn continue_auto_uses_same_thread_resume_on_the_healthiest_other_identity() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let log_path = temp.path().join("resume-log.txt");
    let path = install_fake_codex(temp.path());

    for name in ["Source", "Backup"] {
        add_identity(name, &base_root_string);
    }

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "Source", "--base-root", &base_root_string])
        .assert()
        .success();

    let output = Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LAUNCH_LOG", &log_path)
        .env("FAKE_RESUME_MODE", "shared")
        .args([
            "continue",
            "--thread",
            "thread-1",
            "--auto",
            "--base-root",
            &base_root_string,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("mode: resume_same_thread"));
    assert!(stdout.contains("auto target: Backup"));
    assert!(stdout.contains("decision log:"));

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("backup|resume thread-1"));
}

#[test]
fn continue_auto_falls_back_to_checkpoint_when_resume_is_not_safe() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let log_path = temp.path().join("resume-log.txt");
    let path = install_fake_codex(temp.path());

    for name in ["Source", "Backup"] {
        add_identity(name, &base_root_string);
    }

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "Source", "--base-root", &base_root_string])
        .assert()
        .success();

    let output = Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LAUNCH_LOG", &log_path)
        .env("FAKE_RESUME_MODE", "mismatch")
        .args([
            "continue",
            "--thread",
            "thread-1",
            "--auto",
            "--base-root",
            &base_root_string,
            "--no-launch",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("mode: resume_via_checkpoint"));
    assert!(stdout.contains("fallback reason:"));
    assert!(stdout.contains("decision log:"));

    let decision_log_path = extract_value(&stdout, "decision log:");
    let decision_log = fs::read_to_string(Path::new(&decision_log_path)).unwrap();
    assert!(decision_log.contains("\"kind\": \"thread_handoff\""));
    assert!(decision_log.contains("\"continue_mode\": \"resume_via_checkpoint\""));
}

fn add_identity(name: &str, base_root: &str) {
    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            name,
            "--base-root",
            base_root,
        ])
        .assert()
        .success();
}

fn extract_value(output: &str, prefix: &str) -> String {
    output
        .lines()
        .find_map(|line| line.strip_prefix(prefix).map(str::trim))
        .unwrap()
        .to_string()
}

fn install_fake_codex(root: &std::path::Path) -> String {
    let fake_bin_dir = root.join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
identity="${CODEX_HOME##*/}"

auth_payload='{"authMethod":"chatgpt"}'
baseline='{"thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000001,"status":{"type":"idle"},"path":"/tmp/thread-1","turns":[{"id":"turn-a","status":"completed"}]}}'
missing='{"thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000001,"status":{"type":"idle"},"path":"/tmp/thread-1","turns":[]}}'

case "$identity" in
  primary|source)
    account_payload='{"account":{"type":"chatgpt","email":"source@example.com","planType":"plus"},"requiresOpenaiAuth":false}'
    rate_limit_payload='{"rateLimits":{"primary":{"usedPercent":98,"windowDurationMins":300,"resetsAt":1700000000},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":98,"windowDurationMins":300,"resetsAt":1700000000},"secondary":null}}}'
    ;;
  backup)
    account_payload='{"account":{"type":"chatgpt","email":"backup@example.com","planType":"pro"},"requiresOpenaiAuth":false}'
    rate_limit_payload='{"rateLimits":{"primary":{"usedPercent":10,"windowDurationMins":300,"resetsAt":1700003600},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"pro","primary":{"usedPercent":10,"windowDurationMins":300,"resetsAt":1700003600},"secondary":null}}}'
    ;;
  *)
    account_payload='{"account":{"type":"chatgpt","email":"unknown@example.com","planType":"free"},"requiresOpenaiAuth":false}'
    rate_limit_payload='{"rateLimits":{"primary":{"usedPercent":50,"windowDurationMins":300,"resetsAt":1700007200},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"free","primary":{"usedPercent":50,"windowDurationMins":300,"resetsAt":1700007200},"secondary":null}}}'
    ;;
esac

if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
    case "$line" in
      *'"method":"getAuthStatus"'*)
        printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$auth_payload"
        ;;
      *'"method":"account/read"'*)
        printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$account_payload"
        ;;
      *'"method":"account/rateLimits/read"'*)
        printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$rate_limit_payload"
        ;;
      *'"method":"thread/read"'*)
        printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$baseline"
        ;;
      *'"method":"thread/resume"'*)
        if [ "$identity" = "backup" ] && [ "$FAKE_RESUME_MODE" = "mismatch" ]; then
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

    format!(
        "{}:{}",
        fake_bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    )
}
