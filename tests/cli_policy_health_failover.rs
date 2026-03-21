use std::fs;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use tempfile::tempdir;

#[test]
fn policy_show_and_set_round_trip() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["policy", "show", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("warning used percent: 85"))
        .stdout(predicate::str::contains("avoid used percent: 95"))
        .stdout(predicate::str::contains("hard-stop used percent: 100"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "policy",
            "set",
            "--warning",
            "40",
            "--avoid",
            "55",
            "--hard-stop",
            "70",
            "--rate-limit-cooldown",
            "90",
            "--auth-failure-cooldown",
            "120",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("selection policy updated"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["policy", "show", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("warning used percent: 40"))
        .stdout(predicate::str::contains("avoid used percent: 55"))
        .stdout(predicate::str::contains("hard-stop used percent: 70"))
        .stdout(predicate::str::contains("rate-limit cooldown secs: 90"))
        .stdout(predicate::str::contains("auth-failure cooldown secs: 120"));
}

#[test]
fn policy_set_rejects_out_of_range_values() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "policy",
            "set",
            "--warning",
            "40",
            "--avoid",
            "60",
            "--hard-stop",
            "101",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "hard-stop threshold must be <= 100",
        ));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "policy",
            "set",
            "--rate-limit-cooldown",
            "2592001",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "rate-limit cooldown must be between 0 and 2592000 seconds",
        ));
}

#[test]
fn disable_and_health_show_affect_auto_selection() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path(), 30, 10, ExecMode::Success, ExecMode::Success);

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args(["accounts", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Primary"))
        .stdout(predicate::str::contains("Backup"))
        .stdout(predicate::str::contains("  5h limit:    ["));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "disable",
            "Backup",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "identity disabled for automatic selection",
        ));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "health",
            "show",
            "Backup",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Backup (backup)"))
        .stdout(predicate::str::contains("manually disabled: yes"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", "")
        .args(["accounts", "--cached", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Primary"))
        .stdout(predicate::str::contains("Backup"));

    let stdout = String::from_utf8(
        Command::cargo_bin("codex-switch")
            .unwrap()
            .env("PATH", "")
            .args(["accounts", "--cached", "--base-root", &base_root_string])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    assert!(stdout.find("Primary").unwrap() < stdout.find("Backup").unwrap());

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "enable",
            "Backup",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "identity re-enabled for automatic selection",
        ));
}

#[test]
fn exec_auto_failover_penalizes_and_retries_next_identity() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path(), 10, 20, ExecMode::RateLimit, ExecMode::Success);

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    let output = Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args([
            "exec",
            "--auto-failover",
            "--base-root",
            &base_root_string,
            "--",
            "smoke",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "initial identity: Primary (primary)",
        ))
        .stdout(predicate::str::contains(
            "penalized during run: Primary (primary) kind=rate_limit",
        ))
        .stdout(predicate::str::contains(
            "final launched identity: Backup (backup)",
        ))
        .stdout(predicate::str::contains("ok-backup"))
        .stderr(predicate::str::contains("429 rate limit"))
        .get_output()
        .stdout
        .clone();
    let stdout = String::from_utf8(output).unwrap();
    let decision_log_path = extract_value(&stdout, "decision log:");
    let decision_log = fs::read_to_string(&decision_log_path).unwrap();
    assert!(decision_log.contains("\"kind\": \"exec_failover\""));
    assert!(decision_log.contains("\"failure_kind\": \"rate_limit\""));
    assert!(decision_log.contains("\"final_identity_id\": \"backup\""));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", "")
        .args([
            "identities",
            "health",
            "show",
            "Primary",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("penalty active: yes"))
        .stdout(predicate::str::contains("last failure kind: rate_limit"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", "")
        .args(["accounts", "--cached", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Primary"))
        .stdout(predicate::str::contains("Backup"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "health",
            "clear",
            "Primary",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("identity health cleared"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", "")
        .args([
            "identities",
            "health",
            "show",
            "Primary",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("penalty active: no"))
        .stdout(predicate::str::contains("last failure kind: none"));
}

#[test]
fn exec_auto_failover_records_auth_penalty_without_leaking_raw_output() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path(), 10, 20, ExecMode::Auth, ExecMode::Success);

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "policy",
            "set",
            "--rate-limit-cooldown",
            "90",
            "--auth-failure-cooldown",
            "123",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args([
            "exec",
            "--auto-failover",
            "--base-root",
            &base_root_string,
            "--",
            "smoke",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "penalized during run: Primary (primary) kind=auth",
        ))
        .stdout(predicate::str::contains(
            "final launched identity: Backup (backup)",
        ))
        .stderr(predicate::str::contains("401 unauthorized secret-token"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", "")
        .args([
            "identities",
            "health",
            "show",
            "Primary",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("last failure kind: auth"))
        .stdout(predicate::str::contains(
            "last failure summary: auth failure detected while launching codex",
        ))
        .stdout(predicate::str::contains("secret-token").not());

    let health_path = base_root.join("shared").join("identity-health.json");
    let health: Value = serde_json::from_slice(&fs::read(&health_path).unwrap()).unwrap();
    let primary = &health["identities"]["primary"];
    let penalty_until = primary["penalty_until"].as_i64().unwrap();
    let last_failure_at = primary["last_failure_at"].as_i64().unwrap();
    assert_eq!(penalty_until - last_failure_at, 123);
}

#[test]
fn exec_auto_failover_fails_when_no_identity_is_eligible() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path(), 10, 20, ExecMode::Success, ExecMode::Success);

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    for identity in ["Primary", "Backup"] {
        Command::cargo_bin("codex-switch")
            .unwrap()
            .args([
                "identities",
                "disable",
                identity,
                "--base-root",
                &base_root_string,
            ])
            .assert()
            .success();
    }

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args([
            "exec",
            "--auto-failover",
            "--base-root",
            &base_root_string,
            "--",
            "smoke",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("initial identity: none"))
        .stdout(predicate::str::contains(
            "skipped due to health: Primary (primary) reason=manually_disabled",
        ))
        .stdout(predicate::str::contains(
            "skipped due to health: Backup (backup) reason=manually_disabled",
        ))
        .stdout(predicate::str::contains(
            "no eligible identity after failover",
        ))
        .stdout(predicate::str::contains("final launched identity").not());
}

#[test]
fn explicit_identity_with_auto_failover_does_not_switch_away() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path(), 10, 20, ExecMode::RateLimit, ExecMode::Success);

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args([
            "exec",
            "--identity",
            "Primary",
            "--auto-failover",
            "--base-root",
            &base_root_string,
            "--",
            "smoke",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("429 rate limit"))
        .stdout(predicate::str::contains("final launched identity").not());
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

#[derive(Clone, Copy)]
enum ExecMode {
    Success,
    RateLimit,
    Auth,
}

fn install_fake_codex(
    root: &std::path::Path,
    primary_used_percent: i32,
    backup_used_percent: i32,
    primary_exec_mode: ExecMode,
    backup_exec_mode: ExecMode,
) -> String {
    let fake_bin_dir = root.join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let script = r#"#!/bin/sh
identity=$(basename "$CODEX_HOME")

primary_used=%PRIMARY_USED%
backup_used=%BACKUP_USED%
primary_exec_mode="%PRIMARY_EXEC_MODE%"
backup_exec_mode="%BACKUP_EXEC_MODE%"

case "$identity" in
  primary)
    email="primary@example.com"
    plan="plus"
    used="$primary_used"
    exec_mode="$primary_exec_mode"
    ;;
  backup)
    email="backup@example.com"
    plan="pro"
    used="$backup_used"
    exec_mode="$backup_exec_mode"
    ;;
  *)
    email="other@example.com"
    plan="free"
    used="50"
    exec_mode="success"
    ;;
esac

auth_payload='{"authMethod":"chatgpt"}'
account_payload=$(printf '{"account":{"type":"chatgpt","email":"%s","planType":"%s"},"requiresOpenaiAuth":false}' "$email" "$plan")
rate_limit_payload=$(printf '{"rateLimits":{"primary":{"usedPercent":%s,"windowDurationMins":300,"resetsAt":1700003600},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"%s","primary":{"usedPercent":%s,"windowDurationMins":300,"resetsAt":1700003600},"secondary":null}}}' "$used" "$plan" "$used")

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
      *)
        printf '{"jsonrpc":"2.0","id":"%s","error":{"code":-32601,"message":"unknown"}}\n' "$id"
        ;;
    esac
  done
  exit 0
fi

if [ "$exec_mode" = "rate_limit" ]; then
  echo "429 rate limit" >&2
  exit 1
fi

if [ "$exec_mode" = "auth" ]; then
  echo "401 unauthorized secret-token" >&2
  exit 1
fi

echo "ok-$identity"
exit 0
"#
    .replace("%PRIMARY_USED%", &primary_used_percent.to_string())
    .replace("%BACKUP_USED%", &backup_used_percent.to_string())
    .replace("%PRIMARY_EXEC_MODE%", exec_mode_label(primary_exec_mode))
    .replace("%BACKUP_EXEC_MODE%", exec_mode_label(backup_exec_mode));

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(&fake_codex_path, script).unwrap();

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

fn exec_mode_label(mode: ExecMode) -> &'static str {
    match mode {
        ExecMode::Success => "success",
        ExecMode::RateLimit => "rate_limit",
        ExecMode::Auth => "auth",
    }
}

fn extract_value(output: &str, prefix: &str) -> String {
    output
        .lines()
        .find_map(|line| line.strip_prefix(prefix).map(str::trim))
        .unwrap()
        .to_string()
}
