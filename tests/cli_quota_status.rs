use std::fs;

use assert_cmd::Command;
use chrono::{Local, TimeZone};
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn status_refreshes_and_reads_cached_quota_state() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path());

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args(["status", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Primary (primary)"))
        .stdout(predicate::str::contains("Backup (backup)"))
        .stdout(predicate::str::contains(
            "selector bucket: codex used=100 headroom=0 status=exhausted",
        ))
        .stdout(predicate::str::contains(
            "selector bucket: codex used=55 headroom=45 status=healthy",
        ));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", "")
        .args(["status", "--cached", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Primary (primary)"))
        .stdout(predicate::str::contains("Backup (backup)"))
        .stdout(predicate::str::contains("quota updated at:"))
        .stdout(predicate::str::contains(
            "codex: primary=20%/300m/resets@1700003600",
        ));
}

#[test]
fn select_chooses_best_identity_from_live_and_cached_state() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path());

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args(["select", "--auto", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("selected Backup"))
        .stdout(predicate::str::contains("remaining headroom: 45"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", "")
        .args([
            "select",
            "--auto",
            "--cached",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("selected Backup"))
        .stdout(predicate::str::contains("selector bucket: codex"));
}

#[test]
fn accounts_highlights_the_same_best_candidate_as_auto_selection() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path());

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
        .stdout(predicate::str::contains("  5h limit:    ["))
        .stdout(predicate::str::contains("0% left"))
        .stdout(predicate::str::contains("80% left"))
        .stdout(predicate::str::contains("4% left"))
        .stdout(predicate::str::contains("45% left"))
        .stdout(predicate::str::contains("resets "))
        .stdout(predicate::str::contains("warning").not())
        .stdout(predicate::str::contains("exhausted").not());

    let stdout = String::from_utf8(
        Command::cargo_bin("codex-switch")
            .unwrap()
            .env("PATH", &path)
            .args(["accounts", "--base-root", &base_root_string])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();
    assert!(stdout.find("Backup").unwrap() < stdout.find("Primary").unwrap());
    let reset_columns: std::collections::BTreeSet<_> = stdout
        .lines()
        .filter(|line| line.contains("% left") || line.contains("n/a"))
        .filter_map(|line| line.find("(resets"))
        .collect();
    assert_eq!(reset_columns.len(), 1, "reset columns differed: {stdout}");

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", "")
        .args(["accounts", "--cached", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Backup"))
        .stdout(predicate::str::contains("  1 week:      ["))
        .stdout(predicate::str::contains("45% left"));
}

#[test]
fn accounts_marks_stale_quota_when_live_refresh_fails() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path());

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args(["accounts", "--base-root", &base_root_string])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", "")
        .args(["accounts", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("stale quota: live refresh failed"))
        .stdout(predicate::str::contains("80% left"))
        .stdout(predicate::str::contains("45% left"));
}

#[test]
fn accounts_auto_removes_deactivated_workspace_identities_and_notifies() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex_with_deactivated_workspace(temp.path());
    let deactivated_home = base_root.join("homes").join("deactivated");

    add_identity("Healthy", &base_root_string);
    add_identity("Deactivated", &base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "Deactivated", "--base-root", &base_root_string])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args(["accounts", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Healthy"))
        .stdout(predicate::str::contains("Deactivated").not())
        .stderr(predicate::str::contains(
            "notice: auto-removed Deactivated (deactivated) after live refresh returned 402 deactivated_workspace; cleared current selection",
        ));

    assert!(!deactivated_home.exists());

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["identities", "list", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Healthy (healthy)"))
        .stdout(predicate::str::contains("Deactivated (deactivated)").not());

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("no identity selected"));
}

#[test]
fn select_auto_removes_deactivated_workspace_identities_and_notifies() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex_with_deactivated_workspace(temp.path());

    add_identity("Healthy", &base_root_string);
    add_identity("Deactivated", &base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args(["select", "--auto", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("selected Healthy"))
        .stderr(predicate::str::contains(
            "notice: auto-removed Deactivated (deactivated) after live refresh returned 402 deactivated_workspace",
        ));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["identities", "list", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Healthy (healthy)"))
        .stdout(predicate::str::contains("Deactivated (deactivated)").not());
}

#[test]
fn verify_auto_removes_deactivated_workspace_identity_and_notifies() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex_with_deactivated_workspace(temp.path());
    let deactivated_home = base_root.join("homes").join("deactivated");

    add_identity("Deactivated", &base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "Deactivated", "--base-root", &base_root_string])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args(["identities", "verify", "Deactivated", "--base-root", &base_root_string])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "notice: auto-removed Deactivated (deactivated) after live refresh returned 402 deactivated_workspace; cleared current selection",
        ));

    assert!(!deactivated_home.exists());

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["identities", "list", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Deactivated (deactivated)").not());
}

#[test]
fn accounts_highlights_identity_matching_default_codex_home() {
    let temp = tempdir().unwrap();
    let home_root = temp.path().join("home");
    let default_codex_home = home_root.join(".codex");
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path());

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    fs::create_dir_all(&default_codex_home).unwrap();
    fs::write(
        base_root.join("homes").join("primary").join("auth.json"),
        "primary-auth",
    )
    .unwrap();
    fs::write(
        base_root.join("homes").join("backup").join("auth.json"),
        "backup-auth",
    )
    .unwrap();
    fs::write(default_codex_home.join("auth.json"), "backup-auth").unwrap();

    let stdout = String::from_utf8(
        Command::cargo_bin("codex-switch")
            .unwrap()
            .env("HOME", &home_root)
            .env("PATH", &path)
            .env("CLICOLOR_FORCE", "1")
            .env_remove("NO_COLOR")
            .args(["accounts", "--cached", "--base-root", &base_root_string])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();

    assert!(stdout.contains("\u{1b}[1;36mBackup\u{1b}[0m"));
    assert!(!stdout.contains("\u{1b}[1;36mPrimary\u{1b}[0m"));
}

#[test]
fn accounts_highlights_identity_matching_default_codex_home_account_id() {
    let temp = tempdir().unwrap();
    let home_root = temp.path().join("home");
    let default_codex_home = home_root.join(".codex");
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path());

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    fs::create_dir_all(&default_codex_home).unwrap();
    fs::write(
        base_root.join("homes").join("primary").join("auth.json"),
        r#"{"auth_mode":"chatgpt","tokens":{"account_id":"primary-account"},"last_refresh":1}"#,
    )
    .unwrap();
    fs::write(
        base_root.join("homes").join("backup").join("auth.json"),
        r#"{"auth_mode":"chatgpt","tokens":{"account_id":"shared-account"},"last_refresh":2}"#,
    )
    .unwrap();
    fs::write(
        default_codex_home.join("auth.json"),
        r#"{"auth_mode":"chatgpt","tokens":{"account_id":"shared-account"},"last_refresh":999}"#,
    )
    .unwrap();

    let stdout = String::from_utf8(
        Command::cargo_bin("codex-switch")
            .unwrap()
            .env("HOME", &home_root)
            .env("PATH", &path)
            .env("CLICOLOR_FORCE", "1")
            .env_remove("NO_COLOR")
            .args(["accounts", "--cached", "--base-root", &base_root_string])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();

    assert!(stdout.contains("\u{1b}[1;36mBackup\u{1b}[0m"));
    assert!(!stdout.contains("\u{1b}[1;36mPrimary\u{1b}[0m"));
}

#[test]
fn accounts_highlights_identity_matching_default_codex_home_runtime_state() {
    let temp = tempdir().unwrap();
    let home_root = temp.path().join("home");
    let default_codex_home = home_root.join(".codex");
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path());

    add_identity("Primary", &base_root_string);
    add_identity("Backup", &base_root_string);

    fs::create_dir_all(&default_codex_home).unwrap();
    fs::write(
        base_root.join("homes").join("primary").join("auth.json"),
        "primary-auth",
    )
    .unwrap();
    fs::write(
        base_root.join("homes").join("backup").join("auth.json"),
        "backup-auth",
    )
    .unwrap();
    fs::write(default_codex_home.join("auth.json"), "different-auth").unwrap();

    let stdout = String::from_utf8(
        Command::cargo_bin("codex-switch")
            .unwrap()
            .env("HOME", &home_root)
            .env("PATH", &path)
            .env("CLICOLOR_FORCE", "1")
            .env_remove("NO_COLOR")
            .args(["accounts", "--base-root", &base_root_string])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();

    assert!(stdout.contains("\u{1b}[1;36mBackup\u{1b}[0m"));
    assert!(!stdout.contains("\u{1b}[1;36mPrimary\u{1b}[0m"));
}

#[test]
fn accounts_weekly_window_shows_day_when_reset_is_not_today() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let path = install_fake_codex(temp.path());

    add_identity("Backup", &base_root_string);

    let stdout = String::from_utf8(
        Command::cargo_bin("codex-switch")
            .unwrap()
            .env("PATH", &path)
            .args(["accounts", "--base-root", &base_root_string])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone(),
    )
    .unwrap();

    let expected = Local.timestamp_opt(1_700_603_600, 0).single().unwrap();
    if expected.date_naive() != Local::now().date_naive() {
        assert!(stdout.contains(&format!("resets {}", expected.format("%a %H:%M"))));
    } else {
        assert!(stdout.contains(&format!("resets {}", expected.format("%H:%M"))));
    }
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

fn install_fake_codex(root: &std::path::Path) -> String {
    let fake_bin_dir = root.join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
identity=$(basename "$CODEX_HOME")

auth_payload='{"authMethod":"chatgpt"}'

case "$identity" in
  primary)
    account_payload='{"account":{"type":"chatgpt","email":"primary@example.com","planType":"plus"},"requiresOpenaiAuth":false}'
    rate_limit_payload='{"rateLimits":{"primary":{"usedPercent":100,"windowDurationMins":300,"resetsAt":1700000000},"secondary":{"usedPercent":96,"windowDurationMins":10080,"resetsAt":1700600000}},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":100,"windowDurationMins":300,"resetsAt":1700000000},"secondary":{"usedPercent":96,"windowDurationMins":10080,"resetsAt":1700600000}}}}'
    ;;
  backup)
    account_payload='{"account":{"type":"chatgpt","email":"backup@example.com","planType":"pro"},"requiresOpenaiAuth":false}'
    rate_limit_payload='{"rateLimits":{"primary":{"usedPercent":20,"windowDurationMins":300,"resetsAt":1700003600},"secondary":{"usedPercent":55,"windowDurationMins":10080,"resetsAt":1700603600}},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"pro","primary":{"usedPercent":20,"windowDurationMins":300,"resetsAt":1700003600},"secondary":{"usedPercent":55,"windowDurationMins":10080,"resetsAt":1700603600}}}}'
    ;;
  .codex)
    account_payload='{"account":{"type":"chatgpt","email":"backup@example.com","planType":"pro"},"requiresOpenaiAuth":false}'
    rate_limit_payload='{"rateLimits":{"primary":{"usedPercent":20,"windowDurationMins":300,"resetsAt":1700003600},"secondary":{"usedPercent":55,"windowDurationMins":10080,"resetsAt":1700603600}},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"pro","primary":{"usedPercent":20,"windowDurationMins":300,"resetsAt":1700003600},"secondary":{"usedPercent":55,"windowDurationMins":10080,"resetsAt":1700603600}}}}'
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

    format!(
        "{}:{}",
        fake_bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    )
}

fn install_fake_codex_with_deactivated_workspace(root: &std::path::Path) -> String {
    let fake_bin_dir = root.join("bin-deactivated");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
identity=$(basename "$CODEX_HOME")

auth_payload='{"authMethod":"chatgpt"}'

case "$identity" in
  healthy)
    account_payload='{"account":{"type":"chatgpt","email":"healthy@example.com","planType":"plus"},"requiresOpenaiAuth":false}'
    rate_limit_payload='{"rateLimits":{"primary":{"usedPercent":25,"windowDurationMins":300,"resetsAt":1700003600},"secondary":{"usedPercent":40,"windowDurationMins":10080,"resetsAt":1700603600}},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":25,"windowDurationMins":300,"resetsAt":1700003600},"secondary":{"usedPercent":40,"windowDurationMins":10080,"resetsAt":1700603600}}}}'
    ;;
  deactivated)
    account_payload='{"account":{"type":"chatgpt","email":"deactivated@example.com","planType":"team"},"requiresOpenaiAuth":false}'
    rate_limit_error='failed to fetch codex rate limits: GET https://chatgpt.com/backend-api/wham/usage failed: 402 Payment Required; content-type=application/json; body={\"detail\":{\"code\":\"deactivated_workspace\"}}'
    ;;
  *)
    account_payload='{"account":{"type":"chatgpt","email":"other@example.com","planType":"free"},"requiresOpenaiAuth":false}'
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
        if [ "$identity" = "deactivated" ]; then
          printf '{"jsonrpc":"2.0","id":"%s","error":{"code":-32603,"message":"%s"}}\n' "$id" "$rate_limit_error"
        else
          printf '{"jsonrpc":"2.0","id":"%s","result":%s}\n' "$id" "$rate_limit_payload"
        fi
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

    format!(
        "{}:{}",
        fake_bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    )
}
