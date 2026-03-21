use std::fs;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn workspace_force_exec_path_is_enabled_only_after_probe_passes() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("workspace-force.log");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
workspace=""
if [ -f "$CODEX_HOME/config.toml" ]; then
  workspace=$(sed -n 's/^forced_chatgpt_workspace_id = "\(.*\)"/\1/p' "$CODEX_HOME/config.toml" | head -n 1)
fi
printf '%s|%s|%s\n' "${CODEX_HOME##*/}" "$workspace" "$*" >> "$FAKE_LAUNCH_LOG"
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
            "Personal Plus",
            "--workspace-id",
            "ws_123",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workspace force probe: pending"))
        .stdout(predicate::str::contains("workspace force active: no"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "workspace-force",
            "show",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workspace force probe: pending"))
        .stdout(predicate::str::contains("workspace force active: no"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LAUNCH_LOG", &log_path)
        .args([
            "exec",
            "--identity",
            "Personal Plus",
            "--base-root",
            &base_root_string,
            "--",
            "exec",
            "before",
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "workspace-force",
            "set",
            "Personal Plus",
            "--status",
            "passed",
            "--notes",
            "Probe B passed",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workspace force probe: passed"))
        .stdout(predicate::str::contains("workspace force active: yes"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LAUNCH_LOG", &log_path)
        .args([
            "exec",
            "--identity",
            "Personal Plus",
            "--base-root",
            &base_root_string,
            "--",
            "exec",
            "after-pass",
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "workspace-force",
            "set",
            "Personal Plus",
            "--status",
            "failed",
            "--notes",
            "Restart was not stable",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workspace force probe: failed"))
        .stdout(predicate::str::contains("workspace force active: no"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LAUNCH_LOG", &log_path)
        .args([
            "exec",
            "--identity",
            "Personal Plus",
            "--base-root",
            &base_root_string,
            "--",
            "exec",
            "after-fail",
        ])
        .assert()
        .success();

    let log = fs::read_to_string(&log_path).unwrap();
    let lines: Vec<_> = log.lines().collect();
    assert_eq!(lines[0], "personal-plus||exec before");
    assert_eq!(lines[1], "personal-plus|ws_123|exec after-pass");
    assert_eq!(lines[2], "personal-plus||exec after-fail");
}

#[test]
fn workspace_force_verify_path_syncs_config_after_probe_passes() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("verify-workspace.log");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
workspace=""
if [ -f "$CODEX_HOME/config.toml" ]; then
  workspace=$(sed -n 's/^forced_chatgpt_workspace_id = "\(.*\)"/\1/p' "$CODEX_HOME/config.toml" | head -n 1)
fi

if [ "$1" = "app-server" ]; then
  printf '%s|%s\n' "${CODEX_HOME##*/}" "$workspace" >> "$FAKE_WORKSPACE_LOG"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"authMethod":"chatgpt"}}\n' "$id"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"account":{"type":"chatgpt","email":"qa@example.com","planType":"plus"},"requiresOpenaiAuth":false}}\n' "$id"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"rateLimits":{"primary":{"usedPercent":12,"windowDurationMins":300},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":12,"windowDurationMins":300,"resetsAt":1700000000},"secondary":null}}}}\n' "$id"
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

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Personal Plus",
            "--workspace-id",
            "ws_123",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "workspace-force",
            "set",
            "Personal Plus",
            "--status",
            "passed",
            "--notes",
            "Probe B passed",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_WORKSPACE_LOG", &log_path)
        .args([
            "identities",
            "verify",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workspace force probe: passed"))
        .stdout(predicate::str::contains("workspace force active: yes"))
        .stdout(predicate::str::contains("email: qa@example.com"));

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("personal-plus|ws_123"));
}

#[test]
fn workspace_force_probe_command_records_live_passed_result() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
config_path="$CODEX_HOME/config.toml"
workspace=""
if [ -f "$config_path" ]; then
  workspace=$(sed -n 's/^forced_chatgpt_workspace_id = "\(.*\)"/\1/p' "$config_path" | head -n 1)
fi

write_config() {
  cat > "$config_path" <<EOF
# Managed by codex-switch
cli_auth_credentials_store = "file"
forced_login_method = "chatgpt"
$1
EOF
}

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
        printf '{"jsonrpc":"2.0","id":"%s","result":{"authMethod":"chatgpt"}}\n' "$id"
        ;;
      *'"method":"config/read"'*)
        workspace=$(sed -n 's/^forced_chatgpt_workspace_id = "\(.*\)"/\1/p' "$config_path" | head -n 1)
        if [ -n "$workspace" ]; then
          printf '{"jsonrpc":"2.0","id":"%s","result":{"config":{"forced_chatgpt_workspace_id":"%s","forced_login_method":"chatgpt"},"origins":{}}}\n' "$id" "$workspace"
        else
          printf '{"jsonrpc":"2.0","id":"%s","result":{"config":{"forced_chatgpt_workspace_id":null,"forced_login_method":"chatgpt"},"origins":{}}}\n' "$id"
        fi
        ;;
      *'"method":"config/value/write"'*'"keyPath":"forced_login_method"'*)
        write_config ""
        printf '{"jsonrpc":"2.0","id":"%s","result":{"filePath":"%s","status":"ok","version":"v1"}}\n' "$id" "$config_path"
        ;;
      *'"method":"config/value/write"'*'"keyPath":"forced_chatgpt_workspace_id"'*)
        workspace_target=$(printf '%s\n' "$line" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')
        write_config "forced_chatgpt_workspace_id = \"$workspace_target\""
        printf '{"jsonrpc":"2.0","id":"%s","result":{"filePath":"%s","status":"ok","version":"v2"}}\n' "$id" "$config_path"
        ;;
      *'"method":"account/read"'*)
        workspace=$(sed -n 's/^forced_chatgpt_workspace_id = "\(.*\)"/\1/p' "$config_path" | head -n 1)
        if [ -n "$workspace" ]; then
          printf '{"jsonrpc":"2.0","id":"%s","result":{"account":{"type":"chatgpt","email":"target@example.com","planType":"plus","accountId":"acct-target"},"requiresOpenaiAuth":false}}\n' "$id"
        else
          printf '{"jsonrpc":"2.0","id":"%s","result":{"account":{"type":"chatgpt","email":"baseline@example.com","planType":"plus","accountId":"acct-baseline"},"requiresOpenaiAuth":false}}\n' "$id"
        fi
        ;;
      *'"method":"account/rateLimits/read"'*)
        workspace=$(sed -n 's/^forced_chatgpt_workspace_id = "\(.*\)"/\1/p' "$config_path" | head -n 1)
        if [ -n "$workspace" ]; then
          printf '{"jsonrpc":"2.0","id":"%s","result":{"rateLimits":{"primary":{"usedPercent":10,"windowDurationMins":300},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":10,"windowDurationMins":300,"resetsAt":1700000000},"secondary":null}}}}\n' "$id"
        else
          printf '{"jsonrpc":"2.0","id":"%s","result":{"rateLimits":{"primary":{"usedPercent":90,"windowDurationMins":300},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":90,"windowDurationMins":300,"resetsAt":1700000000},"secondary":null}}}}\n' "$id"
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
            "Personal Plus",
            "--workspace-id",
            "ws_123",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .args([
            "identities",
            "workspace-force",
            "probe",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workspace force probe: passed"))
        .stdout(predicate::str::contains("baseline changed: yes"))
        .stdout(predicate::str::contains("stable across restarts: yes"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "workspace-force",
            "show",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workspace force probe: passed"))
        .stdout(predicate::str::contains("workspace force active: yes"))
        .stdout(predicate::str::contains(
            "workspace override changed account/quota state and remained stable across restarts",
        ));
}
