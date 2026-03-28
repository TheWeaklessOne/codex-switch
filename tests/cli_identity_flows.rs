use std::fs;
use std::process::Command as ProcessCommand;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn add_and_list_identities() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("registered Personal Plus"))
        .stdout(predicate::str::contains("id: personal-plus"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "api",
            "--name",
            "API Fallback",
            "--env-var",
            "CLIENT_A_OPENAI_API_KEY",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "api key env var: CLIENT_A_OPENAI_API_KEY",
        ));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["identities", "list", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Personal Plus (personal-plus)"))
        .stdout(predicate::str::contains("API Fallback (api-fallback)"))
        .stdout(predicate::str::contains("shared sessions:"));
}

#[test]
fn add_without_name_uses_mythic_auto_names() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("registered Atlas"))
        .stdout(predicate::str::contains("id: atlas"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "api",
            "--env-var",
            "CLIENT_A_OPENAI_API_KEY",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("registered Vesper"))
        .stdout(predicate::str::contains("id: vesper"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["identities", "list", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Atlas (atlas)"))
        .stdout(predicate::str::contains("Vesper (vesper)"));
}

#[test]
fn add_without_name_skips_taken_auto_name_slug() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Atlas",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("registered Vesper"))
        .stdout(predicate::str::contains("id: vesper"));
}

#[test]
fn remove_identity_clears_registry_and_selection() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let primary_home = base_root.join("homes").join("primary");

    for name in ["Primary", "Backup"] {
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
        .args(["select", "Primary", "--base-root", &base_root_string])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "remove",
            "Primary",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("removed Primary (primary)"))
        .stdout(predicate::str::contains("home removed:"))
        .stdout(predicate::str::contains("selection cleared: yes"));

    assert!(!primary_home.exists());

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["identities", "list", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Backup (backup)"))
        .stdout(predicate::str::contains("Primary (primary)").not());

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["select", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("no identity selected"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Primary",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("registered Primary"))
        .stdout(predicate::str::contains("id: primary"));
}

#[test]
fn verify_identity_via_fake_codex_binary() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
if [ "$1" = "app-server" ]; then
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
  printf '{"jsonrpc":"2.0","id":"%s","result":{"rateLimits":{"primary":{"usedPercent":12,"windowDurationMins":300},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":12,"windowDurationMins":300,"resetsAt":1700000000},"secondary":{"usedPercent":88,"windowDurationMins":10080,"resetsAt":1700003600}}}}}\n' "$id"
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

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    let path = format!(
        "{}:{}",
        fake_bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", path)
        .args([
            "identities",
            "verify",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("authenticated: yes"))
        .stdout(predicate::str::contains("email: qa@example.com"))
        .stdout(predicate::str::contains(
            "codex: primary=12%/300m/resets@1700000000",
        ));
}

#[test]
fn login_runs_managed_chatgpt_login_and_verifies_identity() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("login-log.txt");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
if [ "$1" = "login" ]; then
  printf '%s|%s\n' "${CODEX_HOME##*/}" "$*" >> "$FAKE_LOGIN_LOG"
  exit 0
fi

if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"authMethod":"chatgpt"}}\n' "$id"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"account":{"type":"chatgpt","email":"login@example.com","planType":"plus"},"requiresOpenaiAuth":false}}\n' "$id"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"rateLimits":{"primary":{"usedPercent":15,"windowDurationMins":300},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":15,"windowDurationMins":300,"resetsAt":1700000000},"secondary":null}}}}\n' "$id"
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
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LOGIN_LOG", &log_path)
        .args([
            "identities",
            "login",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "login completed for Personal Plus (personal-plus)",
        ))
        .stdout(predicate::str::contains("launched: codex login"))
        .stdout(predicate::str::contains("email: login@example.com"));

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("personal-plus|login"));
}

#[test]
fn login_succeeds_when_post_login_verify_fails() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("login-log.txt");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
if [ "$1" = "login" ]; then
  printf '%s|%s\n' "${CODEX_HOME##*/}" "$*" >> "$FAKE_LOGIN_LOG"
  exit 0
fi

if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"authMethod":"chatgpt"}}\n' "$id"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"account":{"type":"chatgpt","email":"login@example.com","planType":"plus"},"requiresOpenaiAuth":false}}\n' "$id"
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
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LOGIN_LOG", &log_path)
        .args([
            "identities",
            "login",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "login completed for Personal Plus (personal-plus)",
        ))
        .stdout(predicate::str::contains("verification deferred"))
        .stdout(predicate::str::contains(
            "next: codex-switch identities verify \"Personal Plus\"",
        ))
        .stderr(predicate::str::contains(
            "warning: login succeeded but post-login verification failed:",
        ));

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("personal-plus|login"));
}

#[test]
fn add_with_login_runs_managed_chatgpt_login_and_verifies_identity() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("add-login-log.txt");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
if [ "$1" = "login" ]; then
  printf '%s|%s\n' "${CODEX_HOME##*/}" "$*" >> "$FAKE_LOGIN_LOG"
  exit 0
fi

if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"authMethod":"chatgpt"}}\n' "$id"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"account":{"type":"chatgpt","email":"add-login@example.com","planType":"plus"},"requiresOpenaiAuth":false}}\n' "$id"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"rateLimits":{"primary":{"usedPercent":5,"windowDurationMins":300},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":5,"windowDurationMins":300,"resetsAt":1700000000},"secondary":null}}}}\n' "$id"
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
        .env("PATH", &path)
        .env("FAKE_LOGIN_LOG", &log_path)
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Personal Plus",
            "--login",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("registered Personal Plus"))
        .stdout(predicate::str::contains(
            "login completed for Personal Plus (personal-plus)",
        ))
        .stdout(predicate::str::contains("email: add-login@example.com"));

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("personal-plus|login"));
}

#[test]
fn login_pipes_api_key_into_managed_login() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("api-login-log.txt");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
if [ "$1" = "login" ] && [ "$2" = "--with-api-key" ]; then
  IFS= read -r api_key
  printf '%s|%s|%s\n' "${CODEX_HOME##*/}" "$*" "$api_key" >> "$FAKE_LOGIN_LOG"
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
            "api",
            "--name",
            "API Fallback",
            "--env-var",
            "CLIENT_A_OPENAI_API_KEY",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .env("PATH", &path)
        .env("FAKE_LOGIN_LOG", &log_path)
        .env("CLIENT_A_OPENAI_API_KEY", "sk-live-test")
        .args([
            "identities",
            "login",
            "API Fallback",
            "--no-verify",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "login completed for API Fallback (api-fallback)",
        ))
        .stdout(predicate::str::contains(
            "launched: codex login --with-api-key",
        ))
        .stdout(predicate::str::contains("verification skipped"));

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("api-fallback|login --with-api-key|sk-live-test"));
}

#[test]
fn add_with_login_pipes_api_key_and_can_skip_verify() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("add-api-login-log.txt");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
if [ "$1" = "login" ] && [ "$2" = "--with-api-key" ]; then
  IFS= read -r api_key
  printf '%s|%s|%s\n' "${CODEX_HOME##*/}" "$*" "$api_key" >> "$FAKE_LOGIN_LOG"
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
        .env("PATH", &path)
        .env("FAKE_LOGIN_LOG", &log_path)
        .env("CLIENT_A_OPENAI_API_KEY", "sk-live-test")
        .args([
            "identities",
            "add",
            "api",
            "--name",
            "API Fallback",
            "--env-var",
            "CLIENT_A_OPENAI_API_KEY",
            "--login",
            "--no-verify",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("registered API Fallback"))
        .stdout(predicate::str::contains(
            "login completed for API Fallback (api-fallback)",
        ))
        .stdout(predicate::str::contains("verification skipped"));

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("api-fallback|login --with-api-key|sk-live-test"));
}

#[test]
fn add_with_login_without_name_uses_generated_identity_name() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let fake_bin_dir = temp.path().join("bin");
    let log_path = temp.path().join("add-api-login-auto-name-log.txt");
    fs::create_dir_all(&fake_bin_dir).unwrap();

    let fake_codex_path = fake_bin_dir.join("codex");
    fs::write(
        &fake_codex_path,
        r#"#!/bin/sh
if [ "$1" = "login" ] && [ "$2" = "--with-api-key" ]; then
  IFS= read -r api_key
  printf '%s|%s|%s\n' "${CODEX_HOME##*/}" "$*" "$api_key" >> "$FAKE_LOGIN_LOG"
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
        .env("PATH", &path)
        .env("FAKE_LOGIN_LOG", &log_path)
        .env("CLIENT_A_OPENAI_API_KEY", "sk-live-test")
        .args([
            "identities",
            "add",
            "api",
            "--env-var",
            "CLIENT_A_OPENAI_API_KEY",
            "--login",
            "--no-verify",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("registered Atlas"))
        .stdout(predicate::str::contains(
            "login completed for Atlas (atlas)",
        ))
        .stdout(predicate::str::contains("verification skipped"));

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("atlas|login --with-api-key|sk-live-test"));
}

#[test]
fn add_rejects_no_verify_without_login() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Personal Plus",
            "--no-verify",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "--no-verify requires --login when adding an identity",
        ));
}

#[test]
fn python_helper_reads_rust_written_registry() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Personal Plus",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    let output = ProcessCommand::new("python3")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "tools/codex_identity.py",
            "list",
            "--base-root",
            &base_root_string,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Personal Plus (personal-plus)"));
    assert!(stdout.contains("auth mode: chatgpt"));
}

#[test]
fn rust_cli_reads_legacy_python_registry_shape() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let registry_path = base_root.join("registry.json");
    let legacy_home = base_root.join("homes").join("legacy-helper");
    let shared_sessions = base_root.join("shared").join("sessions");
    fs::create_dir_all(legacy_home.parent().unwrap()).unwrap();
    fs::create_dir_all(&legacy_home).unwrap();
    fs::create_dir_all(&shared_sessions).unwrap();
    fs::write(
        &registry_path,
        format!(
            r#"{{
  "version": 1,
  "identities": {{
    "legacy-helper": {{
      "name": "Legacy Helper",
      "home": "{}",
      "shared_sessions": "{}",
      "auth_mode": "chatgpt",
      "enabled": true,
      "priority": 0,
      "imported_auth": false,
      "created_at": 0
    }}
  }}
}}
"#,
            legacy_home.display(),
            shared_sessions.display()
        ),
    )
    .unwrap();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "list",
            "--base-root",
            &base_root.to_string_lossy(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Legacy Helper (legacy-helper)"))
        .stdout(predicate::str::contains("auth mode: chatgpt"))
        .stdout(predicate::str::contains("shared sessions:"));
}

#[test]
fn inject_copies_auth_and_merges_config_into_target() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let target_home = temp.path().join("target-codex");

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Donor Account",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    let identity_home = base_root.join("homes").join("donor-account");
    fs::write(
        identity_home.join("auth.json"),
        r#"{"auth_mode":"chatgpt","tokens":{"refresh_token":"rt_test"}}"#,
    )
    .unwrap();

    fs::create_dir_all(&target_home).unwrap();
    fs::write(
        target_home.join("config.toml"),
        "model = \"gpt-5\"\napproval_policy = \"never\"\n\n[mcp_servers.playwright]\ncommand = \"npx\"\n",
    )
    .unwrap();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "inject",
            "--identity",
            "Donor Account",
            "--base-root",
            &base_root_string,
            "--target",
            &target_home.to_string_lossy(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("injected auth from Donor Account"))
        .stdout(predicate::str::contains("auth mode: chatgpt"));

    let injected_auth = fs::read_to_string(target_home.join("auth.json")).unwrap();
    assert!(injected_auth.contains("rt_test"));

    let config = fs::read_to_string(target_home.join("config.toml")).unwrap();
    assert!(config.contains("cli_auth_credentials_store = \"file\""));
    assert!(config.contains("forced_login_method = \"chatgpt\""));
    assert!(config.contains("model = \"gpt-5\""));
    assert!(config.contains("approval_policy = \"never\""));
    assert!(config.contains("[mcp_servers.playwright]"));
    assert!(!config.contains("forced_chatgpt_workspace_id"));
}

#[test]
fn inject_fails_when_source_auth_is_missing() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let target_home = temp.path().join("target-codex");

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Empty Account",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "inject",
            "--identity",
            "Empty Account",
            "--base-root",
            &base_root_string,
            "--target",
            &target_home.to_string_lossy(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("missing auth.json"));
}

#[test]
fn inject_requires_explicit_identity_or_auto() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let target_home = temp.path().join("target-codex");

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Some Account",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "inject",
            "--base-root",
            &base_root_string,
            "--target",
            &target_home.to_string_lossy(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "inject requires either --identity <name> or --auto",
        ));
}

#[test]
fn inject_preserves_existing_target_state_when_auth_copy_fails() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let target_home = temp.path().join("target-codex");

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "identities",
            "add",
            "chatgpt",
            "--name",
            "Bad Source",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    let identity_home = base_root.join("homes").join("bad-source");

    // Create auth.json as a directory so copy_file fails after config was written.
    fs::create_dir_all(identity_home.join("auth.json")).unwrap();

    fs::create_dir_all(&target_home).unwrap();
    let original_config = "model = \"gpt-5\"\n";
    fs::write(target_home.join("config.toml"), original_config).unwrap();
    let original_auth = r#"{"auth_mode":"chatgpt","tokens":{"refresh_token":"rt_original"}}"#;
    fs::write(target_home.join("auth.json"), original_auth).unwrap();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "inject",
            "--identity",
            "Bad Source",
            "--base-root",
            &base_root_string,
            "--target",
            &target_home.to_string_lossy(),
        ])
        .assert()
        .failure();

    // Config should be rolled back to original — no managed lines left behind.
    let config = fs::read_to_string(target_home.join("config.toml")).unwrap();
    assert!(
        config.contains("model = \"gpt-5\""),
        "original config should be preserved"
    );
    assert!(
        !config.contains("cli_auth_credentials_store"),
        "managed lines should be rolled back"
    );

    let auth = fs::read_to_string(target_home.join("auth.json")).unwrap();
    assert_eq!(
        auth, original_auth,
        "existing auth.json should remain untouched"
    );
}
