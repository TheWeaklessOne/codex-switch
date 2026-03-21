use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn registers_projects_and_submits_tasks() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let repo_root = temp.path().join("repo");
    std::fs::create_dir_all(&repo_root).unwrap();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "projects",
            "add",
            "--name",
            "demo",
            "--repo-root",
            &repo_root.to_string_lossy(),
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("project registered: demo"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["projects", "list", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("demo"));

    enable_scheduler(&base_root_string);

    let output = Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "tasks",
            "submit",
            "--project",
            "demo",
            "--title",
            "Implement scheduler",
            "--prompt",
            "Build the orchestration runtime",
            "--base-root",
            &base_root_string,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("task submitted: Implement scheduler"));
    let task_id = stdout
        .lines()
        .find_map(|line| line.strip_prefix("task id: "))
        .unwrap()
        .to_string();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["tasks", "list", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("Implement scheduler"))
        .stdout(predicate::str::contains(&task_id));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "tasks",
            "explain",
            &task_id,
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("no dispatch decision recorded"));
}

#[test]
fn reports_scheduler_health_without_dispatchable_identities() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let repo_root = temp.path().join("repo");
    std::fs::create_dir_all(&repo_root).unwrap();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "projects",
            "add",
            "--name",
            "demo",
            "--repo-root",
            &repo_root.to_string_lossy(),
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    enable_scheduler(&base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "tasks",
            "submit",
            "--project",
            "demo",
            "--title",
            "Implement scheduler",
            "--prompt",
            "Build the orchestration runtime",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "scheduler",
            "tick",
            "--once",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("no dispatches"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["scheduler", "health", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("queued runs: 1"))
        .stdout(predicate::str::contains("active runs: 0"));
}

#[test]
fn rejects_task_submission_until_scheduler_rollout_is_enabled() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let repo_root = temp.path().join("repo");
    std::fs::create_dir_all(&repo_root).unwrap();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "projects",
            "add",
            "--name",
            "demo",
            "--repo-root",
            &repo_root.to_string_lossy(),
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "tasks",
            "submit",
            "--project",
            "demo",
            "--title",
            "Blocked",
            "--prompt",
            "do not run yet",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "scheduler rollout gate scheduler_v1 is disabled",
        ));
}

#[test]
fn reset_state_clears_scheduler_bounded_context() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let repo_root = temp.path().join("repo");
    std::fs::create_dir_all(&repo_root).unwrap();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "projects",
            "add",
            "--name",
            "demo",
            "--repo-root",
            &repo_root.to_string_lossy(),
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    enable_scheduler(&base_root_string);

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "tasks",
            "submit",
            "--project",
            "demo",
            "--title",
            "Reset me",
            "--prompt",
            "temporary",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["scheduler", "reset-state", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("scheduler state reset"));

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["projects", "list", "--base-root", &base_root_string])
        .assert()
        .success()
        .stdout(predicate::str::contains("no projects registered"));
}

fn enable_scheduler(base_root: &str) {
    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["scheduler", "enable", "--base-root", base_root])
        .assert()
        .success()
        .stdout(predicate::str::contains("scheduler_v1 enabled: yes"));
}
