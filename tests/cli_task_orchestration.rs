use assert_cmd::Command;
use codex_switch::domain::identity::IdentityId;
use codex_switch::storage::paths::canonicalize_location;
use codex_switch::task_orchestration::store::AssignmentClaim;
use codex_switch::task_orchestration::{
    CleanupPolicy, DecisionKind, DispatchDecisionId, DispatchDecisionRecord, LaunchMode,
    LineageMode, ProjectExecutionMode, ProjectSubmitRequest, RunKind, SchedulerSettings,
    SchedulerStore, TaskAffinityPolicy, TaskSubmitRequest, WorktreeId, WorktreeRecord,
    WorktreeState,
};
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

#[test]
fn jobs_run_creates_or_reuses_workspace_project_without_explicit_project_name() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();

    enable_scheduler(&base_root_string);

    let output = Command::cargo_bin("codex-switch")
        .unwrap()
        .current_dir(&workspace)
        .args([
            "jobs",
            "run",
            "--title",
            "Ad hoc job",
            "--prompt",
            "do the thing",
            "--base-root",
            &base_root_string,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("job submitted: Ad hoc job"));

    let output = Command::cargo_bin("codex-switch")
        .unwrap()
        .current_dir(&workspace)
        .args([
            "jobs",
            "run",
            "--title",
            "Second ad hoc job",
            "--prompt",
            "do another thing",
            "--base-root",
            &base_root_string,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);

    let store = SchedulerStore::open(&base_root).unwrap();
    let projects = store.list_projects().unwrap();
    assert_eq!(projects.len(), 1);
    assert_eq!(
        projects[0].repo_root,
        canonicalize_location(&workspace).unwrap()
    );
    assert_eq!(store.list_tasks(None).unwrap().len(), 2);
}

#[test]
fn jobs_follow_up_reuses_existing_task_lineage() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();

    enable_scheduler(&base_root_string);

    let output = Command::cargo_bin("codex-switch")
        .unwrap()
        .current_dir(&workspace)
        .args([
            "jobs",
            "run",
            "--title",
            "Ad hoc job",
            "--prompt",
            "initial prompt",
            "--base-root",
            &base_root_string,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let task_id = stdout
        .lines()
        .find_map(|line| line.strip_prefix("task id: "))
        .unwrap()
        .to_string();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "jobs",
            "follow-up",
            &task_id,
            "--prompt",
            "follow-up prompt",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("follow-up queued"));

    let store = SchedulerStore::open(&base_root).unwrap();
    let lineage = store.task_lineage(&task_id).unwrap();
    assert_eq!(lineage.runs.len(), 2);
    assert_eq!(lineage.runs[1].run_kind, RunKind::FollowUp);
    assert_eq!(lineage.runs[0].sequence_no, 1);
    assert_eq!(lineage.runs[1].sequence_no, 2);
    assert_eq!(lineage.task.pending_followup_count, 1);
}

#[test]
fn jobs_run_supports_explicit_workspace_and_git_detection() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let base_root_string = base_root.to_string_lossy().into_owned();

    enable_scheduler(&base_root_string);

    let explicit_workspace = temp.path().join("plain-workspace");
    std::fs::create_dir_all(&explicit_workspace).unwrap();
    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "jobs",
            "run",
            "--workspace",
            &explicit_workspace.to_string_lossy(),
            "--title",
            "Plain workspace job",
            "--prompt",
            "copy mode please",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workspace mode: copy_workspace"))
        .stdout(predicate::str::contains(
            explicit_workspace.to_string_lossy().as_ref(),
        ));

    let git_repo = temp.path().join("git-repo");
    std::fs::create_dir_all(&git_repo).unwrap();
    std::process::Command::new("git")
        .args(["init", &git_repo.to_string_lossy()])
        .status()
        .unwrap();
    let nested = git_repo.join("nested").join("dir");
    std::fs::create_dir_all(&nested).unwrap();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "jobs",
            "run",
            "--workspace",
            &nested.to_string_lossy(),
            "--title",
            "Git workspace job",
            "--prompt",
            "git mode please",
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("workspace mode: git_worktree"))
        .stdout(predicate::str::contains(
            git_repo.to_string_lossy().as_ref(),
        ));
}

#[test]
fn scheduler_reset_state_fails_when_active_runs_exist() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let repo_root = temp.path().join("repo");
    std::fs::create_dir_all(&repo_root).unwrap();
    let mut store = SchedulerStore::open(&base_root).unwrap();
    let (task_id, _) = seed_active_run(&mut store, &repo_root, 4242);
    let base_root_string = base_root.to_string_lossy().into_owned();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["scheduler", "reset-state", "--base-root", &base_root_string])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "scheduler state reset is blocked while 1 active runs, 1 account leases, or 1 worktree leases remain",
        ));

    let store = SchedulerStore::open(&base_root).unwrap();
    assert!(store.get_task(task_id.as_str()).is_ok());
    assert_eq!(store.list_projects().unwrap().len(), 1);
}

#[test]
fn cancel_schedules_cleanup_even_without_confirmed_worker_interrupt() {
    let temp = tempdir().unwrap();
    let base_root = temp.path().join("managed");
    let repo_root = temp.path().join("repo");
    std::fs::create_dir_all(&repo_root).unwrap();
    let mut store = SchedulerStore::open(&base_root).unwrap();
    let (task_id, run_id) = seed_active_run(&mut store, &repo_root, 999_999);
    let base_root_string = base_root.to_string_lossy().into_owned();

    Command::cargo_bin("codex-switch")
        .unwrap()
        .args([
            "tasks",
            "cancel",
            task_id.as_str(),
            "--base-root",
            &base_root_string,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "cleanup scheduled after quarantine for 1 runs without confirmed worker interruption",
        ));

    let store = SchedulerStore::open(&base_root).unwrap();
    let run = store.get_run(run_id.as_str()).unwrap();
    let worktree = store
        .get_worktree(run.assigned_worktree_id.as_ref().unwrap())
        .unwrap();
    assert_eq!(worktree.state, WorktreeState::Corrupted);
    assert!(worktree.cleanup_after.is_some());
}

fn enable_scheduler(base_root: &str) {
    Command::cargo_bin("codex-switch")
        .unwrap()
        .args(["scheduler", "enable", "--base-root", base_root])
        .assert()
        .success()
        .stdout(predicate::str::contains("scheduler_v1 enabled: yes"));
}

fn seed_active_run(
    store: &mut SchedulerStore,
    repo_root: &std::path::Path,
    worker_pid: u32,
) -> (
    codex_switch::task_orchestration::TaskId,
    codex_switch::task_orchestration::TaskRunId,
) {
    let project = store
        .create_project(ProjectSubmitRequest {
            name: "demo".to_string(),
            repo_root: repo_root.to_path_buf(),
            execution_mode: ProjectExecutionMode::CopyWorkspace,
            default_codex_args: Vec::new(),
            default_model_or_profile: None,
            env_allowlist: Vec::new(),
            cleanup_policy: CleanupPolicy::default(),
        })
        .unwrap();
    let snapshot = store
        .submit_task(TaskSubmitRequest {
            project: project.project_id.to_string(),
            title: "Task".to_string(),
            prompt_text: "hello".to_string(),
            prompt_file_path: None,
            priority: 1,
            labels: Vec::new(),
            created_by: "test".to_string(),
            max_runtime_secs: None,
            queue_if_busy: true,
            allow_oversubscribe: false,
            affinity_policy: TaskAffinityPolicy::Spread,
        })
        .unwrap();
    let run = snapshot.runs[0].clone();
    let worktree = WorktreeRecord {
        worktree_id: WorktreeId::from_string("worktree-test"),
        project_id: project.project_id.clone(),
        task_id: snapshot.task.task_id.clone(),
        path: repo_root.join("worktree"),
        execution_mode: ProjectExecutionMode::CopyWorkspace,
        state: WorktreeState::Ready,
        last_run_id: None,
        last_used_at: 1,
        created_at: 1,
        updated_at: 1,
        cleanup_after: None,
        reusable: true,
    };
    let claim = AssignmentClaim {
        run_id: run.run_id.clone(),
        task_id: run.task_id.clone(),
        project_id: project.project_id.clone(),
        identity_id: IdentityId::from_string("identity-1"),
        worktree: worktree.clone(),
        worker_owner_id: "worker-lease-test".to_string(),
        launch_mode: LaunchMode::NewThread,
        lineage_mode: LineageMode::NewThread,
        reason: "test assignment".to_string(),
        decision: DispatchDecisionRecord {
            decision_id: DispatchDecisionId::new(),
            run_id: run.run_id.clone(),
            decision_kind: DecisionKind::Dispatch,
            selected_identity_id: Some(IdentityId::from_string("identity-1")),
            selected_worktree_id: Some(worktree.worktree_id.clone()),
            lineage_mode: LineageMode::NewThread,
            reason: "test assignment".to_string(),
            candidates: Vec::new(),
            policy_snapshot_json: serde_json::json!({}),
            created_at: 1,
        },
        lease_expires_at: 100,
    };
    assert!(store
        .claim_assignment(&claim, &SchedulerSettings::default())
        .unwrap());
    store
        .mark_worker_spawned(run.run_id.as_str(), "worker-lease-test", worker_pid)
        .unwrap();
    store
        .start_run_launching(run.run_id.as_str(), "worker-lease-test", worker_pid, 150)
        .unwrap();
    store
        .mark_run_running(
            run.run_id.as_str(),
            "worker-lease-test",
            "thread-1",
            Some("turn-1"),
            200,
        )
        .unwrap();
    (snapshot.task.task_id, run.run_id)
}
