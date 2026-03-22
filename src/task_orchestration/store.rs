use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::types::Type;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::domain::identity::{current_timestamp, IdentityId};
use crate::error::{AppError, Result};
use crate::storage::paths::{
    atomic_write, ensure_directory, scheduler_db_path, scheduler_root_path,
    task_artifact_events_path, task_artifact_prompt_path, task_artifact_run_path,
    task_artifacts_path, task_worktrees_path,
};
use crate::task_orchestration::config::{SchedulerControlRecord, SchedulerSettings};
use crate::task_orchestration::domain::*;

const MIGRATION_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS scheduler_process_lock (
    lock_name TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    project_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    repo_root TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    default_codex_args_json TEXT NOT NULL,
    default_model_or_profile TEXT,
    env_allowlist_json TEXT NOT NULL,
    cleanup_policy_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL,
    labels_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    current_lineage_thread_id TEXT,
    preferred_identity_id TEXT,
    last_identity_id TEXT,
    last_checkpoint_id TEXT,
    last_completed_run_id TEXT,
    pending_followup_count INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    run_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    input_artifact_path TEXT NOT NULL,
    requested_at INTEGER NOT NULL,
    assigned_identity_id TEXT,
    assigned_worktree_id TEXT,
    assigned_thread_id TEXT,
    launch_mode TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    started_at INTEGER,
    finished_at INTEGER,
    exit_code INTEGER,
    failure_kind TEXT,
    failure_message TEXT,
    max_runtime_secs INTEGER,
    queue_if_busy INTEGER NOT NULL DEFAULT 1,
    allow_oversubscribe INTEGER NOT NULL DEFAULT 0,
    affinity_policy TEXT NOT NULL DEFAULT 'spread',
    worker_pid INTEGER,
    worker_owner_id TEXT,
    heartbeat_at INTEGER,
    heartbeat_expires_at INTEGER,
    last_turn_id TEXT,
    run_attempt_no INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE RESTRICT,
    UNIQUE(task_id, sequence_no)
);

CREATE TABLE IF NOT EXISTS task_run_inputs (
    run_id TEXT PRIMARY KEY,
    prompt_text TEXT NOT NULL,
    prompt_file_path TEXT,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(run_id) REFERENCES task_runs(run_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS account_runtime (
    identity_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    active_run_id TEXT,
    active_count INTEGER NOT NULL,
    last_dispatch_at INTEGER,
    last_success_at INTEGER,
    last_failure_at INTEGER,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS account_leases (
    identity_id TEXT NOT NULL,
    lease_owner_id TEXT NOT NULL,
    run_id TEXT NOT NULL UNIQUE,
    lease_started_at INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(identity_id, run_id),
    FOREIGN KEY(run_id) REFERENCES task_runs(run_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS worktrees (
    worktree_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    execution_mode TEXT NOT NULL,
    state TEXT NOT NULL,
    last_run_id TEXT,
    last_used_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    cleanup_after INTEGER,
    reusable INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE RESTRICT,
    FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS worktree_leases (
    worktree_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    lease_owner_id TEXT NOT NULL,
    run_id TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(worktree_id) REFERENCES worktrees(worktree_id) ON DELETE RESTRICT,
    FOREIGN KEY(run_id) REFERENCES task_runs(run_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS dispatch_decisions (
    decision_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    decision_kind TEXT NOT NULL,
    selected_identity_id TEXT,
    selected_worktree_id TEXT,
    lineage_mode TEXT NOT NULL,
    reason TEXT NOT NULL,
    candidates_json TEXT NOT NULL,
    policy_snapshot_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(run_id) REFERENCES task_runs(run_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS scheduler_events (
    event_id TEXT PRIMARY KEY,
    project_id TEXT,
    task_id TEXT,
    run_id TEXT,
    event_kind TEXT NOT NULL,
    message TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_project_status_priority_created
    ON tasks(project_id, status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_task_runs_task_sequence
    ON task_runs(task_id, sequence_no);
CREATE INDEX IF NOT EXISTS idx_task_runs_status_requested
    ON task_runs(status, requested_at);
CREATE INDEX IF NOT EXISTS idx_account_runtime_state_updated
    ON account_runtime(state, updated_at);
CREATE INDEX IF NOT EXISTS idx_account_leases_identity
    ON account_leases(identity_id);
CREATE INDEX IF NOT EXISTS idx_worktrees_project_state
    ON worktrees(project_id, state);
CREATE INDEX IF NOT EXISTS idx_dispatch_decisions_run
    ON dispatch_decisions(run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_single_active_per_task
    ON task_runs(task_id)
    WHERE status IN ('assigned', 'launching', 'running', 'handoff_pending');
"#;

const MIGRATION_V2: &str = r#"
CREATE TABLE IF NOT EXISTS scheduler_control (
    control_key TEXT PRIMARY KEY,
    scheduler_v1_enabled INTEGER NOT NULL DEFAULT 0,
    last_quota_refresh_at INTEGER,
    last_quota_refresh_error TEXT,
    last_gc_at INTEGER,
    last_gc_error TEXT,
    updated_at INTEGER NOT NULL
);
"#;

const SCHEDULER_CONTROL_KEY: &str = "default";

#[derive(Debug, Clone)]
pub struct ProjectSubmitRequest {
    pub name: String,
    pub repo_root: PathBuf,
    pub execution_mode: ProjectExecutionMode,
    pub default_codex_args: Vec<String>,
    pub default_model_or_profile: Option<String>,
    pub env_allowlist: Vec<String>,
    pub cleanup_policy: CleanupPolicy,
}

#[derive(Debug, Clone)]
pub struct TaskSubmitRequest {
    pub project: String,
    pub title: String,
    pub prompt_text: String,
    pub prompt_file_path: Option<PathBuf>,
    pub priority: i64,
    pub labels: Vec<String>,
    pub created_by: String,
    pub max_runtime_secs: Option<i64>,
    pub queue_if_busy: bool,
    pub allow_oversubscribe: bool,
    pub affinity_policy: TaskAffinityPolicy,
}

#[derive(Debug, Clone)]
pub struct TaskFollowUpRequest {
    pub task_id: String,
    pub prompt_text: String,
    pub prompt_file_path: Option<PathBuf>,
    pub created_by: String,
    pub max_runtime_secs: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct TaskRetryRequest {
    pub task_id: String,
    pub created_by: String,
}

#[derive(Debug, Clone)]
pub struct QueuedRunContext {
    pub project: ProjectRecord,
    pub task: TaskRecord,
    pub run: TaskRunRecord,
    pub input: TaskRunInputRecord,
    pub reusable_worktree: Option<WorktreeRecord>,
}

#[derive(Debug, Clone)]
pub struct AssignmentClaim {
    pub run_id: TaskRunId,
    pub task_id: TaskId,
    pub project_id: ProjectId,
    pub identity_id: IdentityId,
    pub worktree: WorktreeRecord,
    pub worker_owner_id: String,
    pub launch_mode: LaunchMode,
    pub lineage_mode: LineageMode,
    pub reason: String,
    pub decision: DispatchDecisionRecord,
    pub lease_expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct SchedulerHealthSnapshot {
    pub queued_runs: usize,
    pub active_runs: usize,
    pub stale_runs: usize,
    pub active_identities: usize,
    pub free_identities: usize,
}

#[derive(Debug, Clone)]
pub struct RunBuildOptions {
    pub max_runtime_secs: Option<i64>,
    pub queue_if_busy: bool,
    pub allow_oversubscribe: bool,
    pub affinity_policy: TaskAffinityPolicy,
}

#[derive(Debug, Clone)]
pub struct RunCompletion {
    pub status: TaskRunStatus,
    pub exit_code: Option<i32>,
    pub failure_kind: Option<FailureKind>,
    pub failure_message: Option<String>,
    pub thread_id: Option<String>,
    pub checkpoint_id: Option<String>,
    pub last_identity_id: Option<IdentityId>,
}

#[derive(Debug, Clone)]
pub struct CancelTaskOutcome {
    pub task_id: TaskId,
    pub interrupted_runs: Vec<CanceledRunRecord>,
}

#[derive(Debug, Clone)]
pub struct CanceledRunRecord {
    pub run_id: TaskRunId,
    pub worker_pid: Option<u32>,
    pub worktree_id: Option<WorktreeId>,
}

#[derive(Debug)]
pub struct SchedulerStore {
    base_root: PathBuf,
    connection: Connection,
}

impl SchedulerStore {
    pub fn reset_state(base_root: &Path, settings: &SchedulerSettings) -> Result<()> {
        let owner_id = format!("scheduler-reset-{}", std::process::id());
        let mut store = Self::open(base_root)?;
        store.acquire_scheduler_lock(&owner_id, settings.scheduler_lock_ttl)?;
        let result = store.reset_state_locked(&owner_id);
        if result.is_err() {
            let _ = store.release_scheduler_lock(&owner_id);
        }
        result
    }

    pub fn open(base_root: &Path) -> Result<Self> {
        ensure_directory(&scheduler_root_path(base_root), 0o700)?;
        let path = scheduler_db_path(base_root);
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        let store = Self {
            base_root: base_root.to_path_buf(),
            connection,
        };
        store.run_migrations()?;
        Ok(store)
    }

    pub fn base_root(&self) -> &Path {
        &self.base_root
    }

    fn run_migrations(&self) -> Result<()> {
        self.connection.execute_batch(MIGRATION_V1)?;
        self.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version) VALUES(1)",
            [],
        )?;
        self.connection.execute_batch(MIGRATION_V2)?;
        self.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version) VALUES(2)",
            [],
        )?;
        self.ensure_scheduler_control_row()?;
        Ok(())
    }

    fn ensure_scheduler_control_row(&self) -> Result<()> {
        let now = current_timestamp()?;
        self.connection.execute(
            "INSERT OR IGNORE INTO scheduler_control(control_key, scheduler_v1_enabled, updated_at)
             VALUES(?1, 0, ?2)",
            params![SCHEDULER_CONTROL_KEY, now],
        )?;
        Ok(())
    }

    pub fn scheduler_control(&self) -> Result<SchedulerControlRecord> {
        self.ensure_scheduler_control_row()?;
        self.connection
            .query_row(
                "SELECT scheduler_v1_enabled, last_quota_refresh_at, last_quota_refresh_error, last_gc_at, last_gc_error, updated_at
                 FROM scheduler_control WHERE control_key = ?1",
                params![SCHEDULER_CONTROL_KEY],
                |row| {
                    Ok(SchedulerControlRecord {
                        scheduler_v1_enabled: row.get::<_, i64>(0)? != 0,
                        last_quota_refresh_at: row.get(1)?,
                        last_quota_refresh_error: row.get(2)?,
                        last_gc_at: row.get(3)?,
                        last_gc_error: row.get(4)?,
                        updated_at: row.get(5)?,
                    })
                },
            )
            .map_err(Into::into)
    }

    pub fn set_scheduler_feature_enabled(
        &mut self,
        enabled: bool,
    ) -> Result<SchedulerControlRecord> {
        self.ensure_scheduler_control_row()?;
        let now = current_timestamp()?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "UPDATE scheduler_control
             SET scheduler_v1_enabled = ?2, updated_at = ?3
             WHERE control_key = ?1",
            params![SCHEDULER_CONTROL_KEY, enabled as i64, now],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                None,
                None,
                if enabled {
                    "scheduler_feature_enabled"
                } else {
                    "scheduler_feature_disabled"
                },
                if enabled {
                    "scheduler rollout gate enabled".to_string()
                } else {
                    "scheduler rollout gate disabled".to_string()
                },
                json!({
                    "feature": "scheduler_v1",
                    "enabled": enabled,
                }),
            )?,
        )?;
        tx.commit()?;
        self.scheduler_control()
    }

    pub fn record_quota_refresh_outcome(
        &mut self,
        refreshed_at: i64,
        error: Option<&str>,
        reports: usize,
    ) -> Result<()> {
        self.ensure_scheduler_control_row()?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "UPDATE scheduler_control
             SET last_quota_refresh_at = ?2,
                 last_quota_refresh_error = ?3,
                 updated_at = ?2
             WHERE control_key = ?1",
            params![SCHEDULER_CONTROL_KEY, refreshed_at, error],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                None,
                None,
                "scheduler_quota_refresh",
                if let Some(error) = error {
                    format!("quota refresh completed with errors: {error}")
                } else {
                    format!("quota refresh completed for {reports} identities")
                },
                json!({
                    "reports": reports,
                    "error": error,
                }),
            )?,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn record_gc_outcome(
        &mut self,
        ran_at: i64,
        removed_count: usize,
        error: Option<&str>,
    ) -> Result<()> {
        self.ensure_scheduler_control_row()?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "UPDATE scheduler_control
             SET last_gc_at = ?2,
                 last_gc_error = ?3,
                 updated_at = ?2
             WHERE control_key = ?1",
            params![SCHEDULER_CONTROL_KEY, ran_at, error],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                None,
                None,
                "scheduler_gc",
                if let Some(error) = error {
                    format!("scheduler gc completed with errors: {error}")
                } else {
                    format!("scheduler gc removed {removed_count} worktrees")
                },
                json!({
                    "removed_count": removed_count,
                    "error": error,
                }),
            )?,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn create_project(&mut self, request: ProjectSubmitRequest) -> Result<ProjectRecord> {
        let now = current_timestamp()?;
        let project = ProjectRecord {
            project_id: ProjectId::new(),
            name: request.name,
            repo_root: request.repo_root,
            execution_mode: request.execution_mode,
            default_codex_args: request.default_codex_args,
            default_model_or_profile: request.default_model_or_profile,
            env_allowlist: request.env_allowlist,
            cleanup_policy: request.cleanup_policy,
            created_at: now,
            updated_at: now,
        };
        let tx = self.connection.transaction()?;
        if tx
            .query_row(
                "SELECT 1 FROM projects WHERE name = ?1",
                params![project.name],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Err(AppError::ProjectAlreadyExists {
                project: project.name.clone(),
            });
        }
        tx.execute(
            "INSERT INTO projects(project_id, name, repo_root, execution_mode, default_codex_args_json, default_model_or_profile, env_allowlist_json, cleanup_policy_json, created_at, updated_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                project.project_id.as_str(),
                project.name,
                project.repo_root.to_string_lossy(),
                project.execution_mode.as_str(),
                serde_json::to_string(&project.default_codex_args)?,
                project.default_model_or_profile,
                serde_json::to_string(&project.env_allowlist)?,
                serde_json::to_string(&project.cleanup_policy)?,
                project.created_at,
                project.updated_at
            ],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                Some(project.project_id.clone()),
                None,
                None,
                "project_created",
                format!("project {} created", project.name),
                json!({
                    "project_id": project.project_id,
                    "execution_mode": project.execution_mode.as_str(),
                    "repo_root": project.repo_root,
                }),
            )?,
        )?;
        tx.commit()?;
        Ok(project)
    }

    pub fn list_projects(&self) -> Result<Vec<ProjectRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT project_id, name, repo_root, execution_mode, default_codex_args_json, default_model_or_profile, env_allowlist_json, cleanup_policy_json, created_at, updated_at
             FROM projects ORDER BY name",
        )?;
        let mut rows = statement.query([])?;
        let mut projects = Vec::new();
        while let Some(row) = rows.next()? {
            projects.push(project_from_row(row)?);
        }
        Ok(projects)
    }

    pub fn get_project(&self, name_or_id: &str) -> Result<ProjectRecord> {
        let mut statement = self.connection.prepare(
            "SELECT project_id, name, repo_root, execution_mode, default_codex_args_json, default_model_or_profile, env_allowlist_json, cleanup_policy_json, created_at, updated_at
             FROM projects
             WHERE project_id = ?1 OR name = ?1",
        )?;
        statement
            .query_row(params![name_or_id], project_from_row)
            .optional()?
            .ok_or_else(|| AppError::ProjectNotFound {
                project: name_or_id.to_string(),
            })
    }

    pub fn projects_for_repo_root(&self, repo_root: &Path) -> Result<Vec<ProjectRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT project_id, name, repo_root, execution_mode, default_codex_args_json, default_model_or_profile, env_allowlist_json, cleanup_policy_json, created_at, updated_at
             FROM projects
             WHERE repo_root = ?1
             ORDER BY created_at, name, project_id",
        )?;
        let mut rows = statement.query(params![repo_root.to_string_lossy().as_ref()])?;
        let mut projects = Vec::new();
        while let Some(row) = rows.next()? {
            projects.push(project_from_row(row)?);
        }
        Ok(projects)
    }

    pub fn resolve_or_create_workspace_project(
        &mut self,
        repo_root: &Path,
        execution_mode: ProjectExecutionMode,
    ) -> Result<ProjectRecord> {
        let mut matches = self.projects_for_repo_root(repo_root)?;
        match matches.len() {
            0 => self.create_project(ProjectSubmitRequest {
                name: workspace_project_name(repo_root),
                repo_root: repo_root.to_path_buf(),
                execution_mode,
                default_codex_args: Vec::new(),
                default_model_or_profile: None,
                env_allowlist: Vec::new(),
                cleanup_policy: CleanupPolicy::default(),
            }),
            1 => Ok(matches.remove(0)),
            _ => Err(AppError::WorkspaceProjectAmbiguous {
                workspace_root: repo_root.to_path_buf(),
                projects: matches.into_iter().map(|project| project.name).collect(),
            }),
        }
    }

    pub fn submit_task(&mut self, request: TaskSubmitRequest) -> Result<TaskLineageSnapshot> {
        let project = self.get_project(&request.project)?;
        let now = current_timestamp()?;
        let task = TaskRecord {
            task_id: TaskId::new(),
            project_id: project.project_id.clone(),
            title: request.title,
            status: TaskStatus::Queued,
            priority: request.priority,
            labels: request.labels,
            created_by: request.created_by,
            created_at: now,
            updated_at: now,
            current_lineage_thread_id: None,
            preferred_identity_id: None,
            last_identity_id: None,
            last_checkpoint_id: None,
            last_completed_run_id: None,
            pending_followup_count: 0,
        };
        let run = self.build_run_record(
            &task,
            1,
            RunKind::Initial,
            now,
            RunBuildOptions {
                max_runtime_secs: request.max_runtime_secs,
                queue_if_busy: request.queue_if_busy,
                allow_oversubscribe: request.allow_oversubscribe,
                affinity_policy: request.affinity_policy,
            },
        );
        let input = TaskRunInputRecord {
            run_id: run.run_id.clone(),
            prompt_text: request.prompt_text,
            prompt_file_path: request.prompt_file_path,
            created_at: now,
        };
        let tx = self.connection.transaction()?;
        insert_task_tx(&tx, &task)?;
        insert_run_tx(&tx, &run)?;
        insert_run_input_tx(&tx, &input)?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                Some(task.project_id.clone()),
                Some(task.task_id.clone()),
                Some(run.run_id.clone()),
                "task_submitted",
                format!("task {} submitted", task.title),
                json!({
                    "task_id": task.task_id,
                    "run_id": run.run_id,
                    "priority": task.priority,
                    "labels": task.labels,
                }),
            )?,
        )?;
        tx.commit()?;
        self.persist_prompt_artifacts_after_commit(&task.task_id, &run.run_id, &input.prompt_text);
        Ok(TaskLineageSnapshot {
            task,
            runs: vec![run.clone()],
            latest_input: Some(input),
            latest_dispatch_decision: None,
        })
    }

    pub fn submit_follow_up(&mut self, request: TaskFollowUpRequest) -> Result<TaskRunRecord> {
        let task = self.get_task(&request.task_id)?;
        let sequence_no = self.next_sequence_no(&task.task_id)? + 1;
        let now = current_timestamp()?;
        let run = self.build_run_record(
            &task,
            sequence_no,
            RunKind::FollowUp,
            now,
            RunBuildOptions {
                max_runtime_secs: request.max_runtime_secs,
                queue_if_busy: true,
                allow_oversubscribe: false,
                affinity_policy: TaskAffinityPolicy::PreferSameIdentity,
            },
        );
        let input = TaskRunInputRecord {
            run_id: run.run_id.clone(),
            prompt_text: request.prompt_text,
            prompt_file_path: request.prompt_file_path,
            created_at: now,
        };
        let tx = self.connection.transaction()?;
        insert_run_tx(&tx, &run)?;
        insert_run_input_tx(&tx, &input)?;
        tx.execute(
            "UPDATE tasks SET pending_followup_count = pending_followup_count + 1, updated_at = ?2 WHERE task_id = ?1",
            params![task.task_id.as_str(), now],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                Some(task.project_id.clone()),
                Some(task.task_id.clone()),
                Some(run.run_id.clone()),
                "task_follow_up_submitted",
                format!("follow-up submitted for {}", task.title),
                json!({
                    "task_id": task.task_id,
                    "run_id": run.run_id,
                    "created_by": request.created_by,
                }),
            )?,
        )?;
        tx.commit()?;
        self.persist_prompt_artifacts_after_commit(&task.task_id, &run.run_id, &input.prompt_text);
        Ok(run)
    }

    pub fn retry_task(&mut self, request: TaskRetryRequest) -> Result<TaskRunRecord> {
        let task = self.get_task(&request.task_id)?;
        let latest_input = self.latest_task_input(&task.task_id)?.ok_or_else(|| {
            AppError::TaskRetryInputMissing {
                task_id: task.task_id.to_string(),
            }
        })?;
        let sequence_no = self.next_sequence_no(&task.task_id)? + 1;
        let now = current_timestamp()?;
        let run = self.build_run_record(
            &task,
            sequence_no,
            RunKind::Retry,
            now,
            RunBuildOptions {
                max_runtime_secs: None,
                queue_if_busy: true,
                allow_oversubscribe: false,
                affinity_policy: TaskAffinityPolicy::PreferSameIdentity,
            },
        );
        let retry_input = TaskRunInputRecord {
            run_id: run.run_id.clone(),
            prompt_text: latest_input.prompt_text,
            prompt_file_path: latest_input.prompt_file_path,
            created_at: now,
        };
        let tx = self.connection.transaction()?;
        insert_run_tx(&tx, &run)?;
        insert_run_input_tx(&tx, &retry_input)?;
        tx.execute(
            "UPDATE tasks SET status = 'queued', updated_at = ?2 WHERE task_id = ?1",
            params![task.task_id.as_str(), now],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                Some(task.project_id.clone()),
                Some(task.task_id.clone()),
                Some(run.run_id.clone()),
                "task_retried",
                format!("retry queued for {}", task.title),
                json!({
                    "task_id": task.task_id,
                    "run_id": run.run_id,
                    "created_by": request.created_by,
                }),
            )?,
        )?;
        tx.commit()?;
        self.persist_prompt_artifacts_after_commit(
            &task.task_id,
            &run.run_id,
            &retry_input.prompt_text,
        );
        Ok(run)
    }

    fn persist_prompt_artifacts_after_commit(
        &self,
        task_id: &TaskId,
        run_id: &TaskRunId,
        prompt: &str,
    ) {
        if let Err(error) = persist_run_prompt(&self.base_root, task_id, run_id, prompt) {
            if let Ok(event) = SchedulerEventRecord::new(
                None,
                Some(task_id.clone()),
                Some(run_id.clone()),
                "prompt_artifact_persist_failed",
                format!("prompt artifact persistence failed for run {run_id}: {error}"),
                json!({
                    "run_id": run_id,
                    "error": error.to_string(),
                }),
            ) {
                let _ = self.append_event(&event);
            }
        }
    }

    pub fn list_tasks(&self, project: Option<&str>) -> Result<Vec<TaskRecord>> {
        let sql = if project.is_some() {
            "SELECT task_id, project_id, title, status, priority, labels_json, created_by, created_at, updated_at, current_lineage_thread_id, preferred_identity_id, last_identity_id, last_checkpoint_id, last_completed_run_id, pending_followup_count
             FROM tasks
             WHERE project_id = (SELECT project_id FROM projects WHERE name = ?1 OR project_id = ?1)
             ORDER BY priority DESC, created_at ASC"
        } else {
            "SELECT task_id, project_id, title, status, priority, labels_json, created_by, created_at, updated_at, current_lineage_thread_id, preferred_identity_id, last_identity_id, last_checkpoint_id, last_completed_run_id, pending_followup_count
             FROM tasks
             ORDER BY priority DESC, created_at ASC"
        };
        let mut statement = self.connection.prepare(sql)?;
        let mut rows = match project {
            Some(project) => statement.query(params![project])?,
            None => statement.query([])?,
        };
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(task_from_row(row)?);
        }
        Ok(tasks)
    }

    pub fn get_task(&self, task_id: &str) -> Result<TaskRecord> {
        let mut statement = self.connection.prepare(
            "SELECT task_id, project_id, title, status, priority, labels_json, created_by, created_at, updated_at, current_lineage_thread_id, preferred_identity_id, last_identity_id, last_checkpoint_id, last_completed_run_id, pending_followup_count
             FROM tasks WHERE task_id = ?1",
        )?;
        statement
            .query_row(params![task_id], task_from_row)
            .optional()?
            .ok_or_else(|| AppError::TaskNotFound {
                task_id: task_id.to_string(),
            })
    }

    pub fn task_lineage(&self, task_id: &str) -> Result<TaskLineageSnapshot> {
        let task = self.get_task(task_id)?;
        let mut statement = self.connection.prepare(
            "SELECT run_id, task_id, sequence_no, run_kind, status, input_artifact_path, requested_at, assigned_identity_id, assigned_worktree_id, assigned_thread_id, launch_mode, retry_count, started_at, finished_at, exit_code, failure_kind, failure_message, max_runtime_secs, queue_if_busy, allow_oversubscribe, affinity_policy, worker_pid, worker_owner_id, heartbeat_at, heartbeat_expires_at, last_turn_id, run_attempt_no
             FROM task_runs WHERE task_id = ?1 ORDER BY sequence_no ASC",
        )?;
        let runs = statement
            .query_map(params![task_id], run_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(TaskLineageSnapshot {
            latest_input: self.latest_task_input(&task.task_id)?,
            latest_dispatch_decision: self.latest_dispatch_for_task(&task.task_id)?,
            task,
            runs,
        })
    }

    pub fn get_run(&self, run_id: &str) -> Result<TaskRunRecord> {
        let mut statement = self.connection.prepare(
            "SELECT run_id, task_id, sequence_no, run_kind, status, input_artifact_path, requested_at, assigned_identity_id, assigned_worktree_id, assigned_thread_id, launch_mode, retry_count, started_at, finished_at, exit_code, failure_kind, failure_message, max_runtime_secs, queue_if_busy, allow_oversubscribe, affinity_policy, worker_pid, worker_owner_id, heartbeat_at, heartbeat_expires_at, last_turn_id, run_attempt_no
             FROM task_runs WHERE run_id = ?1",
        )?;
        statement
            .query_row(params![run_id], run_from_row)
            .optional()?
            .ok_or_else(|| AppError::TaskRunNotFound {
                run_id: run_id.to_string(),
            })
    }

    pub fn run_context(
        &self,
        run_id: &str,
    ) -> Result<(ProjectRecord, TaskRecord, TaskRunRecord, TaskRunInputRecord)> {
        let run = self.get_run(run_id)?;
        let task = self.get_task(run.task_id.as_str())?;
        let project = self.get_project(task.project_id.as_str())?;
        let input = self.run_input(&run.run_id)?;
        Ok((project, task, run, input))
    }

    pub fn update_task_preferred_identity(
        &mut self,
        task_id: &TaskId,
        identity_id: &IdentityId,
    ) -> Result<()> {
        self.connection.execute(
            "UPDATE tasks SET preferred_identity_id = ?2 WHERE task_id = ?1",
            params![task_id.as_str(), identity_id.as_str()],
        )?;
        Ok(())
    }

    pub fn task_events(&self, task_id: &str) -> Result<Vec<SchedulerEventRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT event_id, project_id, task_id, run_id, event_kind, message, payload_json, created_at
             FROM scheduler_events WHERE task_id = ?1 ORDER BY created_at ASC, event_id ASC",
        )?;
        let events = statement
            .query_map(params![task_id], scheduler_event_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(events)
    }

    pub fn acquire_scheduler_lock(&mut self, owner_id: &str, ttl: Duration) -> Result<()> {
        let now = current_timestamp()?;
        let expires_at = now + ttl.as_secs() as i64;
        let tx = self.connection.transaction()?;
        let existing = tx
            .query_row(
                "SELECT owner_id, expires_at FROM scheduler_process_lock WHERE lock_name = 'dispatcher'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        if let Some((existing_owner, existing_expiry)) = existing {
            if existing_expiry >= now && existing_owner != owner_id {
                return Err(AppError::SchedulerAlreadyRunning {
                    owner_id: existing_owner,
                });
            }
        }
        tx.execute(
            "INSERT INTO scheduler_process_lock(lock_name, owner_id, heartbeat_at, expires_at)
             VALUES('dispatcher', ?1, ?2, ?3)
             ON CONFLICT(lock_name) DO UPDATE SET owner_id = excluded.owner_id, heartbeat_at = excluded.heartbeat_at, expires_at = excluded.expires_at",
            params![owner_id, now, expires_at],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn heartbeat_scheduler_lock(&mut self, owner_id: &str, ttl: Duration) -> Result<()> {
        let now = current_timestamp()?;
        let expires_at = now + ttl.as_secs() as i64;
        let updated = self.connection.execute(
            "UPDATE scheduler_process_lock SET heartbeat_at = ?2, expires_at = ?3 WHERE lock_name = 'dispatcher' AND owner_id = ?1",
            params![owner_id, now, expires_at],
        )?;
        if updated == 0 {
            return Err(AppError::SchedulerAlreadyRunning {
                owner_id: "unknown".to_string(),
            });
        }
        Ok(())
    }

    pub fn release_scheduler_lock(&mut self, owner_id: &str) -> Result<()> {
        self.connection.execute(
            "DELETE FROM scheduler_process_lock WHERE lock_name = 'dispatcher' AND owner_id = ?1",
            params![owner_id],
        )?;
        Ok(())
    }

    pub fn queued_runs(&self) -> Result<Vec<QueuedRunContext>> {
        let mut statement = self.connection.prepare(
            "SELECT
                r.run_id, r.task_id, r.sequence_no, r.run_kind, r.status, r.input_artifact_path, r.requested_at,
                r.assigned_identity_id, r.assigned_worktree_id, r.assigned_thread_id, r.launch_mode, r.retry_count,
                r.started_at, r.finished_at, r.exit_code, r.failure_kind, r.failure_message, r.max_runtime_secs,
                r.queue_if_busy, r.allow_oversubscribe, r.affinity_policy, r.worker_pid, r.worker_owner_id,
                r.heartbeat_at, r.heartbeat_expires_at, r.last_turn_id, r.run_attempt_no,
                t.task_id, t.project_id, t.title, t.status, t.priority, t.labels_json, t.created_by, t.created_at, t.updated_at,
                t.current_lineage_thread_id, t.preferred_identity_id, t.last_identity_id, t.last_checkpoint_id, t.last_completed_run_id, t.pending_followup_count,
                p.project_id, p.name, p.repo_root, p.execution_mode, p.default_codex_args_json, p.default_model_or_profile, p.env_allowlist_json, p.cleanup_policy_json, p.created_at, p.updated_at
             FROM task_runs r
             JOIN tasks t ON t.task_id = r.task_id
             JOIN projects p ON p.project_id = t.project_id
             WHERE r.status = 'pending_assignment'
             ORDER BY t.priority DESC, r.requested_at ASC, r.run_id ASC",
        )?;
        let mut rows = statement.query([])?;
        let mut queued = Vec::new();
        while let Some(row) = rows.next()? {
            let run = run_from_row_prefix(row, 0)?;
            let task = task_from_row_prefix(row, 27)?;
            let project = project_from_row_prefix(row, 42)?;
            queued.push(QueuedRunContext {
                reusable_worktree: self.reusable_worktree_for_task(&task.task_id)?,
                input: self.run_input(&run.run_id)?,
                project,
                task,
                run,
            });
        }
        Ok(queued)
    }

    pub fn active_account_leases(&self) -> Result<Vec<AccountLeaseRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT identity_id, lease_owner_id, run_id, lease_started_at, heartbeat_at, expires_at, updated_at
             FROM account_leases ORDER BY identity_id, run_id",
        )?;
        let leases = statement
            .query_map([], account_lease_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(leases)
    }

    pub fn account_runtime(&self) -> Result<Vec<AccountRuntimeRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT identity_id, state, active_run_id, active_count, last_dispatch_at, last_success_at, last_failure_at, updated_at
             FROM account_runtime ORDER BY identity_id",
        )?;
        let runtimes = statement
            .query_map([], account_runtime_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(runtimes)
    }

    pub fn worktree_leases(&self) -> Result<Vec<WorktreeLeaseRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT worktree_id, project_id, lease_owner_id, run_id, path, heartbeat_at, expires_at, created_at
             FROM worktree_leases ORDER BY worktree_id",
        )?;
        let leases = statement
            .query_map([], worktree_lease_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(leases)
    }

    pub fn claim_assignment(
        &mut self,
        claim: &AssignmentClaim,
        settings: &SchedulerSettings,
    ) -> Result<bool> {
        let now = current_timestamp()?;
        let tx = self.connection.transaction()?;
        let active_for_task: i64 = tx.query_row(
            "SELECT COUNT(1) FROM task_runs WHERE task_id = ?1 AND run_id != ?2 AND status IN ('assigned','launching','running','handoff_pending')",
            params![claim.task_id.as_str(), claim.run_id.as_str()],
            |row| row.get(0),
        )?;
        if active_for_task > 0 {
            return Ok(false);
        }
        let run = tx
            .query_row(
                "SELECT status, allow_oversubscribe FROM task_runs WHERE run_id = ?1",
                params![claim.run_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0)),
            )
            .optional()?;
        let Some((status, allow_oversubscribe)) = run else {
            return Err(AppError::TaskRunNotFound {
                run_id: claim.run_id.to_string(),
            });
        };
        if status != TaskRunStatus::PendingAssignment.as_str() {
            return Ok(false);
        }

        let lease_count: i64 = tx.query_row(
            "SELECT COUNT(1) FROM account_leases WHERE identity_id = ?1",
            params![claim.identity_id.as_str()],
            |row| row.get(0),
        )?;
        if lease_count >= i64::from(settings.max_active_runs_per_identity)
            && !(allow_oversubscribe || settings.allow_oversubscribe_when_pool_full)
        {
            return Ok(false);
        }

        let worktree_busy = tx
            .query_row(
                "SELECT 1 FROM worktree_leases WHERE worktree_id = ?1",
                params![claim.worktree.worktree_id.as_str()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if worktree_busy {
            return Ok(false);
        }

        tx.execute(
            "UPDATE task_runs
             SET status = 'assigned', assigned_identity_id = ?2, assigned_worktree_id = ?3, launch_mode = ?4, worker_owner_id = ?5, heartbeat_expires_at = ?6
             WHERE run_id = ?1 AND status = 'pending_assignment'",
            params![
                claim.run_id.as_str(),
                claim.identity_id.as_str(),
                claim.worktree.worktree_id.as_str(),
                claim.launch_mode.as_str(),
                claim.worker_owner_id,
                claim.lease_expires_at,
            ],
        )?;
        if tx.changes() == 0 {
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO account_leases(identity_id, lease_owner_id, run_id, lease_started_at, heartbeat_at, expires_at, updated_at)
             VALUES(?1, ?2, ?3, ?4, ?4, ?5, ?4)",
            params![
                claim.identity_id.as_str(),
                claim.worker_owner_id,
                claim.run_id.as_str(),
                now,
                claim.lease_expires_at
            ],
        )?;
        tx.execute(
            "INSERT INTO worktrees(worktree_id, project_id, task_id, path, execution_mode, state, last_run_id, last_used_at, created_at, updated_at, cleanup_after, reusable)
             VALUES(?1, ?2, ?3, ?4, ?5, 'leased', ?6, ?7, ?8, ?8, NULL, 1)
             ON CONFLICT(worktree_id) DO UPDATE SET
                state = 'leased',
                last_run_id = excluded.last_run_id,
                last_used_at = excluded.last_used_at,
                updated_at = excluded.updated_at",
            params![
                claim.worktree.worktree_id.as_str(),
                claim.project_id.as_str(),
                claim.task_id.as_str(),
                claim.worktree.path.to_string_lossy(),
                claim.worktree.execution_mode.as_str(),
                claim.run_id.as_str(),
                now,
                claim.worktree.created_at,
            ],
        )?;
        tx.execute(
            "INSERT INTO worktree_leases(worktree_id, project_id, lease_owner_id, run_id, path, heartbeat_at, expires_at, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?6)",
            params![
                claim.worktree.worktree_id.as_str(),
                claim.project_id.as_str(),
                claim.worker_owner_id,
                claim.run_id.as_str(),
                claim.worktree.path.to_string_lossy(),
                now,
                claim.lease_expires_at,
            ],
        )?;
        upsert_account_runtime_tx(
            &tx,
            &AccountRuntimeRecord {
                identity_id: claim.identity_id.clone(),
                state: AccountRuntimeState::Reserved,
                active_run_id: Some(claim.run_id.clone()),
                active_count: (lease_count + 1) as u32,
                last_dispatch_at: Some(now),
                last_success_at: None,
                last_failure_at: None,
                updated_at: now,
            },
        )?;
        tx.execute(
            "INSERT INTO dispatch_decisions(decision_id, run_id, decision_kind, selected_identity_id, selected_worktree_id, lineage_mode, reason, candidates_json, policy_snapshot_json, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                claim.decision.decision_id.as_str(),
                claim.decision.run_id.as_str(),
                claim.decision.decision_kind.as_str(),
                claim
                    .decision
                    .selected_identity_id
                    .as_ref()
                    .map(IdentityId::as_str),
                claim
                    .decision
                    .selected_worktree_id
                    .as_ref()
                    .map(WorktreeId::as_str),
                claim.decision.lineage_mode.as_str(),
                claim.decision.reason,
                serde_json::to_string(&claim.decision.candidates)?,
                serde_json::to_string(&claim.decision.policy_snapshot_json)?,
                claim.decision.created_at,
            ],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                Some(claim.project_id.clone()),
                Some(claim.task_id.clone()),
                Some(claim.run_id.clone()),
                "run_assigned",
                claim.reason.clone(),
                json!({
                    "run_id": claim.run_id,
                    "identity_id": claim.identity_id,
                    "worktree_id": claim.worktree.worktree_id,
                    "lineage_mode": claim.lineage_mode.as_str(),
                    "launch_mode": claim.launch_mode.as_str(),
                }),
            )?,
        )?;
        tx.execute(
            "UPDATE tasks SET status = 'running', updated_at = ?2 WHERE task_id = ?1",
            params![claim.task_id.as_str(), now],
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn mark_worker_spawned(
        &mut self,
        run_id: &str,
        worker_owner_id: &str,
        worker_pid: u32,
    ) -> Result<()> {
        let now = current_timestamp()?;
        let updated = self.connection.execute(
            "UPDATE task_runs
             SET worker_pid = ?3, run_attempt_no = run_attempt_no + 1, heartbeat_at = ?4
             WHERE run_id = ?1 AND status = 'assigned' AND worker_owner_id = ?2",
            params![run_id, worker_owner_id, worker_pid, now],
        )?;
        if updated == 0 {
            return Err(AppError::WorkerNotActive {
                run_id: run_id.to_string(),
            });
        }
        Ok(())
    }

    pub fn start_run_launching(
        &mut self,
        run_id: &str,
        worker_owner_id: &str,
        worker_pid: u32,
        lease_expires_at: i64,
    ) -> Result<TaskRunRecord> {
        let now = current_timestamp()?;
        let tx = self.connection.transaction()?;
        let updated = tx.execute(
            "UPDATE task_runs
             SET status = 'launching', worker_pid = ?3, heartbeat_at = ?4, heartbeat_expires_at = ?5
             WHERE run_id = ?1 AND status IN ('assigned', 'launching') AND worker_owner_id = ?2",
            params![run_id, worker_owner_id, worker_pid, now, lease_expires_at],
        )?;
        if updated == 0 {
            return Err(AppError::WorkerNotActive {
                run_id: run_id.to_string(),
            });
        }
        let account_updates = tx.execute(
            "UPDATE account_leases SET heartbeat_at = ?3, expires_at = ?4, updated_at = ?3 WHERE run_id = ?1 AND lease_owner_id = ?2",
            params![run_id, worker_owner_id, now, lease_expires_at],
        )?;
        let worktree_updates = tx.execute(
            "UPDATE worktree_leases SET heartbeat_at = ?3, expires_at = ?4 WHERE run_id = ?1 AND lease_owner_id = ?2",
            params![run_id, worker_owner_id, now, lease_expires_at],
        )?;
        if account_updates == 0 || worktree_updates == 0 {
            return Err(AppError::WorkerNotActive {
                run_id: run_id.to_string(),
            });
        }
        tx.execute(
            "UPDATE account_runtime
             SET state = 'launching', updated_at = ?2
             WHERE identity_id = (SELECT assigned_identity_id FROM task_runs WHERE run_id = ?1)",
            params![run_id, now],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                None,
                Some(TaskRunId::from_string(run_id)),
                "worker_started",
                format!("worker started for run {run_id}"),
                json!({
                    "worker_owner_id": worker_owner_id,
                    "worker_pid": worker_pid,
                }),
            )?,
        )?;
        let run = tx
            .query_row(
                "SELECT run_id, task_id, sequence_no, run_kind, status, input_artifact_path, requested_at, assigned_identity_id, assigned_worktree_id, assigned_thread_id, launch_mode, retry_count, started_at, finished_at, exit_code, failure_kind, failure_message, max_runtime_secs, queue_if_busy, allow_oversubscribe, affinity_policy, worker_pid, worker_owner_id, heartbeat_at, heartbeat_expires_at, last_turn_id, run_attempt_no
                 FROM task_runs WHERE run_id = ?1",
                params![run_id],
                run_from_row,
            )?;
        tx.commit()?;
        Ok(run)
    }

    pub fn mark_run_running(
        &mut self,
        run_id: &str,
        worker_owner_id: &str,
        thread_id: &str,
        turn_id: Option<&str>,
        lease_expires_at: i64,
    ) -> Result<()> {
        let now = current_timestamp()?;
        let tx = self.connection.transaction()?;
        let updated = tx.execute(
            "UPDATE task_runs
             SET status = 'running', started_at = COALESCE(started_at, ?3), assigned_thread_id = ?4, last_turn_id = COALESCE(?5, last_turn_id), heartbeat_at = ?3, heartbeat_expires_at = ?6
             WHERE run_id = ?1 AND worker_owner_id = ?2 AND status IN ('assigned', 'launching', 'running')",
            params![run_id, worker_owner_id, now, thread_id, turn_id, lease_expires_at],
        )?;
        if updated == 0 {
            return Err(AppError::WorkerNotActive {
                run_id: run_id.to_string(),
            });
        }
        let account_updates = tx.execute(
            "UPDATE account_leases SET heartbeat_at = ?3, expires_at = ?4, updated_at = ?3 WHERE run_id = ?1 AND lease_owner_id = ?2",
            params![run_id, worker_owner_id, now, lease_expires_at],
        )?;
        let worktree_updates = tx.execute(
            "UPDATE worktree_leases SET heartbeat_at = ?3, expires_at = ?4 WHERE run_id = ?1 AND lease_owner_id = ?2",
            params![run_id, worker_owner_id, now, lease_expires_at],
        )?;
        if account_updates == 0 || worktree_updates == 0 {
            return Err(AppError::WorkerNotActive {
                run_id: run_id.to_string(),
            });
        }
        tx.execute(
            "UPDATE account_runtime SET state = 'running', updated_at = ?2 WHERE identity_id = (SELECT assigned_identity_id FROM task_runs WHERE run_id = ?1)",
            params![run_id, now],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                None,
                Some(TaskRunId::from_string(run_id)),
                "run_running",
                format!("run {run_id} is running"),
                json!({
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                }),
            )?,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn heartbeat_run(
        &mut self,
        run_id: &str,
        worker_owner_id: &str,
        turn_id: Option<&str>,
        lease_expires_at: i64,
    ) -> Result<()> {
        let now = current_timestamp()?;
        let tx = self.connection.transaction()?;
        let updated = tx.execute(
            "UPDATE task_runs SET heartbeat_at = ?3, heartbeat_expires_at = ?4, last_turn_id = COALESCE(?5, last_turn_id)
             WHERE run_id = ?1 AND worker_owner_id = ?2 AND status IN ('launching', 'running', 'handoff_pending')",
            params![run_id, worker_owner_id, now, lease_expires_at, turn_id],
        )?;
        if updated == 0 {
            return Err(AppError::WorkerNotActive {
                run_id: run_id.to_string(),
            });
        }
        let account_updates = tx.execute(
            "UPDATE account_leases SET heartbeat_at = ?3, expires_at = ?4, updated_at = ?3 WHERE run_id = ?1 AND lease_owner_id = ?2",
            params![run_id, worker_owner_id, now, lease_expires_at],
        )?;
        let worktree_updates = tx.execute(
            "UPDATE worktree_leases SET heartbeat_at = ?3, expires_at = ?4 WHERE run_id = ?1 AND lease_owner_id = ?2",
            params![run_id, worker_owner_id, now, lease_expires_at],
        )?;
        if account_updates == 0 || worktree_updates == 0 {
            return Err(AppError::WorkerNotActive {
                run_id: run_id.to_string(),
            });
        }
        tx.commit()?;
        Ok(())
    }

    pub fn finish_run(&mut self, run_id: &str, completion: RunCompletion) -> Result<()> {
        let now = current_timestamp()?;
        let tx = self.connection.transaction()?;
        let run = tx
            .query_row(
                "SELECT task_id, assigned_identity_id, assigned_worktree_id, status
                 FROM task_runs WHERE run_id = ?1",
                params![run_id],
                |row| {
                    Ok((
                        TaskId::from_string(row.get::<_, String>(0)?),
                        row.get::<_, Option<String>>(1)?
                            .map(IdentityId::from_string),
                        row.get::<_, Option<String>>(2)?
                            .map(WorktreeId::from_string),
                        TaskRunStatus::parse(&row.get::<_, String>(3)?).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                Type::Text,
                                Box::new(error),
                            )
                        })?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| AppError::TaskRunNotFound {
                run_id: run_id.to_string(),
            })?;
        if run.3.is_terminal() {
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "UPDATE task_runs SET status = ?2, finished_at = ?3, exit_code = ?4, failure_kind = ?5, failure_message = ?6, assigned_thread_id = COALESCE(?7, assigned_thread_id), heartbeat_at = ?3, heartbeat_expires_at = NULL
             , worker_pid = NULL, worker_owner_id = NULL
             WHERE run_id = ?1 AND status NOT IN ('completed', 'failed', 'timed_out', 'abandoned', 'canceled', 'orphaned')",
            params![
                run_id,
                completion.status.as_str(),
                now,
                completion.exit_code,
                completion.failure_kind.as_ref().map(|kind| kind.as_str()),
                completion.failure_message.as_deref(),
                completion.thread_id.as_deref()
            ],
        )?;
        tx.execute(
            "DELETE FROM account_leases WHERE run_id = ?1",
            params![run_id],
        )?;
        tx.execute(
            "DELETE FROM worktree_leases WHERE run_id = ?1",
            params![run_id],
        )?;
        if let Some(identity_id) = run.1.as_ref() {
            let remaining_leases: i64 = tx.query_row(
                "SELECT COUNT(1) FROM account_leases WHERE identity_id = ?1",
                params![identity_id.as_str()],
                |row| row.get(0),
            )?;
            tx.execute(
                "UPDATE account_runtime
                 SET state = CASE WHEN ?3 = 0 THEN 'free' ELSE state END,
                     active_count = ?3,
                     active_run_id = CASE WHEN ?3 = 0 THEN NULL ELSE active_run_id END,
                     last_success_at = CASE WHEN ?2 = 'completed' THEN ?4 ELSE last_success_at END,
                     last_failure_at = CASE WHEN ?2 != 'completed' THEN ?4 ELSE last_failure_at END,
                     updated_at = ?4
                 WHERE identity_id = ?1",
                params![
                    identity_id.as_str(),
                    completion.status.as_str(),
                    remaining_leases,
                    now
                ],
            )?;
        }
        if let Some(worktree_id) = run.2.as_ref() {
            let cleanup_after = match completion.status {
                TaskRunStatus::Completed => now + 60 * 60 * 24,
                TaskRunStatus::Failed | TaskRunStatus::TimedOut | TaskRunStatus::Canceled => {
                    now + 60 * 60
                }
                _ => now + 60 * 60,
            };
            tx.execute(
                "UPDATE worktrees SET state = 'ready', last_run_id = ?2, last_used_at = ?3, updated_at = ?3, cleanup_after = ?4 WHERE worktree_id = ?1",
                params![worktree_id.as_str(), run_id, now, cleanup_after],
            )?;
        }
        let task_status = match completion.status {
            TaskRunStatus::Completed => TaskStatus::AwaitingFollowup,
            TaskRunStatus::Failed | TaskRunStatus::TimedOut => TaskStatus::FailedRetryable,
            TaskRunStatus::Canceled => TaskStatus::Canceled,
            TaskRunStatus::Orphaned => TaskStatus::Orphaned,
            _ => TaskStatus::FailedTerminal,
        };
        let current_task_status: TaskStatus = tx.query_row(
            "SELECT status FROM tasks WHERE task_id = ?1",
            params![run.0.as_str()],
            |row| {
                TaskStatus::parse(&row.get::<_, String>(0)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
                })
            },
        )?;
        let effective_task_status = if current_task_status == TaskStatus::Canceled {
            TaskStatus::Canceled
        } else {
            task_status
        };
        tx.execute(
            "UPDATE tasks
             SET status = ?2,
                 updated_at = ?3,
                 current_lineage_thread_id = COALESCE(?4, current_lineage_thread_id),
                 last_checkpoint_id = COALESCE(?5, last_checkpoint_id),
                 last_identity_id = COALESCE(?6, last_identity_id),
                 last_completed_run_id = CASE WHEN ?2 = 'awaiting_followup' THEN ?7 ELSE last_completed_run_id END,
                 pending_followup_count = CASE WHEN pending_followup_count > 0 AND ?2 = 'awaiting_followup' THEN pending_followup_count - 1 ELSE pending_followup_count END
             WHERE task_id = ?1",
            params![
                run.0.as_str(),
                effective_task_status.as_str(),
                now,
                completion.thread_id.as_deref(),
                completion.checkpoint_id.as_deref(),
                completion.last_identity_id.as_ref().map(IdentityId::as_str),
                run_id,
            ],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                Some(run.0.clone()),
                Some(TaskRunId::from_string(run_id)),
                "run_finished",
                format!("run {run_id} finished with {}", completion.status.as_str()),
                json!({
                    "status": completion.status.as_str(),
                    "exit_code": completion.exit_code,
                    "failure_kind": completion.failure_kind.as_ref().map(|kind| kind.as_str()),
                    "failure_message": completion.failure_message,
                    "thread_id": completion.thread_id,
                    "checkpoint_id": completion.checkpoint_id,
                }),
            )?,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn mark_run_orphaned(&mut self, run_id: &str, reason: &str) -> Result<()> {
        self.finish_run(
            run_id,
            RunCompletion {
                status: TaskRunStatus::Orphaned,
                exit_code: None,
                failure_kind: Some(FailureKind::WorkerExited),
                failure_message: Some(reason.to_string()),
                thread_id: None,
                checkpoint_id: None,
                last_identity_id: None,
            },
        )
    }

    pub fn reconcile_orphaned_runs(
        &mut self,
        now: i64,
        settings: &SchedulerSettings,
    ) -> Result<Vec<TaskRunId>> {
        let candidates = {
            let mut statement = self.connection.prepare(
                "SELECT run_id, worker_pid, heartbeat_expires_at
                 FROM task_runs
                 WHERE status IN ('assigned','launching','running','handoff_pending')",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<u32>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut orphaned = Vec::new();
        for (run_id, worker_pid, expires_at) in candidates {
            let alive = worker_pid.map(process_is_alive).unwrap_or(false);
            let stale = expires_at.map(|value| value < now).unwrap_or(true);
            if !alive || stale {
                self.mark_run_orphaned(
                    &run_id,
                    if !alive {
                        "worker process is not alive"
                    } else {
                        "worker heartbeat expired"
                    },
                )?;
                if settings.requeue_orphaned_runs {
                    self.requeue_orphaned_run(&run_id)?;
                }
                orphaned.push(TaskRunId::from_string(run_id));
            }
        }
        Ok(orphaned)
    }

    pub fn requeue_orphaned_run(&mut self, run_id: &str) -> Result<()> {
        let now = current_timestamp()?;
        let tx = self.connection.transaction()?;
        tx.execute(
            "UPDATE task_runs
             SET status = 'pending_assignment',
                 worker_pid = NULL,
                 worker_owner_id = NULL,
                 heartbeat_at = NULL,
                 heartbeat_expires_at = NULL,
                 failure_kind = NULL,
                 failure_message = NULL
             WHERE run_id = ?1 AND status = 'orphaned'",
            params![run_id],
        )?;
        tx.execute(
            "UPDATE tasks
             SET status = 'queued', updated_at = ?2
             WHERE task_id = (SELECT task_id FROM task_runs WHERE run_id = ?1)",
            params![run_id, now],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                None,
                Some(TaskRunId::from_string(run_id)),
                "run_requeued",
                format!("orphaned run {run_id} requeued"),
                json!({}),
            )?,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn rollback_assignment_after_spawn_failure(
        &mut self,
        run_id: &str,
        worker_owner_id: &str,
        message: &str,
    ) -> Result<bool> {
        let now = current_timestamp()?;
        let tx = self.connection.transaction()?;
        let run = tx
            .query_row(
                "SELECT task_id, assigned_identity_id, assigned_worktree_id, status
                 FROM task_runs WHERE run_id = ?1",
                params![run_id],
                |row| {
                    Ok((
                        TaskId::from_string(row.get::<_, String>(0)?),
                        row.get::<_, Option<String>>(1)?
                            .map(IdentityId::from_string),
                        row.get::<_, Option<String>>(2)?
                            .map(WorktreeId::from_string),
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((task_id, identity_id, worktree_id, status)) = run else {
            return Ok(false);
        };
        if status != TaskRunStatus::Assigned.as_str() {
            tx.rollback()?;
            return Ok(false);
        }
        let updated = tx.execute(
            "UPDATE task_runs
             SET status = 'pending_assignment',
                 assigned_identity_id = NULL,
                 assigned_worktree_id = NULL,
                 launch_mode = NULL,
                 worker_pid = NULL,
                 worker_owner_id = NULL,
                 heartbeat_at = NULL,
                 heartbeat_expires_at = NULL,
                 failure_kind = NULL,
                 failure_message = NULL
             WHERE run_id = ?1 AND status = 'assigned' AND worker_owner_id = ?2",
            params![run_id, worker_owner_id],
        )?;
        if updated == 0 {
            tx.rollback()?;
            return Ok(false);
        }
        tx.execute(
            "DELETE FROM account_leases WHERE run_id = ?1",
            params![run_id],
        )?;
        tx.execute(
            "DELETE FROM worktree_leases WHERE run_id = ?1",
            params![run_id],
        )?;
        if let Some(identity_id) = identity_id.as_ref() {
            let remaining_leases: i64 = tx.query_row(
                "SELECT COUNT(1) FROM account_leases WHERE identity_id = ?1",
                params![identity_id.as_str()],
                |row| row.get(0),
            )?;
            tx.execute(
                "UPDATE account_runtime
                 SET state = CASE WHEN ?2 = 0 THEN 'free' ELSE 'reserved' END,
                     active_count = ?2,
                     active_run_id = CASE WHEN ?2 = 0 THEN NULL ELSE active_run_id END,
                     last_failure_at = ?3,
                     updated_at = ?3
                 WHERE identity_id = ?1",
                params![identity_id.as_str(), remaining_leases, now],
            )?;
        }
        if let Some(worktree_id) = worktree_id.as_ref() {
            tx.execute(
                "UPDATE worktrees
                 SET state = 'ready', updated_at = ?2, cleanup_after = ?3
                 WHERE worktree_id = ?1",
                params![worktree_id.as_str(), now, now + 60 * 60],
            )?;
        }
        tx.execute(
            "UPDATE tasks SET status = 'queued', updated_at = ?2 WHERE task_id = ?1",
            params![task_id.as_str(), now],
        )?;
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                Some(task_id),
                Some(TaskRunId::from_string(run_id)),
                "worker_spawn_failed",
                format!("worker spawn failed for run {run_id}: {message}"),
                json!({
                    "run_id": run_id,
                    "worker_owner_id": worker_owner_id,
                    "message": message,
                }),
            )?,
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn cancel_task(&mut self, task_id: &str) -> Result<CancelTaskOutcome> {
        let now = current_timestamp()?;
        let tx = self.connection.transaction()?;
        let task = tx
            .query_row(
                "SELECT task_id FROM tasks WHERE task_id = ?1",
                params![task_id],
                |row| Ok(TaskId::from_string(row.get::<_, String>(0)?)),
            )
            .optional()?
            .ok_or_else(|| AppError::TaskNotFound {
                task_id: task_id.to_string(),
            })?;
        let active_runs = {
            let mut statement = tx.prepare(
                "SELECT run_id, worker_pid, assigned_identity_id, assigned_worktree_id
                 FROM task_runs
                 WHERE task_id = ?1
                   AND status IN ('pending_assignment', 'assigned', 'launching', 'running', 'handoff_pending')",
            )?;
            let rows = statement.query_map(params![task_id], |row| {
                Ok((
                    TaskRunId::from_string(row.get::<_, String>(0)?),
                    row.get::<_, Option<u32>>(1)?,
                    row.get::<_, Option<String>>(2)?
                        .map(IdentityId::from_string),
                    row.get::<_, Option<String>>(3)?
                        .map(WorktreeId::from_string),
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        tx.execute(
            "UPDATE tasks SET status = 'canceled', updated_at = ?2 WHERE task_id = ?1",
            params![task_id, now],
        )?;
        tx.execute(
            "UPDATE task_runs
             SET status = 'canceled',
                 finished_at = COALESCE(finished_at, ?2),
                 failure_kind = 'canceled',
                 failure_message = 'task canceled',
                 worker_pid = NULL,
                 worker_owner_id = NULL,
                 heartbeat_at = ?2,
                 heartbeat_expires_at = NULL
             WHERE task_id = ?1
               AND status IN ('pending_assignment', 'assigned', 'launching', 'running', 'handoff_pending')",
            params![task_id, now],
        )?;
        tx.execute(
            "DELETE FROM account_leases
             WHERE run_id IN (
                SELECT run_id FROM task_runs WHERE task_id = ?1 AND status = 'canceled'
             )",
            params![task_id],
        )?;
        tx.execute(
            "DELETE FROM worktree_leases
             WHERE run_id IN (
                SELECT run_id FROM task_runs WHERE task_id = ?1 AND status = 'canceled'
             )",
            params![task_id],
        )?;
        for (_, _, identity_id, worktree_id) in &active_runs {
            if let Some(identity_id) = identity_id.as_ref() {
                let remaining_leases: i64 = tx.query_row(
                    "SELECT COUNT(1) FROM account_leases WHERE identity_id = ?1",
                    params![identity_id.as_str()],
                    |row| row.get(0),
                )?;
                tx.execute(
                    "UPDATE account_runtime
                     SET state = CASE WHEN ?2 = 0 THEN 'free' ELSE 'reserved' END,
                         active_count = ?2,
                         active_run_id = CASE WHEN ?2 = 0 THEN NULL ELSE active_run_id END,
                         last_failure_at = ?3,
                         updated_at = ?3
                     WHERE identity_id = ?1",
                    params![identity_id.as_str(), remaining_leases, now],
                )?;
            }
            if let Some(worktree_id) = worktree_id.as_ref() {
                tx.execute(
                    "UPDATE worktrees
                     SET state = 'corrupted', reusable = 0, updated_at = ?2, cleanup_after = NULL
                     WHERE worktree_id = ?1",
                    params![worktree_id.as_str(), now],
                )?;
            }
        }
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                Some(task.clone()),
                None,
                "task_canceled",
                format!("task {task_id} canceled"),
                json!({
                    "interrupted_runs": active_runs
                        .iter()
                        .map(|(run_id, worker_pid, _, _)| json!({
                            "run_id": run_id,
                            "worker_pid": worker_pid,
                        }))
                        .collect::<Vec<_>>(),
                }),
            )?,
        )?;
        tx.commit()?;
        Ok(CancelTaskOutcome {
            task_id: task,
            interrupted_runs: active_runs
                .into_iter()
                .map(|(run_id, worker_pid, _, worktree_id)| CanceledRunRecord {
                    run_id,
                    worker_pid,
                    worktree_id,
                })
                .collect(),
        })
    }

    pub fn schedule_canceled_worktree_cleanup(
        &mut self,
        worktree_ids: &[WorktreeId],
        ttl: Duration,
    ) -> Result<()> {
        if worktree_ids.is_empty() {
            return Ok(());
        }
        let now = current_timestamp()?;
        let cleanup_after = now + ttl.as_secs() as i64;
        let tx = self.connection.transaction()?;
        for worktree_id in worktree_ids {
            tx.execute(
                "UPDATE worktrees
                 SET cleanup_after = ?2, updated_at = ?3
                 WHERE worktree_id = ?1 AND state = 'corrupted'",
                params![worktree_id.as_str(), cleanup_after, now],
            )?;
        }
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                None,
                None,
                None,
                "canceled_worktree_cleanup_scheduled",
                format!(
                    "scheduled cleanup for {} canceled worktrees",
                    worktree_ids.len()
                ),
                json!({
                    "worktree_ids": worktree_ids,
                    "cleanup_after": cleanup_after,
                }),
            )?,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn reserve_worktree_for_gc(
        &mut self,
        worktree_id: &WorktreeId,
        now: i64,
    ) -> Result<Option<WorktreeRecord>> {
        let tx = self.connection.transaction()?;
        let worktree = tx
            .query_row(
                "SELECT worktree_id, project_id, task_id, path, execution_mode, state, last_run_id, last_used_at, created_at, updated_at, cleanup_after, reusable
                 FROM worktrees
                 WHERE worktree_id = ?1
                   AND cleanup_after IS NOT NULL
                   AND cleanup_after <= ?2
                   AND worktree_id NOT IN (SELECT worktree_id FROM worktree_leases)",
                params![worktree_id.as_str(), now],
                worktree_from_row,
            )
            .optional()?;
        let Some(worktree) = worktree else {
            tx.rollback()?;
            return Ok(None);
        };
        let updated = tx.execute(
            "UPDATE worktrees
             SET state = 'cleaning', updated_at = ?2
             WHERE worktree_id = ?1
               AND cleanup_after IS NOT NULL
               AND cleanup_after <= ?2
               AND worktree_id NOT IN (SELECT worktree_id FROM worktree_leases)",
            params![worktree_id.as_str(), now],
        )?;
        if updated == 0 {
            tx.rollback()?;
            return Ok(None);
        }
        tx.commit()?;
        Ok(Some(worktree))
    }

    pub fn release_worktree_gc_reservation(
        &mut self,
        worktree_id: &WorktreeId,
        state: WorktreeState,
    ) -> Result<()> {
        let now = current_timestamp()?;
        self.connection.execute(
            "UPDATE worktrees SET state = ?2, updated_at = ?3 WHERE worktree_id = ?1 AND state = 'cleaning'",
            params![worktree_id.as_str(), state.as_str(), now],
        )?;
        Ok(())
    }

    pub fn latest_dispatch_for_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<DispatchDecisionRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT d.decision_id, d.run_id, d.decision_kind, d.selected_identity_id, d.selected_worktree_id, d.lineage_mode, d.reason, d.candidates_json, d.policy_snapshot_json, d.created_at
             FROM dispatch_decisions d
             JOIN task_runs r ON r.run_id = d.run_id
             WHERE r.task_id = ?1
             ORDER BY d.created_at DESC, d.decision_id DESC
             LIMIT 1",
        )?;
        statement
            .query_row(params![task_id.as_str()], dispatch_decision_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn latest_task_input(&self, task_id: &TaskId) -> Result<Option<TaskRunInputRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT i.run_id, i.prompt_text, i.prompt_file_path, i.created_at
             FROM task_run_inputs i
             JOIN task_runs r ON r.run_id = i.run_id
             WHERE r.task_id = ?1
             ORDER BY r.sequence_no DESC LIMIT 1",
        )?;
        statement
            .query_row(params![task_id.as_str()], run_input_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn scheduler_health(
        &self,
        registry_identities: &BTreeSet<IdentityId>,
        now: i64,
    ) -> Result<SchedulerHealthSnapshot> {
        let queued_runs: i64 = self.connection.query_row(
            "SELECT COUNT(1) FROM task_runs WHERE status = 'pending_assignment'",
            [],
            |row| row.get(0),
        )?;
        let active_runs: i64 = self.connection.query_row(
            "SELECT COUNT(1) FROM task_runs WHERE status IN ('assigned','launching','running','handoff_pending')",
            [],
            |row| row.get(0),
        )?;
        let stale_runs: i64 = self.connection.query_row(
            "SELECT COUNT(1) FROM task_runs WHERE status IN ('assigned','launching','running','handoff_pending') AND COALESCE(heartbeat_expires_at, 0) < ?1",
            params![now],
            |row| row.get(0),
        )?;
        let active_identities = self
            .active_account_leases()?
            .into_iter()
            .map(|lease| lease.identity_id)
            .collect::<BTreeSet<_>>();
        let free_identities = registry_identities.difference(&active_identities).count();
        Ok(SchedulerHealthSnapshot {
            queued_runs: queued_runs as usize,
            active_runs: active_runs as usize,
            stale_runs: stale_runs as usize,
            active_identities: active_identities.len(),
            free_identities,
        })
    }

    pub fn gc_worktrees(&self, now: i64) -> Result<Vec<WorktreeRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT worktree_id, project_id, task_id, path, execution_mode, state, last_run_id, last_used_at, created_at, updated_at, cleanup_after, reusable
             FROM worktrees
             WHERE cleanup_after IS NOT NULL AND cleanup_after <= ?1 AND worktree_id NOT IN (SELECT worktree_id FROM worktree_leases)",
        )?;
        let worktrees = statement
            .query_map(params![now], worktree_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(worktrees)
    }

    pub fn delete_worktree(&mut self, worktree: &WorktreeRecord) -> Result<bool> {
        let tx = self.connection.transaction()?;
        tx.execute(
            "DELETE FROM worktrees
             WHERE worktree_id = ?1
               AND state = 'cleaning'
               AND worktree_id NOT IN (SELECT worktree_id FROM worktree_leases)",
            params![worktree.worktree_id.as_str()],
        )?;
        if tx.changes() == 0 {
            tx.rollback()?;
            return Ok(false);
        }
        append_scheduler_event_tx(
            &tx,
            &SchedulerEventRecord::new(
                Some(worktree.project_id.clone()),
                Some(worktree.task_id.clone()),
                worktree.last_run_id.clone(),
                "worktree_deleted",
                format!("worktree {} deleted", worktree.path.display()),
                json!({
                    "worktree_id": worktree.worktree_id,
                    "path": worktree.path,
                    "execution_mode": worktree.execution_mode.as_str(),
                }),
            )?,
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn append_event(&self, event: &SchedulerEventRecord) -> Result<()> {
        self.connection.execute(
            "INSERT INTO scheduler_events(event_id, project_id, task_id, run_id, event_kind, message, payload_json, created_at)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                event.event_id.as_str(),
                event.project_id.as_ref().map(ProjectId::as_str),
                event.task_id.as_ref().map(TaskId::as_str),
                event.run_id.as_ref().map(TaskRunId::as_str),
                event.event_kind,
                event.message,
                serde_json::to_string(&event.payload_json)?,
                event.created_at
            ],
        )?;
        Ok(())
    }

    pub fn run_input(&self, run_id: &TaskRunId) -> Result<TaskRunInputRecord> {
        let mut statement = self.connection.prepare(
            "SELECT run_id, prompt_text, prompt_file_path, created_at FROM task_run_inputs WHERE run_id = ?1",
        )?;
        statement
            .query_row(params![run_id.as_str()], run_input_from_row)
            .optional()?
            .ok_or_else(|| AppError::TaskRunNotFound {
                run_id: run_id.to_string(),
            })
    }

    pub fn reusable_worktree_for_task(&self, task_id: &TaskId) -> Result<Option<WorktreeRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT worktree_id, project_id, task_id, path, execution_mode, state, last_run_id, last_used_at, created_at, updated_at, cleanup_after, reusable
             FROM worktrees
             WHERE task_id = ?1 AND reusable = 1 AND state = 'ready'
             ORDER BY updated_at DESC LIMIT 1",
        )?;
        statement
            .query_row(params![task_id.as_str()], worktree_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn get_worktree(&self, worktree_id: &WorktreeId) -> Result<WorktreeRecord> {
        let mut statement = self.connection.prepare(
            "SELECT worktree_id, project_id, task_id, path, execution_mode, state, last_run_id, last_used_at, created_at, updated_at, cleanup_after, reusable
             FROM worktrees WHERE worktree_id = ?1",
        )?;
        statement
            .query_row(params![worktree_id.as_str()], worktree_from_row)
            .optional()?
            .ok_or_else(|| AppError::WorktreeBusy {
                path: PathBuf::from(worktree_id.as_str()),
            })
    }

    fn build_run_record(
        &self,
        task: &TaskRecord,
        sequence_no: u32,
        run_kind: RunKind,
        now: i64,
        options: RunBuildOptions,
    ) -> TaskRunRecord {
        let run_id = TaskRunId::new();
        TaskRunRecord {
            input_artifact_path: task_artifact_run_path(
                &self.base_root,
                task.task_id.as_str(),
                run_id.as_str(),
            ),
            run_id,
            task_id: task.task_id.clone(),
            sequence_no,
            run_kind,
            status: TaskRunStatus::PendingAssignment,
            requested_at: now,
            assigned_identity_id: None,
            assigned_worktree_id: None,
            assigned_thread_id: task.current_lineage_thread_id.clone(),
            launch_mode: None,
            retry_count: 0,
            started_at: None,
            finished_at: None,
            exit_code: None,
            failure_kind: None,
            failure_message: None,
            max_runtime_secs: options.max_runtime_secs,
            queue_if_busy: options.queue_if_busy,
            allow_oversubscribe: options.allow_oversubscribe,
            affinity_policy: options.affinity_policy,
            worker_pid: None,
            worker_owner_id: None,
            heartbeat_at: None,
            heartbeat_expires_at: None,
            last_turn_id: None,
            run_attempt_no: 0,
        }
    }

    fn next_sequence_no(&self, task_id: &TaskId) -> Result<u32> {
        let sequence_no: Option<u32> = self
            .connection
            .query_row(
                "SELECT MAX(sequence_no) FROM task_runs WHERE task_id = ?1",
                params![task_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        Ok(sequence_no.unwrap_or(0))
    }

    fn reset_state_locked(&mut self, owner_id: &str) -> Result<()> {
        let active_runs: usize = self.connection.query_row(
            "SELECT COUNT(1) FROM task_runs WHERE status IN ('assigned', 'launching', 'running', 'handoff_pending')",
            [],
            |row| row.get::<_, i64>(0),
        )? as usize;
        let account_leases = self.active_account_leases()?.len();
        let worktree_leases = self.worktree_leases()?.len();
        if active_runs > 0 || account_leases > 0 || worktree_leases > 0 {
            return Err(AppError::SchedulerResetBlocked {
                active_runs,
                account_leases,
                worktree_leases,
            });
        }

        let tx = self.connection.transaction()?;
        let lock_owner = tx
            .query_row(
                "SELECT owner_id FROM scheduler_process_lock WHERE lock_name = 'dispatcher'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        match lock_owner {
            Some(lock_owner) if lock_owner == owner_id => {}
            Some(lock_owner) => {
                return Err(AppError::SchedulerAlreadyRunning {
                    owner_id: lock_owner,
                });
            }
            None => {
                return Err(AppError::InvalidSchedulerConfiguration {
                    message: "scheduler reset requires an active scheduler lock".to_string(),
                });
            }
        }

        tx.execute("DELETE FROM worktree_leases", [])?;
        tx.execute("DELETE FROM account_leases", [])?;
        tx.execute("DELETE FROM dispatch_decisions", [])?;
        tx.execute("DELETE FROM task_run_inputs", [])?;
        tx.execute("DELETE FROM task_runs", [])?;
        tx.execute("DELETE FROM scheduler_events", [])?;
        tx.execute("DELETE FROM worktrees", [])?;
        tx.execute("DELETE FROM account_runtime", [])?;
        tx.execute("DELETE FROM tasks", [])?;
        tx.execute("DELETE FROM projects", [])?;
        tx.execute(
            "DELETE FROM scheduler_control WHERE control_key = ?1",
            params![SCHEDULER_CONTROL_KEY],
        )?;
        tx.execute(
            "DELETE FROM scheduler_process_lock WHERE lock_name = 'dispatcher'",
            [],
        )?;
        tx.commit()?;

        let task_artifacts = task_artifacts_path(&self.base_root);
        if task_artifacts.exists() {
            fs::remove_dir_all(&task_artifacts)?;
        }
        let task_worktrees = task_worktrees_path(&self.base_root);
        if task_worktrees.exists() {
            fs::remove_dir_all(&task_worktrees)?;
        }
        self.ensure_scheduler_control_row()?;
        Ok(())
    }
}

fn insert_task_tx(tx: &Transaction<'_>, task: &TaskRecord) -> Result<()> {
    tx.execute(
        "INSERT INTO tasks(task_id, project_id, title, status, priority, labels_json, created_by, created_at, updated_at, current_lineage_thread_id, preferred_identity_id, last_identity_id, last_checkpoint_id, last_completed_run_id, pending_followup_count)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            task.task_id.as_str(),
            task.project_id.as_str(),
            task.title,
            task.status.as_str(),
            task.priority,
            serde_json::to_string(&task.labels)?,
            task.created_by,
            task.created_at,
            task.updated_at,
            task.current_lineage_thread_id,
            task.preferred_identity_id.as_ref().map(IdentityId::as_str),
            task.last_identity_id.as_ref().map(IdentityId::as_str),
            task.last_checkpoint_id,
            task.last_completed_run_id.as_ref().map(TaskRunId::as_str),
            i64::from(task.pending_followup_count),
        ],
    )?;
    Ok(())
}

fn insert_run_tx(tx: &Transaction<'_>, run: &TaskRunRecord) -> Result<()> {
    tx.execute(
        "INSERT INTO task_runs(run_id, task_id, sequence_no, run_kind, status, input_artifact_path, requested_at, assigned_identity_id, assigned_worktree_id, assigned_thread_id, launch_mode, retry_count, started_at, finished_at, exit_code, failure_kind, failure_message, max_runtime_secs, queue_if_busy, allow_oversubscribe, affinity_policy, worker_pid, worker_owner_id, heartbeat_at, heartbeat_expires_at, last_turn_id, run_attempt_no)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27)",
        params![
            run.run_id.as_str(),
            run.task_id.as_str(),
            i64::from(run.sequence_no),
            run.run_kind.as_str(),
            run.status.as_str(),
            run.input_artifact_path.to_string_lossy(),
            run.requested_at,
            run.assigned_identity_id.as_ref().map(IdentityId::as_str),
            run.assigned_worktree_id.as_ref().map(WorktreeId::as_str),
            run.assigned_thread_id,
            run.launch_mode.map(LaunchMode::as_str),
            i64::from(run.retry_count),
            run.started_at,
            run.finished_at,
            run.exit_code,
            run.failure_kind.map(FailureKind::as_str),
            run.failure_message,
            run.max_runtime_secs,
            i64::from(run.queue_if_busy as u8),
            i64::from(run.allow_oversubscribe as u8),
            run.affinity_policy.as_str(),
            run.worker_pid,
            run.worker_owner_id,
            run.heartbeat_at,
            run.heartbeat_expires_at,
            run.last_turn_id,
            i64::from(run.run_attempt_no),
        ],
    )?;
    Ok(())
}

fn insert_run_input_tx(tx: &Transaction<'_>, input: &TaskRunInputRecord) -> Result<()> {
    tx.execute(
        "INSERT INTO task_run_inputs(run_id, prompt_text, prompt_file_path, created_at)
         VALUES(?1, ?2, ?3, ?4)",
        params![
            input.run_id.as_str(),
            input.prompt_text,
            input
                .prompt_file_path
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            input.created_at,
        ],
    )?;
    Ok(())
}

fn append_scheduler_event_tx(tx: &Transaction<'_>, event: &SchedulerEventRecord) -> Result<()> {
    tx.execute(
        "INSERT INTO scheduler_events(event_id, project_id, task_id, run_id, event_kind, message, payload_json, created_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            event.event_id.as_str(),
            event.project_id.as_ref().map(ProjectId::as_str),
            event.task_id.as_ref().map(TaskId::as_str),
            event.run_id.as_ref().map(TaskRunId::as_str),
            event.event_kind,
            event.message,
            serde_json::to_string(&event.payload_json)?,
            event.created_at,
        ],
    )?;
    Ok(())
}

fn upsert_account_runtime_tx(tx: &Transaction<'_>, runtime: &AccountRuntimeRecord) -> Result<()> {
    tx.execute(
        "INSERT INTO account_runtime(identity_id, state, active_run_id, active_count, last_dispatch_at, last_success_at, last_failure_at, updated_at)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(identity_id) DO UPDATE SET
            state = excluded.state,
            active_run_id = excluded.active_run_id,
            active_count = excluded.active_count,
            last_dispatch_at = COALESCE(excluded.last_dispatch_at, account_runtime.last_dispatch_at),
            last_success_at = COALESCE(excluded.last_success_at, account_runtime.last_success_at),
            last_failure_at = COALESCE(excluded.last_failure_at, account_runtime.last_failure_at),
            updated_at = excluded.updated_at",
        params![
            runtime.identity_id.as_str(),
            runtime.state.as_str(),
            runtime.active_run_id.as_ref().map(TaskRunId::as_str),
            i64::from(runtime.active_count),
            runtime.last_dispatch_at,
            runtime.last_success_at,
            runtime.last_failure_at,
            runtime.updated_at
        ],
    )?;
    Ok(())
}

fn persist_run_prompt(
    base_root: &Path,
    task_id: &TaskId,
    run_id: &TaskRunId,
    prompt: &str,
) -> Result<()> {
    let run_root = task_artifact_run_path(base_root, task_id.as_str(), run_id.as_str());
    ensure_directory(&run_root, 0o700)?;
    let mut payload = prompt.as_bytes().to_vec();
    payload.push(b'\n');
    atomic_write(
        &task_artifact_prompt_path(base_root, task_id.as_str(), run_id.as_str()),
        &payload,
        0o600,
    )?;
    atomic_write(
        &task_artifact_events_path(base_root, task_id.as_str(), run_id.as_str()),
        b"",
        0o600,
    )?;
    Ok(())
}

fn project_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProjectRecord> {
    project_from_row_prefix(row, 0)
}

fn workspace_project_name(repo_root: &Path) -> String {
    format!("workspace:{}", repo_root.display())
}

fn project_from_row_prefix(
    row: &rusqlite::Row<'_>,
    start: usize,
) -> rusqlite::Result<ProjectRecord> {
    Ok(ProjectRecord {
        project_id: ProjectId::from_string(row.get::<_, String>(start)?),
        name: row.get(start + 1)?,
        repo_root: PathBuf::from(row.get::<_, String>(start + 2)?),
        execution_mode: parse_db(
            ProjectExecutionMode::parse,
            row.get::<_, String>(start + 3)?,
        )?,
        default_codex_args: parse_json(row.get::<_, String>(start + 4)?)?,
        default_model_or_profile: row.get(start + 5)?,
        env_allowlist: parse_json(row.get::<_, String>(start + 6)?)?,
        cleanup_policy: parse_json(row.get::<_, String>(start + 7)?)?,
        created_at: row.get(start + 8)?,
        updated_at: row.get(start + 9)?,
    })
}

fn task_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRecord> {
    task_from_row_prefix(row, 0)
}

fn task_from_row_prefix(row: &rusqlite::Row<'_>, start: usize) -> rusqlite::Result<TaskRecord> {
    Ok(TaskRecord {
        task_id: TaskId::from_string(row.get::<_, String>(start)?),
        project_id: ProjectId::from_string(row.get::<_, String>(start + 1)?),
        title: row.get(start + 2)?,
        status: parse_db(TaskStatus::parse, row.get::<_, String>(start + 3)?)?,
        priority: row.get(start + 4)?,
        labels: parse_json(row.get::<_, String>(start + 5)?)?,
        created_by: row.get(start + 6)?,
        created_at: row.get(start + 7)?,
        updated_at: row.get(start + 8)?,
        current_lineage_thread_id: row.get(start + 9)?,
        preferred_identity_id: row
            .get::<_, Option<String>>(start + 10)?
            .map(IdentityId::from_string),
        last_identity_id: row
            .get::<_, Option<String>>(start + 11)?
            .map(IdentityId::from_string),
        last_checkpoint_id: row.get(start + 12)?,
        last_completed_run_id: row
            .get::<_, Option<String>>(start + 13)?
            .map(TaskRunId::from_string),
        pending_followup_count: row.get::<_, i64>(start + 14)? as u32,
    })
}

fn run_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRunRecord> {
    run_from_row_prefix(row, 0)
}

fn run_from_row_prefix(row: &rusqlite::Row<'_>, start: usize) -> rusqlite::Result<TaskRunRecord> {
    Ok(TaskRunRecord {
        run_id: TaskRunId::from_string(row.get::<_, String>(start)?),
        task_id: TaskId::from_string(row.get::<_, String>(start + 1)?),
        sequence_no: row.get::<_, i64>(start + 2)? as u32,
        run_kind: parse_db(RunKind::parse, row.get::<_, String>(start + 3)?)?,
        status: parse_db(TaskRunStatus::parse, row.get::<_, String>(start + 4)?)?,
        input_artifact_path: PathBuf::from(row.get::<_, String>(start + 5)?),
        requested_at: row.get(start + 6)?,
        assigned_identity_id: row
            .get::<_, Option<String>>(start + 7)?
            .map(IdentityId::from_string),
        assigned_worktree_id: row
            .get::<_, Option<String>>(start + 8)?
            .map(WorktreeId::from_string),
        assigned_thread_id: row.get(start + 9)?,
        launch_mode: row
            .get::<_, Option<String>>(start + 10)?
            .map(|value| parse_db(LaunchMode::parse, value))
            .transpose()?,
        retry_count: row.get::<_, i64>(start + 11)? as u32,
        started_at: row.get(start + 12)?,
        finished_at: row.get(start + 13)?,
        exit_code: row.get(start + 14)?,
        failure_kind: row
            .get::<_, Option<String>>(start + 15)?
            .map(|value| parse_db(FailureKind::parse, value))
            .transpose()?,
        failure_message: row.get(start + 16)?,
        max_runtime_secs: row.get(start + 17)?,
        queue_if_busy: row.get::<_, i64>(start + 18)? != 0,
        allow_oversubscribe: row.get::<_, i64>(start + 19)? != 0,
        affinity_policy: parse_db(TaskAffinityPolicy::parse, row.get::<_, String>(start + 20)?)?,
        worker_pid: row.get(start + 21)?,
        worker_owner_id: row.get(start + 22)?,
        heartbeat_at: row.get(start + 23)?,
        heartbeat_expires_at: row.get(start + 24)?,
        last_turn_id: row.get(start + 25)?,
        run_attempt_no: row.get::<_, i64>(start + 26)? as u32,
    })
}

fn run_input_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRunInputRecord> {
    Ok(TaskRunInputRecord {
        run_id: TaskRunId::from_string(row.get::<_, String>(0)?),
        prompt_text: row.get(1)?,
        prompt_file_path: row.get::<_, Option<String>>(2)?.map(PathBuf::from),
        created_at: row.get(3)?,
    })
}

fn account_runtime_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AccountRuntimeRecord> {
    Ok(AccountRuntimeRecord {
        identity_id: IdentityId::from_string(row.get::<_, String>(0)?),
        state: parse_db(AccountRuntimeState::parse, row.get::<_, String>(1)?)?,
        active_run_id: row.get::<_, Option<String>>(2)?.map(TaskRunId::from_string),
        active_count: row.get::<_, i64>(3)? as u32,
        last_dispatch_at: row.get(4)?,
        last_success_at: row.get(5)?,
        last_failure_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn account_lease_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AccountLeaseRecord> {
    Ok(AccountLeaseRecord {
        identity_id: IdentityId::from_string(row.get::<_, String>(0)?),
        lease_owner_id: row.get(1)?,
        run_id: TaskRunId::from_string(row.get::<_, String>(2)?),
        lease_started_at: row.get(3)?,
        heartbeat_at: row.get(4)?,
        expires_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

fn worktree_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorktreeRecord> {
    Ok(WorktreeRecord {
        worktree_id: WorktreeId::from_string(row.get::<_, String>(0)?),
        project_id: ProjectId::from_string(row.get::<_, String>(1)?),
        task_id: TaskId::from_string(row.get::<_, String>(2)?),
        path: PathBuf::from(row.get::<_, String>(3)?),
        execution_mode: parse_db(ProjectExecutionMode::parse, row.get::<_, String>(4)?)?,
        state: parse_db(WorktreeState::parse, row.get::<_, String>(5)?)?,
        last_run_id: row.get::<_, Option<String>>(6)?.map(TaskRunId::from_string),
        last_used_at: row.get(7)?,
        created_at: row.get(8)?,
        updated_at: row.get(9)?,
        cleanup_after: row.get(10)?,
        reusable: row.get::<_, i64>(11)? != 0,
    })
}

fn worktree_lease_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorktreeLeaseRecord> {
    Ok(WorktreeLeaseRecord {
        worktree_id: WorktreeId::from_string(row.get::<_, String>(0)?),
        project_id: ProjectId::from_string(row.get::<_, String>(1)?),
        lease_owner_id: row.get(2)?,
        run_id: TaskRunId::from_string(row.get::<_, String>(3)?),
        path: PathBuf::from(row.get::<_, String>(4)?),
        heartbeat_at: row.get(5)?,
        expires_at: row.get(6)?,
        created_at: row.get(7)?,
    })
}

fn dispatch_decision_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DispatchDecisionRecord> {
    Ok(DispatchDecisionRecord {
        decision_id: DispatchDecisionId::from_string(row.get::<_, String>(0)?),
        run_id: TaskRunId::from_string(row.get::<_, String>(1)?),
        decision_kind: match row.get::<_, String>(2)?.as_str() {
            "dispatch" => DecisionKind::Dispatch,
            "follow_up" => DecisionKind::FollowUp,
            "retry" => DecisionKind::Retry,
            _ => DecisionKind::Reconcile,
        },
        selected_identity_id: row
            .get::<_, Option<String>>(3)?
            .map(IdentityId::from_string),
        selected_worktree_id: row
            .get::<_, Option<String>>(4)?
            .map(WorktreeId::from_string),
        lineage_mode: match row.get::<_, String>(5)?.as_str() {
            "resume_same_identity" => LineageMode::ResumeSameIdentity,
            "resume_handoff" => LineageMode::ResumeHandoff,
            "resume_checkpoint" => LineageMode::ResumeCheckpoint,
            "pending_behind_active_run" => LineageMode::PendingBehindActiveRun,
            _ => LineageMode::NewThread,
        },
        reason: row.get(6)?,
        candidates: parse_json(row.get::<_, String>(7)?)?,
        policy_snapshot_json: parse_json(row.get::<_, String>(8)?)?,
        created_at: row.get(9)?,
    })
}

fn scheduler_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SchedulerEventRecord> {
    Ok(SchedulerEventRecord {
        event_id: SchedulerEventId::from_string(row.get::<_, String>(0)?),
        project_id: row.get::<_, Option<String>>(1)?.map(ProjectId::from_string),
        task_id: row.get::<_, Option<String>>(2)?.map(TaskId::from_string),
        run_id: row.get::<_, Option<String>>(3)?.map(TaskRunId::from_string),
        event_kind: row.get(4)?,
        message: row.get(5)?,
        payload_json: parse_json(row.get::<_, String>(6)?)?,
        created_at: row.get(7)?,
    })
}

fn parse_json<T: DeserializeOwned>(payload: String) -> rusqlite::Result<T> {
    serde_json::from_str(&payload)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_db<T>(parser: impl FnOnce(&str) -> Result<T>, value: String) -> rusqlite::Result<T> {
    parser(&value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{ProjectSubmitRequest, SchedulerStore, TaskSubmitRequest};
    use crate::domain::identity::IdentityId;
    use crate::error::AppError;
    use crate::task_orchestration::config::SchedulerSettings;
    use crate::task_orchestration::domain::{
        CleanupPolicy, DispatchDecisionId, DispatchDecisionRecord, LineageMode,
        ProjectExecutionMode, TaskAffinityPolicy, TaskRunStatus, TaskStatus, WorktreeId,
        WorktreeRecord, WorktreeState,
    };

    #[test]
    fn creates_projects_and_tasks_in_sqlite() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let project = store
            .create_project(ProjectSubmitRequest {
                name: "demo".to_string(),
                repo_root: temp.path().join("repo"),
                execution_mode: ProjectExecutionMode::CopyWorkspace,
                default_codex_args: vec!["--full-auto".to_string()],
                default_model_or_profile: Some("gpt-5.4".to_string()),
                env_allowlist: vec!["OPENAI_API_KEY".to_string()],
                cleanup_policy: CleanupPolicy::default(),
            })
            .unwrap();

        let task = store
            .submit_task(TaskSubmitRequest {
                project: project.name.clone(),
                title: "Implement storage".to_string(),
                prompt_text: "Build the storage layer".to_string(),
                prompt_file_path: None,
                priority: 10,
                labels: vec!["backend".to_string()],
                created_by: "test".to_string(),
                max_runtime_secs: Some(600),
                queue_if_busy: true,
                allow_oversubscribe: false,
                affinity_policy: TaskAffinityPolicy::Spread,
            })
            .unwrap();

        assert_eq!(task.task.title, "Implement storage");
        assert_eq!(store.list_projects().unwrap().len(), 1);
        assert_eq!(store.list_tasks(None).unwrap().len(), 1);
        assert_eq!(
            store.task_events(task.task.task_id.as_str()).unwrap().len(),
            1
        );
    }

    #[test]
    fn resolves_existing_workspace_project_for_same_root() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let repo_root = temp.path().join("repo");
        let project = store
            .create_project(ProjectSubmitRequest {
                name: "demo".to_string(),
                repo_root: repo_root.clone(),
                execution_mode: ProjectExecutionMode::CopyWorkspace,
                default_codex_args: Vec::new(),
                default_model_or_profile: None,
                env_allowlist: Vec::new(),
                cleanup_policy: CleanupPolicy::default(),
            })
            .unwrap();

        let resolved = store
            .resolve_or_create_workspace_project(&repo_root, ProjectExecutionMode::GitWorktree)
            .unwrap();
        assert_eq!(resolved.project_id, project.project_id);
        assert_eq!(store.list_projects().unwrap().len(), 1);
    }

    #[test]
    fn rejects_ambiguous_workspace_projects_for_same_root() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let repo_root = temp.path().join("repo");
        store
            .create_project(ProjectSubmitRequest {
                name: "demo-a".to_string(),
                repo_root: repo_root.clone(),
                execution_mode: ProjectExecutionMode::CopyWorkspace,
                default_codex_args: Vec::new(),
                default_model_or_profile: None,
                env_allowlist: Vec::new(),
                cleanup_policy: CleanupPolicy::default(),
            })
            .unwrap();
        store
            .create_project(ProjectSubmitRequest {
                name: "demo-b".to_string(),
                repo_root: repo_root.clone(),
                execution_mode: ProjectExecutionMode::GitWorktree,
                default_codex_args: Vec::new(),
                default_model_or_profile: None,
                env_allowlist: Vec::new(),
                cleanup_policy: CleanupPolicy::default(),
            })
            .unwrap();

        let error = store
            .resolve_or_create_workspace_project(&repo_root, ProjectExecutionMode::CopyWorkspace)
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::WorkspaceProjectAmbiguous { projects, .. }
                if projects == vec!["demo-a".to_string(), "demo-b".to_string()]
        ));
    }

    #[test]
    fn scheduler_control_defaults_disabled_and_can_be_toggled() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();

        let control = store.scheduler_control().unwrap();
        assert!(!control.scheduler_v1_enabled);
        assert!(control.last_quota_refresh_at.is_none());
        assert!(control.last_gc_at.is_none());

        let enabled = store.set_scheduler_feature_enabled(true).unwrap();
        assert!(enabled.scheduler_v1_enabled);

        let disabled = store.set_scheduler_feature_enabled(false).unwrap();
        assert!(!disabled.scheduler_v1_enabled);
    }

    #[test]
    fn enforces_single_active_assignment_per_task() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let project = store
            .create_project(ProjectSubmitRequest {
                name: "demo".to_string(),
                repo_root: temp.path().join("repo"),
                execution_mode: ProjectExecutionMode::CopyWorkspace,
                default_codex_args: Vec::new(),
                default_model_or_profile: None,
                env_allowlist: Vec::new(),
                cleanup_policy: CleanupPolicy::default(),
            })
            .unwrap();
        let snapshot = store
            .submit_task(TaskSubmitRequest {
                project: project.name.clone(),
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
            worktree_id: WorktreeId::from_string("worktree-1"),
            project_id: project.project_id.clone(),
            task_id: snapshot.task.task_id.clone(),
            path: temp.path().join("wt"),
            execution_mode: ProjectExecutionMode::CopyWorkspace,
            state: WorktreeState::Ready,
            last_run_id: None,
            last_used_at: 1,
            created_at: 1,
            updated_at: 1,
            cleanup_after: None,
            reusable: true,
        };
        let decision = DispatchDecisionRecord {
            decision_id: DispatchDecisionId::new(),
            run_id: run.run_id.clone(),
            decision_kind: crate::task_orchestration::domain::DecisionKind::Dispatch,
            selected_identity_id: Some(IdentityId::from_string("identity-1")),
            selected_worktree_id: Some(worktree.worktree_id.clone()),
            lineage_mode: LineageMode::NewThread,
            reason: "test assignment".to_string(),
            candidates: Vec::new(),
            policy_snapshot_json: json!({}),
            created_at: 1,
        };

        let claimed = store
            .claim_assignment(
                &super::AssignmentClaim {
                    run_id: run.run_id.clone(),
                    task_id: run.task_id.clone(),
                    project_id: project.project_id.clone(),
                    identity_id: IdentityId::from_string("identity-1"),
                    worktree,
                    worker_owner_id: "worker-1".to_string(),
                    launch_mode: crate::task_orchestration::domain::LaunchMode::NewThread,
                    lineage_mode: LineageMode::NewThread,
                    reason: "test assignment".to_string(),
                    decision,
                    lease_expires_at: 100,
                },
                &SchedulerSettings::default(),
            )
            .unwrap();
        assert!(claimed);

        let active = store.scheduler_health(&BTreeSet::new(), 0).unwrap();
        assert_eq!(active.active_runs, 1);
    }

    #[test]
    fn requeues_orphaned_runs_when_configured() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let project = store
            .create_project(ProjectSubmitRequest {
                name: "demo".to_string(),
                repo_root: temp.path().join("repo"),
                execution_mode: ProjectExecutionMode::CopyWorkspace,
                default_codex_args: Vec::new(),
                default_model_or_profile: None,
                env_allowlist: Vec::new(),
                cleanup_policy: CleanupPolicy::default(),
            })
            .unwrap();
        let snapshot = store
            .submit_task(TaskSubmitRequest {
                project: project.name.clone(),
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
            worktree_id: WorktreeId::from_string("worktree-1"),
            project_id: project.project_id.clone(),
            task_id: snapshot.task.task_id.clone(),
            path: temp.path().join("wt"),
            execution_mode: ProjectExecutionMode::CopyWorkspace,
            state: WorktreeState::Ready,
            last_run_id: None,
            last_used_at: 1,
            created_at: 1,
            updated_at: 1,
            cleanup_after: None,
            reusable: true,
        };
        let decision = DispatchDecisionRecord {
            decision_id: DispatchDecisionId::new(),
            run_id: run.run_id.clone(),
            decision_kind: crate::task_orchestration::domain::DecisionKind::Dispatch,
            selected_identity_id: Some(IdentityId::from_string("identity-1")),
            selected_worktree_id: Some(worktree.worktree_id.clone()),
            lineage_mode: LineageMode::NewThread,
            reason: "test assignment".to_string(),
            candidates: Vec::new(),
            policy_snapshot_json: json!({}),
            created_at: 1,
        };
        store
            .claim_assignment(
                &super::AssignmentClaim {
                    run_id: run.run_id.clone(),
                    task_id: run.task_id.clone(),
                    project_id: project.project_id.clone(),
                    identity_id: IdentityId::from_string("identity-1"),
                    worktree,
                    worker_owner_id: "worker-1".to_string(),
                    launch_mode: crate::task_orchestration::domain::LaunchMode::NewThread,
                    lineage_mode: LineageMode::NewThread,
                    reason: "test assignment".to_string(),
                    decision,
                    lease_expires_at: 1,
                },
                &SchedulerSettings::default(),
            )
            .unwrap();

        let requeued = store
            .reconcile_orphaned_runs(100, &SchedulerSettings::default())
            .unwrap();
        assert_eq!(requeued.len(), 1);
        let run = store.get_run(run.run_id.as_str()).unwrap();
        assert_eq!(run.status, TaskRunStatus::PendingAssignment);
        let task = store.get_task(snapshot.task.task_id.as_str()).unwrap();
        assert_eq!(task.status, TaskStatus::Queued);
    }

    #[test]
    fn keeps_leases_alive_with_stable_worker_owner() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let (project, snapshot) = create_project_and_task(&mut store, temp.path());
        let run = snapshot.runs[0].clone();
        let worktree = make_worktree(&project, &snapshot.task.task_id, temp.path().join("wt"));
        let worker_owner_id = "worker-lease-1";

        claim_run(
            &mut store,
            &project,
            &run,
            &snapshot.task.task_id,
            worktree,
            worker_owner_id,
        );
        store
            .mark_worker_spawned(run.run_id.as_str(), worker_owner_id, 4242)
            .unwrap();
        store
            .start_run_launching(run.run_id.as_str(), worker_owner_id, 4242, 150)
            .unwrap();
        store
            .mark_run_running(
                run.run_id.as_str(),
                worker_owner_id,
                "thread-1",
                Some("turn-1"),
                200,
            )
            .unwrap();
        store
            .heartbeat_run(run.run_id.as_str(), worker_owner_id, Some("turn-1"), 250)
            .unwrap();

        let account_leases = store.active_account_leases().unwrap();
        assert_eq!(account_leases.len(), 1);
        assert_eq!(account_leases[0].lease_owner_id, worker_owner_id);
        assert_eq!(account_leases[0].expires_at, 250);

        let worktree_leases = store.worktree_leases().unwrap();
        assert_eq!(worktree_leases.len(), 1);
        assert_eq!(worktree_leases[0].lease_owner_id, worker_owner_id);
        assert_eq!(worktree_leases[0].expires_at, 250);
    }

    #[test]
    fn canceling_running_task_is_terminal_and_releases_leases() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let (project, snapshot) = create_project_and_task(&mut store, temp.path());
        let run = snapshot.runs[0].clone();
        let worktree = make_worktree(&project, &snapshot.task.task_id, temp.path().join("wt"));
        let worker_owner_id = "worker-lease-2";

        claim_run(
            &mut store,
            &project,
            &run,
            &snapshot.task.task_id,
            worktree,
            worker_owner_id,
        );
        store
            .mark_worker_spawned(run.run_id.as_str(), worker_owner_id, 9898)
            .unwrap();
        store
            .start_run_launching(run.run_id.as_str(), worker_owner_id, 9898, 150)
            .unwrap();
        store
            .mark_run_running(
                run.run_id.as_str(),
                worker_owner_id,
                "thread-1",
                Some("turn-1"),
                200,
            )
            .unwrap();

        let outcome = store.cancel_task(snapshot.task.task_id.as_str()).unwrap();
        assert_eq!(outcome.interrupted_runs.len(), 1);
        assert_eq!(outcome.interrupted_runs[0].worker_pid, Some(9898));
        assert!(store.active_account_leases().unwrap().is_empty());
        assert!(store.worktree_leases().unwrap().is_empty());
        let worktree = store
            .get_worktree(outcome.interrupted_runs[0].worktree_id.as_ref().unwrap())
            .unwrap();
        assert_eq!(worktree.state, WorktreeState::Corrupted);
        assert!(worktree.cleanup_after.is_none());
        assert!(store
            .reusable_worktree_for_task(&snapshot.task.task_id)
            .unwrap()
            .is_none());

        store
            .schedule_canceled_worktree_cleanup(
                &[outcome.interrupted_runs[0].worktree_id.clone().unwrap()],
                Duration::from_secs(60),
            )
            .unwrap();
        let worktree = store
            .get_worktree(outcome.interrupted_runs[0].worktree_id.as_ref().unwrap())
            .unwrap();
        assert!(worktree.cleanup_after.is_some());

        store
            .finish_run(
                run.run_id.as_str(),
                super::RunCompletion {
                    status: TaskRunStatus::Completed,
                    exit_code: Some(0),
                    failure_kind: None,
                    failure_message: None,
                    thread_id: Some("thread-1".to_string()),
                    checkpoint_id: None,
                    last_identity_id: Some(IdentityId::from_string("identity-1")),
                },
            )
            .unwrap();

        let canceled_run = store.get_run(run.run_id.as_str()).unwrap();
        assert_eq!(canceled_run.status, TaskRunStatus::Canceled);
        let task = store.get_task(snapshot.task.task_id.as_str()).unwrap();
        assert_eq!(task.status, TaskStatus::Canceled);
    }

    #[test]
    fn reset_state_is_blocked_while_active_runs_exist() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let (project, snapshot) = create_project_and_task(&mut store, temp.path());
        let run = snapshot.runs[0].clone();
        let worktree = make_worktree(&project, &snapshot.task.task_id, temp.path().join("wt"));

        claim_run(
            &mut store,
            &project,
            &run,
            &snapshot.task.task_id,
            worktree,
            "worker-lease-reset",
        );

        let error =
            SchedulerStore::reset_state(temp.path(), &SchedulerSettings::default()).unwrap_err();
        assert!(matches!(
            error,
            AppError::SchedulerResetBlocked {
                active_runs: 1,
                account_leases: 1,
                worktree_leases: 1,
            }
        ));
    }

    #[test]
    fn spawn_failure_rollback_requeues_assignment_and_releases_leases() {
        let temp = tempdir().unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let (project, snapshot) = create_project_and_task(&mut store, temp.path());
        let run = snapshot.runs[0].clone();
        let worktree = make_worktree(&project, &snapshot.task.task_id, temp.path().join("wt"));
        let worker_owner_id = "worker-lease-3";

        claim_run(
            &mut store,
            &project,
            &run,
            &snapshot.task.task_id,
            worktree,
            worker_owner_id,
        );
        assert!(store
            .rollback_assignment_after_spawn_failure(
                run.run_id.as_str(),
                worker_owner_id,
                "spawn failed"
            )
            .unwrap());

        let run = store.get_run(run.run_id.as_str()).unwrap();
        assert_eq!(run.status, TaskRunStatus::PendingAssignment);
        assert!(run.assigned_identity_id.is_none());
        assert!(run.assigned_worktree_id.is_none());
        assert!(store.active_account_leases().unwrap().is_empty());
        assert!(store.worktree_leases().unwrap().is_empty());
        let task = store.get_task(snapshot.task.task_id.as_str()).unwrap();
        assert_eq!(task.status, TaskStatus::Queued);
    }

    #[test]
    fn scheduler_lock_requires_expiry_before_takeover() {
        let temp = tempdir().unwrap();
        let mut store_a = SchedulerStore::open(temp.path()).unwrap();
        let mut store_b = SchedulerStore::open(temp.path()).unwrap();

        store_a
            .acquire_scheduler_lock("scheduler-a", Duration::from_secs(60))
            .unwrap();
        let error = store_b
            .acquire_scheduler_lock("scheduler-b", Duration::from_secs(60))
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::SchedulerAlreadyRunning { owner_id } if owner_id == "scheduler-a"
        ));

        store_a
            .connection
            .execute(
                "UPDATE scheduler_process_lock SET expires_at = 0 WHERE lock_name = 'dispatcher'",
                [],
            )
            .unwrap();

        store_b
            .acquire_scheduler_lock("scheduler-b", Duration::from_secs(60))
            .unwrap();
    }

    fn create_project_and_task(
        store: &mut SchedulerStore,
        base_root: &std::path::Path,
    ) -> (
        crate::task_orchestration::domain::ProjectRecord,
        crate::task_orchestration::domain::TaskLineageSnapshot,
    ) {
        let project = store
            .create_project(ProjectSubmitRequest {
                name: "demo".to_string(),
                repo_root: base_root.join("repo"),
                execution_mode: ProjectExecutionMode::CopyWorkspace,
                default_codex_args: Vec::new(),
                default_model_or_profile: None,
                env_allowlist: Vec::new(),
                cleanup_policy: CleanupPolicy::default(),
            })
            .unwrap();
        let snapshot = store
            .submit_task(TaskSubmitRequest {
                project: project.name.clone(),
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
        (project, snapshot)
    }

    fn make_worktree(
        project: &crate::task_orchestration::domain::ProjectRecord,
        task_id: &crate::task_orchestration::domain::TaskId,
        path: std::path::PathBuf,
    ) -> WorktreeRecord {
        WorktreeRecord {
            worktree_id: WorktreeId::from_string(format!("worktree-{}", path.display())),
            project_id: project.project_id.clone(),
            task_id: task_id.clone(),
            path,
            execution_mode: ProjectExecutionMode::CopyWorkspace,
            state: WorktreeState::Ready,
            last_run_id: None,
            last_used_at: 1,
            created_at: 1,
            updated_at: 1,
            cleanup_after: None,
            reusable: true,
        }
    }

    fn claim_run(
        store: &mut SchedulerStore,
        project: &crate::task_orchestration::domain::ProjectRecord,
        run: &crate::task_orchestration::domain::TaskRunRecord,
        task_id: &crate::task_orchestration::domain::TaskId,
        worktree: WorktreeRecord,
        worker_owner_id: &str,
    ) {
        let decision = DispatchDecisionRecord {
            decision_id: DispatchDecisionId::new(),
            run_id: run.run_id.clone(),
            decision_kind: crate::task_orchestration::domain::DecisionKind::Dispatch,
            selected_identity_id: Some(IdentityId::from_string("identity-1")),
            selected_worktree_id: Some(worktree.worktree_id.clone()),
            lineage_mode: LineageMode::NewThread,
            reason: "test assignment".to_string(),
            candidates: Vec::new(),
            policy_snapshot_json: json!({}),
            created_at: 1,
        };
        let claimed = store
            .claim_assignment(
                &super::AssignmentClaim {
                    run_id: run.run_id.clone(),
                    task_id: task_id.clone(),
                    project_id: project.project_id.clone(),
                    identity_id: IdentityId::from_string("identity-1"),
                    worktree,
                    worker_owner_id: worker_owner_id.to_string(),
                    launch_mode: crate::task_orchestration::domain::LaunchMode::NewThread,
                    lineage_mode: LineageMode::NewThread,
                    reason: "test assignment".to_string(),
                    decision,
                    lease_expires_at: 100,
                },
                &SchedulerSettings::default(),
            )
            .unwrap();
        assert!(claimed);
    }
}
