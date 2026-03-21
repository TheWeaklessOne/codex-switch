use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::domain::identity::{current_timestamp, IdentityId};
use crate::error::{AppError, Result};

static ENTITY_COUNTER: AtomicU64 = AtomicU64::new(1);

macro_rules! string_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self(new_entity_id($prefix))
            }

            pub fn from_string(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(ProjectId, "project");
string_id!(TaskId, "task");
string_id!(TaskRunId, "run");
string_id!(WorktreeId, "worktree");
string_id!(DispatchDecisionId, "dispatch");
string_id!(SchedulerEventId, "scheduler-event");

fn new_entity_id(prefix: &str) -> String {
    let counter = ENTITY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{prefix}-{}-{}-{}", std::process::id(), nanos, counter)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectExecutionMode {
    GitWorktree,
    CopyWorkspace,
}

impl ProjectExecutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GitWorktree => "git_worktree",
            Self::CopyWorkspace => "copy_workspace",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "git_worktree" => Ok(Self::GitWorktree),
            "copy_workspace" => Ok(Self::CopyWorkspace),
            _ => Err(AppError::UnsupportedProjectExecutionMode {
                mode: value.to_string(),
            }),
        }
    }
}

impl fmt::Display for ProjectExecutionMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanupPolicy {
    pub completed_ttl_secs: i64,
    pub failed_ttl_secs: i64,
    pub immediate_cleanup_terminal_failures: bool,
}

impl Default for CleanupPolicy {
    fn default() -> Self {
        Self {
            completed_ttl_secs: 60 * 60 * 24,
            failed_ttl_secs: 60 * 60,
            immediate_cleanup_terminal_failures: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub project_id: ProjectId,
    pub name: String,
    pub repo_root: PathBuf,
    pub execution_mode: ProjectExecutionMode,
    pub default_codex_args: Vec<String>,
    pub default_model_or_profile: Option<String>,
    pub env_allowlist: Vec<String>,
    pub cleanup_policy: CleanupPolicy,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Running,
    AwaitingFollowup,
    Completed,
    FailedRetryable,
    FailedTerminal,
    Canceled,
    Orphaned,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::AwaitingFollowup => "awaiting_followup",
            Self::Completed => "completed",
            Self::FailedRetryable => "failed_retryable",
            Self::FailedTerminal => "failed_terminal",
            Self::Canceled => "canceled",
            Self::Orphaned => "orphaned",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "awaiting_followup" => Ok(Self::AwaitingFollowup),
            "completed" => Ok(Self::Completed),
            "failed_retryable" => Ok(Self::FailedRetryable),
            "failed_terminal" => Ok(Self::FailedTerminal),
            "canceled" => Ok(Self::Canceled),
            "orphaned" => Ok(Self::Orphaned),
            _ => Err(AppError::InvalidTaskTransition {
                task_id: "unknown".to_string(),
                from: value.to_string(),
                to: "parse".to_string(),
            }),
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: TaskId,
    pub project_id: ProjectId,
    pub title: String,
    pub status: TaskStatus,
    pub priority: i64,
    pub labels: Vec<String>,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub current_lineage_thread_id: Option<String>,
    pub preferred_identity_id: Option<IdentityId>,
    pub last_identity_id: Option<IdentityId>,
    pub last_checkpoint_id: Option<String>,
    pub last_completed_run_id: Option<TaskRunId>,
    pub pending_followup_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    Initial,
    FollowUp,
    Retry,
}

impl RunKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::FollowUp => "follow_up",
            Self::Retry => "retry",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "initial" => Ok(Self::Initial),
            "follow_up" => Ok(Self::FollowUp),
            "retry" => Ok(Self::Retry),
            _ => Err(AppError::InvalidRunTransition {
                run_id: "unknown".to_string(),
                from: value.to_string(),
                to: "parse".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunStatus {
    PendingAssignment,
    Assigned,
    Launching,
    Running,
    Completed,
    Failed,
    TimedOut,
    HandoffPending,
    Abandoned,
    Canceled,
    Orphaned,
}

impl TaskRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PendingAssignment => "pending_assignment",
            Self::Assigned => "assigned",
            Self::Launching => "launching",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::HandoffPending => "handoff_pending",
            Self::Abandoned => "abandoned",
            Self::Canceled => "canceled",
            Self::Orphaned => "orphaned",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "pending_assignment" => Ok(Self::PendingAssignment),
            "assigned" => Ok(Self::Assigned),
            "launching" => Ok(Self::Launching),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "timed_out" => Ok(Self::TimedOut),
            "handoff_pending" => Ok(Self::HandoffPending),
            "abandoned" => Ok(Self::Abandoned),
            "canceled" => Ok(Self::Canceled),
            "orphaned" => Ok(Self::Orphaned),
            _ => Err(AppError::InvalidRunTransition {
                run_id: "unknown".to_string(),
                from: value.to_string(),
                to: "parse".to_string(),
            }),
        }
    }

    pub fn is_active(self) -> bool {
        matches!(
            self,
            Self::Assigned | Self::Launching | Self::Running | Self::HandoffPending
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::Failed
                | Self::TimedOut
                | Self::Abandoned
                | Self::Canceled
                | Self::Orphaned
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchMode {
    NewThread,
    ResumeSameIdentity,
    ResumeHandoff,
    ResumeCheckpoint,
}

impl LaunchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewThread => "new_thread",
            Self::ResumeSameIdentity => "resume_same_identity",
            Self::ResumeHandoff => "resume_handoff",
            Self::ResumeCheckpoint => "resume_checkpoint",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "new_thread" => Ok(Self::NewThread),
            "resume_same_identity" => Ok(Self::ResumeSameIdentity),
            "resume_handoff" => Ok(Self::ResumeHandoff),
            "resume_checkpoint" => Ok(Self::ResumeCheckpoint),
            _ => Err(AppError::InvalidRunTransition {
                run_id: "unknown".to_string(),
                from: value.to_string(),
                to: "parse".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Launch,
    Runtime,
    RetryableAuth,
    RetryableRateLimit,
    Handoff,
    Checkpoint,
    Timeout,
    WorkerExited,
    WorkerSpawn,
    Canceled,
    Validation,
}

impl FailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Launch => "launch",
            Self::Runtime => "runtime",
            Self::RetryableAuth => "retryable_auth",
            Self::RetryableRateLimit => "retryable_rate_limit",
            Self::Handoff => "handoff",
            Self::Checkpoint => "checkpoint",
            Self::Timeout => "timeout",
            Self::WorkerExited => "worker_exited",
            Self::WorkerSpawn => "worker_spawn",
            Self::Canceled => "canceled",
            Self::Validation => "validation",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "launch" => Ok(Self::Launch),
            "runtime" => Ok(Self::Runtime),
            "retryable_auth" => Ok(Self::RetryableAuth),
            "retryable_rate_limit" => Ok(Self::RetryableRateLimit),
            "handoff" => Ok(Self::Handoff),
            "checkpoint" => Ok(Self::Checkpoint),
            "timeout" => Ok(Self::Timeout),
            "worker_exited" => Ok(Self::WorkerExited),
            "worker_spawn" => Ok(Self::WorkerSpawn),
            "canceled" => Ok(Self::Canceled),
            "validation" => Ok(Self::Validation),
            _ => Err(AppError::InvalidRunTransition {
                run_id: "unknown".to_string(),
                from: value.to_string(),
                to: "parse".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAffinityPolicy {
    Spread,
    PreferSameIdentity,
    PreferProjectLocality,
}

impl TaskAffinityPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spread => "spread",
            Self::PreferSameIdentity => "prefer_same_identity",
            Self::PreferProjectLocality => "prefer_project_locality",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "spread" => Ok(Self::Spread),
            "prefer_same_identity" => Ok(Self::PreferSameIdentity),
            "prefer_project_locality" => Ok(Self::PreferProjectLocality),
            _ => Err(AppError::InvalidSchedulerConfiguration {
                message: format!("unsupported affinity policy {value}"),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRunRecord {
    pub run_id: TaskRunId,
    pub task_id: TaskId,
    pub sequence_no: u32,
    pub run_kind: RunKind,
    pub status: TaskRunStatus,
    pub input_artifact_path: PathBuf,
    pub requested_at: i64,
    pub assigned_identity_id: Option<IdentityId>,
    pub assigned_worktree_id: Option<WorktreeId>,
    pub assigned_thread_id: Option<String>,
    pub launch_mode: Option<LaunchMode>,
    pub retry_count: u32,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub exit_code: Option<i32>,
    pub failure_kind: Option<FailureKind>,
    pub failure_message: Option<String>,
    pub max_runtime_secs: Option<i64>,
    pub queue_if_busy: bool,
    pub allow_oversubscribe: bool,
    pub affinity_policy: TaskAffinityPolicy,
    pub worker_pid: Option<u32>,
    pub worker_owner_id: Option<String>,
    pub heartbeat_at: Option<i64>,
    pub heartbeat_expires_at: Option<i64>,
    pub last_turn_id: Option<String>,
    pub run_attempt_no: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRunInputRecord {
    pub run_id: TaskRunId,
    pub prompt_text: String,
    pub prompt_file_path: Option<PathBuf>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountRuntimeState {
    Free,
    Reserved,
    Launching,
    Running,
    Draining,
    Offline,
}

impl AccountRuntimeState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Free => "free",
            Self::Reserved => "reserved",
            Self::Launching => "launching",
            Self::Running => "running",
            Self::Draining => "draining",
            Self::Offline => "offline",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "free" => Ok(Self::Free),
            "reserved" => Ok(Self::Reserved),
            "launching" => Ok(Self::Launching),
            "running" => Ok(Self::Running),
            "draining" => Ok(Self::Draining),
            "offline" => Ok(Self::Offline),
            _ => Err(AppError::InvalidSchedulerConfiguration {
                message: format!("unsupported account runtime state {value}"),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountRuntimeRecord {
    pub identity_id: IdentityId,
    pub state: AccountRuntimeState,
    pub active_run_id: Option<TaskRunId>,
    pub active_count: u32,
    pub last_dispatch_at: Option<i64>,
    pub last_success_at: Option<i64>,
    pub last_failure_at: Option<i64>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountLeaseRecord {
    pub identity_id: IdentityId,
    pub lease_owner_id: String,
    pub run_id: TaskRunId,
    pub lease_started_at: i64,
    pub heartbeat_at: i64,
    pub expires_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeState {
    Ready,
    Leased,
    Missing,
    Corrupted,
    Cleaning,
    Removed,
}

impl WorktreeState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Leased => "leased",
            Self::Missing => "missing",
            Self::Corrupted => "corrupted",
            Self::Cleaning => "cleaning",
            Self::Removed => "removed",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "ready" => Ok(Self::Ready),
            "leased" => Ok(Self::Leased),
            "missing" => Ok(Self::Missing),
            "corrupted" => Ok(Self::Corrupted),
            "cleaning" => Ok(Self::Cleaning),
            "removed" => Ok(Self::Removed),
            _ => Err(AppError::InvalidSchedulerConfiguration {
                message: format!("unsupported worktree state {value}"),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeRecord {
    pub worktree_id: WorktreeId,
    pub project_id: ProjectId,
    pub task_id: TaskId,
    pub path: PathBuf,
    pub execution_mode: ProjectExecutionMode,
    pub state: WorktreeState,
    pub last_run_id: Option<TaskRunId>,
    pub last_used_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub cleanup_after: Option<i64>,
    pub reusable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeLeaseRecord {
    pub worktree_id: WorktreeId,
    pub project_id: ProjectId,
    pub lease_owner_id: String,
    pub run_id: TaskRunId,
    pub path: PathBuf,
    pub heartbeat_at: i64,
    pub expires_at: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    Dispatch,
    FollowUp,
    Retry,
    Reconcile,
}

impl DecisionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dispatch => "dispatch",
            Self::FollowUp => "follow_up",
            Self::Retry => "retry",
            Self::Reconcile => "reconcile",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineageMode {
    NewThread,
    ResumeSameIdentity,
    ResumeHandoff,
    ResumeCheckpoint,
    PendingBehindActiveRun,
}

impl LineageMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewThread => "new_thread",
            Self::ResumeSameIdentity => "resume_same_identity",
            Self::ResumeHandoff => "resume_handoff",
            Self::ResumeCheckpoint => "resume_checkpoint",
            Self::PendingBehindActiveRun => "pending_behind_active_run",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateAssessment {
    pub identity_id: IdentityId,
    pub display_name: String,
    pub eligible: bool,
    pub rejection_reason: Option<String>,
    pub occupancy_state: String,
    pub active_count: u32,
    pub same_task_affinity: bool,
    pub same_identity_affinity: bool,
    pub quota_bucket: Option<String>,
    pub remaining_headroom_percent: Option<i32>,
    pub priority: u32,
    pub selected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchDecisionRecord {
    pub decision_id: DispatchDecisionId,
    pub run_id: TaskRunId,
    pub decision_kind: DecisionKind,
    pub selected_identity_id: Option<IdentityId>,
    pub selected_worktree_id: Option<WorktreeId>,
    pub lineage_mode: LineageMode,
    pub reason: String,
    pub candidates: Vec<CandidateAssessment>,
    pub policy_snapshot_json: serde_json::Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerEventRecord {
    pub event_id: SchedulerEventId,
    pub project_id: Option<ProjectId>,
    pub task_id: Option<TaskId>,
    pub run_id: Option<TaskRunId>,
    pub event_kind: String,
    pub message: String,
    pub payload_json: serde_json::Value,
    pub created_at: i64,
}

impl SchedulerEventRecord {
    pub fn new(
        project_id: Option<ProjectId>,
        task_id: Option<TaskId>,
        run_id: Option<TaskRunId>,
        event_kind: impl Into<String>,
        message: impl Into<String>,
        payload_json: serde_json::Value,
    ) -> Result<Self> {
        Ok(Self {
            event_id: SchedulerEventId::new(),
            project_id,
            task_id,
            run_id,
            event_kind: event_kind.into(),
            message: message.into(),
            payload_json,
            created_at: current_timestamp()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskLineageSnapshot {
    pub task: TaskRecord,
    pub runs: Vec<TaskRunRecord>,
    pub latest_input: Option<TaskRunInputRecord>,
    pub latest_dispatch_decision: Option<DispatchDecisionRecord>,
}
