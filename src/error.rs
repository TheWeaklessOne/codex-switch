use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

use crate::domain::identity::IdentityId;
use crate::domain::thread::{ThreadLeaseState, TrackedTurnStateStatus};

pub type Result<T> = std::result::Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("home directory is not available in the environment")]
    MissingHomeDirectory,

    #[error("current working directory is not available: {source}")]
    CurrentDirectoryUnavailable { source: std::io::Error },

    #[error("identity name resolves to an empty slug")]
    EmptyIdentitySlug,

    #[error("identity {identity_id} is already registered")]
    IdentityAlreadyExists { identity_id: IdentityId },

    #[error("identity {identity_id} is not registered")]
    IdentityNotFound { identity_id: IdentityId },

    #[error("codex home {path} is already registered to identity {identity_id}")]
    HomeAlreadyRegistered {
        path: PathBuf,
        identity_id: IdentityId,
    },

    #[error("{path} already exists and is not a directory")]
    ExpectedDirectory { path: PathBuf },

    #[error("{path} exists and is a symlink; managed state must not use symlinked paths here")]
    UnexpectedSymlink { path: PathBuf },

    #[error("{path} exists and is not a regular file")]
    ExpectedFile { path: PathBuf },

    #[error("{path} already exists; rerun with --overwrite-config if you intend to replace it")]
    ConfigAlreadyExists { path: PathBuf },

    #[error("{path} exists and is not a symlink")]
    SessionsLinkNotSymlink { path: PathBuf },

    #[error("{path} points to {actual}, expected {expected}")]
    SessionsLinkConflict {
        path: PathBuf,
        actual: PathBuf,
        expected: PathBuf,
    },

    #[error("missing auth.json in {source_home}")]
    MissingAuthFile { source_home: PathBuf },

    #[error("registry version {found} is not supported")]
    UnsupportedRegistryVersion { found: u32 },

    #[error("quota status version {found} is not supported")]
    UnsupportedQuotaStatusVersion { found: u32 },

    #[error("selection policy version {found} is not supported")]
    UnsupportedSelectionPolicyVersion { found: u32 },

    #[error("identity health version {found} is not supported")]
    UnsupportedIdentityHealthVersion { found: u32 },

    #[error("selection state version {found} is not supported")]
    UnsupportedSelectionStateVersion { found: u32 },

    #[error("selection event version {found} is not supported")]
    UnsupportedSelectionEventVersion { found: u32 },

    #[error("task checkpoint version {found} is not supported")]
    UnsupportedTaskCheckpointVersion { found: u32 },

    #[error("rpc call {method} timed out after {timeout:?}")]
    RpcTimeout { method: String, timeout: Duration },

    #[error("rpc call {method} failed with code {code}: {message}")]
    RpcServer {
        method: String,
        code: i64,
        message: String,
    },

    #[error("rpc response for {method} was missing a result payload")]
    MissingRpcResult { method: String },

    #[error("app-server exited before completing {method}. stderr: {stderr}")]
    AppServerExited { method: String, stderr: String },

    #[error("failed to decode rpc payload for {method}: {source}")]
    RpcPayloadDecode {
        method: String,
        source: serde_json::Error,
    },

    #[error("invalid environment variable name: {name}")]
    InvalidEnvironmentVariableName { name: String },

    #[error("shared sessions root mismatch: {source_identity_id} uses {source_root}, {target_identity_id} uses {target_root}")]
    SharedSessionsRootMismatch {
        source_identity_id: IdentityId,
        source_root: PathBuf,
        target_identity_id: IdentityId,
        target_root: PathBuf,
    },

    #[error("thread {thread_id} is already leased to {owner_identity_id}")]
    ThreadLeaseHeld {
        thread_id: String,
        owner_identity_id: IdentityId,
    },

    #[error("no lease record exists for thread {thread_id}")]
    ThreadLeaseNotFound { thread_id: String },

    #[error("lease token for thread {thread_id} does not match the current owner token")]
    ThreadLeaseTokenMismatch { thread_id: String },

    #[error("thread {thread_id} expected lease state {expected} but found {actual}")]
    ThreadLeaseStateConflict {
        thread_id: String,
        expected: ThreadLeaseState,
        actual: ThreadLeaseState,
    },

    #[error("handoff target is missing for thread {thread_id}")]
    HandoffTargetMissing { thread_id: String },

    #[error("thread {thread_id} is reserved for {expected_identity_id}, not {actual_identity_id}")]
    HandoffTargetMismatch {
        thread_id: String,
        expected_identity_id: IdentityId,
        actual_identity_id: IdentityId,
    },

    #[error("timed out while waiting to lock {path} after {timeout:?}")]
    LeaseLockTimeout { path: PathBuf, timeout: Duration },

    #[error("shared sessions store does not expose the same persisted history for thread {thread_id}: {source_identity_id} -> {target_identity_id}")]
    ThreadHistoryNotShared {
        thread_id: String,
        source_identity_id: IdentityId,
        target_identity_id: IdentityId,
    },

    #[error("no tracked turn state exists for thread {thread_id}")]
    ThreadStateNotFound { thread_id: String },

    #[error("no handoff has been prepared for thread {thread_id}")]
    HandoffNotPrepared { thread_id: String },

    #[error("session {session_id} is not registered")]
    SessionNotFound { session_id: String },

    #[error("turn {turn_id} is not registered")]
    SessionTurnNotFound { turn_id: String },

    #[error("handoff {handoff_id} is not registered")]
    SessionHandoffNotFound { handoff_id: String },

    #[error("thread {thread_id} expected tracked state {expected} but found {actual}")]
    TrackedTurnStateConflict {
        thread_id: String,
        expected: TrackedTurnStateStatus,
        actual: TrackedTurnStateStatus,
    },

    #[error("persisted history for thread {thread_id} did not advance after handoff (baseline turns {baseline_turn_count}, current turns {current_turn_count})")]
    HandoffHistoryUnchanged {
        thread_id: String,
        baseline_turn_count: usize,
        current_turn_count: usize,
    },

    #[error("no eligible identity is available for a new session")]
    NoSelectableIdentity,

    #[error("no identity is currently selected")]
    NoIdentitySelected,

    #[error("cannot combine an explicit identity with --auto")]
    ConflictingIdentityResolution,

    #[error("continue requires either --to <identity> or --auto")]
    ContinueTargetRequired,

    #[error("session {session_id} already has an active turn{active_turn_suffix}")]
    SessionTurnAlreadyActive {
        session_id: String,
        active_turn_suffix: String,
    },

    #[error("session {session_id} has a pending handoff {handoff_id}")]
    SessionHandoffPending {
        session_id: String,
        handoff_id: String,
    },

    #[error("session {session_id} cannot resume same-thread from {current_identity_id} to {requested_identity_id} without an explicit handoff or fallback")]
    UnsafeSameThreadResume {
        session_id: String,
        current_identity_id: IdentityId,
        requested_identity_id: IdentityId,
    },

    #[error("session {session_id} requires checkpoint fallback before more work can start")]
    CheckpointFallbackRequired {
        session_id: String,
        handoff_id: Option<String>,
    },

    #[error("inject requires either --identity <name> or --auto")]
    InjectIdentityRequired,

    #[error("--no-verify requires --login when adding an identity")]
    AddNoVerifyRequiresLogin,

    #[error("invalid selection policy: {message}")]
    InvalidSelectionPolicy { message: String },

    #[error("identity {identity_id} requires environment variable {name} to be set before launch")]
    MissingApiKeyEnvironmentVariable {
        identity_id: IdentityId,
        name: String,
    },

    #[error("identity {identity_id} does not support forced workspace switching")]
    WorkspaceForceUnsupported { identity_id: IdentityId },

    #[error("identity {identity_id} has no recorded workspace id")]
    WorkspaceForceWorkspaceIdMissing { identity_id: IdentityId },

    #[error("{program} exited with status {code}")]
    ChildProcessFailed { program: String, code: String },

    #[error("{operation} failed: {primary}; rollback failed: {rollback}")]
    RollbackFailed {
        operation: String,
        primary: String,
        rollback: String,
    },

    #[error("sqlite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("project {project} is not registered")]
    ProjectNotFound { project: String },

    #[error("task {task_id} is not registered")]
    TaskNotFound { task_id: String },

    #[error("run {run_id} is not registered")]
    TaskRunNotFound { run_id: String },

    #[error("scheduler lock is held by {owner_id}")]
    SchedulerAlreadyRunning { owner_id: String },

    #[error("scheduler state reset is blocked while {active_runs} active runs, {account_leases} account leases, or {worktree_leases} worktree leases remain")]
    SchedulerResetBlocked {
        active_runs: usize,
        account_leases: usize,
        worktree_leases: usize,
    },

    #[error("scheduler feature requires a git repository at {path}")]
    GitRepositoryRequired { path: PathBuf },

    #[error("task {task_id} already has an active run")]
    TaskAlreadyActive { task_id: String },

    #[error("identity {identity_id} has no available scheduler lease capacity")]
    IdentityBusy { identity_id: IdentityId },

    #[error("worktree {path} is already leased")]
    WorktreeBusy { path: PathBuf },

    #[error("run {run_id} cannot transition from {from} to {to}")]
    InvalidRunTransition {
        run_id: String,
        from: String,
        to: String,
    },

    #[error("task {task_id} cannot transition from {from} to {to}")]
    InvalidTaskTransition {
        task_id: String,
        from: String,
        to: String,
    },

    #[error("scheduler worker for run {run_id} is not active")]
    WorkerNotActive { run_id: String },

    #[error("scheduler worker spawn failed for run {run_id}: {message}")]
    WorkerSpawnFailed { run_id: String, message: String },

    #[error("scheduler configuration is invalid: {message}")]
    InvalidSchedulerConfiguration { message: String },

    #[error("task prompt must be provided via --prompt or --prompt-file")]
    TaskPromptRequired,

    #[error("task prompt cannot be provided both inline and from file")]
    TaskPromptConflict,

    #[error("task {task_id} has no stored input to retry")]
    TaskRetryInputMissing { task_id: String },

    #[error("project {project} already exists")]
    ProjectAlreadyExists { project: String },

    #[error("workspace {workspace_root} maps to multiple scheduler projects: {projects:?}; use an explicit --project")]
    WorkspaceProjectAmbiguous {
        workspace_root: PathBuf,
        projects: Vec<String>,
    },

    #[error("unsupported project execution mode {mode}")]
    UnsupportedProjectExecutionMode { mode: String },

    #[error("scheduler rollout gate scheduler_v1 is disabled for {operation}; run `codex-switch scheduler enable` first")]
    SchedulerFeatureDisabled { operation: String },

    #[error("session-control runtime is unavailable: {message}")]
    RuntimeUnavailable { message: String },

    #[error("session-control state is invalid: {message}")]
    InvalidSessionControlState { message: String },

    #[error("machine-readable json failure has already been emitted")]
    JsonFailureRendered,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
