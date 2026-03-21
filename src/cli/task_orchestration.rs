use std::fs;
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand, ValueEnum};

use crate::domain::identity::IdentityId;
use crate::error::{AppError, Result};
use crate::storage::paths::{default_base_root, resolve_path, task_artifact_events_path};
use crate::task_orchestration::{
    CleanupPolicy, ProjectExecutionMode, ProjectSubmitRequest, SchedulerDaemon, SchedulerSettings,
    SchedulerStore, TaskAffinityPolicy, TaskFollowUpRequest, TaskRetryRequest, TaskRuntimeWorker,
    TaskSubmitRequest,
};

#[derive(Debug, Args)]
pub struct ProjectsCommand {
    #[command(subcommand)]
    pub command: ProjectsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ProjectsSubcommand {
    Add(AddProjectCommand),
    List(ListProjectsCommand),
    Show(ShowProjectCommand),
}

#[derive(Debug, Args)]
pub struct AddProjectCommand {
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub repo_root: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = ExecutionModeArg::CopyWorkspace)]
    pub execution_mode: ExecutionModeArg,
    #[arg(long)]
    pub model: Option<String>,
    #[arg(long = "codex-arg")]
    pub codex_args: Vec<String>,
    #[arg(long = "env")]
    pub env_allowlist: Vec<String>,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ListProjectsCommand {
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ShowProjectCommand {
    pub project: String,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct TasksCommand {
    #[command(subcommand)]
    pub command: TasksSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum TasksSubcommand {
    Submit(SubmitTaskCommand),
    FollowUp(FollowUpTaskCommand),
    List(ListTasksCommand),
    Status(TaskIdCommand),
    Show(TaskIdCommand),
    Logs(TaskIdCommand),
    Explain(TaskIdCommand),
    Cancel(TaskIdCommand),
    Retry(TaskIdCommand),
}

#[derive(Debug, Args)]
pub struct SubmitTaskCommand {
    #[arg(long)]
    pub project: String,
    #[arg(long)]
    pub title: String,
    #[arg(long)]
    pub prompt: Option<String>,
    #[arg(long)]
    pub prompt_file: Option<PathBuf>,
    #[arg(long, default_value_t = 0)]
    pub priority: i64,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    #[arg(long)]
    pub max_runtime: Option<i64>,
    #[arg(long, default_value_t = true)]
    pub queue_if_busy: bool,
    #[arg(long, default_value_t = false)]
    pub allow_oversubscribe: bool,
    #[arg(long, value_enum, default_value_t = AffinityArg::Spread)]
    pub affinity: AffinityArg,
    #[arg(long)]
    pub created_by: Option<String>,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct FollowUpTaskCommand {
    pub task_id: String,
    #[arg(long)]
    pub prompt: Option<String>,
    #[arg(long)]
    pub prompt_file: Option<PathBuf>,
    #[arg(long)]
    pub created_by: Option<String>,
    #[arg(long)]
    pub max_runtime: Option<i64>,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ListTasksCommand {
    #[arg(long)]
    pub project: Option<String>,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct TaskIdCommand {
    pub task_id: String,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SchedulerCommand {
    #[command(subcommand)]
    pub command: SchedulerSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum SchedulerSubcommand {
    Run(SchedulerRunCommand),
    Tick(SchedulerTickCommand),
    Health(SchedulerHealthCommand),
    Gc(SchedulerGcCommand),
    ResetState(SchedulerResetStateCommand),
    Enable(SchedulerToggleCommand),
    Disable(SchedulerToggleCommand),
    #[command(hide = true)]
    Worker(SchedulerWorkerCommand),
}

#[derive(Debug, Args)]
pub struct SchedulerRunCommand {
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SchedulerTickCommand {
    #[arg(long)]
    pub once: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SchedulerHealthCommand {
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SchedulerGcCommand {
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SchedulerResetStateCommand {
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SchedulerToggleCommand {
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SchedulerWorkerCommand {
    #[arg(long)]
    pub run_id: String,
    #[arg(long)]
    pub lease_owner_id: String,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ExecutionModeArg {
    GitWorktree,
    CopyWorkspace,
}

impl From<ExecutionModeArg> for ProjectExecutionMode {
    fn from(value: ExecutionModeArg) -> Self {
        match value {
            ExecutionModeArg::GitWorktree => Self::GitWorktree,
            ExecutionModeArg::CopyWorkspace => Self::CopyWorkspace,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AffinityArg {
    Spread,
    PreferSameIdentity,
    PreferProjectLocality,
}

impl From<AffinityArg> for TaskAffinityPolicy {
    fn from(value: AffinityArg) -> Self {
        match value {
            AffinityArg::Spread => Self::Spread,
            AffinityArg::PreferSameIdentity => Self::PreferSameIdentity,
            AffinityArg::PreferProjectLocality => Self::PreferProjectLocality,
        }
    }
}

pub fn run_projects(command: ProjectsCommand) -> Result<()> {
    match command.command {
        ProjectsSubcommand::Add(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let repo_root = match command.repo_root.as_deref() {
                Some(path) => resolve_path(path)?,
                None => std::env::current_dir()?,
            };
            let mut store = SchedulerStore::open(&base_root)?;
            let project = store.create_project(ProjectSubmitRequest {
                name: command.name,
                repo_root,
                execution_mode: command.execution_mode.into(),
                default_codex_args: command.codex_args,
                default_model_or_profile: command.model,
                env_allowlist: command.env_allowlist,
                cleanup_policy: CleanupPolicy::default(),
            })?;
            println!("project registered: {}", project.name);
            println!("project id: {}", project.project_id);
            println!("repo root: {}", project.repo_root.display());
            println!("execution mode: {}", project.execution_mode);
            Ok(())
        }
        ProjectsSubcommand::List(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let store = SchedulerStore::open(&base_root)?;
            let projects = store.list_projects()?;
            if projects.is_empty() {
                println!("no projects registered");
                return Ok(());
            }
            for project in projects {
                println!(
                    "{} ({}) [{}]",
                    project.name, project.project_id, project.execution_mode
                );
            }
            Ok(())
        }
        ProjectsSubcommand::Show(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let store = SchedulerStore::open(&base_root)?;
            let project = store.get_project(&command.project)?;
            println!("project: {}", project.name);
            println!("id: {}", project.project_id);
            println!("repo root: {}", project.repo_root.display());
            println!("execution mode: {}", project.execution_mode);
            println!(
                "default model/profile: {}",
                project
                    .default_model_or_profile
                    .as_deref()
                    .unwrap_or("not set")
            );
            println!(
                "default codex args: {}",
                if project.default_codex_args.is_empty() {
                    "(none)".to_string()
                } else {
                    project.default_codex_args.join(" ")
                }
            );
            println!(
                "env allowlist: {}",
                if project.env_allowlist.is_empty() {
                    "(none)".to_string()
                } else {
                    project.env_allowlist.join(", ")
                }
            );
            Ok(())
        }
    }
}

pub fn run_tasks(command: TasksCommand) -> Result<()> {
    match command.command {
        TasksSubcommand::Submit(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            require_scheduler_rollout_enabled(&base_root, "tasks submit")?;
            let prompt_text =
                resolve_prompt(command.prompt.as_deref(), command.prompt_file.as_deref())?;
            let mut store = SchedulerStore::open(&base_root)?;
            let snapshot = store.submit_task(TaskSubmitRequest {
                project: command.project,
                title: command.title,
                prompt_text,
                prompt_file_path: command
                    .prompt_file
                    .as_deref()
                    .map(resolve_path)
                    .transpose()?,
                priority: command.priority,
                labels: command.labels,
                created_by: command.created_by.unwrap_or_else(default_created_by),
                max_runtime_secs: command.max_runtime,
                queue_if_busy: command.queue_if_busy,
                allow_oversubscribe: command.allow_oversubscribe,
                affinity_policy: command.affinity.into(),
            })?;
            println!("task submitted: {}", snapshot.task.title);
            println!("task id: {}", snapshot.task.task_id);
            println!("run id: {}", snapshot.runs[0].run_id);
            Ok(())
        }
        TasksSubcommand::FollowUp(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            require_scheduler_rollout_enabled(&base_root, "tasks follow-up")?;
            let prompt_text =
                resolve_prompt(command.prompt.as_deref(), command.prompt_file.as_deref())?;
            let mut store = SchedulerStore::open(&base_root)?;
            let run = store.submit_follow_up(TaskFollowUpRequest {
                task_id: command.task_id,
                prompt_text,
                prompt_file_path: command
                    .prompt_file
                    .as_deref()
                    .map(resolve_path)
                    .transpose()?,
                created_by: command.created_by.unwrap_or_else(default_created_by),
                max_runtime_secs: command.max_runtime,
            })?;
            println!("follow-up queued");
            println!("run id: {}", run.run_id);
            Ok(())
        }
        TasksSubcommand::List(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let store = SchedulerStore::open(&base_root)?;
            let tasks = store.list_tasks(command.project.as_deref())?;
            if tasks.is_empty() {
                println!("no tasks found");
                return Ok(());
            }
            for task in tasks {
                println!(
                    "{} [{}] priority={} project={}",
                    task.task_id, task.status, task.priority, task.project_id
                );
                println!("  {}", task.title);
            }
            Ok(())
        }
        TasksSubcommand::Status(command) | TasksSubcommand::Show(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let store = SchedulerStore::open(&base_root)?;
            let lineage = store.task_lineage(&command.task_id)?;
            println!("task: {}", lineage.task.title);
            println!("task id: {}", lineage.task.task_id);
            println!("status: {}", lineage.task.status);
            println!(
                "current thread: {}",
                lineage
                    .task
                    .current_lineage_thread_id
                    .as_deref()
                    .unwrap_or("none")
            );
            println!(
                "last identity: {}",
                lineage
                    .task
                    .last_identity_id
                    .as_ref()
                    .map(IdentityId::as_str)
                    .unwrap_or("none")
            );
            for run in lineage.runs {
                println!(
                    "run {} seq={} kind={} status={}",
                    run.run_id,
                    run.sequence_no,
                    run.run_kind.as_str(),
                    run.status.as_str()
                );
            }
            Ok(())
        }
        TasksSubcommand::Logs(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let store = SchedulerStore::open(&base_root)?;
            let lineage = store.task_lineage(&command.task_id)?;
            for event in store.task_events(&command.task_id)? {
                println!(
                    "{} {} {}",
                    event.created_at, event.event_kind, event.message
                );
            }
            if let Some(run) = lineage.runs.last() {
                let artifact_path = task_artifact_events_path(
                    &base_root,
                    lineage.task.task_id.as_str(),
                    run.run_id.as_str(),
                );
                if artifact_path.exists() {
                    println!("artifact log: {}", artifact_path.display());
                    print!("{}", fs::read_to_string(artifact_path)?);
                }
            }
            Ok(())
        }
        TasksSubcommand::Explain(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let store = SchedulerStore::open(&base_root)?;
            let lineage = store.task_lineage(&command.task_id)?;
            let Some(decision) = lineage.latest_dispatch_decision else {
                println!("no dispatch decision recorded");
                return Ok(());
            };
            println!("decision id: {}", decision.decision_id);
            println!("reason: {}", decision.reason);
            println!("lineage mode: {}", decision.lineage_mode.as_str());
            for candidate in decision.candidates {
                println!(
                    "{} eligible={} occupancy={} active_count={} same_task_affinity={} same_identity_affinity={} headroom={}",
                    candidate.display_name,
                    if candidate.eligible { "yes" } else { "no" },
                    candidate.occupancy_state,
                    candidate.active_count,
                    if candidate.same_task_affinity { "yes" } else { "no" },
                    if candidate.same_identity_affinity { "yes" } else { "no" },
                    candidate
                        .remaining_headroom_percent
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "n/a".to_string()),
                );
                if let Some(reason) = candidate.rejection_reason {
                    println!("  rejection: {reason}");
                }
            }
            Ok(())
        }
        TasksSubcommand::Cancel(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let mut store = SchedulerStore::open(&base_root)?;
            let outcome = store.cancel_task(&command.task_id)?;
            let interrupted = interrupt_task_runs(&outcome);
            println!("task canceled: {}", command.task_id);
            if interrupted > 0 {
                println!("interrupted runs: {interrupted}");
            }
            Ok(())
        }
        TasksSubcommand::Retry(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            require_scheduler_rollout_enabled(&base_root, "tasks retry")?;
            let mut store = SchedulerStore::open(&base_root)?;
            let run = store.retry_task(TaskRetryRequest {
                task_id: command.task_id,
                created_by: default_created_by(),
            })?;
            println!("retry queued");
            println!("run id: {}", run.run_id);
            Ok(())
        }
    }
}

pub fn run_scheduler(command: SchedulerCommand) -> Result<()> {
    match command.command {
        SchedulerSubcommand::Run(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            SchedulerDaemon::new(&base_root, SchedulerSettings::default())?.run_loop()
        }
        SchedulerSubcommand::Tick(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let daemon = SchedulerDaemon::new(&base_root, SchedulerSettings::default())?;
            let outcomes = daemon.tick_once()?;
            let control = daemon.health()?.control;
            if !control.scheduler_v1_enabled {
                println!("scheduler_v1 disabled; no dispatches");
            } else if outcomes.is_empty() {
                println!("no dispatches");
            } else {
                for outcome in outcomes {
                    println!(
                        "dispatched run {} task {} -> {} ({})",
                        outcome.run_id,
                        outcome.task_id,
                        outcome.identity_id,
                        outcome.launch_mode.as_str()
                    );
                }
            }
            if command.once {
                return Ok(());
            }
            Ok(())
        }
        SchedulerSubcommand::Health(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let report =
                SchedulerDaemon::new(&base_root, SchedulerSettings::default())?.health()?;
            println!(
                "scheduler_v1 enabled: {}",
                if report.control.scheduler_v1_enabled {
                    "yes"
                } else {
                    "no"
                }
            );
            println!(
                "quota refresh interval secs: {}",
                report.settings.quota_refresh_interval.as_secs()
            );
            println!(
                "gc interval secs: {}",
                report.settings.gc_interval.as_secs()
            );
            println!(
                "last quota refresh at: {}",
                report
                    .control
                    .last_quota_refresh_at
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "never".to_string())
            );
            println!(
                "last quota refresh error: {}",
                report
                    .control
                    .last_quota_refresh_error
                    .as_deref()
                    .unwrap_or("none")
            );
            println!(
                "last gc at: {}",
                report
                    .control
                    .last_gc_at
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "never".to_string())
            );
            println!(
                "last gc error: {}",
                report.control.last_gc_error.as_deref().unwrap_or("none")
            );
            println!("queued runs: {}", report.snapshot.queued_runs);
            println!("active runs: {}", report.snapshot.active_runs);
            println!("stale runs: {}", report.snapshot.stale_runs);
            println!("active identities: {}", report.snapshot.active_identities);
            println!("free identities: {}", report.snapshot.free_identities);
            for lease in report.active_leases {
                println!(
                    "lease identity={} run={} expires_at={}",
                    lease.identity_id, lease.run_id, lease.expires_at
                );
            }
            Ok(())
        }
        SchedulerSubcommand::Gc(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let removed = SchedulerDaemon::new(&base_root, SchedulerSettings::default())?.gc()?;
            if removed.is_empty() {
                println!("no worktrees removed");
            } else {
                for path in removed {
                    println!("removed {}", path.display());
                }
            }
            Ok(())
        }
        SchedulerSubcommand::ResetState(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            SchedulerStore::reset_state(&base_root)?;
            let _ = SchedulerStore::open(&base_root)?;
            println!("scheduler state reset: {}", base_root.display());
            Ok(())
        }
        SchedulerSubcommand::Enable(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let mut store = SchedulerStore::open(&base_root)?;
            let control = store.set_scheduler_feature_enabled(true)?;
            println!(
                "scheduler_v1 enabled: {}",
                if control.scheduler_v1_enabled {
                    "yes"
                } else {
                    "no"
                }
            );
            Ok(())
        }
        SchedulerSubcommand::Disable(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let mut store = SchedulerStore::open(&base_root)?;
            let control = store.set_scheduler_feature_enabled(false)?;
            println!(
                "scheduler_v1 enabled: {}",
                if control.scheduler_v1_enabled {
                    "yes"
                } else {
                    "no"
                }
            );
            Ok(())
        }
        SchedulerSubcommand::Worker(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            TaskRuntimeWorker::new(&base_root, SchedulerSettings::default())
                .run(&command.run_id, &command.lease_owner_id)
        }
    }
}

fn interrupt_task_runs(outcome: &crate::task_orchestration::store::CancelTaskOutcome) -> usize {
    outcome
        .interrupted_runs
        .iter()
        .filter_map(|run| run.worker_pid)
        .filter(|pid| terminate_process_group(*pid))
        .count()
}

fn terminate_process_group(pid: u32) -> bool {
    #[cfg(unix)]
    unsafe {
        let process_group = -(pid as i32);
        if libc::kill(process_group, libc::SIGTERM) != 0 {
            return false;
        }
        for _ in 0..10 {
            if libc::kill(pid as i32, 0) != 0 {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        libc::kill(process_group, libc::SIGKILL) == 0
    }

    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

fn resolve_prompt(inline: Option<&str>, file: Option<&Path>) -> Result<String> {
    match (inline, file) {
        (Some(_), Some(_)) => Err(AppError::TaskPromptConflict),
        (Some(prompt), None) => Ok(prompt.to_string()),
        (None, Some(path)) => Ok(fs::read_to_string(path)?),
        (None, None) => Err(AppError::TaskPromptRequired),
    }
}

fn resolve_base_root(path: Option<&Path>) -> Result<PathBuf> {
    match path {
        Some(path) => resolve_path(path),
        None => default_base_root(),
    }
}

fn default_created_by() -> String {
    std::env::var("USER")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "operator".to_string())
}

fn require_scheduler_rollout_enabled(base_root: &Path, operation: &str) -> Result<()> {
    let store = SchedulerStore::open(base_root)?;
    if store.scheduler_control()?.scheduler_v1_enabled {
        return Ok(());
    }
    Err(AppError::SchedulerFeatureDisabled {
        operation: operation.to_string(),
    })
}
