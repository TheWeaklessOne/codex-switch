mod session_control;
mod task_orchestration;

use std::ffi::OsString;
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{Local, TimeZone};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde_json::Value;

use crate::automatic_handoff::{
    AutomaticContinueThreadRequest, AutomaticHandoffService, AutomaticHandoffStores,
    SelectionContextStores,
};
use crate::automatic_selection::AutomaticSelectionService;
use crate::bootstrap::BootstrapIdentityRequest;
use crate::codex_rpc::{AppServerCommand, CodexAppServerVerifier};
use crate::continuation::{ContinueService, ContinueThreadRequest};
use crate::domain::health::{IdentityHealthRecord, IdentityHealthState};
use crate::domain::identity::{
    current_timestamp, AuthMode, CodexIdentity, IdentityId, IdentityKind, WorkspaceForceProbeStatus,
};
use crate::domain::policy::IdentitySelectionPolicy;
use crate::domain::quota::IdentityQuotaStatus;
use crate::domain::selection::SelectionMode;
use crate::domain::thread::{ThreadLeaseRecord, ThreadSnapshot, TrackedTurnState};
use crate::domain::verification::{IdentityVerification, RateLimitSnapshot, RateLimitWindow};
use crate::error::{AppError, Result};
use crate::exec_failover::{
    ExecFailoverRequest, ExecFailoverResult, ExecFailoverService, ExecFailoverStores,
};
use crate::handoff::{HandoffAcceptance, HandoffPreparation, HandoffService};
use crate::identity_cleanup::{
    auto_remove_deactivated_workspace_identities, AutoRemovalNotice, ManagedIdentityRemovalService,
    WorkspaceDeactivationSummary,
};
use crate::identity_health::IdentityHealthService;
use crate::identity_naming::next_auto_display_name;
use crate::identity_registry::IdentityRegistryService;
use crate::identity_selection::{CurrentIdentitySelection, IdentitySelectionService};
use crate::identity_selector::{IdentityEvaluation, IdentitySelector, SelectedIdentity};
use crate::launcher::CodexLauncher;
use crate::quota_status::{
    classify_identity_refresh_error, IdentityRefreshErrorKind, IdentityStatusReport,
    QuotaStatusService,
};
use crate::selection_policy::{SelectionPolicyService, UpdateSelectionPolicyRequest};
use crate::storage::checkpoint_store::JsonTaskCheckpointStore;
use crate::storage::health_store::JsonIdentityHealthStore;
use crate::storage::paths::{default_base_root, default_codex_home, resolve_path};
use crate::storage::policy_store::JsonSelectionPolicyStore;
use crate::storage::quota_store::JsonQuotaStore;
use crate::storage::registry_store::JsonRegistryStore;
use crate::storage::selection_event_store::JsonSelectionEventStore;
use crate::storage::selection_store::JsonSelectionStore;
use crate::workspace_switching::{
    inject_auth_into_home, validate_workspace_force_identity, UpdateWorkspaceForceProbeRequest,
    WorkspaceForceProbeOutcome, WorkspaceSwitchingService,
};
use session_control::{
    run_handoffs, run_sessions, run_turns, HandoffsCommand, SessionsCommand, TurnsCommand,
};
use task_orchestration::{
    run_jobs, run_projects, run_scheduler, run_tasks, JobsCommand, ProjectsCommand,
    SchedulerCommand, TasksCommand,
};

pub fn run<I, T>(arguments: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::parse_from(arguments);
    match cli.command {
        Command::Add(command) => run_quick_add(command),
        Command::Identities(command) => run_identities(command),
        Command::Policy(command) => run_policy(command),
        Command::Status(command) => run_status(command),
        Command::Accounts(command) => run_accounts(command),
        Command::Select(command) => run_select(command),
        Command::Inject(command) => run_inject(command),
        Command::Exec(command) => run_exec(command),
        Command::AppServer(command) => run_app_server(command),
        Command::Continue(command) => run_continue(command),
        Command::Threads(command) => run_threads(command),
        Command::Projects(command) => run_projects(command),
        Command::Jobs(command) => run_jobs(command),
        Command::Tasks(command) => run_tasks(command),
        Command::Scheduler(command) => run_scheduler(command),
        Command::Sessions(command) => run_sessions(command),
        Command::Turns(command) => run_turns(command),
        Command::Handoffs(command) => run_handoffs(command),
    }
}

#[derive(Debug, Parser)]
#[command(name = "codex-switch", about = "Manage isolated Codex identity homes.")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Add(QuickAddCommand),
    Identities(IdentitiesCommand),
    Policy(PolicyCommand),
    Status(StatusCommand),
    Accounts(AccountsCommand),
    Select(SelectCommand),
    Inject(InjectCommand),
    Exec(ExecCommand),
    AppServer(AppServerWrapperCommand),
    Continue(ContinueCommand),
    Threads(ThreadsCommand),
    Projects(ProjectsCommand),
    Jobs(JobsCommand),
    Tasks(TasksCommand),
    Scheduler(SchedulerCommand),
    Sessions(SessionsCommand),
    Turns(TurnsCommand),
    Handoffs(HandoffsCommand),
}

#[derive(Debug, Args)]
struct QuickAddCommand {
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    home: Option<PathBuf>,
    #[arg(long)]
    overwrite_config: bool,
    #[arg(long)]
    workspace_id: Option<String>,
    #[arg(long)]
    no_verify: bool,
}

#[derive(Debug, Args)]
struct IdentitiesCommand {
    #[command(subcommand)]
    command: IdentitiesSubcommand,
}

#[derive(Debug, Subcommand)]
enum IdentitiesSubcommand {
    Add(AddIdentityCommand),
    List(ListIdentitiesCommand),
    Login(LoginIdentityCommand),
    Verify(VerifyIdentityCommand),
    Remove(RemoveIdentityCommand),
    Health(IdentityHealthCommand),
    Disable(ToggleIdentityHealthCommand),
    Enable(ToggleIdentityHealthCommand),
    WorkspaceForce(WorkspaceForceCommand),
}

#[derive(Debug, Args)]
struct PolicyCommand {
    #[command(subcommand)]
    command: PolicySubcommand,
}

#[derive(Debug, Subcommand)]
enum PolicySubcommand {
    Show(ShowPolicyCommand),
    Set(SetPolicyCommand),
}

#[derive(Debug, Args)]
struct ShowPolicyCommand {
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct SetPolicyCommand {
    #[arg(long)]
    warning: Option<i32>,
    #[arg(long)]
    avoid: Option<i32>,
    #[arg(long = "hard-stop")]
    hard_stop: Option<i32>,
    #[arg(long = "rate-limit-cooldown")]
    rate_limit_cooldown_secs: Option<i64>,
    #[arg(long = "auth-failure-cooldown")]
    auth_failure_cooldown_secs: Option<i64>,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct AddIdentityCommand {
    #[command(subcommand)]
    command: AddIdentitySubcommand,
}

#[derive(Debug, Subcommand)]
enum AddIdentitySubcommand {
    Chatgpt(AddSharedIdentityArgs),
    Api(AddApiIdentityArgs),
}

#[derive(Debug, Args)]
struct AddSharedIdentityArgs {
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    home: Option<PathBuf>,
    #[arg(long)]
    import_auth_from_home: Option<PathBuf>,
    #[arg(long)]
    overwrite_config: bool,
    #[arg(long)]
    workspace_id: Option<String>,
    #[arg(long)]
    login: bool,
    #[arg(long)]
    no_verify: bool,
}

#[derive(Debug, Args)]
struct AddApiIdentityArgs {
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    home: Option<PathBuf>,
    #[arg(long)]
    env_var: Option<String>,
    #[arg(long)]
    import_auth_from_home: Option<PathBuf>,
    #[arg(long)]
    overwrite_config: bool,
    #[arg(long)]
    login: bool,
    #[arg(long)]
    no_verify: bool,
}

#[derive(Debug, Args)]
struct ListIdentitiesCommand {
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct VerifyIdentityCommand {
    name: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct RemoveIdentityCommand {
    name: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct IdentityHealthCommand {
    #[command(subcommand)]
    command: IdentityHealthSubcommand,
}

#[derive(Debug, Subcommand)]
enum IdentityHealthSubcommand {
    Show(ShowIdentityHealthCommand),
    Clear(ClearIdentityHealthCommand),
}

#[derive(Debug, Args)]
struct ShowIdentityHealthCommand {
    name: Option<String>,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ClearIdentityHealthCommand {
    name: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ToggleIdentityHealthCommand {
    name: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct LoginIdentityCommand {
    name: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    no_verify: bool,
}

#[derive(Debug, Args)]
struct WorkspaceForceCommand {
    #[command(subcommand)]
    command: WorkspaceForceSubcommand,
}

#[derive(Debug, Subcommand)]
enum WorkspaceForceSubcommand {
    Show(ShowWorkspaceForceCommand),
    Set(SetWorkspaceForceCommand),
    Probe(ProbeWorkspaceForceCommand),
}

#[derive(Debug, Args)]
struct ShowWorkspaceForceCommand {
    name: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct SetWorkspaceForceCommand {
    name: String,
    #[arg(long, value_enum)]
    status: WorkspaceForceProbeStatusArg,
    #[arg(long)]
    notes: Option<String>,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ProbeWorkspaceForceCommand {
    name: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WorkspaceForceProbeStatusArg {
    Pending,
    Passed,
    Failed,
}

impl From<WorkspaceForceProbeStatusArg> for WorkspaceForceProbeStatus {
    fn from(value: WorkspaceForceProbeStatusArg) -> Self {
        match value {
            WorkspaceForceProbeStatusArg::Pending => Self::Pending,
            WorkspaceForceProbeStatusArg::Passed => Self::Passed,
            WorkspaceForceProbeStatusArg::Failed => Self::Failed,
        }
    }
}

#[derive(Debug, Args)]
struct StatusCommand {
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    cached: bool,
}

#[derive(Debug, Args)]
struct AccountsCommand {
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    cached: bool,
}

#[derive(Debug, Args)]
struct SelectCommand {
    identity: Option<String>,
    #[arg(long)]
    auto: bool,
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    cached: bool,
}

#[derive(Debug, Args)]
struct InjectCommand {
    #[arg(long)]
    identity: Option<String>,
    #[arg(long)]
    auto: bool,
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    cached: bool,
    #[arg(long)]
    target: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ExecCommand {
    #[arg(long)]
    identity: Option<String>,
    #[arg(long)]
    auto: bool,
    #[arg(long)]
    auto_failover: bool,
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    cached: bool,
    #[arg(last = true)]
    args: Vec<OsString>,
}

#[derive(Debug, Args)]
struct AppServerWrapperCommand {
    #[arg(long)]
    identity: Option<String>,
    #[arg(long)]
    auto: bool,
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    cached: bool,
    #[arg(last = true)]
    args: Vec<OsString>,
}

#[derive(Debug, Args)]
struct ContinueCommand {
    #[arg(long)]
    thread: String,
    #[arg(long)]
    to: Option<String>,
    #[arg(long)]
    auto: bool,
    #[arg(long)]
    from: Option<String>,
    #[arg(long, default_value = "manual_switch")]
    reason: String,
    #[arg(long)]
    no_launch: bool,
    #[arg(long)]
    base_root: Option<PathBuf>,
    #[arg(long)]
    cached: bool,
    #[arg(last = true)]
    resume_args: Vec<OsString>,
}

#[derive(Debug, Args)]
struct ThreadsCommand {
    #[command(subcommand)]
    command: ThreadsSubcommand,
}

#[derive(Debug, Subcommand)]
enum ThreadsSubcommand {
    Inspect(InspectThreadCommand),
    Lease(ThreadLeaseCommand),
    Handoff(ThreadHandoffCommand),
    State(ThreadStateCommand),
}

#[derive(Debug, Args)]
struct InspectThreadCommand {
    thread: String,
    #[arg(long)]
    identity: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ThreadLeaseCommand {
    #[command(subcommand)]
    command: ThreadLeaseSubcommand,
}

#[derive(Debug, Subcommand)]
enum ThreadLeaseSubcommand {
    Acquire(LeaseAcquireCommand),
    Heartbeat(LeaseHeartbeatCommand),
    Show(LeaseShowCommand),
}

#[derive(Debug, Args)]
struct LeaseAcquireCommand {
    thread: String,
    #[arg(long)]
    identity: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct LeaseHeartbeatCommand {
    thread: String,
    #[arg(long)]
    identity: String,
    #[arg(long)]
    lease_token: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct LeaseShowCommand {
    thread: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ThreadHandoffCommand {
    #[command(subcommand)]
    command: ThreadHandoffSubcommand,
}

#[derive(Debug, Subcommand)]
enum ThreadHandoffSubcommand {
    Prepare(HandoffPrepareCommand),
    Accept(HandoffAcceptCommand),
    Confirm(HandoffConfirmCommand),
}

#[derive(Debug, Args)]
struct HandoffPrepareCommand {
    thread: String,
    #[arg(long)]
    from: String,
    #[arg(long)]
    to: String,
    #[arg(long)]
    lease_token: String,
    #[arg(long)]
    reason: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct HandoffAcceptCommand {
    thread: String,
    #[arg(long)]
    to: String,
    #[arg(long)]
    lease_token: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct HandoffConfirmCommand {
    thread: String,
    #[arg(long)]
    to: String,
    #[arg(long)]
    lease_token: String,
    #[arg(long)]
    observed_turn_id: Option<String>,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ThreadStateCommand {
    thread: String,
    #[arg(long)]
    base_root: Option<PathBuf>,
}

fn run_identities(command: IdentitiesCommand) -> Result<()> {
    match command.command {
        IdentitiesSubcommand::Add(command) => run_add_identity(command),
        IdentitiesSubcommand::List(command) => run_list_identities(command),
        IdentitiesSubcommand::Login(command) => run_login_identity(command),
        IdentitiesSubcommand::Verify(command) => run_verify_identity(command),
        IdentitiesSubcommand::Remove(command) => run_remove_identity(command),
        IdentitiesSubcommand::Health(command) => run_identity_health(command),
        IdentitiesSubcommand::Disable(command) => run_disable_identity(command),
        IdentitiesSubcommand::Enable(command) => run_enable_identity(command),
        IdentitiesSubcommand::WorkspaceForce(command) => run_workspace_force(command),
    }
}

fn run_policy(command: PolicyCommand) -> Result<()> {
    match command.command {
        PolicySubcommand::Show(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let policy = build_selection_policy_service(&base_root).load_policy()?;
            print_selection_policy(&policy);
            Ok(())
        }
        PolicySubcommand::Set(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let policy = build_selection_policy_service(&base_root).update_policy(
                UpdateSelectionPolicyRequest {
                    warning_used_percent: command.warning,
                    avoid_used_percent: command.avoid,
                    hard_stop_used_percent: command.hard_stop,
                    rate_limit_cooldown_secs: command.rate_limit_cooldown_secs,
                    auth_failure_cooldown_secs: command.auth_failure_cooldown_secs,
                },
            )?;
            println!("selection policy updated");
            print_selection_policy(&policy);
            Ok(())
        }
    }
}

fn run_add_identity(command: AddIdentityCommand) -> Result<()> {
    match command.command {
        AddIdentitySubcommand::Chatgpt(arguments) => run_add_chatgpt_identity(arguments, false),
        AddIdentitySubcommand::Api(arguments) => {
            validate_add_login_flags(arguments.login, arguments.no_verify)?;
            let base_root = resolve_base_root(arguments.base_root.as_deref())?;
            let service = IdentityRegistryService::new(JsonRegistryStore::new(&base_root));
            let display_name = resolve_add_identity_name(&service, arguments.name)?;
            let result = service.register_identity(BootstrapIdentityRequest {
                display_name,
                base_root: base_root.clone(),
                auth_mode: AuthMode::Apikey,
                home_override: arguments.home,
                import_auth_from_home: arguments.import_auth_from_home,
                overwrite_config: arguments.overwrite_config,
                api_key_env_var: arguments.env_var,
                forced_chatgpt_workspace_id: None,
            })?;
            print_registered_identity(&base_root, &result.identity, result.next_login_command);
            if arguments.login {
                login_identity(&base_root, &result.identity, arguments.no_verify)?;
            }
            Ok(())
        }
    }
}

fn run_quick_add(command: QuickAddCommand) -> Result<()> {
    run_add_chatgpt_identity(
        AddSharedIdentityArgs {
            name: None,
            base_root: command.base_root,
            home: command.home,
            import_auth_from_home: None,
            overwrite_config: command.overwrite_config,
            workspace_id: command.workspace_id,
            login: true,
            no_verify: command.no_verify,
        },
        true,
    )
}

fn run_add_chatgpt_identity(arguments: AddSharedIdentityArgs, force_login: bool) -> Result<()> {
    let should_login = force_login || arguments.login;
    validate_add_login_flags(should_login, arguments.no_verify)?;
    let base_root = resolve_base_root(arguments.base_root.as_deref())?;
    let service = IdentityRegistryService::new(JsonRegistryStore::new(&base_root));
    let display_name = resolve_add_identity_name(&service, arguments.name)?;
    let result = service.register_identity(BootstrapIdentityRequest {
        display_name,
        base_root: base_root.clone(),
        auth_mode: AuthMode::Chatgpt,
        home_override: arguments.home,
        import_auth_from_home: arguments.import_auth_from_home,
        overwrite_config: arguments.overwrite_config,
        api_key_env_var: None,
        forced_chatgpt_workspace_id: arguments.workspace_id,
    })?;
    print_registered_identity(&base_root, &result.identity, result.next_login_command);
    if should_login {
        login_identity(&base_root, &result.identity, arguments.no_verify)?;
    }
    Ok(())
}

fn run_list_identities(command: ListIdentitiesCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let service = IdentityRegistryService::new(JsonRegistryStore::new(&base_root));
    let identities = service.list_identities()?;
    if identities.is_empty() {
        println!("no identities registered");
        return Ok(());
    }

    for identity in identities {
        print_identity(identity);
    }
    Ok(())
}

fn run_login_identity(command: LoginIdentityCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let identity = build_selection_service(&base_root).resolve_by_name(&command.name)?;
    login_identity(&base_root, &identity, command.no_verify)
}

fn run_verify_identity(command: VerifyIdentityCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    verify_identity_by_name(&base_root, &command.name).map(|_| ())
}

fn run_remove_identity(command: RemoveIdentityCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let removal_service = ManagedIdentityRemovalService::new(
        JsonRegistryStore::new(&base_root),
        JsonQuotaStore::new(&base_root),
        JsonIdentityHealthStore::new(&base_root),
        JsonSelectionStore::new(&base_root),
    );
    let outcome = removal_service.remove_identity_by_name(&command.name)?;
    println!(
        "removed {} ({})",
        outcome.identity.display_name, outcome.identity.id
    );
    if outcome.home_removed {
        println!("home removed: {}", outcome.identity.codex_home.display());
    } else {
        println!(
            "home already absent: {}",
            outcome.identity.codex_home.display()
        );
    }
    if outcome.selection_cleared {
        println!("selection cleared: yes");
    }
    Ok(())
}

fn run_identity_health(command: IdentityHealthCommand) -> Result<()> {
    match command.command {
        IdentityHealthSubcommand::Show(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_identity_health_service(&base_root);
            let now = current_time_for_selector()?;
            if let Some(name) = command.name.as_deref() {
                let identity = build_selection_service(&base_root).resolve_by_name(name)?;
                let state = service.load_record()?.identities.get(&identity.id).cloned();
                print_identity_health_snapshot(&identity, state.as_ref(), now);
                return Ok(());
            }

            let identities = IdentityRegistryService::new(JsonRegistryStore::new(&base_root))
                .list_identities()?;
            let record = service.load_record()?;
            if identities.is_empty() {
                println!("no identities registered");
                return Ok(());
            }
            for identity in identities {
                let state = record.identities.get(&identity.id);
                print_identity_health_snapshot(&identity, state, now);
            }
            Ok(())
        }
        IdentityHealthSubcommand::Clear(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let identity = build_selection_service(&base_root).resolve_by_name(&command.name)?;
            let state = build_identity_health_service(&base_root).clear_identity(&command.name)?;
            println!("identity health cleared");
            print_identity_health_snapshot(&identity, Some(&state), current_time_for_selector()?);
            Ok(())
        }
    }
}

fn run_disable_identity(command: ToggleIdentityHealthCommand) -> Result<()> {
    run_toggle_identity_health(command, true)
}

fn run_enable_identity(command: ToggleIdentityHealthCommand) -> Result<()> {
    run_toggle_identity_health(command, false)
}

fn run_toggle_identity_health(
    command: ToggleIdentityHealthCommand,
    manually_disabled: bool,
) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let identity = build_selection_service(&base_root).resolve_by_name(&command.name)?;
    let state = build_identity_health_service(&base_root)
        .set_manually_disabled(&command.name, manually_disabled)?;
    if manually_disabled {
        println!("identity disabled for automatic selection");
    } else {
        println!("identity re-enabled for automatic selection");
    }
    print_identity_health_snapshot(&identity, Some(&state), current_time_for_selector()?);
    Ok(())
}

fn run_workspace_force(command: WorkspaceForceCommand) -> Result<()> {
    match command.command {
        WorkspaceForceSubcommand::Show(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_workspace_switching_service(&base_root);
            let identity = service.inspect_identity(&command.name)?;
            validate_workspace_force_identity(&identity)?;
            print_workspace_force_details(&identity);
            Ok(())
        }
        WorkspaceForceSubcommand::Set(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_workspace_switching_service(&base_root);
            let identity = service.update_probe(UpdateWorkspaceForceProbeRequest {
                identity_name: command.name,
                status: command.status.into(),
                notes: command.notes,
            })?;
            println!("workspace force probe updated");
            print_workspace_force_details(&identity);
            Ok(())
        }
        WorkspaceForceSubcommand::Probe(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_workspace_switching_service(&base_root);
            let verifier = CodexAppServerVerifier::default();
            let outcome = service.probe_identity(&command.name, &verifier)?;
            println!("workspace force probe completed");
            print_workspace_force_probe_outcome(&outcome);
            Ok(())
        }
    }
}

fn run_status(command: StatusCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let loaded = load_identity_reports(&base_root, command.cached)?;
    print_auto_removal_notices(&loaded.auto_removal_notices);
    let reports = loaded.reports;

    let selection_service = build_selection_service(&base_root);
    match selection_service.current()? {
        Some(current) => print_selection_summary(&current),
        None => println!("current selection: none"),
    }

    let (selector, health) = load_selector_context(&base_root)?;

    if reports.is_empty() {
        println!("no identities registered");
        return Ok(());
    }

    for report in reports {
        let evaluation = selector.evaluate(
            &report.identity,
            report.quota_status.as_ref(),
            health.identities.get(&report.identity.id),
        );
        print_identity_report(
            report,
            evaluation,
            IdentityReportFormat::Status,
            selector.evaluation_time(),
        );
    }
    Ok(())
}

fn run_accounts(command: AccountsCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let loaded = load_identity_reports(&base_root, command.cached)?;
    print_auto_removal_notices(&loaded.auto_removal_notices);
    let reports = loaded.reports;
    let (selector, health) = load_selector_context(&base_root)?;

    if reports.is_empty() {
        println!("no identities registered");
        return Ok(());
    }

    let color_output = stdout_supports_color();
    let ordered = order_account_reports(&selector, &health, reports, command.cached);
    let mut default_codex_identity_ids =
        detect_default_codex_identity_ids(ordered.iter().map(|report| &report.identity));
    if default_codex_identity_ids.is_empty() && !command.cached {
        default_codex_identity_ids = detect_default_codex_identity_ids_from_runtime(&ordered);
    }
    let report_count = ordered.len();
    for (index, report) in ordered.into_iter().enumerate() {
        print_account_overview(
            &report.identity,
            report.quota_status.as_ref(),
            report.refresh_error_kind.as_ref(),
            report.refresh_error.as_deref(),
            default_codex_identity_ids.contains(&report.identity.id),
            color_output,
        );
        if index + 1 < report_count {
            println!();
        }
    }
    Ok(())
}

fn run_select(command: SelectCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let selection_service = build_selection_service(&base_root);

    if command.auto && command.identity.is_some() {
        return Err(AppError::ConflictingIdentityResolution);
    }

    if let Some(identity_name) = command.identity.as_deref() {
        let current =
            selection_service.select_manual(identity_name, Some("selected explicitly"))?;
        print_current_selection(&current);
        return Ok(());
    }

    if command.auto {
        let result = build_automatic_selection_service(&base_root)
            .select_for_new_session(command.cached, "selected automatically from quota state")?;
        print_auto_removal_notices(&result.auto_removal_notices);
        print_selected_identity(&result.selected);
        let current = result.current;
        println!("selection mode: {}", current.selection.mode);
        println!("decision log: {}", result.decision_log.path.display());
        return Ok(());
    }

    match selection_service.current()? {
        Some(current) => print_current_selection(&current),
        None => println!("no identity selected"),
    }
    Ok(())
}

fn run_inject(command: InjectCommand) -> Result<()> {
    if !command.auto && command.identity.is_none() {
        return Err(AppError::InjectIdentityRequired);
    }

    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let identity = resolve_runtime_identity(
        &base_root,
        command.identity.as_deref(),
        command.auto,
        command.cached,
    )?;
    let target_home = match command.target {
        Some(path) => resolve_path(&path)?,
        None => default_codex_home()?,
    };

    if matches!(identity.auth_mode, AuthMode::Apikey) {
        let env_var = identity
            .api_key_env_var
            .as_deref()
            .unwrap_or("OPENAI_API_KEY");
        eprintln!(
            "warning: {} uses API-key auth; the environment variable {} must still be set at launch",
            identity.display_name, env_var
        );
    }

    inject_auth_into_home(&identity, &target_home)?;
    println!(
        "injected auth from {} into {}",
        identity.display_name,
        target_home.display()
    );
    println!("id: {}", identity.id);
    println!("auth mode: {}", identity.auth_mode.as_str());
    if let Some(email) = identity.email.as_deref() {
        println!("email: {}", email);
    }
    println!("target: {}", target_home.display());
    Ok(())
}

fn run_exec(command: ExecCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let auto_resolution =
        uses_automatic_exec_resolution(&base_root, command.identity.as_deref(), command.auto)?;
    if command.auto_failover && auto_resolution {
        let result = build_exec_failover_service(&base_root).launch(ExecFailoverRequest {
            cached: command.cached,
            reason: "selected automatically with exec failover".to_string(),
            args: command.args,
        })?;
        print_exec_failover_result(&result);
        if result.no_eligible_identity() {
            return Err(AppError::NoSelectableIdentity);
        }
        return Ok(());
    }

    let identity = resolve_runtime_identity(
        &base_root,
        command.identity.as_deref(),
        command.auto,
        command.cached,
    )?;
    let launcher = CodexLauncher;
    let _ = launcher.launch_codex(&identity, &command.args)?;
    Ok(())
}

fn run_app_server(command: AppServerWrapperCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let identity = resolve_runtime_identity(
        &base_root,
        command.identity.as_deref(),
        command.auto,
        command.cached,
    )?;
    let launcher = CodexLauncher;
    let _ = launcher.launch_app_server(&identity, &command.args)?;
    Ok(())
}

fn run_continue(command: ContinueCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    if command.auto && command.to.is_some() {
        return Err(AppError::ConflictingIdentityResolution);
    }

    if command.auto {
        let service = build_automatic_handoff_service(&base_root);
        let result = service.continue_thread(AutomaticContinueThreadRequest {
            thread_id: command.thread,
            from_identity_name: command.from,
            reason: command.reason,
            cached: command.cached,
            launch_after_switch: !command.no_launch,
            extra_resume_args: command.resume_args,
        })?;
        print_automatic_continue_result(&result);
        return Ok(());
    }

    let to_identity_name = command.to.ok_or(AppError::ContinueTargetRequired)?;
    let service = build_continue_service(&base_root);
    let result = service.continue_thread(ContinueThreadRequest {
        thread_id: command.thread,
        from_identity_name: command.from,
        to_identity_name,
        reason: command.reason,
        target_selection_mode: SelectionMode::Manual,
        selection_reason: Some("manual switch and continue".to_string()),
        launch_after_switch: !command.no_launch,
        extra_resume_args: command.resume_args,
    })?;
    print_continue_result(&result);
    Ok(())
}

fn run_threads(command: ThreadsCommand) -> Result<()> {
    match command.command {
        ThreadsSubcommand::Inspect(command) => run_thread_inspect(command),
        ThreadsSubcommand::Lease(command) => run_thread_lease(command),
        ThreadsSubcommand::Handoff(command) => run_thread_handoff(command),
        ThreadsSubcommand::State(command) => run_thread_state(command),
    }
}

fn run_thread_inspect(command: InspectThreadCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let service = build_handoff_service(&base_root);
    let snapshot = service.inspect_thread(&command.identity, &command.thread)?;
    print_thread_snapshot(&snapshot);
    Ok(())
}

fn run_thread_lease(command: ThreadLeaseCommand) -> Result<()> {
    match command.command {
        ThreadLeaseSubcommand::Acquire(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_handoff_service(&base_root);
            let lease = service.acquire_lease(&command.identity, &command.thread)?;
            print_thread_lease(&lease);
            Ok(())
        }
        ThreadLeaseSubcommand::Heartbeat(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_handoff_service(&base_root);
            let lease = service.heartbeat_lease(
                &command.identity,
                &command.thread,
                &command.lease_token,
            )?;
            print_thread_lease(&lease);
            Ok(())
        }
        ThreadLeaseSubcommand::Show(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_handoff_service(&base_root);
            match service.read_lease(&command.thread)? {
                Some(lease) => print_thread_lease(&lease),
                None => println!("no lease record for {}", command.thread),
            }
            Ok(())
        }
    }
}

fn run_thread_handoff(command: ThreadHandoffCommand) -> Result<()> {
    match command.command {
        ThreadHandoffSubcommand::Prepare(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_handoff_service(&base_root);
            let preparation = service.prepare_handoff(
                &command.thread,
                &command.from,
                &command.to,
                &command.lease_token,
                &command.reason,
            )?;
            print_handoff_preparation(&preparation);
            Ok(())
        }
        ThreadHandoffSubcommand::Accept(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_handoff_service(&base_root);
            let acceptance =
                service.accept_handoff(&command.thread, &command.to, &command.lease_token)?;
            print_handoff_acceptance(&acceptance);
            Ok(())
        }
        ThreadHandoffSubcommand::Confirm(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = build_handoff_service(&base_root);
            let confirmation = service.confirm_handoff(
                &command.thread,
                &command.to,
                &command.lease_token,
                command.observed_turn_id.as_deref(),
            )?;
            println!("handoff confirmed");
            print_thread_snapshot(&confirmation.snapshot);
            if let Some(turn_id) = confirmation.matched_turn_id.as_deref() {
                println!("matched turn id: {}", turn_id);
            }
            Ok(())
        }
    }
}

fn run_thread_state(command: ThreadStateCommand) -> Result<()> {
    let base_root = resolve_base_root(command.base_root.as_deref())?;
    let service = build_handoff_service(&base_root);
    match service.tracked_state(&command.thread)? {
        Some(state) => print_tracked_turn_state(&state),
        None => println!("no tracked thread state for {}", command.thread),
    }
    Ok(())
}

fn resolve_base_root(path: Option<&Path>) -> Result<PathBuf> {
    match path {
        Some(path) => resolve_path(path),
        None => default_base_root(),
    }
}

fn resolve_runtime_identity(
    base_root: &Path,
    explicit_identity_name: Option<&str>,
    auto: bool,
    cached: bool,
) -> Result<CodexIdentity> {
    if auto && explicit_identity_name.is_some() {
        return Err(AppError::ConflictingIdentityResolution);
    }

    let selection_service = build_selection_service(base_root);
    if let Some(identity_name) = explicit_identity_name {
        return selection_service.resolve_by_name(identity_name);
    }

    if auto {
        let result = build_automatic_selection_service(base_root)
            .select_for_new_session(cached, "selected automatically for launch")?;
        print_auto_removal_notices(&result.auto_removal_notices);
        return Ok(result.selected.identity);
    }

    match selection_service.current()? {
        Some(current) if current.selection.mode == SelectionMode::Manual => Ok(current.identity),
        _ => {
            let result = build_automatic_selection_service(base_root)
                .select_for_new_session(cached, "selected automatically for launch")?;
            print_auto_removal_notices(&result.auto_removal_notices);
            Ok(result.selected.identity)
        }
    }
}

fn uses_automatic_exec_resolution(
    base_root: &Path,
    explicit_identity_name: Option<&str>,
    auto: bool,
) -> Result<bool> {
    if explicit_identity_name.is_some() {
        return Ok(false);
    }

    if auto {
        return Ok(true);
    }

    let selection_service = build_selection_service(base_root);
    Ok(!matches!(
        selection_service.current()?,
        Some(current) if current.selection.mode == SelectionMode::Manual
    ))
}

fn build_handoff_service(
    base_root: &Path,
) -> HandoffService<JsonRegistryStore, CodexAppServerVerifier> {
    HandoffService::new(
        base_root,
        JsonRegistryStore::new(base_root),
        CodexAppServerVerifier::default(),
    )
}

fn build_automatic_selection_service(
    base_root: &Path,
) -> AutomaticSelectionService<
    JsonRegistryStore,
    JsonQuotaStore,
    JsonSelectionStore,
    JsonSelectionEventStore,
    JsonSelectionPolicyStore,
    JsonIdentityHealthStore,
    CodexAppServerVerifier,
> {
    AutomaticSelectionService::new(
        JsonRegistryStore::new(base_root),
        JsonQuotaStore::new(base_root),
        JsonSelectionStore::new(base_root),
        JsonSelectionEventStore::new(base_root),
        JsonSelectionPolicyStore::new(base_root),
        JsonIdentityHealthStore::new(base_root),
        CodexAppServerVerifier::default(),
    )
}

fn build_exec_failover_service(
    base_root: &Path,
) -> ExecFailoverService<
    JsonRegistryStore,
    JsonQuotaStore,
    JsonSelectionStore,
    JsonSelectionEventStore,
    JsonSelectionPolicyStore,
    JsonIdentityHealthStore,
    CodexAppServerVerifier,
> {
    ExecFailoverService::new(
        base_root,
        ExecFailoverStores {
            registry_store: JsonRegistryStore::new(base_root),
            quota_store: JsonQuotaStore::new(base_root),
            selection_store: JsonSelectionStore::new(base_root),
            decision_store: JsonSelectionEventStore::new(base_root),
            policy_store: JsonSelectionPolicyStore::new(base_root),
            health_store: JsonIdentityHealthStore::new(base_root),
        },
        CodexAppServerVerifier::default(),
    )
}

fn build_quota_status_service(
    base_root: &Path,
) -> QuotaStatusService<JsonRegistryStore, JsonQuotaStore> {
    QuotaStatusService::new(
        JsonRegistryStore::new(base_root),
        JsonQuotaStore::new(base_root),
    )
}

struct LoadedIdentityReports {
    reports: Vec<IdentityStatusReport>,
    auto_removal_notices: Vec<AutoRemovalNotice>,
}

enum VerifyIdentityOutcome {
    Verified,
    AutoRemoved,
}

fn resolve_add_identity_name(
    service: &IdentityRegistryService<JsonRegistryStore>,
    requested_name: Option<String>,
) -> Result<String> {
    match requested_name {
        Some(name) => Ok(name),
        None => {
            let identities = service.list_identities()?;
            next_auto_display_name(identities.iter().map(|identity| &identity.id))
        }
    }
}

fn load_identity_reports(base_root: &Path, cached: bool) -> Result<LoadedIdentityReports> {
    let service = build_quota_status_service(base_root);
    if cached {
        Ok(LoadedIdentityReports {
            reports: service.cached_statuses()?,
            auto_removal_notices: Vec::new(),
        })
    } else {
        let reports = service.refresh_all(&CodexAppServerVerifier::default())?;
        let remover = ManagedIdentityRemovalService::new(
            JsonRegistryStore::new(base_root),
            JsonQuotaStore::new(base_root),
            JsonIdentityHealthStore::new(base_root),
            JsonSelectionStore::new(base_root),
        );
        let sweep = auto_remove_deactivated_workspace_identities(reports, &remover);
        Ok(LoadedIdentityReports {
            reports: sweep.reports,
            auto_removal_notices: sweep.notices,
        })
    }
}

fn verify_identity_by_name(base_root: &Path, identity_name: &str) -> Result<VerifyIdentityOutcome> {
    let service = build_quota_status_service(base_root);
    match service.refresh_identity(identity_name, &CodexAppServerVerifier::default()) {
        Ok(refreshed) => {
            print_verification(refreshed.identity, refreshed.verification);
            Ok(VerifyIdentityOutcome::Verified)
        }
        Err(error) => {
            if let Some(notice) =
                maybe_auto_remove_identity_after_refresh_error(base_root, identity_name, &error)?
            {
                print_auto_removal_notices(std::slice::from_ref(&notice));
                return Ok(VerifyIdentityOutcome::AutoRemoved);
            }
            Err(error)
        }
    }
}

fn maybe_auto_remove_identity_after_refresh_error(
    base_root: &Path,
    identity_name: &str,
    error: &AppError,
) -> Result<Option<AutoRemovalNotice>> {
    let Some(IdentityRefreshErrorKind::WorkspaceDeactivated { http_status, code }) =
        classify_identity_refresh_error(error)
    else {
        return Ok(None);
    };

    let reason = WorkspaceDeactivationSummary { http_status, code };
    let identity = build_selection_service(base_root).resolve_by_name(identity_name)?;
    let remover = ManagedIdentityRemovalService::new(
        JsonRegistryStore::new(base_root),
        JsonQuotaStore::new(base_root),
        JsonIdentityHealthStore::new(base_root),
        JsonSelectionStore::new(base_root),
    );
    let notice = match remover.remove_identity_by_name(identity_name) {
        Ok(outcome) => AutoRemovalNotice::Removed {
            identity: outcome.identity,
            reason,
            selection_cleared: outcome.selection_cleared,
        },
        Err(remove_error) => AutoRemovalNotice::RemovalFailed {
            identity,
            reason,
            error: remove_error.to_string(),
        },
    };
    Ok(Some(notice))
}

fn login_identity(base_root: &Path, identity: &CodexIdentity, no_verify: bool) -> Result<()> {
    let launcher = CodexLauncher;
    let outcome = launcher.launch_login(identity)?;
    println!(
        "login completed for {} ({})",
        outcome.identity.display_name, outcome.identity.id
    );
    println!("launched: codex {}", outcome.command.join(" "));
    if no_verify {
        println!("verification skipped");
        println!(
            "next: codex-switch identities verify \"{}\" --base-root \"{}\"",
            outcome.identity.display_name,
            base_root.display()
        );
        return Ok(());
    }
    match verify_identity_by_name(base_root, &identity.display_name) {
        Ok(_) => Ok(()),
        Err(error) => {
            eprintln!("warning: login succeeded but post-login verification failed: {error}");
            println!("verification deferred");
            println!(
                "next: codex-switch identities verify \"{}\" --base-root \"{}\"",
                outcome.identity.display_name,
                base_root.display()
            );
            Ok(())
        }
    }
}

fn validate_add_login_flags(login: bool, no_verify: bool) -> Result<()> {
    if no_verify && !login {
        return Err(AppError::AddNoVerifyRequiresLogin);
    }
    Ok(())
}

fn build_workspace_switching_service(
    base_root: &Path,
) -> WorkspaceSwitchingService<JsonRegistryStore> {
    WorkspaceSwitchingService::new(JsonRegistryStore::new(base_root))
}

fn build_selection_policy_service(
    base_root: &Path,
) -> SelectionPolicyService<JsonSelectionPolicyStore> {
    SelectionPolicyService::new(JsonSelectionPolicyStore::new(base_root))
}

fn build_identity_health_service(
    base_root: &Path,
) -> IdentityHealthService<JsonRegistryStore, JsonIdentityHealthStore> {
    IdentityHealthService::new(
        JsonRegistryStore::new(base_root),
        JsonIdentityHealthStore::new(base_root),
    )
}

fn build_selection_service(
    base_root: &Path,
) -> IdentitySelectionService<JsonSelectionStore, JsonRegistryStore> {
    IdentitySelectionService::new(
        JsonSelectionStore::new(base_root),
        JsonRegistryStore::new(base_root),
    )
}

fn build_automatic_handoff_service(
    base_root: &Path,
) -> AutomaticHandoffService<
    JsonRegistryStore,
    JsonQuotaStore,
    JsonSelectionStore,
    JsonTaskCheckpointStore,
    JsonSelectionEventStore,
    JsonSelectionPolicyStore,
    JsonIdentityHealthStore,
    CodexAppServerVerifier,
> {
    AutomaticHandoffService::new(
        base_root,
        AutomaticHandoffStores {
            registry_store: JsonRegistryStore::new(base_root),
            quota_store: JsonQuotaStore::new(base_root),
            selection_store: JsonSelectionStore::new(base_root),
            checkpoint_store: JsonTaskCheckpointStore::new(base_root),
            decision_store: JsonSelectionEventStore::new(base_root),
            selection_context_stores: SelectionContextStores {
                policy_store: JsonSelectionPolicyStore::new(base_root),
                health_store: JsonIdentityHealthStore::new(base_root),
            },
        },
        CodexAppServerVerifier::default(),
    )
}

fn build_continue_service(
    base_root: &Path,
) -> ContinueService<
    JsonRegistryStore,
    CodexAppServerVerifier,
    JsonSelectionStore,
    JsonTaskCheckpointStore,
> {
    ContinueService::new(
        base_root,
        JsonRegistryStore::new(base_root),
        CodexAppServerVerifier::default(),
        JsonSelectionStore::new(base_root),
        JsonTaskCheckpointStore::new(base_root),
    )
}

fn print_workspace_force_summary(identity: &CodexIdentity, prefix: &str) {
    let Some(workspace_id) = identity.forced_chatgpt_workspace_id.as_deref() else {
        return;
    };

    println!("{prefix}workspace id: {workspace_id}");
    println!(
        "{prefix}workspace force probe: {}",
        identity.workspace_force_probe_status()
    );
    println!(
        "{prefix}workspace force active: {}",
        yes_no(identity.workspace_force_enabled())
    );
    if let Some(probe) = identity.workspace_force_probe.as_ref() {
        println!("{prefix}workspace force updated at: {}", probe.updated_at);
        if let Some(notes) = probe.notes.as_deref() {
            println!("{prefix}workspace force notes: {}", notes);
        }
    }
}

fn current_time_for_selector() -> Result<i64> {
    current_timestamp()
}

fn load_selector_context(base_root: &Path) -> Result<(IdentitySelector, IdentityHealthRecord)> {
    let policy = build_selection_policy_service(base_root).load_policy()?;
    let health = build_identity_health_service(base_root).load_record()?;
    let selector = IdentitySelector::new(policy, current_time_for_selector()?);
    Ok((selector, health))
}

fn print_selection_policy(policy: &IdentitySelectionPolicy) {
    println!("warning used percent: {}", policy.warning_used_percent);
    println!("avoid used percent: {}", policy.avoid_used_percent);
    println!("hard-stop used percent: {}", policy.hard_stop_used_percent);
    println!(
        "rate-limit cooldown secs: {}",
        policy.rate_limit_cooldown_secs
    );
    println!(
        "auth-failure cooldown secs: {}",
        policy.auth_failure_cooldown_secs
    );
}

fn print_identity_health_snapshot(
    identity: &CodexIdentity,
    state: Option<&IdentityHealthState>,
    now: i64,
) {
    println!("{} ({})", identity.display_name, identity.id);
    print_health_details(state, now, "  ");
}

fn print_workspace_force_details(identity: &CodexIdentity) {
    println!("name: {}", identity.display_name);
    println!("id: {}", identity.id);
    print_workspace_force_summary(identity, "");
}

fn print_workspace_force_probe_outcome(outcome: &WorkspaceForceProbeOutcome) {
    print_workspace_force_details(&outcome.identity);
    println!("probe summary: {}", outcome.report.summary);
    println!(
        "baseline changed: {}",
        yes_no(outcome.report.changed_from_baseline())
    );
    println!(
        "stable across restarts: {}",
        yes_no(outcome.report.stable_after_restart())
    );
    println!(
        "baseline authenticated: {}",
        yes_no(outcome.report.baseline.authenticated)
    );
    println!(
        "forced authenticated (first restart): {}",
        yes_no(outcome.report.forced_once.authenticated)
    );
    println!(
        "forced authenticated (second restart): {}",
        yes_no(outcome.report.forced_twice.authenticated)
    );
    println!(
        "effective workspace (first restart): {}",
        outcome
            .report
            .forced_once
            .effective_workspace_id
            .as_deref()
            .unwrap_or("none")
    );
    println!(
        "effective workspace (second restart): {}",
        outcome
            .report
            .forced_twice
            .effective_workspace_id
            .as_deref()
            .unwrap_or("none")
    );
}

fn print_registered_identity(
    base_root: &Path,
    identity: &CodexIdentity,
    next_login_command: Option<String>,
) {
    println!("registered {}", identity.display_name);
    println!("id: {}", identity.id);
    println!("kind: {}", format_identity_kind(identity.kind));
    println!("auth mode: {}", identity.auth_mode.as_str());
    println!("home: {}", identity.codex_home.display());
    println!(
        "shared sessions: {}",
        identity.shared_sessions_root.display()
    );
    println!("auth imported: {}", yes_no(identity.imported_auth));
    print_workspace_force_summary(identity, "");
    if let Some(env_var) = identity.api_key_env_var.as_deref() {
        println!("api key env var: {}", env_var);
    }
    if let Some(command) = next_login_command {
        println!("next:");
        println!("  {}", command);
    }
    println!("verify:");
    println!(
        "  codex-switch identities verify \"{}\" --base-root \"{}\"",
        identity.id,
        base_root.display()
    );
}

fn print_identity(identity: CodexIdentity) {
    println!("{} ({})", identity.display_name, identity.id);
    println!("  kind: {}", format_identity_kind(identity.kind));
    println!("  auth mode: {}", identity.auth_mode.as_str());
    println!("  home: {}", identity.codex_home.display());
    println!(
        "  shared sessions: {}",
        identity.shared_sessions_root.display()
    );
    println!("  enabled: {}", yes_no(identity.enabled));
    println!("  imported auth: {}", yes_no(identity.imported_auth));
    if let Some(email) = identity.email.as_deref() {
        println!("  email: {}", email);
    }
    if let Some(plan_type) = identity.plan_type {
        println!("  plan type: {}", plan_type);
    }
    if let Some(account_type) = identity.account_type {
        println!("  account type: {}", account_type);
    }
    if let Some(authenticated) = identity.authenticated {
        println!("  authenticated: {}", yes_no(authenticated));
    }
    if let Some(last_verified_at) = identity.last_verified_at {
        println!("  last verified at: {}", last_verified_at);
    }
    print_workspace_force_summary(&identity, "  ");
}

fn print_verification(identity: CodexIdentity, summary: IdentityVerification) {
    println!("name: {}", identity.display_name);
    println!("id: {}", identity.id);
    println!("home: {}", identity.codex_home.display());
    print_workspace_force_summary(&identity, "");
    println!("authenticated: {}", yes_no(summary.authenticated));
    println!(
        "auth method: {}",
        summary.auth_method.as_deref().unwrap_or("unknown")
    );
    println!(
        "account type: {}",
        summary
            .account_type
            .map(|account_type| account_type.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!("email: {}", summary.email.as_deref().unwrap_or("unknown"));
    println!(
        "plan type: {}",
        summary
            .plan_type
            .map(|plan_type| plan_type.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!(
        "requires OpenAI auth: {}",
        yes_no(summary.requires_openai_auth)
    );

    if summary.rate_limits_by_limit_id.is_empty() {
        match summary.fallback_rate_limit {
            Some(snapshot) => {
                println!("rate limits:");
                println!("  default: {}", format_rate_limit_snapshot(&snapshot));
            }
            None => println!("rate limits: none"),
        }
        return;
    }

    println!("rate limits:");
    for (limit_id, snapshot) in summary.rate_limits_by_limit_id {
        println!("  {}: {}", limit_id, format_rate_limit_snapshot(&snapshot));
    }
}

enum IdentityReportFormat {
    Status,
}

fn print_identity_report(
    report: IdentityStatusReport,
    evaluation: IdentityEvaluation,
    format: IdentityReportFormat,
    evaluated_at: i64,
) {
    let IdentityStatusReport {
        identity,
        quota_status,
        refresh_error,
        refresh_error_kind: _,
    } = report;

    println!("{} ({})", identity.display_name, identity.id);
    match format {
        IdentityReportFormat::Status => {
            println!("  kind: {}", format_identity_kind(identity.kind));
            println!("  auth mode: {}", identity.auth_mode.as_str());
        }
    }

    print_identity_account_details(&identity);
    print_health_details(evaluation.health_state.as_ref(), evaluated_at, "  ");
    print_selector_details(&evaluation);
    if let Some(error) = refresh_error.as_deref() {
        println!("  refresh error: {}", error);
    }
    print_quota_status(quota_status.as_ref());
}

fn print_account_overview(
    identity: &CodexIdentity,
    quota_status: Option<&IdentityQuotaStatus>,
    refresh_error_kind: Option<&crate::quota_status::IdentityRefreshErrorKind>,
    refresh_error: Option<&str>,
    matches_default_codex_home: bool,
    color_output: bool,
) {
    let snapshot = preferred_account_snapshot(quota_status);
    let name = if matches_default_codex_home {
        style_text(&identity.display_name, "1;36", color_output)
    } else {
        identity.display_name.clone()
    };
    println!("{name}");
    if let Some(refresh_error) = refresh_error {
        println!(
            "  {}",
            style_text(
                &format!(
                    "stale quota: {}",
                    summarize_account_refresh_error(refresh_error_kind, refresh_error)
                ),
                "1;33",
                color_output
            )
        );
    }
    println!(
        "  5h limit:    {}",
        format_account_window(find_window_by_duration(snapshot, 300), color_output)
    );
    println!(
        "  1 week:      {}",
        format_account_window(find_window_by_duration(snapshot, 10_080), color_output)
    );
}

fn print_identity_account_details(identity: &CodexIdentity) {
    println!(
        "  authenticated: {}",
        format_optional_yes_no(identity.authenticated)
    );
    println!(
        "  account type: {}",
        identity
            .account_type
            .map(|account_type| account_type.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!(
        "  email: {}",
        identity.email.as_deref().unwrap_or("unknown")
    );
    println!(
        "  plan type: {}",
        identity
            .plan_type
            .map(|plan_type| plan_type.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
}

fn print_health_details(state: Option<&IdentityHealthState>, now: i64, prefix: &str) {
    let manually_disabled = state.is_some_and(|health| health.manually_disabled);
    let penalty_active = state.is_some_and(|health| health.penalty_active_at(now));
    println!("{prefix}manually disabled: {}", yes_no(manually_disabled));
    println!("{prefix}penalty active: {}", yes_no(penalty_active));
    println!(
        "{prefix}penalty until: {}",
        state
            .and_then(|health| health.penalty_until)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "{prefix}last failure kind: {}",
        state
            .and_then(|health| health.last_failure_kind.map(|kind| kind.to_string()))
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "{prefix}last failure summary: {}",
        state
            .and_then(|health| health.last_failure_message.clone())
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "{prefix}health updated at: {}",
        state
            .map(|health| health.updated_at.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
}

fn print_selector_details(evaluation: &IdentityEvaluation) {
    println!("  selector eligible: {}", yes_no(evaluation.selectable()));
    if let Some(reason) = evaluation.rejection_reason.as_ref() {
        println!("  selector reason: {}", reason.as_str());
    }
    if let Some(relevant_bucket) = evaluation.relevant_bucket.as_ref() {
        println!(
            "  selector bucket: {} used={} headroom={} status={}",
            relevant_bucket.source.label(),
            relevant_bucket.max_used_percent,
            relevant_bucket.remaining_headroom_percent,
            relevant_bucket.usage_band.as_str()
        );
    }
}

fn print_selected_identity(selected: &SelectedIdentity) {
    println!("selected {}", selected.identity.display_name);
    println!("id: {}", selected.identity.id);
    println!("auth mode: {}", selected.identity.auth_mode.as_str());
    println!(
        "selector bucket: {}",
        selected.relevant_bucket.source.label()
    );
    println!(
        "used percent: {}",
        selected.relevant_bucket.max_used_percent
    );
    println!(
        "remaining headroom: {}",
        selected.relevant_bucket.remaining_headroom_percent
    );
    println!("status: {}", selected.relevant_bucket.usage_band.as_str());
}

fn print_exec_failover_result(result: &ExecFailoverResult) {
    print_auto_removal_notices(&result.auto_removal_notices);

    if let Some(initial_identity) = result.initial_identity.as_ref() {
        println!(
            "initial identity: {} ({})",
            initial_identity.display_name, initial_identity.id
        );
    } else {
        println!("initial identity: none");
    }

    if result.skipped_due_to_health.is_empty() {
        println!("skipped due to health: none");
    } else {
        for skipped in &result.skipped_due_to_health {
            println!(
                "skipped due to health: {} ({}) reason={}",
                skipped.identity.display_name,
                skipped.identity.id,
                skipped.rejection_reason.as_str()
            );
        }
    }

    if result.penalized_during_run.is_empty() {
        println!("penalized during run: none");
    } else {
        for penalized in &result.penalized_during_run {
            println!(
                "penalized during run: {} ({}) kind={} penalty_until={}",
                penalized.identity.display_name,
                penalized.identity.id,
                penalized.failure_kind,
                penalized
                    .penalty_until
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            );
        }
    }

    if let Some(launch) = result.launched.as_ref() {
        println!(
            "final launched identity: {} ({})",
            launch.identity.display_name, launch.identity.id
        );
        println!("launched: codex {}", launch.command.join(" "));
    } else {
        println!("no eligible identity after failover");
    }

    if let Some(decision_log) = result.decision_log.as_ref() {
        println!("decision log: {}", decision_log.path.display());
    }
}

fn preferred_account_snapshot(
    quota_status: Option<&IdentityQuotaStatus>,
) -> Option<&RateLimitSnapshot> {
    let quota_status = quota_status?;
    quota_status
        .rate_limits_by_limit_id
        .get("codex")
        .or(quota_status.default_rate_limit.as_ref())
        .or_else(|| quota_status.rate_limits_by_limit_id.values().next())
}

fn order_account_reports(
    selector: &IdentitySelector,
    health: &IdentityHealthRecord,
    reports: Vec<IdentityStatusReport>,
    cached: bool,
) -> Vec<IdentityStatusReport> {
    let mut ranked = reports
        .into_iter()
        .map(|report| {
            let evaluation = selector.evaluate(
                &report.identity,
                report.quota_status.as_ref(),
                health.identities.get(&report.identity.id),
            );
            (report, evaluation)
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|(left_report, left_eval), (right_report, right_eval)| {
        compare_account_reports(left_report, left_eval, right_report, right_eval, cached)
    });
    ranked.into_iter().map(|(report, _)| report).collect()
}

fn compare_account_reports(
    left_report: &IdentityStatusReport,
    left_eval: &IdentityEvaluation,
    right_report: &IdentityStatusReport,
    right_eval: &IdentityEvaluation,
    cached: bool,
) -> std::cmp::Ordering {
    let left_rank = account_report_rank(left_report, left_eval, cached);
    let right_rank = account_report_rank(right_report, right_eval, cached);

    left_rank
        .cmp(&right_rank)
        .then_with(|| compare_account_bucket(left_eval, right_eval))
        .then_with(|| {
            right_report
                .identity
                .priority
                .cmp(&left_report.identity.priority)
        })
        .then_with(|| {
            left_report
                .identity
                .id
                .as_str()
                .cmp(right_report.identity.id.as_str())
        })
}

fn account_report_rank(
    report: &IdentityStatusReport,
    evaluation: &IdentityEvaluation,
    cached: bool,
) -> u8 {
    if !cached && report.refresh_error.is_some() {
        return 7;
    }

    match evaluation.rejection_reason {
        None => 0,
        Some(crate::identity_selector::RejectionReason::AvoidNewSession) => 1,
        Some(crate::identity_selector::RejectionReason::Exhausted) => 2,
        Some(crate::identity_selector::RejectionReason::PenaltyActive) => 3,
        Some(crate::identity_selector::RejectionReason::ManuallyDisabled) => 4,
        Some(crate::identity_selector::RejectionReason::Unauthenticated) => 5,
        Some(crate::identity_selector::RejectionReason::MissingQuotaState)
        | Some(crate::identity_selector::RejectionReason::MissingBucketData) => 6,
        Some(crate::identity_selector::RejectionReason::Disabled) => 8,
    }
}

fn compare_account_bucket(
    left_eval: &IdentityEvaluation,
    right_eval: &IdentityEvaluation,
) -> std::cmp::Ordering {
    let left_headroom = left_eval
        .relevant_bucket
        .as_ref()
        .map(|bucket| bucket.remaining_headroom_percent)
        .unwrap_or(-1);
    let right_headroom = right_eval
        .relevant_bucket
        .as_ref()
        .map(|bucket| bucket.remaining_headroom_percent)
        .unwrap_or(-1);
    right_headroom.cmp(&left_headroom)
}

fn find_window_by_duration(
    snapshot: Option<&RateLimitSnapshot>,
    duration_mins: i64,
) -> Option<&RateLimitWindow> {
    let snapshot = snapshot?;
    [snapshot.primary.as_ref(), snapshot.secondary.as_ref()]
        .into_iter()
        .flatten()
        .find(|window| window.window_duration_mins == Some(duration_mins))
}

fn format_account_window(window: Option<&RateLimitWindow>, color_output: bool) -> String {
    const REMAINING_LABEL_WIDTH: usize = 9;

    let Some(window) = window else {
        return format!(
            "{} {} ({})",
            render_remaining_bar(0),
            style_text(
                &format!("{:>width$}", "n/a", width = REMAINING_LABEL_WIDTH),
                "2",
                color_output
            ),
            format_reset_time(None, None, color_output)
        );
    };
    let used = window.used_percent.clamp(0, 100);
    let remaining = (100 - used).clamp(0, 100);
    let (red, green, blue) = gradient_rgb(remaining);
    let remaining_label = format!("{remaining:>3}% left");
    let percent = style_rgb_text(&remaining_label, red, green, blue, false, color_output);
    let reset = format_reset_time(window.resets_at, window.window_duration_mins, color_output);
    format!(
        "{} {} ({})",
        render_remaining_bar(remaining),
        percent,
        reset
    )
}

fn stdout_supports_color() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }

    if std::env::var("CLICOLOR_FORCE")
        .map(|value| value != "0")
        .unwrap_or(false)
    {
        return true;
    }

    std::io::stdout().is_terminal()
}

fn style_text(text: &str, ansi_code: &str, enabled: bool) -> String {
    if enabled {
        format!("\x1b[{ansi_code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

fn style_rgb_text(text: &str, red: u8, green: u8, blue: u8, bold: bool, enabled: bool) -> String {
    if !enabled {
        return text.to_string();
    }
    let weight = if bold { "1;" } else { "" };
    format!("\x1b[{weight}38;2;{red};{green};{blue}m{text}\x1b[0m")
}

fn render_remaining_bar(remaining_percent: i32) -> String {
    const BAR_WIDTH: usize = 20;
    let filled = ((remaining_percent.clamp(0, 100) as usize * BAR_WIDTH) + 50) / 100;
    let filled_part = "█".repeat(filled);
    let empty_part = "░".repeat(BAR_WIDTH.saturating_sub(filled));
    format!("[{filled_part}{empty_part}]")
}

fn gradient_rgb(percent: i32) -> (u8, u8, u8) {
    let clamped = percent.clamp(0, 100) as f32 / 100.0;
    if clamped <= 0.5 {
        interpolate_rgb((255, 59, 48), (255, 204, 0), clamped / 0.5)
    } else {
        interpolate_rgb((255, 204, 0), (52, 199, 89), (clamped - 0.5) / 0.5)
    }
}

fn interpolate_rgb(start: (u8, u8, u8), end: (u8, u8, u8), progress: f32) -> (u8, u8, u8) {
    let blend = |from: u8, to: u8| -> u8 {
        let value = from as f32 + (to as f32 - from as f32) * progress.clamp(0.0, 1.0);
        value.round() as u8
    };
    (
        blend(start.0, end.0),
        blend(start.1, end.1),
        blend(start.2, end.2),
    )
}

fn format_reset_time(
    resets_at: Option<i64>,
    window_duration_mins: Option<i64>,
    color_output: bool,
) -> String {
    let Some(resets_at) = resets_at else {
        return style_text("resets --:--", "2", color_output);
    };
    let Some(datetime) = Local.timestamp_opt(resets_at, 0).single() else {
        return style_text("resets --:--", "2", color_output);
    };
    let show_day =
        window_duration_mins == Some(10_080) && datetime.date_naive() != Local::now().date_naive();
    if show_day {
        format!("resets {}", datetime.format("%a %H:%M"))
    } else {
        format!("resets {}", datetime.format("%H:%M"))
    }
}

fn detect_default_codex_identity_ids<'a>(
    identities: impl IntoIterator<Item = &'a CodexIdentity>,
) -> std::collections::BTreeSet<IdentityId> {
    let mut matches = std::collections::BTreeSet::new();
    let Ok(default_home) = default_codex_home() else {
        return matches;
    };
    let Ok(default_auth) = read_auth_fingerprint(&default_home.join("auth.json")) else {
        return matches;
    };
    let Some(default_auth) = default_auth else {
        return matches;
    };
    let default_marker = read_auth_identity_marker(&default_home.join("auth.json"))
        .ok()
        .flatten();

    for identity in identities {
        let auth_path = identity.codex_home.join("auth.json");
        match read_auth_fingerprint(&auth_path) {
            Ok(Some(identity_auth)) if identity_auth == default_auth => {
                matches.insert(identity.id.clone());
            }
            _ => {
                if let (Some(default_marker), Ok(Some(identity_marker))) =
                    (&default_marker, read_auth_identity_marker(&auth_path))
                {
                    if identity_marker == *default_marker {
                        matches.insert(identity.id.clone());
                    }
                }
            }
        }
    }

    matches
}

fn detect_default_codex_identity_ids_from_runtime(
    reports: &[IdentityStatusReport],
) -> std::collections::BTreeSet<IdentityId> {
    let mut matches = std::collections::BTreeSet::new();
    let Ok(default_home) = default_codex_home() else {
        return matches;
    };
    let verifier = CodexAppServerVerifier::new(AppServerCommand::default(), Duration::from_secs(5));
    let Ok(verification) = verifier.verify_codex_home(&default_home) else {
        return matches;
    };

    for report in reports {
        if report_matches_default_codex(report, &verification) {
            matches.insert(report.identity.id.clone());
        }
    }

    matches
}

fn report_matches_default_codex(
    report: &IdentityStatusReport,
    verification: &IdentityVerification,
) -> bool {
    report.identity.account_type == verification.account_type
        && report.identity.email == verification.email
        && report.identity.plan_type == verification.plan_type
        && report.quota_status.as_ref().is_some_and(|quota_status| {
            quota_status.default_rate_limit == verification.fallback_rate_limit
                && quota_status.rate_limits_by_limit_id == verification.rate_limits_by_limit_id
        })
}

fn read_auth_fingerprint(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(content) => Ok(Some(content)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthIdentityMarker {
    account_id: String,
    auth_mode: Option<String>,
}

fn read_auth_identity_marker(path: &Path) -> Result<Option<AuthIdentityMarker>> {
    let Some(bytes) = read_auth_fingerprint(path)? else {
        return Ok(None);
    };
    let payload: Value = serde_json::from_slice(&bytes)?;
    let account_id = payload
        .get("tokens")
        .and_then(|tokens| tokens.get("account_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let auth_mode = payload
        .get("auth_mode")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    Ok(account_id.map(|account_id| AuthIdentityMarker {
        account_id,
        auth_mode,
    }))
}

fn print_current_selection(current: &CurrentIdentitySelection) {
    println!("selected {}", current.identity.display_name);
    println!("id: {}", current.identity.id);
    println!("mode: {}", current.selection.mode);
    println!("auth mode: {}", current.identity.auth_mode.as_str());
    println!("home: {}", current.identity.codex_home.display());
    if let Some(reason) = current.selection.reason.as_deref() {
        println!("reason: {}", reason);
    }
    println!("updated at: {}", current.selection.updated_at);
}

fn print_selection_summary(current: &CurrentIdentitySelection) {
    println!(
        "current selection: {} ({}) mode={}",
        current.identity.display_name, current.identity.id, current.selection.mode
    );
}

fn print_auto_removal_notices(notices: &[AutoRemovalNotice]) {
    for notice in notices {
        let prefix = if notice.is_failure() {
            "warning"
        } else {
            "notice"
        };
        eprintln!("{prefix}: {}", notice.summary());
    }
}

fn summarize_account_refresh_error(
    refresh_error_kind: Option<&crate::quota_status::IdentityRefreshErrorKind>,
    refresh_error: &str,
) -> String {
    if let Some(crate::quota_status::IdentityRefreshErrorKind::WorkspaceDeactivated {
        http_status,
        code,
    }) = refresh_error_kind
    {
        return format!("{http_status} {code}");
    }

    let _ = refresh_error;
    "live refresh failed".to_string()
}

fn print_continue_result(result: &crate::continuation::ContinueThreadResult) {
    println!(
        "source identity: {} ({})",
        result.source_identity.display_name, result.source_identity.id
    );
    println!(
        "target identity: {} ({})",
        result.target_identity.display_name, result.target_identity.id
    );
    println!("mode: {}", result.mode.as_str());
    println!("checkpoint: {}", result.checkpoint_path.display());
    println!("lease state: {}", result.lease.lease_state);
    println!(
        "baseline latest turn id: {}",
        result
            .baseline_snapshot
            .latest_turn_id
            .as_deref()
            .unwrap_or("none")
    );
    if let Some(target_snapshot) = result.target_snapshot.as_ref() {
        println!("target visibility turns: {}", target_snapshot.turn_count());
        println!(
            "target latest turn id: {}",
            target_snapshot.latest_turn_id.as_deref().unwrap_or("none")
        );
    }
    if let Some(fallback_reason) = result.checkpoint.fallback_reason.as_deref() {
        println!("fallback reason: {}", fallback_reason);
        println!("resume prompt: {}", result.checkpoint.resume_prompt);
    }
    if let Some(launch) = result.launch.as_ref() {
        println!("launched: codex {}", launch.command.join(" "));
    }
}

fn print_automatic_continue_result(
    result: &crate::automatic_handoff::AutomaticContinueThreadResult,
) {
    print_continue_result(&result.continue_result);
    println!("auto target: {}", result.selected.identity.display_name);
    println!(
        "auto selector bucket: {}",
        result.selected.relevant_bucket.source.label()
    );
    println!("decision log: {}", result.decision_log.path.display());
}

fn print_quota_status(quota_status: Option<&IdentityQuotaStatus>) {
    let Some(quota_status) = quota_status else {
        println!("  quota updated at: none");
        println!("  rate limits: none");
        return;
    };

    println!("  quota updated at: {}", quota_status.updated_at);

    if quota_status.rate_limits_by_limit_id.is_empty() {
        match quota_status.default_rate_limit.as_ref() {
            Some(snapshot) => {
                println!("  rate limits:");
                println!("    default: {}", format_rate_limit_snapshot(snapshot));
            }
            None => println!("  rate limits: none"),
        }
        return;
    }

    println!("  rate limits:");
    for (limit_id, snapshot) in &quota_status.rate_limits_by_limit_id {
        println!("    {}: {}", limit_id, format_rate_limit_snapshot(snapshot));
    }
}

fn print_thread_snapshot(snapshot: &ThreadSnapshot) {
    println!("thread id: {}", snapshot.thread_id);
    println!("status: {}", snapshot.status);
    println!("turn count: {}", snapshot.turn_count());
    println!(
        "latest turn id: {}",
        snapshot.latest_turn_id.as_deref().unwrap_or("none")
    );
    println!(
        "latest turn status: {}",
        snapshot
            .latest_turn_status
            .as_ref()
            .map(|status| status.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!("updated at: {}", snapshot.updated_at);
    if let Some(path) = snapshot.path.as_deref() {
        println!("path: {}", path);
    }
}

#[cfg(test)]
mod tests {
    use super::summarize_account_refresh_error;
    use crate::quota_status::IdentityRefreshErrorKind;

    #[test]
    fn summarizes_refresh_error_with_http_status_and_detail_code() {
        let error = r#"rpc call account/rateLimits/read failed with code -32603: failed to fetch codex rate limits: GET https://chatgpt.com/backend-api/wham/usage failed: 402 Payment Required; content-type=application/json; body={"detail":{"code":"deactivated_workspace"}}"#;
        assert_eq!(
            summarize_account_refresh_error(
                Some(&IdentityRefreshErrorKind::WorkspaceDeactivated {
                    http_status: 402,
                    code: "deactivated_workspace".to_string(),
                }),
                error,
            ),
            "402 deactivated_workspace"
        );
    }

    #[test]
    fn falls_back_to_generic_refresh_error_summary() {
        let error = "rpc call account/read timed out after 20s";
        assert_eq!(
            summarize_account_refresh_error(None, error),
            "live refresh failed"
        );
    }
}

fn print_thread_lease(lease: &ThreadLeaseRecord) {
    println!("thread id: {}", lease.thread_id);
    println!("owner: {}", lease.owner_identity_id);
    println!("state: {}", lease.lease_state);
    println!("lease token: {}", lease.lease_token);
    println!("last heartbeat at: {}", lease.last_heartbeat_at);
    println!("updated at: {}", lease.updated_at);
    if let Some(target) = lease.handoff_to_identity_id.as_ref() {
        println!("handoff target: {}", target);
    }
    if let Some(reason) = lease.handoff_reason.as_deref() {
        println!("handoff reason: {}", reason);
    }
}

fn print_handoff_preparation(preparation: &HandoffPreparation) {
    println!("handoff prepared");
    print_thread_lease(&preparation.lease);
    println!("baseline:");
    println!("  turns: {}", preparation.baseline_snapshot.turn_count());
    println!(
        "  latest turn id: {}",
        preparation
            .baseline_snapshot
            .latest_turn_id
            .as_deref()
            .unwrap_or("none")
    );
    println!("target visibility:");
    println!("  turns: {}", preparation.target_snapshot.turn_count());
    println!(
        "  latest turn id: {}",
        preparation
            .target_snapshot
            .latest_turn_id
            .as_deref()
            .unwrap_or("none")
    );
}

fn print_handoff_acceptance(acceptance: &HandoffAcceptance) {
    println!("handoff accepted");
    print_thread_lease(&acceptance.lease);
    print_tracked_turn_state(&acceptance.turn_state);
}

fn print_tracked_turn_state(state: &TrackedTurnState) {
    println!("thread id: {}", state.thread_id);
    println!("tracked state: {}", state.state);
    println!(
        "owner: {}",
        state
            .owner_identity_id
            .as_ref()
            .map(|identity| identity.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    println!("turn count: {}", state.turn_count);
    println!(
        "latest turn id: {}",
        state.latest_turn_id.as_deref().unwrap_or("none")
    );
    if let Some(handoff) = state.handoff.as_ref() {
        println!("handoff from: {}", handoff.from_identity_id);
        println!("handoff to: {}", handoff.to_identity_id);
        println!("handoff reason: {}", handoff.reason);
        println!("handoff baseline turns: {}", handoff.baseline_turn_count);
        if let Some(confirmed_turn_id) = handoff.confirmed_turn_id.as_deref() {
            println!("handoff confirmed turn id: {}", confirmed_turn_id);
        }
    }
}

fn format_rate_limit_snapshot(snapshot: &RateLimitSnapshot) -> String {
    let primary = snapshot
        .primary
        .as_ref()
        .map(format_rate_limit_window)
        .unwrap_or_else(|| "none".to_string());
    let secondary = snapshot
        .secondary
        .as_ref()
        .map(format_rate_limit_window)
        .unwrap_or_else(|| "none".to_string());

    let mut parts = vec![
        format!("primary={primary}"),
        format!("secondary={secondary}"),
    ];
    if let Some(limit_name) = snapshot.limit_name.as_deref() {
        parts.push(format!("name={limit_name}"));
    }
    if let Some(plan_type) = snapshot.plan_type {
        parts.push(format!("plan={plan_type}"));
    }
    if let Some(credits) = snapshot.credits.as_ref() {
        parts.push(format!(
            "credits=has:{} unlimited:{} balance:{}",
            yes_no(credits.has_credits),
            yes_no(credits.unlimited),
            credits.balance.as_deref().unwrap_or("unknown")
        ));
    }
    parts.join(" ")
}

fn format_rate_limit_window(window: &RateLimitWindow) -> String {
    let mut parts = vec![format!("{}%", window.used_percent)];
    if let Some(duration) = window.window_duration_mins {
        parts.push(format!("{duration}m"));
    }
    if let Some(resets_at) = window.resets_at {
        parts.push(format!("resets@{resets_at}"));
    }
    parts.join("/")
}

fn format_identity_kind(kind: IdentityKind) -> &'static str {
    match kind {
        IdentityKind::ChatgptWorkspace => "chatgpt_workspace",
        IdentityKind::ApiKey => "api_key",
    }
}

fn format_optional_yes_no(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "yes",
        Some(false) => "no",
        None => "unknown",
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}
