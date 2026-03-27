use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::{json, Value};

use crate::error::{AppError, Result};
use crate::session_control::{
    error_code, HandoffAcceptRequest, HandoffConfirmFallback, HandoffConfirmRequest,
    HandoffPrepareRequest, ResumeSessionRequest, SessionControlService, SessionControlWorker,
    SessionStartRequest, SessionStreamRequest, SessionTurnStartRequest,
};
use crate::storage::paths::{default_base_root, resolve_path};

const INTERFACE_VERSION: &str = "1";

#[derive(Debug, Args)]
pub struct SessionsCommand {
    #[command(subcommand)]
    pub command: SessionsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum SessionsSubcommand {
    Start(SessionStartCommand),
    Resume(SessionResumeCommand),
    Show(SessionRefCommand),
    List(SessionListCommand),
    Stream(SessionStreamCommand),
    Cancel(SessionRefCommand),
}

#[derive(Debug, Args)]
pub struct TurnsCommand {
    #[command(subcommand)]
    pub command: TurnsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum TurnsSubcommand {
    Start(TurnStartCommand),
    Show(TurnRefCommand),
    Wait(TurnWaitCommand),
    Cancel(TurnRefCommand),
    #[command(hide = true)]
    Worker(TurnWorkerCommand),
}

#[derive(Debug, Args)]
pub struct HandoffsCommand {
    #[command(subcommand)]
    pub command: HandoffsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum HandoffsSubcommand {
    Prepare(HandoffPrepareCommand),
    Accept(HandoffAcceptCommand),
    Confirm(HandoffConfirmCommand),
    Show(HandoffShowCommand),
}

#[derive(Debug, Args)]
pub struct SessionStartCommand {
    #[arg(long)]
    pub topic_key: String,
    #[arg(long)]
    pub prompt: Option<String>,
    #[arg(long)]
    pub prompt_file: Option<PathBuf>,
    #[arg(long)]
    pub workspace: Option<PathBuf>,
    #[arg(long)]
    pub identity: Option<String>,
    #[arg(long)]
    pub auto: bool,
    #[arg(long)]
    pub cached: bool,
    #[arg(long)]
    pub model: Option<String>,
    #[arg(long)]
    pub idempotency_key: Option<String>,
    #[arg(long)]
    pub max_runtime_secs: Option<i64>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SessionResumeCommand {
    #[arg(long)]
    pub session: String,
    #[arg(long)]
    pub prompt: Option<String>,
    #[arg(long)]
    pub prompt_file: Option<PathBuf>,
    #[arg(long)]
    pub identity: Option<String>,
    #[arg(long)]
    pub auto: bool,
    #[arg(long)]
    pub cached: bool,
    #[arg(long)]
    pub idempotency_key: Option<String>,
    #[arg(long)]
    pub allow_checkpoint_fallback: bool,
    #[arg(long)]
    pub max_runtime_secs: Option<i64>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SessionRefCommand {
    #[arg(long)]
    pub session: String,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SessionListCommand {
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SessionStreamCommand {
    #[arg(long)]
    pub session: String,
    #[arg(long, default_value_t = 0)]
    pub after_sequence: i64,
    #[arg(long)]
    pub follow: bool,
    #[arg(long, default_value_t = 250)]
    pub poll_interval_ms: u64,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct TurnStartCommand {
    #[arg(long)]
    pub session: String,
    #[arg(long)]
    pub prompt: Option<String>,
    #[arg(long)]
    pub prompt_file: Option<PathBuf>,
    #[arg(long)]
    pub identity: Option<String>,
    #[arg(long)]
    pub auto: bool,
    #[arg(long)]
    pub cached: bool,
    #[arg(long)]
    pub idempotency_key: Option<String>,
    #[arg(long)]
    pub allow_checkpoint_fallback: bool,
    #[arg(long)]
    pub max_runtime_secs: Option<i64>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct TurnRefCommand {
    #[arg(long)]
    pub turn: String,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct TurnWaitCommand {
    #[arg(long)]
    pub turn: String,
    #[arg(long)]
    pub timeout_secs: Option<u64>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct TurnWorkerCommand {
    #[arg(long)]
    pub turn_id: String,
    #[arg(long)]
    pub lease_owner_id: String,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct HandoffPrepareCommand {
    #[arg(long)]
    pub session: String,
    #[arg(long)]
    pub to_identity: String,
    #[arg(long)]
    pub reason: String,
    #[arg(long)]
    pub idempotency_key: Option<String>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct HandoffAcceptCommand {
    #[arg(long)]
    pub handoff: String,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum HandoffFallbackArg {
    CheckpointFallback,
}

#[derive(Debug, Args)]
pub struct HandoffConfirmCommand {
    #[arg(long)]
    pub handoff: String,
    #[arg(long)]
    pub observed_turn_id: Option<String>,
    #[arg(long, value_enum)]
    pub fallback: Option<HandoffFallbackArg>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct HandoffShowCommand {
    #[arg(long)]
    pub handoff: String,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub base_root: Option<PathBuf>,
}

pub fn run_sessions(command: SessionsCommand) -> Result<()> {
    match command.command {
        SessionsSubcommand::Start(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            let prompt_text = load_prompt(command.prompt, command.prompt_file)?;
            match service.start_session(SessionStartRequest {
                topic_key: command.topic_key,
                prompt_text,
                workspace_root: resolve_workspace(command.workspace.as_deref())?,
                identity_name: command.identity,
                auto: command.auto,
                cached: command.cached,
                model: command.model,
                idempotency_key: command.idempotency_key,
                max_runtime_secs: command.max_runtime_secs,
            }) {
                Ok(result) => {
                    if command.json {
                        print_json_success(
                            "sessions.start",
                            json!({
                                "session": result.session.session,
                                "active_turn": result.session.active_turn,
                                "last_turn": result.session.last_turn,
                                "pending_handoff": result.session.pending_handoff,
                                "started_turn": result.started_turn,
                            }),
                        )?;
                    } else {
                        print_session_snapshot(&result.session);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "sessions.start", error),
            }
        }
        SessionsSubcommand::Resume(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            let prompt_text = load_prompt(command.prompt, command.prompt_file)?;
            match service.resume_session(ResumeSessionRequest {
                session_id: command.session,
                prompt_text,
                identity_name: command.identity,
                auto: command.auto,
                cached: command.cached,
                idempotency_key: command.idempotency_key,
                allow_checkpoint_fallback: command.allow_checkpoint_fallback,
                max_runtime_secs: command.max_runtime_secs,
            }) {
                Ok(result) => {
                    if command.json {
                        print_json_success(
                            "sessions.resume",
                            json!({
                                "session": result.session.session,
                                "active_turn": result.session.active_turn,
                                "last_turn": result.session.last_turn,
                                "pending_handoff": result.session.pending_handoff,
                                "turn": result.turn,
                                "continuity_mode": result.continuity_mode,
                            }),
                        )?;
                    } else {
                        print_session_snapshot(&result.session);
                        println!("continuity mode: {}", result.continuity_mode.as_str());
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "sessions.resume", error),
            }
        }
        SessionsSubcommand::Show(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service.show_session(&command.session) {
                Ok(snapshot) => {
                    if command.json {
                        print_json_success(
                            "sessions.show",
                            json!({
                                "session": snapshot.session,
                                "active_turn": snapshot.active_turn,
                                "last_turn": snapshot.last_turn,
                                "pending_handoff": snapshot.pending_handoff,
                            }),
                        )?;
                    } else {
                        print_session_snapshot(&snapshot);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "sessions.show", error),
            }
        }
        SessionsSubcommand::List(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service.list_sessions() {
                Ok(sessions) => {
                    if command.json {
                        print_json_success("sessions.list", json!({ "sessions": sessions }))?;
                    } else if sessions.is_empty() {
                        println!("no sessions");
                    } else {
                        for snapshot in sessions {
                            print_session_snapshot(&snapshot);
                            println!();
                        }
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "sessions.list", error),
            }
        }
        SessionsSubcommand::Stream(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            if !command.json {
                return Err(AppError::InvalidSessionControlState {
                    message: "sessions stream currently requires --json".to_string(),
                });
            }
            let mut after_sequence = command.after_sequence;
            loop {
                let events = service.session_events(SessionStreamRequest {
                    session_id: command.session.clone(),
                    after_sequence_no: after_sequence,
                })?;
                for event in events {
                    println!("{}", serde_json::to_string(&event)?);
                    io::stdout().flush()?;
                    after_sequence = event.sequence_no;
                }
                if !command.follow {
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(command.poll_interval_ms));
            }
        }
        SessionsSubcommand::Cancel(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service.cancel_session(&command.session) {
                Ok(snapshot) => {
                    if command.json {
                        print_json_success(
                            "sessions.cancel",
                            json!({
                                "session": snapshot.session,
                                "active_turn": snapshot.active_turn,
                                "last_turn": snapshot.last_turn,
                                "pending_handoff": snapshot.pending_handoff,
                            }),
                        )?;
                    } else {
                        print_session_snapshot(&snapshot);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "sessions.cancel", error),
            }
        }
    }
}

pub fn run_turns(command: TurnsCommand) -> Result<()> {
    match command.command {
        TurnsSubcommand::Start(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            let prompt_text = load_prompt(command.prompt, command.prompt_file)?
                .ok_or(AppError::TaskPromptRequired)?;
            match service.start_turn(SessionTurnStartRequest {
                session_id: command.session,
                prompt_text,
                identity_name: command.identity,
                auto: command.auto,
                cached: command.cached,
                idempotency_key: command.idempotency_key,
                allow_checkpoint_fallback: command.allow_checkpoint_fallback,
                max_runtime_secs: command.max_runtime_secs,
            }) {
                Ok(result) => {
                    if command.json {
                        print_json_success(
                            "turns.start",
                            json!({
                                "turn": result.turn,
                                "session": result.session.session,
                                "active_turn": result.session.active_turn,
                                "pending_handoff": result.session.pending_handoff,
                                "continuity_mode": result.continuity_mode,
                            }),
                        )?;
                    } else {
                        print_turn(&result.turn);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "turns.start", error),
            }
        }
        TurnsSubcommand::Show(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service.show_turn(&command.turn) {
                Ok(turn) => {
                    if command.json {
                        print_json_success("turns.show", json!({ "turn": turn }))?;
                    } else {
                        print_turn(&turn);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "turns.show", error),
            }
        }
        TurnsSubcommand::Wait(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service
                .wait_for_turn(&command.turn, command.timeout_secs.map(Duration::from_secs))
            {
                Ok(turn) => {
                    if command.json {
                        print_json_success("turns.wait", json!({ "turn": turn }))?;
                    } else {
                        print_turn(&turn);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "turns.wait", error),
            }
        }
        TurnsSubcommand::Cancel(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service.cancel_turn(&command.turn) {
                Ok(turn) => {
                    if command.json {
                        print_json_success("turns.cancel", json!({ "turn": turn }))?;
                    } else {
                        print_turn(&turn);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "turns.cancel", error),
            }
        }
        TurnsSubcommand::Worker(command) => {
            if std::env::var_os("CODEX_SWITCH_TEST_WORKER_EXIT_BEFORE_START").is_some() {
                return Err(AppError::RuntimeUnavailable {
                    message: "worker exit requested by test".to_string(),
                });
            }
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            let worker: SessionControlWorker = service.worker();
            worker.run(&command.turn_id, &command.lease_owner_id)
        }
    }
}

pub fn run_handoffs(command: HandoffsCommand) -> Result<()> {
    match command.command {
        HandoffsSubcommand::Prepare(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service.prepare_handoff(HandoffPrepareRequest {
                session_id: command.session,
                to_identity_name: command.to_identity,
                reason: command.reason,
                idempotency_key: command.idempotency_key,
            }) {
                Ok(handoff) => {
                    if command.json {
                        print_json_success("handoffs.prepare", json!({ "handoff": handoff }))?;
                    } else {
                        print_handoff(&handoff);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "handoffs.prepare", error),
            }
        }
        HandoffsSubcommand::Accept(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service.accept_handoff(HandoffAcceptRequest {
                handoff_id: command.handoff,
            }) {
                Ok(handoff) => {
                    if command.json {
                        print_json_success("handoffs.accept", json!({ "handoff": handoff }))?;
                    } else {
                        print_handoff(&handoff);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "handoffs.accept", error),
            }
        }
        HandoffsSubcommand::Confirm(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service.confirm_handoff(HandoffConfirmRequest {
                handoff_id: command.handoff,
                observed_turn_id: command.observed_turn_id,
                fallback: command.fallback.map(|fallback| match fallback {
                    HandoffFallbackArg::CheckpointFallback => {
                        HandoffConfirmFallback::CheckpointFallback
                    }
                }),
            }) {
                Ok(handoff) => {
                    if command.json {
                        print_json_success("handoffs.confirm", json!({ "handoff": handoff }))?;
                    } else {
                        print_handoff(&handoff);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "handoffs.confirm", error),
            }
        }
        HandoffsSubcommand::Show(command) => {
            let base_root = resolve_base_root(command.base_root.as_deref())?;
            let service = SessionControlService::new(&base_root)?;
            match service.show_handoff(&command.handoff) {
                Ok(handoff) => {
                    if command.json {
                        print_json_success("handoffs.show", json!({ "handoff": handoff }))?;
                    } else {
                        print_handoff(&handoff);
                    }
                    Ok(())
                }
                Err(error) => maybe_print_json_error(command.json, "handoffs.show", error),
            }
        }
    }
}

fn load_prompt(prompt: Option<String>, prompt_file: Option<PathBuf>) -> Result<Option<String>> {
    match (prompt, prompt_file) {
        (Some(_), Some(_)) => Err(AppError::TaskPromptConflict),
        (Some(prompt), None) => Ok(Some(prompt)),
        (None, Some(path)) => Ok(Some(fs::read_to_string(resolve_path(&path)?)?)),
        (None, None) => Ok(None),
    }
}

fn resolve_workspace(path: Option<&Path>) -> Result<PathBuf> {
    match path {
        Some(path) => resolve_path(path),
        None => std::env::current_dir()
            .map_err(|source| AppError::CurrentDirectoryUnavailable { source }),
    }
}

fn resolve_base_root(path: Option<&Path>) -> Result<PathBuf> {
    match path {
        Some(path) => resolve_path(path),
        None => default_base_root(),
    }
}

fn maybe_print_json_error(json: bool, command: &str, error: AppError) -> Result<()> {
    if json {
        print_json_failure(command, &error)?;
        Err(AppError::JsonFailureRendered)
    } else {
        Err(error)
    }
}

fn print_json_success(command: &str, data: Value) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string(&json!({
            "interface_version": INTERFACE_VERSION,
            "ok": true,
            "command": command,
            "data": data,
        }))?
    );
    Ok(())
}

fn print_json_failure(command: &str, error: &AppError) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string(&json!({
            "interface_version": INTERFACE_VERSION,
            "ok": false,
            "command": command,
            "error": {
                "code": error_code(error),
                "message": error.to_string(),
                "retryable": is_retryable(error),
                "details": error_details(error),
            }
        }))?
    );
    Ok(())
}

fn is_retryable(error: &AppError) -> bool {
    matches!(
        error,
        AppError::NoSelectableIdentity
            | AppError::RuntimeUnavailable { .. }
            | AppError::RpcTimeout { .. }
            | AppError::RpcServer { .. }
            | AppError::AppServerExited { .. }
            | AppError::ThreadLeaseHeld { .. }
            | AppError::ThreadLeaseStateConflict { .. }
    )
}

fn error_details(error: &AppError) -> Value {
    match error {
        AppError::IdentityNotFound { identity_id } => json!({ "identity_id": identity_id }),
        AppError::SessionNotFound { session_id } => json!({ "session_id": session_id }),
        AppError::SessionTurnNotFound { turn_id } => json!({ "turn_id": turn_id }),
        AppError::SessionHandoffNotFound { handoff_id } => json!({ "handoff_id": handoff_id }),
        AppError::SessionTurnAlreadyActive {
            session_id,
            active_turn_suffix,
        } => json!({
            "session_id": session_id,
            "active_turn_suffix": active_turn_suffix,
        }),
        AppError::SessionHandoffPending {
            session_id,
            handoff_id,
        } => json!({
            "session_id": session_id,
            "handoff_id": handoff_id,
        }),
        AppError::UnsafeSameThreadResume {
            session_id,
            current_identity_id,
            requested_identity_id,
        } => json!({
            "session_id": session_id,
            "current_identity_id": current_identity_id,
            "requested_identity_id": requested_identity_id,
        }),
        AppError::CheckpointFallbackRequired {
            session_id,
            handoff_id,
        } => json!({
            "session_id": session_id,
            "handoff_id": handoff_id,
        }),
        AppError::ThreadLeaseHeld {
            thread_id,
            owner_identity_id,
        } => json!({
            "thread_id": thread_id,
            "owner_identity_id": owner_identity_id,
        }),
        AppError::ThreadLeaseStateConflict {
            thread_id,
            expected,
            actual,
        } => json!({
            "thread_id": thread_id,
            "expected": expected,
            "actual": actual,
        }),
        AppError::RpcServer { method, code, .. } => json!({
            "method": method,
            "rpc_code": code,
        }),
        AppError::RpcTimeout { method, timeout } => json!({
            "method": method,
            "timeout_secs": timeout.as_secs(),
        }),
        _ => json!({}),
    }
}

fn print_session_snapshot(snapshot: &crate::session_control::SessionSnapshot) {
    println!("session id: {}", snapshot.session.session_id);
    println!("topic key: {}", snapshot.session.topic_key);
    println!("thread id: {}", snapshot.session.thread_id);
    println!("identity: {}", snapshot.session.current_identity_name);
    println!("status: {}", snapshot.session.status.as_str());
    println!(
        "continuity mode: {}",
        snapshot.session.continuity_mode.as_str()
    );
    println!(
        "safe to continue: {}",
        yes_no(snapshot.session.safe_to_continue)
    );
    if let Some(turn) = snapshot.active_turn.as_ref() {
        println!("active turn: {} ({})", turn.turn_id, turn.status.as_str());
    }
    if let Some(handoff) = snapshot.pending_handoff.as_ref() {
        println!(
            "pending handoff: {} ({})",
            handoff.handoff_id,
            handoff.status.as_str()
        );
    }
}

fn print_turn<T>(turn: &T)
where
    T: Serialize,
{
    println!("{}", serde_json::to_string_pretty(turn).unwrap_or_default());
}

fn print_handoff<T>(handoff: &T)
where
    T: Serialize,
{
    println!(
        "{}",
        serde_json::to_string_pretty(handoff).unwrap_or_default()
    );
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}
