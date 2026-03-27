use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use serde_json::{json, Value};

use crate::automatic_selection::AutomaticSelectionService;
use crate::codex_rpc::{AppServerCommand, AppServerSession, CodexAppServerVerifier};
use crate::domain::checkpoint::TaskCheckpoint;
use crate::domain::identity::{current_timestamp, CodexIdentity, IdentityId};
use crate::error::{AppError, Result};
use crate::handoff::HandoffService;
use crate::identity_selection::IdentitySelectionService;
use crate::session_control::domain::{
    ContinuityMode, HandoffFallbackMode, HandoffLeaseStateKind, SessionHandoffId,
    SessionHandoffRecord, SessionHandoffStatus, SessionId, SessionRecord, SessionSnapshot,
    SessionStatus, SessionTurnId, SessionTurnRecord, SessionTurnStatus,
};
use crate::session_control::store::{AppendEvent, SessionControlStore};
use crate::storage::checkpoint_store::{JsonTaskCheckpointStore, TaskCheckpointStore};
use crate::storage::health_store::JsonIdentityHealthStore;
use crate::storage::policy_store::JsonSelectionPolicyStore;
use crate::storage::quota_store::JsonQuotaStore;
use crate::storage::registry_store::{JsonRegistryStore, RegistryStore};
use crate::storage::selection_event_store::JsonSelectionEventStore;
use crate::storage::selection_store::JsonSelectionStore;
use crate::thread_leases::ThreadLeaseManager;

const STARTUP_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
const WORKER_LEASE_TTL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct SessionStartRequest {
    pub topic_key: String,
    pub prompt_text: Option<String>,
    pub workspace_root: PathBuf,
    pub identity_name: Option<String>,
    pub auto: bool,
    pub cached: bool,
    pub model: Option<String>,
    pub idempotency_key: Option<String>,
    pub max_runtime_secs: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SessionTurnStartRequest {
    pub session_id: String,
    pub prompt_text: String,
    pub identity_name: Option<String>,
    pub auto: bool,
    pub cached: bool,
    pub idempotency_key: Option<String>,
    pub allow_checkpoint_fallback: bool,
    pub max_runtime_secs: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ResumeSessionRequest {
    pub session_id: String,
    pub prompt_text: Option<String>,
    pub identity_name: Option<String>,
    pub auto: bool,
    pub cached: bool,
    pub idempotency_key: Option<String>,
    pub allow_checkpoint_fallback: bool,
    pub max_runtime_secs: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SessionStreamRequest {
    pub session_id: String,
    pub after_sequence_no: i64,
}

#[derive(Debug, Clone)]
pub struct HandoffPrepareRequest {
    pub session_id: String,
    pub to_identity_name: String,
    pub reason: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HandoffAcceptRequest {
    pub handoff_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoffConfirmFallback {
    CheckpointFallback,
}

#[derive(Debug, Clone)]
pub struct HandoffConfirmRequest {
    pub handoff_id: String,
    pub observed_turn_id: Option<String>,
    pub fallback: Option<HandoffConfirmFallback>,
}

#[derive(Debug, Clone)]
pub struct TurnStartResult {
    pub session: SessionSnapshot,
    pub turn: SessionTurnRecord,
    pub continuity_mode: ContinuityMode,
}

#[derive(Debug, Clone)]
pub struct SessionStartResult {
    pub session: SessionSnapshot,
    pub started_turn: Option<SessionTurnRecord>,
}

#[derive(Debug, Clone)]
pub struct SessionControlService {
    base_root: PathBuf,
    worker_program: PathBuf,
    app_server_command: AppServerCommand,
    timeout: Duration,
    worker_heartbeat_interval: Duration,
    worker_lease_ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct SessionControlWorker {
    base_root: PathBuf,
    app_server_command: AppServerCommand,
    timeout: Duration,
    worker_heartbeat_interval: Duration,
    worker_lease_ttl: Duration,
}

#[derive(Debug, Clone)]
struct LeaseHandle {
    thread_id: String,
    token: String,
    persist_on_finish: bool,
}

#[derive(Debug, Clone)]
enum WorkerOutcome {
    Completed,
    Failed(String, String),
    TimedOut(String),
    Canceled,
}

#[derive(Debug, Clone)]
struct TerminalTurnUpdate {
    status: SessionTurnStatus,
    failure_kind: Option<String>,
    failure_message: Option<String>,
}

impl SessionControlService {
    pub fn new(base_root: &Path) -> Result<Self> {
        Ok(Self {
            base_root: base_root.to_path_buf(),
            worker_program: worker_program_path()?,
            app_server_command: AppServerCommand::default(),
            timeout: Duration::from_secs(30),
            worker_heartbeat_interval: WORKER_HEARTBEAT_INTERVAL,
            worker_lease_ttl: WORKER_LEASE_TTL,
        })
    }

    pub fn with_app_server_command(
        base_root: &Path,
        app_server_command: AppServerCommand,
    ) -> Result<Self> {
        Ok(Self {
            base_root: base_root.to_path_buf(),
            worker_program: worker_program_path()?,
            app_server_command,
            timeout: Duration::from_secs(30),
            worker_heartbeat_interval: WORKER_HEARTBEAT_INTERVAL,
            worker_lease_ttl: WORKER_LEASE_TTL,
        })
    }

    pub fn worker(&self) -> SessionControlWorker {
        SessionControlWorker {
            base_root: self.base_root.clone(),
            app_server_command: self.app_server_command.clone(),
            timeout: self.timeout,
            worker_heartbeat_interval: self.worker_heartbeat_interval,
            worker_lease_ttl: self.worker_lease_ttl,
        }
    }

    pub fn start_session(&self, request: SessionStartRequest) -> Result<SessionStartResult> {
        if request.topic_key.trim().is_empty() {
            return Err(AppError::InvalidSessionControlState {
                message: "topic_key must not be empty".to_string(),
            });
        }
        let mut store = SessionControlStore::open(&self.base_root)?;
        if let Some(idempotency_key) = request.idempotency_key.as_deref() {
            if let Some(existing) = store.load_session_by_idempotency(idempotency_key)? {
                let session = self.show_session(existing.session_id.as_str())?;
                let started_turn = if request.prompt_text.is_some() {
                    store.find_turn_by_session_idempotency(
                        existing.session_id.as_str(),
                        idempotency_key,
                    )?
                } else {
                    None
                };
                return Ok(SessionStartResult {
                    session,
                    started_turn,
                });
            }
        }
        if let Some(existing) = store.load_session_by_topic_key(&request.topic_key)? {
            return Err(AppError::InvalidSessionControlState {
                message: format!("topic_key {} already exists", existing.topic_key),
            });
        }
        let identity = self.resolve_launch_identity(
            request.identity_name.as_deref(),
            request.auto,
            request.cached,
        )?;
        let workspace_root = request.workspace_root.to_string_lossy().into_owned();
        let mut session = AppServerSession::connect(
            &self.app_server_command,
            &identity.codex_home,
            self.timeout,
        )?;
        let thread_id = start_thread(
            &mut session,
            &request.workspace_root,
            request.model.as_deref(),
            &identity,
        )?;
        let now = current_timestamp()?;
        let record = SessionRecord {
            session_id: SessionId::new(),
            topic_key: request.topic_key,
            workspace_root,
            model: request.model,
            thread_id: thread_id.clone(),
            current_identity_id: identity.id.clone(),
            current_identity_name: identity.display_name.clone(),
            status: SessionStatus::Idle,
            last_turn_id: None,
            active_turn_id: None,
            continuity_mode: ContinuityMode::SameThread,
            safe_to_continue: true,
            pending_handoff_id: None,
            last_checkpoint_id: None,
            created_at: now,
            updated_at: now,
        };
        store.create_session(&record, request.idempotency_key.as_deref())?;
        let _ = store.append_event(AppendEvent {
            session_id: &record.session_id,
            thread_id: &record.thread_id,
            turn_id: None,
            runtime_turn_id: None,
            handoff_id: None,
            event: "session.started",
            timestamp: now,
            payload: &json!({
                "topic_key": record.topic_key,
                "identity_id": record.current_identity_id,
                "identity_name": record.current_identity_name,
                "workspace_root": record.workspace_root,
            }),
        })?;
        let mut snapshot = store
            .load_session_snapshot(record.session_id.as_str())?
            .ok_or_else(|| AppError::SessionNotFound {
                session_id: record.session_id.to_string(),
            })?;
        let started_turn = match request.prompt_text {
            Some(prompt_text) => {
                let result = self.start_turn(SessionTurnStartRequest {
                    session_id: snapshot.session.session_id.to_string(),
                    prompt_text,
                    identity_name: Some(identity.display_name),
                    auto: false,
                    cached: request.cached,
                    idempotency_key: request.idempotency_key,
                    allow_checkpoint_fallback: false,
                    max_runtime_secs: request.max_runtime_secs,
                })?;
                snapshot = result.session;
                Some(result.turn)
            }
            None => None,
        };
        Ok(SessionStartResult {
            session: snapshot,
            started_turn,
        })
    }

    pub fn resume_session(&self, request: ResumeSessionRequest) -> Result<TurnStartResult> {
        let _ = self.show_session(&request.session_id)?;
        let prompt_text =
            request
                .prompt_text
                .ok_or_else(|| AppError::InvalidSessionControlState {
                    message: "sessions resume currently requires --prompt or --prompt-file"
                        .to_string(),
                })?;
        self.start_turn(SessionTurnStartRequest {
            session_id: request.session_id,
            prompt_text,
            identity_name: request.identity_name,
            auto: request.auto,
            cached: request.cached,
            idempotency_key: request.idempotency_key,
            allow_checkpoint_fallback: request.allow_checkpoint_fallback,
            max_runtime_secs: request.max_runtime_secs,
        })
    }

    pub fn show_session(&self, session_id: &str) -> Result<SessionSnapshot> {
        let store = SessionControlStore::open(&self.base_root)?;
        let snapshot =
            store
                .load_session_snapshot(session_id)?
                .ok_or_else(|| AppError::SessionNotFound {
                    session_id: session_id.to_string(),
                })?;
        self.apply_runtime_safety(snapshot)
    }

    pub fn list_sessions(&self) -> Result<Vec<SessionSnapshot>> {
        let store = SessionControlStore::open(&self.base_root)?;
        let sessions = store.list_sessions()?;
        let mut snapshots = Vec::new();
        for session in sessions {
            if let Some(snapshot) = store.load_session_snapshot(session.session_id.as_str())? {
                snapshots.push(self.apply_runtime_safety(snapshot)?);
            }
        }
        Ok(snapshots)
    }

    pub fn session_events(
        &self,
        request: SessionStreamRequest,
    ) -> Result<Vec<crate::session_control::domain::SessionEventRecord>> {
        let store = SessionControlStore::open(&self.base_root)?;
        let session =
            store
                .load_session(&request.session_id)?
                .ok_or_else(|| AppError::SessionNotFound {
                    session_id: request.session_id.clone(),
                })?;
        store.events_after(&session.session_id, request.after_sequence_no)
    }

    pub fn start_turn(&self, request: SessionTurnStartRequest) -> Result<TurnStartResult> {
        let mut store = SessionControlStore::open(&self.base_root)?;
        let snapshot = self.apply_runtime_safety(
            store
                .load_session_snapshot(&request.session_id)?
                .ok_or_else(|| AppError::SessionNotFound {
                    session_id: request.session_id.clone(),
                })?,
        )?;
        if let Some(key) = request.idempotency_key.as_deref() {
            if let Some(existing) =
                store.find_turn_by_session_idempotency(snapshot.session.session_id.as_str(), key)?
            {
                let session = self.show_session(snapshot.session.session_id.as_str())?;
                return Ok(TurnStartResult {
                    session,
                    continuity_mode: existing.continuity_mode,
                    turn: existing,
                });
            }
        }
        if let Some(active_turn_id) = snapshot.session.active_turn_id.as_ref() {
            return Err(AppError::SessionTurnAlreadyActive {
                session_id: snapshot.session.session_id.to_string(),
                active_turn_suffix: format!(" ({active_turn_id})"),
            });
        }
        let desired_identity = self.resolve_turn_identity(
            &snapshot.session,
            request.identity_name.as_deref(),
            request.auto,
            request.cached,
        )?;
        let decision = self.turn_decision(
            &snapshot,
            &desired_identity,
            request.allow_checkpoint_fallback,
        )?;
        let now = current_timestamp()?;
        let turn = SessionTurnRecord {
            turn_id: SessionTurnId::new(),
            session_id: snapshot.session.session_id.clone(),
            thread_id: snapshot.session.thread_id.clone(),
            identity_id: desired_identity.id.clone(),
            identity_name: desired_identity.display_name.clone(),
            status: SessionTurnStatus::Starting,
            prompt_text: request.prompt_text,
            continuity_mode: decision,
            runtime_turn_id: None,
            started_at: None,
            finished_at: None,
            failure_kind: None,
            failure_message: None,
            worker_owner_id: None,
            worker_pid: None,
            heartbeat_at: None,
            heartbeat_expires_at: None,
            cancel_requested: false,
            lease_thread_id: None,
            lease_token: None,
            lease_persist_on_finish: decision == ContinuityMode::Handoff,
            idempotency_key: request.idempotency_key,
            created_at: now,
            updated_at: now,
            max_runtime_secs: request.max_runtime_secs,
        };
        store.create_turn_and_activate_session(&turn, SessionStatus::Running)?;
        let _ = store.append_event(AppendEvent {
            session_id: &snapshot.session.session_id,
            thread_id: &snapshot.session.thread_id,
            turn_id: Some(&turn.turn_id),
            runtime_turn_id: None,
            handoff_id: snapshot.session.pending_handoff_id.as_ref(),
            event: "session.status.changed",
            timestamp: now,
            payload: &json!({
                "status": SessionStatus::Running.as_str(),
                "continuity_mode": turn.continuity_mode.as_str(),
            }),
        })?;
        let worker_owner_id = format!("session-worker-{}", turn.turn_id);
        let worker_pid = match self.spawn_worker(turn.turn_id.as_str(), &worker_owner_id) {
            Ok(worker_pid) => worker_pid,
            Err(error) => {
                self.finalize_startup_failure(&mut store, &snapshot.session, &turn, &error)?;
                return Err(error);
            }
        };
        let expires_at = now + self.worker_lease_ttl.as_secs() as i64;
        if let Err(error) = store.mark_turn_worker_spawned(
            turn.turn_id.as_str(),
            &worker_owner_id,
            worker_pid,
            now,
            expires_at,
        ) {
            let _ = terminate_process_group(worker_pid);
            self.finalize_startup_failure(&mut store, &snapshot.session, &turn, &error)?;
            return Err(error);
        }
        let started_turn = self.wait_for_turn_start(turn.turn_id.as_str())?;
        if started_turn.runtime_turn_id.is_none() {
            if started_turn.status.is_terminal() {
                return Err(startup_error_from_turn(&started_turn));
            }
            let error = AppError::RuntimeUnavailable {
                message: format!(
                    "turn {} did not start within {:?}",
                    started_turn.turn_id, STARTUP_WAIT_TIMEOUT
                ),
            };
            self.finalize_startup_failure(&mut store, &snapshot.session, &started_turn, &error)?;
            return Err(error);
        }
        let session = self.show_session(snapshot.session.session_id.as_str())?;
        Ok(TurnStartResult {
            session,
            continuity_mode: started_turn.continuity_mode,
            turn: started_turn,
        })
    }

    pub fn show_turn(&self, turn_id: &str) -> Result<SessionTurnRecord> {
        let store = SessionControlStore::open(&self.base_root)?;
        store
            .load_turn(turn_id)?
            .ok_or_else(|| AppError::SessionTurnNotFound {
                turn_id: turn_id.to_string(),
            })
    }

    pub fn wait_for_turn(
        &self,
        turn_id: &str,
        timeout: Option<Duration>,
    ) -> Result<SessionTurnRecord> {
        let started = Instant::now();
        loop {
            let turn = self.show_turn(turn_id)?;
            if turn.status.is_terminal() {
                return Ok(turn);
            }
            if timeout.is_some_and(|timeout| started.elapsed() >= timeout) {
                return Err(AppError::RpcTimeout {
                    method: "turns.wait".to_string(),
                    timeout: timeout.expect("checked timeout"),
                });
            }
            thread::sleep(Duration::from_millis(200));
        }
    }

    pub fn cancel_turn(&self, turn_id: &str) -> Result<SessionTurnRecord> {
        let mut store = SessionControlStore::open(&self.base_root)?;
        let mut turn = store
            .load_turn(turn_id)?
            .ok_or_else(|| AppError::SessionTurnNotFound {
                turn_id: turn_id.to_string(),
            })?;
        if turn.status.is_terminal() {
            return Ok(turn);
        }
        let mut session = store
            .load_session(&turn.session_id.to_string())?
            .ok_or_else(|| AppError::SessionNotFound {
                session_id: turn.session_id.to_string(),
            })?;
        if let Some(pid) = turn.worker_pid {
            let _ = terminate_process_group(pid);
        }
        if !turn.lease_persist_on_finish {
            self.release_turn_lease(&turn)?;
        }
        let now = current_timestamp()?;
        turn.status = SessionTurnStatus::Canceled;
        turn.failure_kind = Some("canceled".to_string());
        turn.failure_message = Some("turn canceled".to_string());
        turn.finished_at = Some(now);
        turn.updated_at = now;
        turn.cancel_requested = true;
        session.active_turn_id = None;
        session.last_turn_id = Some(turn.turn_id.clone());
        session.updated_at = now;
        apply_terminal_session_state(&store, &mut session, &turn, SessionTurnStatus::Canceled)?;
        store.save_session_and_turn(&session, &turn)?;
        let _ = store.append_event(AppendEvent {
            session_id: &session.session_id,
            thread_id: &session.thread_id,
            turn_id: Some(&turn.turn_id),
            runtime_turn_id: turn.runtime_turn_id.as_deref(),
            handoff_id: session.pending_handoff_id.as_ref(),
            event: "turn.canceled",
            timestamp: now,
            payload: &json!({
                "status": turn.status.as_str(),
            }),
        })?;
        Ok(turn)
    }

    pub fn cancel_session(&self, session_id: &str) -> Result<SessionSnapshot> {
        let mut store = SessionControlStore::open(&self.base_root)?;
        let mut snapshot =
            store
                .load_session_snapshot(session_id)?
                .ok_or_else(|| AppError::SessionNotFound {
                    session_id: session_id.to_string(),
                })?;
        if let Some(active_turn) = snapshot.active_turn.as_ref() {
            let _ = self.cancel_turn(active_turn.turn_id.as_str())?;
            snapshot = store.load_session_snapshot(session_id)?.ok_or_else(|| {
                AppError::SessionNotFound {
                    session_id: session_id.to_string(),
                }
            })?;
        }
        let now = current_timestamp()?;
        if let Some(mut handoff) = snapshot.pending_handoff.clone() {
            self.release_or_abort_handoff_lease(&handoff)?;
            handoff.status = SessionHandoffStatus::Aborted;
            handoff.updated_at = now;
            snapshot.pending_handoff = Some(handoff.clone());
            snapshot.session.pending_handoff_id = None;
            store.save_handoff(&handoff)?;
        }
        snapshot.session.status = SessionStatus::Canceled;
        snapshot.session.safe_to_continue = false;
        snapshot.session.continuity_mode = ContinuityMode::SameThread;
        snapshot.session.updated_at = now;
        store.save_session(&snapshot.session)?;
        let _ = store.append_event(AppendEvent {
            session_id: &snapshot.session.session_id,
            thread_id: &snapshot.session.thread_id,
            turn_id: None,
            runtime_turn_id: None,
            handoff_id: snapshot
                .pending_handoff
                .as_ref()
                .map(|handoff| &handoff.handoff_id),
            event: "session.status.changed",
            timestamp: now,
            payload: &json!({
                "status": SessionStatus::Canceled.as_str(),
            }),
        })?;
        self.show_session(session_id)
    }

    pub fn prepare_handoff(&self, request: HandoffPrepareRequest) -> Result<SessionHandoffRecord> {
        let HandoffPrepareRequest {
            session_id,
            to_identity_name,
            reason,
            idempotency_key,
        } = request;
        let mut store = SessionControlStore::open(&self.base_root)?;
        let snapshot = self.show_session(&session_id)?;
        if let Some(idempotency_key) = idempotency_key.as_deref() {
            if let Some(existing) = store.load_handoff_by_session_idempotency(
                snapshot.session.session_id.as_str(),
                idempotency_key,
            )? {
                return Ok(existing);
            }
        }
        if snapshot.session.active_turn_id.is_some() {
            return Err(AppError::SessionTurnAlreadyActive {
                session_id: snapshot.session.session_id.to_string(),
                active_turn_suffix: String::new(),
            });
        }
        if let Some(pending) = snapshot.pending_handoff {
            return Err(AppError::SessionHandoffPending {
                session_id: snapshot.session.session_id.to_string(),
                handoff_id: pending.handoff_id.to_string(),
            });
        }
        let target = self.resolve_identity_by_name(&to_identity_name)?;
        if target.id == snapshot.session.current_identity_id {
            return Err(AppError::InvalidSessionControlState {
                message: "handoff target must differ from current session identity".to_string(),
            });
        }
        let source = self.resolve_identity_by_id(&snapshot.session.current_identity_id)?;
        let service = self.handoff_service();
        let source_lease =
            service.acquire_lease(&source.display_name, &snapshot.session.thread_id)?;
        match service.prepare_handoff(
            &snapshot.session.thread_id,
            &source.display_name,
            &target.display_name,
            &source_lease.lease_token,
            &reason,
        ) {
            Ok(preparation) => {
                let now = current_timestamp()?;
                let record = SessionHandoffRecord {
                    handoff_id: SessionHandoffId::new(),
                    session_id: snapshot.session.session_id.clone(),
                    thread_id: snapshot.session.thread_id.clone(),
                    from_identity_id: source.id.clone(),
                    from_identity_name: source.display_name.clone(),
                    to_identity_id: target.id,
                    to_identity_name: target.display_name,
                    status: SessionHandoffStatus::Prepared,
                    lease_token: preparation.lease.lease_token,
                    lease_owner_identity_id: source.id.clone(),
                    lease_owner_identity_name: source.display_name.clone(),
                    lease_state_kind: HandoffLeaseStateKind::HandoffPending,
                    reason: reason.clone(),
                    baseline_turn_id: preparation.baseline_snapshot.latest_turn_id,
                    observed_turn_id: None,
                    fallback_mode: None,
                    created_at: now,
                    updated_at: now,
                };
                if let Err(error) = store.create_handoff_and_update_session(
                    &record,
                    idempotency_key.as_deref(),
                    SessionStatus::HandoffPending,
                    ContinuityMode::Handoff,
                    false,
                ) {
                    let _ = ThreadLeaseManager::with_default_locking(&self.base_root)
                        .abort_handoff(
                            &record.thread_id,
                            &record.lease_owner_identity_id,
                            &record.lease_token,
                        );
                    return Err(error);
                }
                let _ = store.append_event(AppendEvent {
                    session_id: &snapshot.session.session_id,
                    thread_id: &snapshot.session.thread_id,
                    turn_id: None,
                    runtime_turn_id: None,
                    handoff_id: Some(&record.handoff_id),
                    event: "handoff.prepared",
                    timestamp: now,
                    payload: &json!({
                        "from_identity_id": record.from_identity_id,
                        "to_identity_id": record.to_identity_id,
                        "reason": record.reason,
                    }),
                })?;
                Ok(record)
            }
            Err(error) if is_checkpoint_fallback_error(&error) => {
                let checkpoint = TaskCheckpoint::new(
                    &service.inspect_thread(&source.display_name, &snapshot.session.thread_id)?,
                    source.id.clone(),
                    target.id.clone(),
                    crate::domain::checkpoint::CheckpointMode::ResumeViaCheckpoint,
                    &reason,
                    Some(error.to_string()),
                )?;
                JsonTaskCheckpointStore::new(&self.base_root).save(&checkpoint)?;
                let now = current_timestamp()?;
                let record = SessionHandoffRecord {
                    handoff_id: SessionHandoffId::new(),
                    session_id: snapshot.session.session_id.clone(),
                    thread_id: snapshot.session.thread_id.clone(),
                    from_identity_id: source.id.clone(),
                    from_identity_name: source.display_name.clone(),
                    to_identity_id: target.id.clone(),
                    to_identity_name: target.display_name.clone(),
                    status: SessionHandoffStatus::FallbackRequired,
                    lease_token: source_lease.lease_token.clone(),
                    lease_owner_identity_id: source.id.clone(),
                    lease_owner_identity_name: source.display_name.clone(),
                    lease_state_kind: HandoffLeaseStateKind::Active,
                    reason: reason.clone(),
                    baseline_turn_id: checkpoint.latest_turn_id.clone(),
                    observed_turn_id: None,
                    fallback_mode: Some(HandoffFallbackMode::CheckpointFallback),
                    created_at: now,
                    updated_at: now,
                };
                let mut session = snapshot.session;
                session.status = SessionStatus::Blocked;
                session.safe_to_continue = false;
                session.continuity_mode = ContinuityMode::CheckpointFallback;
                session.pending_handoff_id = Some(record.handoff_id.clone());
                session.last_checkpoint_id = Some(checkpoint.id);
                session.updated_at = now;
                if let Err(error) = store.create_handoff_and_update_session(
                    &record,
                    idempotency_key.as_deref(),
                    session.status,
                    session.continuity_mode,
                    session.safe_to_continue,
                ) {
                    let _ = ThreadLeaseManager::with_default_locking(&self.base_root).release(
                        &record.thread_id,
                        &record.lease_owner_identity_id,
                        &record.lease_token,
                    );
                    return Err(error);
                }
                store.save_session(&session)?;
                let _ = store.append_event(AppendEvent {
                    session_id: &session.session_id,
                    thread_id: &session.thread_id,
                    turn_id: None,
                    runtime_turn_id: None,
                    handoff_id: Some(&record.handoff_id),
                    event: "handoff.fallback_required",
                    timestamp: now,
                    payload: &json!({
                        "reason": error.to_string(),
                        "fallback_mode": HandoffFallbackMode::CheckpointFallback.as_str(),
                    }),
                })?;
                Ok(record)
            }
            Err(error) => {
                let _ = ThreadLeaseManager::with_default_locking(&self.base_root).release(
                    &snapshot.session.thread_id,
                    &source.id,
                    &source_lease.lease_token,
                );
                Err(error)
            }
        }
    }

    pub fn accept_handoff(&self, request: HandoffAcceptRequest) -> Result<SessionHandoffRecord> {
        let mut store = SessionControlStore::open(&self.base_root)?;
        let mut handoff = store.load_handoff(&request.handoff_id)?.ok_or_else(|| {
            AppError::SessionHandoffNotFound {
                handoff_id: request.handoff_id.clone(),
            }
        })?;
        if handoff.status != SessionHandoffStatus::Prepared {
            return Ok(handoff);
        }
        let service = self.handoff_service();
        let acceptance = service.accept_handoff(
            &handoff.thread_id,
            &handoff.to_identity_name,
            &handoff.lease_token,
        )?;
        let now = current_timestamp()?;
        let mut session = store
            .load_session(&handoff.session_id.to_string())?
            .ok_or_else(|| AppError::SessionNotFound {
                session_id: handoff.session_id.to_string(),
            })?;
        handoff.status = SessionHandoffStatus::Accepted;
        handoff.lease_token = acceptance.lease.lease_token;
        handoff.lease_owner_identity_id = handoff.to_identity_id.clone();
        handoff.lease_owner_identity_name = handoff.to_identity_name.clone();
        handoff.lease_state_kind = HandoffLeaseStateKind::Active;
        handoff.updated_at = now;
        session.status = SessionStatus::HandoffReady;
        session.continuity_mode = ContinuityMode::Handoff;
        session.safe_to_continue = false;
        session.current_identity_id = handoff.to_identity_id.clone();
        session.current_identity_name = handoff.to_identity_name.clone();
        session.updated_at = now;
        store.save_session_and_handoff(&session, &handoff)?;
        let _ = store.append_event(AppendEvent {
            session_id: &session.session_id,
            thread_id: &session.thread_id,
            turn_id: None,
            runtime_turn_id: None,
            handoff_id: Some(&handoff.handoff_id),
            event: "handoff.accepted",
            timestamp: now,
            payload: &json!({
                "to_identity_id": handoff.to_identity_id,
            }),
        })?;
        Ok(handoff)
    }

    pub fn confirm_handoff(&self, request: HandoffConfirmRequest) -> Result<SessionHandoffRecord> {
        let mut store = SessionControlStore::open(&self.base_root)?;
        let mut handoff = store.load_handoff(&request.handoff_id)?.ok_or_else(|| {
            AppError::SessionHandoffNotFound {
                handoff_id: request.handoff_id.clone(),
            }
        })?;
        let mut session = store
            .load_session(&handoff.session_id.to_string())?
            .ok_or_else(|| AppError::SessionNotFound {
                session_id: handoff.session_id.to_string(),
            })?;
        let now = current_timestamp()?;
        match request.fallback {
            Some(HandoffConfirmFallback::CheckpointFallback) => {
                self.release_or_abort_handoff_lease(&handoff)?;
                handoff.status = SessionHandoffStatus::FallbackRequired;
                handoff.fallback_mode = Some(HandoffFallbackMode::CheckpointFallback);
                handoff.updated_at = now;
                session.current_identity_id = handoff.to_identity_id.clone();
                session.current_identity_name = handoff.to_identity_name.clone();
                session.status = SessionStatus::WaitingForFollowup;
                session.continuity_mode = ContinuityMode::CheckpointFallback;
                session.safe_to_continue = true;
                session.pending_handoff_id = None;
                session.updated_at = now;
                store.save_session_and_handoff(&session, &handoff)?;
                let _ = store.append_event(AppendEvent {
                    session_id: &session.session_id,
                    thread_id: &session.thread_id,
                    turn_id: None,
                    runtime_turn_id: None,
                    handoff_id: Some(&handoff.handoff_id),
                    event: "handoff.fallback_required",
                    timestamp: now,
                    payload: &json!({
                        "fallback_mode": HandoffFallbackMode::CheckpointFallback.as_str(),
                    }),
                })?;
                Ok(handoff)
            }
            None => {
                let confirmation = self.handoff_service().confirm_handoff(
                    &handoff.thread_id,
                    &handoff.to_identity_name,
                    &handoff.lease_token,
                    request.observed_turn_id.as_deref(),
                )?;
                self.release_or_abort_handoff_lease(&handoff)?;
                handoff.status = SessionHandoffStatus::Confirmed;
                handoff.observed_turn_id =
                    confirmation.matched_turn_id.or(request.observed_turn_id);
                handoff.updated_at = now;
                session.status = SessionStatus::WaitingForFollowup;
                session.continuity_mode = ContinuityMode::SameThread;
                session.safe_to_continue = true;
                session.pending_handoff_id = None;
                session.thread_id = handoff.thread_id.clone();
                session.updated_at = now;
                store.save_session_and_handoff(&session, &handoff)?;
                let _ = store.append_event(AppendEvent {
                    session_id: &session.session_id,
                    thread_id: &session.thread_id,
                    turn_id: None,
                    runtime_turn_id: None,
                    handoff_id: Some(&handoff.handoff_id),
                    event: "handoff.confirmed",
                    timestamp: now,
                    payload: &json!({
                        "observed_turn_id": handoff.observed_turn_id,
                    }),
                })?;
                Ok(handoff)
            }
        }
    }

    pub fn show_handoff(&self, handoff_id: &str) -> Result<SessionHandoffRecord> {
        let store = SessionControlStore::open(&self.base_root)?;
        store
            .load_handoff(handoff_id)?
            .ok_or_else(|| AppError::SessionHandoffNotFound {
                handoff_id: handoff_id.to_string(),
            })
    }

    fn wait_for_turn_start(&self, turn_id: &str) -> Result<SessionTurnRecord> {
        let started = Instant::now();
        loop {
            let turn = self.show_turn(turn_id)?;
            if turn.runtime_turn_id.is_some() || turn.status.is_terminal() {
                return Ok(turn);
            }
            if started.elapsed() >= STARTUP_WAIT_TIMEOUT {
                return Ok(turn);
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn spawn_worker(&self, turn_id: &str, worker_owner_id: &str) -> Result<u32> {
        let mut command = Command::new(&self.worker_program);
        command
            .arg("turns")
            .arg("worker")
            .arg("--turn-id")
            .arg(turn_id)
            .arg("--lease-owner-id")
            .arg(worker_owner_id)
            .arg("--base-root")
            .arg(&self.base_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(unix)]
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command
            .spawn()
            .map_err(|error| AppError::RuntimeUnavailable {
                message: error.to_string(),
            })?;
        Ok(child.id())
    }

    fn apply_runtime_safety(&self, mut snapshot: SessionSnapshot) -> Result<SessionSnapshot> {
        let lease = ThreadLeaseManager::with_default_locking(&self.base_root)
            .read(&snapshot.session.thread_id)?;
        snapshot.session.safe_to_continue = snapshot.session.active_turn_id.is_none()
            && snapshot.session.pending_handoff_id.is_none()
            && lease.as_ref().is_none_or(|lease| {
                lease.lease_state == crate::domain::thread::ThreadLeaseState::Released
            });
        Ok(snapshot)
    }

    fn turn_decision(
        &self,
        snapshot: &SessionSnapshot,
        desired_identity: &CodexIdentity,
        _allow_checkpoint_fallback: bool,
    ) -> Result<ContinuityMode> {
        if let Some(handoff) = snapshot.pending_handoff.as_ref() {
            return match handoff.status {
                SessionHandoffStatus::Prepared => Err(AppError::SessionHandoffPending {
                    session_id: snapshot.session.session_id.to_string(),
                    handoff_id: handoff.handoff_id.to_string(),
                }),
                SessionHandoffStatus::Accepted => {
                    if handoff.to_identity_id != desired_identity.id {
                        Err(AppError::SessionHandoffPending {
                            session_id: snapshot.session.session_id.to_string(),
                            handoff_id: handoff.handoff_id.to_string(),
                        })
                    } else {
                        Ok(ContinuityMode::Handoff)
                    }
                }
                SessionHandoffStatus::FallbackRequired => {
                    Err(AppError::CheckpointFallbackRequired {
                        session_id: snapshot.session.session_id.to_string(),
                        handoff_id: Some(handoff.handoff_id.to_string()),
                    })
                }
                SessionHandoffStatus::Confirmed
                | SessionHandoffStatus::Aborted
                | SessionHandoffStatus::Expired => Ok(ContinuityMode::SameThread),
            };
        }
        if snapshot.session.continuity_mode == ContinuityMode::CheckpointFallback {
            if desired_identity.id != snapshot.session.current_identity_id {
                return Err(AppError::UnsafeSameThreadResume {
                    session_id: snapshot.session.session_id.to_string(),
                    current_identity_id: snapshot.session.current_identity_id.clone(),
                    requested_identity_id: desired_identity.id.clone(),
                });
            }
            return Ok(ContinuityMode::CheckpointFallback);
        }
        if desired_identity.id != snapshot.session.current_identity_id {
            return Err(AppError::UnsafeSameThreadResume {
                session_id: snapshot.session.session_id.to_string(),
                current_identity_id: snapshot.session.current_identity_id.clone(),
                requested_identity_id: desired_identity.id.clone(),
            });
        }
        Ok(ContinuityMode::SameThread)
    }

    fn resolve_turn_identity(
        &self,
        session: &SessionRecord,
        explicit_identity_name: Option<&str>,
        auto: bool,
        cached: bool,
    ) -> Result<CodexIdentity> {
        if auto && explicit_identity_name.is_some() {
            return Err(AppError::ConflictingIdentityResolution);
        }
        if let Some(identity_name) = explicit_identity_name {
            return self.resolve_identity_by_name(identity_name);
        }
        if auto {
            return self.resolve_launch_identity(None, true, cached);
        }
        self.resolve_identity_by_id(&session.current_identity_id)
    }

    fn resolve_launch_identity(
        &self,
        explicit_identity_name: Option<&str>,
        auto: bool,
        cached: bool,
    ) -> Result<CodexIdentity> {
        if auto && explicit_identity_name.is_some() {
            return Err(AppError::ConflictingIdentityResolution);
        }
        let selection_service = IdentitySelectionService::new(
            JsonSelectionStore::new(&self.base_root),
            JsonRegistryStore::new(&self.base_root),
        );
        if let Some(identity_name) = explicit_identity_name {
            return selection_service.resolve_by_name(identity_name);
        }
        if auto {
            return Ok(self
                .automatic_selection_service()
                .select_for_new_session(cached, "selected automatically for session control")?
                .selected
                .identity);
        }
        match selection_service.current()? {
            Some(current)
                if current.selection.mode == crate::domain::selection::SelectionMode::Manual =>
            {
                Ok(current.identity)
            }
            _ => Ok(self
                .automatic_selection_service()
                .select_for_new_session(cached, "selected automatically for session control")?
                .selected
                .identity),
        }
    }

    fn resolve_identity_by_name(&self, identity_name: &str) -> Result<CodexIdentity> {
        IdentitySelectionService::new(
            JsonSelectionStore::new(&self.base_root),
            JsonRegistryStore::new(&self.base_root),
        )
        .resolve_by_name(identity_name)
    }

    fn resolve_identity_by_id(&self, identity_id: &IdentityId) -> Result<CodexIdentity> {
        JsonRegistryStore::new(&self.base_root)
            .load()?
            .identities
            .get(identity_id)
            .cloned()
            .ok_or_else(|| AppError::IdentityNotFound {
                identity_id: identity_id.clone(),
            })
    }

    fn automatic_selection_service(
        &self,
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
            JsonRegistryStore::new(&self.base_root),
            JsonQuotaStore::new(&self.base_root),
            JsonSelectionStore::new(&self.base_root),
            JsonSelectionEventStore::new(&self.base_root),
            JsonSelectionPolicyStore::new(&self.base_root),
            JsonIdentityHealthStore::new(&self.base_root),
            CodexAppServerVerifier::default(),
        )
    }

    fn release_turn_lease(&self, turn: &SessionTurnRecord) -> Result<()> {
        let Some(token) = turn.lease_token.as_deref() else {
            return Ok(());
        };
        let Some(thread_id) = turn.lease_thread_id.as_deref() else {
            return Ok(());
        };
        ThreadLeaseManager::with_default_locking(&self.base_root).release(
            thread_id,
            &turn.identity_id,
            token,
        )?;
        Ok(())
    }

    fn release_or_abort_handoff_lease(&self, handoff: &SessionHandoffRecord) -> Result<()> {
        let manager = ThreadLeaseManager::with_default_locking(&self.base_root);
        match handoff.lease_state_kind {
            HandoffLeaseStateKind::Active => {
                manager.release(
                    &handoff.thread_id,
                    &handoff.lease_owner_identity_id,
                    &handoff.lease_token,
                )?;
            }
            HandoffLeaseStateKind::HandoffPending => {
                manager.abort_handoff(
                    &handoff.thread_id,
                    &handoff.lease_owner_identity_id,
                    &handoff.lease_token,
                )?;
            }
        }
        Ok(())
    }

    fn finalize_startup_failure(
        &self,
        store: &mut SessionControlStore,
        session: &SessionRecord,
        turn: &SessionTurnRecord,
        error: &AppError,
    ) -> Result<()> {
        let now = current_timestamp()?;
        let mut session = session.clone();
        let mut turn = turn.clone();
        turn.status = SessionTurnStatus::Failed;
        turn.failure_kind = Some(error_code(error).to_string());
        turn.failure_message = Some(error.to_string());
        turn.finished_at = Some(now);
        turn.updated_at = now;
        session.active_turn_id = None;
        session.last_turn_id = Some(turn.turn_id.clone());
        session.updated_at = now;
        apply_terminal_session_state(store, &mut session, &turn, SessionTurnStatus::Failed)?;
        store.save_session_and_turn(&session, &turn)?;
        let _ = store.append_event(AppendEvent {
            session_id: &session.session_id,
            thread_id: &session.thread_id,
            turn_id: Some(&turn.turn_id),
            runtime_turn_id: None,
            handoff_id: session.pending_handoff_id.as_ref(),
            event: "turn.failed",
            timestamp: now,
            payload: &json!({
                "status": turn.status.as_str(),
                "failure_kind": turn.failure_kind,
                "failure_message": turn.failure_message,
            }),
        })?;
        Ok(())
    }

    fn handoff_service(&self) -> HandoffService<JsonRegistryStore, CodexAppServerVerifier> {
        HandoffService::new(
            &self.base_root,
            JsonRegistryStore::new(&self.base_root),
            CodexAppServerVerifier::default(),
        )
    }
}

impl SessionControlWorker {
    pub fn run(&self, turn_id: &str, worker_owner_id: &str) -> Result<()> {
        let mut store = SessionControlStore::open(&self.base_root)?;
        let mut turn = store
            .load_turn(turn_id)?
            .ok_or_else(|| AppError::SessionTurnNotFound {
                turn_id: turn_id.to_string(),
            })?;
        if turn.status.is_terminal() {
            return Ok(());
        }
        let mut session = store
            .load_session(&turn.session_id.to_string())?
            .ok_or_else(|| AppError::SessionNotFound {
                session_id: turn.session_id.to_string(),
            })?;
        if turn.worker_owner_id.as_deref() != Some(worker_owner_id) {
            return Err(AppError::RuntimeUnavailable {
                message: format!("turn {turn_id} is not owned by worker {worker_owner_id}"),
            });
        }
        let identity = JsonRegistryStore::new(&self.base_root)
            .load()?
            .identities
            .get(&turn.identity_id)
            .cloned()
            .ok_or_else(|| AppError::IdentityNotFound {
                identity_id: turn.identity_id.clone(),
            })?;
        let mut runtime_session = AppServerSession::connect(
            &self.app_server_command,
            &identity.codex_home,
            self.timeout,
        )?;
        let mut effective_prompt = turn.prompt_text.clone();
        let lease = match self.prepare_turn_execution(
            &mut store,
            &mut session,
            &mut turn,
            &mut runtime_session,
            &identity,
            &mut effective_prompt,
        ) {
            Ok(lease) => lease,
            Err(error) => {
                self.finish_failure(&mut store, &mut session, &mut turn, None, error)?;
                return Ok(());
            }
        };
        let effective_thread_id = lease
            .as_ref()
            .map(|lease| lease.thread_id.as_str())
            .unwrap_or(session.thread_id.as_str())
            .to_string();
        let runtime_turn_id = match start_turn(
            &mut runtime_session,
            Path::new(&session.workspace_root),
            session.model.as_deref(),
            &effective_thread_id,
            &effective_prompt,
        ) {
            Ok(turn_id) => turn_id,
            Err(error) => {
                self.finish_failure(&mut store, &mut session, &mut turn, lease.as_ref(), error)?;
                return Ok(());
            }
        };
        let running_at = current_timestamp()?;
        turn.runtime_turn_id = Some(runtime_turn_id.clone());
        turn.thread_id = effective_thread_id.clone();
        turn.status = SessionTurnStatus::Running;
        turn.started_at = Some(running_at);
        turn.updated_at = running_at;
        if let Some(lease) = lease.as_ref() {
            turn.lease_thread_id = Some(lease.thread_id.clone());
            turn.lease_token = Some(lease.token.clone());
            turn.lease_persist_on_finish = lease.persist_on_finish;
        }
        session.thread_id = effective_thread_id.clone();
        session.status = SessionStatus::Running;
        session.updated_at = running_at;
        store.save_session_and_turn(&session, &turn)?;
        let _ = store.append_event(AppendEvent {
            session_id: &session.session_id,
            thread_id: &session.thread_id,
            turn_id: Some(&turn.turn_id),
            runtime_turn_id: Some(&runtime_turn_id),
            handoff_id: session.pending_handoff_id.as_ref(),
            event: "turn.started",
            timestamp: running_at,
            payload: &json!({
                "continuity_mode": turn.continuity_mode.as_str(),
                "identity_id": turn.identity_id,
                "identity_name": turn.identity_name,
                "runtime_turn_id": runtime_turn_id,
            }),
        })?;
        let outcome = self.wait_for_turn(
            &mut store,
            &mut runtime_session,
            &session,
            &turn,
            lease.as_ref(),
        )?;
        match outcome {
            WorkerOutcome::Completed => {
                self.finish_success(&mut store, &mut session, &mut turn, lease.as_ref())?;
            }
            WorkerOutcome::Failed(kind, message) => {
                self.finish_terminal(
                    &mut store,
                    &mut session,
                    &mut turn,
                    lease.as_ref(),
                    TerminalTurnUpdate {
                        status: SessionTurnStatus::Failed,
                        failure_kind: Some(kind),
                        failure_message: Some(message),
                    },
                )?;
            }
            WorkerOutcome::TimedOut(message) => {
                self.finish_terminal(
                    &mut store,
                    &mut session,
                    &mut turn,
                    lease.as_ref(),
                    TerminalTurnUpdate {
                        status: SessionTurnStatus::TimedOut,
                        failure_kind: Some("rpc_timeout".to_string()),
                        failure_message: Some(message),
                    },
                )?;
            }
            WorkerOutcome::Canceled => {
                if let Some(lease) = lease.as_ref() {
                    if !lease.persist_on_finish {
                        let _ = ThreadLeaseManager::with_default_locking(&self.base_root).release(
                            &lease.thread_id,
                            &turn.identity_id,
                            &lease.token,
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn prepare_turn_execution(
        &self,
        store: &mut SessionControlStore,
        session: &mut SessionRecord,
        turn: &mut SessionTurnRecord,
        runtime_session: &mut AppServerSession,
        identity: &CodexIdentity,
        effective_prompt: &mut String,
    ) -> Result<Option<LeaseHandle>> {
        Ok(match turn.continuity_mode {
            ContinuityMode::SameThread => {
                let acquired = ThreadLeaseManager::with_default_locking(&self.base_root)
                    .acquire(&session.thread_id, &identity.id)?;
                if let Err(error) = resume_thread(
                    runtime_session,
                    Path::new(&session.workspace_root),
                    session.model.as_deref(),
                    &session.thread_id,
                ) {
                    let _ = ThreadLeaseManager::with_default_locking(&self.base_root).release(
                        &session.thread_id,
                        &identity.id,
                        &acquired.lease_token,
                    );
                    return Err(error);
                }
                Some(LeaseHandle {
                    thread_id: session.thread_id.clone(),
                    token: acquired.lease_token,
                    persist_on_finish: false,
                })
            }
            ContinuityMode::Handoff => {
                let handoff = store
                    .load_handoff(
                        session
                            .pending_handoff_id
                            .as_ref()
                            .ok_or_else(|| AppError::InvalidSessionControlState {
                                message: "handoff continuity requested without a pending handoff"
                                    .to_string(),
                            })?
                            .as_str(),
                    )?
                    .ok_or_else(|| AppError::SessionHandoffNotFound {
                        handoff_id: session
                            .pending_handoff_id
                            .as_ref()
                            .expect("checked pending handoff")
                            .to_string(),
                    })?;
                self.handoff_service().heartbeat_lease(
                    &handoff.to_identity_name,
                    &handoff.thread_id,
                    &handoff.lease_token,
                )?;
                resume_thread(
                    runtime_session,
                    Path::new(&session.workspace_root),
                    session.model.as_deref(),
                    &handoff.thread_id,
                )?;
                Some(LeaseHandle {
                    thread_id: handoff.thread_id.clone(),
                    token: handoff.lease_token,
                    persist_on_finish: true,
                })
            }
            ContinuityMode::CheckpointFallback => {
                let checkpoint = JsonTaskCheckpointStore::new(&self.base_root)
                    .load(session.last_checkpoint_id.as_deref().ok_or_else(|| {
                        AppError::CheckpointFallbackRequired {
                            session_id: session.session_id.to_string(),
                            handoff_id: session
                                .pending_handoff_id
                                .as_ref()
                                .map(ToString::to_string),
                        }
                    })?)?
                    .ok_or_else(|| AppError::CheckpointFallbackRequired {
                        session_id: session.session_id.to_string(),
                        handoff_id: session.pending_handoff_id.as_ref().map(ToString::to_string),
                    })?;
                *effective_prompt = format!(
                    "{}\n\nOperator follow-up:\n{}",
                    checkpoint.resume_prompt, turn.prompt_text
                );
                let new_thread_id = start_thread(
                    runtime_session,
                    Path::new(&session.workspace_root),
                    session.model.as_deref(),
                    identity,
                )?;
                let acquired = ThreadLeaseManager::with_default_locking(&self.base_root)
                    .acquire(&new_thread_id, &identity.id)?;
                session.thread_id = new_thread_id.clone();
                Some(LeaseHandle {
                    thread_id: new_thread_id,
                    token: acquired.lease_token,
                    persist_on_finish: false,
                })
            }
        })
    }

    fn wait_for_turn(
        &self,
        store: &mut SessionControlStore,
        session: &mut AppServerSession,
        session_record: &SessionRecord,
        turn: &SessionTurnRecord,
        lease: Option<&LeaseHandle>,
    ) -> Result<WorkerOutcome> {
        let deadline_at = turn.max_runtime_secs.and_then(|seconds| {
            turn.started_at
                .map(|started_at| started_at + seconds.max(0))
        });
        loop {
            if let Some(deadline_at) = deadline_at {
                if current_timestamp()? >= deadline_at {
                    return Ok(WorkerOutcome::TimedOut(format!(
                        "turn exceeded max_runtime_secs deadline at {deadline_at}"
                    )));
                }
            }
            if let Some(message) = session.next_message(self.worker_heartbeat_interval)? {
                let event = message
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or("notification");
                let now = current_timestamp()?;
                let event_name = match event {
                    "turn/completed" => "turn.completed",
                    "error" => "turn.failed",
                    _ => "turn.output.delta",
                };
                let _ = store.append_event(AppendEvent {
                    session_id: &session_record.session_id,
                    thread_id: &session_record.thread_id,
                    turn_id: Some(&turn.turn_id),
                    runtime_turn_id: turn.runtime_turn_id.as_deref(),
                    handoff_id: session_record.pending_handoff_id.as_ref(),
                    event: event_name,
                    timestamp: now,
                    payload: &json!({ "rpc": message }),
                })?;
                match event {
                    "turn/completed" => {
                        let notification_turn_id = message
                            .get("params")
                            .and_then(|value| value.get("turn"))
                            .and_then(|value| value.get("id"))
                            .and_then(Value::as_str);
                        if notification_turn_id == turn.runtime_turn_id.as_deref() {
                            return Ok(WorkerOutcome::Completed);
                        }
                    }
                    "error" => {
                        let error_message = message
                            .get("params")
                            .and_then(|value| value.get("message"))
                            .and_then(Value::as_str)
                            .unwrap_or("runtime error")
                            .to_string();
                        return Ok(WorkerOutcome::Failed(
                            "rpc_server_error".to_string(),
                            error_message,
                        ));
                    }
                    _ => {}
                }
            }
            let now = current_timestamp()?;
            if !store.heartbeat_turn(
                turn.turn_id.as_str(),
                turn.worker_owner_id.as_deref().unwrap_or_default(),
                now,
                now + self.worker_lease_ttl.as_secs() as i64,
            )? {
                return Ok(WorkerOutcome::Canceled);
            }
            if let Some(lease) = lease {
                if ThreadLeaseManager::with_default_locking(&self.base_root)
                    .heartbeat(&lease.thread_id, &turn.identity_id, &lease.token)
                    .is_err()
                {
                    let _ = store.append_event(AppendEvent {
                        session_id: &session_record.session_id,
                        thread_id: &session_record.thread_id,
                        turn_id: Some(&turn.turn_id),
                        runtime_turn_id: turn.runtime_turn_id.as_deref(),
                        handoff_id: session_record.pending_handoff_id.as_ref(),
                        event: "lease.lost",
                        timestamp: now,
                        payload: &json!({
                            "thread_id": lease.thread_id,
                        }),
                    })?;
                    return Ok(WorkerOutcome::Failed(
                        "thread_lease_conflict".to_string(),
                        "turn lease heartbeat failed".to_string(),
                    ));
                }
            }
        }
    }

    fn finish_success(
        &self,
        store: &mut SessionControlStore,
        session: &mut SessionRecord,
        turn: &mut SessionTurnRecord,
        lease: Option<&LeaseHandle>,
    ) -> Result<()> {
        self.finish_terminal(
            store,
            session,
            turn,
            lease,
            TerminalTurnUpdate {
                status: SessionTurnStatus::Completed,
                failure_kind: None,
                failure_message: None,
            },
        )
    }

    fn finish_failure(
        &self,
        store: &mut SessionControlStore,
        session: &mut SessionRecord,
        turn: &mut SessionTurnRecord,
        lease: Option<&LeaseHandle>,
        error: AppError,
    ) -> Result<()> {
        self.finish_terminal(
            store,
            session,
            turn,
            lease,
            TerminalTurnUpdate {
                status: SessionTurnStatus::Failed,
                failure_kind: Some(error_code(&error).to_string()),
                failure_message: Some(error.to_string()),
            },
        )
    }

    fn finish_terminal(
        &self,
        store: &mut SessionControlStore,
        session: &mut SessionRecord,
        turn: &mut SessionTurnRecord,
        lease: Option<&LeaseHandle>,
        terminal: TerminalTurnUpdate,
    ) -> Result<()> {
        let now = current_timestamp()?;
        if let Some(lease) = lease {
            if !lease.persist_on_finish {
                let _ = ThreadLeaseManager::with_default_locking(&self.base_root).release(
                    &lease.thread_id,
                    &turn.identity_id,
                    &lease.token,
                );
            }
        }
        turn.status = terminal.status;
        turn.failure_kind = terminal.failure_kind.clone();
        turn.failure_message = terminal.failure_message.clone();
        turn.finished_at = Some(now);
        turn.updated_at = now;
        session.active_turn_id = None;
        session.last_turn_id = Some(turn.turn_id.clone());
        session.updated_at = now;
        apply_terminal_session_state(store, session, turn, terminal.status)?;
        store.save_session_and_turn(session, turn)?;
        let event_name = match terminal.status {
            SessionTurnStatus::Completed => "turn.completed",
            SessionTurnStatus::Failed => "turn.failed",
            SessionTurnStatus::TimedOut => "turn.timed_out",
            SessionTurnStatus::Canceled => "turn.canceled",
            SessionTurnStatus::Queued
            | SessionTurnStatus::Starting
            | SessionTurnStatus::Running => "runtime.warning",
        };
        let _ = store.append_event(AppendEvent {
            session_id: &session.session_id,
            thread_id: &session.thread_id,
            turn_id: Some(&turn.turn_id),
            runtime_turn_id: turn.runtime_turn_id.as_deref(),
            handoff_id: session.pending_handoff_id.as_ref(),
            event: event_name,
            timestamp: now,
            payload: &json!({
                "status": turn.status.as_str(),
                "failure_kind": turn.failure_kind,
                "failure_message": turn.failure_message,
            }),
        })?;
        Ok(())
    }

    fn handoff_service(&self) -> HandoffService<JsonRegistryStore, CodexAppServerVerifier> {
        HandoffService::new(
            &self.base_root,
            JsonRegistryStore::new(&self.base_root),
            CodexAppServerVerifier::default(),
        )
    }
}

fn apply_terminal_session_state(
    store: &SessionControlStore,
    session: &mut SessionRecord,
    turn: &SessionTurnRecord,
    terminal_status: SessionTurnStatus,
) -> Result<()> {
    let pending_handoff = match session.pending_handoff_id.as_ref() {
        Some(handoff_id) => store.load_handoff(handoff_id.as_str())?,
        None => None,
    };
    if let Some(handoff) = pending_handoff {
        match handoff.status {
            SessionHandoffStatus::Prepared | SessionHandoffStatus::Accepted => {
                session.status = SessionStatus::HandoffReady;
                session.safe_to_continue = false;
                session.continuity_mode = ContinuityMode::Handoff;
                return Ok(());
            }
            SessionHandoffStatus::FallbackRequired => {
                session.status = SessionStatus::Blocked;
                session.safe_to_continue = false;
                session.continuity_mode = ContinuityMode::CheckpointFallback;
                return Ok(());
            }
            SessionHandoffStatus::Confirmed
            | SessionHandoffStatus::Expired
            | SessionHandoffStatus::Aborted => {}
        }
    }
    match turn.continuity_mode {
        ContinuityMode::CheckpointFallback => match terminal_status {
            SessionTurnStatus::Completed => {
                session.status = SessionStatus::WaitingForFollowup;
                session.safe_to_continue = true;
                session.continuity_mode = ContinuityMode::SameThread;
            }
            SessionTurnStatus::Canceled => {
                session.status = SessionStatus::WaitingForFollowup;
                session.safe_to_continue = true;
                session.continuity_mode = ContinuityMode::CheckpointFallback;
            }
            SessionTurnStatus::Failed | SessionTurnStatus::TimedOut => {
                session.status = SessionStatus::Failed;
                session.safe_to_continue = true;
                session.continuity_mode = ContinuityMode::CheckpointFallback;
            }
            SessionTurnStatus::Queued
            | SessionTurnStatus::Starting
            | SessionTurnStatus::Running => {}
        },
        ContinuityMode::SameThread | ContinuityMode::Handoff => match terminal_status {
            SessionTurnStatus::Completed | SessionTurnStatus::Canceled => {
                session.status = SessionStatus::WaitingForFollowup;
                session.safe_to_continue = true;
                session.continuity_mode = ContinuityMode::SameThread;
            }
            SessionTurnStatus::Failed | SessionTurnStatus::TimedOut => {
                session.status = SessionStatus::Failed;
                session.safe_to_continue = true;
                session.continuity_mode = ContinuityMode::SameThread;
            }
            SessionTurnStatus::Queued
            | SessionTurnStatus::Starting
            | SessionTurnStatus::Running => {}
        },
    }
    Ok(())
}

fn startup_error_from_turn(turn: &SessionTurnRecord) -> AppError {
    match turn.failure_kind.as_deref() {
        Some("rpc_timeout") => AppError::RpcTimeout {
            method: "turns.start".to_string(),
            timeout: STARTUP_WAIT_TIMEOUT,
        },
        Some("thread_lease_conflict") => AppError::RuntimeUnavailable {
            message: turn
                .failure_message
                .clone()
                .unwrap_or_else(|| "thread lease conflict".to_string()),
        },
        Some("runtime_unavailable") => AppError::RuntimeUnavailable {
            message: turn
                .failure_message
                .clone()
                .unwrap_or_else(|| "session-control runtime unavailable".to_string()),
        },
        Some("rpc_server_error") => AppError::RuntimeUnavailable {
            message: turn
                .failure_message
                .clone()
                .unwrap_or_else(|| "app-server startup failed".to_string()),
        },
        _ => AppError::RuntimeUnavailable {
            message: turn
                .failure_message
                .clone()
                .unwrap_or_else(|| "worker exited before the turn started".to_string()),
        },
    }
}

fn worker_program_path() -> Result<PathBuf> {
    match std::env::var_os("CODEX_SWITCH_TEST_WORKER_PROGRAM") {
        Some(program) => Ok(PathBuf::from(program)),
        None => Ok(std::env::current_exe()?),
    }
}

fn start_thread(
    session: &mut AppServerSession,
    working_directory: &Path,
    model: Option<&str>,
    identity: &CodexIdentity,
) -> Result<String> {
    let response: Value = session.request(
        "thread/start",
        Some(json!({
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "cwd": working_directory,
            "model": model,
            "personality": "pragmatic",
            "sandbox": "danger-full-access",
            "serviceName": format!("codex-switch/{}", identity.id),
        })),
    )?;
    response
        .get("thread")
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| AppError::MissingRpcResult {
            method: "thread/start".to_string(),
        })
}

fn resume_thread(
    session: &mut AppServerSession,
    working_directory: &Path,
    model: Option<&str>,
    thread_id: &str,
) -> Result<()> {
    let _: Value = session.request(
        "thread/resume",
        Some(json!({
            "threadId": thread_id,
            "cwd": working_directory,
            "model": model,
            "personality": "pragmatic",
            "sandbox": "danger-full-access",
        })),
    )?;
    Ok(())
}

fn start_turn(
    session: &mut AppServerSession,
    working_directory: &Path,
    model: Option<&str>,
    thread_id: &str,
    prompt: &str,
) -> Result<String> {
    let response: Value = session.request(
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "cwd": working_directory,
            "input": [{
                "type": "text",
                "text": prompt
            }],
            "model": model,
            "personality": "pragmatic",
            "approvalPolicy": "never",
            "sandboxPolicy": {
                "type": "dangerFullAccess"
            }
        })),
    )?;
    response
        .get("turn")
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| AppError::MissingRpcResult {
            method: "turn/start".to_string(),
        })
}

fn is_checkpoint_fallback_error(error: &AppError) -> bool {
    matches!(
        error,
        AppError::SharedSessionsRootMismatch { .. }
            | AppError::ThreadHistoryNotShared { .. }
            | AppError::RpcTimeout { .. }
            | AppError::RpcServer { .. }
            | AppError::MissingRpcResult { .. }
            | AppError::AppServerExited { .. }
            | AppError::RpcPayloadDecode { .. }
    )
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
            thread::sleep(Duration::from_millis(50));
        }
        libc::kill(process_group, libc::SIGKILL) == 0
    }

    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

pub fn error_code(error: &AppError) -> &'static str {
    match error {
        AppError::IdentityNotFound { .. } => "identity_not_found",
        AppError::NoSelectableIdentity => "no_selectable_identity",
        AppError::SessionNotFound { .. } => "session_not_found",
        AppError::SessionTurnNotFound { .. } => "turn_not_found",
        AppError::SessionHandoffNotFound { .. } => "handoff_not_found",
        AppError::ThreadLeaseHeld { .. }
        | AppError::ThreadLeaseNotFound { .. }
        | AppError::ThreadLeaseTokenMismatch { .. }
        | AppError::ThreadLeaseStateConflict { .. } => "thread_lease_conflict",
        AppError::SessionTurnAlreadyActive { .. } => "turn_already_active",
        AppError::SessionHandoffPending { .. } => "handoff_pending",
        AppError::UnsafeSameThreadResume { .. } => "unsafe_same_thread_resume",
        AppError::CheckpointFallbackRequired { .. } => "checkpoint_fallback_required",
        AppError::RuntimeUnavailable { .. } => "runtime_unavailable",
        AppError::RpcTimeout { .. } => "rpc_timeout",
        AppError::RpcServer { .. }
        | AppError::MissingRpcResult { .. }
        | AppError::AppServerExited { .. }
        | AppError::RpcPayloadDecode { .. } => "rpc_server_error",
        AppError::SchedulerFeatureDisabled { .. } => "scheduler_disabled",
        AppError::CurrentDirectoryUnavailable { .. } => "workspace_unavailable",
        _ => "validation_error",
    }
}
