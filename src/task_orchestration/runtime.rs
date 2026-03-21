use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{json, Value};

use crate::codex_rpc::{AppServerCommand, AppServerSession, CodexAppServerVerifier};
use crate::domain::checkpoint::{CheckpointMode, TaskCheckpoint};
use crate::domain::identity::{current_timestamp, CodexIdentity, IdentityId};
use crate::error::{AppError, Result};
use crate::handoff::HandoffService;
use crate::storage::checkpoint_store::{JsonTaskCheckpointStore, TaskCheckpointStore};
use crate::storage::paths::{
    atomic_write, task_artifact_events_path, task_artifact_thread_snapshot_path,
};
use crate::storage::registry_store::{JsonRegistryStore, RegistryStore};
use crate::task_orchestration::config::SchedulerSettings;
use crate::task_orchestration::domain::{FailureKind, LaunchMode, TaskRunStatus};
use crate::task_orchestration::store::{RunCompletion, SchedulerStore};
use crate::task_orchestration::worktree::WorktreeManager;

#[derive(Debug, Clone)]
pub struct TaskRuntimeWorker {
    base_root: PathBuf,
    settings: SchedulerSettings,
    app_server_command: AppServerCommand,
    timeout: Duration,
}

impl TaskRuntimeWorker {
    pub fn new(base_root: &Path, settings: SchedulerSettings) -> Self {
        Self {
            base_root: base_root.to_path_buf(),
            settings,
            app_server_command: AppServerCommand::default(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_app_server_command(
        base_root: &Path,
        settings: SchedulerSettings,
        app_server_command: AppServerCommand,
    ) -> Self {
        Self {
            base_root: base_root.to_path_buf(),
            settings,
            app_server_command,
            timeout: Duration::from_secs(30),
        }
    }

    pub fn run(&self, run_id: &str) -> Result<()> {
        let pid = std::process::id();
        let owner_id = format!("worker-{pid}-{run_id}");
        let mut store = SchedulerStore::open(&self.base_root)?;
        let lease_expires_at = lease_expiry(&self.settings)?;
        let run = store.start_run_launching(run_id, &owner_id, pid, lease_expires_at)?;
        let (project, task, _, input) = store.run_context(run_id)?;
        let worktree =
            store.get_worktree(run.assigned_worktree_id.as_ref().ok_or_else(|| {
                AppError::InvalidSchedulerConfiguration {
                    message: format!("run {run_id} missing assigned worktree"),
                }
            })?)?;
        let materialized = WorktreeManager.materialize(
            &self.base_root,
            &project,
            &task.task_id,
            &run.run_id,
            Some(&worktree.path),
        )?;
        let working_directory = materialized.path;
        let identity = resolve_identity(&self.base_root, run.assigned_identity_id.as_ref())?;
        let mut thread_id = run.assigned_thread_id.clone();
        let mut checkpoint_id: Option<String> = None;
        let checkpoint_store = JsonTaskCheckpointStore::new(&self.base_root);

        let runtime_setup = match run.launch_mode.unwrap_or(LaunchMode::NewThread) {
            LaunchMode::NewThread => RuntimeSetup {
                prompt_text: input.prompt_text.clone(),
                session_kind: SessionKind::NewThread,
                handoff_acceptance: None,
            },
            LaunchMode::ResumeSameIdentity => RuntimeSetup {
                prompt_text: input.prompt_text.clone(),
                session_kind: SessionKind::ResumeThread(thread_id.clone().ok_or_else(|| {
                    AppError::InvalidSchedulerConfiguration {
                        message: format!("run {run_id} missing assigned thread id"),
                    }
                })?),
                handoff_acceptance: None,
            },
            LaunchMode::ResumeHandoff => {
                let source_identity =
                    resolve_identity(&self.base_root, task.last_identity_id.as_ref())?;
                let source_thread_id = task.current_lineage_thread_id.clone().ok_or_else(|| {
                    AppError::InvalidSchedulerConfiguration {
                        message: format!("follow-up run {run_id} missing lineage thread id"),
                    }
                })?;
                let handoff_service = HandoffService::new(
                    &self.base_root,
                    JsonRegistryStore::new(&self.base_root),
                    CodexAppServerVerifier::default(),
                );
                let source_lease = handoff_service
                    .acquire_lease(&source_identity.display_name, &source_thread_id)?;
                match handoff_service.prepare_handoff(
                    &source_thread_id,
                    &source_identity.display_name,
                    &identity.display_name,
                    &source_lease.lease_token,
                    &format!("scheduler run {}", run.run_id),
                ) {
                    Ok(preparation) => {
                        let acceptance = handoff_service.accept_handoff(
                            &source_thread_id,
                            &identity.display_name,
                            &preparation.lease.lease_token,
                        )?;
                        RuntimeSetup {
                            prompt_text: input.prompt_text.clone(),
                            session_kind: SessionKind::ResumeThread(source_thread_id),
                            handoff_acceptance: Some((
                                handoff_service,
                                acceptance.lease.lease_token,
                            )),
                        }
                    }
                    Err(error) if is_checkpoint_fallback_error(&error) => {
                        let snapshot = handoff_service
                            .inspect_thread(&source_identity.display_name, &source_thread_id)?;
                        let checkpoint = TaskCheckpoint::new(
                            &snapshot,
                            source_identity.id.clone(),
                            identity.id.clone(),
                            CheckpointMode::ResumeViaCheckpoint,
                            format!("scheduler run {}", run.run_id),
                            Some(error.to_string()),
                        )?;
                        checkpoint_store.save(&checkpoint)?;
                        checkpoint_id = Some(checkpoint.id.clone());
                        RuntimeSetup {
                            prompt_text: format!(
                                "{}\n\nOperator follow-up:\n{}",
                                checkpoint.resume_prompt, input.prompt_text
                            ),
                            session_kind: SessionKind::NewThread,
                            handoff_acceptance: None,
                        }
                    }
                    Err(error) => {
                        store.finish_run(
                            run_id,
                            RunCompletion {
                                status: TaskRunStatus::Failed,
                                exit_code: None,
                                failure_kind: Some(FailureKind::Handoff),
                                failure_message: Some(error.to_string()),
                                thread_id: None,
                                checkpoint_id: None,
                                last_identity_id: Some(identity.id.clone()),
                            },
                        )?;
                        return Err(error);
                    }
                }
            }
            LaunchMode::ResumeCheckpoint => {
                let checkpoint = match task.last_checkpoint_id.as_deref() {
                    Some(checkpoint_id) => checkpoint_store.load(checkpoint_id)?,
                    None => None,
                };
                let prompt_text = match checkpoint {
                    Some(checkpoint) => format!(
                        "{}\n\nOperator follow-up:\n{}",
                        checkpoint.resume_prompt, input.prompt_text
                    ),
                    None => input.prompt_text.clone(),
                };
                RuntimeSetup {
                    prompt_text,
                    session_kind: SessionKind::NewThread,
                    handoff_acceptance: None,
                }
            }
        };

        let mut session = AppServerSession::connect(
            &self.app_server_command,
            &identity.codex_home,
            self.timeout,
        )?;
        thread_id = Some(match runtime_setup.session_kind {
            SessionKind::NewThread => {
                start_thread(&mut session, &working_directory, &project, &identity)?
            }
            SessionKind::ResumeThread(ref thread_id) => {
                resume_thread(&mut session, &working_directory, &project, thread_id)?;
                thread_id.clone()
            }
        });
        let turn_id = start_turn(
            &mut session,
            &working_directory,
            &project,
            thread_id.as_deref().expect("thread id"),
            &runtime_setup.prompt_text,
        )?;
        store.mark_run_running(
            run_id,
            &owner_id,
            thread_id.as_deref().expect("thread id"),
            Some(&turn_id),
            lease_expiry(&self.settings)?,
        )?;
        let artifact_path =
            task_artifact_events_path(&self.base_root, task.task_id.as_str(), run.run_id.as_str());
        let completed = self.wait_for_turn(
            &mut store,
            &mut session,
            run_id,
            &owner_id,
            &artifact_path,
            &turn_id,
        )?;
        let final_snapshot =
            read_thread_snapshot(&mut session, thread_id.as_deref().expect("thread id"))?;
        persist_thread_snapshot(
            &self.base_root,
            task.task_id.as_str(),
            run.run_id.as_str(),
            &final_snapshot,
        )?;
        if let Some((handoff_service, lease_token)) = runtime_setup.handoff_acceptance {
            let _ = handoff_service.confirm_handoff(
                thread_id.as_deref().expect("thread id"),
                &identity.display_name,
                &lease_token,
                Some(&turn_id),
            );
        }

        let (status, failure_kind, failure_message) = if completed {
            (TaskRunStatus::Completed, None, None)
        } else {
            (
                TaskRunStatus::Failed,
                Some(FailureKind::Runtime),
                Some("turn completed without a terminal completion notification".to_string()),
            )
        };
        store.finish_run(
            run_id,
            RunCompletion {
                status,
                exit_code: Some(0),
                failure_kind,
                failure_message,
                thread_id,
                checkpoint_id,
                last_identity_id: Some(identity.id.clone()),
            },
        )?;
        store.update_task_preferred_identity(&task.task_id, &identity.id)?;
        Ok(())
    }

    fn wait_for_turn(
        &self,
        store: &mut SchedulerStore,
        session: &mut AppServerSession,
        run_id: &str,
        owner_id: &str,
        artifact_path: &Path,
        turn_id: &str,
    ) -> Result<bool> {
        loop {
            let lease_expires_at = lease_expiry(&self.settings)?;
            if let Some(message) = session.next_message(self.settings.worker_heartbeat_interval)? {
                append_jsonl(artifact_path, &message)?;
                if let Some(method) = message.get("method").and_then(Value::as_str) {
                    match method {
                        "turn/completed" => {
                            let notification_turn_id = message
                                .get("params")
                                .and_then(|value| value.get("turn"))
                                .and_then(|value| value.get("id"))
                                .and_then(Value::as_str);
                            if notification_turn_id == Some(turn_id) {
                                return Ok(true);
                            }
                        }
                        "error" => {
                            let error_message = message
                                .get("params")
                                .and_then(|value| value.get("message"))
                                .and_then(Value::as_str)
                                .unwrap_or("runtime error");
                            store.finish_run(
                                run_id,
                                RunCompletion {
                                    status: TaskRunStatus::Failed,
                                    exit_code: None,
                                    failure_kind: Some(FailureKind::Runtime),
                                    failure_message: Some(error_message.to_string()),
                                    thread_id: None,
                                    checkpoint_id: None,
                                    last_identity_id: None,
                                },
                            )?;
                            return Err(AppError::RpcServer {
                                method: "turn/start".to_string(),
                                code: -32000,
                                message: error_message.to_string(),
                            });
                        }
                        _ => {}
                    }
                }
            }
            store.heartbeat_run(run_id, owner_id, Some(turn_id), lease_expires_at)?;
        }
    }
}

#[derive(Debug)]
struct RuntimeSetup {
    prompt_text: String,
    session_kind: SessionKind,
    handoff_acceptance: Option<(
        HandoffService<JsonRegistryStore, CodexAppServerVerifier>,
        String,
    )>,
}

#[derive(Debug)]
enum SessionKind {
    NewThread,
    ResumeThread(String),
}

fn resolve_identity(base_root: &Path, identity_id: Option<&IdentityId>) -> Result<CodexIdentity> {
    let identity_id = identity_id.ok_or_else(|| AppError::InvalidSchedulerConfiguration {
        message: "run is missing assigned identity".to_string(),
    })?;
    let registry = JsonRegistryStore::new(base_root).load()?;
    registry
        .identities
        .get(identity_id)
        .cloned()
        .ok_or_else(|| AppError::IdentityNotFound {
            identity_id: identity_id.clone(),
        })
}

fn start_thread(
    session: &mut AppServerSession,
    working_directory: &Path,
    project: &crate::task_orchestration::domain::ProjectRecord,
    identity: &CodexIdentity,
) -> Result<String> {
    let response: Value = session.request(
        "thread/start",
        Some(json!({
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "cwd": working_directory,
            "model": project.default_model_or_profile,
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
    project: &crate::task_orchestration::domain::ProjectRecord,
    thread_id: &str,
) -> Result<()> {
    let _: Value = session.request(
        "thread/resume",
        Some(json!({
            "threadId": thread_id,
            "cwd": working_directory,
            "model": project.default_model_or_profile,
            "personality": "pragmatic",
            "sandbox": "danger-full-access",
        })),
    )?;
    Ok(())
}

fn start_turn(
    session: &mut AppServerSession,
    working_directory: &Path,
    project: &crate::task_orchestration::domain::ProjectRecord,
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
            "model": project.default_model_or_profile,
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

fn read_thread_snapshot(session: &mut AppServerSession, thread_id: &str) -> Result<Value> {
    session.request(
        "thread/read",
        Some(json!({
            "threadId": thread_id,
            "includeTurns": true
        })),
    )
}

fn append_jsonl(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new().append(true).create(true).open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn persist_thread_snapshot(
    base_root: &Path,
    task_id: &str,
    run_id: &str,
    value: &Value,
) -> Result<()> {
    let mut payload = serde_json::to_vec_pretty(value)?;
    payload.push(b'\n');
    atomic_write(
        &task_artifact_thread_snapshot_path(base_root, task_id, run_id),
        &payload,
        0o600,
    )
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

fn lease_expiry(settings: &SchedulerSettings) -> Result<i64> {
    Ok(current_timestamp()? + settings.worker_lease_ttl.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::append_jsonl;

    #[test]
    fn appends_jsonl_artifacts() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("events.jsonl");
        append_jsonl(&path, &json!({"method": "turn/started"})).unwrap();
        append_jsonl(&path, &json!({"method": "turn/completed"})).unwrap();
        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("\"turn/started\""));
        assert!(content.contains("\"turn/completed\""));
    }
}
