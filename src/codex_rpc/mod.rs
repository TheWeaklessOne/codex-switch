use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

use crate::domain::identity::{AccountType, CodexIdentity, PlanType, WorkspaceForceProbeStatus};
use crate::domain::thread::{ThreadSnapshot, TurnStatus};
use crate::domain::verification::{IdentityVerification, RateLimitSnapshot};
use crate::error::{AppError, Result};
use crate::workspace_switching::{
    sync_managed_config, WorkspaceForceProbeObservation, WorkspaceForceProbeReport,
    WorkspaceForceProber,
};

pub trait IdentityVerifier {
    fn verify(&self, identity: &CodexIdentity) -> Result<IdentityVerification>;
}

pub trait ThreadRuntime {
    fn read_thread(&self, identity: &CodexIdentity, thread_id: &str) -> Result<ThreadSnapshot>;
    fn resume_thread(&self, identity: &CodexIdentity, thread_id: &str) -> Result<ThreadSnapshot>;
}

#[derive(Debug)]
pub struct AppServerSession {
    process: ManagedAppServerProcess,
}

#[derive(Debug, Clone)]
pub struct AppServerCommand {
    program: PathBuf,
    args: Vec<String>,
}

impl Default for AppServerCommand {
    fn default() -> Self {
        Self {
            program: PathBuf::from("codex"),
            args: vec![
                "app-server".to_string(),
                "--listen".to_string(),
                "stdio://".to_string(),
            ],
        }
    }
}

impl AppServerCommand {
    pub fn new(
        program: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CodexAppServerVerifier {
    command: AppServerCommand,
    timeout: Duration,
}

impl Default for CodexAppServerVerifier {
    fn default() -> Self {
        Self {
            command: AppServerCommand::default(),
            timeout: Duration::from_secs(20),
        }
    }
}

impl CodexAppServerVerifier {
    pub fn new(command: AppServerCommand, timeout: Duration) -> Self {
        Self { command, timeout }
    }

    pub fn verify_codex_home(&self, codex_home: &Path) -> Result<IdentityVerification> {
        let mut process = ManagedAppServerProcess::spawn(&self.command, codex_home, self.timeout)?;
        process.initialize()?;

        let auth_status =
            match process.request::<AuthStatusPayload>("getAuthStatus", Some(json!({}))) {
                Ok(status) => Some(status),
                Err(AppError::RpcServer { code: -32601, .. }) => None,
                Err(error) => return Err(error),
            };
        let account = process.request::<AccountReadResponse>("account/read", Some(json!({})))?;
        let rate_limits = process
            .request::<RateLimitsReadResponse>("account/rateLimits/read", Some(json!({})))?;

        let (account_type, email, plan_type) = match account.account.as_ref() {
            Some(AccountPayload::ApiKey { .. }) => (Some(AccountType::ApiKey), None, None),
            Some(AccountPayload::Chatgpt {
                email, plan_type, ..
            }) => (
                Some(AccountType::Chatgpt),
                Some(email.clone()),
                Some(*plan_type),
            ),
            None => (None, None, None),
        };

        let auth_method = auth_status
            .and_then(|status| status.auth_method)
            .or_else(|| {
                account_type.map(|kind| match kind {
                    AccountType::Chatgpt => "chatgpt".to_string(),
                    AccountType::ApiKey => "apikey".to_string(),
                })
            });

        Ok(IdentityVerification {
            authenticated: auth_method.is_some()
                || account.account.is_some()
                || !account.requires_openai_auth,
            auth_method,
            account_type,
            email,
            plan_type,
            requires_openai_auth: account.requires_openai_auth,
            fallback_rate_limit: Some(rate_limits.rate_limits),
            rate_limits_by_limit_id: rate_limits.rate_limits_by_limit_id.unwrap_or_default(),
        })
    }

    fn capture_workspace_probe_observation(
        &self,
        codex_home: &Path,
    ) -> Result<WorkspaceForceProbeObservation> {
        let mut process = ManagedAppServerProcess::spawn(&self.command, codex_home, self.timeout)?;
        process.initialize()?;

        let auth_status =
            match process.request::<AuthStatusPayload>("getAuthStatus", Some(json!({}))) {
                Ok(status) => Some(status),
                Err(AppError::RpcServer { code: -32601, .. }) => None,
                Err(error) => return Err(error),
            };
        let config = process.request::<ConfigReadResponse>(
            "config/read",
            Some(json!({
                "includeLayers": false
            })),
        )?;
        let account = process.request::<Value>("account/read", Some(json!({})))?;
        let account_typed = serde_json::from_value::<AccountReadResponse>(account.clone())
            .map_err(|source| AppError::RpcPayloadDecode {
                method: "account/read".to_string(),
                source,
            })?;
        let rate_limits = process.request::<Value>("account/rateLimits/read", Some(json!({})))?;
        let _rate_limits_typed = serde_json::from_value::<RateLimitsReadResponse>(
            rate_limits.clone(),
        )
        .map_err(|source| AppError::RpcPayloadDecode {
            method: "account/rateLimits/read".to_string(),
            source,
        })?;

        let authenticated = auth_status
            .as_ref()
            .and_then(|status| status.auth_method.as_ref())
            .is_some()
            || account_typed.account.is_some()
            || !account_typed.requires_openai_auth;

        Ok(WorkspaceForceProbeObservation {
            authenticated,
            auth_method: auth_status.and_then(|status| status.auth_method),
            effective_workspace_id: config.config.forced_chatgpt_workspace_id,
            effective_login_method: config.config.forced_login_method,
            account,
            rate_limits,
        })
    }

    fn write_workspace_probe_override(
        &self,
        codex_home: &Path,
        workspace_id: &str,
    ) -> Result<WorkspaceProbeWriteResult> {
        let mut process = ManagedAppServerProcess::spawn(&self.command, codex_home, self.timeout)?;
        process.initialize()?;

        let config_path = codex_home.join("config.toml");
        let config_path_string = config_path.to_string_lossy().into_owned();
        let first = process.request::<ConfigWriteResponse>(
            "config/value/write",
            Some(json!({
                "filePath": config_path_string,
                "keyPath": "forced_login_method",
                "mergeStrategy": "upsert",
                "value": "chatgpt"
            })),
        )?;
        let second = process.request::<ConfigWriteResponse>(
            "config/value/write",
            Some(json!({
                "expectedVersion": first.version.clone(),
                "filePath": config_path,
                "keyPath": "forced_chatgpt_workspace_id",
                "mergeStrategy": "upsert",
                "value": workspace_id
            })),
        )?;
        let readback = process.request::<ConfigReadResponse>(
            "config/read",
            Some(json!({
                "includeLayers": false
            })),
        )?;

        Ok(WorkspaceProbeWriteResult {
            login_write_status: first.status,
            workspace_write_status: second.status,
            effective_workspace_id: readback.config.forced_chatgpt_workspace_id,
            effective_login_method: readback.config.forced_login_method,
        })
    }

    fn build_workspace_probe_report(
        &self,
        workspace_id: &str,
        baseline: WorkspaceForceProbeObservation,
        forced_once: WorkspaceForceProbeObservation,
        forced_twice: WorkspaceForceProbeObservation,
        write_result: WorkspaceProbeWriteResult,
    ) -> WorkspaceForceProbeReport {
        let writes_clean = matches!(write_result.login_write_status, WriteStatus::Ok)
            && matches!(write_result.workspace_write_status, WriteStatus::Ok);
        let config_effective = write_result.effective_workspace_id.as_deref() == Some(workspace_id)
            && write_result.effective_login_method.as_deref() == Some("chatgpt");
        let auth_stable = forced_once.authenticated && forced_twice.authenticated;
        let baseline_changed = baseline.account != forced_once.account
            || baseline.rate_limits != forced_once.rate_limits;
        let restart_stable = forced_once == forced_twice;

        let (status, summary) = if !writes_clean {
            (
                WorkspaceForceProbeStatus::Failed,
                "config/value/write reported an overridden or non-ok result".to_string(),
            )
        } else if !config_effective {
            (
                WorkspaceForceProbeStatus::Failed,
                "config/read did not retain the requested workspace override".to_string(),
            )
        } else if !auth_stable {
            (
                WorkspaceForceProbeStatus::Failed,
                "auth did not remain valid after forcing the workspace".to_string(),
            )
        } else if !baseline_changed {
            (
                WorkspaceForceProbeStatus::Failed,
                "account/read and rate limits did not change from baseline; probe cannot prove workspace switching".to_string(),
            )
        } else if !restart_stable {
            (
                WorkspaceForceProbeStatus::Failed,
                "forced workspace state changed across restarts".to_string(),
            )
        } else {
            (
                WorkspaceForceProbeStatus::Passed,
                "workspace override changed account/quota state and remained stable across restarts".to_string(),
            )
        };

        WorkspaceForceProbeReport {
            status,
            summary,
            baseline,
            forced_once,
            forced_twice,
        }
    }
}

impl AppServerSession {
    pub fn connect(
        command: &AppServerCommand,
        codex_home: &Path,
        timeout: Duration,
    ) -> Result<Self> {
        let mut process = ManagedAppServerProcess::spawn(command, codex_home, timeout)?;
        process.initialize()?;
        Ok(Self { process })
    }

    pub fn request<T>(&mut self, method: &str, params: Option<Value>) -> Result<T>
    where
        T: DeserializeOwned,
    {
        self.process.request(method, params)
    }

    pub fn send_notification(&mut self, method: &str, params: Option<Value>) -> Result<()> {
        self.process.send_notification(method, params)
    }

    pub fn next_message(&mut self, timeout: Duration) -> Result<Option<Value>> {
        self.process.next_message(timeout)
    }
}

impl IdentityVerifier for CodexAppServerVerifier {
    fn verify(&self, identity: &CodexIdentity) -> Result<IdentityVerification> {
        sync_managed_config(identity)?;
        self.verify_codex_home(&identity.codex_home)
    }
}

impl ThreadRuntime for CodexAppServerVerifier {
    fn read_thread(&self, identity: &CodexIdentity, thread_id: &str) -> Result<ThreadSnapshot> {
        sync_managed_config(identity)?;
        let mut process =
            ManagedAppServerProcess::spawn(&self.command, &identity.codex_home, self.timeout)?;
        process.initialize()?;
        let response = process.request::<ThreadReadResponse>(
            "thread/read",
            Some(json!({
                "threadId": thread_id,
                "includeTurns": true
            })),
        )?;
        Ok(ThreadSnapshot::from(response.thread))
    }

    fn resume_thread(&self, identity: &CodexIdentity, thread_id: &str) -> Result<ThreadSnapshot> {
        sync_managed_config(identity)?;
        let mut process =
            ManagedAppServerProcess::spawn(&self.command, &identity.codex_home, self.timeout)?;
        process.initialize()?;
        let response = process.request::<ThreadResumeResponse>(
            "thread/resume",
            Some(json!({
                "threadId": thread_id
            })),
        )?;
        Ok(ThreadSnapshot::from(response.thread))
    }
}

impl WorkspaceForceProber for CodexAppServerVerifier {
    fn probe(&self, identity: &CodexIdentity) -> Result<WorkspaceForceProbeReport> {
        let workspace_id = identity
            .forced_chatgpt_workspace_id
            .as_deref()
            .ok_or_else(|| AppError::WorkspaceForceWorkspaceIdMissing {
                identity_id: identity.id.clone(),
            })?;

        let mut baseline_identity = identity.clone();
        baseline_identity.workspace_force_probe = None;
        sync_managed_config(&baseline_identity)?;

        let baseline = self.capture_workspace_probe_observation(&identity.codex_home)?;
        let write_result =
            self.write_workspace_probe_override(&identity.codex_home, workspace_id)?;
        let forced_once = self.capture_workspace_probe_observation(&identity.codex_home)?;
        let forced_twice = self.capture_workspace_probe_observation(&identity.codex_home)?;

        Ok(self.build_workspace_probe_report(
            workspace_id,
            baseline,
            forced_once,
            forced_twice,
            write_result,
        ))
    }
}

#[derive(Debug)]
struct ManagedAppServerProcess {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout_messages: Receiver<Result<Value>>,
    stderr_buffer: Arc<Mutex<String>>,
    timeout: Duration,
}

impl ManagedAppServerProcess {
    fn spawn(command: &AppServerCommand, codex_home: &Path, timeout: Duration) -> Result<Self> {
        let mut process = Command::new(&command.program);
        process.args(&command.args);
        process.env("CODEX_HOME", codex_home);
        process.stdin(Stdio::piped());
        process.stdout(Stdio::piped());
        process.stderr(Stdio::piped());

        let mut child = process.spawn()?;
        let stdin = child.stdin.take().ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "failed to capture app-server stdin",
            ))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "failed to capture app-server stdout",
            ))
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "failed to capture app-server stderr",
            ))
        })?;

        let stdout_messages = spawn_stdout_reader(stdout);
        let stderr_buffer = spawn_stderr_reader(stderr);

        Ok(Self {
            child,
            stdin: BufWriter::new(stdin),
            stdout_messages,
            stderr_buffer,
            timeout,
        })
    }

    fn initialize(&mut self) -> Result<()> {
        let _: Value = self.request(
            "initialize",
            Some(json!({
                "clientInfo": {
                    "name": "codex-switch",
                    "title": "Codex Switch",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": false,
                }
            })),
        )?;
        self.send_notification("initialized", None)
    }

    fn request<T>(&mut self, method: &str, params: Option<Value>) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let request_id = RequestId::String(format!("{method}-{}", request_counter::next()));
        let request = RpcRequest {
            jsonrpc: "2.0",
            id: request_id.clone(),
            method,
            params,
        };
        serde_json::to_writer(&mut self.stdin, &request)?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;

        let started = Instant::now();
        loop {
            let remaining = self.timeout.checked_sub(started.elapsed()).ok_or_else(|| {
                AppError::RpcTimeout {
                    method: method.to_string(),
                    timeout: self.timeout,
                }
            })?;

            match self.stdout_messages.recv_timeout(remaining) {
                Ok(Ok(value)) => {
                    if value.get("id").is_none() {
                        continue;
                    }
                    let response: RpcResponseEnvelope = serde_json::from_value(value)?;
                    if response.id != request_id {
                        continue;
                    }
                    if let Some(error) = response.error {
                        return Err(AppError::RpcServer {
                            method: method.to_string(),
                            code: error.code,
                            message: error.message,
                        });
                    }
                    let payload = response.result.ok_or_else(|| AppError::MissingRpcResult {
                        method: method.to_string(),
                    })?;
                    return serde_json::from_value(payload).map_err(|source| {
                        AppError::RpcPayloadDecode {
                            method: method.to_string(),
                            source,
                        }
                    });
                }
                Ok(Err(error)) => return Err(error),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(AppError::RpcTimeout {
                        method: method.to_string(),
                        timeout: self.timeout,
                    });
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(AppError::AppServerExited {
                        method: method.to_string(),
                        stderr: self.stderr_text(),
                    });
                }
            }
        }
    }

    fn send_notification(&mut self, method: &str, params: Option<Value>) -> Result<()> {
        let notification = RpcNotification {
            jsonrpc: "2.0",
            method,
            params,
        };
        serde_json::to_writer(&mut self.stdin, &notification)?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;
        Ok(())
    }

    fn next_message(&mut self, timeout: Duration) -> Result<Option<Value>> {
        match self.stdout_messages.recv_timeout(timeout) {
            Ok(Ok(value)) => Ok(Some(value)),
            Ok(Err(error)) => Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(AppError::AppServerExited {
                method: "notification".to_string(),
                stderr: self.stderr_text(),
            }),
        }
    }

    fn stderr_text(&self) -> String {
        self.stderr_buffer
            .lock()
            .map(|buffer| buffer.trim().to_string())
            .unwrap_or_else(|_| "stderr unavailable".to_string())
    }
}

impl Drop for ManagedAppServerProcess {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn spawn_stdout_reader(stdout: ChildStdout) -> Receiver<Result<Value>> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(line) if line.trim().is_empty() => continue,
                Ok(line) => {
                    let parsed = serde_json::from_str::<Value>(&line).map_err(Into::into);
                    if sender.send(parsed).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error.into()));
                    break;
                }
            }
        }
    });
    receiver
}

fn spawn_stderr_reader(stderr: ChildStderr) -> Arc<Mutex<String>> {
    let buffer = Arc::new(Mutex::new(String::new()));
    let buffer_clone = Arc::clone(&buffer);
    thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(|line| line.ok()) {
            if let Ok(mut text) = buffer_clone.lock() {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(&line);
            }
        }
    });
    buffer
}

#[derive(Debug, Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: RequestId,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct RpcNotification<'a> {
    jsonrpc: &'static str,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct RpcResponseEnvelope {
    id: RequestId,
    result: Option<Value>,
    error: Option<RpcErrorPayload>,
}

#[derive(Debug, Deserialize)]
struct RpcErrorPayload {
    code: i64,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum RequestId {
    String(String),
    Number(i64),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthStatusPayload {
    auth_method: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ConfigReadResponse {
    config: ConfigPayload,
}

#[derive(Debug, Deserialize)]
struct ConfigPayload {
    forced_chatgpt_workspace_id: Option<String>,
    forced_login_method: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigWriteResponse {
    status: WriteStatus,
    version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
enum WriteStatus {
    Ok,
    OkOverridden,
}

#[derive(Debug, Clone)]
struct WorkspaceProbeWriteResult {
    login_write_status: WriteStatus,
    workspace_write_status: WriteStatus,
    effective_workspace_id: Option<String>,
    effective_login_method: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountReadResponse {
    account: Option<AccountPayload>,
    requires_openai_auth: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
enum AccountPayload {
    #[serde(rename = "apiKey")]
    ApiKey {},
    #[serde(rename = "chatgpt")]
    Chatgpt {
        email: String,
        #[serde(rename = "planType")]
        plan_type: PlanType,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RateLimitsReadResponse {
    rate_limits: RateLimitSnapshot,
    rate_limits_by_limit_id: Option<BTreeMap<String, RateLimitSnapshot>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadReadResponse {
    thread: ThreadPayload,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadResumeResponse {
    thread: ThreadPayload,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadPayload {
    id: String,
    created_at: i64,
    updated_at: i64,
    status: ThreadStatusPayload,
    path: Option<String>,
    turns: Vec<TurnPayload>,
}

#[derive(Debug, Deserialize)]
struct ThreadStatusPayload {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Debug, Deserialize)]
struct TurnPayload {
    id: String,
    status: TurnStatus,
}

impl From<ThreadPayload> for ThreadSnapshot {
    fn from(thread: ThreadPayload) -> Self {
        let latest_turn = thread.turns.last();
        Self {
            thread_id: thread.id,
            created_at: thread.created_at,
            updated_at: thread.updated_at,
            status: thread.status.kind,
            path: thread.path,
            turn_ids: thread.turns.iter().map(|turn| turn.id.clone()).collect(),
            latest_turn_id: latest_turn.map(|turn| turn.id.clone()),
            latest_turn_status: latest_turn.map(|turn| turn.status.clone()),
        }
    }
}

mod request_counter {
    use std::sync::atomic::{AtomicU64, Ordering};

    static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    pub fn next() -> u64 {
        REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::{AppServerCommand, CodexAppServerVerifier, IdentityVerifier, ThreadRuntime};
    use crate::domain::identity::{
        current_timestamp, AccountType, AuthMode, CodexIdentity, ForcedLoginMethod, IdentityId,
        IdentityKind, PlanType,
    };
    use crate::domain::thread::TurnStatus;

    #[test]
    fn verifies_identity_against_fake_app_server() {
        let temp = tempdir().unwrap();
        let script_path = temp.path().join("codex");
        fs::write(
            &script_path,
            r#"#!/bin/sh
if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"authMethod":"chatgpt"}}\n' "$id"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"account":{"type":"chatgpt","email":"qa@example.com","planType":"plus"},"requiresOpenaiAuth":false}}\n' "$id"
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"rateLimits":{"primary":{"usedPercent":12,"windowDurationMins":300},"secondary":null},"rateLimitsByLimitId":{"codex":{"limitId":"codex","limitName":"Codex","planType":"plus","primary":{"usedPercent":12,"windowDurationMins":300,"resetsAt":1700000000},"secondary":{"usedPercent":88,"windowDurationMins":10080,"resetsAt":1700003600}}}}}\n' "$id"
  exit 0
fi
echo "unexpected invocation: $@" >&2
exit 1
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script_path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script_path, permissions).unwrap();
        }

        let identity = CodexIdentity {
            id: IdentityId::from_display_name("Personal Plus").unwrap(),
            display_name: "Personal Plus".to_string(),
            kind: IdentityKind::ChatgptWorkspace,
            auth_mode: AuthMode::Chatgpt,
            codex_home: temp.path().join("home"),
            shared_sessions_root: temp.path().join("shared").join("sessions"),
            forced_login_method: Some(ForcedLoginMethod::Chatgpt),
            forced_chatgpt_workspace_id: None,
            api_key_env_var: None,
            email: None,
            plan_type: None,
            account_type: None,
            authenticated: None,
            last_auth_method: None,
            enabled: true,
            priority: 0,
            notes: None,
            workspace_force_probe: None,
            imported_auth: false,
            created_at: current_timestamp().unwrap(),
            last_verified_at: None,
        };

        fs::create_dir_all(&identity.codex_home).unwrap();
        let verifier = CodexAppServerVerifier::new(
            AppServerCommand::new(&script_path, ["app-server", "--listen", "stdio://"]),
            Duration::from_secs(5),
        );
        let summary = verifier.verify(&identity).unwrap();

        assert!(summary.authenticated);
        assert_eq!(summary.auth_method.as_deref(), Some("chatgpt"));
        assert_eq!(summary.account_type, Some(AccountType::Chatgpt));
        assert_eq!(summary.email.as_deref(), Some("qa@example.com"));
        assert_eq!(summary.plan_type, Some(PlanType::Plus));
        assert_eq!(
            summary
                .rate_limits_by_limit_id
                .get("codex")
                .and_then(|snapshot| snapshot.primary.as_ref())
                .map(|window| window.used_percent),
            Some(12)
        );
    }

    #[test]
    fn reads_and_resumes_threads_against_fake_app_server() {
        let temp = tempdir().unwrap();
        let script_path = temp.path().join("codex");
        fs::write(
            &script_path,
            r#"#!/bin/sh
if [ "$1" = "app-server" ]; then
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":"2"}}\n' "$id"
  read _
  read line
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"method":"thread/read"'*)
      printf '{"jsonrpc":"2.0","id":"%s","result":{"thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000001,"status":{"type":"idle"},"path":"/tmp/thread-1","turns":[{"id":"turn-a","status":"completed"}]}}}\n' "$id"
      ;;
    *'"method":"thread/resume"'*)
      printf '{"jsonrpc":"2.0","id":"%s","result":{"approvalPolicy":"never","approvalsReviewer":"user","cwd":"/tmp","model":"gpt-5.4","modelProvider":"openai","sandbox":"danger-full-access","thread":{"id":"thread-1","createdAt":1700000000,"updatedAt":1700000002,"status":{"type":"active"},"path":"/tmp/thread-1","turns":[{"id":"turn-a","status":"completed"},{"id":"turn-b","status":"inProgress"}]}}}\n' "$id"
      ;;
    *)
      printf '{"jsonrpc":"2.0","id":"%s","error":{"code":-32601,"message":"unknown"}}\n' "$id"
      ;;
  esac
  exit 0
fi
echo "unexpected invocation: $@" >&2
exit 1
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script_path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script_path, permissions).unwrap();
        }

        let identity = CodexIdentity {
            id: IdentityId::from_display_name("Personal Plus").unwrap(),
            display_name: "Personal Plus".to_string(),
            kind: IdentityKind::ChatgptWorkspace,
            auth_mode: AuthMode::Chatgpt,
            codex_home: temp.path().join("home"),
            shared_sessions_root: temp.path().join("shared").join("sessions"),
            forced_login_method: Some(ForcedLoginMethod::Chatgpt),
            forced_chatgpt_workspace_id: None,
            api_key_env_var: None,
            email: None,
            plan_type: None,
            account_type: None,
            authenticated: None,
            last_auth_method: None,
            enabled: true,
            priority: 0,
            notes: None,
            workspace_force_probe: None,
            imported_auth: false,
            created_at: current_timestamp().unwrap(),
            last_verified_at: None,
        };

        fs::create_dir_all(&identity.codex_home).unwrap();
        let runtime = CodexAppServerVerifier::new(
            AppServerCommand::new(&script_path, ["app-server", "--listen", "stdio://"]),
            Duration::from_secs(5),
        );

        let read = runtime.read_thread(&identity, "thread-1").unwrap();
        assert_eq!(read.latest_turn_id.as_deref(), Some("turn-a"));
        assert_eq!(read.latest_turn_status, Some(TurnStatus::Completed));

        let resumed = runtime.resume_thread(&identity, "thread-1").unwrap();
        assert_eq!(resumed.status, "active");
        assert_eq!(resumed.latest_turn_id.as_deref(), Some("turn-b"));
        assert_eq!(resumed.latest_turn_status, Some(TurnStatus::InProgress));
    }
}
