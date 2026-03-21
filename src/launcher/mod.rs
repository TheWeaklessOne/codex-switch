use std::ffi::OsString;
use std::io::{Read, Write};
use std::process::{Command, Stdio};

use crate::domain::identity::{AuthMode, CodexIdentity};
use crate::error::{AppError, Result};
use crate::workspace_switching::sync_managed_config;

#[derive(Debug, Clone)]
pub struct LaunchOutcome {
    pub identity: CodexIdentity,
    pub command: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CapturedLaunchFailure {
    pub identity: CodexIdentity,
    pub command: Vec<String>,
    pub code: String,
    pub stdout: String,
    pub stderr: String,
}

impl CapturedLaunchFailure {
    pub fn to_app_error(&self) -> AppError {
        AppError::ChildProcessFailed {
            program: "codex".to_string(),
            code: self.code.clone(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CodexLauncher;

impl CodexLauncher {
    pub fn launch_codex(
        &self,
        identity: &CodexIdentity,
        args: &[OsString],
    ) -> Result<LaunchOutcome> {
        self.run(identity, args.iter().cloned())
    }

    pub fn launch_app_server(
        &self,
        identity: &CodexIdentity,
        args: &[OsString],
    ) -> Result<LaunchOutcome> {
        let mut command = Vec::with_capacity(args.len() + 1);
        command.push(OsString::from("app-server"));
        command.extend(args.iter().cloned());
        self.run(identity, command)
    }

    pub fn launch_resume(
        &self,
        identity: &CodexIdentity,
        thread_id: &str,
        args: &[OsString],
    ) -> Result<LaunchOutcome> {
        let mut command = Vec::with_capacity(args.len() + 2);
        command.push(OsString::from("resume"));
        command.push(OsString::from(thread_id));
        command.extend(args.iter().cloned());
        self.run(identity, command)
    }

    pub fn launch_login(&self, identity: &CodexIdentity) -> Result<LaunchOutcome> {
        match identity.auth_mode {
            AuthMode::Chatgpt => self.run(identity, [OsString::from("login")]),
            AuthMode::Apikey => {
                sync_managed_config(identity)?;
                ensure_launch_environment(identity)?;
                let env_var = identity
                    .api_key_env_var
                    .as_deref()
                    .unwrap_or("OPENAI_API_KEY");
                let api_key = std::env::var(env_var).map_err(|_| {
                    AppError::MissingApiKeyEnvironmentVariable {
                        identity_id: identity.id.clone(),
                        name: env_var.to_string(),
                    }
                })?;
                self.run_with_piped_stdin(
                    identity,
                    vec![OsString::from("login"), OsString::from("--with-api-key")],
                    &(api_key + "\n"),
                )
            }
        }
    }

    pub fn launch_codex_captured(
        &self,
        identity: &CodexIdentity,
        args: &[OsString],
    ) -> Result<std::result::Result<LaunchOutcome, CapturedLaunchFailure>> {
        self.run_captured(identity, args.iter().cloned())
    }

    fn run(
        &self,
        identity: &CodexIdentity,
        args: impl IntoIterator<Item = OsString>,
    ) -> Result<LaunchOutcome> {
        sync_managed_config(identity)?;
        ensure_launch_environment(identity)?;
        let command_arguments: Vec<OsString> = args.into_iter().collect();

        let status = Command::new("codex")
            .env("CODEX_HOME", &identity.codex_home)
            .args(&command_arguments)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()?;

        if !status.success() {
            let code = status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string());
            return Err(AppError::ChildProcessFailed {
                program: "codex".to_string(),
                code,
            });
        }

        Ok(LaunchOutcome {
            identity: identity.clone(),
            command: command_arguments
                .iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
        })
    }

    fn run_with_piped_stdin(
        &self,
        identity: &CodexIdentity,
        command_arguments: Vec<OsString>,
        stdin_payload: &str,
    ) -> Result<LaunchOutcome> {
        let mut child = Command::new("codex")
            .env("CODEX_HOME", &identity.codex_home)
            .args(&command_arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        {
            let stdin = child.stdin.as_mut().ok_or_else(|| {
                AppError::Io(std::io::Error::other(
                    "failed to open codex stdin for managed login",
                ))
            })?;
            stdin.write_all(stdin_payload.as_bytes())?;
        }

        let status = child.wait()?;
        if !status.success() {
            let code = status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string());
            return Err(AppError::ChildProcessFailed {
                program: "codex".to_string(),
                code,
            });
        }

        Ok(LaunchOutcome {
            identity: identity.clone(),
            command: command_arguments
                .iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
        })
    }

    fn run_captured(
        &self,
        identity: &CodexIdentity,
        args: impl IntoIterator<Item = OsString>,
    ) -> Result<std::result::Result<LaunchOutcome, CapturedLaunchFailure>> {
        sync_managed_config(identity)?;
        ensure_launch_environment(identity)?;
        let command_arguments: Vec<OsString> = args.into_iter().collect();
        let rendered_command = command_arguments
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        let mut child = Command::new("codex")
            .env("CODEX_HOME", &identity.codex_home)
            .args(&command_arguments)
            .stdin(Stdio::inherit())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = child.stdout.take().ok_or_else(|| {
            AppError::Io(std::io::Error::other(
                "failed to capture codex stdout for auto-failover",
            ))
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            AppError::Io(std::io::Error::other(
                "failed to capture codex stderr for auto-failover",
            ))
        })?;

        let stdout_handle = tee_stream(stdout, false);
        let stderr_handle = tee_stream(stderr, true);
        let status = child.wait()?;
        let stdout = join_stream(stdout_handle)?;
        let stderr = join_stream(stderr_handle)?;

        if status.success() {
            return Ok(Ok(LaunchOutcome {
                identity: identity.clone(),
                command: rendered_command,
            }));
        }

        let code = status
            .code()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "signal".to_string());
        Ok(Err(CapturedLaunchFailure {
            identity: identity.clone(),
            command: rendered_command,
            code,
            stdout,
            stderr,
        }))
    }
}

fn ensure_launch_environment(identity: &CodexIdentity) -> Result<()> {
    if matches!(identity.auth_mode, AuthMode::Apikey) {
        let env_var = identity
            .api_key_env_var
            .as_deref()
            .unwrap_or("OPENAI_API_KEY");
        if std::env::var_os(env_var).is_none() {
            return Err(AppError::MissingApiKeyEnvironmentVariable {
                identity_id: identity.id.clone(),
                name: env_var.to_string(),
            });
        }
    }
    Ok(())
}

fn tee_stream<R>(mut reader: R, stderr: bool) -> std::thread::JoinHandle<std::io::Result<Vec<u8>>>
where
    R: Read + Send + 'static,
{
    std::thread::spawn(move || {
        let mut collected = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            collected.extend_from_slice(&buffer[..bytes_read]);
            if stderr {
                let mut output = std::io::stderr().lock();
                output.write_all(&buffer[..bytes_read])?;
                output.flush()?;
            } else {
                let mut output = std::io::stdout().lock();
                output.write_all(&buffer[..bytes_read])?;
                output.flush()?;
            }
        }

        Ok(collected)
    })
}

fn join_stream(handle: std::thread::JoinHandle<std::io::Result<Vec<u8>>>) -> Result<String> {
    let bytes = handle.join().map_err(|_| {
        AppError::Io(std::io::Error::other(
            "failed to join captured codex output thread",
        ))
    })??;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::CodexLauncher;
    use crate::domain::identity::{
        current_timestamp, AuthMode, CodexIdentity, ForcedLoginMethod, IdentityId, IdentityKind,
    };

    #[test]
    fn rejects_api_key_launch_without_required_env_var() {
        let temp = tempdir().unwrap();
        let launcher = CodexLauncher;
        let identity = CodexIdentity {
            id: IdentityId::from_display_name("API Fallback").unwrap(),
            display_name: "API Fallback".to_string(),
            kind: IdentityKind::ApiKey,
            auth_mode: AuthMode::Apikey,
            codex_home: temp.path().join("home"),
            shared_sessions_root: temp.path().join("shared").join("sessions"),
            forced_login_method: None,
            forced_chatgpt_workspace_id: None,
            api_key_env_var: Some("MISSING_KEY".to_string()),
            email: None,
            plan_type: None,
            account_type: None,
            authenticated: Some(true),
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

        let error = launcher.launch_codex(&identity, &[]).unwrap_err();
        assert!(error
            .to_string()
            .contains("requires environment variable MISSING_KEY"));
    }

    #[test]
    fn chatgpt_identities_do_not_require_extra_launch_env() {
        let temp = tempdir().unwrap();
        let identity = CodexIdentity {
            id: IdentityId::from_display_name("Primary").unwrap(),
            display_name: "Primary".to_string(),
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
            authenticated: Some(true),
            last_auth_method: None,
            enabled: true,
            priority: 0,
            notes: None,
            workspace_force_probe: None,
            imported_auth: false,
            created_at: current_timestamp().unwrap(),
            last_verified_at: None,
        };

        assert!(super::ensure_launch_environment(&identity).is_ok());
    }
}
