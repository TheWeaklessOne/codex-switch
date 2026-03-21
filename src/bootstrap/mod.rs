use std::path::{Path, PathBuf};

use crate::domain::identity::{
    current_timestamp, AuthMode, CodexIdentity, ForcedLoginMethod, IdentityId,
};
use crate::error::{AppError, Result};
use crate::storage::paths::{
    atomic_write, copy_file, default_home_path, ensure_directory, ensure_sessions_symlink,
    resolve_path, shared_sessions_path,
};

#[derive(Debug, Clone)]
pub struct BootstrapPlan {
    pub identity_id: IdentityId,
    pub base_root: PathBuf,
    pub codex_home: PathBuf,
    pub shared_sessions_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct BootstrapIdentityRequest {
    pub display_name: String,
    pub base_root: PathBuf,
    pub auth_mode: AuthMode,
    pub home_override: Option<PathBuf>,
    pub import_auth_from_home: Option<PathBuf>,
    pub overwrite_config: bool,
    pub api_key_env_var: Option<String>,
    pub forced_chatgpt_workspace_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BootstrapIdentityResult {
    pub identity: CodexIdentity,
    pub next_login_command: Option<String>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct AuthBootstrap;

impl AuthBootstrap {
    pub fn plan_identity(&self, request: &BootstrapIdentityRequest) -> Result<BootstrapPlan> {
        let identity_id = IdentityId::from_display_name(&request.display_name)?;
        let base_root = resolve_path(&request.base_root)?;
        let codex_home = match request.home_override.as_ref() {
            Some(path) => resolve_path(path)?,
            None => default_home_path(&base_root, identity_id.as_str()),
        };
        let shared_sessions_root = shared_sessions_path(&base_root);

        Ok(BootstrapPlan {
            identity_id,
            base_root,
            codex_home,
            shared_sessions_root,
        })
    }

    pub fn prepare_identity(
        &self,
        request: BootstrapIdentityRequest,
    ) -> Result<BootstrapIdentityResult> {
        let plan = self.plan_identity(&request)?;

        ensure_directory(&plan.base_root, 0o700)?;
        ensure_directory(&plan.base_root.join("homes"), 0o700)?;
        ensure_directory(&plan.base_root.join("shared"), 0o700)?;
        ensure_directory(&plan.codex_home, 0o700)?;
        ensure_sessions_symlink(
            &plan.codex_home.join("sessions"),
            &plan.shared_sessions_root,
        )?;

        write_config(
            &plan.codex_home.join("config.toml"),
            request.auth_mode,
            request.overwrite_config,
        )?;

        if let Some(source_home) = request.import_auth_from_home.as_deref() {
            import_auth(source_home, &plan.codex_home)?;
        }

        let api_key_env_var = match request.auth_mode {
            AuthMode::Chatgpt => None,
            AuthMode::Apikey => Some(
                request
                    .api_key_env_var
                    .unwrap_or_else(|| "OPENAI_API_KEY".to_string()),
            ),
        };

        validate_env_var_name(api_key_env_var.as_deref())?;

        let identity = CodexIdentity {
            id: plan.identity_id.clone(),
            display_name: request.display_name,
            kind: request.auth_mode.identity_kind(),
            auth_mode: request.auth_mode,
            codex_home: plan.codex_home.clone(),
            shared_sessions_root: plan.shared_sessions_root,
            forced_login_method: matches!(request.auth_mode, AuthMode::Chatgpt)
                .then_some(ForcedLoginMethod::Chatgpt),
            forced_chatgpt_workspace_id: request.forced_chatgpt_workspace_id,
            api_key_env_var: api_key_env_var.clone(),
            email: None,
            plan_type: None,
            account_type: None,
            authenticated: None,
            last_auth_method: None,
            enabled: true,
            priority: 0,
            notes: None,
            workspace_force_probe: None,
            imported_auth: request.import_auth_from_home.is_some(),
            created_at: current_timestamp()?,
            last_verified_at: None,
        };

        let next_login_command = if request.import_auth_from_home.is_some() {
            None
        } else {
            Some(match request.auth_mode {
                AuthMode::Chatgpt => {
                    format!("CODEX_HOME=\"{}\" codex login", plan.codex_home.display())
                }
                AuthMode::Apikey => format!(
                    "printenv {} | CODEX_HOME=\"{}\" codex login --with-api-key",
                    api_key_env_var.unwrap_or_else(|| "OPENAI_API_KEY".to_string()),
                    plan.codex_home.display()
                ),
            })
        };

        Ok(BootstrapIdentityResult {
            identity,
            next_login_command,
        })
    }
}

fn write_config(path: &Path, auth_mode: AuthMode, overwrite_config: bool) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(AppError::UnexpectedSymlink {
                path: path.to_path_buf(),
            });
        }
        Ok(_) if !overwrite_config => {
            return Err(AppError::ConfigAlreadyExists {
                path: path.to_path_buf(),
            });
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let mut lines = vec![
        "# Managed by codex-switch".to_string(),
        "cli_auth_credentials_store = \"file\"".to_string(),
    ];

    if matches!(auth_mode, AuthMode::Chatgpt) {
        lines.push("forced_login_method = \"chatgpt\"".to_string());
    }

    let mut content = lines.join("\n");
    content.push('\n');
    atomic_write(path, content.as_bytes(), 0o600)
}

fn import_auth(source_home: &Path, target_home: &Path) -> Result<()> {
    let source_home = resolve_path(source_home)?;
    let source_auth = source_home.join("auth.json");
    if !source_auth.exists() {
        return Err(AppError::MissingAuthFile { source_home });
    }
    copy_file(&source_auth, &target_home.join("auth.json"), 0o600)
}

fn validate_env_var_name(value: Option<&str>) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };

    let valid = !value.is_empty()
        && value.chars().enumerate().all(|(index, character)| {
            if index == 0 {
                character == '_' || character.is_ascii_alphabetic()
            } else {
                character == '_' || character.is_ascii_alphanumeric()
            }
        });
    if !valid {
        return Err(AppError::InvalidEnvironmentVariableName {
            name: value.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{AuthBootstrap, BootstrapIdentityRequest};
    use crate::domain::identity::AuthMode;

    #[test]
    fn prepares_chatgpt_home_with_shared_sessions() {
        let temp = tempdir().unwrap();
        let bootstrap = AuthBootstrap;
        let result = bootstrap
            .prepare_identity(BootstrapIdentityRequest {
                display_name: "Personal Plus".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: None,
            })
            .unwrap();

        let config = fs::read_to_string(result.identity.codex_home.join("config.toml")).unwrap();
        assert!(config.contains("cli_auth_credentials_store = \"file\""));
        assert!(config.contains("forced_login_method = \"chatgpt\""));
        assert_eq!(
            fs::read_link(result.identity.codex_home.join("sessions")).unwrap(),
            temp.path().join("shared").join("sessions")
        );
        let expected_command = format!(
            "CODEX_HOME=\"{}\" codex login",
            result.identity.codex_home.display()
        );
        assert_eq!(
            result.next_login_command.as_deref(),
            Some(expected_command.as_str())
        );
    }

    #[test]
    fn prepares_api_key_home_with_env_var_login_hint() {
        let temp = tempdir().unwrap();
        let bootstrap = AuthBootstrap;
        let result = bootstrap
            .prepare_identity(BootstrapIdentityRequest {
                display_name: "API Fallback".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Apikey,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: Some("CLIENT_A_OPENAI_API_KEY".to_string()),
                forced_chatgpt_workspace_id: None,
            })
            .unwrap();

        let expected_command = format!(
            "printenv CLIENT_A_OPENAI_API_KEY | CODEX_HOME=\"{}\" codex login --with-api-key",
            result.identity.codex_home.display()
        );
        assert_eq!(
            result.next_login_command.as_deref(),
            Some(expected_command.as_str())
        );
        assert_eq!(
            result.identity.api_key_env_var.as_deref(),
            Some("CLIENT_A_OPENAI_API_KEY")
        );
    }
}
