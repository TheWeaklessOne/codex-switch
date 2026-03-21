use std::fs;
use std::path::Path;

use serde_json::Value;

use crate::domain::identity::{
    current_timestamp, AuthMode, CodexIdentity, IdentityId, WorkspaceForceProbe,
    WorkspaceForceProbeStatus,
};
use crate::error::{AppError, Result};
use crate::storage::paths::{atomic_write, copy_file, ensure_directory};
use crate::storage::registry_store::RegistryStore;

const MANAGED_CONFIG_COMMENT: &str = "# Managed by codex-switch";
const MANAGED_ROOT_KEYS: &[&str] = &[
    "cli_auth_credentials_store",
    "forced_login_method",
    "forced_chatgpt_workspace_id",
];

#[derive(Debug)]
pub struct WorkspaceSwitchingService<S> {
    store: S,
}

impl<S> WorkspaceSwitchingService<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

#[derive(Debug, Clone)]
pub struct UpdateWorkspaceForceProbeRequest {
    pub identity_name: String,
    pub status: WorkspaceForceProbeStatus,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceForceProbeObservation {
    pub authenticated: bool,
    pub auth_method: Option<String>,
    pub effective_workspace_id: Option<String>,
    pub effective_login_method: Option<String>,
    pub account: Value,
    pub rate_limits: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceForceProbeReport {
    pub status: WorkspaceForceProbeStatus,
    pub summary: String,
    pub baseline: WorkspaceForceProbeObservation,
    pub forced_once: WorkspaceForceProbeObservation,
    pub forced_twice: WorkspaceForceProbeObservation,
}

impl WorkspaceForceProbeReport {
    pub fn changed_from_baseline(&self) -> bool {
        self.baseline.account != self.forced_once.account
            || self.baseline.rate_limits != self.forced_once.rate_limits
    }

    pub fn stable_after_restart(&self) -> bool {
        self.forced_once == self.forced_twice
    }
}

pub trait WorkspaceForceProber {
    fn probe(&self, identity: &CodexIdentity) -> Result<WorkspaceForceProbeReport>;
}

#[derive(Debug, Clone)]
pub struct WorkspaceForceProbeOutcome {
    pub identity: CodexIdentity,
    pub report: WorkspaceForceProbeReport,
}

impl<S> WorkspaceSwitchingService<S>
where
    S: RegistryStore,
{
    pub fn inspect_identity(&self, identity_name: &str) -> Result<CodexIdentity> {
        let registry = self.store.load()?;
        let identity_id = IdentityId::from_display_name(identity_name)?;
        registry
            .identities
            .get(&identity_id)
            .cloned()
            .ok_or(AppError::IdentityNotFound { identity_id })
    }

    pub fn update_probe(&self, request: UpdateWorkspaceForceProbeRequest) -> Result<CodexIdentity> {
        let mut registry = self.store.load()?;
        let identity_id = IdentityId::from_display_name(&request.identity_name)?;
        let identity = registry.identities.get_mut(&identity_id).ok_or_else(|| {
            AppError::IdentityNotFound {
                identity_id: identity_id.clone(),
            }
        })?;

        validate_workspace_force_identity(identity)?;
        let original_identity = identity.clone();
        let mut updated = identity.clone();
        updated.workspace_force_probe = Some(WorkspaceForceProbe {
            status: request.status,
            updated_at: current_timestamp()?,
            notes: request.notes,
        });

        sync_managed_config(&updated)?;
        *identity = updated.clone();
        self.store.save(&registry).map_err(|error| {
            rollback_config_error("workspace_force.update_probe", error, &original_identity)
        })?;
        Ok(updated)
    }

    pub fn probe_identity<R>(
        &self,
        identity_name: &str,
        prober: &R,
    ) -> Result<WorkspaceForceProbeOutcome>
    where
        R: WorkspaceForceProber,
    {
        let mut registry = self.store.load()?;
        let identity_id = IdentityId::from_display_name(identity_name)?;
        let identity = registry.identities.get_mut(&identity_id).ok_or_else(|| {
            AppError::IdentityNotFound {
                identity_id: identity_id.clone(),
            }
        })?;

        validate_workspace_force_identity(identity)?;
        let original_identity = identity.clone();
        let report = match prober.probe(identity) {
            Ok(report) => report,
            Err(error) => {
                return Err(rollback_config_error(
                    "workspace_force.probe_identity",
                    error,
                    &original_identity,
                ));
            }
        };

        let mut updated = identity.clone();
        updated.workspace_force_probe = Some(WorkspaceForceProbe {
            status: report.status,
            updated_at: current_timestamp()?,
            notes: Some(report.summary.clone()),
        });

        sync_managed_config(&updated).map_err(|error| {
            rollback_config_error("workspace_force.probe_identity", error, &original_identity)
        })?;
        *identity = updated.clone();
        self.store.save(&registry).map_err(|error| {
            rollback_config_error("workspace_force.probe_identity", error, &original_identity)
        })?;
        Ok(WorkspaceForceProbeOutcome {
            identity: updated,
            report,
        })
    }
}

pub fn validate_workspace_force_identity(identity: &CodexIdentity) -> Result<()> {
    if !matches!(identity.auth_mode, AuthMode::Chatgpt) {
        return Err(AppError::WorkspaceForceUnsupported {
            identity_id: identity.id.clone(),
        });
    }

    if identity.forced_chatgpt_workspace_id.is_none() {
        return Err(AppError::WorkspaceForceWorkspaceIdMissing {
            identity_id: identity.id.clone(),
        });
    }

    Ok(())
}

pub fn sync_managed_config(identity: &CodexIdentity) -> Result<()> {
    ensure_directory(&identity.codex_home, 0o700)?;
    let config_path = identity.codex_home.join("config.toml");
    let existing = read_existing_config(&config_path)?;
    let updated = rewrite_managed_config(&existing, identity);
    atomic_write(&config_path, updated.as_bytes(), 0o600)
}

pub fn inject_auth_into_home(identity: &CodexIdentity, target_home: &Path) -> Result<()> {
    let source_auth = identity.codex_home.join("auth.json");
    if !source_auth.exists() {
        return Err(AppError::MissingAuthFile {
            source_home: identity.codex_home.clone(),
        });
    }

    // Phase 1: prepare — read and compute new config before touching any files.
    ensure_directory(target_home, 0o700)?;
    let config_path = target_home.join("config.toml");
    let existing_config = read_existing_config(&config_path)?;
    let updated_config =
        rewrite_config_with_lines(&existing_config, auth_config_lines(&identity.auth_mode));

    // Phase 2: write config first (atomic via temp-file rename).
    atomic_write(&config_path, updated_config.as_bytes(), 0o600)?;

    // Phase 3: copy auth.json; rollback config on failure.
    let target_auth = target_home.join("auth.json");
    if let Err(copy_error) = copy_file(&source_auth, &target_auth, 0o600) {
        // Restore original config so the target home is not left half-switched.
        return match atomic_write(&config_path, existing_config.as_bytes(), 0o600) {
            Ok(()) => Err(copy_error),
            Err(rollback) => Err(AppError::RollbackFailed {
                operation: "inject auth into target home".to_string(),
                primary: copy_error.to_string(),
                rollback: rollback.to_string(),
            }),
        };
    }

    Ok(())
}

fn rollback_config_error(operation: &str, primary: AppError, identity: &CodexIdentity) -> AppError {
    match sync_managed_config(identity) {
        Ok(()) => primary,
        Err(rollback) => AppError::RollbackFailed {
            operation: operation.to_string(),
            primary: primary.to_string(),
            rollback: rollback.to_string(),
        },
    }
}

fn read_existing_config(path: &Path) -> Result<String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(AppError::UnexpectedSymlink {
            path: path.to_path_buf(),
        }),
        Ok(metadata) if !metadata.is_file() => Err(AppError::ExpectedFile {
            path: path.to_path_buf(),
        }),
        Ok(_) => Ok(fs::read_to_string(path)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error.into()),
    }
}

fn rewrite_managed_config(existing: &str, identity: &CodexIdentity) -> String {
    rewrite_config_with_lines(existing, managed_config_lines(identity))
}

fn rewrite_config_with_lines(existing: &str, new_managed_lines: Vec<String>) -> String {
    let existing_lines: Vec<String> = existing.lines().map(str::to_string).collect();
    let split_index = existing_lines
        .iter()
        .position(|line| is_table_header(line))
        .unwrap_or(existing_lines.len());

    let mut preserved_root = existing_lines[..split_index]
        .iter()
        .filter(|line| !is_managed_root_line(line))
        .cloned()
        .collect::<Vec<_>>();
    trim_trailing_blank_lines(&mut preserved_root);

    let mut remainder = existing_lines[split_index..].to_vec();
    trim_leading_blank_lines(&mut remainder);

    let mut rendered = Vec::new();
    rendered.extend(preserved_root);
    if !rendered.is_empty() {
        rendered.push(String::new());
    }
    rendered.extend(new_managed_lines);
    if !remainder.is_empty() {
        rendered.push(String::new());
        rendered.extend(remainder);
    }

    let mut output = rendered.join("\n");
    output.push('\n');
    output
}

fn auth_config_lines(auth_mode: &AuthMode) -> Vec<String> {
    let mut lines = vec![
        MANAGED_CONFIG_COMMENT.to_string(),
        "cli_auth_credentials_store = \"file\"".to_string(),
    ];
    if matches!(auth_mode, AuthMode::Chatgpt) {
        lines.push("forced_login_method = \"chatgpt\"".to_string());
    }
    lines
}

fn managed_config_lines(identity: &CodexIdentity) -> Vec<String> {
    let mut lines = auth_config_lines(&identity.auth_mode);

    if identity.workspace_force_enabled() {
        if let Some(workspace_id) = identity.forced_chatgpt_workspace_id.as_deref() {
            lines.push(format!(
                "forced_chatgpt_workspace_id = \"{}\"",
                escape_toml_basic_string(workspace_id)
            ));
        }
    }

    lines
}

fn is_managed_root_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed == MANAGED_CONFIG_COMMENT
        || MANAGED_ROOT_KEYS.iter().any(|key| {
            trimmed.starts_with(key) && trimmed[key.len()..].trim_start().starts_with('=')
        })
}

fn is_table_header(line: &str) -> bool {
    let trimmed = line.trim();
    (trimmed.starts_with('[') && trimmed.ends_with(']'))
        || (trimmed.starts_with("[[") && trimmed.ends_with("]]"))
}

fn trim_trailing_blank_lines(lines: &mut Vec<String>) {
    while lines.last().is_some_and(|line| line.trim().is_empty()) {
        lines.pop();
    }
}

fn trim_leading_blank_lines(lines: &mut Vec<String>) {
    while lines.first().is_some_and(|line| line.trim().is_empty()) {
        lines.remove(0);
    }
}

fn escape_toml_basic_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        inject_auth_into_home, sync_managed_config, UpdateWorkspaceForceProbeRequest,
        WorkspaceForceProbeObservation, WorkspaceForceProbeReport, WorkspaceForceProber,
        WorkspaceSwitchingService,
    };
    use crate::bootstrap::{AuthBootstrap, BootstrapIdentityRequest};
    use crate::domain::identity::{
        AuthMode, CodexIdentity, IdentityRegistryRecord, WorkspaceForceProbe,
        WorkspaceForceProbeStatus,
    };
    use crate::error::{AppError, Result};
    use crate::storage::registry_store::{JsonRegistryStore, RegistryStore};

    struct StubProber {
        report: WorkspaceForceProbeReport,
    }

    impl WorkspaceForceProber for StubProber {
        fn probe(&self, _identity: &CodexIdentity) -> Result<WorkspaceForceProbeReport> {
            Ok(self.report.clone())
        }
    }

    #[derive(Debug, Clone)]
    struct FailingSaveRegistryStore {
        record: Arc<Mutex<IdentityRegistryRecord>>,
    }

    impl FailingSaveRegistryStore {
        fn new(record: IdentityRegistryRecord) -> Self {
            Self {
                record: Arc::new(Mutex::new(record)),
            }
        }
    }

    impl RegistryStore for FailingSaveRegistryStore {
        fn load(&self) -> Result<IdentityRegistryRecord> {
            Ok(self.record.lock().unwrap().clone())
        }

        fn save(&self, _registry: &IdentityRegistryRecord) -> Result<()> {
            Err(AppError::Io(std::io::Error::other("registry save failed")))
        }
    }

    fn probe_report(status: WorkspaceForceProbeStatus, summary: &str) -> WorkspaceForceProbeReport {
        let baseline = WorkspaceForceProbeObservation {
            authenticated: true,
            auth_method: Some("chatgpt".to_string()),
            effective_workspace_id: None,
            effective_login_method: Some("chatgpt".to_string()),
            account: json!({
                "account": {
                    "type": "chatgpt",
                    "email": "baseline@example.com",
                    "planType": "plus"
                },
                "requiresOpenaiAuth": false
            }),
            rate_limits: json!({
                "rateLimits": {
                    "primary": {
                        "usedPercent": 90,
                        "windowDurationMins": 300
                    },
                    "secondary": null
                }
            }),
        };
        let forced = WorkspaceForceProbeObservation {
            authenticated: true,
            auth_method: Some("chatgpt".to_string()),
            effective_workspace_id: Some("ws_123".to_string()),
            effective_login_method: Some("chatgpt".to_string()),
            account: json!({
                "account": {
                    "type": "chatgpt",
                    "email": "target@example.com",
                    "planType": "plus"
                },
                "requiresOpenaiAuth": false
            }),
            rate_limits: json!({
                "rateLimits": {
                    "primary": {
                        "usedPercent": 10,
                        "windowDurationMins": 300
                    },
                    "secondary": null
                }
            }),
        };

        WorkspaceForceProbeReport {
            status,
            summary: summary.to_string(),
            baseline,
            forced_once: forced.clone(),
            forced_twice: forced,
        }
    }

    #[test]
    fn sync_config_preserves_existing_root_and_tables() {
        let temp = tempdir().unwrap();
        let home = temp.path().join("home");
        let mut identity = AuthBootstrap
            .prepare_identity(BootstrapIdentityRequest {
                display_name: "Workspace".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: Some(home.clone()),
                import_auth_from_home: None,
                overwrite_config: true,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: Some("ws_123".to_string()),
            })
            .unwrap()
            .identity;
        fs::write(
            home.join("config.toml"),
            "approval_policy = \"never\"\n\n[profiles.default]\nmodel = \"gpt-5\"\n",
        )
        .unwrap();
        identity.workspace_force_probe = Some(WorkspaceForceProbe {
            status: WorkspaceForceProbeStatus::Passed,
            updated_at: 1,
            notes: None,
        });

        sync_managed_config(&identity).unwrap();

        let config = fs::read_to_string(home.join("config.toml")).unwrap();
        assert!(config.contains("approval_policy = \"never\""));
        assert!(config.contains("[profiles.default]"));
        assert!(config.contains("forced_chatgpt_workspace_id = \"ws_123\""));
        assert!(config.contains("forced_login_method = \"chatgpt\""));
        assert!(config.contains("cli_auth_credentials_store = \"file\""));
    }

    #[test]
    fn sync_config_removes_workspace_key_until_probe_passes() {
        let temp = tempdir().unwrap();
        let mut identity = AuthBootstrap
            .prepare_identity(BootstrapIdentityRequest {
                display_name: "Workspace".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: Some("ws_123".to_string()),
            })
            .unwrap()
            .identity;
        identity.workspace_force_probe = Some(WorkspaceForceProbe {
            status: WorkspaceForceProbeStatus::Failed,
            updated_at: 1,
            notes: Some("did not survive restart".to_string()),
        });

        sync_managed_config(&identity).unwrap();
        let config = fs::read_to_string(identity.codex_home.join("config.toml")).unwrap();
        assert!(!config.contains("forced_chatgpt_workspace_id"));
        assert!(config.contains("forced_login_method = \"chatgpt\""));
    }

    #[test]
    fn updates_probe_state_for_registered_identity() {
        let temp = tempdir().unwrap();
        let store = JsonRegistryStore::new(temp.path());
        let registry = crate::identity_registry::IdentityRegistryService::new(store.clone());
        registry
            .register_identity(BootstrapIdentityRequest {
                display_name: "Workspace".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: Some("ws_123".to_string()),
            })
            .unwrap();

        let service = WorkspaceSwitchingService::new(store);
        let updated = service
            .update_probe(UpdateWorkspaceForceProbeRequest {
                identity_name: "Workspace".to_string(),
                status: WorkspaceForceProbeStatus::Passed,
                notes: Some("Probe B passed".to_string()),
            })
            .unwrap();

        assert_eq!(
            updated.workspace_force_probe.unwrap().status,
            WorkspaceForceProbeStatus::Passed
        );
        let config = fs::read_to_string(updated.codex_home.join("config.toml")).unwrap();
        assert!(config.contains("forced_chatgpt_workspace_id = \"ws_123\""));
    }

    #[test]
    fn rejects_workspace_probe_updates_for_api_identities() {
        let temp = tempdir().unwrap();
        let store = JsonRegistryStore::new(temp.path());
        let registry = crate::identity_registry::IdentityRegistryService::new(store.clone());
        registry
            .register_identity(BootstrapIdentityRequest {
                display_name: "API".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Apikey,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: None,
            })
            .unwrap();

        let service = WorkspaceSwitchingService::new(store);
        let error = service
            .update_probe(UpdateWorkspaceForceProbeRequest {
                identity_name: "API".to_string(),
                status: WorkspaceForceProbeStatus::Passed,
                notes: None,
            })
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "identity api does not support forced workspace switching"
        );
    }

    #[test]
    fn probe_identity_persists_passed_result_and_enables_force() {
        let temp = tempdir().unwrap();
        let store = JsonRegistryStore::new(temp.path());
        let registry = crate::identity_registry::IdentityRegistryService::new(store.clone());
        registry
            .register_identity(BootstrapIdentityRequest {
                display_name: "Workspace".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: Some("ws_123".to_string()),
            })
            .unwrap();

        let service = WorkspaceSwitchingService::new(store);
        let outcome = service
            .probe_identity(
                "Workspace",
                &StubProber {
                    report: probe_report(
                        WorkspaceForceProbeStatus::Passed,
                        "probe passed after restart validation",
                    ),
                },
            )
            .unwrap();

        assert_eq!(outcome.report.status, WorkspaceForceProbeStatus::Passed);
        assert!(outcome.identity.workspace_force_enabled());
        let config = fs::read_to_string(outcome.identity.codex_home.join("config.toml")).unwrap();
        assert!(config.contains("forced_chatgpt_workspace_id = \"ws_123\""));
    }

    #[test]
    fn probe_identity_persists_failed_result_and_removes_force() {
        let temp = tempdir().unwrap();
        let store = JsonRegistryStore::new(temp.path());
        let registry = crate::identity_registry::IdentityRegistryService::new(store.clone());
        let mut initial = registry
            .register_identity(BootstrapIdentityRequest {
                display_name: "Workspace".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: Some("ws_123".to_string()),
            })
            .unwrap()
            .identity;
        initial.workspace_force_probe = Some(WorkspaceForceProbe {
            status: WorkspaceForceProbeStatus::Passed,
            updated_at: 1,
            notes: Some("previous pass".to_string()),
        });
        sync_managed_config(&initial).unwrap();

        let service = WorkspaceSwitchingService::new(store);
        let outcome = service
            .probe_identity(
                "Workspace",
                &StubProber {
                    report: probe_report(
                        WorkspaceForceProbeStatus::Failed,
                        "forced state was not stable across restarts",
                    ),
                },
            )
            .unwrap();

        assert_eq!(outcome.report.status, WorkspaceForceProbeStatus::Failed);
        assert!(!outcome.identity.workspace_force_enabled());
        let config = fs::read_to_string(outcome.identity.codex_home.join("config.toml")).unwrap();
        assert!(!config.contains("forced_chatgpt_workspace_id"));
    }

    #[test]
    fn update_probe_rolls_back_config_when_registry_save_fails() {
        let temp = tempdir().unwrap();
        let identity = AuthBootstrap
            .prepare_identity(BootstrapIdentityRequest {
                display_name: "Workspace".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: Some("ws_123".to_string()),
            })
            .unwrap()
            .identity;
        let store = FailingSaveRegistryStore::new(IdentityRegistryRecord {
            version: crate::domain::identity::REGISTRY_VERSION,
            identities: std::collections::BTreeMap::from([(identity.id.clone(), identity.clone())]),
        });
        let service = WorkspaceSwitchingService::new(store.clone());

        let error = service
            .update_probe(UpdateWorkspaceForceProbeRequest {
                identity_name: "Workspace".to_string(),
                status: WorkspaceForceProbeStatus::Passed,
                notes: Some("passed".to_string()),
            })
            .unwrap_err();
        assert!(error.to_string().contains("registry save failed"));

        let stored = store
            .load()
            .unwrap()
            .identities
            .get(&identity.id)
            .unwrap()
            .clone();
        assert_eq!(stored.workspace_force_probe, None);

        let config = fs::read_to_string(identity.codex_home.join("config.toml")).unwrap();
        assert!(!config.contains("forced_chatgpt_workspace_id"));
    }

    #[test]
    fn probe_identity_rolls_back_config_when_registry_save_fails() {
        let temp = tempdir().unwrap();
        let identity = AuthBootstrap
            .prepare_identity(BootstrapIdentityRequest {
                display_name: "Workspace".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: Some("ws_123".to_string()),
            })
            .unwrap()
            .identity;
        let store = FailingSaveRegistryStore::new(IdentityRegistryRecord {
            version: crate::domain::identity::REGISTRY_VERSION,
            identities: std::collections::BTreeMap::from([(identity.id.clone(), identity.clone())]),
        });
        let service = WorkspaceSwitchingService::new(store.clone());

        let error = service
            .probe_identity(
                "Workspace",
                &StubProber {
                    report: probe_report(
                        WorkspaceForceProbeStatus::Passed,
                        "workspace override changed account/quota state and remained stable across restarts",
                    ),
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("registry save failed"));

        let stored = store
            .load()
            .unwrap()
            .identities
            .get(&identity.id)
            .unwrap()
            .clone();
        assert_eq!(stored.workspace_force_probe, None);

        let config = fs::read_to_string(identity.codex_home.join("config.toml")).unwrap();
        assert!(!config.contains("forced_chatgpt_workspace_id"));
    }

    #[test]
    fn inject_copies_auth_and_writes_only_auth_config_lines() {
        let temp = tempdir().unwrap();
        let mut identity = AuthBootstrap
            .prepare_identity(BootstrapIdentityRequest {
                display_name: "Donor".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: Some("ws_999".to_string()),
            })
            .unwrap()
            .identity;

        identity.workspace_force_probe = Some(WorkspaceForceProbe {
            status: WorkspaceForceProbeStatus::Passed,
            updated_at: 1,
            notes: None,
        });

        fs::write(
            identity.codex_home.join("auth.json"),
            r#"{"auth_mode":"chatgpt","tokens":{"refresh_token":"rt_inject_test"}}"#,
        )
        .unwrap();

        let target = temp.path().join("target-home");
        fs::create_dir_all(&target).unwrap();
        fs::write(
            target.join("config.toml"),
            "model = \"gpt-5\"\n\n[profiles.default]\nkey = \"val\"\n",
        )
        .unwrap();

        inject_auth_into_home(&identity, &target).unwrap();

        let auth = fs::read_to_string(target.join("auth.json")).unwrap();
        assert!(auth.contains("rt_inject_test"));

        let config = fs::read_to_string(target.join("config.toml")).unwrap();
        assert!(config.contains("cli_auth_credentials_store = \"file\""));
        assert!(config.contains("forced_login_method = \"chatgpt\""));
        assert!(config.contains("model = \"gpt-5\""));
        assert!(config.contains("[profiles.default]"));
        assert!(
            !config.contains("forced_chatgpt_workspace_id"),
            "inject must not carry workspace_id into target"
        );
    }
}
