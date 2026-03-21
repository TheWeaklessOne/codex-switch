use std::fmt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{AppError, Result};

pub const REGISTRY_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdentityId(String);

impl IdentityId {
    pub fn from_display_name(value: &str) -> Result<Self> {
        let mut slug = String::new();
        let mut previous_was_dash = false;

        for character in value.trim().chars() {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                previous_was_dash = false;
                slug.push(character.to_ascii_lowercase());
                continue;
            }

            if !previous_was_dash {
                previous_was_dash = true;
                slug.push('-');
            }
        }

        let normalized = slug.trim_matches('-').to_string();
        if normalized.is_empty() {
            return Err(AppError::EmptyIdentitySlug);
        }

        Ok(Self(normalized))
    }

    pub fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdentityId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityKind {
    ChatgptWorkspace,
    ApiKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    Chatgpt,
    Apikey,
}

impl AuthMode {
    pub fn identity_kind(self) -> IdentityKind {
        match self {
            Self::Chatgpt => IdentityKind::ChatgptWorkspace,
            Self::Apikey => IdentityKind::ApiKey,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chatgpt => "chatgpt",
            Self::Apikey => "apikey",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ForcedLoginMethod {
    Chatgpt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceForceProbeStatus {
    Pending,
    Passed,
    Failed,
}

impl fmt::Display for WorkspaceForceProbeStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Pending => "pending",
            Self::Passed => "passed",
            Self::Failed => "failed",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceForceProbe {
    pub status: WorkspaceForceProbeStatus,
    pub updated_at: i64,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanType {
    #[serde(rename = "free")]
    Free,
    #[serde(rename = "go")]
    Go,
    #[serde(rename = "plus")]
    Plus,
    #[serde(rename = "pro")]
    Pro,
    #[serde(rename = "team")]
    Team,
    #[serde(rename = "business")]
    Business,
    #[serde(rename = "enterprise")]
    Enterprise,
    #[serde(rename = "edu")]
    Edu,
    #[serde(rename = "unknown")]
    Unknown,
}

impl fmt::Display for PlanType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Free => "free",
            Self::Go => "go",
            Self::Plus => "plus",
            Self::Pro => "pro",
            Self::Team => "team",
            Self::Business => "business",
            Self::Enterprise => "enterprise",
            Self::Edu => "edu",
            Self::Unknown => "unknown",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountType {
    #[serde(rename = "chatgpt")]
    Chatgpt,
    #[serde(rename = "apiKey")]
    ApiKey,
}

impl fmt::Display for AccountType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Chatgpt => "chatgpt",
            Self::ApiKey => "apiKey",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexIdentity {
    pub id: IdentityId,
    pub display_name: String,
    pub kind: IdentityKind,
    pub auth_mode: AuthMode,
    pub codex_home: PathBuf,
    pub shared_sessions_root: PathBuf,
    pub forced_login_method: Option<ForcedLoginMethod>,
    pub forced_chatgpt_workspace_id: Option<String>,
    pub api_key_env_var: Option<String>,
    pub email: Option<String>,
    pub plan_type: Option<PlanType>,
    pub account_type: Option<AccountType>,
    pub authenticated: Option<bool>,
    pub last_auth_method: Option<String>,
    pub enabled: bool,
    pub priority: u32,
    pub notes: Option<String>,
    pub workspace_force_probe: Option<WorkspaceForceProbe>,
    pub imported_auth: bool,
    pub created_at: i64,
    pub last_verified_at: Option<i64>,
}

impl CodexIdentity {
    pub fn workspace_force_probe_status(&self) -> WorkspaceForceProbeStatus {
        self.workspace_force_probe
            .as_ref()
            .map(|probe| probe.status)
            .unwrap_or(WorkspaceForceProbeStatus::Pending)
    }

    pub fn workspace_force_enabled(&self) -> bool {
        matches!(self.auth_mode, AuthMode::Chatgpt)
            && self.forced_chatgpt_workspace_id.is_some()
            && matches!(
                self.workspace_force_probe_status(),
                WorkspaceForceProbeStatus::Passed
            )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityRegistryRecord {
    pub version: u32,
    pub identities: std::collections::BTreeMap<IdentityId, CodexIdentity>,
}

impl Default for IdentityRegistryRecord {
    fn default() -> Self {
        Self {
            version: REGISTRY_VERSION,
            identities: std::collections::BTreeMap::new(),
        }
    }
}

pub fn current_timestamp() -> Result<i64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?;
    Ok(now.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use super::{CodexIdentity, IdentityId, WorkspaceForceProbeStatus};

    #[test]
    fn slugifies_display_name() {
        let slug = IdentityId::from_display_name(" Client A Business  ").unwrap();
        assert_eq!(slug.as_str(), "client-a-business");
    }

    #[test]
    fn retains_supported_slug_characters() {
        let slug = IdentityId::from_display_name("client_a.v2-prod").unwrap();
        assert_eq!(slug.as_str(), "client_a.v2-prod");
    }

    #[test]
    fn rejects_empty_slug() {
        let error = IdentityId::from_display_name("   !!! ").unwrap_err();
        assert_eq!(error.to_string(), "identity name resolves to an empty slug");
    }

    #[test]
    fn workspace_force_requires_passed_probe() {
        let mut identity: CodexIdentity = serde_json::from_value(serde_json::json!({
            "id": "personal-plus",
            "display_name": "Personal Plus",
            "kind": "chatgpt_workspace",
            "auth_mode": "chatgpt",
            "codex_home": "/tmp/home",
            "shared_sessions_root": "/tmp/shared/sessions",
            "forced_login_method": "chatgpt",
            "forced_chatgpt_workspace_id": "ws_123",
            "api_key_env_var": null,
            "email": null,
            "plan_type": null,
            "account_type": null,
            "authenticated": null,
            "last_auth_method": null,
            "enabled": true,
            "priority": 0,
            "notes": null,
            "imported_auth": false,
            "created_at": 0,
            "last_verified_at": null
        }))
        .unwrap();

        assert_eq!(
            identity.workspace_force_probe_status(),
            WorkspaceForceProbeStatus::Pending
        );
        assert!(!identity.workspace_force_enabled());

        identity.workspace_force_probe = Some(super::WorkspaceForceProbe {
            status: WorkspaceForceProbeStatus::Passed,
            updated_at: 1,
            notes: None,
        });

        assert!(identity.workspace_force_enabled());
    }
}
