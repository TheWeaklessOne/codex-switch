use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::domain::identity::{
    AccountType, AuthMode, CodexIdentity, ForcedLoginMethod, IdentityId, IdentityRegistryRecord,
    PlanType, REGISTRY_VERSION,
};
use crate::error::{AppError, Result};
use crate::storage::paths::{atomic_write, registry_path};

pub trait RegistryStore {
    fn load(&self) -> Result<IdentityRegistryRecord>;
    fn save(&self, registry: &IdentityRegistryRecord) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct JsonRegistryStore {
    path: PathBuf,
}

impl JsonRegistryStore {
    pub fn new(base_root: &Path) -> Self {
        Self {
            path: registry_path(base_root),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl RegistryStore for JsonRegistryStore {
    fn load(&self) -> Result<IdentityRegistryRecord> {
        match std::fs::read(&self.path) {
            Ok(bytes) => load_registry_bytes(&bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(IdentityRegistryRecord::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn save(&self, registry: &IdentityRegistryRecord) -> Result<()> {
        let serialized = serde_json::to_vec_pretty(registry)?;
        let mut payload = serialized;
        payload.push(b'\n');
        atomic_write(&self.path, &payload, 0o600)
    }
}

fn load_registry_bytes(bytes: &[u8]) -> Result<IdentityRegistryRecord> {
    match serde_json::from_slice::<IdentityRegistryRecord>(bytes) {
        Ok(registry) => {
            if registry.version != REGISTRY_VERSION {
                return Err(AppError::UnsupportedRegistryVersion {
                    found: registry.version,
                });
            }
            Ok(registry)
        }
        Err(_) => {
            let legacy: LegacyRegistryRecord = serde_json::from_slice(bytes)?;
            if legacy.version != REGISTRY_VERSION {
                return Err(AppError::UnsupportedRegistryVersion {
                    found: legacy.version,
                });
            }
            Ok(legacy.into_current())
        }
    }
}

#[derive(Debug, Deserialize)]
struct LegacyRegistryRecord {
    version: u32,
    identities: std::collections::BTreeMap<String, LegacyIdentityRecord>,
}

impl LegacyRegistryRecord {
    fn into_current(self) -> IdentityRegistryRecord {
        let identities = self
            .identities
            .into_iter()
            .map(|(fallback_slug, legacy)| {
                let id = legacy.identity_id(&fallback_slug);
                let current = legacy.into_current(id.clone());
                (id, current)
            })
            .collect();

        IdentityRegistryRecord {
            version: REGISTRY_VERSION,
            identities,
        }
    }
}

#[derive(Debug, Deserialize)]
struct LegacyIdentityRecord {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    auth_mode: AuthMode,
    home: PathBuf,
    shared_sessions: PathBuf,
    #[serde(default)]
    imported_auth: bool,
    #[serde(default)]
    created_at: Option<i64>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    plan_type: Option<PlanType>,
    #[serde(default)]
    account_type: Option<AccountType>,
    #[serde(default)]
    authenticated: Option<bool>,
    #[serde(default)]
    last_auth_method: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    priority: Option<u32>,
    #[serde(default)]
    notes: Option<String>,
    #[serde(default)]
    last_verified_at: Option<i64>,
}

impl LegacyIdentityRecord {
    fn identity_id(&self, fallback_slug: &str) -> IdentityId {
        serde_json::from_value(serde_json::Value::String(
            self.slug
                .clone()
                .unwrap_or_else(|| fallback_slug.to_string()),
        ))
        .unwrap_or_else(|_| IdentityId::from_display_name(fallback_slug).expect("legacy slug"))
    }

    fn into_current(self, id: IdentityId) -> CodexIdentity {
        let display_name = self.name.unwrap_or_else(|| id.as_str().to_string());
        let auth_mode = self.auth_mode;
        CodexIdentity {
            id,
            display_name,
            kind: auth_mode.identity_kind(),
            auth_mode,
            codex_home: self.home,
            shared_sessions_root: self.shared_sessions,
            forced_login_method: matches!(auth_mode, AuthMode::Chatgpt)
                .then_some(ForcedLoginMethod::Chatgpt),
            forced_chatgpt_workspace_id: None,
            api_key_env_var: matches!(auth_mode, AuthMode::Apikey)
                .then_some("OPENAI_API_KEY".to_string()),
            email: self.email,
            plan_type: self.plan_type,
            account_type: self.account_type,
            authenticated: self.authenticated,
            last_auth_method: self.last_auth_method,
            enabled: self.enabled.unwrap_or(true),
            priority: self.priority.unwrap_or(0),
            notes: self.notes,
            workspace_force_probe: None,
            imported_auth: self.imported_auth,
            created_at: self.created_at.unwrap_or_default(),
            last_verified_at: self.last_verified_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::{load_registry_bytes, JsonRegistryStore, RegistryStore};
    use crate::domain::identity::{IdentityId, IdentityKind};

    #[test]
    fn loads_default_registry_when_missing() {
        let temp = tempdir().unwrap();
        let store = JsonRegistryStore::new(temp.path());
        let registry = store.load().unwrap();
        assert_eq!(registry.version, 1);
        assert!(registry.identities.is_empty());
    }

    #[test]
    fn round_trips_registry() {
        let temp = tempdir().unwrap();
        let store = JsonRegistryStore::new(temp.path());
        let registry = crate::domain::identity::IdentityRegistryRecord::default();
        store.save(&registry).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.version, 1);
        assert!(loaded.identities.is_empty());
    }

    #[test]
    fn loads_legacy_python_registry_shape() {
        let registry = load_registry_bytes(
            br#"{
  "version": 1,
  "identities": {
    "personal-plus": {
      "name": "Personal Plus",
      "slug": "personal-plus",
      "auth_mode": "chatgpt",
      "home": "/tmp/home",
      "shared_sessions": "/tmp/shared/sessions",
      "imported_auth": true,
      "created_at": 123
    }
  }
}"#,
        )
        .unwrap();

        let identity = registry
            .identities
            .get(&IdentityId::from_display_name("personal-plus").unwrap())
            .unwrap();
        assert_eq!(identity.display_name, "Personal Plus");
        assert_eq!(identity.codex_home, PathBuf::from("/tmp/home"));
        assert_eq!(
            identity.shared_sessions_root,
            PathBuf::from("/tmp/shared/sessions")
        );
        assert_eq!(identity.kind, IdentityKind::ChatgptWorkspace);
        assert!(identity.imported_auth);
    }
}
