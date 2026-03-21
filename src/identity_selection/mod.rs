use crate::domain::identity::{current_timestamp, CodexIdentity, IdentityId};
use crate::domain::selection::{SelectedIdentityState, SelectionMode, SelectionStateRecord};
use crate::error::{AppError, Result};
use crate::storage::registry_store::RegistryStore;
use crate::storage::selection_store::SelectionStore;

#[derive(Debug, Clone)]
pub struct CurrentIdentitySelection {
    pub selection: SelectedIdentityState,
    pub identity: CodexIdentity,
}

#[derive(Debug)]
pub struct IdentitySelectionService<S, R> {
    selection_store: S,
    registry_store: R,
}

impl<S, R> IdentitySelectionService<S, R> {
    pub fn new(selection_store: S, registry_store: R) -> Self {
        Self {
            selection_store,
            registry_store,
        }
    }
}

impl<S, R> IdentitySelectionService<S, R>
where
    S: SelectionStore,
    R: RegistryStore,
{
    pub fn current(&self) -> Result<Option<CurrentIdentitySelection>> {
        let record = self.selection_store.load()?;
        let Some(selection) = record.current else {
            return Ok(None);
        };

        let identity = self.identity_by_id(&selection.identity_id)?;
        Ok(Some(CurrentIdentitySelection {
            selection,
            identity,
        }))
    }

    pub fn resolve_by_name(&self, identity_name: &str) -> Result<CodexIdentity> {
        let identity_id = IdentityId::from_display_name(identity_name)?;
        self.identity_by_id(&identity_id)
    }

    pub fn require_current(&self) -> Result<CurrentIdentitySelection> {
        self.current()?.ok_or(AppError::NoIdentitySelected)
    }

    pub fn select_manual(
        &self,
        identity_name: &str,
        reason: Option<&str>,
    ) -> Result<CurrentIdentitySelection> {
        let identity = self.resolve_by_name(identity_name)?;
        self.store_current(identity, SelectionMode::Manual, reason)
    }

    pub fn store_automatic(
        &self,
        identity: CodexIdentity,
        reason: Option<&str>,
    ) -> Result<CurrentIdentitySelection> {
        self.store_current(identity, SelectionMode::Automatic, reason)
    }

    pub fn store_manual(
        &self,
        identity: CodexIdentity,
        reason: Option<&str>,
    ) -> Result<CurrentIdentitySelection> {
        self.store_current(identity, SelectionMode::Manual, reason)
    }

    fn store_current(
        &self,
        identity: CodexIdentity,
        mode: SelectionMode,
        reason: Option<&str>,
    ) -> Result<CurrentIdentitySelection> {
        let selection = SelectedIdentityState {
            identity_id: identity.id.clone(),
            mode,
            reason: reason.map(std::string::ToString::to_string),
            updated_at: current_timestamp()?,
        };
        let record = SelectionStateRecord {
            version: crate::domain::selection::SELECTION_STATE_VERSION,
            current: Some(selection.clone()),
        };
        self.selection_store.save(&record)?;
        Ok(CurrentIdentitySelection {
            selection,
            identity,
        })
    }

    fn identity_by_id(&self, identity_id: &IdentityId) -> Result<CodexIdentity> {
        let registry = self.registry_store.load()?;
        registry
            .identities
            .get(identity_id)
            .cloned()
            .ok_or_else(|| AppError::IdentityNotFound {
                identity_id: identity_id.clone(),
            })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::IdentitySelectionService;
    use crate::bootstrap::BootstrapIdentityRequest;
    use crate::domain::identity::AuthMode;
    use crate::identity_registry::IdentityRegistryService;
    use crate::storage::registry_store::JsonRegistryStore;
    use crate::storage::selection_store::JsonSelectionStore;

    #[test]
    fn stores_and_loads_current_identity_selection() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let selection_store = JsonSelectionStore::new(temp.path());
        let registry = IdentityRegistryService::new(registry_store.clone());
        registry
            .register_identity(BootstrapIdentityRequest {
                display_name: "Primary".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: None,
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: None,
            })
            .unwrap();

        let service = IdentitySelectionService::new(selection_store, registry_store);
        let current = service
            .select_manual("Primary", Some("selected in test"))
            .unwrap();
        assert_eq!(current.identity.id.as_str(), "primary");
        assert_eq!(current.selection.mode.as_str(), "manual");

        let loaded = service.require_current().unwrap();
        assert_eq!(loaded.identity.id.as_str(), "primary");
    }
}
