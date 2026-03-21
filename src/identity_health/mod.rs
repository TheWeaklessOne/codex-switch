use crate::domain::health::{IdentityFailureKind, IdentityHealthRecord, IdentityHealthState};
use crate::domain::identity::{current_timestamp, IdentityId};
use crate::error::{AppError, Result};
use crate::storage::health_store::IdentityHealthStore;
use crate::storage::registry_store::RegistryStore;

#[derive(Debug, Clone)]
pub struct IdentityHealthService<R, H> {
    registry_store: R,
    health_store: H,
}

impl<R, H> IdentityHealthService<R, H> {
    pub fn new(registry_store: R, health_store: H) -> Self {
        Self {
            registry_store,
            health_store,
        }
    }
}

impl<R, H> IdentityHealthService<R, H>
where
    R: RegistryStore,
    H: IdentityHealthStore,
{
    pub fn load_record(&self) -> Result<IdentityHealthRecord> {
        self.health_store.load()
    }

    pub fn state_by_name(&self, identity_name: &str) -> Result<IdentityHealthState> {
        let identity = self.resolve_identity(identity_name)?;
        let record = self.health_store.load()?;
        Ok(record
            .identities
            .get(&identity.id)
            .cloned()
            .unwrap_or_else(|| IdentityHealthState::new(identity.id.clone(), 0)))
    }

    pub fn clear_identity(&self, identity_name: &str) -> Result<IdentityHealthState> {
        let identity = self.resolve_identity(identity_name)?;
        let updated_at = current_timestamp()?;
        self.update_state(&identity.id, updated_at, |state| {
            state.penalty_until = None;
            state.last_failure_kind = None;
            state.last_failure_at = None;
            state.last_failure_message = None;
        })
    }

    pub fn set_manually_disabled(
        &self,
        identity_name: &str,
        manually_disabled: bool,
    ) -> Result<IdentityHealthState> {
        let identity = self.resolve_identity(identity_name)?;
        let updated_at = current_timestamp()?;
        self.update_state(&identity.id, updated_at, |state| {
            state.manually_disabled = manually_disabled;
        })
    }

    pub fn apply_penalty(
        &self,
        identity_id: &IdentityId,
        failure_kind: IdentityFailureKind,
        failure_message: Option<String>,
        cooldown_secs: i64,
        now: i64,
    ) -> Result<IdentityHealthState> {
        let penalty_until =
            now.checked_add(cooldown_secs)
                .ok_or_else(|| AppError::InvalidSelectionPolicy {
                    message: "cooldown results in an invalid penalty deadline".to_string(),
                })?;
        self.update_state(identity_id, now, |state| {
            state.penalty_until = Some(penalty_until);
            state.last_failure_kind = Some(failure_kind);
            state.last_failure_at = Some(now);
            state.last_failure_message = failure_message.clone();
        })
    }

    fn update_state<F>(
        &self,
        identity_id: &IdentityId,
        updated_at: i64,
        mutator: F,
    ) -> Result<IdentityHealthState>
    where
        F: FnOnce(&mut IdentityHealthState),
    {
        let mut record = self.health_store.load()?;
        let state = record
            .identities
            .entry(identity_id.clone())
            .or_insert_with(|| IdentityHealthState::new(identity_id.clone(), updated_at));
        mutator(state);
        state.updated_at = updated_at;
        let state = state.clone();
        self.health_store.save(&record)?;
        Ok(state)
    }

    fn resolve_identity(
        &self,
        identity_name: &str,
    ) -> Result<crate::domain::identity::CodexIdentity> {
        let registry = self.registry_store.load()?;
        let identity_id = IdentityId::from_display_name(identity_name)?;
        registry
            .identities
            .get(&identity_id)
            .cloned()
            .ok_or(AppError::IdentityNotFound { identity_id })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::IdentityHealthService;
    use crate::bootstrap::BootstrapIdentityRequest;
    use crate::domain::health::IdentityFailureKind;
    use crate::domain::identity::AuthMode;
    use crate::error::AppError;
    use crate::identity_registry::IdentityRegistryService;
    use crate::storage::health_store::JsonIdentityHealthStore;
    use crate::storage::registry_store::JsonRegistryStore;

    #[test]
    fn disables_and_clears_health_state() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
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

        let service =
            IdentityHealthService::new(registry_store, JsonIdentityHealthStore::new(temp.path()));
        let disabled = service.set_manually_disabled("Primary", true).unwrap();
        assert!(disabled.manually_disabled);

        let penalized = service
            .apply_penalty(
                &disabled.identity_id,
                IdentityFailureKind::RateLimit,
                Some("429".to_string()),
                60,
                100,
            )
            .unwrap();
        assert_eq!(penalized.penalty_until, Some(160));

        let cleared = service.clear_identity("Primary").unwrap();
        assert!(!cleared.penalty_active_at(120));
        assert!(cleared.last_failure_kind.is_none());
    }

    #[test]
    fn rejects_penalty_deadline_overflow() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let registry = IdentityRegistryService::new(registry_store.clone());
        let identity = registry
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

        let service =
            IdentityHealthService::new(registry_store, JsonIdentityHealthStore::new(temp.path()));
        let error = service
            .apply_penalty(
                &identity.identity.id,
                IdentityFailureKind::RateLimit,
                Some("rate-limit failure".to_string()),
                i64::MAX,
                1,
            )
            .unwrap_err();
        assert!(matches!(error, AppError::InvalidSelectionPolicy { .. }));
    }
}
