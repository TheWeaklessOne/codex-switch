use crate::bootstrap::{AuthBootstrap, BootstrapIdentityRequest, BootstrapIdentityResult};
use crate::codex_rpc::IdentityVerifier;
use crate::domain::identity::{CodexIdentity, IdentityId};
use crate::domain::verification::IdentityVerification;
use crate::error::{AppError, Result};
use crate::storage::paths::canonicalize_location;
use crate::storage::registry_store::RegistryStore;

#[derive(Debug)]
pub struct IdentityRegistryService<S> {
    store: S,
    bootstrap: AuthBootstrap,
}

impl<S> IdentityRegistryService<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            bootstrap: AuthBootstrap,
        }
    }
}

impl<S> IdentityRegistryService<S>
where
    S: RegistryStore,
{
    pub fn register_identity(
        &self,
        request: BootstrapIdentityRequest,
    ) -> Result<BootstrapIdentityResult> {
        let mut registry = self.store.load()?;
        let plan = self.bootstrap.plan_identity(&request)?;
        let identity_id = plan.identity_id.clone();

        if registry.identities.contains_key(&identity_id) {
            return Err(AppError::IdentityAlreadyExists { identity_id });
        }

        let planned_home = canonicalize_location(&plan.codex_home)?;
        if let Some(existing) = registry.identities.values().find(|identity| {
            canonicalize_location(&identity.codex_home)
                .map(|path| path == planned_home)
                .unwrap_or(false)
        }) {
            return Err(AppError::HomeAlreadyRegistered {
                path: plan.codex_home.clone(),
                identity_id: existing.id.clone(),
            });
        }

        let result = self.bootstrap.prepare_identity(request)?;
        registry
            .identities
            .insert(result.identity.id.clone(), result.identity.clone());
        self.store.save(&registry)?;
        Ok(result)
    }

    pub fn list_identities(&self) -> Result<Vec<CodexIdentity>> {
        let registry = self.store.load()?;
        Ok(registry.identities.into_values().collect())
    }

    pub fn verify_identity<V>(
        &self,
        identity_name: &str,
        verifier: &V,
    ) -> Result<(CodexIdentity, IdentityVerification)>
    where
        V: IdentityVerifier,
    {
        let mut registry = self.store.load()?;
        let identity_id = IdentityId::from_display_name(identity_name)?;
        let identity = registry.identities.get_mut(&identity_id).ok_or_else(|| {
            AppError::IdentityNotFound {
                identity_id: identity_id.clone(),
            }
        })?;

        let summary = verifier.verify(identity)?;
        summary.apply_to_identity(identity, crate::domain::identity::current_timestamp()?);

        let updated_identity = identity.clone();
        self.store.save(&registry)?;
        Ok((updated_identity, summary))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use tempfile::tempdir;

    use super::IdentityRegistryService;
    use crate::bootstrap::BootstrapIdentityRequest;
    use crate::codex_rpc::IdentityVerifier;
    use crate::domain::identity::{AccountType, AuthMode, PlanType};
    use crate::domain::verification::IdentityVerification;
    use crate::error::AppError;
    use crate::storage::registry_store::JsonRegistryStore;

    struct StubVerifier;

    impl IdentityVerifier for StubVerifier {
        fn verify(
            &self,
            _identity: &crate::domain::identity::CodexIdentity,
        ) -> crate::error::Result<IdentityVerification> {
            Ok(IdentityVerification {
                authenticated: true,
                auth_method: Some("chatgpt".to_string()),
                account_type: Some(AccountType::Chatgpt),
                email: Some("person@example.com".to_string()),
                plan_type: Some(PlanType::Plus),
                requires_openai_auth: false,
                fallback_rate_limit: None,
                rate_limits_by_limit_id: BTreeMap::new(),
            })
        }
    }

    #[test]
    fn registers_and_lists_identities() {
        let temp = tempdir().unwrap();
        let service = IdentityRegistryService::new(JsonRegistryStore::new(temp.path()));

        service
            .register_identity(BootstrapIdentityRequest {
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

        let identities = service.list_identities().unwrap();
        assert_eq!(identities.len(), 1);
        assert_eq!(identities[0].display_name, "Personal Plus");
    }

    #[test]
    fn rejects_duplicate_identity_before_mutating_registry() {
        let temp = tempdir().unwrap();
        let service = IdentityRegistryService::new(JsonRegistryStore::new(temp.path()));

        let request = BootstrapIdentityRequest {
            display_name: "Personal Plus".to_string(),
            base_root: temp.path().to_path_buf(),
            auth_mode: AuthMode::Chatgpt,
            home_override: None,
            import_auth_from_home: None,
            overwrite_config: false,
            api_key_env_var: None,
            forced_chatgpt_workspace_id: None,
        };

        service.register_identity(request.clone()).unwrap();
        let error = service.register_identity(request).unwrap_err();
        assert_eq!(
            error.to_string(),
            "identity personal-plus is already registered"
        );

        let identities = service.list_identities().unwrap();
        assert_eq!(identities.len(), 1);
    }

    #[test]
    fn verify_updates_cached_identity_fields() {
        let temp = tempdir().unwrap();
        let service = IdentityRegistryService::new(JsonRegistryStore::new(temp.path()));

        service
            .register_identity(BootstrapIdentityRequest {
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

        let (identity, summary) = service
            .verify_identity("Personal Plus", &StubVerifier)
            .unwrap();
        assert_eq!(summary.email.as_deref(), Some("person@example.com"));
        assert_eq!(identity.email.as_deref(), Some("person@example.com"));
        assert_eq!(identity.plan_type, Some(PlanType::Plus));
        assert_eq!(identity.account_type, Some(AccountType::Chatgpt));
        assert_eq!(identity.authenticated, Some(true));
        assert_eq!(identity.last_auth_method.as_deref(), Some("chatgpt"));
        assert!(identity.last_verified_at.is_some());
    }

    #[test]
    fn rejects_symlink_alias_for_existing_home() {
        let temp = tempdir().unwrap();
        let service = IdentityRegistryService::new(JsonRegistryStore::new(temp.path()));
        let real_home = temp.path().join("real-home");
        let alias_home = temp.path().join("alias-home");
        fs::create_dir_all(&real_home).unwrap();

        #[cfg(unix)]
        std::os::unix::fs::symlink(&real_home, &alias_home).unwrap();

        service
            .register_identity(BootstrapIdentityRequest {
                display_name: "Primary".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: Some(real_home),
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: None,
            })
            .unwrap();

        let error = service
            .register_identity(BootstrapIdentityRequest {
                display_name: "Alias".to_string(),
                base_root: temp.path().to_path_buf(),
                auth_mode: AuthMode::Chatgpt,
                home_override: Some(alias_home),
                import_auth_from_home: None,
                overwrite_config: false,
                api_key_env_var: None,
                forced_chatgpt_workspace_id: None,
            })
            .unwrap_err();

        assert!(matches!(error, AppError::HomeAlreadyRegistered { .. }));
    }
}
