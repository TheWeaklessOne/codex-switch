use std::path::PathBuf;

use crate::codex_rpc::IdentityVerifier;
use crate::decision_log::{DecisionLogService, LoggedSelectionEvent};
use crate::domain::identity::current_timestamp;
use crate::error::Result;
use crate::identity_cleanup::{
    auto_remove_deactivated_workspace_identities, AutoRemovalNotice, ManagedIdentityRemovalService,
};
use crate::identity_selection::{CurrentIdentitySelection, IdentitySelectionService};
use crate::identity_selector::{IdentitySelector, SelectedIdentity};
use crate::quota_status::{IdentityStatusReport, QuotaStatusService};
use crate::storage::health_store::IdentityHealthStore;
use crate::storage::policy_store::SelectionPolicyStore;
use crate::storage::quota_store::QuotaStore;
use crate::storage::registry_store::RegistryStore;
use crate::storage::selection_event_store::SelectionEventStore;
use crate::storage::selection_store::SelectionStore;

#[derive(Debug, Clone)]
pub struct AutomaticSelectionResult {
    pub selected: SelectedIdentity,
    pub current: CurrentIdentitySelection,
    pub decision_log: LoggedSelectionEvent,
    pub auto_removal_notices: Vec<AutoRemovalNotice>,
}

#[derive(Debug, Clone)]
pub struct AutomaticSelectionService<R, Q, S, E, P, H, V> {
    registry_store: R,
    quota_store: Q,
    selection_store: S,
    decision_store: E,
    policy_store: P,
    health_store: H,
    verifier: V,
}

impl<R, Q, S, E, P, H, V> AutomaticSelectionService<R, Q, S, E, P, H, V> {
    pub fn new(
        registry_store: R,
        quota_store: Q,
        selection_store: S,
        decision_store: E,
        policy_store: P,
        health_store: H,
        verifier: V,
    ) -> Self {
        Self {
            registry_store,
            quota_store,
            selection_store,
            decision_store,
            policy_store,
            health_store,
            verifier,
        }
    }
}

impl<R, Q, S, E, P, H, V> AutomaticSelectionService<R, Q, S, E, P, H, V>
where
    R: RegistryStore + Clone,
    Q: QuotaStore + Clone,
    S: SelectionStore + Clone,
    E: SelectionEventStore,
    P: SelectionPolicyStore + Clone,
    H: IdentityHealthStore + Clone,
    V: IdentityVerifier + Sync,
{
    pub fn select_for_new_session(
        &self,
        cached: bool,
        reason: &str,
    ) -> Result<AutomaticSelectionResult> {
        let quota_service =
            QuotaStatusService::new(self.registry_store.clone(), self.quota_store.clone());
        let reports = if cached {
            quota_service.cached_statuses()?
        } else {
            quota_service.refresh_all(&self.verifier)?
        };
        let auto_removal_notices;
        let reports = if cached {
            auto_removal_notices = Vec::new();
            reports
        } else {
            let remover = ManagedIdentityRemovalService::new(
                self.registry_store.clone(),
                self.quota_store.clone(),
                self.health_store.clone(),
                self.selection_store.clone(),
            );
            let sweep = auto_remove_deactivated_workspace_identities(reports, &remover);
            auto_removal_notices = sweep.notices;
            sweep.reports
        };
        self.select_from_reports(reports, cached, reason, auto_removal_notices)
    }

    pub fn select_from_reports(
        &self,
        reports: Vec<IdentityStatusReport>,
        cached: bool,
        reason: &str,
        auto_removal_notices: Vec<AutoRemovalNotice>,
    ) -> Result<AutomaticSelectionResult> {
        let policy = self.policy_store.load()?.policy;
        let health = self.health_store.load()?;
        let selector = IdentitySelector::new(policy, current_timestamp()?);
        let selected = selector.select_best(
            reports
                .iter()
                .filter(|report| cached || report.refresh_error.is_none())
                .map(|report| {
                    (
                        &report.identity,
                        report.quota_status.as_ref(),
                        health.identities.get(&report.identity.id),
                    )
                }),
        )?;
        let selection_service = IdentitySelectionService::new(
            self.selection_store.clone(),
            self.registry_store.clone(),
        );
        let current = selection_service.store_automatic(selected.identity.clone(), Some(reason))?;
        let decision_log = DecisionLogService::new(&self.decision_store, selector, health)
            .log_new_session_selection(&selected, &reports, cached, reason)?;

        Ok(AutomaticSelectionResult {
            selected,
            current,
            decision_log,
            auto_removal_notices,
        })
    }
}

impl<T> SelectionEventStore for &T
where
    T: SelectionEventStore,
{
    fn append(&self, event: &crate::domain::decision::SelectionEvent) -> Result<PathBuf> {
        (*self).append(event)
    }

    fn load(&self, event_id: &str) -> Result<Option<crate::domain::decision::SelectionEvent>> {
        (*self).load(event_id)
    }

    fn list(&self) -> Result<Vec<(crate::domain::decision::SelectionEvent, PathBuf)>> {
        (*self).list()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::AutomaticSelectionService;
    use crate::bootstrap::BootstrapIdentityRequest;
    use crate::codex_rpc::IdentityVerifier;
    use crate::domain::identity::{AuthMode, PlanType};
    use crate::domain::quota::{
        IdentityQuotaStatus, QuotaStatusRecord, RateLimitSnapshot, RateLimitWindow,
    };
    use crate::domain::verification::IdentityVerification;
    use crate::error::Result;
    use crate::identity_registry::IdentityRegistryService;
    use crate::storage::health_store::JsonIdentityHealthStore;
    use crate::storage::policy_store::JsonSelectionPolicyStore;
    use crate::storage::quota_store::{JsonQuotaStore, QuotaStore};
    use crate::storage::registry_store::{JsonRegistryStore, RegistryStore};
    use crate::storage::selection_event_store::JsonSelectionEventStore;
    use crate::storage::selection_store::JsonSelectionStore;

    #[derive(Debug, Clone, Copy)]
    struct StubVerifier;

    impl IdentityVerifier for StubVerifier {
        fn verify(
            &self,
            _identity: &crate::domain::identity::CodexIdentity,
        ) -> Result<IdentityVerification> {
            unreachable!("cached test should not call verifier")
        }
    }

    #[test]
    fn selects_best_cached_identity_and_logs_event() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let quota_store = JsonQuotaStore::new(temp.path());
        let registry_service = IdentityRegistryService::new(registry_store.clone());

        for name in ["Primary", "Backup"] {
            registry_service
                .register_identity(BootstrapIdentityRequest {
                    display_name: name.to_string(),
                    base_root: temp.path().to_path_buf(),
                    auth_mode: AuthMode::Chatgpt,
                    home_override: None,
                    import_auth_from_home: None,
                    overwrite_config: false,
                    api_key_env_var: None,
                    forced_chatgpt_workspace_id: None,
                })
                .unwrap();
        }

        let identities = registry_service.list_identities().unwrap();
        let primary = identities
            .iter()
            .find(|identity| identity.id.as_str() == "primary")
            .unwrap();
        let backup = identities
            .iter()
            .find(|identity| identity.id.as_str() == "backup")
            .unwrap();
        let mut registry = registry_store.load().unwrap();
        for identity in registry.identities.values_mut() {
            identity.authenticated = Some(true);
            identity.last_auth_method = Some("chatgpt".to_string());
        }
        registry_store.save(&registry).unwrap();

        quota_store
            .save(&QuotaStatusRecord {
                version: crate::domain::quota::QUOTA_STATUS_VERSION,
                statuses: BTreeMap::from([
                    (primary.id.clone(), quota_status(primary.id.clone(), 98)),
                    (backup.id.clone(), quota_status(backup.id.clone(), 25)),
                ]),
            })
            .unwrap();

        let service = AutomaticSelectionService::new(
            registry_store,
            quota_store,
            JsonSelectionStore::new(temp.path()),
            JsonSelectionEventStore::new(temp.path()),
            JsonSelectionPolicyStore::new(temp.path()),
            JsonIdentityHealthStore::new(temp.path()),
            StubVerifier,
        );
        let result = service
            .select_for_new_session(true, "selected automatically for launch")
            .unwrap();

        assert_eq!(result.selected.identity.id.as_str(), "backup");
        assert_eq!(result.current.selection.mode.as_str(), "automatic");
        assert!(result.decision_log.path.exists());
    }

    fn quota_status(
        identity_id: crate::domain::identity::IdentityId,
        used_percent: i32,
    ) -> IdentityQuotaStatus {
        IdentityQuotaStatus {
            identity_id,
            default_rate_limit: None,
            rate_limits_by_limit_id: BTreeMap::from([(
                "codex".to_string(),
                RateLimitSnapshot {
                    credits: None,
                    limit_id: Some("codex".to_string()),
                    limit_name: Some("Codex".to_string()),
                    plan_type: Some(PlanType::Plus),
                    primary: Some(RateLimitWindow {
                        resets_at: Some(1_700_000_000),
                        used_percent,
                        window_duration_mins: Some(300),
                    }),
                    secondary: None,
                },
            )]),
            updated_at: 1,
        }
    }
}
