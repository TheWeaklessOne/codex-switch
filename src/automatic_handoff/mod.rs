use std::ffi::OsString;
use std::path::Path;

use crate::codex_rpc::{IdentityVerifier, ThreadRuntime};
use crate::continuation::{ContinueService, ContinueThreadRequest, ContinueThreadResult};
use crate::decision_log::{DecisionLogService, LoggedSelectionEvent};
use crate::domain::identity::current_timestamp;
use crate::domain::selection::SelectionMode;
use crate::error::Result;
use crate::identity_selection::IdentitySelectionService;
use crate::identity_selector::{IdentitySelector, SelectedIdentity};
use crate::quota_status::{IdentityStatusReport, QuotaStatusService};
use crate::storage::checkpoint_store::TaskCheckpointStore;
use crate::storage::health_store::IdentityHealthStore;
use crate::storage::policy_store::SelectionPolicyStore;
use crate::storage::quota_store::QuotaStore;
use crate::storage::registry_store::RegistryStore;
use crate::storage::selection_event_store::SelectionEventStore;
use crate::storage::selection_store::SelectionStore;

#[derive(Debug, Clone)]
pub struct SelectionContextStores<P, H> {
    pub policy_store: P,
    pub health_store: H,
}

#[derive(Debug, Clone)]
pub struct AutomaticHandoffStores<R, Q, S, C, E, P, H> {
    pub registry_store: R,
    pub quota_store: Q,
    pub selection_store: S,
    pub checkpoint_store: C,
    pub decision_store: E,
    pub selection_context_stores: SelectionContextStores<P, H>,
}

#[derive(Debug, Clone)]
pub struct AutomaticContinueThreadRequest {
    pub thread_id: String,
    pub from_identity_name: Option<String>,
    pub reason: String,
    pub cached: bool,
    pub launch_after_switch: bool,
    pub extra_resume_args: Vec<OsString>,
}

#[derive(Debug, Clone)]
pub struct AutomaticContinueThreadResult {
    pub selected: SelectedIdentity,
    pub continue_result: ContinueThreadResult,
    pub decision_log: LoggedSelectionEvent,
}

#[derive(Debug, Clone)]
pub struct AutomaticHandoffService<R, Q, S, C, E, P, H, V> {
    base_root: std::path::PathBuf,
    registry_store: R,
    quota_store: Q,
    selection_store: S,
    checkpoint_store: C,
    decision_store: E,
    selection_context_stores: SelectionContextStores<P, H>,
    verifier: V,
}

impl<R, Q, S, C, E, P, H, V> AutomaticHandoffService<R, Q, S, C, E, P, H, V> {
    pub fn new(
        base_root: &Path,
        stores: AutomaticHandoffStores<R, Q, S, C, E, P, H>,
        verifier: V,
    ) -> Self {
        Self {
            base_root: base_root.to_path_buf(),
            registry_store: stores.registry_store,
            quota_store: stores.quota_store,
            selection_store: stores.selection_store,
            checkpoint_store: stores.checkpoint_store,
            decision_store: stores.decision_store,
            selection_context_stores: stores.selection_context_stores,
            verifier,
        }
    }
}

impl<R, Q, S, C, E, P, H, V> AutomaticHandoffService<R, Q, S, C, E, P, H, V>
where
    R: RegistryStore + Clone,
    Q: QuotaStore + Clone,
    S: SelectionStore + Clone,
    C: TaskCheckpointStore + Clone,
    E: SelectionEventStore,
    P: SelectionPolicyStore + Clone,
    H: IdentityHealthStore + Clone,
    V: IdentityVerifier + ThreadRuntime + Clone + Sync,
{
    pub fn continue_thread(
        &self,
        request: AutomaticContinueThreadRequest,
    ) -> Result<AutomaticContinueThreadResult> {
        let selection_service = IdentitySelectionService::new(
            self.selection_store.clone(),
            self.registry_store.clone(),
        );
        let source_identity = match request.from_identity_name.as_deref() {
            Some(identity_name) => selection_service.resolve_by_name(identity_name)?,
            None => selection_service.require_current()?.identity,
        };

        let quota_service =
            QuotaStatusService::new(self.registry_store.clone(), self.quota_store.clone());
        let reports = if request.cached {
            quota_service.cached_statuses()?
        } else {
            quota_service.refresh_all(&self.verifier)?
        };
        let health = self.selection_context_stores.health_store.load()?;
        let selector = IdentitySelector::new(
            self.selection_context_stores.policy_store.load()?.policy,
            current_timestamp()?,
        );
        let selected = self.select_target(
            &selector,
            &health,
            &source_identity.id,
            &reports,
            request.cached,
        )?;

        let continue_service = ContinueService::new(
            &self.base_root,
            self.registry_store.clone(),
            self.verifier.clone(),
            self.selection_store.clone(),
            self.checkpoint_store.clone(),
        );
        let continue_result = continue_service.continue_thread(ContinueThreadRequest {
            thread_id: request.thread_id,
            from_identity_name: Some(source_identity.display_name.clone()),
            to_identity_name: selected.identity.display_name.clone(),
            reason: request.reason.clone(),
            target_selection_mode: SelectionMode::Automatic,
            selection_reason: Some("selected automatically for thread handoff".to_string()),
            launch_after_switch: request.launch_after_switch,
            extra_resume_args: request.extra_resume_args,
        })?;
        let decision_log = DecisionLogService::new(&self.decision_store, selector, health)
            .log_thread_handoff(
                &selected,
                &reports,
                request.cached,
                &request.reason,
                &continue_result,
            )?;

        Ok(AutomaticContinueThreadResult {
            selected,
            continue_result,
            decision_log,
        })
    }

    fn select_target(
        &self,
        selector: &IdentitySelector,
        health: &crate::domain::health::IdentityHealthRecord,
        source_identity_id: &crate::domain::identity::IdentityId,
        reports: &[IdentityStatusReport],
        cached: bool,
    ) -> Result<SelectedIdentity> {
        selector.select_best(
            reports
                .iter()
                .filter(|report| &report.identity.id != source_identity_id)
                .filter(|report| cached || report.refresh_error.is_none())
                .map(|report| {
                    (
                        &report.identity,
                        report.quota_status.as_ref(),
                        health.identities.get(&report.identity.id),
                    )
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use super::{
        AutomaticContinueThreadRequest, AutomaticHandoffService, AutomaticHandoffStores,
        SelectionContextStores,
    };
    use crate::bootstrap::BootstrapIdentityRequest;
    use crate::codex_rpc::{IdentityVerifier, ThreadRuntime};
    use crate::domain::identity::{AuthMode, CodexIdentity, IdentityId};
    use crate::domain::quota::{
        IdentityQuotaStatus, QuotaStatusRecord, RateLimitSnapshot, RateLimitWindow,
    };
    use crate::domain::thread::{ThreadSnapshot, TurnStatus};
    use crate::domain::verification::IdentityVerification;
    use crate::identity_registry::IdentityRegistryService;
    use crate::identity_selection::IdentitySelectionService;
    use crate::storage::checkpoint_store::JsonTaskCheckpointStore;
    use crate::storage::health_store::JsonIdentityHealthStore;
    use crate::storage::policy_store::JsonSelectionPolicyStore;
    use crate::storage::quota_store::{JsonQuotaStore, QuotaStore};
    use crate::storage::registry_store::{JsonRegistryStore, RegistryStore};
    use crate::storage::selection_event_store::JsonSelectionEventStore;
    use crate::storage::selection_store::JsonSelectionStore;

    #[derive(Debug, Clone)]
    struct StubVerifier {
        snapshots: Arc<Mutex<BTreeMap<(String, String), ThreadSnapshot>>>,
    }

    impl IdentityVerifier for StubVerifier {
        fn verify(&self, _identity: &CodexIdentity) -> crate::error::Result<IdentityVerification> {
            unreachable!("cached test should not call verifier")
        }
    }

    impl ThreadRuntime for StubVerifier {
        fn read_thread(
            &self,
            identity: &CodexIdentity,
            thread_id: &str,
        ) -> crate::error::Result<ThreadSnapshot> {
            Ok(self
                .snapshots
                .lock()
                .unwrap()
                .get(&(identity.id.to_string(), thread_id.to_string()))
                .unwrap()
                .clone())
        }

        fn resume_thread(
            &self,
            identity: &CodexIdentity,
            thread_id: &str,
        ) -> crate::error::Result<ThreadSnapshot> {
            self.read_thread(identity, thread_id)
        }
    }

    #[test]
    fn automatically_switches_to_healthiest_target_and_logs_decision() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let quota_store = JsonQuotaStore::new(temp.path());
        let selection_store = JsonSelectionStore::new(temp.path());
        let checkpoint_store = JsonTaskCheckpointStore::new(temp.path());
        let event_store = JsonSelectionEventStore::new(temp.path());
        let registry_service = IdentityRegistryService::new(registry_store.clone());

        for name in ["Source", "Target"] {
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
        let source = identities
            .iter()
            .find(|identity| identity.id.as_str() == "source")
            .unwrap()
            .clone();
        let target = identities
            .iter()
            .find(|identity| identity.id.as_str() == "target")
            .unwrap()
            .clone();
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
                    (source.id.clone(), quota_status(source.id.clone(), 98)),
                    (target.id.clone(), quota_status(target.id.clone(), 10)),
                ]),
            })
            .unwrap();

        let _ = IdentitySelectionService::new(selection_store.clone(), registry_store.clone())
            .select_manual("Source", Some("test source"))
            .unwrap();

        let snapshots = Arc::new(Mutex::new(BTreeMap::from([
            (
                (source.id.to_string(), "thread-1".to_string()),
                snapshot("thread-1", &["turn-a"], 2),
            ),
            (
                (target.id.to_string(), "thread-1".to_string()),
                snapshot("thread-1", &["turn-a"], 2),
            ),
        ])));
        let verifier = StubVerifier { snapshots };
        let service = AutomaticHandoffService::new(
            temp.path(),
            AutomaticHandoffStores {
                registry_store,
                quota_store,
                selection_store,
                checkpoint_store,
                decision_store: event_store,
                selection_context_stores: SelectionContextStores {
                    policy_store: JsonSelectionPolicyStore::new(temp.path()),
                    health_store: JsonIdentityHealthStore::new(temp.path()),
                },
            },
            verifier,
        );
        let result = service
            .continue_thread(AutomaticContinueThreadRequest {
                thread_id: "thread-1".to_string(),
                from_identity_name: None,
                reason: "automatic_handoff".to_string(),
                cached: true,
                launch_after_switch: false,
                extra_resume_args: Vec::new(),
            })
            .unwrap();

        assert_eq!(result.selected.identity.id.as_str(), "target");
        assert_eq!(result.continue_result.mode.as_str(), "resume_same_thread");
        assert!(result.decision_log.path.exists());
    }

    fn quota_status(identity_id: IdentityId, used_percent: i32) -> IdentityQuotaStatus {
        IdentityQuotaStatus {
            identity_id,
            default_rate_limit: None,
            rate_limits_by_limit_id: BTreeMap::from([(
                "codex".to_string(),
                RateLimitSnapshot {
                    credits: None,
                    limit_id: Some("codex".to_string()),
                    limit_name: Some("Codex".to_string()),
                    plan_type: None,
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

    fn snapshot(thread_id: &str, turns: &[&str], updated_at: i64) -> ThreadSnapshot {
        ThreadSnapshot {
            thread_id: thread_id.to_string(),
            created_at: 1,
            updated_at,
            status: "idle".to_string(),
            path: None,
            turn_ids: turns.iter().map(|turn| (*turn).to_string()).collect(),
            latest_turn_id: turns.last().map(|turn| (*turn).to_string()),
            latest_turn_status: Some(TurnStatus::Completed),
        }
    }
}
