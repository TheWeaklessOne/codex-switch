use std::thread;
use std::time::Duration;

use crate::codex_rpc::IdentityVerifier;
use crate::domain::identity::{current_timestamp, CodexIdentity, IdentityId};
use crate::domain::quota::IdentityQuotaStatus;
use crate::domain::verification::IdentityVerification;
use crate::error::{AppError, Result};
use crate::storage::quota_store::QuotaStore;
use crate::storage::registry_store::RegistryStore;

#[derive(Debug, Clone)]
pub struct IdentityStatusReport {
    pub identity: CodexIdentity,
    pub quota_status: Option<IdentityQuotaStatus>,
    pub refresh_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RefreshedIdentityStatus {
    pub identity: CodexIdentity,
    pub verification: IdentityVerification,
    pub quota_status: IdentityQuotaStatus,
}

#[derive(Debug)]
pub struct QuotaStatusService<R, Q> {
    registry_store: R,
    quota_store: Q,
}

const REFRESH_RETRY_ATTEMPTS: usize = 2;
const REFRESH_RETRY_BACKOFF: Duration = Duration::from_millis(250);

impl<R, Q> QuotaStatusService<R, Q> {
    pub fn new(registry_store: R, quota_store: Q) -> Self {
        Self {
            registry_store,
            quota_store,
        }
    }
}

impl<R, Q> QuotaStatusService<R, Q>
where
    R: RegistryStore,
    Q: QuotaStore,
{
    pub fn cached_statuses(&self) -> Result<Vec<IdentityStatusReport>> {
        let registry = self.registry_store.load()?;
        let mut quota_record = self.quota_store.load()?;
        quota_record
            .statuses
            .retain(|identity_id, _| registry.identities.contains_key(identity_id));

        Ok(registry
            .identities
            .into_values()
            .map(|identity| {
                let quota_status = quota_record.statuses.remove(&identity.id);
                IdentityStatusReport {
                    identity,
                    quota_status,
                    refresh_error: None,
                }
            })
            .collect())
    }

    pub fn refresh_identity<V>(
        &self,
        identity_name: &str,
        verifier: &V,
    ) -> Result<RefreshedIdentityStatus>
    where
        V: IdentityVerifier,
    {
        let mut registry = self.registry_store.load()?;
        let original_registry = registry.clone();
        let mut quota_record = self.quota_store.load()?;
        quota_record
            .statuses
            .retain(|identity_id, _| registry.identities.contains_key(identity_id));

        let identity_id = IdentityId::from_display_name(identity_name)?;
        let identity = registry.identities.get_mut(&identity_id).ok_or_else(|| {
            AppError::IdentityNotFound {
                identity_id: identity_id.clone(),
            }
        })?;

        let verification = verify_identity_with_retry(verifier, identity)?;
        let updated_at = current_timestamp()?;
        verification.apply_to_identity(identity, updated_at);

        let refreshed_identity = identity.clone();
        let quota_status = verification.to_quota_status(refreshed_identity.id.clone(), updated_at);
        quota_record
            .statuses
            .insert(refreshed_identity.id.clone(), quota_status.clone());

        self.registry_store.save(&registry)?;
        self.quota_store.save(&quota_record).map_err(|error| {
            self.rollback_registry_save("quota_status.refresh_identity", error, &original_registry)
        })?;

        Ok(RefreshedIdentityStatus {
            identity: refreshed_identity,
            verification,
            quota_status,
        })
    }

    pub fn refresh_all<V>(&self, verifier: &V) -> Result<Vec<IdentityStatusReport>>
    where
        V: IdentityVerifier + Sync,
    {
        let mut registry = self.registry_store.load()?;
        let original_registry = registry.clone();
        let mut quota_record = self.quota_store.load()?;
        quota_record
            .statuses
            .retain(|identity_id, _| registry.identities.contains_key(identity_id));

        let identities: Vec<CodexIdentity> = registry.identities.values().cloned().collect();
        let concurrency = refresh_concurrency(identities.len());
        let mut verification_results = Vec::with_capacity(identities.len());
        for batch in identities.chunks(concurrency) {
            let batch_results = thread::scope(|scope| {
                let handles: Vec<_> = batch
                    .iter()
                    .cloned()
                    .map(|identity| {
                        scope.spawn(move || {
                            (
                                identity.id.clone(),
                                verify_identity_with_retry(verifier, &identity),
                            )
                        })
                    })
                    .collect();

                let mut joined = Vec::with_capacity(handles.len());
                for handle in handles {
                    match handle.join() {
                        Ok(result) => joined.push(result),
                        Err(_) => {
                            return Err(AppError::Io(std::io::Error::other(
                                "quota refresh worker panicked",
                            )));
                        }
                    }
                }
                Ok(joined)
            })?;
            verification_results.extend(batch_results);
        }

        let mut reports = Vec::with_capacity(registry.identities.len());
        for (identity_id, verification_result) in verification_results {
            let identity = registry.identities.get_mut(&identity_id).ok_or_else(|| {
                AppError::IdentityNotFound {
                    identity_id: identity_id.clone(),
                }
            })?;
            let cached_quota_status = quota_record.statuses.get(&identity.id).cloned();
            match verification_result {
                Ok(verification) => {
                    let updated_at = current_timestamp()?;
                    verification.apply_to_identity(identity, updated_at);
                    let quota_status =
                        verification.to_quota_status(identity.id.clone(), updated_at);
                    quota_record
                        .statuses
                        .insert(identity.id.clone(), quota_status.clone());
                    reports.push(IdentityStatusReport {
                        identity: identity.clone(),
                        quota_status: Some(quota_status),
                        refresh_error: None,
                    });
                }
                Err(error) => {
                    reports.push(IdentityStatusReport {
                        identity: identity.clone(),
                        quota_status: cached_quota_status,
                        refresh_error: Some(error.to_string()),
                    });
                }
            }
        }

        self.registry_store.save(&registry)?;
        self.quota_store.save(&quota_record).map_err(|error| {
            self.rollback_registry_save("quota_status.refresh_all", error, &original_registry)
        })?;

        Ok(reports)
    }

    fn rollback_registry_save(
        &self,
        operation: &str,
        primary: AppError,
        original_registry: &crate::domain::identity::IdentityRegistryRecord,
    ) -> AppError {
        match self.registry_store.save(original_registry) {
            Ok(()) => primary,
            Err(rollback) => AppError::RollbackFailed {
                operation: operation.to_string(),
                primary: primary.to_string(),
                rollback: rollback.to_string(),
            },
        }
    }
}

fn verify_identity_with_retry<V>(
    verifier: &V,
    identity: &CodexIdentity,
) -> Result<IdentityVerification>
where
    V: IdentityVerifier,
{
    let mut last_error = None;

    for attempt in 0..REFRESH_RETRY_ATTEMPTS {
        match verifier.verify(identity) {
            Ok(verification) => return Ok(verification),
            Err(error) => {
                let retryable = is_retryable_refresh_error(&error);
                last_error = Some(error);
                if !retryable || attempt + 1 == REFRESH_RETRY_ATTEMPTS {
                    break;
                }
                thread::sleep(REFRESH_RETRY_BACKOFF);
            }
        }
    }

    Err(last_error.expect("retry loop must capture an error"))
}

fn is_retryable_refresh_error(error: &AppError) -> bool {
    match error {
        AppError::RpcTimeout { .. }
        | AppError::AppServerExited { .. }
        | AppError::RpcPayloadDecode { .. }
        | AppError::MissingRpcResult { .. } => true,
        AppError::ChildProcessFailed { .. } => true,
        AppError::RpcServer { code, message, .. } => {
            if is_known_non_retryable_refresh_error(*code, message) {
                return false;
            }
            *code == -32603 || *code == -32000
        }
        _ => false,
    }
}

fn is_known_non_retryable_refresh_error(code: i64, message: &str) -> bool {
    if code != -32603 {
        return false;
    }

    let lower = message.to_ascii_lowercase();
    lower.contains("deactivated_workspace") || lower.contains("402 payment required")
}

fn refresh_concurrency(identity_count: usize) -> usize {
    const MAX_REFRESH_CONCURRENCY: usize = 8;

    if identity_count == 0 {
        return 1;
    }

    let available = thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(4);
    available
        .clamp(1, MAX_REFRESH_CONCURRENCY)
        .min(identity_count)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tempfile::tempdir;

    use super::QuotaStatusService;
    use crate::bootstrap::BootstrapIdentityRequest;
    use crate::codex_rpc::IdentityVerifier;
    use crate::domain::identity::{AccountType, AuthMode, PlanType};
    use crate::domain::quota::QuotaStatusRecord;
    use crate::domain::verification::IdentityVerification;
    use crate::error::{AppError, Result};
    use crate::storage::quota_store::{JsonQuotaStore, QuotaStore};
    use crate::storage::registry_store::{JsonRegistryStore, RegistryStore};

    struct StubVerifier;

    impl IdentityVerifier for StubVerifier {
        fn verify(
            &self,
            identity: &crate::domain::identity::CodexIdentity,
        ) -> Result<IdentityVerification> {
            if identity.id.as_str() == "broken" {
                return Err(AppError::RpcServer {
                    method: "account/read".to_string(),
                    code: -32000,
                    message: "boom".to_string(),
                });
            }

            let used_percent = if identity.id.as_str() == "backup" {
                20
            } else {
                65
            };
            let rate_limit = crate::domain::quota::RateLimitSnapshot {
                credits: None,
                limit_id: Some("codex".to_string()),
                limit_name: Some("Codex".to_string()),
                plan_type: Some(PlanType::Plus),
                primary: Some(crate::domain::quota::RateLimitWindow {
                    resets_at: Some(1_700_000_000),
                    used_percent,
                    window_duration_mins: Some(300),
                }),
                secondary: None,
            };

            Ok(IdentityVerification {
                authenticated: true,
                auth_method: Some("chatgpt".to_string()),
                account_type: Some(AccountType::Chatgpt),
                email: Some(format!("{}@example.com", identity.id.as_str())),
                plan_type: Some(PlanType::Plus),
                requires_openai_auth: false,
                fallback_rate_limit: Some(rate_limit.clone()),
                rate_limits_by_limit_id: BTreeMap::from([("codex".to_string(), rate_limit)]),
            })
        }
    }

    #[derive(Debug, Clone)]
    struct FailingQuotaStore {
        record: Arc<Mutex<QuotaStatusRecord>>,
    }

    impl FailingQuotaStore {
        fn new(record: QuotaStatusRecord) -> Self {
            Self {
                record: Arc::new(Mutex::new(record)),
            }
        }
    }

    impl QuotaStore for FailingQuotaStore {
        fn load(&self) -> Result<QuotaStatusRecord> {
            Ok(self.record.lock().unwrap().clone())
        }

        fn save(&self, _record: &QuotaStatusRecord) -> Result<()> {
            Err(AppError::Io(std::io::Error::other("quota save failed")))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct FlakyVerifier {
        attempts: Arc<Mutex<BTreeMap<String, usize>>>,
    }

    impl FlakyVerifier {
        fn attempts_for(&self, identity_id: &str) -> usize {
            *self.attempts.lock().unwrap().get(identity_id).unwrap_or(&0)
        }
    }

    impl IdentityVerifier for FlakyVerifier {
        fn verify(
            &self,
            identity: &crate::domain::identity::CodexIdentity,
        ) -> Result<IdentityVerification> {
            let mut attempts = self.attempts.lock().unwrap();
            let attempt = attempts
                .entry(identity.id.as_str().to_string())
                .or_default();
            *attempt += 1;
            if identity.id.as_str() == "primary" && *attempt == 1 {
                return Err(AppError::RpcTimeout {
                    method: "account/rateLimits/read".to_string(),
                    timeout: Duration::from_secs(20),
                });
            }

            StubVerifier.verify(identity)
        }
    }

    #[derive(Debug, Clone, Default)]
    struct WorkspaceDeactivatedVerifier {
        attempts: Arc<Mutex<BTreeMap<String, usize>>>,
    }

    impl WorkspaceDeactivatedVerifier {
        fn attempts_for(&self, identity_id: &str) -> usize {
            *self.attempts.lock().unwrap().get(identity_id).unwrap_or(&0)
        }
    }

    impl IdentityVerifier for WorkspaceDeactivatedVerifier {
        fn verify(
            &self,
            identity: &crate::domain::identity::CodexIdentity,
        ) -> Result<IdentityVerification> {
            let mut attempts = self.attempts.lock().unwrap();
            let attempt = attempts
                .entry(identity.id.as_str().to_string())
                .or_default();
            *attempt += 1;

            Err(AppError::RpcServer {
                method: "account/rateLimits/read".to_string(),
                code: -32603,
                message: "failed to fetch codex rate limits: GET https://chatgpt.com/backend-api/wham/usage failed: 402 Payment Required; content-type=application/json; body={\"detail\":{\"code\":\"deactivated_workspace\"}}".to_string(),
            })
        }
    }

    #[test]
    fn refreshes_and_persists_quota_status_for_registered_identities() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let quota_store = JsonQuotaStore::new(temp.path());
        let registry_service =
            crate::identity_registry::IdentityRegistryService::new(registry_store.clone());

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

        let service = QuotaStatusService::new(registry_store, quota_store);
        let reports = service.refresh_all(&StubVerifier).unwrap();

        assert_eq!(reports.len(), 2);
        assert!(reports.iter().all(|report| report.refresh_error.is_none()));

        let cached_reports = service.cached_statuses().unwrap();
        assert_eq!(cached_reports.len(), 2);
        assert_eq!(
            cached_reports
                .iter()
                .find(|report| report.identity.id.as_str() == "backup")
                .and_then(|report| report.quota_status.as_ref())
                .and_then(|quota| quota.rate_limits_by_limit_id.get("codex"))
                .and_then(|snapshot| snapshot.primary.as_ref())
                .map(|window| window.used_percent),
            Some(20)
        );
    }

    #[test]
    fn refresh_all_reports_errors_without_hiding_successful_identities() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let quota_store = JsonQuotaStore::new(temp.path());
        let registry_service =
            crate::identity_registry::IdentityRegistryService::new(registry_store.clone());

        for name in ["Primary", "Broken"] {
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

        let service = QuotaStatusService::new(registry_store, quota_store);

        service.refresh_identity("Primary", &StubVerifier).unwrap();
        service
            .refresh_identity("Broken", &StubVerifier)
            .unwrap_err();

        let reports = service.refresh_all(&StubVerifier).unwrap();
        let broken_report = reports
            .iter()
            .find(|report| report.identity.id.as_str() == "broken")
            .unwrap();

        assert!(broken_report.refresh_error.is_some());
        assert!(broken_report.quota_status.is_none());
    }

    #[test]
    fn refresh_all_retries_transient_failure_once() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let quota_store = JsonQuotaStore::new(temp.path());
        let registry_service =
            crate::identity_registry::IdentityRegistryService::new(registry_store.clone());

        registry_service
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

        let verifier = FlakyVerifier::default();
        let service = QuotaStatusService::new(registry_store, quota_store);
        let reports = service.refresh_all(&verifier).unwrap();

        assert_eq!(verifier.attempts_for("primary"), 2);
        assert_eq!(reports.len(), 1);
        assert!(reports[0].refresh_error.is_none());
        assert_eq!(
            reports[0]
                .quota_status
                .as_ref()
                .and_then(|quota| quota.rate_limits_by_limit_id.get("codex"))
                .and_then(|snapshot| snapshot.primary.as_ref())
                .map(|window| window.used_percent),
            Some(65)
        );
    }

    #[test]
    fn refresh_all_does_not_retry_non_retryable_workspace_deactivation() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let quota_store = JsonQuotaStore::new(temp.path());
        let registry_service =
            crate::identity_registry::IdentityRegistryService::new(registry_store.clone());

        registry_service
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

        let verifier = WorkspaceDeactivatedVerifier::default();
        let service = QuotaStatusService::new(registry_store, quota_store);
        let reports = service.refresh_all(&verifier).unwrap();

        assert_eq!(verifier.attempts_for("primary"), 1);
        assert_eq!(reports.len(), 1);
        assert!(reports[0].refresh_error.is_some());
    }

    #[test]
    fn refresh_identity_rolls_back_registry_when_quota_save_fails() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let quota_store = FailingQuotaStore::new(QuotaStatusRecord::default());
        let registry_service =
            crate::identity_registry::IdentityRegistryService::new(registry_store.clone());

        registry_service
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

        let service = QuotaStatusService::new(registry_store.clone(), quota_store);
        let error = service
            .refresh_identity("Primary", &StubVerifier)
            .unwrap_err();
        assert!(error.to_string().contains("quota save failed"));

        let stored = registry_store.load().unwrap();
        let identity = stored
            .identities
            .values()
            .find(|identity| identity.id.as_str() == "primary")
            .unwrap();
        assert_eq!(identity.email, None);
        assert_eq!(identity.plan_type, None);
        assert_eq!(identity.last_verified_at, None);
    }

    #[test]
    fn refresh_all_rolls_back_registry_when_quota_save_fails() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let quota_store = FailingQuotaStore::new(QuotaStatusRecord::default());
        let registry_service =
            crate::identity_registry::IdentityRegistryService::new(registry_store.clone());

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

        let service = QuotaStatusService::new(registry_store.clone(), quota_store);
        let error = service.refresh_all(&StubVerifier).unwrap_err();
        assert!(error.to_string().contains("quota save failed"));

        let stored = registry_store.load().unwrap();
        assert!(stored
            .identities
            .values()
            .all(|identity| identity.email.is_none() && identity.last_verified_at.is_none()));
    }

    #[test]
    fn refresh_concurrency_is_bounded_and_non_zero() {
        assert_eq!(super::refresh_concurrency(0), 1);
        assert_eq!(super::refresh_concurrency(1), 1);
        assert!(super::refresh_concurrency(16) >= 1);
        assert!(super::refresh_concurrency(16) <= 8);
    }
}
