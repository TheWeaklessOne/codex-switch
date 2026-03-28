use std::ffi::OsString;
use std::path::Path;

use crate::codex_rpc::IdentityVerifier;
use crate::decision_log::{DecisionLogService, LoggedSelectionEvent};
use crate::domain::health::{IdentityFailureKind, IdentityHealthRecord};
use crate::domain::identity::{current_timestamp, CodexIdentity};
use crate::error::Result;
use crate::identity_cleanup::{
    auto_remove_deactivated_workspace_identities, AutoRemovalNotice, ManagedIdentityRemovalService,
};
use crate::identity_health::IdentityHealthService;
use crate::identity_selection::IdentitySelectionService;
use crate::identity_selector::{IdentitySelector, RejectionReason, SelectedIdentity};
use crate::launcher::{CapturedLaunchFailure, CodexLauncher, LaunchOutcome};
use crate::quota_status::{IdentityStatusReport, QuotaStatusService};
use crate::storage::health_store::IdentityHealthStore;
use crate::storage::policy_store::SelectionPolicyStore;
use crate::storage::quota_store::QuotaStore;
use crate::storage::registry_store::RegistryStore;
use crate::storage::selection_event_store::SelectionEventStore;
use crate::storage::selection_store::SelectionStore;

#[derive(Debug, Clone)]
pub struct ExecFailoverRequest {
    pub cached: bool,
    pub reason: String,
    pub args: Vec<OsString>,
}

#[derive(Debug, Clone)]
pub struct PenalizedIdentity {
    pub identity: CodexIdentity,
    pub failure_kind: IdentityFailureKind,
    pub penalty_until: Option<i64>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SkippedIdentity {
    pub identity: CodexIdentity,
    pub rejection_reason: RejectionReason,
}

#[derive(Debug, Clone)]
pub struct ExecFailoverResult {
    pub initial_identity: Option<CodexIdentity>,
    pub skipped_due_to_health: Vec<SkippedIdentity>,
    pub penalized_during_run: Vec<PenalizedIdentity>,
    pub launched: Option<LaunchOutcome>,
    pub launched_candidate: Option<SelectedIdentity>,
    pub decision_log: Option<LoggedSelectionEvent>,
    pub auto_removal_notices: Vec<AutoRemovalNotice>,
}

impl ExecFailoverResult {
    pub fn no_eligible_identity(&self) -> bool {
        self.launched.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct ExecFailoverStores<R, Q, S, E, P, H> {
    pub registry_store: R,
    pub quota_store: Q,
    pub selection_store: S,
    pub decision_store: E,
    pub policy_store: P,
    pub health_store: H,
}

#[derive(Debug, Clone)]
pub struct ExecFailoverService<R, Q, S, E, P, H, V> {
    stores: ExecFailoverStores<R, Q, S, E, P, H>,
    verifier: V,
    launcher: CodexLauncher,
}

impl<R, Q, S, E, P, H, V> ExecFailoverService<R, Q, S, E, P, H, V> {
    pub fn new(
        _base_root: &Path,
        stores: ExecFailoverStores<R, Q, S, E, P, H>,
        verifier: V,
    ) -> Self {
        Self {
            stores,
            verifier,
            launcher: CodexLauncher,
        }
    }
}

impl<R, Q, S, E, P, H, V> ExecFailoverService<R, Q, S, E, P, H, V>
where
    R: RegistryStore + Clone,
    Q: QuotaStore + Clone,
    S: SelectionStore + Clone,
    E: SelectionEventStore,
    P: SelectionPolicyStore + Clone,
    H: IdentityHealthStore + Clone,
    V: IdentityVerifier + Sync,
{
    pub fn launch(&self, request: ExecFailoverRequest) -> Result<ExecFailoverResult> {
        let (reports, auto_removal_notices) = self.load_reports(request.cached)?;
        let policy = self.stores.policy_store.load()?.policy;
        let health_record = self.stores.health_store.load()?;
        let selector = IdentitySelector::new(policy.clone(), current_timestamp()?);
        let skipped_due_to_health =
            skipped_due_to_health(&selector, &health_record, &reports, request.cached);
        let candidates = selector.selectable_candidates(
            reports
                .iter()
                .filter(|report| request.cached || report.refresh_error.is_none())
                .map(|report| {
                    (
                        &report.identity,
                        report.quota_status.as_ref(),
                        health_record.identities.get(&report.identity.id),
                    )
                }),
        );
        let initial_identity = candidates
            .first()
            .map(|candidate| candidate.identity.clone());
        let mut penalized_during_run = Vec::new();

        for candidate in candidates {
            match self
                .launcher
                .launch_codex_captured(&candidate.identity, &request.args)?
            {
                Ok(outcome) => {
                    let selection_service = IdentitySelectionService::new(
                        self.stores.selection_store.clone(),
                        self.stores.registry_store.clone(),
                    );
                    selection_service.store_automatic(
                        outcome.identity.clone(),
                        Some("selected automatically after exec failover"),
                    )?;
                    let mut result = ExecFailoverResult {
                        initial_identity,
                        skipped_due_to_health,
                        penalized_during_run,
                        launched: Some(outcome),
                        launched_candidate: Some(candidate),
                        decision_log: None,
                        auto_removal_notices: auto_removal_notices.clone(),
                    };
                    result.decision_log = DecisionLogService::new(
                        &self.stores.decision_store,
                        selector,
                        health_record.clone(),
                    )
                    .log_exec_failover(
                        &result,
                        &reports,
                        request.cached,
                        &request.reason,
                    )?;
                    return Ok(result);
                }
                Err(failure) => {
                    let Some(failure_kind) = classify_failure(&failure) else {
                        return Err(failure.to_app_error());
                    };
                    let failure_summary = failure_summary(failure_kind, &failure);
                    let cooldown_secs = match failure_kind {
                        IdentityFailureKind::RateLimit => policy.rate_limit_cooldown_secs,
                        IdentityFailureKind::Auth => policy.auth_failure_cooldown_secs,
                    };
                    let health_service = IdentityHealthService::new(
                        self.stores.registry_store.clone(),
                        self.stores.health_store.clone(),
                    );
                    let updated_state = health_service.apply_penalty(
                        &candidate.identity.id,
                        failure_kind,
                        Some(failure_summary.clone()),
                        cooldown_secs,
                        current_timestamp()?,
                    )?;
                    penalized_during_run.push(PenalizedIdentity {
                        identity: candidate.identity,
                        failure_kind,
                        penalty_until: updated_state.penalty_until,
                        failure_message: Some(failure_summary),
                    });
                }
            }
        }

        let mut result = ExecFailoverResult {
            initial_identity,
            skipped_due_to_health,
            penalized_during_run,
            launched: None,
            launched_candidate: None,
            decision_log: None,
            auto_removal_notices,
        };
        result.decision_log =
            DecisionLogService::new(&self.stores.decision_store, selector, health_record)
                .log_exec_failover(&result, &reports, request.cached, &request.reason)?;
        Ok(result)
    }

    fn load_reports(
        &self,
        cached: bool,
    ) -> Result<(Vec<IdentityStatusReport>, Vec<AutoRemovalNotice>)> {
        let service = QuotaStatusService::new(
            self.stores.registry_store.clone(),
            self.stores.quota_store.clone(),
        );
        if cached {
            Ok((service.cached_statuses()?, Vec::new()))
        } else {
            let reports = service.refresh_all(&self.verifier)?;
            let remover = ManagedIdentityRemovalService::new(
                self.stores.registry_store.clone(),
                self.stores.quota_store.clone(),
                self.stores.health_store.clone(),
                self.stores.selection_store.clone(),
            );
            let sweep = auto_remove_deactivated_workspace_identities(reports, &remover);
            Ok((sweep.reports, sweep.notices))
        }
    }
}

fn skipped_due_to_health(
    selector: &IdentitySelector,
    health_record: &IdentityHealthRecord,
    reports: &[IdentityStatusReport],
    cached: bool,
) -> Vec<SkippedIdentity> {
    reports
        .iter()
        .filter(|report| cached || report.refresh_error.is_none())
        .filter_map(|report| {
            let evaluation = selector.evaluate(
                &report.identity,
                report.quota_status.as_ref(),
                health_record.identities.get(&report.identity.id),
            );
            let rejection_reason = evaluation.rejection_reason?;
            if !matches!(
                rejection_reason,
                RejectionReason::ManuallyDisabled | RejectionReason::PenaltyActive
            ) {
                return None;
            }
            Some(SkippedIdentity {
                identity: report.identity.clone(),
                rejection_reason,
            })
        })
        .collect()
}

fn classify_failure(failure: &CapturedLaunchFailure) -> Option<IdentityFailureKind> {
    let combined = format!("{}\n{}", failure.stdout, failure.stderr).to_ascii_lowercase();

    const RATE_LIMIT_TOKENS: &[&str] =
        &["429", "rate limit", "quota exhausted", "too many requests"];
    if RATE_LIMIT_TOKENS
        .iter()
        .any(|token| combined.contains(token))
    {
        return Some(IdentityFailureKind::RateLimit);
    }

    const AUTH_TOKENS: &[&str] = &[
        "401",
        "unauthorized",
        "login required",
        "authentication required",
        "payment required",
        "subscription inactive",
    ];
    if AUTH_TOKENS.iter().any(|token| combined.contains(token)) {
        return Some(IdentityFailureKind::Auth);
    }

    None
}

fn failure_summary(failure_kind: IdentityFailureKind, failure: &CapturedLaunchFailure) -> String {
    let failure_label = match failure_kind {
        IdentityFailureKind::RateLimit => "rate-limit failure",
        IdentityFailureKind::Auth => "auth failure",
    };
    format!(
        "{failure_label} detected while launching codex (exit {})",
        failure.code
    )
}

#[cfg(test)]
mod tests {
    use super::classify_failure;
    use crate::domain::health::IdentityFailureKind;
    use crate::domain::identity::{
        current_timestamp, AuthMode, CodexIdentity, IdentityId, IdentityKind,
    };
    use crate::launcher::CapturedLaunchFailure;

    #[test]
    fn classifies_rate_limit_failures() {
        let failure = failure("too many requests", "");
        assert_eq!(
            classify_failure(&failure),
            Some(IdentityFailureKind::RateLimit)
        );
    }

    #[test]
    fn classifies_auth_failures() {
        let failure = failure("", "401 unauthorized");
        assert_eq!(classify_failure(&failure), Some(IdentityFailureKind::Auth));
    }

    #[test]
    fn leaves_unknown_failures_unclassified() {
        let failure = failure("", "segmentation fault");
        assert!(classify_failure(&failure).is_none());
    }

    fn failure(stdout: &str, stderr: &str) -> CapturedLaunchFailure {
        CapturedLaunchFailure {
            identity: CodexIdentity {
                id: IdentityId::from_display_name("Primary").unwrap(),
                display_name: "Primary".to_string(),
                kind: IdentityKind::ChatgptWorkspace,
                auth_mode: AuthMode::Chatgpt,
                codex_home: "/tmp/primary".into(),
                shared_sessions_root: "/tmp/shared/sessions".into(),
                forced_login_method: None,
                forced_chatgpt_workspace_id: None,
                api_key_env_var: None,
                email: None,
                plan_type: None,
                account_type: None,
                authenticated: Some(true),
                last_auth_method: None,
                enabled: true,
                priority: 0,
                notes: None,
                workspace_force_probe: None,
                imported_auth: false,
                created_at: current_timestamp().unwrap(),
                last_verified_at: None,
            },
            command: vec!["resume".to_string()],
            code: "1".to_string(),
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
        }
    }
}
