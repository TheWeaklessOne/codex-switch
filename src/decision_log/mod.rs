use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::continuation::ContinueThreadResult;
use crate::domain::decision::SelectionEvent;
use crate::domain::health::IdentityHealthRecord;
use crate::domain::identity::IdentityId;
use crate::domain::policy::IdentitySelectionPolicy;
use crate::error::Result;
use crate::exec_failover::ExecFailoverResult;
use crate::identity_selector::{IdentityEvaluation, IdentitySelector, SelectedIdentity};
use crate::quota_status::IdentityStatusReport;
use crate::storage::selection_event_store::SelectionEventStore;

#[derive(Debug, Clone)]
pub struct LoggedSelectionEvent {
    pub event: SelectionEvent,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThresholdPolicyLog {
    pub warning_used_percent: i32,
    pub avoid_new_sessions_percent: i32,
    pub hard_stop_used_percent: i32,
}

impl Default for ThresholdPolicyLog {
    fn default() -> Self {
        Self {
            warning_used_percent: 85,
            avoid_new_sessions_percent: 95,
            hard_stop_used_percent: 100,
        }
    }
}

impl From<&IdentitySelectionPolicy> for ThresholdPolicyLog {
    fn from(value: &IdentitySelectionPolicy) -> Self {
        Self {
            warning_used_percent: value.warning_used_percent,
            avoid_new_sessions_percent: value.avoid_used_percent,
            hard_stop_used_percent: value.hard_stop_used_percent,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateDecisionLog {
    pub identity_id: IdentityId,
    pub display_name: String,
    pub eligible: bool,
    pub rejection_reason: Option<String>,
    pub manually_disabled: bool,
    pub penalty_active: bool,
    pub penalty_until: Option<i64>,
    pub last_failure_kind: Option<String>,
    pub bucket_source: Option<String>,
    pub used_percent: Option<i32>,
    pub remaining_headroom_percent: Option<i32>,
    pub usage_band: Option<String>,
    pub refresh_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSessionDecisionLog {
    pub kind: String,
    pub cached: bool,
    pub threshold_policy: ThresholdPolicyLog,
    pub selected_bucket_source: String,
    pub selected_usage_band: String,
    pub candidates: Vec<CandidateDecisionLog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadHandoffDecisionLog {
    pub kind: String,
    pub thread_id: String,
    pub cached: bool,
    pub threshold_policy: ThresholdPolicyLog,
    pub continue_mode: String,
    pub checkpoint_id: String,
    pub fallback_reason: Option<String>,
    pub source_identity_id: IdentityId,
    pub selected_bucket_source: String,
    pub selected_usage_band: String,
    pub candidates: Vec<CandidateDecisionLog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecFailoverPenaltyLog {
    pub identity_id: IdentityId,
    pub display_name: String,
    pub failure_kind: String,
    pub penalty_until: Option<i64>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecFailoverDecisionLog {
    pub kind: String,
    pub cached: bool,
    pub threshold_policy: ThresholdPolicyLog,
    pub initial_identity_id: Option<IdentityId>,
    pub initial_display_name: Option<String>,
    pub final_identity_id: Option<IdentityId>,
    pub final_display_name: Option<String>,
    pub final_bucket_source: Option<String>,
    pub final_usage_band: Option<String>,
    pub penalized_during_run: Vec<ExecFailoverPenaltyLog>,
    pub candidates: Vec<CandidateDecisionLog>,
}

#[derive(Debug, Clone)]
pub struct DecisionLogService<S> {
    store: S,
    selector: IdentitySelector,
    health: IdentityHealthRecord,
}

impl<S> DecisionLogService<S> {
    pub fn new(store: S, selector: IdentitySelector, health: IdentityHealthRecord) -> Self {
        Self {
            store,
            selector,
            health,
        }
    }
}

impl<S> DecisionLogService<S>
where
    S: SelectionEventStore,
{
    pub fn log_new_session_selection(
        &self,
        selected: &SelectedIdentity,
        reports: &[IdentityStatusReport],
        cached: bool,
        reason: &str,
    ) -> Result<LoggedSelectionEvent> {
        let decision = NewSessionDecisionLog {
            kind: "new_session".to_string(),
            cached,
            threshold_policy: ThresholdPolicyLog::from(self.selector.policy()),
            selected_bucket_source: selected.relevant_bucket.source.label(),
            selected_usage_band: selected.relevant_bucket.usage_band.as_str().to_string(),
            candidates: decision_candidates(&self.selector, &self.health, reports),
        };

        self.append_event(
            selected.identity.id.clone(),
            reason,
            None,
            serde_json::to_value(decision)?,
        )
    }

    pub fn log_thread_handoff(
        &self,
        selected: &SelectedIdentity,
        reports: &[IdentityStatusReport],
        cached: bool,
        reason: &str,
        result: &ContinueThreadResult,
    ) -> Result<LoggedSelectionEvent> {
        let decision = ThreadHandoffDecisionLog {
            kind: "thread_handoff".to_string(),
            thread_id: result.baseline_snapshot.thread_id.clone(),
            cached,
            threshold_policy: ThresholdPolicyLog::from(self.selector.policy()),
            continue_mode: result.mode.as_str().to_string(),
            checkpoint_id: result.checkpoint.id.clone(),
            fallback_reason: result.checkpoint.fallback_reason.clone(),
            source_identity_id: result.source_identity.id.clone(),
            selected_bucket_source: selected.relevant_bucket.source.label(),
            selected_usage_band: selected.relevant_bucket.usage_band.as_str().to_string(),
            candidates: decision_candidates(&self.selector, &self.health, reports),
        };

        self.append_event(
            selected.identity.id.clone(),
            reason,
            Some(result.source_identity.id.clone()),
            serde_json::to_value(decision)?,
        )
    }

    pub fn log_exec_failover(
        &self,
        result: &ExecFailoverResult,
        reports: &[IdentityStatusReport],
        cached: bool,
        reason: &str,
    ) -> Result<Option<LoggedSelectionEvent>> {
        let event_identity_id = result
            .launched_candidate
            .as_ref()
            .map(|selected| selected.identity.id.clone())
            .or_else(|| {
                result
                    .initial_identity
                    .as_ref()
                    .map(|identity| identity.id.clone())
            });
        let Some(event_identity_id) = event_identity_id else {
            return Ok(None);
        };

        let decision = ExecFailoverDecisionLog {
            kind: "exec_failover".to_string(),
            cached,
            threshold_policy: ThresholdPolicyLog::from(self.selector.policy()),
            initial_identity_id: result
                .initial_identity
                .as_ref()
                .map(|identity| identity.id.clone()),
            initial_display_name: result
                .initial_identity
                .as_ref()
                .map(|identity| identity.display_name.clone()),
            final_identity_id: result
                .launched_candidate
                .as_ref()
                .map(|selected| selected.identity.id.clone()),
            final_display_name: result
                .launched_candidate
                .as_ref()
                .map(|selected| selected.identity.display_name.clone()),
            final_bucket_source: result
                .launched_candidate
                .as_ref()
                .map(|selected| selected.relevant_bucket.source.label()),
            final_usage_band: result
                .launched_candidate
                .as_ref()
                .map(|selected| selected.relevant_bucket.usage_band.as_str().to_string()),
            penalized_during_run: result
                .penalized_during_run
                .iter()
                .map(|penalty| ExecFailoverPenaltyLog {
                    identity_id: penalty.identity.id.clone(),
                    display_name: penalty.identity.display_name.clone(),
                    failure_kind: penalty.failure_kind.to_string(),
                    penalty_until: penalty.penalty_until,
                    failure_message: penalty.failure_message.clone(),
                })
                .collect(),
            candidates: decision_candidates(&self.selector, &self.health, reports),
        };

        self.append_event(
            event_identity_id,
            reason,
            None,
            serde_json::to_value(decision)?,
        )
        .map(Some)
    }

    fn append_event(
        &self,
        identity_id: IdentityId,
        reason: &str,
        from_identity_id: Option<IdentityId>,
        decision_json: Value,
    ) -> Result<LoggedSelectionEvent> {
        let event = SelectionEvent::new(identity_id, reason, from_identity_id, decision_json)?;
        let path = self.store.append(&event)?;
        Ok(LoggedSelectionEvent { event, path })
    }
}

fn decision_candidates(
    selector: &IdentitySelector,
    health: &IdentityHealthRecord,
    reports: &[IdentityStatusReport],
) -> Vec<CandidateDecisionLog> {
    reports
        .iter()
        .map(|report| {
            candidate_log(
                selector.evaluate(
                    &report.identity,
                    report.quota_status.as_ref(),
                    health.identities.get(&report.identity.id),
                ),
                selector.evaluation_time(),
                report,
            )
        })
        .collect()
}

fn candidate_log(
    evaluation: IdentityEvaluation,
    now: i64,
    report: &IdentityStatusReport,
) -> CandidateDecisionLog {
    let relevant_bucket = evaluation.relevant_bucket.as_ref();
    let health_state = evaluation.health_state.as_ref();
    CandidateDecisionLog {
        identity_id: report.identity.id.clone(),
        display_name: report.identity.display_name.clone(),
        eligible: evaluation.selectable(),
        rejection_reason: evaluation
            .rejection_reason
            .as_ref()
            .map(|reason| reason.as_str().to_string()),
        manually_disabled: health_state.is_some_and(|state| state.manually_disabled),
        penalty_active: health_state.is_some_and(|state| state.penalty_active_at(now)),
        penalty_until: health_state.and_then(|state| state.penalty_until),
        last_failure_kind: health_state
            .and_then(|state| state.last_failure_kind.map(|kind| kind.to_string())),
        bucket_source: relevant_bucket.map(|bucket| bucket.source.label()),
        used_percent: relevant_bucket.map(|bucket| bucket.max_used_percent),
        remaining_headroom_percent: relevant_bucket.map(|bucket| bucket.remaining_headroom_percent),
        usage_band: relevant_bucket.map(|bucket| bucket.usage_band.as_str().to_string()),
        refresh_error: report.refresh_error.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::DecisionLogService;
    use crate::continuation::ContinueMode;
    use crate::domain::checkpoint::{CheckpointMode, TaskCheckpoint};
    use crate::domain::health::IdentityHealthRecord;
    use crate::domain::identity::{
        current_timestamp, AccountType, AuthMode, CodexIdentity, IdentityId, IdentityKind, PlanType,
    };
    use crate::domain::policy::IdentitySelectionPolicy;
    use crate::domain::quota::{IdentityQuotaStatus, RateLimitSnapshot, RateLimitWindow};
    use crate::domain::thread::{ThreadLeaseRecord, ThreadLeaseState, ThreadSnapshot, TurnStatus};
    use crate::identity_selector::{
        BucketSource, IdentitySelector, RelevantBucket, SelectedIdentity, UsageBand,
    };
    use crate::quota_status::IdentityStatusReport;
    use crate::storage::selection_event_store::JsonSelectionEventStore;

    #[test]
    fn logs_new_session_selection_with_candidate_details() {
        let temp = tempdir().unwrap();
        let logger = DecisionLogService::new(
            JsonSelectionEventStore::new(temp.path()),
            IdentitySelector::new(IdentitySelectionPolicy::default(), 1_700_000_000),
            IdentityHealthRecord::default(),
        );
        let primary = identity("Primary", true);
        let backup = identity("Backup", true);
        let reports = vec![report(primary.clone(), 98), report(backup.clone(), 20)];
        let selected = SelectedIdentity {
            identity: backup,
            relevant_bucket: RelevantBucket {
                source: BucketSource::LimitId("codex".to_string()),
                snapshot: rate_limit(20),
                max_used_percent: 20,
                remaining_headroom_percent: 80,
                usage_band: UsageBand::Healthy,
            },
        };

        let logged = logger
            .log_new_session_selection(&selected, &reports, true, "auto launch")
            .unwrap();

        let event = std::fs::read_to_string(logged.path).unwrap();
        assert!(event.contains("\"kind\": \"new_session\""));
        assert!(event.contains("\"avoid_new_session_threshold\""));
        assert!(event.contains("\"identity_id\": \"backup\""));
    }

    #[test]
    fn logs_thread_handoff_mode_and_fallback_reason() {
        let temp = tempdir().unwrap();
        let logger = DecisionLogService::new(
            JsonSelectionEventStore::new(temp.path()),
            IdentitySelector::new(IdentitySelectionPolicy::default(), 1_700_000_000),
            IdentityHealthRecord::default(),
        );
        let source = identity("Source", true);
        let target = identity("Target", true);
        let reports = vec![report(source.clone(), 99), report(target.clone(), 10)];
        let selected = SelectedIdentity {
            identity: target.clone(),
            relevant_bucket: RelevantBucket {
                source: BucketSource::LimitId("codex".to_string()),
                snapshot: rate_limit(10),
                max_used_percent: 10,
                remaining_headroom_percent: 90,
                usage_band: UsageBand::Healthy,
            },
        };
        let checkpoint = TaskCheckpoint::new(
            &ThreadSnapshot {
                thread_id: "thread-1".to_string(),
                created_at: 1,
                updated_at: 2,
                status: "interrupted".to_string(),
                path: None,
                turn_ids: vec!["turn-a".to_string()],
                latest_turn_id: Some("turn-a".to_string()),
                latest_turn_status: Some(TurnStatus::Interrupted),
            },
            source.id.clone(),
            target.id.clone(),
            CheckpointMode::ResumeViaCheckpoint,
            "automatic_handoff",
            Some("shared history mismatch".to_string()),
        )
        .unwrap();
        let result = crate::continuation::ContinueThreadResult {
            source_identity: source.clone(),
            target_identity: target,
            mode: ContinueMode::ResumeViaCheckpoint,
            checkpoint,
            checkpoint_path: temp.path().join("checkpoint.json"),
            lease: ThreadLeaseRecord {
                thread_id: "thread-1".to_string(),
                owner_identity_id: source.id.clone(),
                lease_state: ThreadLeaseState::Released,
                lease_token: "lease".to_string(),
                handoff_to_identity_id: None,
                handoff_reason: None,
                last_heartbeat_at: 1,
                updated_at: 1,
            },
            baseline_snapshot: ThreadSnapshot {
                thread_id: "thread-1".to_string(),
                created_at: 1,
                updated_at: 2,
                status: "interrupted".to_string(),
                path: None,
                turn_ids: vec!["turn-a".to_string()],
                latest_turn_id: Some("turn-a".to_string()),
                latest_turn_status: Some(TurnStatus::Interrupted),
            },
            target_snapshot: None,
            launch: None,
        };

        let logged = logger
            .log_thread_handoff(&selected, &reports, false, "automatic handoff", &result)
            .unwrap();

        let event = std::fs::read_to_string(logged.path).unwrap();
        assert!(event.contains("\"kind\": \"thread_handoff\""));
        assert!(event.contains("\"continue_mode\": \"resume_via_checkpoint\""));
        assert!(event.contains("\"fallback_reason\": \"shared history mismatch\""));
        assert!(event.contains("\"from_identity_id\": \"source\""));
    }

    fn identity(name: &str, authenticated: bool) -> CodexIdentity {
        CodexIdentity {
            id: IdentityId::from_display_name(name).unwrap(),
            display_name: name.to_string(),
            kind: IdentityKind::ChatgptWorkspace,
            auth_mode: AuthMode::Chatgpt,
            codex_home: PathBuf::from(format!("/tmp/{name}")),
            shared_sessions_root: PathBuf::from("/tmp/shared/sessions"),
            forced_login_method: None,
            forced_chatgpt_workspace_id: None,
            api_key_env_var: None,
            email: Some(format!("{}@example.com", name.to_lowercase())),
            plan_type: Some(PlanType::Plus),
            account_type: Some(AccountType::Chatgpt),
            authenticated: Some(authenticated),
            last_auth_method: Some("chatgpt".to_string()),
            enabled: true,
            priority: 0,
            notes: None,
            workspace_force_probe: None,
            imported_auth: false,
            created_at: current_timestamp().unwrap(),
            last_verified_at: Some(current_timestamp().unwrap()),
        }
    }

    fn report(identity: CodexIdentity, used_percent: i32) -> IdentityStatusReport {
        IdentityStatusReport {
            identity: identity.clone(),
            quota_status: Some(IdentityQuotaStatus {
                identity_id: identity.id,
                default_rate_limit: None,
                rate_limits_by_limit_id: BTreeMap::from([(
                    "codex".to_string(),
                    rate_limit(used_percent),
                )]),
                updated_at: 1,
            }),
            refresh_error: None,
            refresh_error_kind: None,
        }
    }

    fn rate_limit(used_percent: i32) -> RateLimitSnapshot {
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
        }
    }
}
