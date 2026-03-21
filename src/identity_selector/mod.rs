use crate::domain::health::IdentityHealthState;
use crate::domain::identity::CodexIdentity;
use crate::domain::policy::IdentitySelectionPolicy;
use crate::domain::quota::{IdentityQuotaStatus, RateLimitSnapshot};
use crate::error::{AppError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageBand {
    Healthy,
    Warning,
    Avoid,
    Exhausted,
}

impl UsageBand {
    pub fn from_used_percent(used_percent: i32, policy: &IdentitySelectionPolicy) -> Self {
        if used_percent >= policy.hard_stop_used_percent {
            Self::Exhausted
        } else if used_percent >= policy.avoid_used_percent {
            Self::Avoid
        } else if used_percent >= policy.warning_used_percent {
            Self::Warning
        } else {
            Self::Healthy
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Warning => "warning",
            Self::Avoid => "avoid",
            Self::Exhausted => "exhausted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketSource {
    Default,
    LimitId(String),
}

impl BucketSource {
    pub fn label(&self) -> String {
        match self {
            Self::Default => "default".to_string(),
            Self::LimitId(limit_id) => limit_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelevantBucket {
    pub source: BucketSource,
    pub snapshot: RateLimitSnapshot,
    pub max_used_percent: i32,
    pub remaining_headroom_percent: i32,
    pub usage_band: UsageBand,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectionReason {
    Disabled,
    ManuallyDisabled,
    PenaltyActive,
    Unauthenticated,
    MissingQuotaState,
    MissingBucketData,
    AvoidNewSession,
    Exhausted,
}

impl RejectionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::ManuallyDisabled => "manually_disabled",
            Self::PenaltyActive => "penalty_active",
            Self::Unauthenticated => "unauthenticated",
            Self::MissingQuotaState => "missing_quota_state",
            Self::MissingBucketData => "missing_bucket_data",
            Self::AvoidNewSession => "avoid_new_session_threshold",
            Self::Exhausted => "hard_stop_reached",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityEvaluation {
    pub relevant_bucket: Option<RelevantBucket>,
    pub rejection_reason: Option<RejectionReason>,
    pub health_state: Option<IdentityHealthState>,
}

impl IdentityEvaluation {
    pub fn selectable(&self) -> bool {
        self.rejection_reason.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct SelectedIdentity {
    pub identity: CodexIdentity,
    pub relevant_bucket: RelevantBucket,
}

#[derive(Debug, Clone)]
pub struct IdentitySelector {
    policy: IdentitySelectionPolicy,
    now: i64,
}

impl IdentitySelector {
    pub fn new(policy: IdentitySelectionPolicy, now: i64) -> Self {
        Self { policy, now }
    }

    pub fn policy(&self) -> &IdentitySelectionPolicy {
        &self.policy
    }

    pub fn evaluation_time(&self) -> i64 {
        self.now
    }

    pub fn evaluate(
        &self,
        identity: &CodexIdentity,
        quota_status: Option<&IdentityQuotaStatus>,
        health_state: Option<&IdentityHealthState>,
    ) -> IdentityEvaluation {
        if !identity.enabled {
            return IdentityEvaluation {
                relevant_bucket: None,
                rejection_reason: Some(RejectionReason::Disabled),
                health_state: health_state.cloned(),
            };
        }

        if let Some(health_state) = health_state {
            if health_state.manually_disabled {
                return IdentityEvaluation {
                    relevant_bucket: None,
                    rejection_reason: Some(RejectionReason::ManuallyDisabled),
                    health_state: Some(health_state.clone()),
                };
            }

            if health_state.penalty_active_at(self.now) {
                return IdentityEvaluation {
                    relevant_bucket: None,
                    rejection_reason: Some(RejectionReason::PenaltyActive),
                    health_state: Some(health_state.clone()),
                };
            }
        }

        if identity.authenticated != Some(true) {
            return IdentityEvaluation {
                relevant_bucket: None,
                rejection_reason: Some(RejectionReason::Unauthenticated),
                health_state: health_state.cloned(),
            };
        }

        let Some(quota_status) = quota_status else {
            return IdentityEvaluation {
                relevant_bucket: None,
                rejection_reason: Some(RejectionReason::MissingQuotaState),
                health_state: health_state.cloned(),
            };
        };

        let Some(relevant_bucket) = relevant_bucket(quota_status, &self.policy) else {
            return IdentityEvaluation {
                relevant_bucket: None,
                rejection_reason: Some(RejectionReason::MissingBucketData),
                health_state: health_state.cloned(),
            };
        };

        if relevant_bucket.max_used_percent >= self.policy.hard_stop_used_percent {
            return IdentityEvaluation {
                relevant_bucket: Some(relevant_bucket),
                rejection_reason: Some(RejectionReason::Exhausted),
                health_state: health_state.cloned(),
            };
        }

        if relevant_bucket.max_used_percent >= self.policy.avoid_used_percent {
            return IdentityEvaluation {
                relevant_bucket: Some(relevant_bucket),
                rejection_reason: Some(RejectionReason::AvoidNewSession),
                health_state: health_state.cloned(),
            };
        }

        IdentityEvaluation {
            relevant_bucket: Some(relevant_bucket),
            rejection_reason: None,
            health_state: health_state.cloned(),
        }
    }

    pub fn selectable_candidates<'a, I>(&self, candidates: I) -> Vec<SelectedIdentity>
    where
        I: IntoIterator<
            Item = (
                &'a CodexIdentity,
                Option<&'a IdentityQuotaStatus>,
                Option<&'a IdentityHealthState>,
            ),
        >,
    {
        let mut ranked = candidates
            .into_iter()
            .filter_map(|(identity, quota_status, health_state)| {
                let evaluation = self.evaluate(identity, quota_status, health_state);
                if !evaluation.selectable() {
                    return None;
                }
                Some(SelectedIdentity {
                    identity: identity.clone(),
                    relevant_bucket: evaluation.relevant_bucket?,
                })
            })
            .collect::<Vec<_>>();
        ranked.sort_by(compare_candidates);
        ranked.reverse();
        ranked
    }

    pub fn select_best<'a, I>(&self, candidates: I) -> Result<SelectedIdentity>
    where
        I: IntoIterator<
            Item = (
                &'a CodexIdentity,
                Option<&'a IdentityQuotaStatus>,
                Option<&'a IdentityHealthState>,
            ),
        >,
    {
        self.selectable_candidates(candidates)
            .into_iter()
            .next()
            .ok_or(AppError::NoSelectableIdentity)
    }
}

fn compare_candidates(left: &SelectedIdentity, right: &SelectedIdentity) -> std::cmp::Ordering {
    left.relevant_bucket
        .remaining_headroom_percent
        .cmp(&right.relevant_bucket.remaining_headroom_percent)
        .then_with(|| left.identity.priority.cmp(&right.identity.priority))
        .then_with(|| right.identity.id.as_str().cmp(left.identity.id.as_str()))
}

fn relevant_bucket(
    quota_status: &IdentityQuotaStatus,
    policy: &IdentitySelectionPolicy,
) -> Option<RelevantBucket> {
    if let Some(snapshot) = quota_status.rate_limits_by_limit_id.get("codex") {
        return build_bucket(BucketSource::LimitId("codex".to_string()), snapshot, policy);
    }

    if quota_status.rate_limits_by_limit_id.len() > 1 {
        return quota_status
            .rate_limits_by_limit_id
            .iter()
            .filter_map(|(limit_id, snapshot)| {
                build_bucket(BucketSource::LimitId(limit_id.clone()), snapshot, policy)
            })
            .max_by(|left, right| left.max_used_percent.cmp(&right.max_used_percent));
    }

    if let Some((limit_id, snapshot)) = quota_status.rate_limits_by_limit_id.iter().next() {
        return build_bucket(BucketSource::LimitId(limit_id.clone()), snapshot, policy);
    }

    quota_status
        .default_rate_limit
        .as_ref()
        .and_then(|snapshot| build_bucket(BucketSource::Default, snapshot, policy))
}

fn build_bucket(
    source: BucketSource,
    snapshot: &RateLimitSnapshot,
    policy: &IdentitySelectionPolicy,
) -> Option<RelevantBucket> {
    let max_used_percent = snapshot.max_used_percent()?;
    let remaining_headroom_percent = snapshot.remaining_headroom_percent()?;
    Some(RelevantBucket {
        source,
        snapshot: snapshot.clone(),
        max_used_percent,
        remaining_headroom_percent,
        usage_band: UsageBand::from_used_percent(max_used_percent, policy),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{BucketSource, IdentitySelector, UsageBand};
    use crate::domain::health::{IdentityFailureKind, IdentityHealthState};
    use crate::domain::identity::{
        current_timestamp, AccountType, AuthMode, CodexIdentity, IdentityId, IdentityKind, PlanType,
    };
    use crate::domain::policy::IdentitySelectionPolicy;
    use crate::domain::quota::{IdentityQuotaStatus, RateLimitSnapshot, RateLimitWindow};

    #[test]
    fn prefers_explicit_codex_bucket() {
        let identity = identity("primary", 0, true);
        let quota_status = IdentityQuotaStatus {
            identity_id: identity.id.clone(),
            default_rate_limit: Some(rate_limit("default", 10)),
            rate_limits_by_limit_id: BTreeMap::from([
                ("codex".to_string(), rate_limit("codex", 70)),
                ("weekly".to_string(), rate_limit("weekly", 95)),
            ]),
            updated_at: 100,
        };

        let evaluation = selector().evaluate(&identity, Some(&quota_status), None);
        assert!(evaluation.selectable());
        let relevant_bucket = evaluation.relevant_bucket.unwrap();
        assert_eq!(
            relevant_bucket.source,
            BucketSource::LimitId("codex".to_string())
        );
        assert_eq!(relevant_bucket.max_used_percent, 70);
        assert_eq!(relevant_bucket.usage_band, UsageBand::Healthy);
    }

    #[test]
    fn uses_worst_named_bucket_when_codex_is_absent() {
        let identity = identity("backup", 0, true);
        let quota_status = IdentityQuotaStatus {
            identity_id: identity.id.clone(),
            default_rate_limit: Some(rate_limit("default", 10)),
            rate_limits_by_limit_id: BTreeMap::from([
                ("short".to_string(), rate_limit("short", 25)),
                ("weekly".to_string(), rate_limit("weekly", 96)),
            ]),
            updated_at: 100,
        };

        let evaluation = selector().evaluate(&identity, Some(&quota_status), None);
        assert!(!evaluation.selectable());
        assert_eq!(
            evaluation.rejection_reason.unwrap().as_str(),
            "avoid_new_session_threshold"
        );
    }

    #[test]
    fn rejects_avoid_band_for_new_sessions() {
        let identity = identity("backup", 0, true);
        let quota_status = IdentityQuotaStatus {
            identity_id: identity.id.clone(),
            default_rate_limit: None,
            rate_limits_by_limit_id: BTreeMap::from([(
                "codex".to_string(),
                rate_limit("codex", 95),
            )]),
            updated_at: 100,
        };

        let evaluation = selector().evaluate(&identity, Some(&quota_status), None);
        assert!(!evaluation.selectable());
        assert_eq!(
            evaluation.rejection_reason.unwrap().as_str(),
            "avoid_new_session_threshold"
        );
    }

    #[test]
    fn chooses_warning_band_when_it_is_the_best_healthy_candidate() {
        let warning = identity("warning", 0, true);
        let exhausted = identity("exhausted", 0, true);
        let selector = selector();
        let selected = selector
            .select_best([
                (
                    &warning,
                    Some(&IdentityQuotaStatus {
                        identity_id: warning.id.clone(),
                        default_rate_limit: None,
                        rate_limits_by_limit_id: BTreeMap::from([(
                            "codex".to_string(),
                            rate_limit("codex", 90),
                        )]),
                        updated_at: 1,
                    }),
                    None,
                ),
                (
                    &exhausted,
                    Some(&IdentityQuotaStatus {
                        identity_id: exhausted.id.clone(),
                        default_rate_limit: None,
                        rate_limits_by_limit_id: BTreeMap::from([(
                            "codex".to_string(),
                            rate_limit("codex", 100),
                        )]),
                        updated_at: 1,
                    }),
                    None,
                ),
            ])
            .unwrap();

        assert_eq!(selected.identity.id.as_str(), "warning");
        assert_eq!(selected.relevant_bucket.usage_band, UsageBand::Warning);
    }

    #[test]
    fn rejects_manual_disable_and_active_penalty_before_quota_comparison() {
        let best = identity("best", 10, true);
        let penalized = identity("penalized", 100, true);
        let manually_disabled = identity("manual", 100, true);
        let selector = selector();
        let selected = selector
            .select_best([
                (&best, Some(&quota_status(&best, 50)), None),
                (
                    &penalized,
                    Some(&quota_status(&penalized, 10)),
                    Some(&IdentityHealthState {
                        identity_id: penalized.id.clone(),
                        penalty_until: Some(selector.evaluation_time() + 60),
                        last_failure_kind: Some(IdentityFailureKind::RateLimit),
                        last_failure_at: Some(selector.evaluation_time()),
                        last_failure_message: Some("429".to_string()),
                        manually_disabled: false,
                        updated_at: selector.evaluation_time(),
                    }),
                ),
                (
                    &manually_disabled,
                    Some(&quota_status(&manually_disabled, 10)),
                    Some(&IdentityHealthState {
                        identity_id: manually_disabled.id.clone(),
                        penalty_until: None,
                        last_failure_kind: None,
                        last_failure_at: None,
                        last_failure_message: None,
                        manually_disabled: true,
                        updated_at: selector.evaluation_time(),
                    }),
                ),
            ])
            .unwrap();

        assert_eq!(selected.identity.id.as_str(), "best");
    }

    #[test]
    fn honors_policy_thresholds_instead_of_hardcoded_defaults() {
        let identity = identity("primary", 0, true);
        let selector = IdentitySelector::new(
            IdentitySelectionPolicy {
                warning_used_percent: 10,
                avoid_used_percent: 20,
                hard_stop_used_percent: 30,
                rate_limit_cooldown_secs: 1,
                auth_failure_cooldown_secs: 1,
            },
            1_700_000_000,
        );
        let evaluation = selector.evaluate(&identity, Some(&quota_status(&identity, 25)), None);
        assert_eq!(
            evaluation.rejection_reason.unwrap().as_str(),
            "avoid_new_session_threshold"
        );
    }

    #[test]
    fn rejects_exhausted_or_unauthenticated_identities_and_tie_breaks_by_priority() {
        let best = identity("best", 10, true);
        let fallback = identity("fallback", 1, true);
        let exhausted = identity("exhausted", 100, true);
        let unauthenticated = identity("unauthenticated", 100, false);

        let selector = selector();
        let selected = selector
            .select_best([
                (&best, Some(&quota_status(&best, 50)), None),
                (&fallback, Some(&quota_status(&fallback, 50)), None),
                (&exhausted, Some(&quota_status(&exhausted, 100)), None),
                (
                    &unauthenticated,
                    Some(&quota_status(&unauthenticated, 10)),
                    None,
                ),
            ])
            .unwrap();

        assert_eq!(selected.identity.id.as_str(), "best");
        assert_eq!(selected.relevant_bucket.remaining_headroom_percent, 50);
    }

    fn selector() -> IdentitySelector {
        IdentitySelector::new(IdentitySelectionPolicy::default(), 1_700_000_000)
    }

    fn quota_status(identity: &CodexIdentity, used_percent: i32) -> IdentityQuotaStatus {
        IdentityQuotaStatus {
            identity_id: identity.id.clone(),
            default_rate_limit: None,
            rate_limits_by_limit_id: BTreeMap::from([(
                "codex".to_string(),
                rate_limit("codex", used_percent),
            )]),
            updated_at: 1,
        }
    }

    fn identity(id: &str, priority: u32, authenticated: bool) -> CodexIdentity {
        CodexIdentity {
            id: IdentityId::from_display_name(id).unwrap(),
            display_name: id.to_string(),
            kind: IdentityKind::ChatgptWorkspace,
            auth_mode: AuthMode::Chatgpt,
            codex_home: std::path::PathBuf::from(format!("/tmp/{id}")),
            shared_sessions_root: std::path::PathBuf::from("/tmp/shared/sessions"),
            forced_login_method: None,
            forced_chatgpt_workspace_id: None,
            api_key_env_var: None,
            email: Some(format!("{id}@example.com")),
            plan_type: Some(PlanType::Plus),
            account_type: Some(AccountType::Chatgpt),
            authenticated: Some(authenticated),
            last_auth_method: Some("chatgpt".to_string()),
            enabled: true,
            priority,
            notes: None,
            workspace_force_probe: None,
            imported_auth: false,
            created_at: current_timestamp().unwrap(),
            last_verified_at: Some(current_timestamp().unwrap()),
        }
    }

    fn rate_limit(limit_id: &str, used_percent: i32) -> RateLimitSnapshot {
        RateLimitSnapshot {
            credits: None,
            limit_id: Some(limit_id.to_string()),
            limit_name: Some(limit_id.to_string()),
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
