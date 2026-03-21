use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::domain::identity::{IdentityId, PlanType};

pub const QUOTA_STATUS_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreditsSnapshot {
    pub balance: Option<String>,
    pub has_credits: bool,
    pub unlimited: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitWindow {
    pub resets_at: Option<i64>,
    pub used_percent: i32,
    pub window_duration_mins: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitSnapshot {
    pub credits: Option<CreditsSnapshot>,
    pub limit_id: Option<String>,
    pub limit_name: Option<String>,
    pub plan_type: Option<PlanType>,
    pub primary: Option<RateLimitWindow>,
    pub secondary: Option<RateLimitWindow>,
}

impl RateLimitSnapshot {
    pub fn max_used_percent(&self) -> Option<i32> {
        [self.primary.as_ref(), self.secondary.as_ref()]
            .into_iter()
            .flatten()
            .map(|window| window.used_percent)
            .max()
    }

    pub fn remaining_headroom_percent(&self) -> Option<i32> {
        self.max_used_percent()
            .map(|used_percent| (100 - used_percent).clamp(0, 100))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityQuotaStatus {
    pub identity_id: IdentityId,
    pub default_rate_limit: Option<RateLimitSnapshot>,
    pub rate_limits_by_limit_id: BTreeMap<String, RateLimitSnapshot>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaStatusRecord {
    pub version: u32,
    pub statuses: BTreeMap<IdentityId, IdentityQuotaStatus>,
}

impl Default for QuotaStatusRecord {
    fn default() -> Self {
        Self {
            version: QUOTA_STATUS_VERSION,
            statuses: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RateLimitSnapshot, RateLimitWindow};

    #[test]
    fn computes_headroom_from_worst_window() {
        let snapshot = RateLimitSnapshot {
            credits: None,
            limit_id: Some("codex".to_string()),
            limit_name: Some("Codex".to_string()),
            plan_type: None,
            primary: Some(RateLimitWindow {
                resets_at: Some(100),
                used_percent: 20,
                window_duration_mins: Some(300),
            }),
            secondary: Some(RateLimitWindow {
                resets_at: Some(200),
                used_percent: 88,
                window_duration_mins: Some(10_080),
            }),
        };

        assert_eq!(snapshot.max_used_percent(), Some(88));
        assert_eq!(snapshot.remaining_headroom_percent(), Some(12));
    }
}
