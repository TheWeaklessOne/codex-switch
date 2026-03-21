use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::domain::identity::IdentityId;

pub const IDENTITY_HEALTH_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityFailureKind {
    RateLimit,
    Auth,
}

impl fmt::Display for IdentityFailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::RateLimit => "rate_limit",
            Self::Auth => "auth",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityHealthState {
    pub identity_id: IdentityId,
    pub penalty_until: Option<i64>,
    pub last_failure_kind: Option<IdentityFailureKind>,
    pub last_failure_at: Option<i64>,
    pub last_failure_message: Option<String>,
    pub manually_disabled: bool,
    pub updated_at: i64,
}

impl IdentityHealthState {
    pub fn new(identity_id: IdentityId, updated_at: i64) -> Self {
        Self {
            identity_id,
            penalty_until: None,
            last_failure_kind: None,
            last_failure_at: None,
            last_failure_message: None,
            manually_disabled: false,
            updated_at,
        }
    }

    pub fn penalty_active_at(&self, now: i64) -> bool {
        self.penalty_until.is_some_and(|deadline| deadline > now)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityHealthRecord {
    pub version: u32,
    pub identities: BTreeMap<IdentityId, IdentityHealthState>,
}

impl Default for IdentityHealthRecord {
    fn default() -> Self {
        Self {
            version: IDENTITY_HEALTH_VERSION,
            identities: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IdentityFailureKind, IdentityHealthRecord, IdentityHealthState};
    use crate::domain::identity::IdentityId;

    #[test]
    fn defaults_to_empty_record() {
        let record = IdentityHealthRecord::default();
        assert_eq!(record.version, 1);
        assert!(record.identities.is_empty());
    }

    #[test]
    fn penalty_state_is_time_bounded() {
        let mut state =
            IdentityHealthState::new(IdentityId::from_display_name("Primary").unwrap(), 100);
        state.penalty_until = Some(200);
        state.last_failure_kind = Some(IdentityFailureKind::RateLimit);

        assert!(state.penalty_active_at(150));
        assert!(!state.penalty_active_at(200));
    }
}
