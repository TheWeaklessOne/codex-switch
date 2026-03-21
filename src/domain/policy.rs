use serde::{Deserialize, Serialize};

use crate::error::{AppError, Result};

pub const SELECTION_POLICY_VERSION: u32 = 1;
pub const DEFAULT_WARNING_USED_PERCENT: i32 = 85;
pub const DEFAULT_AVOID_USED_PERCENT: i32 = 95;
pub const DEFAULT_HARD_STOP_USED_PERCENT: i32 = 100;
pub const DEFAULT_RATE_LIMIT_COOLDOWN_SECS: i64 = 1_800;
pub const DEFAULT_AUTH_FAILURE_COOLDOWN_SECS: i64 = 21_600;
pub const MAX_USED_PERCENT: i32 = 100;
pub const MAX_FAILURE_COOLDOWN_SECS: i64 = 2_592_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentitySelectionPolicy {
    pub warning_used_percent: i32,
    pub avoid_used_percent: i32,
    pub hard_stop_used_percent: i32,
    pub rate_limit_cooldown_secs: i64,
    pub auth_failure_cooldown_secs: i64,
}

impl Default for IdentitySelectionPolicy {
    fn default() -> Self {
        Self {
            warning_used_percent: DEFAULT_WARNING_USED_PERCENT,
            avoid_used_percent: DEFAULT_AVOID_USED_PERCENT,
            hard_stop_used_percent: DEFAULT_HARD_STOP_USED_PERCENT,
            rate_limit_cooldown_secs: DEFAULT_RATE_LIMIT_COOLDOWN_SECS,
            auth_failure_cooldown_secs: DEFAULT_AUTH_FAILURE_COOLDOWN_SECS,
        }
    }
}

impl IdentitySelectionPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.warning_used_percent < 0 || self.warning_used_percent > MAX_USED_PERCENT {
            return Err(AppError::InvalidSelectionPolicy {
                message: format!("warning threshold must be between 0 and {MAX_USED_PERCENT}"),
            });
        }

        if self.avoid_used_percent > MAX_USED_PERCENT {
            return Err(AppError::InvalidSelectionPolicy {
                message: format!("avoid threshold must be <= {MAX_USED_PERCENT}"),
            });
        }

        if self.avoid_used_percent < self.warning_used_percent {
            return Err(AppError::InvalidSelectionPolicy {
                message: "avoid threshold must be >= warning threshold".to_string(),
            });
        }

        if self.hard_stop_used_percent > MAX_USED_PERCENT {
            return Err(AppError::InvalidSelectionPolicy {
                message: format!("hard-stop threshold must be <= {MAX_USED_PERCENT}"),
            });
        }

        if self.hard_stop_used_percent < self.avoid_used_percent {
            return Err(AppError::InvalidSelectionPolicy {
                message: "hard-stop threshold must be >= avoid threshold".to_string(),
            });
        }

        if self.rate_limit_cooldown_secs < 0
            || self.rate_limit_cooldown_secs > MAX_FAILURE_COOLDOWN_SECS
        {
            return Err(AppError::InvalidSelectionPolicy {
                message: format!(
                    "rate-limit cooldown must be between 0 and {MAX_FAILURE_COOLDOWN_SECS} seconds"
                ),
            });
        }

        if self.auth_failure_cooldown_secs < 0
            || self.auth_failure_cooldown_secs > MAX_FAILURE_COOLDOWN_SECS
        {
            return Err(AppError::InvalidSelectionPolicy {
                message: format!(
                    "auth-failure cooldown must be between 0 and {MAX_FAILURE_COOLDOWN_SECS} seconds"
                ),
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectionPolicyRecord {
    pub version: u32,
    pub policy: IdentitySelectionPolicy,
}

impl Default for SelectionPolicyRecord {
    fn default() -> Self {
        Self {
            version: SELECTION_POLICY_VERSION,
            policy: IdentitySelectionPolicy::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IdentitySelectionPolicy, SelectionPolicyRecord, DEFAULT_AUTH_FAILURE_COOLDOWN_SECS,
        DEFAULT_AVOID_USED_PERCENT, DEFAULT_HARD_STOP_USED_PERCENT,
        DEFAULT_RATE_LIMIT_COOLDOWN_SECS, DEFAULT_WARNING_USED_PERCENT, MAX_FAILURE_COOLDOWN_SECS,
        MAX_USED_PERCENT, SELECTION_POLICY_VERSION,
    };

    #[test]
    fn defaults_match_expected_thresholds_and_cooldowns() {
        let record = SelectionPolicyRecord::default();
        assert_eq!(record.version, SELECTION_POLICY_VERSION);
        assert_eq!(
            record.policy.warning_used_percent,
            DEFAULT_WARNING_USED_PERCENT
        );
        assert_eq!(record.policy.avoid_used_percent, DEFAULT_AVOID_USED_PERCENT);
        assert_eq!(
            record.policy.hard_stop_used_percent,
            DEFAULT_HARD_STOP_USED_PERCENT
        );
        assert_eq!(
            record.policy.rate_limit_cooldown_secs,
            DEFAULT_RATE_LIMIT_COOLDOWN_SECS
        );
        assert_eq!(
            record.policy.auth_failure_cooldown_secs,
            DEFAULT_AUTH_FAILURE_COOLDOWN_SECS
        );
    }

    #[test]
    fn rejects_invalid_threshold_order() {
        let policy = IdentitySelectionPolicy {
            warning_used_percent: 90,
            avoid_used_percent: 80,
            hard_stop_used_percent: 100,
            rate_limit_cooldown_secs: 1,
            auth_failure_cooldown_secs: 1,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn rejects_negative_cooldowns() {
        let policy = IdentitySelectionPolicy {
            warning_used_percent: 10,
            avoid_used_percent: 20,
            hard_stop_used_percent: 30,
            rate_limit_cooldown_secs: -1,
            auth_failure_cooldown_secs: 1,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn rejects_thresholds_above_max_percent() {
        let policy = IdentitySelectionPolicy {
            warning_used_percent: MAX_USED_PERCENT + 1,
            avoid_used_percent: MAX_USED_PERCENT + 1,
            hard_stop_used_percent: MAX_USED_PERCENT + 1,
            rate_limit_cooldown_secs: 1,
            auth_failure_cooldown_secs: 1,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn rejects_excessive_cooldowns() {
        let policy = IdentitySelectionPolicy {
            warning_used_percent: 10,
            avoid_used_percent: 20,
            hard_stop_used_percent: 30,
            rate_limit_cooldown_secs: MAX_FAILURE_COOLDOWN_SECS + 1,
            auth_failure_cooldown_secs: 1,
        };
        assert!(policy.validate().is_err());
    }
}
