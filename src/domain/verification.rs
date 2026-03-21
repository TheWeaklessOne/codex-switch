use std::collections::BTreeMap;

use crate::domain::identity::{AccountType, PlanType};
use crate::domain::quota::IdentityQuotaStatus;
pub use crate::domain::quota::{CreditsSnapshot, RateLimitSnapshot, RateLimitWindow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityVerification {
    pub authenticated: bool,
    pub auth_method: Option<String>,
    pub account_type: Option<AccountType>,
    pub email: Option<String>,
    pub plan_type: Option<PlanType>,
    pub requires_openai_auth: bool,
    pub fallback_rate_limit: Option<RateLimitSnapshot>,
    pub rate_limits_by_limit_id: BTreeMap<String, RateLimitSnapshot>,
}

impl IdentityVerification {
    pub fn apply_to_identity(
        &self,
        identity: &mut crate::domain::identity::CodexIdentity,
        verified_at: i64,
    ) {
        identity.authenticated = Some(self.authenticated);
        identity.last_auth_method = self.auth_method.clone();
        identity.account_type = self.account_type;
        identity.email = self.email.clone();
        identity.plan_type = self.plan_type;
        identity.last_verified_at = Some(verified_at);
    }

    pub fn to_quota_status(
        &self,
        identity_id: crate::domain::identity::IdentityId,
        updated_at: i64,
    ) -> IdentityQuotaStatus {
        IdentityQuotaStatus {
            identity_id,
            default_rate_limit: self.fallback_rate_limit.clone(),
            rate_limits_by_limit_id: self.rate_limits_by_limit_id.clone(),
            updated_at,
        }
    }
}
