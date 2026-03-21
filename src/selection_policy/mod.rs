use crate::domain::policy::{IdentitySelectionPolicy, SelectionPolicyRecord};
use crate::error::Result;
use crate::storage::policy_store::SelectionPolicyStore;

#[derive(Debug, Clone, Default)]
pub struct UpdateSelectionPolicyRequest {
    pub warning_used_percent: Option<i32>,
    pub avoid_used_percent: Option<i32>,
    pub hard_stop_used_percent: Option<i32>,
    pub rate_limit_cooldown_secs: Option<i64>,
    pub auth_failure_cooldown_secs: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SelectionPolicyService<S> {
    store: S,
}

impl<S> SelectionPolicyService<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S> SelectionPolicyService<S>
where
    S: SelectionPolicyStore,
{
    pub fn load_policy(&self) -> Result<IdentitySelectionPolicy> {
        Ok(self.store.load()?.policy)
    }

    pub fn update_policy(
        &self,
        request: UpdateSelectionPolicyRequest,
    ) -> Result<IdentitySelectionPolicy> {
        let mut record = self.store.load()?;
        if let Some(value) = request.warning_used_percent {
            record.policy.warning_used_percent = value;
        }
        if let Some(value) = request.avoid_used_percent {
            record.policy.avoid_used_percent = value;
        }
        if let Some(value) = request.hard_stop_used_percent {
            record.policy.hard_stop_used_percent = value;
        }
        if let Some(value) = request.rate_limit_cooldown_secs {
            record.policy.rate_limit_cooldown_secs = value;
        }
        if let Some(value) = request.auth_failure_cooldown_secs {
            record.policy.auth_failure_cooldown_secs = value;
        }
        record.policy.validate()?;
        self.store.save(&record)?;
        Ok(record.policy)
    }

    pub fn reset_defaults(&self) -> Result<IdentitySelectionPolicy> {
        let record = SelectionPolicyRecord::default();
        self.store.save(&record)?;
        Ok(record.policy)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{SelectionPolicyService, UpdateSelectionPolicyRequest};
    use crate::storage::policy_store::JsonSelectionPolicyStore;

    #[test]
    fn updates_persisted_policy() {
        let temp = tempdir().unwrap();
        let service = SelectionPolicyService::new(JsonSelectionPolicyStore::new(temp.path()));

        let policy = service
            .update_policy(UpdateSelectionPolicyRequest {
                warning_used_percent: Some(50),
                avoid_used_percent: Some(70),
                hard_stop_used_percent: Some(90),
                rate_limit_cooldown_secs: Some(60),
                auth_failure_cooldown_secs: Some(120),
            })
            .unwrap();

        assert_eq!(policy.warning_used_percent, 50);
        assert_eq!(service.load_policy().unwrap().hard_stop_used_percent, 90);
    }
}
