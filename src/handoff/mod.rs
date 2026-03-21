use std::path::Path;

use crate::codex_rpc::ThreadRuntime;
use crate::domain::identity::{CodexIdentity, IdentityId};
use crate::domain::thread::{
    HandoffConfirmation, ThreadLeaseRecord, ThreadSnapshot, TrackedTurnState,
};
use crate::error::{AppError, Result};
use crate::shared_session_store::SharedSessionStore;
use crate::storage::registry_store::RegistryStore;
use crate::storage::thread_lease_store::JsonThreadLeaseStore;
use crate::storage::turn_state_store::JsonTurnStateStore;
use crate::thread_leases::ThreadLeaseManager;
use crate::turn_state::TurnStateTracker;

#[derive(Debug, Clone)]
pub struct HandoffPreparation {
    pub lease: ThreadLeaseRecord,
    pub baseline_snapshot: ThreadSnapshot,
    pub target_snapshot: ThreadSnapshot,
    pub turn_state: TrackedTurnState,
}

#[derive(Debug, Clone)]
pub struct HandoffAcceptance {
    pub lease: ThreadLeaseRecord,
    pub turn_state: TrackedTurnState,
}

#[derive(Debug, Clone)]
pub struct HandoffService<S, R> {
    registry_store: S,
    shared_session_store: SharedSessionStore<R>,
    lease_manager: ThreadLeaseManager<JsonThreadLeaseStore>,
    turn_state_tracker: TurnStateTracker<JsonTurnStateStore>,
}

impl<S, R> HandoffService<S, R>
where
    S: RegistryStore,
    R: ThreadRuntime,
{
    pub fn new(base_root: &Path, registry_store: S, runtime: R) -> Self {
        Self {
            registry_store,
            shared_session_store: SharedSessionStore::new(runtime),
            lease_manager: ThreadLeaseManager::with_default_locking(base_root),
            turn_state_tracker: TurnStateTracker::new(JsonTurnStateStore::new(base_root)),
        }
    }

    pub fn inspect_thread(&self, identity_name: &str, thread_id: &str) -> Result<ThreadSnapshot> {
        let identity = self.identity_by_name(identity_name)?;
        self.shared_session_store.read_thread(&identity, thread_id)
    }

    pub fn read_lease(&self, thread_id: &str) -> Result<Option<ThreadLeaseRecord>> {
        self.lease_manager.read(thread_id)
    }

    pub fn acquire_lease(&self, identity_name: &str, thread_id: &str) -> Result<ThreadLeaseRecord> {
        let identity = self.identity_by_name(identity_name)?;
        self.lease_manager.acquire(thread_id, &identity.id)
    }

    pub fn heartbeat_lease(
        &self,
        identity_name: &str,
        thread_id: &str,
        lease_token: &str,
    ) -> Result<ThreadLeaseRecord> {
        let identity = self.identity_by_name(identity_name)?;
        self.lease_manager
            .heartbeat(thread_id, &identity.id, lease_token)
    }

    pub fn prepare_handoff(
        &self,
        thread_id: &str,
        from_identity_name: &str,
        to_identity_name: &str,
        lease_token: &str,
        reason: &str,
    ) -> Result<HandoffPreparation> {
        let from_identity = self.identity_by_name(from_identity_name)?;
        let to_identity = self.identity_by_name(to_identity_name)?;
        let baseline_snapshot = self
            .shared_session_store
            .read_thread(&from_identity, thread_id)?;
        let target_snapshot = self.shared_session_store.ensure_cross_identity_visibility(
            &from_identity,
            &to_identity,
            thread_id,
        )?;
        let previous_turn_state = self.turn_state_tracker.load(thread_id)?;
        let turn_state = self.turn_state_tracker.prepare_handoff(
            thread_id,
            from_identity.id.clone(),
            to_identity.id.clone(),
            reason,
            &baseline_snapshot,
        )?;
        let lease = match self.lease_manager.begin_handoff(
            thread_id,
            &from_identity.id,
            lease_token,
            &to_identity.id,
            reason,
        ) {
            Ok(lease) => lease,
            Err(error) => {
                return Err(self.rollback_turn_state_error(
                    "prepare_handoff",
                    error,
                    thread_id,
                    previous_turn_state.as_ref(),
                ));
            }
        };

        Ok(HandoffPreparation {
            lease,
            baseline_snapshot,
            target_snapshot,
            turn_state,
        })
    }

    pub fn accept_handoff(
        &self,
        thread_id: &str,
        to_identity_name: &str,
        pending_token: &str,
    ) -> Result<HandoffAcceptance> {
        let to_identity = self.identity_by_name(to_identity_name)?;
        let previous_turn_state = self.turn_state_tracker.load(thread_id)?;
        let turn_state = self
            .turn_state_tracker
            .mark_handoff_accepted(thread_id, to_identity.id.clone())?;
        let lease =
            match self
                .lease_manager
                .accept_handoff(thread_id, &to_identity.id, pending_token)
            {
                Ok(lease) => lease,
                Err(error) => {
                    return Err(self.rollback_turn_state_error(
                        "accept_handoff",
                        error,
                        thread_id,
                        previous_turn_state.as_ref(),
                    ));
                }
            };

        Ok(HandoffAcceptance { lease, turn_state })
    }

    pub fn confirm_handoff(
        &self,
        thread_id: &str,
        to_identity_name: &str,
        lease_token: &str,
        observed_turn_id: Option<&str>,
    ) -> Result<HandoffConfirmation> {
        let to_identity = self.identity_by_name(to_identity_name)?;
        let lease =
            self.lease_manager
                .read(thread_id)?
                .ok_or_else(|| AppError::ThreadLeaseNotFound {
                    thread_id: thread_id.to_string(),
                })?;
        if lease.lease_state != crate::domain::thread::ThreadLeaseState::Active {
            return Err(AppError::ThreadLeaseStateConflict {
                thread_id: thread_id.to_string(),
                expected: crate::domain::thread::ThreadLeaseState::Active,
                actual: lease.lease_state,
            });
        }
        if lease.owner_identity_id != to_identity.id {
            return Err(AppError::ThreadLeaseHeld {
                thread_id: thread_id.to_string(),
                owner_identity_id: lease.owner_identity_id,
            });
        }
        if lease.lease_token != lease_token {
            return Err(AppError::ThreadLeaseTokenMismatch {
                thread_id: thread_id.to_string(),
            });
        }
        let snapshot = self
            .shared_session_store
            .read_thread(&to_identity, thread_id)?;
        self.turn_state_tracker.confirm_handoff(
            thread_id,
            to_identity.id,
            &snapshot,
            observed_turn_id,
        )
    }

    pub fn tracked_state(&self, thread_id: &str) -> Result<Option<TrackedTurnState>> {
        self.turn_state_tracker.load(thread_id)
    }

    pub fn restore_source_lease(
        &self,
        thread_id: &str,
        expected_current_token: &str,
        previous_lease: &ThreadLeaseRecord,
        baseline_snapshot: &ThreadSnapshot,
    ) -> Result<()> {
        self.lease_manager
            .restore(thread_id, Some(expected_current_token), previous_lease)?;
        self.turn_state_tracker.record_snapshot(
            thread_id,
            Some(previous_lease.owner_identity_id.clone()),
            baseline_snapshot,
        )?;
        Ok(())
    }

    fn identity_by_name(&self, identity_name: &str) -> Result<CodexIdentity> {
        let registry = self.registry_store.load()?;
        let identity_id = IdentityId::from_display_name(identity_name)?;
        registry
            .identities
            .get(&identity_id)
            .cloned()
            .ok_or(AppError::IdentityNotFound { identity_id })
    }

    fn rollback_turn_state_error(
        &self,
        operation: &str,
        primary: AppError,
        thread_id: &str,
        previous_turn_state: Option<&TrackedTurnState>,
    ) -> AppError {
        match self
            .turn_state_tracker
            .restore(thread_id, previous_turn_state)
        {
            Ok(()) => primary,
            Err(rollback) => AppError::RollbackFailed {
                operation: operation.to_string(),
                primary: primary.to_string(),
                rollback: rollback.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use super::HandoffService;
    use crate::bootstrap::BootstrapIdentityRequest;
    use crate::codex_rpc::ThreadRuntime;
    use crate::domain::identity::{AuthMode, CodexIdentity};
    use crate::domain::thread::{
        ThreadLeaseState, ThreadSnapshot, TrackedTurnStateStatus, TurnStatus,
    };
    use crate::identity_registry::IdentityRegistryService;
    use crate::storage::registry_store::JsonRegistryStore;

    #[derive(Debug, Clone)]
    struct StubRuntime {
        snapshots: Arc<Mutex<BTreeMap<(String, String), ThreadSnapshot>>>,
    }

    impl ThreadRuntime for StubRuntime {
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
    fn prepares_accepts_and_confirms_handoff() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
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

        let source_identity = registry_service
            .list_identities()
            .unwrap()
            .into_iter()
            .find(|identity| identity.display_name == "Source")
            .unwrap();
        let target_identity = registry_service
            .list_identities()
            .unwrap()
            .into_iter()
            .find(|identity| identity.display_name == "Target")
            .unwrap();

        let baseline = ThreadSnapshot {
            thread_id: "thread-1".to_string(),
            created_at: 1,
            updated_at: 2,
            status: "idle".to_string(),
            path: None,
            turn_ids: vec!["turn-a".to_string()],
            latest_turn_id: Some("turn-a".to_string()),
            latest_turn_status: Some(TurnStatus::Completed),
        };
        let advanced = ThreadSnapshot {
            thread_id: "thread-1".to_string(),
            created_at: 1,
            updated_at: 3,
            status: "idle".to_string(),
            path: None,
            turn_ids: vec!["turn-a".to_string(), "turn-b".to_string()],
            latest_turn_id: Some("turn-b".to_string()),
            latest_turn_status: Some(TurnStatus::Completed),
        };
        let mut snapshots = BTreeMap::new();
        snapshots.insert(
            (source_identity.id.to_string(), "thread-1".to_string()),
            baseline.clone(),
        );
        snapshots.insert(
            (target_identity.id.to_string(), "thread-1".to_string()),
            baseline.clone(),
        );
        let snapshots = Arc::new(Mutex::new(snapshots));
        let runtime = StubRuntime {
            snapshots: Arc::clone(&snapshots),
        };
        let service = HandoffService::new(temp.path(), registry_store.clone(), runtime);

        let active = service.acquire_lease("Source", "thread-1").unwrap();
        let pending = service
            .prepare_handoff("thread-1", "Source", "Target", &active.lease_token, "quota")
            .unwrap();
        assert_eq!(pending.lease.lease_state, ThreadLeaseState::HandoffPending);
        assert_eq!(
            pending.turn_state.state,
            TrackedTurnStateStatus::HandoffPrepared
        );

        let accepted = service
            .accept_handoff("thread-1", "Target", &pending.lease.lease_token)
            .unwrap();
        assert_eq!(accepted.lease.lease_state, ThreadLeaseState::Active);
        assert_eq!(
            accepted.turn_state.state,
            TrackedTurnStateStatus::HandoffAccepted
        );
        snapshots.lock().unwrap().insert(
            (target_identity.id.to_string(), "thread-1".to_string()),
            advanced.clone(),
        );

        let confirmation = service
            .confirm_handoff(
                "thread-1",
                "Target",
                &accepted.lease.lease_token,
                Some("turn-b"),
            )
            .unwrap();
        assert_eq!(
            confirmation.snapshot.latest_turn_id.as_deref(),
            Some("turn-b")
        );

        let tracked = service.tracked_state("thread-1").unwrap().unwrap();
        assert_eq!(tracked.state, TrackedTurnStateStatus::HandoffConfirmed);
    }
}
