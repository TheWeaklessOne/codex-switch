use std::time::Duration;

use crate::domain::identity::{current_timestamp, IdentityId};
use crate::domain::thread::{new_lease_token, ThreadLeaseRecord, ThreadLeaseState};
use crate::error::{AppError, Result};
use crate::storage::thread_lease_store::{JsonThreadLeaseStore, ThreadLeaseStore};

const STALE_LEASE_TIMEOUT: i64 = 15 * 60;

#[derive(Debug, Clone)]
pub struct ThreadLeaseManager<S> {
    store: S,
}

impl<S> ThreadLeaseManager<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

impl ThreadLeaseManager<JsonThreadLeaseStore> {
    pub fn with_default_locking(base_root: &std::path::Path) -> Self {
        Self::new(JsonThreadLeaseStore::new(
            base_root,
            Duration::from_secs(5),
            Duration::from_millis(50),
        ))
    }
}

impl<S> ThreadLeaseManager<S>
where
    S: ThreadLeaseStore,
{
    pub fn read(&self, thread_id: &str) -> Result<Option<ThreadLeaseRecord>> {
        self.store.load(thread_id)
    }

    pub fn acquire(
        &self,
        thread_id: &str,
        owner_identity_id: &IdentityId,
    ) -> Result<ThreadLeaseRecord> {
        self.store.with_locked_lease(thread_id, |current| {
            if let Some(existing) = current.as_ref() {
                if !lease_is_stale(existing)? {
                    match existing.lease_state {
                        ThreadLeaseState::Active => {
                            return Err(AppError::ThreadLeaseHeld {
                                thread_id: thread_id.to_string(),
                                owner_identity_id: existing.owner_identity_id.clone(),
                            });
                        }
                        ThreadLeaseState::HandoffPending => {
                            return Err(AppError::ThreadLeaseStateConflict {
                                thread_id: thread_id.to_string(),
                                expected: ThreadLeaseState::Active,
                                actual: existing.lease_state.clone(),
                            });
                        }
                        ThreadLeaseState::Released => {}
                    }
                }
            }

            let timestamp = current_timestamp()?;
            let lease = ThreadLeaseRecord {
                thread_id: thread_id.to_string(),
                owner_identity_id: owner_identity_id.clone(),
                lease_state: ThreadLeaseState::Active,
                lease_token: new_lease_token(),
                handoff_to_identity_id: None,
                handoff_reason: None,
                last_heartbeat_at: timestamp,
                updated_at: timestamp,
            };
            self.store.save(&lease)?;
            Ok(lease)
        })
    }

    pub fn restore(
        &self,
        thread_id: &str,
        expected_current_token: Option<&str>,
        record: &ThreadLeaseRecord,
    ) -> Result<ThreadLeaseRecord> {
        self.store.with_locked_lease(thread_id, |current| {
            match (expected_current_token, current.as_ref()) {
                (Some(_), None) => {
                    return Err(AppError::ThreadLeaseNotFound {
                        thread_id: thread_id.to_string(),
                    });
                }
                (Some(expected_token), Some(current)) if current.lease_token != expected_token => {
                    return Err(AppError::ThreadLeaseTokenMismatch {
                        thread_id: thread_id.to_string(),
                    });
                }
                _ => {}
            }

            self.store.save(record)?;
            Ok(record.clone())
        })
    }

    pub fn heartbeat(
        &self,
        thread_id: &str,
        owner_identity_id: &IdentityId,
        lease_token: &str,
    ) -> Result<ThreadLeaseRecord> {
        self.store.with_locked_lease(thread_id, |current| {
            let mut lease =
                require_active_lease(current, thread_id, owner_identity_id, lease_token)?;
            let timestamp = current_timestamp()?;
            lease.last_heartbeat_at = timestamp;
            lease.updated_at = timestamp;
            self.store.save(&lease)?;
            Ok(lease)
        })
    }

    pub fn begin_handoff(
        &self,
        thread_id: &str,
        from_identity_id: &IdentityId,
        lease_token: &str,
        to_identity_id: &IdentityId,
        reason: &str,
    ) -> Result<ThreadLeaseRecord> {
        self.store.with_locked_lease(thread_id, |current| {
            let mut lease =
                require_active_lease(current, thread_id, from_identity_id, lease_token)?;
            let timestamp = current_timestamp()?;
            lease.lease_state = ThreadLeaseState::HandoffPending;
            lease.lease_token = new_lease_token();
            lease.handoff_to_identity_id = Some(to_identity_id.clone());
            lease.handoff_reason = Some(reason.to_string());
            lease.last_heartbeat_at = timestamp;
            lease.updated_at = timestamp;
            self.store.save(&lease)?;
            Ok(lease)
        })
    }

    pub fn accept_handoff(
        &self,
        thread_id: &str,
        to_identity_id: &IdentityId,
        pending_token: &str,
    ) -> Result<ThreadLeaseRecord> {
        self.store.with_locked_lease(thread_id, |current| {
            let current = current.ok_or_else(|| AppError::ThreadLeaseNotFound {
                thread_id: thread_id.to_string(),
            })?;
            if current.lease_state != ThreadLeaseState::HandoffPending {
                return Err(AppError::ThreadLeaseStateConflict {
                    thread_id: thread_id.to_string(),
                    expected: ThreadLeaseState::HandoffPending,
                    actual: current.lease_state,
                });
            }
            if current.lease_token != pending_token {
                return Err(AppError::ThreadLeaseTokenMismatch {
                    thread_id: thread_id.to_string(),
                });
            }
            let expected_target = current.handoff_to_identity_id.clone().ok_or_else(|| {
                AppError::HandoffTargetMissing {
                    thread_id: thread_id.to_string(),
                }
            })?;
            if expected_target != *to_identity_id {
                return Err(AppError::HandoffTargetMismatch {
                    thread_id: thread_id.to_string(),
                    expected_identity_id: expected_target,
                    actual_identity_id: to_identity_id.clone(),
                });
            }

            let timestamp = current_timestamp()?;
            let lease = ThreadLeaseRecord {
                thread_id: thread_id.to_string(),
                owner_identity_id: to_identity_id.clone(),
                lease_state: ThreadLeaseState::Active,
                lease_token: new_lease_token(),
                handoff_to_identity_id: None,
                handoff_reason: None,
                last_heartbeat_at: timestamp,
                updated_at: timestamp,
            };
            self.store.save(&lease)?;
            Ok(lease)
        })
    }

    pub fn release(
        &self,
        thread_id: &str,
        owner_identity_id: &IdentityId,
        lease_token: &str,
    ) -> Result<ThreadLeaseRecord> {
        self.store.with_locked_lease(thread_id, |current| {
            let mut lease =
                require_active_lease(current, thread_id, owner_identity_id, lease_token)?;
            let timestamp = current_timestamp()?;
            lease.lease_state = ThreadLeaseState::Released;
            lease.lease_token = new_lease_token();
            lease.handoff_to_identity_id = None;
            lease.handoff_reason = None;
            lease.updated_at = timestamp;
            self.store.save(&lease)?;
            Ok(lease)
        })
    }
}

fn lease_is_stale(record: &ThreadLeaseRecord) -> Result<bool> {
    let now = current_timestamp()?;
    Ok(now.saturating_sub(record.last_heartbeat_at) >= STALE_LEASE_TIMEOUT)
}

fn require_active_lease(
    current: Option<ThreadLeaseRecord>,
    thread_id: &str,
    owner_identity_id: &IdentityId,
    lease_token: &str,
) -> Result<ThreadLeaseRecord> {
    let lease = current.ok_or_else(|| AppError::ThreadLeaseNotFound {
        thread_id: thread_id.to_string(),
    })?;
    if lease.lease_state != ThreadLeaseState::Active {
        return Err(AppError::ThreadLeaseStateConflict {
            thread_id: thread_id.to_string(),
            expected: ThreadLeaseState::Active,
            actual: lease.lease_state,
        });
    }
    if lease.owner_identity_id != *owner_identity_id {
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
    Ok(lease)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{ThreadLeaseManager, STALE_LEASE_TIMEOUT};
    use crate::domain::identity::IdentityId;
    use crate::domain::thread::ThreadLeaseState;
    use crate::storage::thread_lease_store::{JsonThreadLeaseStore, ThreadLeaseStore};

    #[test]
    fn only_one_owner_can_hold_active_lease() {
        let temp = tempdir().unwrap();
        let manager = ThreadLeaseManager::new(JsonThreadLeaseStore::test_store(temp.path()));
        let source = IdentityId::from_display_name("Source").unwrap();
        let target = IdentityId::from_display_name("Target").unwrap();

        let lease = manager.acquire("thread-1", &source).unwrap();
        assert_eq!(lease.lease_state, ThreadLeaseState::Active);

        let error = manager.acquire("thread-1", &target).unwrap_err();
        assert_eq!(
            error.to_string(),
            "thread thread-1 is already leased to source"
        );
    }

    #[test]
    fn same_identity_cannot_reacquire_live_lease() {
        let temp = tempdir().unwrap();
        let manager = ThreadLeaseManager::new(JsonThreadLeaseStore::test_store(temp.path()));
        let source = IdentityId::from_display_name("Source").unwrap();

        manager.acquire("thread-1", &source).unwrap();
        let error = manager.acquire("thread-1", &source).unwrap_err();
        assert_eq!(
            error.to_string(),
            "thread thread-1 is already leased to source"
        );
    }

    #[test]
    fn handoff_rotates_tokens_and_transfers_owner() {
        let temp = tempdir().unwrap();
        let manager = ThreadLeaseManager::new(JsonThreadLeaseStore::test_store(temp.path()));
        let source = IdentityId::from_display_name("Source").unwrap();
        let target = IdentityId::from_display_name("Target").unwrap();

        let active = manager.acquire("thread-1", &source).unwrap();
        let pending = manager
            .begin_handoff("thread-1", &source, &active.lease_token, &target, "quota")
            .unwrap();
        assert_eq!(pending.lease_state, ThreadLeaseState::HandoffPending);
        assert_ne!(pending.lease_token, active.lease_token);

        let transferred = manager
            .accept_handoff("thread-1", &target, &pending.lease_token)
            .unwrap();
        assert_eq!(transferred.lease_state, ThreadLeaseState::Active);
        assert_eq!(transferred.owner_identity_id, target);
        assert_ne!(transferred.lease_token, pending.lease_token);
    }

    #[test]
    fn stale_active_lease_can_be_reclaimed() {
        let temp = tempdir().unwrap();
        let store = JsonThreadLeaseStore::test_store(temp.path());
        let manager = ThreadLeaseManager::new(store.clone());
        let source = IdentityId::from_display_name("Source").unwrap();
        let target = IdentityId::from_display_name("Target").unwrap();
        let mut lease = manager.acquire("thread-1", &source).unwrap();
        lease.last_heartbeat_at -= STALE_LEASE_TIMEOUT + 1;
        store.save(&lease).unwrap();

        let reclaimed = manager.acquire("thread-1", &target).unwrap();
        assert_eq!(reclaimed.owner_identity_id, target);
        assert_eq!(reclaimed.lease_state, ThreadLeaseState::Active);
    }
}
