use crate::domain::identity::{current_timestamp, IdentityId};
use crate::domain::thread::{
    HandoffConfirmation, HandoffRecord, ThreadSnapshot, TrackedTurnState, TrackedTurnStateStatus,
};
use crate::error::{AppError, Result};
use crate::storage::turn_state_store::TurnStateStore;

#[derive(Debug, Clone)]
pub struct TurnStateTracker<S> {
    store: S,
}

impl<S> TurnStateTracker<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S> TurnStateTracker<S>
where
    S: TurnStateStore,
{
    pub fn load(&self, thread_id: &str) -> Result<Option<TrackedTurnState>> {
        self.store.load(thread_id)
    }

    pub fn restore(&self, thread_id: &str, previous: Option<&TrackedTurnState>) -> Result<()> {
        match previous {
            Some(state) => self.store.save(state),
            None => self.store.delete(thread_id),
        }
    }

    pub fn record_snapshot(
        &self,
        thread_id: &str,
        owner_identity_id: Option<IdentityId>,
        snapshot: &ThreadSnapshot,
    ) -> Result<TrackedTurnState> {
        let state = TrackedTurnState {
            thread_id: thread_id.to_string(),
            owner_identity_id,
            state: TrackedTurnStateStatus::Active,
            last_thread_updated_at: Some(snapshot.updated_at),
            turn_count: snapshot.turn_count(),
            latest_turn_id: snapshot.latest_turn_id.clone(),
            latest_turn_status: snapshot.latest_turn_status.clone(),
            handoff: None,
            updated_at: current_timestamp()?,
        };
        self.store.save(&state)?;
        Ok(state)
    }

    pub fn prepare_handoff(
        &self,
        thread_id: &str,
        from_identity_id: IdentityId,
        to_identity_id: IdentityId,
        reason: &str,
        snapshot: &ThreadSnapshot,
    ) -> Result<TrackedTurnState> {
        let timestamp = current_timestamp()?;
        let state = TrackedTurnState {
            thread_id: thread_id.to_string(),
            owner_identity_id: Some(from_identity_id.clone()),
            state: TrackedTurnStateStatus::HandoffPrepared,
            last_thread_updated_at: Some(snapshot.updated_at),
            turn_count: snapshot.turn_count(),
            latest_turn_id: snapshot.latest_turn_id.clone(),
            latest_turn_status: snapshot.latest_turn_status.clone(),
            handoff: Some(HandoffRecord {
                from_identity_id,
                to_identity_id,
                reason: reason.to_string(),
                baseline_turn_count: snapshot.turn_count(),
                baseline_latest_turn_id: snapshot.latest_turn_id.clone(),
                baseline_thread_updated_at: snapshot.updated_at,
                observed_notification_turn_id: None,
                confirmed_turn_id: None,
                prepared_at: timestamp,
                accepted_at: None,
                confirmed_at: None,
            }),
            updated_at: timestamp,
        };
        self.store.save(&state)?;
        Ok(state)
    }

    pub fn mark_handoff_accepted(
        &self,
        thread_id: &str,
        owner_identity_id: IdentityId,
    ) -> Result<TrackedTurnState> {
        let mut state =
            self.store
                .load(thread_id)?
                .ok_or_else(|| AppError::ThreadStateNotFound {
                    thread_id: thread_id.to_string(),
                })?;
        if state.state != TrackedTurnStateStatus::HandoffPrepared {
            return Err(AppError::TrackedTurnStateConflict {
                thread_id: thread_id.to_string(),
                expected: TrackedTurnStateStatus::HandoffPrepared,
                actual: state.state,
            });
        }
        if state
            .handoff
            .as_ref()
            .is_some_and(|handoff| handoff.to_identity_id != owner_identity_id)
        {
            let expected_identity_id = state
                .handoff
                .as_ref()
                .expect("checked handoff")
                .to_identity_id
                .clone();
            return Err(AppError::HandoffTargetMismatch {
                thread_id: thread_id.to_string(),
                expected_identity_id,
                actual_identity_id: owner_identity_id,
            });
        }
        let timestamp = current_timestamp()?;
        state.owner_identity_id = Some(owner_identity_id);
        state.state = TrackedTurnStateStatus::HandoffAccepted;
        state.updated_at = timestamp;
        if let Some(handoff) = state.handoff.as_mut() {
            handoff.accepted_at = Some(timestamp);
        }
        self.store.save(&state)?;
        Ok(state)
    }

    pub fn confirm_handoff(
        &self,
        thread_id: &str,
        owner_identity_id: IdentityId,
        snapshot: &ThreadSnapshot,
        observed_turn_id: Option<&str>,
    ) -> Result<HandoffConfirmation> {
        let mut state =
            self.store
                .load(thread_id)?
                .ok_or_else(|| AppError::ThreadStateNotFound {
                    thread_id: thread_id.to_string(),
                })?;
        if state.state != TrackedTurnStateStatus::HandoffAccepted {
            return Err(AppError::TrackedTurnStateConflict {
                thread_id: thread_id.to_string(),
                expected: TrackedTurnStateStatus::HandoffAccepted,
                actual: state.state,
            });
        }
        let handoff = state
            .handoff
            .as_mut()
            .ok_or_else(|| AppError::HandoffNotPrepared {
                thread_id: thread_id.to_string(),
            })?;
        if handoff.to_identity_id != owner_identity_id {
            return Err(AppError::HandoffTargetMismatch {
                thread_id: thread_id.to_string(),
                expected_identity_id: handoff.to_identity_id.clone(),
                actual_identity_id: owner_identity_id,
            });
        }

        let matched_turn_id = if let Some(observed_turn_id) = observed_turn_id {
            if snapshot.contains_turn(observed_turn_id) {
                Some(observed_turn_id.to_string())
            } else {
                None
            }
        } else {
            None
        };

        let history_advanced = snapshot.turn_count() > handoff.baseline_turn_count
            || snapshot.latest_turn_id != handoff.baseline_latest_turn_id
            || snapshot.updated_at > handoff.baseline_thread_updated_at;
        if !history_advanced {
            return Err(AppError::HandoffHistoryUnchanged {
                thread_id: thread_id.to_string(),
                baseline_turn_count: handoff.baseline_turn_count,
                current_turn_count: snapshot.turn_count(),
            });
        }

        let timestamp = current_timestamp()?;
        handoff.observed_notification_turn_id =
            observed_turn_id.map(std::string::ToString::to_string);
        handoff.confirmed_turn_id = matched_turn_id
            .clone()
            .or_else(|| snapshot.latest_turn_id.clone());
        handoff.confirmed_at = Some(timestamp);

        state.owner_identity_id = Some(owner_identity_id);
        state.state = TrackedTurnStateStatus::HandoffConfirmed;
        state.last_thread_updated_at = Some(snapshot.updated_at);
        state.turn_count = snapshot.turn_count();
        state.latest_turn_id = snapshot.latest_turn_id.clone();
        state.latest_turn_status = snapshot.latest_turn_status.clone();
        state.updated_at = timestamp;
        self.store.save(&state)?;

        Ok(HandoffConfirmation {
            snapshot: snapshot.clone(),
            matched_turn_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::TurnStateTracker;
    use crate::domain::identity::IdentityId;
    use crate::domain::thread::{ThreadSnapshot, TrackedTurnStateStatus, TurnStatus};
    use crate::error::AppError;
    use crate::storage::turn_state_store::JsonTurnStateStore;

    #[test]
    fn confirms_handoff_only_after_history_advances() {
        let temp = tempdir().unwrap();
        let tracker = TurnStateTracker::new(JsonTurnStateStore::new(temp.path()));
        let source = IdentityId::from_display_name("Source").unwrap();
        let target = IdentityId::from_display_name("Target").unwrap();

        tracker
            .prepare_handoff(
                "thread-1",
                source,
                target.clone(),
                "quota",
                &ThreadSnapshot {
                    thread_id: "thread-1".to_string(),
                    created_at: 1,
                    updated_at: 2,
                    status: "idle".to_string(),
                    path: None,
                    turn_ids: vec!["turn-a".to_string()],
                    latest_turn_id: Some("turn-a".to_string()),
                    latest_turn_status: Some(TurnStatus::Completed),
                },
            )
            .unwrap();

        let error = tracker
            .confirm_handoff(
                "thread-1",
                target.clone(),
                &ThreadSnapshot {
                    thread_id: "thread-1".to_string(),
                    created_at: 1,
                    updated_at: 2,
                    status: "idle".to_string(),
                    path: None,
                    turn_ids: vec!["turn-a".to_string()],
                    latest_turn_id: Some("turn-a".to_string()),
                    latest_turn_status: Some(TurnStatus::Completed),
                },
                None,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::TrackedTurnStateConflict {
                expected: TrackedTurnStateStatus::HandoffAccepted,
                actual: TrackedTurnStateStatus::HandoffPrepared,
                ..
            }
        ));

        tracker
            .mark_handoff_accepted("thread-1", target.clone())
            .unwrap();

        let confirmation = tracker
            .confirm_handoff(
                "thread-1",
                target,
                &ThreadSnapshot {
                    thread_id: "thread-1".to_string(),
                    created_at: 1,
                    updated_at: 3,
                    status: "idle".to_string(),
                    path: None,
                    turn_ids: vec!["turn-a".to_string(), "turn-b".to_string()],
                    latest_turn_id: Some("turn-b".to_string()),
                    latest_turn_status: Some(TurnStatus::Completed),
                },
                Some("turn-b"),
            )
            .unwrap();
        assert_eq!(confirmation.matched_turn_id.as_deref(), Some("turn-b"));

        let state = tracker.load("thread-1").unwrap().unwrap();
        assert_eq!(state.state, TrackedTurnStateStatus::HandoffConfirmed);
        assert_eq!(state.latest_turn_id.as_deref(), Some("turn-b"));
    }

    #[test]
    fn rejects_confirmation_before_handoff_acceptance() {
        let temp = tempdir().unwrap();
        let tracker = TurnStateTracker::new(JsonTurnStateStore::new(temp.path()));
        let source = IdentityId::from_display_name("Source").unwrap();
        let target = IdentityId::from_display_name("Target").unwrap();

        tracker
            .prepare_handoff(
                "thread-1",
                source,
                target.clone(),
                "quota",
                &ThreadSnapshot {
                    thread_id: "thread-1".to_string(),
                    created_at: 1,
                    updated_at: 2,
                    status: "idle".to_string(),
                    path: None,
                    turn_ids: vec!["turn-a".to_string()],
                    latest_turn_id: Some("turn-a".to_string()),
                    latest_turn_status: Some(TurnStatus::Completed),
                },
            )
            .unwrap();

        let error = tracker
            .confirm_handoff(
                "thread-1",
                target,
                &ThreadSnapshot {
                    thread_id: "thread-1".to_string(),
                    created_at: 1,
                    updated_at: 3,
                    status: "idle".to_string(),
                    path: None,
                    turn_ids: vec!["turn-a".to_string(), "turn-b".to_string()],
                    latest_turn_id: Some("turn-b".to_string()),
                    latest_turn_status: Some(TurnStatus::Completed),
                },
                Some("turn-b"),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            AppError::TrackedTurnStateConflict {
                expected: TrackedTurnStateStatus::HandoffAccepted,
                actual: TrackedTurnStateStatus::HandoffPrepared,
                ..
            }
        ));
    }

    #[test]
    fn rejects_confirmation_when_observed_turn_id_is_not_new_history() {
        let temp = tempdir().unwrap();
        let tracker = TurnStateTracker::new(JsonTurnStateStore::new(temp.path()));
        let source = IdentityId::from_display_name("Source").unwrap();
        let target = IdentityId::from_display_name("Target").unwrap();

        tracker
            .prepare_handoff(
                "thread-1",
                source,
                target.clone(),
                "quota",
                &ThreadSnapshot {
                    thread_id: "thread-1".to_string(),
                    created_at: 1,
                    updated_at: 2,
                    status: "idle".to_string(),
                    path: None,
                    turn_ids: vec!["turn-a".to_string()],
                    latest_turn_id: Some("turn-a".to_string()),
                    latest_turn_status: Some(TurnStatus::Completed),
                },
            )
            .unwrap();
        tracker
            .mark_handoff_accepted("thread-1", target.clone())
            .unwrap();

        let error = tracker
            .confirm_handoff(
                "thread-1",
                target,
                &ThreadSnapshot {
                    thread_id: "thread-1".to_string(),
                    created_at: 1,
                    updated_at: 2,
                    status: "idle".to_string(),
                    path: None,
                    turn_ids: vec!["turn-a".to_string()],
                    latest_turn_id: Some("turn-a".to_string()),
                    latest_turn_status: Some(TurnStatus::Completed),
                },
                Some("turn-a"),
            )
            .unwrap_err();

        assert!(matches!(error, AppError::HandoffHistoryUnchanged { .. }));
    }
}
