use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::domain::identity::IdentityId;

static TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadLeaseState {
    Active,
    HandoffPending,
    Released,
}

impl fmt::Display for ThreadLeaseState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => formatter.write_str("active"),
            Self::HandoffPending => formatter.write_str("handoff_pending"),
            Self::Released => formatter.write_str("released"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadLeaseRecord {
    pub thread_id: String,
    pub owner_identity_id: IdentityId,
    pub lease_state: ThreadLeaseState,
    pub lease_token: String,
    pub handoff_to_identity_id: Option<IdentityId>,
    pub handoff_reason: Option<String>,
    pub last_heartbeat_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TurnStatus {
    Completed,
    Interrupted,
    Failed,
    InProgress,
}

impl fmt::Display for TurnStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed => formatter.write_str("completed"),
            Self::Interrupted => formatter.write_str("interrupted"),
            Self::Failed => formatter.write_str("failed"),
            Self::InProgress => formatter.write_str("in_progress"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackedTurnStateStatus {
    Active,
    HandoffPrepared,
    HandoffAccepted,
    HandoffConfirmed,
    Blocked,
}

impl fmt::Display for TrackedTurnStateStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => formatter.write_str("active"),
            Self::HandoffPrepared => formatter.write_str("handoff_prepared"),
            Self::HandoffAccepted => formatter.write_str("handoff_accepted"),
            Self::HandoffConfirmed => formatter.write_str("handoff_confirmed"),
            Self::Blocked => formatter.write_str("blocked"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadSnapshot {
    pub thread_id: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub status: String,
    pub path: Option<String>,
    pub turn_ids: Vec<String>,
    pub latest_turn_id: Option<String>,
    pub latest_turn_status: Option<TurnStatus>,
}

impl ThreadSnapshot {
    pub fn turn_count(&self) -> usize {
        self.turn_ids.len()
    }

    pub fn contains_turn(&self, turn_id: &str) -> bool {
        self.turn_ids.iter().any(|candidate| candidate == turn_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffRecord {
    pub from_identity_id: IdentityId,
    pub to_identity_id: IdentityId,
    pub reason: String,
    pub baseline_turn_count: usize,
    pub baseline_latest_turn_id: Option<String>,
    pub baseline_thread_updated_at: i64,
    pub observed_notification_turn_id: Option<String>,
    pub confirmed_turn_id: Option<String>,
    pub prepared_at: i64,
    pub accepted_at: Option<i64>,
    pub confirmed_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackedTurnState {
    pub thread_id: String,
    pub owner_identity_id: Option<IdentityId>,
    pub state: TrackedTurnStateStatus,
    pub last_thread_updated_at: Option<i64>,
    pub turn_count: usize,
    pub latest_turn_id: Option<String>,
    pub latest_turn_status: Option<TurnStatus>,
    pub handoff: Option<HandoffRecord>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffConfirmation {
    pub snapshot: ThreadSnapshot,
    pub matched_turn_id: Option<String>,
}

pub fn new_lease_token() -> String {
    let counter = TOKEN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("lease-{}-{}-{}", std::process::id(), nanos, counter)
}

#[cfg(test)]
mod tests {
    use super::{new_lease_token, ThreadSnapshot};

    #[test]
    fn thread_snapshot_reports_turn_presence() {
        let snapshot = ThreadSnapshot {
            thread_id: "thread-1".to_string(),
            created_at: 1,
            updated_at: 2,
            status: "idle".to_string(),
            path: None,
            turn_ids: vec!["turn-a".to_string(), "turn-b".to_string()],
            latest_turn_id: Some("turn-b".to_string()),
            latest_turn_status: None,
        };

        assert_eq!(snapshot.turn_count(), 2);
        assert!(snapshot.contains_turn("turn-b"));
        assert!(!snapshot.contains_turn("turn-c"));
    }

    #[test]
    fn lease_tokens_are_unique() {
        let first = new_lease_token();
        let second = new_lease_token();
        assert_ne!(first, second);
    }
}
