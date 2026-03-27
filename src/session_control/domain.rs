use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::identity::IdentityId;

static ENTITY_COUNTER: AtomicU64 = AtomicU64::new(1);

macro_rules! string_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self(new_entity_id($prefix))
            }

            pub fn from_string(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(SessionId, "session");
string_id!(SessionTurnId, "turn");
string_id!(SessionHandoffId, "handoff");

fn new_entity_id(prefix: &str) -> String {
    let counter = ENTITY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{prefix}-{}-{}-{}", std::process::id(), nanos, counter)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Running,
    WaitingForFollowup,
    HandoffPending,
    HandoffReady,
    Blocked,
    Failed,
    Canceled,
}

impl SessionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::WaitingForFollowup => "waiting_for_followup",
            Self::HandoffPending => "handoff_pending",
            Self::HandoffReady => "handoff_ready",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "idle" => Some(Self::Idle),
            "running" => Some(Self::Running),
            "waiting_for_followup" => Some(Self::WaitingForFollowup),
            "handoff_pending" => Some(Self::HandoffPending),
            "handoff_ready" => Some(Self::HandoffReady),
            "blocked" => Some(Self::Blocked),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }
}

impl fmt::Display for SessionStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityMode {
    SameThread,
    Handoff,
    CheckpointFallback,
}

impl ContinuityMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SameThread => "same_thread",
            Self::Handoff => "handoff",
            Self::CheckpointFallback => "checkpoint_fallback",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "same_thread" => Some(Self::SameThread),
            "handoff" => Some(Self::Handoff),
            "checkpoint_fallback" => Some(Self::CheckpointFallback),
            _ => None,
        }
    }
}

impl fmt::Display for ContinuityMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTurnStatus {
    Queued,
    Starting,
    Running,
    Completed,
    Failed,
    TimedOut,
    Canceled,
}

impl SessionTurnStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Canceled => "canceled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "starting" => Some(Self::Starting),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "timed_out" => Some(Self::TimedOut),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::TimedOut | Self::Canceled
        )
    }
}

impl fmt::Display for SessionTurnStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionHandoffStatus {
    Prepared,
    Accepted,
    Confirmed,
    Expired,
    Aborted,
    FallbackRequired,
}

impl SessionHandoffStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Accepted => "accepted",
            Self::Confirmed => "confirmed",
            Self::Expired => "expired",
            Self::Aborted => "aborted",
            Self::FallbackRequired => "fallback_required",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "prepared" => Some(Self::Prepared),
            "accepted" => Some(Self::Accepted),
            "confirmed" => Some(Self::Confirmed),
            "expired" => Some(Self::Expired),
            "aborted" => Some(Self::Aborted),
            "fallback_required" => Some(Self::FallbackRequired),
            _ => None,
        }
    }
}

impl fmt::Display for SessionHandoffStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffFallbackMode {
    CheckpointFallback,
}

impl HandoffFallbackMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CheckpointFallback => "checkpoint_fallback",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "checkpoint_fallback" => Some(Self::CheckpointFallback),
            _ => None,
        }
    }
}

impl fmt::Display for HandoffFallbackMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffLeaseStateKind {
    Active,
    HandoffPending,
}

impl HandoffLeaseStateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::HandoffPending => "handoff_pending",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "handoff_pending" => Some(Self::HandoffPending),
            _ => None,
        }
    }
}

impl fmt::Display for HandoffLeaseStateKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: SessionId,
    pub topic_key: String,
    pub workspace_root: String,
    pub model: Option<String>,
    pub thread_id: String,
    pub current_identity_id: IdentityId,
    pub current_identity_name: String,
    pub status: SessionStatus,
    pub last_turn_id: Option<SessionTurnId>,
    pub active_turn_id: Option<SessionTurnId>,
    pub continuity_mode: ContinuityMode,
    pub safe_to_continue: bool,
    pub pending_handoff_id: Option<SessionHandoffId>,
    pub last_checkpoint_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTurnRecord {
    pub turn_id: SessionTurnId,
    pub session_id: SessionId,
    pub thread_id: String,
    pub identity_id: IdentityId,
    pub identity_name: String,
    pub status: SessionTurnStatus,
    pub prompt_text: String,
    pub continuity_mode: ContinuityMode,
    pub runtime_turn_id: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub failure_kind: Option<String>,
    pub failure_message: Option<String>,
    pub worker_owner_id: Option<String>,
    pub worker_pid: Option<u32>,
    pub heartbeat_at: Option<i64>,
    pub heartbeat_expires_at: Option<i64>,
    pub cancel_requested: bool,
    pub lease_thread_id: Option<String>,
    pub lease_token: Option<String>,
    pub lease_persist_on_finish: bool,
    pub idempotency_key: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub max_runtime_secs: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHandoffRecord {
    pub handoff_id: SessionHandoffId,
    pub session_id: SessionId,
    pub thread_id: String,
    pub from_identity_id: IdentityId,
    pub from_identity_name: String,
    pub to_identity_id: IdentityId,
    pub to_identity_name: String,
    pub status: SessionHandoffStatus,
    pub lease_token: String,
    pub lease_owner_identity_id: IdentityId,
    pub lease_owner_identity_name: String,
    pub lease_state_kind: HandoffLeaseStateKind,
    pub reason: String,
    pub baseline_turn_id: Option<String>,
    pub observed_turn_id: Option<String>,
    pub fallback_mode: Option<HandoffFallbackMode>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionEventRecord {
    pub sequence_no: i64,
    pub interface_version: String,
    pub event: String,
    pub session_id: SessionId,
    pub thread_id: String,
    pub turn_id: Option<SessionTurnId>,
    pub runtime_turn_id: Option<String>,
    pub handoff_id: Option<SessionHandoffId>,
    pub timestamp: i64,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub session: SessionRecord,
    pub active_turn: Option<SessionTurnRecord>,
    pub last_turn: Option<SessionTurnRecord>,
    pub pending_handoff: Option<SessionHandoffRecord>,
}
