use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::identity::{current_timestamp, IdentityId};
use crate::error::Result;

pub const SELECTION_EVENT_VERSION: u32 = 1;

static SELECTION_EVENT_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectionEvent {
    pub version: u32,
    pub id: String,
    pub identity_id: IdentityId,
    pub reason: String,
    pub from_identity_id: Option<IdentityId>,
    pub decision_json: Value,
    pub created_at: i64,
}

impl SelectionEvent {
    pub fn new(
        identity_id: IdentityId,
        reason: impl Into<String>,
        from_identity_id: Option<IdentityId>,
        decision_json: Value,
    ) -> Result<Self> {
        Ok(Self {
            version: SELECTION_EVENT_VERSION,
            id: new_selection_event_id(),
            identity_id,
            reason: reason.into(),
            from_identity_id,
            decision_json,
            created_at: current_timestamp()?,
        })
    }
}

pub fn new_selection_event_id() -> String {
    let counter = SELECTION_EVENT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!(
        "selection-event-{}-{}-{}",
        std::process::id(),
        nanos,
        counter
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{SelectionEvent, SELECTION_EVENT_VERSION};
    use crate::domain::identity::IdentityId;

    #[test]
    fn creates_typed_selection_events() {
        let event = SelectionEvent::new(
            IdentityId::from_display_name("Backup").unwrap(),
            "selected automatically",
            Some(IdentityId::from_display_name("Primary").unwrap()),
            json!({
                "kind": "thread_handoff",
                "thread_id": "thread-1"
            }),
        )
        .unwrap();

        assert_eq!(event.version, SELECTION_EVENT_VERSION);
        assert_eq!(event.identity_id.as_str(), "backup");
        assert_eq!(
            event.from_identity_id.as_ref().map(IdentityId::as_str),
            Some("primary")
        );
        assert_eq!(event.decision_json["kind"], "thread_handoff");
    }
}
