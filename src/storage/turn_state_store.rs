use std::path::{Path, PathBuf};

use crate::domain::thread::TrackedTurnState;
use crate::error::Result;
use crate::storage::paths::{atomic_write, turn_state_path};

pub trait TurnStateStore {
    fn load(&self, thread_id: &str) -> Result<Option<TrackedTurnState>>;
    fn save(&self, state: &TrackedTurnState) -> Result<()>;
    fn delete(&self, thread_id: &str) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct JsonTurnStateStore {
    base_root: PathBuf,
}

impl JsonTurnStateStore {
    pub fn new(base_root: &Path) -> Self {
        Self {
            base_root: base_root.to_path_buf(),
        }
    }

    fn record_path(&self, thread_id: &str) -> PathBuf {
        turn_state_path(&self.base_root, thread_id)
    }
}

impl TurnStateStore for JsonTurnStateStore {
    fn load(&self, thread_id: &str) -> Result<Option<TrackedTurnState>> {
        let path = self.record_path(thread_id);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn save(&self, state: &TrackedTurnState) -> Result<()> {
        let path = self.record_path(&state.thread_id);
        let mut payload = serde_json::to_vec_pretty(state)?;
        payload.push(b'\n');
        atomic_write(&path, &payload, 0o600)
    }

    fn delete(&self, thread_id: &str) -> Result<()> {
        let path = self.record_path(thread_id);
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{JsonTurnStateStore, TurnStateStore};
    use crate::domain::thread::TrackedTurnState;

    #[test]
    fn round_trips_tracked_turn_state() {
        let temp = tempdir().unwrap();
        let store = JsonTurnStateStore::new(temp.path());
        let state = TrackedTurnState {
            thread_id: "thread-1".to_string(),
            owner_identity_id: None,
            state: crate::domain::thread::TrackedTurnStateStatus::Active,
            last_thread_updated_at: Some(2),
            turn_count: 1,
            latest_turn_id: Some("turn-a".to_string()),
            latest_turn_status: Some(crate::domain::thread::TurnStatus::Completed),
            handoff: None,
            updated_at: 2,
        };

        store.save(&state).unwrap();
        let loaded = store.load("thread-1").unwrap().unwrap();
        assert_eq!(loaded, state);

        store.delete("thread-1").unwrap();
        assert!(store.load("thread-1").unwrap().is_none());
    }
}
