use std::path::{Path, PathBuf};

use crate::domain::selection::{SelectionStateRecord, SELECTION_STATE_VERSION};
use crate::error::{AppError, Result};
use crate::storage::paths::{atomic_write, selection_state_path};

pub trait SelectionStore {
    fn load(&self) -> Result<SelectionStateRecord>;
    fn save(&self, record: &SelectionStateRecord) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct JsonSelectionStore {
    path: PathBuf,
}

impl JsonSelectionStore {
    pub fn new(base_root: &Path) -> Self {
        Self {
            path: selection_state_path(base_root),
        }
    }
}

impl SelectionStore for JsonSelectionStore {
    fn load(&self) -> Result<SelectionStateRecord> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let record: SelectionStateRecord = serde_json::from_slice(&bytes)?;
                if record.version != SELECTION_STATE_VERSION {
                    return Err(AppError::UnsupportedSelectionStateVersion {
                        found: record.version,
                    });
                }
                Ok(record)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(SelectionStateRecord::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn save(&self, record: &SelectionStateRecord) -> Result<()> {
        let mut payload = serde_json::to_vec_pretty(record)?;
        payload.push(b'\n');
        atomic_write(&self.path, &payload, 0o600)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{JsonSelectionStore, SelectionStore};
    use crate::domain::selection::{SelectionStateRecord, SELECTION_STATE_VERSION};

    #[test]
    fn round_trips_selection_state() {
        let temp = tempdir().unwrap();
        let store = JsonSelectionStore::new(temp.path());
        let record = SelectionStateRecord::default();
        store.save(&record).unwrap();

        let loaded = store.load().unwrap();
        assert_eq!(loaded.version, SELECTION_STATE_VERSION);
        assert!(loaded.current.is_none());
    }
}
