use std::path::{Path, PathBuf};

use crate::domain::policy::{SelectionPolicyRecord, SELECTION_POLICY_VERSION};
use crate::error::{AppError, Result};
use crate::storage::paths::{atomic_write, selection_policy_path};

pub trait SelectionPolicyStore {
    fn load(&self) -> Result<SelectionPolicyRecord>;
    fn save(&self, record: &SelectionPolicyRecord) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct JsonSelectionPolicyStore {
    path: PathBuf,
}

impl JsonSelectionPolicyStore {
    pub fn new(base_root: &Path) -> Self {
        Self {
            path: selection_policy_path(base_root),
        }
    }
}

impl SelectionPolicyStore for JsonSelectionPolicyStore {
    fn load(&self) -> Result<SelectionPolicyRecord> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let record: SelectionPolicyRecord = serde_json::from_slice(&bytes)?;
                if record.version != SELECTION_POLICY_VERSION {
                    return Err(AppError::UnsupportedSelectionPolicyVersion {
                        found: record.version,
                    });
                }
                Ok(record)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(SelectionPolicyRecord::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn save(&self, record: &SelectionPolicyRecord) -> Result<()> {
        let mut payload = serde_json::to_vec_pretty(record)?;
        payload.push(b'\n');
        atomic_write(&self.path, &payload, 0o600)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{JsonSelectionPolicyStore, SelectionPolicyStore};

    #[test]
    fn round_trips_selection_policy() {
        let temp = tempdir().unwrap();
        let store = JsonSelectionPolicyStore::new(temp.path());
        let record = crate::domain::policy::SelectionPolicyRecord::default();
        store.save(&record).unwrap();

        let loaded = store.load().unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.policy, record.policy);
    }
}
