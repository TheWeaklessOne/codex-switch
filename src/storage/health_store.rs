use std::path::{Path, PathBuf};

use crate::domain::health::{IdentityHealthRecord, IDENTITY_HEALTH_VERSION};
use crate::error::{AppError, Result};
use crate::storage::paths::{atomic_write, identity_health_path};

pub trait IdentityHealthStore {
    fn load(&self) -> Result<IdentityHealthRecord>;
    fn save(&self, record: &IdentityHealthRecord) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct JsonIdentityHealthStore {
    path: PathBuf,
}

impl JsonIdentityHealthStore {
    pub fn new(base_root: &Path) -> Self {
        Self {
            path: identity_health_path(base_root),
        }
    }
}

impl IdentityHealthStore for JsonIdentityHealthStore {
    fn load(&self) -> Result<IdentityHealthRecord> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let record: IdentityHealthRecord = serde_json::from_slice(&bytes)?;
                if record.version != IDENTITY_HEALTH_VERSION {
                    return Err(AppError::UnsupportedIdentityHealthVersion {
                        found: record.version,
                    });
                }
                Ok(record)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(IdentityHealthRecord::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn save(&self, record: &IdentityHealthRecord) -> Result<()> {
        let mut payload = serde_json::to_vec_pretty(record)?;
        payload.push(b'\n');
        atomic_write(&self.path, &payload, 0o600)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{IdentityHealthStore, JsonIdentityHealthStore};

    #[test]
    fn round_trips_identity_health_record() {
        let temp = tempdir().unwrap();
        let store = JsonIdentityHealthStore::new(temp.path());
        let record = crate::domain::health::IdentityHealthRecord::default();
        store.save(&record).unwrap();

        let loaded = store.load().unwrap();
        assert_eq!(loaded.version, 1);
        assert!(loaded.identities.is_empty());
    }
}
