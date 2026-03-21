use std::path::{Path, PathBuf};

use crate::domain::quota::{QuotaStatusRecord, QUOTA_STATUS_VERSION};
use crate::error::{AppError, Result};
use crate::storage::paths::{atomic_write, quota_status_path};

pub trait QuotaStore {
    fn load(&self) -> Result<QuotaStatusRecord>;
    fn save(&self, record: &QuotaStatusRecord) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct JsonQuotaStore {
    path: PathBuf,
}

impl JsonQuotaStore {
    pub fn new(base_root: &Path) -> Self {
        Self {
            path: quota_status_path(base_root),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl QuotaStore for JsonQuotaStore {
    fn load(&self) -> Result<QuotaStatusRecord> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let record: QuotaStatusRecord = serde_json::from_slice(&bytes)?;
                if record.version != QUOTA_STATUS_VERSION {
                    return Err(AppError::UnsupportedQuotaStatusVersion {
                        found: record.version,
                    });
                }
                Ok(record)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(QuotaStatusRecord::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn save(&self, record: &QuotaStatusRecord) -> Result<()> {
        let mut payload = serde_json::to_vec_pretty(record)?;
        payload.push(b'\n');
        atomic_write(&self.path, &payload, 0o600)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{JsonQuotaStore, QuotaStore};

    #[test]
    fn loads_default_quota_record_when_missing() {
        let temp = tempdir().unwrap();
        let store = JsonQuotaStore::new(temp.path());
        let record = store.load().unwrap();
        assert_eq!(record.version, 1);
        assert!(record.statuses.is_empty());
    }

    #[test]
    fn round_trips_quota_record() {
        let temp = tempdir().unwrap();
        let store = JsonQuotaStore::new(temp.path());
        let record = crate::domain::quota::QuotaStatusRecord::default();
        store.save(&record).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.version, 1);
        assert!(loaded.statuses.is_empty());
    }
}
