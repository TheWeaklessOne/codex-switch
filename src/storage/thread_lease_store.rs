use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use crate::domain::thread::ThreadLeaseRecord;
use crate::error::{AppError, Result};
use crate::storage::paths::{
    atomic_write, ensure_directory, thread_lease_path, thread_leases_lock_path, thread_leases_path,
};

pub trait ThreadLeaseStore {
    fn load(&self, thread_id: &str) -> Result<Option<ThreadLeaseRecord>>;
    fn save(&self, lease: &ThreadLeaseRecord) -> Result<()>;
    fn with_locked_lease<T>(
        &self,
        thread_id: &str,
        operation: impl FnOnce(Option<ThreadLeaseRecord>) -> Result<T>,
    ) -> Result<T>;
}

#[derive(Debug, Clone)]
pub struct JsonThreadLeaseStore {
    base_root: PathBuf,
    lock_timeout: Duration,
    retry_delay: Duration,
    stale_lock_age: Duration,
}

impl JsonThreadLeaseStore {
    pub fn new(base_root: &Path, lock_timeout: Duration, retry_delay: Duration) -> Self {
        Self::with_stale_lock_age(
            base_root,
            lock_timeout,
            retry_delay,
            Duration::from_secs(30),
        )
    }

    pub fn with_stale_lock_age(
        base_root: &Path,
        lock_timeout: Duration,
        retry_delay: Duration,
        stale_lock_age: Duration,
    ) -> Self {
        Self {
            base_root: base_root.to_path_buf(),
            lock_timeout,
            retry_delay,
            stale_lock_age,
        }
    }

    #[cfg(test)]
    pub fn test_store(base_root: &Path) -> Self {
        Self::with_stale_lock_age(
            base_root,
            Duration::from_secs(1),
            Duration::from_millis(5),
            Duration::from_millis(20),
        )
    }

    fn record_path(&self, thread_id: &str) -> PathBuf {
        thread_lease_path(&self.base_root, thread_id)
    }

    fn lock_path(&self, thread_id: &str) -> PathBuf {
        thread_leases_lock_path(&self.base_root, thread_id)
    }

    fn lock_thread(&self, thread_id: &str) -> Result<LeaseLockGuard> {
        let leases_root = thread_leases_path(&self.base_root);
        ensure_directory(&leases_root, 0o700)?;
        ensure_directory(&leases_root.join("locks"), 0o700)?;
        let lock_path = self.lock_path(thread_id);
        let started = Instant::now();

        loop {
            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path)
            {
                Ok(_) => {
                    return Ok(LeaseLockGuard { path: lock_path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if self.lock_is_stale(&lock_path)? {
                        match fs::remove_file(&lock_path) {
                            Ok(()) => continue,
                            Err(remove_error)
                                if remove_error.kind() == std::io::ErrorKind::NotFound =>
                            {
                                continue;
                            }
                            Err(remove_error) => return Err(remove_error.into()),
                        }
                    }
                    if started.elapsed() >= self.lock_timeout {
                        return Err(AppError::LeaseLockTimeout {
                            path: lock_path,
                            timeout: self.lock_timeout,
                        });
                    }
                    thread::sleep(self.retry_delay);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn lock_is_stale(&self, lock_path: &Path) -> Result<bool> {
        let metadata = fs::metadata(lock_path)?;
        let modified = metadata.modified()?;
        let age = modified.elapsed().unwrap_or_default();
        Ok(age >= self.stale_lock_age)
    }
}

impl ThreadLeaseStore for JsonThreadLeaseStore {
    fn load(&self, thread_id: &str) -> Result<Option<ThreadLeaseRecord>> {
        let path = self.record_path(thread_id);
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn save(&self, lease: &ThreadLeaseRecord) -> Result<()> {
        let path = self.record_path(&lease.thread_id);
        let mut payload = serde_json::to_vec_pretty(lease)?;
        payload.push(b'\n');
        atomic_write(&path, &payload, 0o600)
    }

    fn with_locked_lease<T>(
        &self,
        thread_id: &str,
        operation: impl FnOnce(Option<ThreadLeaseRecord>) -> Result<T>,
    ) -> Result<T> {
        let _guard = self.lock_thread(thread_id)?;
        let current = self.load(thread_id)?;
        operation(current)
    }
}

#[derive(Debug)]
struct LeaseLockGuard {
    path: PathBuf,
}

impl Drop for LeaseLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::thread;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::{JsonThreadLeaseStore, ThreadLeaseStore};
    use crate::domain::identity::IdentityId;
    use crate::domain::thread::{ThreadLeaseRecord, ThreadLeaseState};

    #[test]
    fn round_trips_lease_records() {
        let temp = tempdir().unwrap();
        let store = JsonThreadLeaseStore::test_store(temp.path());
        let lease = ThreadLeaseRecord {
            thread_id: "thread-1".to_string(),
            owner_identity_id: IdentityId::from_display_name("Source").unwrap(),
            lease_state: ThreadLeaseState::Active,
            lease_token: "token".to_string(),
            handoff_to_identity_id: None,
            handoff_reason: None,
            last_heartbeat_at: 1,
            updated_at: 1,
        };

        store.save(&lease).unwrap();
        let loaded = store.load("thread-1").unwrap().unwrap();
        assert_eq!(loaded, lease);
    }

    #[test]
    fn recovers_from_stale_lock_file() {
        let temp = tempdir().unwrap();
        let store = JsonThreadLeaseStore::test_store(temp.path());
        let locks_root = crate::storage::paths::thread_leases_path(temp.path()).join("locks");
        fs::create_dir_all(&locks_root).unwrap();
        let lock_path = locks_root.join("7468726561642d31.lock");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        thread::sleep(Duration::from_millis(30));

        let loaded = store.with_locked_lease("thread-1", Ok).unwrap();
        assert!(loaded.is_none());
        assert!(!lock_path.exists());
    }
}
