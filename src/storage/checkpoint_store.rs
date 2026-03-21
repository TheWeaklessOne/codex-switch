use std::fs;
use std::path::{Path, PathBuf};

use crate::domain::checkpoint::{TaskCheckpoint, TASK_CHECKPOINT_VERSION};
use crate::error::{AppError, Result};
use crate::storage::paths::{
    atomic_write, ensure_directory, task_checkpoint_path, task_checkpoints_path,
};

pub trait TaskCheckpointStore {
    fn save(&self, checkpoint: &TaskCheckpoint) -> Result<PathBuf>;
    fn load(&self, checkpoint_id: &str) -> Result<Option<TaskCheckpoint>>;
    fn delete(&self, checkpoint_id: &str) -> Result<()>;
    fn latest_for_thread(&self, thread_id: &str) -> Result<Option<(TaskCheckpoint, PathBuf)>>;
}

#[derive(Debug, Clone)]
pub struct JsonTaskCheckpointStore {
    base_root: PathBuf,
}

impl JsonTaskCheckpointStore {
    pub fn new(base_root: &Path) -> Self {
        Self {
            base_root: base_root.to_path_buf(),
        }
    }

    fn record_path(&self, checkpoint_id: &str) -> PathBuf {
        task_checkpoint_path(&self.base_root, checkpoint_id)
    }
}

impl TaskCheckpointStore for JsonTaskCheckpointStore {
    fn save(&self, checkpoint: &TaskCheckpoint) -> Result<PathBuf> {
        let directory = task_checkpoints_path(&self.base_root);
        ensure_directory(&directory, 0o700)?;
        let path = self.record_path(&checkpoint.id);
        let mut payload = serde_json::to_vec_pretty(checkpoint)?;
        payload.push(b'\n');
        atomic_write(&path, &payload, 0o600)?;
        Ok(path)
    }

    fn load(&self, checkpoint_id: &str) -> Result<Option<TaskCheckpoint>> {
        let path = self.record_path(checkpoint_id);
        match fs::read(&path) {
            Ok(bytes) => {
                let checkpoint: TaskCheckpoint = serde_json::from_slice(&bytes)?;
                if checkpoint.version != TASK_CHECKPOINT_VERSION {
                    return Err(AppError::UnsupportedTaskCheckpointVersion {
                        found: checkpoint.version,
                    });
                }
                Ok(Some(checkpoint))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn delete(&self, checkpoint_id: &str) -> Result<()> {
        let path = self.record_path(checkpoint_id);
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn latest_for_thread(&self, thread_id: &str) -> Result<Option<(TaskCheckpoint, PathBuf)>> {
        let directory = task_checkpoints_path(&self.base_root);
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        let mut latest: Option<(TaskCheckpoint, PathBuf)> = None;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file() {
                continue;
            }
            let bytes = fs::read(&path)?;
            let checkpoint: TaskCheckpoint = serde_json::from_slice(&bytes)?;
            if checkpoint.version != TASK_CHECKPOINT_VERSION {
                return Err(AppError::UnsupportedTaskCheckpointVersion {
                    found: checkpoint.version,
                });
            }
            if checkpoint.thread_id != thread_id {
                continue;
            }

            let replace = match latest.as_ref() {
                None => true,
                Some((current, _)) => {
                    checkpoint.updated_at > current.updated_at
                        || (checkpoint.updated_at == current.updated_at
                            && checkpoint.id > current.id)
                }
            };
            if replace {
                latest = Some((checkpoint, path));
            }
        }

        Ok(latest)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{JsonTaskCheckpointStore, TaskCheckpointStore};
    use crate::domain::checkpoint::{CheckpointMode, TaskCheckpoint};
    use crate::domain::identity::IdentityId;
    use crate::domain::thread::ThreadSnapshot;

    #[test]
    fn saves_and_loads_latest_checkpoint_for_thread() {
        let temp = tempdir().unwrap();
        let store = JsonTaskCheckpointStore::new(temp.path());
        let checkpoint = TaskCheckpoint::new(
            &ThreadSnapshot {
                thread_id: "thread-1".to_string(),
                created_at: 1,
                updated_at: 2,
                status: "idle".to_string(),
                path: None,
                turn_ids: vec!["turn-a".to_string()],
                latest_turn_id: Some("turn-a".to_string()),
                latest_turn_status: None,
            },
            IdentityId::from_display_name("Source").unwrap(),
            IdentityId::from_display_name("Target").unwrap(),
            CheckpointMode::ResumeSameThread,
            "manual_switch",
            None,
        )
        .unwrap();

        let path = store.save(&checkpoint).unwrap();
        let loaded = store.load(&checkpoint.id).unwrap().unwrap();
        assert_eq!(loaded.id, checkpoint.id);
        assert_eq!(
            path.file_name().and_then(|value| value.to_str()),
            Some(format!("{}.json", checkpoint.id).as_str())
        );

        let latest = store.latest_for_thread("thread-1").unwrap().unwrap();
        assert_eq!(latest.0.id, checkpoint.id);

        store.delete(&checkpoint.id).unwrap();
        assert!(store.load(&checkpoint.id).unwrap().is_none());
    }
}
