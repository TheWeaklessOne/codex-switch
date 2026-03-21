use std::fs;
use std::path::{Path, PathBuf};

use crate::domain::decision::{SelectionEvent, SELECTION_EVENT_VERSION};
use crate::error::{AppError, Result};
use crate::storage::paths::{atomic_write, selection_event_path, selection_events_path};

pub trait SelectionEventStore {
    fn append(&self, event: &SelectionEvent) -> Result<PathBuf>;
    fn load(&self, event_id: &str) -> Result<Option<SelectionEvent>>;
    fn list(&self) -> Result<Vec<(SelectionEvent, PathBuf)>>;
}

#[derive(Debug, Clone)]
pub struct JsonSelectionEventStore {
    base_root: PathBuf,
}

impl JsonSelectionEventStore {
    pub fn new(base_root: &Path) -> Self {
        Self {
            base_root: base_root.to_path_buf(),
        }
    }

    fn record_path(&self, event_id: &str) -> PathBuf {
        selection_event_path(&self.base_root, event_id)
    }
}

impl SelectionEventStore for JsonSelectionEventStore {
    fn append(&self, event: &SelectionEvent) -> Result<PathBuf> {
        let path = self.record_path(&event.id);
        let mut payload = serde_json::to_vec_pretty(event)?;
        payload.push(b'\n');
        atomic_write(&path, &payload, 0o600)?;
        Ok(path)
    }

    fn load(&self, event_id: &str) -> Result<Option<SelectionEvent>> {
        let path = self.record_path(event_id);
        match fs::read(&path) {
            Ok(bytes) => {
                let event: SelectionEvent = serde_json::from_slice(&bytes)?;
                if event.version != SELECTION_EVENT_VERSION {
                    return Err(AppError::UnsupportedSelectionEventVersion {
                        found: event.version,
                    });
                }
                Ok(Some(event))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn list(&self) -> Result<Vec<(SelectionEvent, PathBuf)>> {
        let directory = selection_events_path(&self.base_root);
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };

        let mut events = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file() {
                continue;
            }

            let bytes = fs::read(&path)?;
            let event: SelectionEvent = serde_json::from_slice(&bytes)?;
            if event.version != SELECTION_EVENT_VERSION {
                return Err(AppError::UnsupportedSelectionEventVersion {
                    found: event.version,
                });
            }
            events.push((event, path));
        }

        events.sort_by(|(left, _), (right, _)| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::{JsonSelectionEventStore, SelectionEventStore};
    use crate::domain::decision::SelectionEvent;
    use crate::domain::identity::IdentityId;

    #[test]
    fn appends_and_lists_selection_events() {
        let temp = tempdir().unwrap();
        let store = JsonSelectionEventStore::new(temp.path());
        let first = SelectionEvent::new(
            IdentityId::from_display_name("Primary").unwrap(),
            "selected automatically",
            None,
            json!({"kind": "new_session"}),
        )
        .unwrap();
        let second = SelectionEvent::new(
            IdentityId::from_display_name("Backup").unwrap(),
            "handoff",
            Some(IdentityId::from_display_name("Primary").unwrap()),
            json!({"kind": "thread_handoff"}),
        )
        .unwrap();

        let first_path = store.append(&first).unwrap();
        let second_path = store.append(&second).unwrap();
        assert!(first_path.ends_with(format!("{}.json", first.id)));
        assert!(second_path.ends_with(format!("{}.json", second.id)));

        let loaded = store.load(&first.id).unwrap().unwrap();
        assert_eq!(loaded.identity_id.as_str(), "primary");

        let listed = store.list().unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].0.id, first.id);
        assert_eq!(listed[1].0.id, second.id);
    }
}
