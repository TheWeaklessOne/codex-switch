use std::fmt;

use serde::{Deserialize, Serialize};

use crate::domain::identity::IdentityId;

pub const SELECTION_STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionMode {
    Manual,
    Automatic,
}

impl SelectionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Automatic => "automatic",
        }
    }
}

impl fmt::Display for SelectionMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedIdentityState {
    pub identity_id: IdentityId,
    pub mode: SelectionMode,
    pub reason: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectionStateRecord {
    pub version: u32,
    pub current: Option<SelectedIdentityState>,
}

impl Default for SelectionStateRecord {
    fn default() -> Self {
        Self {
            version: SELECTION_STATE_VERSION,
            current: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SelectionMode, SelectionStateRecord, SELECTION_STATE_VERSION};

    #[test]
    fn exposes_selection_mode_labels() {
        assert_eq!(SelectionMode::Manual.as_str(), "manual");
        assert_eq!(SelectionMode::Automatic.to_string(), "automatic");
    }

    #[test]
    fn defaults_to_empty_state_record() {
        let record = SelectionStateRecord::default();
        assert_eq!(record.version, SELECTION_STATE_VERSION);
        assert!(record.current.is_none());
    }
}
