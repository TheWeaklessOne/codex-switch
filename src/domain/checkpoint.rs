use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::domain::identity::{current_timestamp, IdentityId};
use crate::domain::thread::ThreadSnapshot;
use crate::error::Result;

pub const TASK_CHECKPOINT_VERSION: u32 = 1;

static CHECKPOINT_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointMode {
    ResumeSameThread,
    ResumeViaCheckpoint,
}

impl CheckpointMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResumeSameThread => "resume_same_thread",
            Self::ResumeViaCheckpoint => "resume_via_checkpoint",
        }
    }
}

impl fmt::Display for CheckpointMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCheckpoint {
    pub version: u32,
    pub id: String,
    pub thread_id: String,
    pub source_identity_id: IdentityId,
    pub target_identity_id: IdentityId,
    pub mode: CheckpointMode,
    pub reason: String,
    pub fallback_reason: Option<String>,
    pub latest_turn_id: Option<String>,
    pub thread_updated_at: i64,
    pub thread_status: String,
    pub summary_md: String,
    pub resume_prompt: String,
    pub thread_snapshot: ThreadSnapshot,
    pub created_at: i64,
    pub updated_at: i64,
}

impl TaskCheckpoint {
    pub fn new(
        snapshot: &ThreadSnapshot,
        source_identity_id: IdentityId,
        target_identity_id: IdentityId,
        mode: CheckpointMode,
        reason: impl Into<String>,
        fallback_reason: Option<String>,
    ) -> Result<Self> {
        let reason = reason.into();
        let created_at = current_timestamp()?;
        Ok(Self {
            version: TASK_CHECKPOINT_VERSION,
            id: new_checkpoint_id(),
            thread_id: snapshot.thread_id.clone(),
            source_identity_id: source_identity_id.clone(),
            target_identity_id: target_identity_id.clone(),
            mode,
            reason: reason.clone(),
            fallback_reason: fallback_reason.clone(),
            latest_turn_id: snapshot.latest_turn_id.clone(),
            thread_updated_at: snapshot.updated_at,
            thread_status: snapshot.status.clone(),
            summary_md: build_summary(
                snapshot,
                &source_identity_id,
                &target_identity_id,
                mode,
                &reason,
                fallback_reason.as_deref(),
            ),
            resume_prompt: build_resume_prompt(
                snapshot,
                &source_identity_id,
                &target_identity_id,
                mode,
                &reason,
                fallback_reason.as_deref(),
            ),
            thread_snapshot: snapshot.clone(),
            created_at,
            updated_at: created_at,
        })
    }
}

pub fn new_checkpoint_id() -> String {
    let counter = CHECKPOINT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("checkpoint-{}-{}-{}", std::process::id(), nanos, counter)
}

fn build_summary(
    snapshot: &ThreadSnapshot,
    source_identity_id: &IdentityId,
    target_identity_id: &IdentityId,
    mode: CheckpointMode,
    reason: &str,
    fallback_reason: Option<&str>,
) -> String {
    let mut lines = vec![
        format!("# Codex handoff checkpoint `{}`", snapshot.thread_id),
        String::new(),
        format!("- mode: {}", mode.as_str()),
        format!("- reason: {}", reason),
        format!("- source identity: {}", source_identity_id),
        format!("- target identity: {}", target_identity_id),
        format!(
            "- latest turn id: {}",
            snapshot.latest_turn_id.as_deref().unwrap_or("unknown")
        ),
        format!("- thread status: {}", snapshot.status),
        format!("- thread updated at: {}", snapshot.updated_at),
    ];

    if let Some(fallback_reason) = fallback_reason {
        lines.push(format!("- fallback reason: {}", fallback_reason));
    }

    lines.push(String::new());
    lines.push("## Next step".to_string());
    lines.push(match mode {
        CheckpointMode::ResumeSameThread => format!(
            "Resume thread `{}` on identity `{}` with the shared sessions store.",
            snapshot.thread_id, target_identity_id
        ),
        CheckpointMode::ResumeViaCheckpoint => format!(
            "Start a new Codex session on identity `{}` and continue from this checkpoint because the original thread could not be resumed safely across identities.",
            target_identity_id
        ),
    });

    lines.join("\n")
}

fn build_resume_prompt(
    snapshot: &ThreadSnapshot,
    source_identity_id: &IdentityId,
    target_identity_id: &IdentityId,
    mode: CheckpointMode,
    reason: &str,
    fallback_reason: Option<&str>,
) -> String {
    let latest_turn = snapshot.latest_turn_id.as_deref().unwrap_or("unknown");
    let mut prompt = format!(
        "Continue the work from thread {}. Source identity: {}. Target identity: {}. Latest turn: {}. Thread status: {}. Reason: {}.",
        snapshot.thread_id,
        source_identity_id,
        target_identity_id,
        latest_turn,
        snapshot.status,
        reason
    );

    match mode {
        CheckpointMode::ResumeSameThread => {
            prompt.push_str(" Resume the existing shared-history thread.");
        }
        CheckpointMode::ResumeViaCheckpoint => {
            prompt.push_str(
                " Shared-history resume was not available. Start a new thread and use this checkpoint as the authoritative handoff state.",
            );
        }
    }

    if let Some(fallback_reason) = fallback_reason {
        prompt.push(' ');
        prompt.push_str("Fallback reason: ");
        prompt.push_str(fallback_reason);
        prompt.push('.');
    }

    prompt
}

#[cfg(test)]
mod tests {
    use super::{CheckpointMode, TaskCheckpoint, TASK_CHECKPOINT_VERSION};
    use crate::domain::identity::IdentityId;
    use crate::domain::thread::{ThreadSnapshot, TurnStatus};

    #[test]
    fn builds_resume_via_checkpoint_prompt() {
        let checkpoint = TaskCheckpoint::new(
            &ThreadSnapshot {
                thread_id: "thread-1".to_string(),
                created_at: 1,
                updated_at: 2,
                status: "idle".to_string(),
                path: Some("/tmp/thread-1".to_string()),
                turn_ids: vec!["turn-a".to_string()],
                latest_turn_id: Some("turn-a".to_string()),
                latest_turn_status: Some(TurnStatus::Completed),
            },
            IdentityId::from_display_name("Source").unwrap(),
            IdentityId::from_display_name("Target").unwrap(),
            CheckpointMode::ResumeViaCheckpoint,
            "quota",
            Some("shared session store mismatch".to_string()),
        )
        .unwrap();

        assert_eq!(checkpoint.version, TASK_CHECKPOINT_VERSION);
        assert_eq!(checkpoint.mode, CheckpointMode::ResumeViaCheckpoint);
        assert!(checkpoint.resume_prompt.contains("Start a new thread"));
        assert!(checkpoint.summary_md.contains("fallback reason"));
    }
}
