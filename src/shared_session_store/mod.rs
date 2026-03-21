use crate::codex_rpc::ThreadRuntime;
use crate::domain::identity::CodexIdentity;
use crate::domain::thread::ThreadSnapshot;
use crate::error::{AppError, Result};

#[derive(Debug, Clone)]
pub struct SharedSessionStore<R> {
    runtime: R,
}

impl<R> SharedSessionStore<R> {
    pub fn new(runtime: R) -> Self {
        Self { runtime }
    }
}

impl<R> SharedSessionStore<R>
where
    R: ThreadRuntime,
{
    pub fn read_thread(&self, identity: &CodexIdentity, thread_id: &str) -> Result<ThreadSnapshot> {
        self.runtime.read_thread(identity, thread_id)
    }

    pub fn resume_thread(
        &self,
        identity: &CodexIdentity,
        thread_id: &str,
    ) -> Result<ThreadSnapshot> {
        self.runtime.resume_thread(identity, thread_id)
    }

    pub fn ensure_cross_identity_visibility(
        &self,
        source: &CodexIdentity,
        target: &CodexIdentity,
        thread_id: &str,
    ) -> Result<ThreadSnapshot> {
        if source.shared_sessions_root != target.shared_sessions_root {
            return Err(AppError::SharedSessionsRootMismatch {
                source_identity_id: source.id.clone(),
                source_root: source.shared_sessions_root.clone(),
                target_identity_id: target.id.clone(),
                target_root: target.shared_sessions_root.clone(),
            });
        }

        let source_snapshot = self.read_thread(source, thread_id)?;
        let target_snapshot = self.resume_thread(target, thread_id)?;

        if target_snapshot.turn_count() < source_snapshot.turn_count() {
            return Err(AppError::ThreadHistoryNotShared {
                thread_id: thread_id.to_string(),
                source_identity_id: source.id.clone(),
                target_identity_id: target.id.clone(),
            });
        }
        if target_snapshot.latest_turn_id != source_snapshot.latest_turn_id {
            return Err(AppError::ThreadHistoryNotShared {
                thread_id: thread_id.to_string(),
                source_identity_id: source.id.clone(),
                target_identity_id: target.id.clone(),
            });
        }

        Ok(target_snapshot)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::codex_rpc::ThreadRuntime;
    use crate::domain::identity::{
        current_timestamp, AuthMode, CodexIdentity, ForcedLoginMethod, IdentityId, IdentityKind,
    };
    use crate::domain::thread::ThreadSnapshot;

    use super::SharedSessionStore;

    #[derive(Debug, Default)]
    struct StubRuntime {
        responses: BTreeMap<String, ThreadSnapshot>,
    }

    impl ThreadRuntime for StubRuntime {
        fn read_thread(
            &self,
            _identity: &CodexIdentity,
            thread_id: &str,
        ) -> crate::error::Result<ThreadSnapshot> {
            Ok(self.responses.get(thread_id).unwrap().clone())
        }

        fn resume_thread(
            &self,
            _identity: &CodexIdentity,
            thread_id: &str,
        ) -> crate::error::Result<ThreadSnapshot> {
            Ok(self.responses.get(thread_id).unwrap().clone())
        }
    }

    fn identity(name: &str, shared_root: &str) -> CodexIdentity {
        CodexIdentity {
            id: IdentityId::from_display_name(name).unwrap(),
            display_name: name.to_string(),
            kind: IdentityKind::ChatgptWorkspace,
            auth_mode: AuthMode::Chatgpt,
            codex_home: std::path::PathBuf::from(format!("/tmp/{name}")),
            shared_sessions_root: std::path::PathBuf::from(shared_root),
            forced_login_method: Some(ForcedLoginMethod::Chatgpt),
            forced_chatgpt_workspace_id: None,
            api_key_env_var: None,
            email: None,
            plan_type: None,
            account_type: None,
            authenticated: None,
            last_auth_method: None,
            enabled: true,
            priority: 0,
            notes: None,
            workspace_force_probe: None,
            imported_auth: false,
            created_at: current_timestamp().unwrap(),
            last_verified_at: None,
        }
    }

    #[test]
    fn validates_visibility_via_shared_root_and_history() {
        let snapshot = ThreadSnapshot {
            thread_id: "thread-1".to_string(),
            created_at: 1,
            updated_at: 2,
            status: "idle".to_string(),
            path: None,
            turn_ids: vec!["turn-a".to_string()],
            latest_turn_id: Some("turn-a".to_string()),
            latest_turn_status: None,
        };
        let mut responses = BTreeMap::new();
        responses.insert("thread-1".to_string(), snapshot.clone());
        let store = SharedSessionStore::new(StubRuntime { responses });

        let visible = store
            .ensure_cross_identity_visibility(
                &identity("Source", "/shared/sessions"),
                &identity("Target", "/shared/sessions"),
                "thread-1",
            )
            .unwrap();
        assert_eq!(visible, snapshot);
    }

    #[test]
    fn rejects_mismatched_shared_roots() {
        let snapshot = ThreadSnapshot {
            thread_id: "thread-1".to_string(),
            created_at: 1,
            updated_at: 2,
            status: "idle".to_string(),
            path: None,
            turn_ids: vec![],
            latest_turn_id: None,
            latest_turn_status: None,
        };
        let mut responses = BTreeMap::new();
        responses.insert("thread-1".to_string(), snapshot);
        let store = SharedSessionStore::new(StubRuntime { responses });

        let error = store
            .ensure_cross_identity_visibility(
                &identity("Source", "/shared/a"),
                &identity("Target", "/shared/b"),
                "thread-1",
            )
            .unwrap_err();
        assert!(error.to_string().contains("shared sessions root mismatch"));
    }
}
