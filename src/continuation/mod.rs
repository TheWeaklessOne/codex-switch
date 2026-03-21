use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::codex_rpc::ThreadRuntime;
use crate::domain::checkpoint::{CheckpointMode, TaskCheckpoint};
use crate::domain::identity::CodexIdentity;
use crate::domain::selection::{SelectionMode, SelectionStateRecord};
use crate::domain::thread::{ThreadLeaseRecord, ThreadSnapshot};
use crate::error::{AppError, Result};
use crate::handoff::HandoffService;
use crate::identity_selection::IdentitySelectionService;
use crate::launcher::{CodexLauncher, LaunchOutcome};
use crate::storage::checkpoint_store::TaskCheckpointStore;
use crate::storage::registry_store::RegistryStore;
use crate::storage::selection_store::SelectionStore;
use crate::thread_leases::ThreadLeaseManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinueMode {
    ResumeSameThread,
    ResumeViaCheckpoint,
}

impl ContinueMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResumeSameThread => "resume_same_thread",
            Self::ResumeViaCheckpoint => "resume_via_checkpoint",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContinueThreadRequest {
    pub thread_id: String,
    pub from_identity_name: Option<String>,
    pub to_identity_name: String,
    pub reason: String,
    pub target_selection_mode: SelectionMode,
    pub selection_reason: Option<String>,
    pub launch_after_switch: bool,
    pub extra_resume_args: Vec<OsString>,
}

#[derive(Debug, Clone)]
pub struct ContinueThreadResult {
    pub source_identity: CodexIdentity,
    pub target_identity: CodexIdentity,
    pub mode: ContinueMode,
    pub checkpoint: TaskCheckpoint,
    pub checkpoint_path: PathBuf,
    pub lease: ThreadLeaseRecord,
    pub baseline_snapshot: ThreadSnapshot,
    pub target_snapshot: Option<ThreadSnapshot>,
    pub launch: Option<LaunchOutcome>,
}

#[derive(Debug, Clone)]
pub struct ContinueService<S, R, SS, CS> {
    base_root: PathBuf,
    registry_store: S,
    runtime: R,
    selection_store: SS,
    checkpoint_store: CS,
    launcher: CodexLauncher,
}

#[derive(Debug)]
struct CheckpointFallbackContext<S, SS> {
    request: ContinueThreadRequest,
    selection_service: IdentitySelectionService<SS, S>,
    original_selection: SelectionStateRecord,
    lease_manager: ThreadLeaseManager<crate::storage::thread_lease_store::JsonThreadLeaseStore>,
    source_identity: CodexIdentity,
    target_identity: CodexIdentity,
    lease: ThreadLeaseRecord,
    baseline_snapshot: ThreadSnapshot,
    fallback_reason: String,
}

struct SameThreadContinueContext<S, R, SS> {
    request: ContinueThreadRequest,
    selection_service: IdentitySelectionService<SS, S>,
    original_selection: SelectionStateRecord,
    handoff_service: HandoffService<S, R>,
    source_identity: CodexIdentity,
    target_identity: CodexIdentity,
    previous_lease: ThreadLeaseRecord,
    preparation: crate::handoff::HandoffPreparation,
}

struct SameThreadRollbackContext<'a, S, R> {
    handoff_service: &'a HandoffService<S, R>,
    thread_id: &'a str,
    current_token: &'a str,
    previous_lease: &'a ThreadLeaseRecord,
    baseline_snapshot: &'a ThreadSnapshot,
    original_selection: Option<&'a SelectionStateRecord>,
    checkpoint_id: Option<&'a str>,
}

struct CheckpointFallbackRollbackContext<'a> {
    lease_manager: &'a ThreadLeaseManager<crate::storage::thread_lease_store::JsonThreadLeaseStore>,
    thread_id: &'a str,
    current_token: &'a str,
    previous_lease: &'a ThreadLeaseRecord,
    original_selection: Option<&'a SelectionStateRecord>,
    checkpoint_id: Option<&'a str>,
}

impl<S, R, SS, CS> ContinueService<S, R, SS, CS> {
    pub fn new(
        base_root: &Path,
        registry_store: S,
        runtime: R,
        selection_store: SS,
        checkpoint_store: CS,
    ) -> Self {
        Self {
            base_root: base_root.to_path_buf(),
            registry_store,
            runtime,
            selection_store,
            checkpoint_store,
            launcher: CodexLauncher,
        }
    }
}

impl<S, R, SS, CS> ContinueService<S, R, SS, CS>
where
    S: RegistryStore + Clone,
    R: ThreadRuntime + Clone,
    SS: SelectionStore + Clone,
    CS: TaskCheckpointStore,
{
    pub fn continue_thread(&self, request: ContinueThreadRequest) -> Result<ContinueThreadResult> {
        let selection_service = IdentitySelectionService::new(
            self.selection_store.clone(),
            self.registry_store.clone(),
        );
        let original_selection = self.selection_store.load()?;
        let source_identity = match request.from_identity_name.as_deref() {
            Some(identity_name) => selection_service.resolve_by_name(identity_name)?,
            None => selection_service.require_current()?.identity,
        };
        let target_identity = selection_service.resolve_by_name(&request.to_identity_name)?;
        let handoff_service = HandoffService::new(
            &self.base_root,
            self.registry_store.clone(),
            self.runtime.clone(),
        );
        let lease_manager = ThreadLeaseManager::with_default_locking(&self.base_root);
        let lease = lease_manager.acquire(&request.thread_id, &source_identity.id)?;
        let baseline_snapshot =
            handoff_service.inspect_thread(&source_identity.display_name, &request.thread_id)?;

        match handoff_service.prepare_handoff(
            &request.thread_id,
            &source_identity.display_name,
            &target_identity.display_name,
            &lease.lease_token,
            &request.reason,
        ) {
            Ok(preparation) => self.finish_same_thread_continue(SameThreadContinueContext {
                request,
                selection_service,
                original_selection,
                handoff_service,
                source_identity,
                target_identity,
                previous_lease: lease,
                preparation,
            }),
            Err(error) if is_checkpoint_fallback_error(&error) => {
                self.finish_checkpoint_fallback(CheckpointFallbackContext {
                    request,
                    selection_service,
                    original_selection,
                    lease_manager,
                    source_identity,
                    target_identity,
                    lease,
                    baseline_snapshot,
                    fallback_reason: error.to_string(),
                })
            }
            Err(error) => Err(error),
        }
    }

    fn finish_same_thread_continue(
        &self,
        context: SameThreadContinueContext<S, R, SS>,
    ) -> Result<ContinueThreadResult> {
        let SameThreadContinueContext {
            request,
            selection_service,
            original_selection,
            handoff_service,
            source_identity,
            target_identity,
            previous_lease,
            preparation,
        } = context;
        let accepted = handoff_service
            .accept_handoff(
                &request.thread_id,
                &target_identity.display_name,
                &preparation.lease.lease_token,
            )
            .map_err(|error| {
                self.rollback_same_thread_state(
                    "continue_thread.accept_handoff",
                    error,
                    SameThreadRollbackContext {
                        handoff_service: &handoff_service,
                        thread_id: &request.thread_id,
                        current_token: &preparation.lease.lease_token,
                        previous_lease: &previous_lease,
                        baseline_snapshot: &preparation.baseline_snapshot,
                        original_selection: None,
                        checkpoint_id: None,
                    },
                )
            })?;
        let checkpoint = TaskCheckpoint::new(
            &preparation.baseline_snapshot,
            source_identity.id.clone(),
            target_identity.id.clone(),
            CheckpointMode::ResumeSameThread,
            &request.reason,
            None,
        )?;
        let checkpoint_path = self.checkpoint_store.save(&checkpoint).map_err(|error| {
            self.rollback_same_thread_state(
                "continue_thread.save_checkpoint",
                error,
                SameThreadRollbackContext {
                    handoff_service: &handoff_service,
                    thread_id: &request.thread_id,
                    current_token: &accepted.lease.lease_token,
                    previous_lease: &previous_lease,
                    baseline_snapshot: &preparation.baseline_snapshot,
                    original_selection: None,
                    checkpoint_id: None,
                },
            )
        })?;
        let _ = store_selected_identity(
            &selection_service,
            target_identity.clone(),
            request.target_selection_mode,
            request
                .selection_reason
                .as_deref()
                .unwrap_or("switch and continue"),
        )
        .map_err(|error| {
            self.rollback_same_thread_state(
                "continue_thread.store_selection",
                error,
                SameThreadRollbackContext {
                    handoff_service: &handoff_service,
                    thread_id: &request.thread_id,
                    current_token: &accepted.lease.lease_token,
                    previous_lease: &previous_lease,
                    baseline_snapshot: &preparation.baseline_snapshot,
                    original_selection: Some(&original_selection),
                    checkpoint_id: Some(&checkpoint.id),
                },
            )
        })?;
        let launch = if request.launch_after_switch {
            Some(
                self.launcher
                    .launch_resume(
                        &target_identity,
                        &request.thread_id,
                        &request.extra_resume_args,
                    )
                    .map_err(|error| {
                        self.rollback_same_thread_state(
                            "continue_thread.launch_resume",
                            error,
                            SameThreadRollbackContext {
                                handoff_service: &handoff_service,
                                thread_id: &request.thread_id,
                                current_token: &accepted.lease.lease_token,
                                previous_lease: &previous_lease,
                                baseline_snapshot: &preparation.baseline_snapshot,
                                original_selection: Some(&original_selection),
                                checkpoint_id: Some(&checkpoint.id),
                            },
                        )
                    })?,
            )
        } else {
            None
        };

        Ok(ContinueThreadResult {
            source_identity,
            target_identity,
            mode: ContinueMode::ResumeSameThread,
            checkpoint,
            checkpoint_path,
            lease: accepted.lease,
            baseline_snapshot: preparation.baseline_snapshot,
            target_snapshot: Some(preparation.target_snapshot),
            launch,
        })
    }

    fn finish_checkpoint_fallback(
        &self,
        context: CheckpointFallbackContext<S, SS>,
    ) -> Result<ContinueThreadResult> {
        let CheckpointFallbackContext {
            request,
            selection_service,
            original_selection,
            lease_manager,
            source_identity,
            target_identity,
            lease,
            baseline_snapshot,
            fallback_reason,
        } = context;
        let released =
            lease_manager.release(&request.thread_id, &source_identity.id, &lease.lease_token)?;
        let checkpoint = TaskCheckpoint::new(
            &baseline_snapshot,
            source_identity.id.clone(),
            target_identity.id.clone(),
            CheckpointMode::ResumeViaCheckpoint,
            &request.reason,
            Some(fallback_reason),
        )?;
        let checkpoint_path = self.checkpoint_store.save(&checkpoint).map_err(|error| {
            self.rollback_checkpoint_fallback_state(
                "continue_thread.save_checkpoint_fallback",
                error,
                CheckpointFallbackRollbackContext {
                    lease_manager: &lease_manager,
                    thread_id: &request.thread_id,
                    current_token: &released.lease_token,
                    previous_lease: &lease,
                    original_selection: None,
                    checkpoint_id: None,
                },
            )
        })?;
        let _ = store_selected_identity(
            &selection_service,
            target_identity.clone(),
            request.target_selection_mode,
            request
                .selection_reason
                .as_deref()
                .unwrap_or("switch and checkpoint fallback"),
        )
        .map_err(|error| {
            self.rollback_checkpoint_fallback_state(
                "continue_thread.store_selection_fallback",
                error,
                CheckpointFallbackRollbackContext {
                    lease_manager: &lease_manager,
                    thread_id: &request.thread_id,
                    current_token: &released.lease_token,
                    previous_lease: &lease,
                    original_selection: Some(&original_selection),
                    checkpoint_id: Some(&checkpoint.id),
                },
            )
        })?;

        Ok(ContinueThreadResult {
            source_identity,
            target_identity,
            mode: ContinueMode::ResumeViaCheckpoint,
            checkpoint,
            checkpoint_path,
            lease: released,
            baseline_snapshot,
            target_snapshot: None,
            launch: None,
        })
    }

    fn rollback_same_thread_state(
        &self,
        operation: &str,
        primary: AppError,
        context: SameThreadRollbackContext<'_, S, R>,
    ) -> AppError {
        let SameThreadRollbackContext {
            handoff_service,
            thread_id,
            current_token,
            previous_lease,
            baseline_snapshot,
            original_selection,
            checkpoint_id,
        } = context;
        let mut rollback_errors = Vec::new();

        if let Err(error) = handoff_service.restore_source_lease(
            thread_id,
            current_token,
            previous_lease,
            baseline_snapshot,
        ) {
            rollback_errors.push(format!("restore source lease: {error}"));
        }

        if let Some(selection) = original_selection {
            if let Err(error) = self.selection_store.save(selection) {
                rollback_errors.push(format!("restore selection: {error}"));
            }
        }

        if let Some(checkpoint_id) = checkpoint_id {
            if let Err(error) = self.checkpoint_store.delete(checkpoint_id) {
                rollback_errors.push(format!("delete checkpoint {checkpoint_id}: {error}"));
            }
        }

        if rollback_errors.is_empty() {
            primary
        } else {
            AppError::RollbackFailed {
                operation: operation.to_string(),
                primary: primary.to_string(),
                rollback: rollback_errors.join("; "),
            }
        }
    }

    fn rollback_checkpoint_fallback_state(
        &self,
        operation: &str,
        primary: AppError,
        context: CheckpointFallbackRollbackContext<'_>,
    ) -> AppError {
        let CheckpointFallbackRollbackContext {
            lease_manager,
            thread_id,
            current_token,
            previous_lease,
            original_selection,
            checkpoint_id,
        } = context;
        let mut rollback_errors = Vec::new();

        if let Err(error) = lease_manager.restore(thread_id, Some(current_token), previous_lease) {
            rollback_errors.push(format!("restore source lease: {error}"));
        }

        if let Some(selection) = original_selection {
            if let Err(error) = self.selection_store.save(selection) {
                rollback_errors.push(format!("restore selection: {error}"));
            }
        }

        if let Some(checkpoint_id) = checkpoint_id {
            if let Err(error) = self.checkpoint_store.delete(checkpoint_id) {
                rollback_errors.push(format!("delete checkpoint {checkpoint_id}: {error}"));
            }
        }

        if rollback_errors.is_empty() {
            primary
        } else {
            AppError::RollbackFailed {
                operation: operation.to_string(),
                primary: primary.to_string(),
                rollback: rollback_errors.join("; "),
            }
        }
    }
}

fn store_selected_identity<SS, RS>(
    selection_service: &IdentitySelectionService<SS, RS>,
    identity: CodexIdentity,
    mode: SelectionMode,
    reason: &str,
) -> Result<crate::identity_selection::CurrentIdentitySelection>
where
    SS: SelectionStore,
    RS: RegistryStore,
{
    match mode {
        SelectionMode::Manual => selection_service.store_manual(identity, Some(reason)),
        SelectionMode::Automatic => selection_service.store_automatic(identity, Some(reason)),
    }
}

fn is_checkpoint_fallback_error(error: &AppError) -> bool {
    matches!(
        error,
        AppError::SharedSessionsRootMismatch { .. }
            | AppError::ThreadHistoryNotShared { .. }
            | AppError::RpcTimeout { .. }
            | AppError::RpcServer { .. }
            | AppError::MissingRpcResult { .. }
            | AppError::AppServerExited { .. }
            | AppError::RpcPayloadDecode { .. }
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use super::{ContinueMode, ContinueService, ContinueThreadRequest};
    use crate::bootstrap::BootstrapIdentityRequest;
    use crate::codex_rpc::ThreadRuntime;
    use crate::domain::identity::{AuthMode, CodexIdentity};
    use crate::domain::selection::{SelectedIdentityState, SelectionMode, SelectionStateRecord};
    use crate::domain::thread::{
        ThreadLeaseState, ThreadSnapshot, TrackedTurnStateStatus, TurnStatus,
    };
    use crate::error::{AppError, Result};
    use crate::handoff::HandoffService;
    use crate::identity_registry::IdentityRegistryService;
    use crate::identity_selection::IdentitySelectionService;
    use crate::storage::checkpoint_store::{JsonTaskCheckpointStore, TaskCheckpointStore};
    use crate::storage::registry_store::JsonRegistryStore;
    use crate::storage::selection_store::{JsonSelectionStore, SelectionStore};
    use crate::thread_leases::ThreadLeaseManager;

    #[derive(Debug, Clone)]
    struct StubRuntime {
        snapshots: Arc<Mutex<BTreeMap<(String, String), ThreadSnapshot>>>,
    }

    impl ThreadRuntime for StubRuntime {
        fn read_thread(
            &self,
            identity: &CodexIdentity,
            thread_id: &str,
        ) -> crate::error::Result<ThreadSnapshot> {
            Ok(self
                .snapshots
                .lock()
                .unwrap()
                .get(&(identity.id.to_string(), thread_id.to_string()))
                .unwrap()
                .clone())
        }

        fn resume_thread(
            &self,
            identity: &CodexIdentity,
            thread_id: &str,
        ) -> crate::error::Result<ThreadSnapshot> {
            self.read_thread(identity, thread_id)
        }
    }

    #[derive(Debug, Clone)]
    struct StubSelectionStore {
        state: Arc<Mutex<SelectionStateRecord>>,
        fail_next_save: Arc<Mutex<bool>>,
    }

    impl StubSelectionStore {
        fn new(state: SelectionStateRecord) -> Self {
            Self {
                state: Arc::new(Mutex::new(state)),
                fail_next_save: Arc::new(Mutex::new(false)),
            }
        }

        fn fail_once(&self) {
            *self.fail_next_save.lock().unwrap() = true;
        }
    }

    impl SelectionStore for StubSelectionStore {
        fn load(&self) -> Result<SelectionStateRecord> {
            Ok(self.state.lock().unwrap().clone())
        }

        fn save(&self, record: &SelectionStateRecord) -> Result<()> {
            let mut fail_next = self.fail_next_save.lock().unwrap();
            if *fail_next {
                *fail_next = false;
                return Err(AppError::Io(std::io::Error::other("selection save failed")));
            }

            *self.state.lock().unwrap() = record.clone();
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct FailingCheckpointStore {
        saved_ids: Arc<Mutex<Vec<String>>>,
    }

    impl FailingCheckpointStore {
        fn new() -> Self {
            Self {
                saved_ids: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl TaskCheckpointStore for FailingCheckpointStore {
        fn save(&self, checkpoint: &crate::domain::checkpoint::TaskCheckpoint) -> Result<PathBuf> {
            self.saved_ids.lock().unwrap().push(checkpoint.id.clone());
            Err(AppError::Io(std::io::Error::other(
                "checkpoint save failed",
            )))
        }

        fn load(
            &self,
            _checkpoint_id: &str,
        ) -> Result<Option<crate::domain::checkpoint::TaskCheckpoint>> {
            Ok(None)
        }

        fn delete(&self, checkpoint_id: &str) -> Result<()> {
            self.saved_ids
                .lock()
                .unwrap()
                .retain(|saved_id| saved_id != checkpoint_id);
            Ok(())
        }

        fn latest_for_thread(
            &self,
            _thread_id: &str,
        ) -> Result<Option<(crate::domain::checkpoint::TaskCheckpoint, PathBuf)>> {
            Ok(None)
        }
    }

    fn baseline_snapshot() -> ThreadSnapshot {
        ThreadSnapshot {
            thread_id: "thread-1".to_string(),
            created_at: 1,
            updated_at: 2,
            status: "idle".to_string(),
            path: None,
            turn_ids: vec!["turn-a".to_string()],
            latest_turn_id: Some("turn-a".to_string()),
            latest_turn_status: Some(TurnStatus::Completed),
        }
    }

    #[test]
    fn falls_back_to_checkpoint_when_shared_history_does_not_match() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let selection_store = JsonSelectionStore::new(temp.path());
        let checkpoint_store = JsonTaskCheckpointStore::new(temp.path());
        let registry_service = IdentityRegistryService::new(registry_store.clone());

        for name in ["Source", "Target"] {
            registry_service
                .register_identity(BootstrapIdentityRequest {
                    display_name: name.to_string(),
                    base_root: temp.path().to_path_buf(),
                    auth_mode: AuthMode::Chatgpt,
                    home_override: None,
                    import_auth_from_home: None,
                    overwrite_config: false,
                    api_key_env_var: None,
                    forced_chatgpt_workspace_id: None,
                })
                .unwrap();
        }

        let selection =
            IdentitySelectionService::new(selection_store.clone(), registry_store.clone());
        let _ = selection.select_manual("Source", Some("initial")).unwrap();

        let source = registry_service
            .list_identities()
            .unwrap()
            .into_iter()
            .find(|identity| identity.id.as_str() == "source")
            .unwrap();
        let target = registry_service
            .list_identities()
            .unwrap()
            .into_iter()
            .find(|identity| identity.id.as_str() == "target")
            .unwrap();

        let mut snapshots = BTreeMap::new();
        snapshots.insert(
            (source.id.to_string(), "thread-1".to_string()),
            ThreadSnapshot {
                thread_id: "thread-1".to_string(),
                created_at: 1,
                updated_at: 2,
                status: "idle".to_string(),
                path: None,
                turn_ids: vec!["turn-a".to_string()],
                latest_turn_id: Some("turn-a".to_string()),
                latest_turn_status: Some(TurnStatus::Completed),
            },
        );
        snapshots.insert(
            (target.id.to_string(), "thread-1".to_string()),
            ThreadSnapshot {
                thread_id: "thread-1".to_string(),
                created_at: 1,
                updated_at: 2,
                status: "idle".to_string(),
                path: None,
                turn_ids: vec![],
                latest_turn_id: None,
                latest_turn_status: None,
            },
        );
        let service = ContinueService::new(
            temp.path(),
            registry_store,
            StubRuntime {
                snapshots: Arc::new(Mutex::new(snapshots)),
            },
            selection_store,
            checkpoint_store.clone(),
        );

        let result = service
            .continue_thread(ContinueThreadRequest {
                thread_id: "thread-1".to_string(),
                from_identity_name: None,
                to_identity_name: "Target".to_string(),
                reason: "quota".to_string(),
                target_selection_mode: SelectionMode::Manual,
                selection_reason: Some("manual switch and checkpoint fallback".to_string()),
                launch_after_switch: false,
                extra_resume_args: Vec::new(),
            })
            .unwrap();

        assert_eq!(result.mode, ContinueMode::ResumeViaCheckpoint);
        let checkpoint = checkpoint_store
            .latest_for_thread("thread-1")
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(checkpoint.mode.as_str(), "resume_via_checkpoint");
    }

    #[test]
    fn restores_source_lease_when_same_thread_selection_update_fails() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let checkpoint_store = JsonTaskCheckpointStore::new(temp.path());
        let registry_service = IdentityRegistryService::new(registry_store.clone());

        for name in ["Source", "Target"] {
            registry_service
                .register_identity(BootstrapIdentityRequest {
                    display_name: name.to_string(),
                    base_root: temp.path().to_path_buf(),
                    auth_mode: AuthMode::Chatgpt,
                    home_override: None,
                    import_auth_from_home: None,
                    overwrite_config: false,
                    api_key_env_var: None,
                    forced_chatgpt_workspace_id: None,
                })
                .unwrap();
        }

        let source = registry_service
            .list_identities()
            .unwrap()
            .into_iter()
            .find(|identity| identity.id.as_str() == "source")
            .unwrap();
        let target = registry_service
            .list_identities()
            .unwrap()
            .into_iter()
            .find(|identity| identity.id.as_str() == "target")
            .unwrap();
        let selection_store = StubSelectionStore::new(SelectionStateRecord {
            version: crate::domain::selection::SELECTION_STATE_VERSION,
            current: Some(SelectedIdentityState {
                identity_id: source.id.clone(),
                mode: SelectionMode::Manual,
                reason: Some("initial".to_string()),
                updated_at: 1,
            }),
        });
        selection_store.fail_once();

        let baseline = baseline_snapshot();
        let mut snapshots = BTreeMap::new();
        snapshots.insert(
            (source.id.to_string(), "thread-1".to_string()),
            baseline.clone(),
        );
        snapshots.insert(
            (target.id.to_string(), "thread-1".to_string()),
            baseline.clone(),
        );
        let runtime = StubRuntime {
            snapshots: Arc::new(Mutex::new(snapshots)),
        };
        let service = ContinueService::new(
            temp.path(),
            registry_store.clone(),
            runtime.clone(),
            selection_store.clone(),
            checkpoint_store.clone(),
        );

        let error = service
            .continue_thread(ContinueThreadRequest {
                thread_id: "thread-1".to_string(),
                from_identity_name: Some("Source".to_string()),
                to_identity_name: "Target".to_string(),
                reason: "quota".to_string(),
                target_selection_mode: SelectionMode::Manual,
                selection_reason: Some("manual switch".to_string()),
                launch_after_switch: false,
                extra_resume_args: Vec::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("selection save failed"));

        let current = selection_store.load().unwrap();
        assert_eq!(
            current
                .current
                .as_ref()
                .map(|selection| selection.identity_id.as_str()),
            Some("source")
        );
        assert!(checkpoint_store
            .latest_for_thread("thread-1")
            .unwrap()
            .is_none());

        let handoff_service = HandoffService::new(temp.path(), registry_store.clone(), runtime);
        let lease = handoff_service.read_lease("thread-1").unwrap().unwrap();
        assert_eq!(lease.owner_identity_id.as_str(), "source");
        assert_eq!(lease.lease_state, ThreadLeaseState::Active);

        let tracked = handoff_service.tracked_state("thread-1").unwrap().unwrap();
        assert_eq!(tracked.state, TrackedTurnStateStatus::Active);
        assert_eq!(
            tracked.owner_identity_id.as_ref().map(|id| id.as_str()),
            Some("source")
        );
    }

    #[test]
    fn restores_source_lease_when_checkpoint_fallback_save_fails() {
        let temp = tempdir().unwrap();
        let registry_store = JsonRegistryStore::new(temp.path());
        let selection_store = JsonSelectionStore::new(temp.path());
        let checkpoint_store = FailingCheckpointStore::new();
        let registry_service = IdentityRegistryService::new(registry_store.clone());

        for name in ["Source", "Target"] {
            registry_service
                .register_identity(BootstrapIdentityRequest {
                    display_name: name.to_string(),
                    base_root: temp.path().to_path_buf(),
                    auth_mode: AuthMode::Chatgpt,
                    home_override: None,
                    import_auth_from_home: None,
                    overwrite_config: false,
                    api_key_env_var: None,
                    forced_chatgpt_workspace_id: None,
                })
                .unwrap();
        }

        let selection =
            IdentitySelectionService::new(selection_store.clone(), registry_store.clone());
        let _ = selection.select_manual("Source", Some("initial")).unwrap();

        let source = registry_service
            .list_identities()
            .unwrap()
            .into_iter()
            .find(|identity| identity.id.as_str() == "source")
            .unwrap();
        let target = registry_service
            .list_identities()
            .unwrap()
            .into_iter()
            .find(|identity| identity.id.as_str() == "target")
            .unwrap();

        let mut snapshots = BTreeMap::new();
        snapshots.insert(
            (source.id.to_string(), "thread-1".to_string()),
            baseline_snapshot(),
        );
        snapshots.insert(
            (target.id.to_string(), "thread-1".to_string()),
            ThreadSnapshot {
                thread_id: "thread-1".to_string(),
                created_at: 1,
                updated_at: 2,
                status: "idle".to_string(),
                path: None,
                turn_ids: Vec::new(),
                latest_turn_id: None,
                latest_turn_status: None,
            },
        );
        let runtime = StubRuntime {
            snapshots: Arc::new(Mutex::new(snapshots)),
        };
        let service = ContinueService::new(
            temp.path(),
            registry_store.clone(),
            runtime,
            selection_store.clone(),
            checkpoint_store,
        );

        let error = service
            .continue_thread(ContinueThreadRequest {
                thread_id: "thread-1".to_string(),
                from_identity_name: Some("Source".to_string()),
                to_identity_name: "Target".to_string(),
                reason: "quota".to_string(),
                target_selection_mode: SelectionMode::Manual,
                selection_reason: Some("fallback".to_string()),
                launch_after_switch: false,
                extra_resume_args: Vec::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("checkpoint save failed"));

        let current = selection_store.load().unwrap();
        assert_eq!(
            current
                .current
                .as_ref()
                .map(|selection| selection.identity_id.as_str()),
            Some("source")
        );

        let lease_manager = ThreadLeaseManager::with_default_locking(temp.path());
        let lease = lease_manager.read("thread-1").unwrap().unwrap();
        assert_eq!(lease.owner_identity_id.as_str(), "source");
        assert_eq!(lease.lease_state, ThreadLeaseState::Active);
    }
}
