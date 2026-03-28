use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use serde_json::json;

use crate::codex_rpc::CodexAppServerVerifier;
use crate::domain::health::IdentityHealthRecord;
use crate::domain::identity::{current_timestamp, CodexIdentity, IdentityId};
use crate::error::{AppError, Result};
use crate::identity_cleanup::{
    auto_remove_deactivated_workspace_identities, ManagedIdentityRemovalService,
};
use crate::identity_selector::{IdentityEvaluation, IdentitySelector};
use crate::quota_status::{IdentityStatusReport, QuotaStatusService};
use crate::storage::health_store::{IdentityHealthStore, JsonIdentityHealthStore};
use crate::storage::policy_store::{JsonSelectionPolicyStore, SelectionPolicyStore};
use crate::storage::quota_store::JsonQuotaStore;
use crate::storage::registry_store::{JsonRegistryStore, RegistryStore};
use crate::storage::selection_store::JsonSelectionStore;
use crate::task_orchestration::config::{SchedulerControlRecord, SchedulerSettings};
use crate::task_orchestration::domain::*;
use crate::task_orchestration::store::{
    AssignmentClaim, QueuedRunContext, SchedulerHealthSnapshot, SchedulerStore,
};
use crate::task_orchestration::worktree::WorktreeManager;

#[derive(Debug, Clone)]
pub struct DispatchOutcome {
    pub task_id: TaskId,
    pub run_id: TaskRunId,
    pub identity_id: IdentityId,
    pub launch_mode: LaunchMode,
    pub worktree_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SchedulerHealthReport {
    pub snapshot: SchedulerHealthSnapshot,
    pub active_leases: Vec<AccountLeaseRecord>,
    pub control: SchedulerControlRecord,
    pub settings: SchedulerSettings,
}

#[derive(Debug, Clone)]
pub struct SchedulerDaemon {
    base_root: PathBuf,
    settings: SchedulerSettings,
    worker_program: PathBuf,
    worktree_manager: WorktreeManager,
}

impl SchedulerDaemon {
    pub fn new(base_root: &Path, settings: SchedulerSettings) -> Result<Self> {
        Ok(Self {
            base_root: base_root.to_path_buf(),
            settings,
            worker_program: std::env::current_exe()?,
            worktree_manager: WorktreeManager,
        })
    }

    pub fn with_worker_program(
        base_root: &Path,
        settings: SchedulerSettings,
        worker_program: PathBuf,
    ) -> Self {
        Self {
            base_root: base_root.to_path_buf(),
            settings,
            worker_program,
            worktree_manager: WorktreeManager,
        }
    }

    pub fn tick_once(&self) -> Result<Vec<DispatchOutcome>> {
        let owner_id = scheduler_owner_id();
        let mut store = SchedulerStore::open(&self.base_root)?;
        store.acquire_scheduler_lock(&owner_id, self.settings.scheduler_lock_ttl)?;
        let result = self.tick_iteration(&mut store, true, &owner_id);
        let release_result = store.release_scheduler_lock(&owner_id);
        match (result, release_result) {
            (Ok(outcomes), Ok(())) => Ok(outcomes),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    pub fn run_loop(&self) -> Result<()> {
        let owner_id = scheduler_owner_id();
        let mut store = SchedulerStore::open(&self.base_root)?;
        store.acquire_scheduler_lock(&owner_id, self.settings.scheduler_lock_ttl)?;
        loop {
            self.tick_iteration(&mut store, false, &owner_id)?;
            thread::sleep(self.settings.scheduler_poll_interval);
        }
    }

    pub fn health(&self) -> Result<SchedulerHealthReport> {
        let now = current_timestamp()?;
        let store = SchedulerStore::open(&self.base_root)?;
        let registry = JsonRegistryStore::new(&self.base_root).load()?;
        let registry_ids = registry.identities.keys().cloned().collect::<BTreeSet<_>>();
        let snapshot = store.scheduler_health(&registry_ids, now)?;
        let active_leases = store.active_account_leases()?;
        let control = store.scheduler_control()?;
        Ok(SchedulerHealthReport {
            snapshot,
            active_leases,
            control,
            settings: self.settings.clone(),
        })
    }

    pub fn gc(&self) -> Result<Vec<PathBuf>> {
        let owner_id = scheduler_owner_id();
        let mut store = SchedulerStore::open(&self.base_root)?;
        store.acquire_scheduler_lock(&owner_id, self.settings.scheduler_lock_ttl)?;
        let result = self.gc_inner(&mut store, current_timestamp()?, true, Some(&owner_id));
        let release_result = store.release_scheduler_lock(&owner_id);
        match (result, release_result) {
            (Ok(removed), Ok(())) => Ok(removed),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    fn tick_iteration(
        &self,
        store: &mut SchedulerStore,
        force_maintenance: bool,
        owner_id: &str,
    ) -> Result<Vec<DispatchOutcome>> {
        let now = current_timestamp()?;
        let control = store.scheduler_control()?;
        self.heartbeat_lock(store, owner_id)?;
        self.run_maintenance(store, &control, now, force_maintenance, owner_id)?;
        self.heartbeat_lock(store, owner_id)?;
        self.tick_inner(store, &control, owner_id)
    }

    fn run_maintenance(
        &self,
        store: &mut SchedulerStore,
        control: &SchedulerControlRecord,
        now: i64,
        force: bool,
        owner_id: &str,
    ) -> Result<()> {
        if should_run_interval(
            control.last_quota_refresh_at,
            now,
            self.settings.quota_refresh_interval,
            force,
        ) {
            self.heartbeat_lock(store, owner_id)?;
            self.refresh_quota(store, now)?;
        }
        if should_run_interval(control.last_gc_at, now, self.settings.gc_interval, force) {
            self.heartbeat_lock(store, owner_id)?;
            let _ = self.gc_inner(store, now, false, Some(owner_id))?;
        }
        Ok(())
    }

    fn tick_inner(
        &self,
        store: &mut SchedulerStore,
        control: &SchedulerControlRecord,
        owner_id: &str,
    ) -> Result<Vec<DispatchOutcome>> {
        let now = current_timestamp()?;
        let _ = store.reconcile_orphaned_runs(now, &self.settings)?;
        self.heartbeat_lock(store, owner_id)?;
        if !control.scheduler_v1_enabled {
            return Ok(Vec::new());
        }
        let context = load_selector_context(&self.base_root)?;
        let queued = store.queued_runs()?;
        let leases = store.active_account_leases()?;
        let runtimes = store.account_runtime()?;
        let runtime_map = runtimes
            .into_iter()
            .map(|runtime| (runtime.identity_id.clone(), runtime))
            .collect::<BTreeMap<_, _>>();
        let lease_counts = lease_count_map(&leases);

        let mut dispatched = Vec::new();
        for queued_run in queued.into_iter().take(self.settings.dispatch_batch_size) {
            self.heartbeat_lock(store, owner_id)?;
            if let Some(outcome) =
                self.dispatch_run(store, &context, &lease_counts, &runtime_map, queued_run)?
            {
                dispatched.push(outcome);
            }
        }
        Ok(dispatched)
    }

    fn heartbeat_lock(&self, store: &mut SchedulerStore, owner_id: &str) -> Result<()> {
        store.heartbeat_scheduler_lock(owner_id, self.settings.scheduler_lock_ttl)
    }

    fn refresh_quota(&self, store: &mut SchedulerStore, now: i64) -> Result<()> {
        let registry_store = JsonRegistryStore::new(&self.base_root);
        let quota_store = JsonQuotaStore::new(&self.base_root);
        let service = QuotaStatusService::new(registry_store, quota_store);
        match service.refresh_all(&CodexAppServerVerifier::default()) {
            Ok(reports) => {
                let remover = ManagedIdentityRemovalService::new(
                    JsonRegistryStore::new(&self.base_root),
                    JsonQuotaStore::new(&self.base_root),
                    JsonIdentityHealthStore::new(&self.base_root),
                    JsonSelectionStore::new(&self.base_root),
                );
                let sweep = auto_remove_deactivated_workspace_identities(reports, &remover);
                let mut errors = sweep
                    .reports
                    .iter()
                    .filter_map(|report| {
                        report
                            .refresh_error
                            .as_ref()
                            .map(|error| format!("{}: {error}", report.identity.id))
                    })
                    .collect::<Vec<_>>();
                errors.extend(sweep.notices.iter().map(|notice| notice.summary()));
                let error_summary = (!errors.is_empty()).then(|| errors.join("; "));
                store.record_quota_refresh_outcome(
                    now,
                    error_summary.as_deref(),
                    sweep.reports.len(),
                )?;
            }
            Err(error) => {
                let error_message = error.to_string();
                store.record_quota_refresh_outcome(now, Some(&error_message), 0)?;
            }
        }
        Ok(())
    }

    fn gc_inner(
        &self,
        store: &mut SchedulerStore,
        now: i64,
        strict: bool,
        owner_id: Option<&str>,
    ) -> Result<Vec<PathBuf>> {
        let candidates = store.gc_worktrees(now)?;
        let mut removed = Vec::new();
        let mut errors = Vec::new();
        for worktree in candidates {
            if let Some(owner_id) = owner_id {
                self.heartbeat_lock(store, owner_id)?;
            }
            let Some(reserved_worktree) =
                store.reserve_worktree_for_gc(&worktree.worktree_id, now)?
            else {
                continue;
            };
            let project = match store.get_project(reserved_worktree.project_id.as_str()) {
                Ok(project) => project,
                Err(error) => {
                    let _ = store.release_worktree_gc_reservation(
                        &reserved_worktree.worktree_id,
                        reserved_worktree.state,
                    );
                    errors.push(format!("{}: {error}", reserved_worktree.path.display()));
                    continue;
                }
            };
            match self
                .worktree_manager
                .cleanup(&project, &reserved_worktree.path)
            {
                Ok(()) => match store.delete_worktree(&reserved_worktree) {
                    Ok(true) => removed.push(reserved_worktree.path.clone()),
                    Ok(false) => errors.push(format!(
                        "{}: gc reservation lost before delete",
                        reserved_worktree.path.display()
                    )),
                    Err(error) => {
                        errors.push(format!("{}: {error}", reserved_worktree.path.display()))
                    }
                },
                Err(error) => {
                    let _ = store.release_worktree_gc_reservation(
                        &reserved_worktree.worktree_id,
                        reserved_worktree.state,
                    );
                    errors.push(format!("{}: {error}", reserved_worktree.path.display()));
                }
            }
        }
        let error_summary = (!errors.is_empty()).then(|| errors.join("; "));
        store.record_gc_outcome(now, removed.len(), error_summary.as_deref())?;
        if strict {
            if let Some(error_summary) = error_summary {
                return Err(AppError::InvalidSchedulerConfiguration {
                    message: format!("scheduler gc encountered errors: {error_summary}"),
                });
            }
        }
        Ok(removed)
    }

    fn dispatch_run(
        &self,
        store: &mut SchedulerStore,
        selector_context: &SelectorContext,
        lease_counts: &BTreeMap<IdentityId, u32>,
        runtime_map: &BTreeMap<IdentityId, AccountRuntimeRecord>,
        queued_run: QueuedRunContext,
    ) -> Result<Option<DispatchOutcome>> {
        let run = &queued_run.run;
        let task = &queued_run.task;
        let project = &queued_run.project;
        if matches!(run.run_kind, RunKind::FollowUp | RunKind::Retry)
            && task.status == TaskStatus::Running
        {
            return Ok(None);
        }
        let selection = select_assignment_candidate(
            &self.base_root,
            selector_context,
            lease_counts,
            runtime_map,
            &self.settings,
            &queued_run,
        );
        let Some(selected) = selection else {
            return Ok(None);
        };
        self.worktree_manager
            .ensure_base_directories(&self.base_root)?;
        let claim = AssignmentClaim {
            run_id: run.run_id.clone(),
            task_id: task.task_id.clone(),
            project_id: project.project_id.clone(),
            identity_id: selected.identity.id.clone(),
            worktree: selected.worktree.clone(),
            worker_owner_id: lease_owner_id(run.run_id.as_str()),
            launch_mode: selected.launch_mode,
            lineage_mode: selected.lineage_mode,
            reason: selected.reason.clone(),
            decision: DispatchDecisionRecord {
                decision_id: DispatchDecisionId::new(),
                run_id: run.run_id.clone(),
                decision_kind: match run.run_kind {
                    RunKind::Initial => DecisionKind::Dispatch,
                    RunKind::FollowUp => DecisionKind::FollowUp,
                    RunKind::Retry => DecisionKind::Retry,
                },
                selected_identity_id: Some(selected.identity.id.clone()),
                selected_worktree_id: Some(selected.worktree.worktree_id.clone()),
                lineage_mode: selected.lineage_mode,
                reason: selected.reason.clone(),
                candidates: selected.candidates.clone(),
                policy_snapshot_json: json!({
                    "max_active_runs_per_identity": self.settings.max_active_runs_per_identity,
                    "allow_oversubscribe_when_pool_full": self.settings.allow_oversubscribe_when_pool_full,
                    "task_id": task.task_id,
                    "run_id": run.run_id,
                }),
                created_at: current_timestamp()?,
            },
            lease_expires_at: current_timestamp()?
                + self.settings.worker_lease_ttl.as_secs() as i64,
        };
        if !store.claim_assignment(&claim, &self.settings)? {
            return Ok(None);
        }
        let worker_pid = match self.spawn_worker(run.run_id.as_str(), &claim.worker_owner_id) {
            Ok(worker_pid) => worker_pid,
            Err(error) => {
                let _ = store.rollback_assignment_after_spawn_failure(
                    run.run_id.as_str(),
                    &claim.worker_owner_id,
                    &error.to_string(),
                )?;
                return Ok(None);
            }
        };
        store.mark_worker_spawned(run.run_id.as_str(), &claim.worker_owner_id, worker_pid)?;
        Ok(Some(DispatchOutcome {
            task_id: task.task_id.clone(),
            run_id: run.run_id.clone(),
            identity_id: selected.identity.id,
            launch_mode: selected.launch_mode,
            worktree_path: selected.worktree.path,
        }))
    }

    fn spawn_worker(&self, run_id: &str, worker_owner_id: &str) -> Result<u32> {
        let mut command = Command::new(&self.worker_program);
        command
            .arg("scheduler")
            .arg("worker")
            .arg("--run-id")
            .arg(run_id)
            .arg("--lease-owner-id")
            .arg(worker_owner_id)
            .arg("--base-root")
            .arg(&self.base_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(unix)]
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command
            .spawn()
            .map_err(|error| AppError::WorkerSpawnFailed {
                run_id: run_id.to_string(),
                message: error.to_string(),
            })?;
        Ok(child.id())
    }
}

#[derive(Debug)]
struct SelectorContext {
    selector: IdentitySelector,
    reports: Vec<IdentityStatusReport>,
    health: IdentityHealthRecord,
}

#[derive(Debug, Clone)]
struct SelectedAssignment {
    identity: CodexIdentity,
    launch_mode: LaunchMode,
    lineage_mode: LineageMode,
    reason: String,
    candidates: Vec<CandidateAssessment>,
    worktree: WorktreeRecord,
}

fn load_selector_context(base_root: &Path) -> Result<SelectorContext> {
    let registry_store = JsonRegistryStore::new(base_root);
    let quota_store = JsonQuotaStore::new(base_root);
    let health_store = JsonIdentityHealthStore::new(base_root);
    let policy_store = JsonSelectionPolicyStore::new(base_root);
    let selector = IdentitySelector::new(policy_store.load()?.policy, current_timestamp()?);
    let reports = QuotaStatusService::new(registry_store.clone(), quota_store).cached_statuses()?;
    Ok(SelectorContext {
        selector,
        reports,
        health: health_store.load()?,
    })
}

fn should_run_interval(
    last_at: Option<i64>,
    now: i64,
    interval: std::time::Duration,
    force: bool,
) -> bool {
    if force {
        return true;
    }
    match last_at {
        Some(last_at) => now.saturating_sub(last_at) >= interval.as_secs() as i64,
        None => true,
    }
}

fn select_assignment_candidate(
    base_root: &Path,
    selector_context: &SelectorContext,
    lease_counts: &BTreeMap<IdentityId, u32>,
    runtime_map: &BTreeMap<IdentityId, AccountRuntimeRecord>,
    settings: &SchedulerSettings,
    queued_run: &QueuedRunContext,
) -> Option<SelectedAssignment> {
    let mut candidates = selector_context
        .reports
        .iter()
        .map(|report| {
            let evaluation = selector_context.selector.evaluate(
                &report.identity,
                report.quota_status.as_ref(),
                selector_context.health.identities.get(&report.identity.id),
            );
            build_candidate_assessment(
                report,
                &evaluation,
                lease_counts
                    .get(&report.identity.id)
                    .copied()
                    .unwrap_or_default(),
                runtime_map.get(&report.identity.id),
                queued_run,
                settings,
            )
        })
        .collect::<Vec<_>>();

    let selected_index = select_candidate_index(&candidates, queued_run)?;
    candidates[selected_index].selected = true;
    let identity = selector_context
        .reports
        .iter()
        .find(|report| report.identity.id == candidates[selected_index].identity_id)
        .map(|report| report.identity.clone())?;
    let (launch_mode, lineage_mode) = determine_launch_mode(&identity, queued_run);
    let worktree = queued_run
        .reusable_worktree
        .clone()
        .unwrap_or_else(|| WorktreeRecord {
            worktree_id: WorktreeId::new(),
            project_id: queued_run.project.project_id.clone(),
            task_id: queued_run.task.task_id.clone(),
            path: crate::storage::paths::task_worktree_run_path(
                base_root,
                queued_run.project.project_id.as_str(),
                queued_run.task.task_id.as_str(),
                queued_run.run.run_id.as_str(),
            ),
            execution_mode: queued_run.project.execution_mode,
            state: WorktreeState::Ready,
            last_run_id: None,
            last_used_at: queued_run.run.requested_at,
            created_at: queued_run.run.requested_at,
            updated_at: queued_run.run.requested_at,
            cleanup_after: None,
            reusable: true,
        });
    Some(SelectedAssignment {
        reason: format!(
            "selected {} for run {} ({})",
            identity.display_name,
            queued_run.run.run_id,
            launch_mode.as_str()
        ),
        worktree,
        identity,
        launch_mode,
        lineage_mode,
        candidates,
    })
}

fn build_candidate_assessment(
    report: &IdentityStatusReport,
    evaluation: &IdentityEvaluation,
    active_count: u32,
    runtime: Option<&AccountRuntimeRecord>,
    queued_run: &QueuedRunContext,
    settings: &SchedulerSettings,
) -> CandidateAssessment {
    let same_identity_affinity = queued_run
        .task
        .last_identity_id
        .as_ref()
        .map(|identity_id| identity_id == &report.identity.id)
        .unwrap_or(false);
    let same_task_affinity = queued_run
        .task
        .preferred_identity_id
        .as_ref()
        .map(|identity_id| identity_id == &report.identity.id)
        .unwrap_or(same_identity_affinity);
    let busy = active_count >= settings.max_active_runs_per_identity;
    let rejection_reason = evaluation
        .rejection_reason
        .as_ref()
        .map(|reason| reason.as_str().to_string())
        .or_else(|| busy.then_some("identity_busy".to_string()));
    CandidateAssessment {
        identity_id: report.identity.id.clone(),
        display_name: report.identity.display_name.clone(),
        eligible: rejection_reason.is_none(),
        rejection_reason,
        occupancy_state: runtime
            .map(|runtime| runtime.state.as_str().to_string())
            .unwrap_or_else(|| {
                if busy {
                    "busy".to_string()
                } else {
                    "free".to_string()
                }
            }),
        active_count,
        same_task_affinity,
        same_identity_affinity,
        quota_bucket: evaluation
            .relevant_bucket
            .as_ref()
            .map(|bucket| bucket.usage_band.as_str().to_string()),
        remaining_headroom_percent: evaluation
            .relevant_bucket
            .as_ref()
            .map(|bucket| bucket.remaining_headroom_percent),
        priority: report.identity.priority,
        selected: false,
    }
}

fn select_candidate_index(
    candidates: &[CandidateAssessment],
    queued_run: &QueuedRunContext,
) -> Option<usize> {
    let mut ranked = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| candidate.eligible)
        .collect::<Vec<_>>();
    ranked.sort_by(|(left_index, left), (right_index, right)| {
        compare_candidate(left, right, queued_run, *left_index, *right_index)
    });
    ranked.first().map(|(index, _)| *index)
}

fn compare_candidate(
    left: &CandidateAssessment,
    right: &CandidateAssessment,
    queued_run: &QueuedRunContext,
    left_index: usize,
    right_index: usize,
) -> std::cmp::Ordering {
    let left_free = left.active_count == 0;
    let right_free = right.active_count == 0;
    let left_headroom = left.remaining_headroom_percent.unwrap_or_default();
    let right_headroom = right.remaining_headroom_percent.unwrap_or_default();
    let left_same_identity = left.same_identity_affinity;
    let right_same_identity = right.same_identity_affinity;
    if matches!(queued_run.run.run_kind, RunKind::FollowUp | RunKind::Retry) {
        left_same_identity
            .cmp(&right_same_identity)
            .reverse()
            .then_with(|| left_free.cmp(&right_free).reverse())
            .then_with(|| left_headroom.cmp(&right_headroom).reverse())
            .then_with(|| left.priority.cmp(&right.priority).reverse())
            .then_with(|| left.display_name.cmp(&right.display_name))
            .then_with(|| left_index.cmp(&right_index))
    } else {
        left_free
            .cmp(&right_free)
            .reverse()
            .then_with(|| left_headroom.cmp(&right_headroom).reverse())
            .then_with(|| left.priority.cmp(&right.priority).reverse())
            .then_with(|| {
                queued_run
                    .task
                    .task_id
                    .as_str()
                    .cmp(queued_run.task.task_id.as_str())
            })
            .then_with(|| left.display_name.cmp(&right.display_name))
            .then_with(|| left_index.cmp(&right_index))
    }
}

fn determine_launch_mode(
    identity: &CodexIdentity,
    queued_run: &QueuedRunContext,
) -> (LaunchMode, LineageMode) {
    if queued_run.task.current_lineage_thread_id.is_none() {
        return (LaunchMode::NewThread, LineageMode::NewThread);
    }
    if queued_run
        .task
        .last_identity_id
        .as_ref()
        .map(|identity_id| identity_id == &identity.id)
        .unwrap_or(false)
    {
        return (
            LaunchMode::ResumeSameIdentity,
            LineageMode::ResumeSameIdentity,
        );
    }
    (LaunchMode::ResumeHandoff, LineageMode::ResumeHandoff)
}

fn lease_count_map(leases: &[AccountLeaseRecord]) -> BTreeMap<IdentityId, u32> {
    let mut counts = BTreeMap::new();
    for lease in leases {
        *counts.entry(lease.identity_id.clone()).or_insert(0) += 1;
    }
    counts
}

fn scheduler_owner_id() -> String {
    format!("scheduler-{}", std::process::id())
}

fn lease_owner_id(run_id: &str) -> String {
    format!("worker-lease-{run_id}")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::Command;
    use std::time::Duration;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{should_run_interval, SchedulerDaemon};
    use crate::domain::identity::IdentityId;
    use crate::task_orchestration::config::SchedulerSettings;
    use crate::task_orchestration::domain::{
        CleanupPolicy, DecisionKind, DispatchDecisionId, DispatchDecisionRecord, LineageMode,
        ProjectExecutionMode, TaskAffinityPolicy, TaskRunStatus, WorktreeId, WorktreeRecord,
        WorktreeState,
    };
    use crate::task_orchestration::store::{
        AssignmentClaim, ProjectSubmitRequest, RunCompletion, SchedulerStore, TaskSubmitRequest,
    };
    use crate::task_orchestration::worktree::WorktreeManager;

    #[test]
    fn should_run_interval_respects_force_and_elapsed_time() {
        assert!(should_run_interval(
            None,
            100,
            Duration::from_secs(60),
            false
        ));
        assert!(!should_run_interval(
            Some(90),
            100,
            Duration::from_secs(60),
            false
        ));
        assert!(should_run_interval(
            Some(30),
            100,
            Duration::from_secs(60),
            false
        ));
        assert!(should_run_interval(
            Some(99),
            100,
            Duration::from_secs(60),
            true
        ));
    }

    #[test]
    fn forced_tick_records_quota_refresh_and_gc_timestamps() {
        let temp = tempdir().unwrap();
        let daemon = SchedulerDaemon::new(temp.path(), SchedulerSettings::default()).unwrap();
        let mut store = SchedulerStore::open(temp.path()).unwrap();
        store
            .acquire_scheduler_lock("scheduler-test", Duration::from_secs(60))
            .unwrap();

        let before = store.scheduler_control().unwrap();
        assert!(before.last_quota_refresh_at.is_none());
        assert!(before.last_gc_at.is_none());

        let outcomes = daemon
            .tick_iteration(&mut store, true, "scheduler-test")
            .unwrap();
        assert!(outcomes.is_empty());

        let after = store.scheduler_control().unwrap();
        assert!(after.last_quota_refresh_at.is_some());
        assert!(after.last_gc_at.is_some());
    }

    #[test]
    fn gc_inner_removes_git_worktrees_via_repo_aware_cleanup() {
        let temp = tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        fs::create_dir_all(&repo_root).unwrap();
        fs::write(repo_root.join("README.md"), "demo").unwrap();
        run_git(&repo_root, &["init"]).unwrap();
        run_git(&repo_root, &["add", "."]).unwrap();
        run_git(
            &repo_root,
            &[
                "-c",
                "user.name=Scheduler Test",
                "-c",
                "user.email=scheduler@example.com",
                "commit",
                "-m",
                "initial",
            ],
        )
        .unwrap();

        let mut store = SchedulerStore::open(temp.path()).unwrap();
        let project = store
            .create_project(ProjectSubmitRequest {
                name: "git-demo".to_string(),
                repo_root: repo_root.clone(),
                execution_mode: ProjectExecutionMode::GitWorktree,
                default_codex_args: Vec::new(),
                default_model_or_profile: None,
                env_allowlist: Vec::new(),
                cleanup_policy: CleanupPolicy::default(),
            })
            .unwrap();
        let snapshot = store
            .submit_task(TaskSubmitRequest {
                project: project.name.clone(),
                title: "cleanup".to_string(),
                prompt_text: "cleanup".to_string(),
                prompt_file_path: None,
                priority: 1,
                labels: Vec::new(),
                created_by: "test".to_string(),
                max_runtime_secs: None,
                queue_if_busy: true,
                allow_oversubscribe: false,
                affinity_policy: TaskAffinityPolicy::Spread,
            })
            .unwrap();
        let run = snapshot.runs[0].clone();
        let worktree = WorktreeManager
            .materialize(
                temp.path(),
                &project,
                &snapshot.task.task_id,
                &run.run_id,
                None,
            )
            .unwrap();
        let worktree_record = WorktreeRecord {
            worktree_id: WorktreeId::from_string("worktree-git-1"),
            project_id: project.project_id.clone(),
            task_id: snapshot.task.task_id.clone(),
            path: worktree.path.clone(),
            execution_mode: ProjectExecutionMode::GitWorktree,
            state: WorktreeState::Ready,
            last_run_id: None,
            last_used_at: 1,
            created_at: 1,
            updated_at: 1,
            cleanup_after: None,
            reusable: true,
        };
        let decision = DispatchDecisionRecord {
            decision_id: DispatchDecisionId::new(),
            run_id: run.run_id.clone(),
            decision_kind: DecisionKind::Dispatch,
            selected_identity_id: Some(IdentityId::from_string("identity-1")),
            selected_worktree_id: Some(worktree_record.worktree_id.clone()),
            lineage_mode: LineageMode::NewThread,
            reason: "git gc test".to_string(),
            candidates: Vec::new(),
            policy_snapshot_json: json!({}),
            created_at: 1,
        };
        store
            .claim_assignment(
                &AssignmentClaim {
                    run_id: run.run_id.clone(),
                    task_id: run.task_id.clone(),
                    project_id: project.project_id.clone(),
                    identity_id: IdentityId::from_string("identity-1"),
                    worktree: worktree_record.clone(),
                    worker_owner_id: "worker-1".to_string(),
                    launch_mode: crate::task_orchestration::domain::LaunchMode::NewThread,
                    lineage_mode: LineageMode::NewThread,
                    reason: "git gc test".to_string(),
                    decision,
                    lease_expires_at: 100,
                },
                &SchedulerSettings::default(),
            )
            .unwrap();
        store
            .finish_run(
                run.run_id.as_str(),
                RunCompletion {
                    status: TaskRunStatus::Completed,
                    exit_code: Some(0),
                    failure_kind: None,
                    failure_message: None,
                    thread_id: None,
                    checkpoint_id: None,
                    last_identity_id: Some(IdentityId::from_string("identity-1")),
                },
            )
            .unwrap();

        assert!(worktree.path.exists());
        let daemon = SchedulerDaemon::new(temp.path(), SchedulerSettings::default()).unwrap();
        store
            .acquire_scheduler_lock("scheduler-test", Duration::from_secs(60))
            .unwrap();
        let removed = daemon
            .gc_inner(&mut store, i64::MAX, true, Some("scheduler-test"))
            .unwrap();
        assert_eq!(removed, vec![worktree.path.clone()]);
        assert!(!worktree.path.exists());
        assert!(store.gc_worktrees(i64::MAX).unwrap().is_empty());
        assert!(
            !git_worktree_list(&repo_root).contains(&worktree.path.to_string_lossy().to_string())
        );
    }

    fn run_git(repo_root: &std::path::Path, args: &[&str]) -> crate::error::Result<()> {
        let status = Command::new("git")
            .arg("-C")
            .arg(repo_root)
            .args(args)
            .status()?;
        if status.success() {
            return Ok(());
        }
        Err(crate::error::AppError::ChildProcessFailed {
            program: "git".to_string(),
            code: status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string()),
        })
    }

    fn git_worktree_list(repo_root: &std::path::Path) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo_root)
            .arg("worktree")
            .arg("list")
            .output()
            .unwrap();
        String::from_utf8(output.stdout).unwrap()
    }
}
