use std::path::{Path, PathBuf};

use crate::domain::identity::{current_timestamp, CodexIdentity, IdentityId};
use crate::error::{AppError, Result};
use crate::quota_status::{IdentityRefreshErrorKind, IdentityStatusReport};
use crate::storage::health_store::IdentityHealthStore;
use crate::storage::quota_store::QuotaStore;
use crate::storage::registry_store::RegistryStore;
use crate::storage::selection_store::SelectionStore;

#[derive(Debug, Clone)]
pub struct RemoveIdentityOutcome {
    pub identity: CodexIdentity,
    pub selection_cleared: bool,
    pub home_removed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceDeactivationSummary {
    pub http_status: u16,
    pub code: String,
}

#[derive(Debug, Clone)]
pub enum AutoRemovalNotice {
    Removed {
        identity: CodexIdentity,
        reason: WorkspaceDeactivationSummary,
        selection_cleared: bool,
    },
    RemovalFailed {
        identity: CodexIdentity,
        reason: WorkspaceDeactivationSummary,
        error: String,
    },
}

impl AutoRemovalNotice {
    pub fn summary(&self) -> String {
        match self {
            Self::Removed {
                identity,
                reason,
                selection_cleared,
            } => {
                let mut message = format!(
                    "auto-removed {} ({}) after live refresh returned {} {}",
                    identity.display_name, identity.id, reason.http_status, reason.code
                );
                if *selection_cleared {
                    message.push_str("; cleared current selection");
                }
                message
            }
            Self::RemovalFailed {
                identity,
                reason,
                error,
            } => format!(
                "failed to auto-remove {} ({}) after live refresh returned {} {}: {}",
                identity.display_name, identity.id, reason.http_status, reason.code, error
            ),
        }
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, Self::RemovalFailed { .. })
    }
}

#[derive(Debug, Clone)]
pub struct AutoRemovalSweep {
    pub reports: Vec<IdentityStatusReport>,
    pub notices: Vec<AutoRemovalNotice>,
}

#[derive(Debug, Clone)]
pub struct ManagedIdentityRemovalService<R, Q, H, S> {
    registry_store: R,
    quota_store: Q,
    health_store: H,
    selection_store: S,
}

impl<R, Q, H, S> ManagedIdentityRemovalService<R, Q, H, S> {
    pub fn new(registry_store: R, quota_store: Q, health_store: H, selection_store: S) -> Self {
        Self {
            registry_store,
            quota_store,
            health_store,
            selection_store,
        }
    }
}

impl<R, Q, H, S> ManagedIdentityRemovalService<R, Q, H, S>
where
    R: RegistryStore,
    Q: QuotaStore,
    H: IdentityHealthStore,
    S: SelectionStore,
{
    pub fn remove_identity_by_name(&self, identity_name: &str) -> Result<RemoveIdentityOutcome> {
        let identity_id = IdentityId::from_display_name(identity_name)?;
        self.remove_identity_by_id(&identity_id)
    }

    pub fn remove_identity_by_id(&self, identity_id: &IdentityId) -> Result<RemoveIdentityOutcome> {
        let original_registry = self.registry_store.load()?;
        let mut updated_registry = original_registry.clone();
        let removed_identity =
            updated_registry
                .identities
                .remove(identity_id)
                .ok_or_else(|| AppError::IdentityNotFound {
                    identity_id: identity_id.clone(),
                })?;

        let original_quota = self.quota_store.load()?;
        let mut updated_quota = original_quota.clone();
        updated_quota.statuses.remove(identity_id);

        let original_health = self.health_store.load()?;
        let mut updated_health = original_health.clone();
        updated_health.identities.remove(identity_id);

        let original_selection = self.selection_store.load()?;
        let mut updated_selection = original_selection.clone();
        let selection_cleared = updated_selection
            .current
            .as_ref()
            .is_some_and(|current| current.identity_id == *identity_id);
        if selection_cleared {
            updated_selection.current = None;
        }

        let staged_home = stage_identity_home_removal(&removed_identity.codex_home)?;
        let rollback_context = RemoveIdentityRollbackContext {
            registry: Some((&self.registry_store, &original_registry)),
            quota: Some((&self.quota_store, &original_quota)),
            health: Some((&self.health_store, &original_health)),
            selection_store: &self.selection_store,
            original_selection: &original_selection,
            staged_home: staged_home.as_ref(),
        };

        self.selection_store.save(&updated_selection)?;
        if let Err(primary) = self.quota_store.save(&updated_quota) {
            return Err(rollback_remove_identity(
                primary,
                "remove identity",
                RemoveIdentityRollbackContext {
                    quota: None,
                    health: None,
                    ..rollback_context
                },
            ));
        }
        if let Err(primary) = self.health_store.save(&updated_health) {
            return Err(rollback_remove_identity(
                primary,
                "remove identity",
                RemoveIdentityRollbackContext {
                    health: None,
                    ..rollback_context
                },
            ));
        }
        if let Err(primary) = self.registry_store.save(&updated_registry) {
            return Err(rollback_remove_identity(
                primary,
                "remove identity",
                rollback_context,
            ));
        }

        if let Some(staged_home) = staged_home.as_ref() {
            if let Err(primary) = delete_staged_identity_home(staged_home) {
                return Err(rollback_remove_identity(
                    primary,
                    "remove identity",
                    rollback_context,
                ));
            }
        }

        Ok(RemoveIdentityOutcome {
            identity: removed_identity,
            selection_cleared,
            home_removed: staged_home.is_some(),
        })
    }
}

pub fn auto_remove_deactivated_workspace_identities<R, Q, H, S>(
    reports: Vec<IdentityStatusReport>,
    remover: &ManagedIdentityRemovalService<R, Q, H, S>,
) -> AutoRemovalSweep
where
    R: RegistryStore,
    Q: QuotaStore,
    H: IdentityHealthStore,
    S: SelectionStore,
{
    let mut retained_reports = Vec::with_capacity(reports.len());
    let mut notices = Vec::new();

    for report in reports {
        let Some(reason) = workspace_deactivation_reason(&report) else {
            retained_reports.push(report);
            continue;
        };

        match remover.remove_identity_by_id(&report.identity.id) {
            Ok(outcome) => notices.push(AutoRemovalNotice::Removed {
                identity: outcome.identity,
                reason,
                selection_cleared: outcome.selection_cleared,
            }),
            Err(error) => {
                notices.push(AutoRemovalNotice::RemovalFailed {
                    identity: report.identity.clone(),
                    reason,
                    error: error.to_string(),
                });
                retained_reports.push(report);
            }
        }
    }

    AutoRemovalSweep {
        reports: retained_reports,
        notices,
    }
}

fn workspace_deactivation_reason(
    report: &IdentityStatusReport,
) -> Option<WorkspaceDeactivationSummary> {
    match report.refresh_error_kind.as_ref()? {
        IdentityRefreshErrorKind::WorkspaceDeactivated { http_status, code } => {
            Some(WorkspaceDeactivationSummary {
                http_status: *http_status,
                code: code.clone(),
            })
        }
    }
}

#[derive(Debug, Clone)]
struct StagedHomeRemoval {
    original_path: PathBuf,
    staged_path: PathBuf,
}

#[derive(Clone, Copy)]
struct RemoveIdentityRollbackContext<'a, R, Q, H, S> {
    registry: Option<(&'a R, &'a crate::domain::identity::IdentityRegistryRecord)>,
    quota: Option<(&'a Q, &'a crate::domain::quota::QuotaStatusRecord)>,
    health: Option<(&'a H, &'a crate::domain::health::IdentityHealthRecord)>,
    selection_store: &'a S,
    original_selection: &'a crate::domain::selection::SelectionStateRecord,
    staged_home: Option<&'a StagedHomeRemoval>,
}

fn rollback_remove_identity<R, Q, H, S>(
    primary: AppError,
    operation: &str,
    context: RemoveIdentityRollbackContext<'_, R, Q, H, S>,
) -> AppError
where
    R: RegistryStore,
    Q: QuotaStore,
    H: IdentityHealthStore,
    S: SelectionStore,
{
    let RemoveIdentityRollbackContext {
        registry,
        quota,
        health,
        selection_store,
        original_selection,
        staged_home,
    } = context;
    let mut rollback_errors = Vec::new();

    if let Some((store, record)) = registry {
        if let Err(error) = store.save(record) {
            rollback_errors.push(format!("restore registry: {error}"));
        }
    }

    if let Some((store, record)) = health {
        if let Err(error) = store.save(record) {
            rollback_errors.push(format!("restore identity health: {error}"));
        }
    }

    if let Some((store, record)) = quota {
        if let Err(error) = store.save(record) {
            rollback_errors.push(format!("restore quota status: {error}"));
        }
    }

    if let Err(error) = selection_store.save(original_selection) {
        rollback_errors.push(format!("restore selection state: {error}"));
    }

    if let Some(staged_home) = staged_home {
        if let Err(error) = restore_staged_identity_home(staged_home) {
            rollback_errors.push(format!("restore identity home: {error}"));
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

fn stage_identity_home_removal(path: &Path) -> Result<Option<StagedHomeRemoval>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(AppError::UnexpectedSymlink {
                path: path.to_path_buf(),
            });
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(AppError::ExpectedDirectory {
                path: path.to_path_buf(),
            });
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }

    let file_name = path
        .file_name()
        .ok_or_else(|| AppError::ExpectedDirectory {
            path: path.to_path_buf(),
        })?;
    let staged_path = path.with_file_name(format!(
        ".{}.removing-{}",
        file_name.to_string_lossy(),
        current_timestamp()?
    ));
    std::fs::rename(path, &staged_path)?;
    Ok(Some(StagedHomeRemoval {
        original_path: path.to_path_buf(),
        staged_path,
    }))
}

fn restore_staged_identity_home(staged_home: &StagedHomeRemoval) -> Result<()> {
    std::fs::rename(&staged_home.staged_path, &staged_home.original_path)?;
    Ok(())
}

fn delete_staged_identity_home(staged_home: &StagedHomeRemoval) -> Result<()> {
    std::fs::remove_dir_all(&staged_home.staged_path)?;
    Ok(())
}
