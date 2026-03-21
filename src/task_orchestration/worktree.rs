use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::{AppError, Result};
use crate::storage::paths::{
    atomic_write, ensure_directory, task_worktree_run_path, task_worktree_task_path,
    task_worktrees_path,
};
use crate::storage::worktree_copy::copy_workspace_tree;
use crate::task_orchestration::domain::{ProjectExecutionMode, ProjectRecord, TaskId, TaskRunId};

#[derive(Debug, Clone)]
pub struct WorktreeMaterialization {
    pub path: PathBuf,
    pub reused: bool,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WorktreeManager;

impl WorktreeManager {
    pub fn ensure_base_directories(&self, base_root: &Path) -> Result<()> {
        ensure_directory(&task_worktrees_path(base_root), 0o700)
    }

    pub fn materialize(
        &self,
        base_root: &Path,
        project: &ProjectRecord,
        task_id: &TaskId,
        run_id: &TaskRunId,
        existing_path: Option<&Path>,
    ) -> Result<WorktreeMaterialization> {
        self.ensure_base_directories(base_root)?;
        if let Some(existing_path) = existing_path {
            if existing_path.exists() && worktree_ready_marker_path(existing_path).exists() {
                return Ok(WorktreeMaterialization {
                    path: existing_path.to_path_buf(),
                    reused: true,
                });
            }
        }

        let task_root =
            task_worktree_task_path(base_root, project.project_id.as_str(), task_id.as_str());
        ensure_directory(&task_root, 0o700)?;
        let target = task_worktree_run_path(
            base_root,
            project.project_id.as_str(),
            task_id.as_str(),
            run_id.as_str(),
        );
        if target.exists() && !worktree_ready_marker_path(&target).exists() {
            self.cleanup(project, &target)?;
        }
        match project.execution_mode {
            ProjectExecutionMode::GitWorktree => add_git_worktree(&project.repo_root, &target)?,
            ProjectExecutionMode::CopyWorkspace => {
                copy_workspace_tree(&project.repo_root, &target)?
            }
        }
        write_worktree_ready_marker(&target)?;
        Ok(WorktreeMaterialization {
            path: target,
            reused: false,
        })
    }

    pub fn cleanup(&self, project: &ProjectRecord, path: &Path) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        match project.execution_mode {
            ProjectExecutionMode::GitWorktree => remove_git_worktree(&project.repo_root, path)?,
            ProjectExecutionMode::CopyWorkspace => {
                fs::remove_dir_all(path)?;
            }
        }
        Ok(())
    }
}

fn worktree_ready_marker_path(path: &Path) -> PathBuf {
    path.join(".codex-switch-ready")
}

fn write_worktree_ready_marker(path: &Path) -> Result<()> {
    atomic_write(&worktree_ready_marker_path(path), b"ready\n", 0o600)
}

fn add_git_worktree(repo_root: &Path, destination: &Path) -> Result<()> {
    ensure_git_repo(repo_root)?;
    if let Some(parent) = destination.parent() {
        ensure_directory(parent, 0o700)?;
    }
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("worktree")
        .arg("add")
        .arg("--detach")
        .arg(destination)
        .arg("HEAD")
        .status()?;
    if !status.success() {
        return Err(AppError::ChildProcessFailed {
            program: "git".to_string(),
            code: status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string()),
        });
    }
    Ok(())
}

fn remove_git_worktree(repo_root: &Path, path: &Path) -> Result<()> {
    ensure_git_repo(repo_root)?;
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("worktree")
        .arg("remove")
        .arg("--force")
        .arg(path)
        .status()?;
    if !status.success() {
        return Err(AppError::ChildProcessFailed {
            program: "git".to_string(),
            code: status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string()),
        });
    }
    Ok(())
}

fn ensure_git_repo(repo_root: &Path) -> Result<()> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("rev-parse")
        .arg("--show-toplevel")
        .status()?;
    if !status.success() {
        return Err(AppError::GitRepositoryRequired {
            path: repo_root.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::WorktreeManager;
    use crate::task_orchestration::domain::{
        CleanupPolicy, ProjectExecutionMode, ProjectId, ProjectRecord, TaskId, TaskRunId,
    };

    #[test]
    fn materializes_copy_workspace_runs() {
        let temp = tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        fs::create_dir_all(repo_root.join("src")).unwrap();
        fs::write(repo_root.join("src").join("lib.rs"), "pub fn hello() {}").unwrap();
        let project = ProjectRecord {
            project_id: ProjectId::from_string("project-1"),
            name: "demo".to_string(),
            repo_root: repo_root.clone(),
            execution_mode: ProjectExecutionMode::CopyWorkspace,
            default_codex_args: Vec::new(),
            default_model_or_profile: None,
            env_allowlist: Vec::new(),
            cleanup_policy: CleanupPolicy::default(),
            created_at: 1,
            updated_at: 1,
        };

        let manager = WorktreeManager;
        let materialized = manager
            .materialize(
                temp.path(),
                &project,
                &TaskId::from_string("task-1"),
                &TaskRunId::from_string("run-1"),
                None,
            )
            .unwrap();

        assert!(materialized.path.exists());
        assert!(materialized.path.join(".codex-switch-ready").exists());
        assert_eq!(
            fs::read_to_string(materialized.path.join("src").join("lib.rs")).unwrap(),
            "pub fn hello() {}"
        );
    }

    #[test]
    fn does_not_reuse_existing_path_without_ready_marker() {
        let temp = tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        fs::create_dir_all(repo_root.join("src")).unwrap();
        fs::write(repo_root.join("src").join("lib.rs"), "pub fn hello() {}").unwrap();
        let stale_path = temp.path().join("stale");
        fs::create_dir_all(&stale_path).unwrap();
        fs::write(stale_path.join("partial.txt"), "partial").unwrap();
        let project = ProjectRecord {
            project_id: ProjectId::from_string("project-1"),
            name: "demo".to_string(),
            repo_root: repo_root.clone(),
            execution_mode: ProjectExecutionMode::CopyWorkspace,
            default_codex_args: Vec::new(),
            default_model_or_profile: None,
            env_allowlist: Vec::new(),
            cleanup_policy: CleanupPolicy::default(),
            created_at: 1,
            updated_at: 1,
        };

        let manager = WorktreeManager;
        let materialized = manager
            .materialize(
                temp.path(),
                &project,
                &TaskId::from_string("task-1"),
                &TaskRunId::from_string("run-2"),
                Some(&stale_path),
            )
            .unwrap();

        assert!(!materialized.reused);
        assert_ne!(materialized.path, stale_path);
        assert!(materialized.path.join(".codex-switch-ready").exists());
    }
}
