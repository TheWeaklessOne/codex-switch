use std::env;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::{symlink, PermissionsExt};

use crate::error::{AppError, Result};

pub fn default_base_root() -> Result<PathBuf> {
    let home = env::var_os("HOME").ok_or(AppError::MissingHomeDirectory)?;
    Ok(normalize_path(
        PathBuf::from(home).join(".telex-codex-switcher"),
    ))
}

pub fn default_codex_home() -> Result<PathBuf> {
    let home = env::var_os("HOME").ok_or(AppError::MissingHomeDirectory)?;
    Ok(normalize_path(PathBuf::from(home).join(".codex")))
}

pub fn resolve_path(value: &Path) -> Result<PathBuf> {
    let expanded = expand_tilde(value)?;
    if expanded.is_absolute() {
        return Ok(normalize_path(expanded));
    }

    let current_directory =
        env::current_dir().map_err(|source| AppError::CurrentDirectoryUnavailable { source })?;
    Ok(normalize_path(current_directory.join(expanded)))
}

pub fn canonicalize_location(path: &Path) -> Result<PathBuf> {
    let mut current = normalize_path(path.to_path_buf());
    let mut missing_tail = Vec::<OsString>::new();

    loop {
        match fs::canonicalize(&current) {
            Ok(resolved) => {
                let mut resolved = normalize_path(resolved);
                for component in missing_tail.iter().rev() {
                    resolved.push(component);
                }
                return Ok(normalize_path(resolved));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = current.file_name() else {
                    return Ok(normalize_path(path.to_path_buf()));
                };
                missing_tail.push(name.to_os_string());
                let Some(parent) = current.parent() else {
                    return Ok(normalize_path(path.to_path_buf()));
                };
                current = parent.to_path_buf();
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub fn registry_path(base_root: &Path) -> PathBuf {
    base_root.join("registry.json")
}

pub fn default_home_path(base_root: &Path, identity_slug: &str) -> PathBuf {
    base_root.join("homes").join(identity_slug)
}

pub fn shared_sessions_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("sessions")
}

pub fn thread_leases_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("thread-leases")
}

pub fn thread_leases_lock_path(base_root: &Path, thread_id: &str) -> PathBuf {
    thread_leases_path(base_root)
        .join("locks")
        .join(format!("{}.lock", filesystem_key(thread_id)))
}

pub fn thread_lease_path(base_root: &Path, thread_id: &str) -> PathBuf {
    thread_leases_path(base_root).join(format!("{}.json", filesystem_key(thread_id)))
}

pub fn turn_states_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("turn-states")
}

pub fn turn_state_path(base_root: &Path, thread_id: &str) -> PathBuf {
    turn_states_path(base_root).join(format!("{}.json", filesystem_key(thread_id)))
}

pub fn quota_status_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("quota-status.json")
}

pub fn selection_policy_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("selection-policy.json")
}

pub fn identity_health_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("identity-health.json")
}

pub fn selection_state_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("selection-state.json")
}

pub fn selection_events_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("selection-events")
}

pub fn selection_event_path(base_root: &Path, event_id: &str) -> PathBuf {
    selection_events_path(base_root).join(format!("{event_id}.json"))
}

pub fn task_checkpoints_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("task-checkpoints")
}

pub fn task_checkpoint_path(base_root: &Path, checkpoint_id: &str) -> PathBuf {
    task_checkpoints_path(base_root).join(format!("{checkpoint_id}.json"))
}

pub fn scheduler_root_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("scheduler")
}

pub fn scheduler_db_path(base_root: &Path) -> PathBuf {
    scheduler_root_path(base_root).join("scheduler.db")
}

pub fn task_artifacts_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("task-artifacts")
}

pub fn task_artifact_run_path(base_root: &Path, task_id: &str, run_id: &str) -> PathBuf {
    task_artifacts_path(base_root).join(task_id).join(run_id)
}

pub fn task_artifact_prompt_path(base_root: &Path, task_id: &str, run_id: &str) -> PathBuf {
    task_artifact_run_path(base_root, task_id, run_id).join("prompt.txt")
}

pub fn task_artifact_events_path(base_root: &Path, task_id: &str, run_id: &str) -> PathBuf {
    task_artifact_run_path(base_root, task_id, run_id).join("events.jsonl")
}

pub fn task_artifact_thread_snapshot_path(
    base_root: &Path,
    task_id: &str,
    run_id: &str,
) -> PathBuf {
    task_artifact_run_path(base_root, task_id, run_id).join("thread-snapshot.json")
}

pub fn task_worktrees_path(base_root: &Path) -> PathBuf {
    base_root.join("shared").join("task-worktrees")
}

pub fn task_worktree_project_path(base_root: &Path, project_id: &str) -> PathBuf {
    task_worktrees_path(base_root).join(project_id)
}

pub fn task_worktree_task_path(base_root: &Path, project_id: &str, task_id: &str) -> PathBuf {
    task_worktree_project_path(base_root, project_id).join(task_id)
}

pub fn task_worktree_run_path(
    base_root: &Path,
    project_id: &str,
    task_id: &str,
    run_id: &str,
) -> PathBuf {
    task_worktree_task_path(base_root, project_id, task_id).join(run_id)
}

pub fn ensure_directory(path: &Path, mode: u32) -> Result<()> {
    match fs::symlink_metadata(path) {
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
        Ok(_) => {
            set_mode(path, mode)?;
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    fs::create_dir_all(path)?;
    set_mode(path, mode)?;
    Ok(())
}

pub fn ensure_regular_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(AppError::ExpectedFile {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

pub fn atomic_write(path: &Path, content: &[u8], mode: u32) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_directory(parent, 0o700)?;
    }
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() {
            return Err(AppError::UnexpectedSymlink {
                path: path.to_path_buf(),
            });
        }
    }

    let temporary_path = temporary_path_for(path);

    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary_path)?;
    file.write_all(content)?;
    file.flush()?;
    set_mode(&temporary_path, mode)?;
    fs::rename(&temporary_path, path)?;
    set_mode(path, mode)?;
    Ok(())
}

pub fn copy_file(source: &Path, destination: &Path, mode: u32) -> Result<()> {
    ensure_regular_file(source)?;
    if let Some(parent) = destination.parent() {
        ensure_directory(parent, 0o700)?;
    }
    if let Ok(metadata) = fs::symlink_metadata(destination) {
        if metadata.file_type().is_symlink() {
            return Err(AppError::UnexpectedSymlink {
                path: destination.to_path_buf(),
            });
        }
    }

    let temporary_path = temporary_path_for(destination);
    match fs::copy(source, &temporary_path) {
        Ok(_) => {}
        Err(error) => {
            let _ = fs::remove_file(&temporary_path);
            return Err(error.into());
        }
    }

    if let Err(error) = set_mode(&temporary_path, mode) {
        let _ = fs::remove_file(&temporary_path);
        return Err(error);
    }

    if let Err(error) = fs::rename(&temporary_path, destination) {
        let _ = fs::remove_file(&temporary_path);
        return Err(error.into());
    }

    set_mode(destination, mode)?;
    Ok(())
}

pub fn ensure_sessions_symlink(link_path: &Path, target_path: &Path) -> Result<()> {
    ensure_directory(target_path, 0o700)?;

    match fs::symlink_metadata(link_path) {
        Ok(metadata) if !metadata.file_type().is_symlink() => {
            return Err(AppError::SessionsLinkNotSymlink {
                path: link_path.to_path_buf(),
            });
        }
        Ok(_) => {
            let existing_target = fs::read_link(link_path)?;
            let existing_target = if existing_target.is_absolute() {
                normalize_path(existing_target)
            } else {
                let parent = link_path.parent().unwrap_or_else(|| Path::new("/"));
                normalize_path(parent.join(existing_target))
            };
            let expected_target = normalize_path(target_path.to_path_buf());
            if existing_target != expected_target {
                return Err(AppError::SessionsLinkConflict {
                    path: link_path.to_path_buf(),
                    actual: existing_target,
                    expected: expected_target,
                });
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    #[cfg(unix)]
    symlink(target_path, link_path)?;

    #[cfg(not(unix))]
    {
        let _ = link_path;
        let _ = target_path;
        return Err(AppError::UnexpectedSymlink {
            path: link_path.to_path_buf(),
        });
    }

    Ok(())
}

fn expand_tilde(path: &Path) -> Result<PathBuf> {
    let raw = path.as_os_str().to_string_lossy();
    if raw == "~" {
        let home = env::var_os("HOME").ok_or(AppError::MissingHomeDirectory)?;
        return Ok(PathBuf::from(home));
    }
    if let Some(remainder) = raw.strip_prefix("~/") {
        let home = env::var_os("HOME").ok_or(AppError::MissingHomeDirectory)?;
        return Ok(PathBuf::from(home).join(remainder));
    }
    Ok(path.to_path_buf())
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                let popped = normalized.pop();
                if !popped {
                    normalized.push(component.as_os_str());
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }

    normalized
}

fn filesystem_key(value: &str) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn temporary_path_for(path: &Path) -> PathBuf {
    let temporary_name = format!(
        ".{}.tmp-{}-{}",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("codex-switch"),
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    );
    path.with_file_name(temporary_name)
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    #[cfg(unix)]
    {
        let permissions = fs::Permissions::from_mode(mode);
        fs::set_permissions(path, permissions)?;
    }

    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        ensure_directory, ensure_sessions_symlink, quota_status_path, resolve_path,
        selection_event_path, shared_sessions_path, thread_lease_path, turn_state_path,
    };

    #[test]
    fn resolves_relative_paths_against_current_directory() {
        let current = std::env::current_dir().unwrap();
        let resolved = resolve_path(std::path::Path::new("fixtures/home")).unwrap();
        assert_eq!(resolved, current.join("fixtures/home"));
    }

    #[test]
    fn creates_and_validates_sessions_symlink() {
        let temp = tempdir().unwrap();
        let home = temp.path().join("home");
        let shared = shared_sessions_path(temp.path());
        ensure_directory(&home, 0o700).unwrap();

        ensure_sessions_symlink(&home.join("sessions"), &shared).unwrap();
        ensure_sessions_symlink(&home.join("sessions"), &shared).unwrap();

        let link_target = std::fs::read_link(home.join("sessions")).unwrap();
        assert_eq!(link_target, shared);
    }

    #[test]
    fn encodes_thread_paths_without_raw_thread_id() {
        let base = std::path::Path::new("/tmp/base");
        let lease_path = thread_lease_path(base, "thread/1");
        let state_path = turn_state_path(base, "thread/1");
        let quota_path = quota_status_path(base);
        let event_path = selection_event_path(base, "event-1");

        assert!(lease_path.ends_with("7468726561642f31.json"));
        assert!(state_path.ends_with("7468726561642f31.json"));
        assert!(quota_path.ends_with("shared/quota-status.json"));
        assert!(event_path.ends_with("shared/selection-events/event-1.json"));
    }
}
