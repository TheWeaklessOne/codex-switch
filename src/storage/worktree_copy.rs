use std::fs;
use std::path::Path;

use crate::error::{AppError, Result};
use crate::storage::paths::ensure_directory;

pub fn copy_workspace_tree(source: &Path, destination: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        return Err(AppError::UnexpectedSymlink {
            path: source.to_path_buf(),
        });
    }
    if !metadata.is_dir() {
        return Err(AppError::ExpectedDirectory {
            path: source.to_path_buf(),
        });
    }

    ensure_directory(destination, 0o700)?;
    if let Err(error) = copy_directory_entries(source, destination) {
        let _ = fs::remove_dir_all(destination);
        return Err(error);
    }
    Ok(())
}

fn copy_directory_entries(source: &Path, destination: &Path) -> Result<()> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(AppError::UnexpectedSymlink { path });
        }
        if metadata.is_dir() {
            ensure_directory(&destination_path, 0o700)?;
            copy_directory_entries(&path, &destination_path)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(AppError::ExpectedFile { path });
        }
        fs::copy(&path, &destination_path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::copy_workspace_tree;

    #[test]
    fn copies_regular_directory_trees() {
        let temp = tempdir().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        fs::create_dir_all(source.join("nested")).unwrap();
        fs::write(source.join("README.md"), "hello").unwrap();
        fs::write(source.join("nested").join("note.txt"), "world").unwrap();

        copy_workspace_tree(&source, &destination).unwrap();

        assert_eq!(
            fs::read_to_string(destination.join("README.md")).unwrap(),
            "hello"
        );
        assert_eq!(
            fs::read_to_string(destination.join("nested").join("note.txt")).unwrap(),
            "world"
        );
    }

    #[cfg(unix)]
    #[test]
    fn removes_partial_destination_when_copy_fails() {
        let temp = tempdir().unwrap();
        let source = temp.path().join("source");
        let destination = temp.path().join("destination");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("README.md"), "hello").unwrap();
        symlink(source.join("README.md"), source.join("bad-link")).unwrap();

        let error = copy_workspace_tree(&source, &destination).unwrap_err();
        assert!(matches!(
            error,
            crate::error::AppError::UnexpectedSymlink { .. }
        ));
        assert!(!destination.exists());
    }
}
