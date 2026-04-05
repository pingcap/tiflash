// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use tantivy::directory::{self, error};

/// A Directory wrapper that tracks all newly created (but not deleted) files
/// since it is attached to a Directory.
///
/// Note that it does not track existing files, if the underlying directory is not empty.
#[derive(Clone)]
pub struct TrackedDirectory<T: directory::Directory + Clone> {
    inner: T,
    all_files: Arc<RwLock<HashSet<PathBuf>>>,
}

impl<T: directory::Directory + Clone> directory::Directory for TrackedDirectory<T> {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn directory::FileHandle>, error::OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn open_read(&self, path: &Path) -> Result<directory::FileSlice, error::OpenReadError> {
        self.inner.open_read(path)
    }

    fn delete(&self, path: &Path) -> Result<(), error::DeleteError> {
        self.inner.delete(path)?;
        self.all_files
            .write()
            .expect("lock is poisoned")
            .remove(path);
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, error::OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<directory::WritePtr, error::OpenWriteError> {
        let ptr = self.inner.open_write(path)?;
        self.all_files
            .write()
            .expect("lock is poisoned")
            .insert(path.to_path_buf());
        Ok(ptr)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, error::OpenReadError> {
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.inner.atomic_write(path, data)?;
        self.all_files
            .write()
            .expect("lock is poisoned")
            .insert(path.to_path_buf());
        Ok(())
    }

    fn watch(
        &self,
        watch_callback: directory::WatchCallback,
    ) -> tantivy::Result<directory::WatchHandle> {
        self.inner.watch(watch_callback)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.inner.sync_directory()
    }

    fn acquire_lock(
        &self,
        lock: &directory::Lock,
    ) -> Result<directory::DirectoryLock, error::LockError> {
        self.inner.acquire_lock(lock)
    }
}

impl<T: directory::Directory + Clone> fmt::Debug for TrackedDirectory<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} (files={})",
            self.inner,
            self.all_files.read().expect("lock is poisoned").len()
        )
    }
}

impl<T: directory::Directory + Clone> TrackedDirectory<T> {
    /// Wraps a Directory and start tracking newly added files through this wrapper.
    pub fn wrap(d: T) -> Self {
        TrackedDirectory {
            inner: d,
            all_files: Arc::<RwLock<HashSet<PathBuf>>>::default(),
        }
    }

    /// Lists all tracked files in the directory. The order is unspecified.
    /// Created and then deleted files are not included.
    /// Files before wrapping are also not included.
    pub fn all_files(&self) -> Vec<PathBuf> {
        self.all_files
            .read()
            .expect("lock is poisoned")
            .iter()
            .cloned()
            .collect()
    }

    /// Returns the reference count of the underlying directory.
    pub fn reference_count(&self) -> usize {
        Arc::strong_count(&self.all_files)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::io::Write;
    use tantivy::directory::Directory;

    fn test_list_<Dir: tantivy::Directory + Clone>(base_directory: Dir) -> Result<()> {
        let directory = TrackedDirectory::wrap(base_directory);
        directory.atomic_write(Path::new("a"), b"atomic is the way")?;
        let mut wrt = directory.open_write(Path::new("b"))?;
        wrt.write_all(b"sequential is the way")?;
        wrt.flush()?;

        let mut files = directory.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("a"), PathBuf::from("b")]);

        assert!(directory.delete(Path::new("c")).is_err());

        let mut files = directory.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("a"), PathBuf::from("b")]);

        directory.delete(Path::new("a"))?;

        let mut files = directory.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("b")]);

        assert!(directory.delete(Path::new("a")).is_err());

        Ok(())
    }

    #[test]
    fn test_list() -> Result<()> {
        let directory = directory::RamDirectory::default();
        test_list_(directory)?;

        let directory = directory::MmapDirectory::create_from_tempdir()?;
        test_list_(directory)?;

        Ok(())
    }

    fn test_files_before_wrap_<Dir: tantivy::Directory + Clone>(base_directory: Dir) -> Result<()> {
        base_directory.atomic_write(Path::new("x"), b"file before wrap")?;
        base_directory.atomic_write(Path::new("y"), b"file 2 before wrap")?;

        let directory = TrackedDirectory::wrap(base_directory.clone());
        directory.atomic_write(Path::new("a"), b"atomic is the way")?;

        let mut files = directory.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("a")]);

        // Delete a file that was created before wrapping should succeed.
        directory.delete(Path::new("x"))?;

        let mut files = directory.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("a")]);

        // Call `open_write` to an existing file before wrapping should fail, because tantivy::Directory
        // does not support overwriting.
        assert!(directory.open_write(Path::new("y")).is_err());

        // As write failed, it should not be tracked, although the file is actually there.
        // (as we only track newly created files)
        let mut files = directory.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("a")]);
        // Note: This is tantivy's RamDirectory's bug: although open_write failed, existing file is now broken.
        // MmapDirectory does not have this issue.
        // assert_eq!(
        //     directory.atomic_read(Path::new("y"))?,
        //     b"file 2 before wrap"
        // );

        // Call `atomic_write` to an existing file before wrapping should succeed, because tantivy::Directory
        // supports overwriting in this way.
        directory.atomic_write(Path::new("y"), b"file 2 updated")?;
        let mut files = directory.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("a"), PathBuf::from("y")]);
        assert_eq!(directory.atomic_read(Path::new("y"))?, b"file 2 updated");

        // Direct write to the `base_directory` will not be tracked.
        base_directory.atomic_write(Path::new("z"), b"file after wrap")?;
        let mut files = directory.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("a"), PathBuf::from("y")]);
        assert_eq!(directory.atomic_read(Path::new("z"))?, b"file after wrap");

        Ok(())
    }

    #[test]
    fn test_files_before_wrap() -> Result<()> {
        let directory = directory::RamDirectory::default();
        test_files_before_wrap_(directory)?;

        let directory = directory::MmapDirectory::create_from_tempdir()?;
        test_files_before_wrap_(directory)?;

        Ok(())
    }

    #[test]
    fn test_clone() -> Result<()> {
        let directory = TrackedDirectory::wrap(directory::RamDirectory::default());
        directory.atomic_write(Path::new("a"), b"atomic is the way")?;

        // Cloning a tracked directory will not cause mismatched tracking.
        let directory_clone = directory.clone();
        directory_clone.atomic_write(Path::new("b"), b"cloned")?;

        let mut files = directory.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("a"), PathBuf::from("b")]);

        let mut files = directory_clone.all_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("a"), PathBuf::from("b")]);

        Ok(())
    }
}
