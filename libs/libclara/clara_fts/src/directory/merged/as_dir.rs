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

use std::fmt;
use std::io;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tantivy::directory::{self, error};

use crate::MergedFileDecoder;

/// Read from a mmaped merged file or in-memory merged file. Write is not supported.
#[derive(Clone)]
pub struct MergedFileAsDirectory {
    inner: Arc<MergedFileAsDirectoryInner>,
}

impl fmt::Debug for MergedFileAsDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MergedFileAsDirectory({})", self.inner.debug_tag)
    }
}

struct MergedFileAsDirectoryInner {
    debug_tag: String,          // For debug output
    decoder: MergedFileDecoder, // Includes a mmap ref. Unmap will happen when all references are dropped
}

impl MergedFileAsDirectory {
    pub fn from_mmap_file(file_path: impl AsRef<Path>) -> Result<Self> {
        let decoder = MergedFileDecoder::from_mmap_file(&file_path)?;
        Ok(Self {
            inner: Arc::new(MergedFileAsDirectoryInner {
                debug_tag: file_path.as_ref().to_string_lossy().to_string(),
                decoder,
            }),
        })
    }

    pub fn from_buffer(buffer: Vec<u8>) -> Result<Self> {
        let decoder = MergedFileDecoder::from_buffer(buffer)?;
        Ok(Self {
            inner: Arc::new(MergedFileAsDirectoryInner {
                debug_tag: "<RamBuffer>".to_owned(),
                decoder,
            }),
        })
    }
}

struct DummyLock;

impl directory::Directory for MergedFileAsDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn directory::FileHandle>, error::OpenReadError> {
        if let Some(owned_bytes) = self.inner.decoder.get(path) {
            Ok(Arc::new(owned_bytes))
        } else {
            Err(error::OpenReadError::FileDoesNotExist(path.to_path_buf()))
        }
    }

    fn delete(&self, path: &Path) -> Result<(), error::DeleteError> {
        Err(error::DeleteError::IoError {
            io_error: Arc::new(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "delete is not supported in MmapFromMergedFile",
            )),
            filepath: path.to_path_buf(),
        })
    }

    fn exists(&self, path: &Path) -> Result<bool, error::OpenReadError> {
        Ok(self.inner.decoder.contains(path))
    }

    fn open_write(&self, path: &Path) -> Result<directory::WritePtr, error::OpenWriteError> {
        Err(error::OpenWriteError::IoError {
            io_error: Arc::new(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "open_write is not supported in MmapFromMergedFile",
            )),
            filepath: path.to_path_buf(),
        })
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, error::OpenReadError> {
        let file = self.open_read(path)?;
        let b = file
            .read_bytes()
            .map_err(|e| error::OpenReadError::IoError {
                io_error: Arc::new(e),
                filepath: path.to_path_buf(),
            })?;
        Ok(b.to_vec())
    }

    fn atomic_write(&self, _: &Path, _: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "atomic_write is not supported in MergedFileAsDirectory",
        ))
    }

    fn sync_directory(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "sync_directory is not supported in MergedFileAsDirectory",
        ))
    }

    fn watch(&self, _: directory::WatchCallback) -> tantivy::Result<directory::WatchHandle> {
        // Watch is allowed but will not trigger because merged file is immutable.
        Ok(directory::WatchHandle::empty())
    }

    fn acquire_lock(
        &self,
        _: &directory::Lock,
    ) -> Result<directory::DirectoryLock, error::LockError> {
        // A dummy lock is fine because our file is immutable.
        Ok(directory::DirectoryLock::from(Box::new(DummyLock)))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{MergedFileFromDirectory, MergedFileFromMmapDirectory, MergedFileFromRamDirectory};

    use tantivy::directory::Directory;

    fn test_write_read_<MergingDir, MergingDirFn, FinalizeFn>(
        get_merging_dir: MergingDirFn,
        finalize: FinalizeFn,
    ) -> Result<()>
    where
        MergingDir: MergedFileFromDirectory,
        MergingDirFn: Fn() -> Result<MergingDir>,
        FinalizeFn: Fn(MergingDir) -> Result<MergedFileAsDirectory>,
    {
        // basic write & read
        let merging_dir = get_merging_dir()?;
        merging_dir.directory().atomic_write(
            Path::new("file1"),
            b"Being too popular can be such a hassle",
        )?;
        merging_dir.directory().atomic_write(
            Path::new("file2"),
            b"Who knew the people would adore me so much?",
        )?;
        merging_dir.directory().sync_directory()?;

        let m = finalize(merging_dir)?;

        assert_eq!(
            m.atomic_read(Path::new("file1"))?,
            b"Being too popular can be such a hassle"
        );
        assert_eq!(
            m.atomic_read(Path::new("file2"))?,
            b"Who knew the people would adore me so much?"
        );
        assert!(m.exists(Path::new("file1"))?);
        assert!(!m.exists(Path::new("file"))?);
        assert!(!m.exists(Path::new("file3"))?);

        let file1 = m.get_file_handle(Path::new("file1"))?;
        assert_eq!(file1.len(), 38);
        assert_eq!(
            file1.read_bytes(0..file1.len())?.to_vec(),
            b"Being too popular can be such a hassle".to_vec()
        );
        assert_eq!(file1.read_bytes(0..5)?.to_vec(), b"Being".to_vec());
        assert_eq!(file1.read_bytes(5..10)?.to_vec(), b" too ".to_vec());

        let file2 = m.get_file_handle(Path::new("file2"))?;
        assert_eq!(file2.len(), 43);
        assert_eq!(
            file2.read_bytes(0..file2.len())?.to_vec(),
            b"Who knew the people would adore me so much?".to_vec()
        );
        assert_eq!(file2.read_bytes(0..10)?.to_vec(), b"Who knew t".to_vec());
        assert_eq!(file2.read_bytes(5..14)?.to_vec(), b"new the p".to_vec());

        // empty write & read
        let merging_dir = get_merging_dir()?;
        let m = finalize(merging_dir)?;
        assert!(!m.exists(Path::new("file1"))?);
        assert!(!m.exists(Path::new("file2"))?);
        assert!(!m.exists(Path::new("file3"))?);
        assert!(m.get_file_handle(Path::new("file1")).is_err());

        Ok(())
    }

    #[test]
    fn test_write_read_mmap_merge_via_file() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;

        test_write_read_(
            || MergedFileFromMmapDirectory::new(&dir_guard.path().join("immediate")),
            |merging_dir| {
                assert!(dir_guard.path().join("immediate").exists());
                merging_dir.merge_files(std::fs::File::create(dir_guard.path().join("merged"))?)?;
                assert!(!dir_guard.path().join("immediate").exists());
                assert!(dir_guard.path().join("merged").exists());
                MergedFileAsDirectory::from_mmap_file(dir_guard.path().join("merged"))
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_write_read_mmap_merge_via_buffer() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;

        test_write_read_(
            || MergedFileFromMmapDirectory::new(&dir_guard.path().join("immediate")),
            |merging_dir| {
                let buffer = merging_dir.merge_files_to_buffer()?;
                MergedFileAsDirectory::from_buffer(buffer)
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_write_read_ram_merge_via_buffer() -> Result<()> {
        test_write_read_(
            || Ok(MergedFileFromRamDirectory::new()),
            |merging_dir| {
                let buffer = merging_dir.merge_files_to_buffer()?;
                MergedFileAsDirectory::from_buffer(buffer)
            },
        )?;

        Ok(())
    }
}
