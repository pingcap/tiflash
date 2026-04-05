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

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tantivy::directory::{self, OwnedBytes};

use crate::{MergedFileEncoder, TrackedDirectory};

/// Produce a MergedFile to a buffer or file over the files in a blank `Directory`.
pub trait MergedFileFromDirectory {
    // Note: this is not a object-safe trait, it cannot be boxed.
    // This trait is mostly useful in tests to test against different implementations.

    /// Merge all files in the directory to the writer.
    fn merge_files<W: io::Write + io::Seek>(self, writer: W) -> Result<W>
    where
        Self: Sized;

    /// Merge all files in the directory to a buffer.
    fn merge_files_to_buffer(self) -> Result<Vec<u8>>
    where
        Self: Sized,
    {
        let cursor = self.merge_files(io::Cursor::new(Vec::new()))?;
        Ok(cursor.into_inner())
    }

    /// Access the directory which will be merged.
    fn directory(&self) -> &dyn directory::Directory;
}

/// Based on a non-exist directory on FileSystem. A MmapDirectory is provided
/// for accessing the files in the directory.
///
/// After all access is done, a MergedFile could be produced over the files in the directory
/// by calling `merge_files` and the directory will be removed.
///
/// Before merging, caller should ensure that other directory references are
/// dropped. Otherwise merging will be rejected.
pub struct MergedFileFromMmapDirectory {
    dir_path: PathBuf,
    tracked_directory: Option<TrackedDirectory<directory::MmapDirectory>>,
}

impl MergedFileFromMmapDirectory {
    pub fn new(dir_path: &Path) -> Result<Self> {
        if dir_path.exists() {
            // We expect an empty directory. Otherwise TrackedDirectory will not work correctly.
            bail!(
                "Failed to create directory {}: already exists",
                dir_path.display()
            );
        }
        fs::create_dir_all(dir_path)
            .with_context(|| format!("Failed to create directory {}", dir_path.display()))?;

        let directory = directory::MmapDirectory::open(dir_path)?;
        let tracked_directory = TrackedDirectory::wrap(directory);

        Ok(Self {
            dir_path: dir_path.to_owned(),
            tracked_directory: Some(tracked_directory),
        })
    }

    pub fn tracked_directory(&self) -> &TrackedDirectory<directory::MmapDirectory> {
        self.tracked_directory.as_ref().unwrap()
    }
}

impl MergedFileFromDirectory for MergedFileFromMmapDirectory {
    fn merge_files<W: io::Write + io::Seek>(mut self, writer: W) -> Result<W> {
        // We require callers to ensure all directory references are dropped before merging files.
        // Otherwise after we merging all files in the directory, the directory may be still
        // accessed by someone else, causing problems.
        if self.tracked_directory().reference_count() > 1 {
            bail!(
                "The underlying directory is still referenced somewhere else. These references must be dropped before merging files to prove that the directory will not be ever used (ReferenceCount: {}, Directory: {})",
                self.tracked_directory().reference_count(),
                self.dir_path.display()
            );
        }

        let mut w = MergedFileEncoder::new(writer);
        for file_name in self.tracked_directory().all_files() {
            let file_path = self.dir_path.join(&file_name);
            if file_path.is_dir() {
                bail!("Subdirectory is not supported: {}", file_path.display());
            }
            w.include_file_from_fs(file_path)?;
        }

        let inner_writer = w.finalize().with_context(|| {
            format!(
                "Failed to finalize MergedFile from source directory {}",
                self.dir_path.display()
            )
        })?;

        // Unmap before we remove the directory.
        let d = self.tracked_directory.take();
        drop(d);

        // Completely remove the source directory after all files are merged, because
        // the source directory is created by us.
        // It is user's fault if there are concurrent writes to the source directory during the merge
        // and we will not handle this case.
        _ = fs::remove_dir_all(&self.dir_path);

        Ok(inner_writer)
    }

    fn directory(&self) -> &dyn directory::Directory {
        self.tracked_directory.as_ref().unwrap()
    }
}

impl Drop for MergedFileFromMmapDirectory {
    fn drop(&mut self) {
        // Note: self.directory may be none, or not. We simply drop it before removing the directory
        // so that possible usages are dropped.
        let d = self.tracked_directory.take();
        drop(d);

        // Drop everything anyway, even if merge_files is not called.
        // and we don't care about delete errors.
        _ = fs::remove_dir_all(&self.dir_path);
    }
}

/// Based on a virtual in-memory directory. A RamDirectory is provided
/// for accessing the files in the directory. All files are stored in memory.
///
/// After all access is done, a MergedFile could be produced over the files in the directory
/// by calling `merge_files` and the directory will be removed.
///
/// Before merging, caller should ensure that other directory references are
/// dropped. Otherwise merging will be rejected.
pub struct MergedFileFromRamDirectory {
    tracked_directory: TrackedDirectory<directory::RamDirectory>,
}

impl Default for MergedFileFromRamDirectory {
    fn default() -> Self {
        Self::new()
    }
}

impl MergedFileFromRamDirectory {
    pub fn new() -> Self {
        let directory = directory::RamDirectory::default();
        let tracked_directory = TrackedDirectory::wrap(directory);
        Self { tracked_directory }
    }

    pub fn tracked_directory(&self) -> &TrackedDirectory<directory::RamDirectory> {
        &self.tracked_directory
    }
}

impl MergedFileFromDirectory for MergedFileFromRamDirectory {
    fn merge_files<W>(self, writer: W) -> Result<W>
    where
        W: io::Write + io::Seek,
    {
        use directory::Directory;

        // We require callers to ensure all directory references are dropped before merging files.
        // Otherwise after we merging all files in the directory, the directory may be still
        // accessed by someone else, causing problems.
        if self.tracked_directory.reference_count() > 1 {
            bail!(
                "The underlying directory is still referenced somewhere else. These references must be dropped before merging files to prove that the directory will not be ever used (ReferenceCount: {})",
                self.tracked_directory.reference_count()
            );
        }

        let mut w = MergedFileEncoder::<_, OwnedBytes>::new(writer);
        let all_files_snapshot = self.tracked_directory.all_files();
        for file_name in &all_files_snapshot {
            let src_file = self
                .tracked_directory
                .open_read(file_name)
                .with_context(|| {
                    // Happens when there are concurrent direct access to the Directory
                    format!("Failed to open in-memory file {}", file_name.display())
                })?;
            let file_data = src_file.read_bytes().with_context(|| {
                // Happens when there are concurrent direct access to the Directory
                format!("Failed to read in-memory file {}", file_name.display())
            })?;
            w.include_file_from_buffer(file_name.clone(), file_data);
        }

        let inner_writer = w.finalize().context("Failed to finalize MergedFile")?;

        // Just a protection, in case of there is still some dangling reference to the underlying directory.
        // This could happen even when we checked reference count is 1, because
        // each sub-file is an Arc and can be kept alive by someone else.
        for file_name in &all_files_snapshot {
            // We don't care about delete errors from an in-memory directory.
            _ = self.tracked_directory.delete(file_name);
        }

        Ok(inner_writer)
    }

    fn directory(&self) -> &dyn directory::Directory {
        &self.tracked_directory
    }
}

#[cfg(test)]
mod tests {

    use fs::File;
    use io::{Read, Seek};

    use super::*;
    use crate::MergedFileDecoder;

    #[test]
    fn test_mmap() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;

        let merging_dir = MergedFileFromMmapDirectory::new(&dir_guard.path().join("immediate"))?;
        merging_dir.directory().atomic_write(
            Path::new("file1"),
            b"Who knew the people would adore me so much?",
        )?;
        merging_dir.directory().atomic_write(
            Path::new("file2"),
            b"Being too popular can be such a hassle",
        )?;
        merging_dir.directory().sync_directory()?;
        assert!(dir_guard.path().join("immediate").exists());

        merging_dir.merge_files(File::create(dir_guard.path().join("merged"))?)?;

        // The source directory should be dropped after merging.
        assert!(!dir_guard.path().join("immediate").exists());

        let decoder = MergedFileDecoder::from_mmap_file(dir_guard.path().join("merged"))?;
        assert_eq!(
            decoder.get(Path::new("file1")).unwrap().as_slice(),
            b"Who knew the people would adore me so much?"
        );
        assert_eq!(
            decoder.get(Path::new("file2")).unwrap().as_slice(),
            b"Being too popular can be such a hassle"
        );
        let mut file_names = decoder.list_files();
        file_names.sort();
        assert_eq!(
            file_names,
            vec![PathBuf::from("file1"), PathBuf::from("file2")]
        );

        Ok(())
    }

    #[test]
    fn test_mmap_abandon() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;

        let merging_dir = MergedFileFromMmapDirectory::new(&dir_guard.path().join("immediate"))?;
        merging_dir.directory().atomic_write(
            Path::new("file1"),
            b"Who knew the people would adore me so much?",
        )?;
        merging_dir.directory().atomic_write(
            Path::new("file2"),
            b"Being too popular can be such a hassle",
        )?;
        merging_dir.directory().sync_directory()?;
        assert!(dir_guard.path().join("immediate").exists());

        drop(merging_dir);

        // The source directory should be dropped even without merging.
        assert!(!dir_guard.path().join("immediate").exists());

        Ok(())
    }

    #[test]
    fn test_ram() -> Result<()> {
        let merging_dir = MergedFileFromRamDirectory::new();
        merging_dir.directory().atomic_write(
            Path::new("file1"),
            b"Who knew the people would adore me so much?",
        )?;
        merging_dir.directory().atomic_write(
            Path::new("file2"),
            b"Being too popular can be such a hassle",
        )?;

        let merged_file = merging_dir.merge_files_to_buffer()?;

        let decoder = MergedFileDecoder::from_buffer(merged_file)?;
        assert_eq!(
            decoder.get(Path::new("file1")).unwrap().as_slice(),
            b"Who knew the people would adore me so much?"
        );
        assert_eq!(
            decoder.get(Path::new("file2")).unwrap().as_slice(),
            b"Being too popular can be such a hassle"
        );
        let mut file_names = decoder.list_files();
        file_names.sort();
        assert_eq!(
            file_names,
            vec![PathBuf::from("file1"), PathBuf::from("file2")]
        );

        Ok(())
    }

    #[test]
    fn test_merge_to_file_or_buffer() -> Result<()> {
        let merging_dir = MergedFileFromRamDirectory::new();
        merging_dir.directory().atomic_write(
            Path::new("file1"),
            b"Who knew the people would adore me so much?",
        )?;

        let mut file = merging_dir.merge_files(tempfile::tempfile()?)?;
        let mut file_content = Vec::<u8>::new();
        file.seek(io::SeekFrom::Start(0))?;
        file.read_to_end(&mut file_content)?;

        let merging_dir = MergedFileFromRamDirectory::new();
        merging_dir.directory().atomic_write(
            Path::new("file1"),
            b"Who knew the people would adore me so much?",
        )?;

        let buf = merging_dir.merge_files_to_buffer()?;

        assert_eq!(buf, file_content);

        Ok(())
    }

    #[test]
    fn test_mmap_alive_directory_reference() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;

        let merging_dir = MergedFileFromMmapDirectory::new(&dir_guard.path().join("immediate"))?;
        merging_dir.directory().atomic_write(
            Path::new("file1"),
            b"Who knew the people would adore me so much?",
        )?;

        // Keep a reference to the directory alive when calling merge_files.
        let directory = merging_dir.directory().box_clone();

        // Merging should be rejected because there is still a reference to the directory.
        assert!(merging_dir.merge_files_to_buffer().is_err());

        drop(directory);

        Ok(())
    }

    #[test]
    fn test_ram_alive_directory_reference() -> Result<()> {
        let merging_dir = MergedFileFromRamDirectory::new();
        merging_dir.directory().atomic_write(
            Path::new("file1"),
            b"Who knew the people would adore me so much?",
        )?;

        // Keep a reference to the directory alive when calling merge_files.
        let directory = merging_dir.directory().box_clone();

        // Merging should be rejected because there is still a reference to the directory.
        assert!(merging_dir.merge_files_to_buffer().is_err());

        drop(directory);

        Ok(())
    }

    #[test]
    fn test_mmap_empty_directory() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;

        let merging_dir = MergedFileFromMmapDirectory::new(&dir_guard.path().join("immediate"))?;

        let merged_file = merging_dir.merge_files_to_buffer()?;

        // Decode should success
        let decoder = MergedFileDecoder::from_buffer(merged_file)?;
        assert_eq!(decoder.list_files(), Vec::<PathBuf>::new());

        Ok(())
    }

    #[test]
    fn test_ram_empty_directory() -> Result<()> {
        let merging_dir = MergedFileFromRamDirectory::new();

        let merged_file = merging_dir.merge_files_to_buffer()?;

        // Decode should success
        let decoder = MergedFileDecoder::from_buffer(merged_file)?;
        assert_eq!(decoder.list_files(), Vec::<PathBuf>::new());

        Ok(())
    }
}
