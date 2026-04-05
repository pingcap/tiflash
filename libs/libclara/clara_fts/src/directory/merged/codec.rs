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

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use memmap2::Mmap;
use prost::Message;
use tantivy::directory::OwnedBytes;

fn align_to_8_bytes(n: u64) -> u64 {
    (n + 7) / 8 * 8
}

/// Wraps Mmap to implement StableDeref
struct MmapStableDeref(Mmap);

impl std::ops::Deref for MmapStableDeref {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

/// The mmap's memory is stable, so it's safe to implement StableDeref.
unsafe impl stable_deref_trait::StableDeref for MmapStableDeref {}

/// Encodes a MergedFile based on source files. The source files must not contain subdirectories.
/// The layout of the merged file is as follows:
/// - 4 bytes: length of serialized meta, in LittleEndian
/// - N bytes: serialized meta (in protobuf)
/// - N bytes: the content of all files
///            note: the offset of each file is aligned to 8 bytes boundary
///
/// Files in the merged file is ordered by the file size.
///
/// Caller must call `finalize` to actually write the merged file. Otherwise
/// the operation is abandoned.
pub struct MergedFileEncoder<W: io::Write + io::Seek, R: io::Read> {
    writer: W,
    buffered_files: Vec<(PathBuf, R, u64)>,
}

impl<W: io::Write + io::Seek> MergedFileEncoder<W, File> {
    pub fn include_file_from_fs(&mut self, file_path: impl AsRef<Path>) -> Result<()> {
        let file_path = file_path.as_ref();
        let file_name = file_path.file_name().with_context(|| {
            format!("Failed to get file name from file {}", file_path.display())
        })?;
        let file_size = file_path
            .metadata()
            .with_context(|| format!("Failed to get metadata of file {}", file_path.display()))?
            .len();
        if file_path.is_dir() {
            bail!(
                "MergedFileEncoder does not support directory: {}",
                file_path.display()
            );
        }
        let file = File::open(file_path)
            .with_context(|| format!("Failed to open file {}", file_path.display()))?;
        self.include_file(PathBuf::from(file_name.to_owned()), file, file_size);
        Ok(())
    }
}

impl<W: io::Write + io::Seek> MergedFileEncoder<W, io::Cursor<Vec<u8>>> {
    pub fn include_file_from_vec(&mut self, file_name: PathBuf, buffer: Vec<u8>) {
        let size = buffer.len() as u64;
        let reader = io::Cursor::new(buffer);
        self.include_file(file_name, reader, size);
    }
}

impl<W: io::Write + io::Seek> MergedFileEncoder<W, OwnedBytes> {
    pub fn include_file_from_buffer(&mut self, file_name: PathBuf, buffer: OwnedBytes) {
        let size = buffer.len() as u64;
        self.include_file(file_name, buffer, size);
    }
}

impl<W: io::Write + io::Seek, R: io::Read> MergedFileEncoder<W, R> {
    pub fn new(writer: W) -> Self {
        MergedFileEncoder {
            writer,
            buffered_files: Vec::new(),
        }
    }

    pub fn include_file(&mut self, file_name: PathBuf, file_reader: R, file_size: u64) {
        self.buffered_files
            .push((file_name, file_reader, file_size));
    }

    pub fn finalize(mut self) -> Result<W> {
        // Currently let's only accept UTF8 file names for simplicity
        for (file_name, _, _) in self.buffered_files.iter() {
            let file_name_utf8 = file_name.to_str();
            if file_name_utf8.is_none() {
                bail!(
                    "Meet file {} with invalid UTF8 path",
                    file_name.to_string_lossy()
                );
            }
        }

        // smallest file first
        self.buffered_files
            .sort_by(|(_, _, a_size), (_, _, b_size)| a_size.cmp(b_size));

        let mut meta = super::proto::MergedFileMeta::default();

        for (file_name, _, size) in self.buffered_files.iter_mut() {
            // Move file_name out and use it directly in the proto to reduce alloc and copy.
            let file_name_takeout = std::mem::take(file_name);

            meta.file_names
                .push(file_name_takeout.into_os_string().into_string().unwrap()); // We have already checked the file name is valid UTF8.
            meta.sizes.push(*size);
            // `offsets` field is fixed64, so we can fill it later without affecting serialized size.
            meta.offsets.push(0);
        }

        // Note: Now buffered_files[*].0 is empty. Don't use it.

        // this serialized size is accurate although `offsets` are not filled, because
        // `offsets` are fixed64.
        let meta_len = meta.encoded_len();
        if meta_len > u32::MAX as usize {
            bail!("Unexpected meta serialized size {meta_len} exceeds u32::MAX");
        }

        let data_offset_begin = align_to_8_bytes(4 /* length prefix */ + meta_len as u64);
        let mut data_offset = data_offset_begin;
        for i in 0..meta.file_names.len() {
            meta.offsets[i] = data_offset;
            data_offset += meta.sizes[i];
            data_offset = align_to_8_bytes(data_offset);
        }

        // Just in case of mistakes when we adding more fields to meta.
        let meta_len_actual = meta.encoded_len();
        if meta_len != meta_len_actual {
            bail!("Unexpected meta serialized size changed after filling offsets");
        }

        // now meta is prepared, start writing to the file.
        // TODO: Support EncryptionAtRest.

        // 1. write the length of meta in LittleEndian
        self.writer.write_all(&(meta_len as u32).to_le_bytes())?;
        // 2. serialize meta into file
        // TODO: Discover ways to serialize into file directly without allocating a new buffer
        let encoded = meta.encode_to_vec();
        self.writer.write_all(&encoded)?;

        // 3. write the content of all files at the correct offset
        for i in 0..meta.file_names.len() {
            self.writer
                .seek(std::io::SeekFrom::Start(meta.offsets[i]))?;
            let n_copied = std::io::copy(&mut self.buffered_files[i].1, &mut self.writer)
                .with_context(|| format!("Failed to merge file {}", meta.file_names[i]))?;
            if n_copied != meta.sizes[i] {
                bail!(
                    "Actual data length of file {} is not matching, expected {}, actual {}",
                    meta.file_names[i],
                    meta.sizes[i],
                    n_copied
                );
            }
        }

        Ok(self.writer)
    }
}

/// Decodes a MergedFile from either an in-memory buffer or a mmaped file buffer.
pub struct MergedFileDecoder {
    data: OwnedBytes,
    files_meta: HashMap<PathBuf, MergedFileMeta>,
}

struct MergedFileMeta {
    file_size: usize,
    offset: usize,
}

impl MergedFileDecoder {
    pub fn from_mmap_file(file_path: impl AsRef<Path>) -> Result<Self> {
        let file_path = file_path.as_ref();
        if !file_path.exists() {
            bail!("File {} does not exist", file_path.display());
        }

        let file = File::open(file_path)
            .with_context(|| format!("Failed to open file {}", file_path.display()))?;
        let mmap = unsafe {
            Mmap::map(&file)
                .with_context(|| format!("Failed to map file {}", file_path.display()))?
        };

        let bytes = OwnedBytes::new(MmapStableDeref(mmap));
        let decoder = MergedFileDecoder::new(bytes)
            .with_context(|| format!("Failed to decode merged file {}", file_path.display()))?;
        Ok(decoder)
    }

    pub fn from_buffer(buffer: Vec<u8>) -> Result<Self> {
        let bytes = OwnedBytes::new(buffer);
        MergedFileDecoder::new(bytes)
    }

    pub fn new(data_owned: OwnedBytes) -> Result<Self> {
        let data = data_owned.as_slice();
        if data.len() < 4 {
            bail!("Invalid merged file: expect meta length prefix");
        }
        // 1. read the length of meta in LittleEndian
        let meta_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + meta_len {
            bail!("Invalid merged file: expect meta with {} bytes", meta_len);
        }

        // 2. deserialize meta
        let meta = super::proto::MergedFileMeta::decode(&data[4..4 + meta_len])
            .context("Invalid merged file: failed to decode meta")?;

        // TODO: 3. verify file integrity

        // 4. build files_meta
        let mut files_meta =
            HashMap::<PathBuf, MergedFileMeta>::with_capacity(meta.file_names.len());
        for (i, file_name) in meta.file_names.into_iter().enumerate() {
            if meta.offsets[i] + meta.sizes[i] > data.len() as u64 {
                bail!("Invalid merged file: subfile {} exceeds EOF", file_name);
            }
            files_meta.insert(
                PathBuf::from(file_name),
                MergedFileMeta {
                    file_size: meta.sizes[i] as usize,
                    offset: meta.offsets[i] as usize,
                },
            );
        }

        Ok(MergedFileDecoder {
            data: data_owned,
            files_meta,
        })
    }

    pub fn get(&self, file_name: impl AsRef<Path>) -> Option<OwnedBytes> {
        let meta = self.files_meta.get(file_name.as_ref())?;
        Some(self.data.slice(meta.offset..(meta.offset + meta.file_size)))
    }

    pub fn contains(&self, file_name: impl AsRef<Path>) -> bool {
        self.files_meta.contains_key(file_name.as_ref())
    }

    /// List all files in the merged file. The order is unspecified.
    pub fn list_files(&self) -> Vec<PathBuf> {
        self.files_meta.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use io::Cursor;

    #[test]
    fn test_encode_into_different_writer() -> Result<()> {
        use io::Read;
        use io::Seek;

        let mut encoder = MergedFileEncoder::new(Cursor::new(Vec::new()));
        encoder.include_file_from_vec(
            PathBuf::from("file1"),
            Vec::from(b"Who knew the people would adore me so much?"),
        );
        encoder.include_file_from_vec(
            PathBuf::from("file2"),
            Vec::from(b"Being too popular can be such a hassle"),
        );
        let buf = encoder.finalize()?.into_inner();

        let file = tempfile::tempfile()?;
        let mut encoder_2 = MergedFileEncoder::new(file);
        encoder_2.include_file_from_vec(
            PathBuf::from("file1"),
            Vec::from(b"Who knew the people would adore me so much?"),
        );
        encoder_2.include_file_from_vec(
            PathBuf::from("file2"),
            Vec::from(b"Being too popular can be such a hassle"),
        );
        let mut file = encoder_2.finalize()?;
        let mut file_data = Vec::new();
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_to_end(&mut file_data)?;
        assert_eq!(buf, file_data);

        Ok(())
    }

    #[test]
    fn test_encode_from_different_reader() -> Result<()> {
        let mut encoder = MergedFileEncoder::new(Cursor::new(Vec::new()));
        encoder.include_file_from_vec(
            PathBuf::from("file1"),
            Vec::from(b"Who knew the people would adore me so much?"),
        );
        let buf = encoder.finalize()?.into_inner();

        let mut encoder_2 = MergedFileEncoder::new(Cursor::new(Vec::new()));
        encoder_2.include_file_from_buffer(
            PathBuf::from("file1"),
            OwnedBytes::new(Vec::from(b"Who knew the people would adore me so much?")),
        );
        let buf_2 = encoder_2.finalize()?.into_inner();

        let mut encoder_3 = MergedFileEncoder::new(Cursor::new(Vec::new()));
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("file1"),
            b"Who knew the people would adore me so much?",
        )?;
        encoder_3.include_file_from_fs(dir.path().join("file1"))?;
        let buf_3 = encoder_3.finalize()?.into_inner();

        assert_eq!(buf, buf_2);
        assert_eq!(buf, buf_3);

        Ok(())
    }

    fn test_encode_decode_<EncoderW, EncoderFn, FinalizeFn>(
        get_encoder: EncoderFn,
        finalize: FinalizeFn,
    ) -> Result<()>
    where
        EncoderW: io::Write + io::Seek,
        EncoderFn: Fn() -> MergedFileEncoder<EncoderW, Cursor<Vec<u8>>>,
        FinalizeFn: Fn(MergedFileEncoder<EncoderW, Cursor<Vec<u8>>>) -> Result<MergedFileDecoder>,
    {
        // basic write & read
        let mut encoder = get_encoder();
        encoder.include_file_from_vec(
            PathBuf::from("file1"),
            Vec::from(b"Who knew the people would adore me so much?"),
        );
        encoder.include_file_from_vec(
            PathBuf::from("file2"),
            Vec::from(b"Being too popular can be such a hassle"),
        );

        let decoder = finalize(encoder)?;
        assert_eq!(
            decoder.get(Path::new("file1")).unwrap().as_slice(),
            b"Who knew the people would adore me so much?"
        );
        assert_eq!(
            decoder.get(Path::new("file2")).unwrap().as_slice(),
            b"Being too popular can be such a hassle"
        );
        assert!(decoder.get(Path::new("file3")).is_none());

        let mut files = decoder.list_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("file1"), PathBuf::from("file2")]);

        // empty write, read directly
        let encoder = get_encoder();
        let decoder = finalize(encoder)?;
        assert!(decoder.get(Path::new("file1")).is_none());
        assert!(decoder.get(Path::new("file2")).is_none());
        assert!(decoder.get(Path::new("file3")).is_none());

        let files = decoder.list_files();
        assert!(files.is_empty());

        Ok(())
    }

    #[test]
    fn test_encode_decode_in_memory() -> Result<()> {
        test_encode_decode_(
            || MergedFileEncoder::new(Cursor::new(Vec::new())),
            |encoder| {
                let encoded_data = encoder.finalize()?;
                MergedFileDecoder::from_buffer(encoded_data.into_inner())
            },
        )
    }

    #[test]
    fn test_encode_decode_file() -> Result<()> {
        // MergedFileEncoder reads from memory buffers
        // MergedFile is on-disk.

        let dir_guard = tempfile::tempdir()?;
        test_encode_decode_(
            || MergedFileEncoder::new(File::create(dir_guard.path().join("immediate")).unwrap()),
            |encoder| {
                encoder.finalize()?;
                MergedFileDecoder::from_mmap_file(dir_guard.path().join("immediate"))
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_encode_decode_file_2() -> Result<()> {
        // MergedFileEncoder reads from files on FS.
        // MergedFile is on-disk.

        let dir_guard = tempfile::tempdir()?;
        std::fs::write(
            dir_guard.path().join("file1"),
            b"Who knew the people would adore me so much?",
        )?;
        std::fs::write(
            dir_guard.path().join("file2"),
            b"Being too popular can be such a hassle",
        )?;

        let mut encoder = MergedFileEncoder::new(File::create(dir_guard.path().join("merged"))?);
        encoder.include_file_from_fs(dir_guard.path().join("file1"))?;
        encoder.include_file_from_fs(dir_guard.path().join("file2"))?;
        encoder.finalize()?;

        let decoder = MergedFileDecoder::from_mmap_file(dir_guard.path().join("merged"))?;
        assert_eq!(
            decoder.get(Path::new("file1")).unwrap().as_slice(),
            b"Who knew the people would adore me so much?"
        );
        assert_eq!(
            decoder.get(Path::new("file2")).unwrap().as_slice(),
            b"Being too popular can be such a hassle"
        );
        assert!(decoder.get(Path::new("file3")).is_none());

        let mut files = decoder.list_files();
        files.sort();
        assert_eq!(files, vec![PathBuf::from("file1"), PathBuf::from("file2")]);

        Ok(())
    }

    #[test]
    fn test_encode_skip_finalize() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;

        let mut encoder = MergedFileEncoder::new(File::create(dir_guard.path().join("merged"))?);
        encoder.include_file_from_vec(
            PathBuf::from("file1"),
            Vec::from(b"Who knew the people would adore me so much?"),
        );
        drop(encoder);

        let path = dir_guard.path().join("merged");
        assert!(path.metadata()?.len() == 0);

        Ok(())
    }

    #[test]
    fn test_encode_include_file_error() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;
        std::fs::write(
            dir_guard.path().join("file1"),
            b"Who knew the people would adore me so much?",
        )?;

        let mut encoder = MergedFileEncoder::new(File::create(dir_guard.path().join("merged"))?);

        encoder.include_file_from_fs(dir_guard.path().join("file1"))?;
        assert!(encoder
            .include_file_from_fs(dir_guard.path().join("non_exist_file"))
            .is_err());
        assert!(encoder.include_file_from_fs(dir_guard.path()).is_err());
        encoder.finalize()?;

        let decoder = MergedFileDecoder::from_mmap_file(dir_guard.path().join("merged"))?;
        assert_eq!(
            decoder.get(Path::new("file1")).unwrap().as_slice(),
            b"Who knew the people would adore me so much?"
        );
        assert_eq!(decoder.list_files(), vec![PathBuf::from("file1")]);

        Ok(())
    }

    #[test]
    fn test_encode_mismatch_file_size() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;
        std::fs::write(
            dir_guard.path().join("file1"),
            b"Who knew the people would adore me so much?",
        )?;

        let mut encoder = MergedFileEncoder::new(File::create(dir_guard.path().join("merged"))?);

        encoder.include_file_from_fs(dir_guard.path().join("file1"))?;

        // overwrite file1 so that the file size is different
        std::fs::write(
            dir_guard.path().join("file1"),
            b"Being too popular can be such a hassle",
        )?;

        assert!(encoder.finalize().is_err());

        Ok(())
    }

    #[test]
    fn test_decode_not_a_merged_file() -> Result<()> {
        let decoder = MergedFileDecoder::from_buffer(Vec::from(b"not a merged file"));
        assert!(decoder.is_err());
        Ok(())
    }

    #[test]
    fn test_decode_broken_data() -> Result<()> {
        let decoder = MergedFileDecoder::from_buffer(vec![100u8, 100u8, 100u8, 100u8]);
        assert!(decoder.is_err());
        Ok(())
    }

    #[test]
    fn test_decode_mmap_file_not_found() -> Result<()> {
        let dir_guard = tempfile::tempdir()?;
        let decoder = MergedFileDecoder::from_mmap_file(dir_guard.path().join("non_existent"));
        assert!(decoder.is_err());
        Ok(())
    }
}
