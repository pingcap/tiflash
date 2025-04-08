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
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use tantivy::schema::{Field, Schema};

use crate::MergedFileFromDirectory;

static INDEX_IMMEDIATE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub struct TantivyIndexWriter {
    index_writer: tantivy::SingleSegmentIndexWriter,
    field_body: Field,
}

impl TantivyIndexWriter {
    pub fn new(tokenizer_name: &str, dir: Box<dyn tantivy::Directory>) -> Result<Self> {
        let mut schema_builder = Schema::builder();
        let field_body = schema_builder.add_text_field(
            "body",
            tantivy::schema::TextOptions::default()
                .set_indexing_options(
                    tantivy::schema::TextFieldIndexing::default()
                        .set_tokenizer(tokenizer_name)
                        .set_fieldnorms(true)
                        .set_index_option(
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                        ),
                )
                .set_stored(), // TODO: No need to store
        );
        let schema = schema_builder.build();
        let index_writer = tantivy::IndexBuilder::new()
            .tokenizers(crate::tokenizer::TOKENIZERS.clone())
            .schema(schema)
            .single_segment_index_writer(dir, 32_000_000_000)?;
        Ok(Self {
            index_writer,
            field_body,
        })
    }

    pub fn add_document(&mut self, body: &str) -> Result<()> {
        self.index_writer
            .add_document(tantivy::doc!( self.field_body => body ))?;
        Ok(())
    }

    pub fn add_null(&mut self) -> Result<()> {
        self.index_writer.add_document(tantivy::doc!())?;
        Ok(())
    }

    pub fn finalize(self) -> Result<tantivy::Index> {
        Ok(self.index_writer.finalize()?)
    }
}

pub struct IndexWriterOnDisk {
    index_path: PathBuf,
    index_immediate_path: PathBuf,
    merging_directory: Option<crate::MergedFileFromMmapDirectory>,
    internal_writer: Option<TantivyIndexWriter>,
}

impl IndexWriterOnDisk {
    /// Creates a new `IndexWriterOnDisk` for on-disk index.
    /// If the index file already exists, it will be overwritten.
    /// Immediate index files will be all stored on disk before merging into a single file.
    pub fn new(tokenizer_name: &str, index_path: &str) -> Result<Self> {
        let immediate_path = PathBuf::from(format!(
            "{}-immediate-{}-{}-{}",
            index_path,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
            INDEX_IMMEDIATE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            std::iter::repeat_with(fastrand::alphanumeric)
                .take(10)
                .collect::<String>()
        ));
        if immediate_path.exists() {
            // This shall not happen.
            bail!(
                "Failed to create immediate directory {}: already exists",
                immediate_path.display()
            );
        }

        let merging_dir = crate::MergedFileFromMmapDirectory::new(&immediate_path)?;
        let dir = merging_dir.directory().box_clone();
        Ok(Self {
            index_path: PathBuf::from(index_path),
            index_immediate_path: immediate_path,
            merging_directory: Some(merging_dir),
            internal_writer: Some(TantivyIndexWriter::new(tokenizer_name, dir)?),
        })
    }

    /// Adds a document to the index.
    /// The first document will have ID = 0, the second will have ID = 1, and so on.
    /// Parameters use simple primitive types for FFI compatibility.
    pub fn add_document(&mut self, body: &str) -> Result<()> {
        self.internal_writer
            .as_mut()
            .ok_or_else(|| anyhow!("IndexWriterOnDisk is already finalized"))?
            .add_document(body)
    }

    /// Adds a null document to the index, which will also occupy an ID.
    pub fn add_null(&mut self) -> Result<()> {
        self.internal_writer
            .as_mut()
            .ok_or_else(|| anyhow!("IndexWriterOnDisk is already finalized"))?
            .add_null()
    }

    /// Finalizes the index. If this function is not called before drop, the index will be discarded
    /// and index file will not be actually created.
    pub fn finalize(&mut self) -> Result<()> {
        if self.internal_writer.is_none() || self.merging_directory.is_none() {
            bail!("IndexWriterOnDisk is already finalized");
        }

        self.internal_writer.take().unwrap().finalize()?;

        // Merge directory into a single file.
        let file = fs::File::create(&self.index_path)?;
        self.merging_directory
            .take()
            .unwrap()
            .merge_files(file)
            .with_context(|| {
                format!(
                    "Failed to merge immediate index {} to index file {}",
                    self.index_immediate_path.display(),
                    self.index_path.display()
                )
            })?;

        Ok(())
    }
}

impl Drop for IndexWriterOnDisk {
    fn drop(&mut self) {
        // Ensure internal_writer is dropped before merging_directory.
        // Merging directory will take care of cleaning up.
        drop(self.internal_writer.take());
        drop(self.merging_directory.take());
    }
}

pub struct IndexWriterInMemory {
    merging_directory: Option<crate::MergedFileFromRamDirectory>,
    internal_writer: Option<TantivyIndexWriter>,
}

impl IndexWriterInMemory {
    /// Creates a new `IndexWriterInMemory` for in-memory index. The index will be stored in memory and will not be persisted to disk.
    pub fn new(tokenizer_name: &str) -> Result<Self> {
        let merging_dir = crate::MergedFileFromRamDirectory::new();
        let dir = merging_dir.directory().box_clone();
        Ok(Self {
            merging_directory: Some(merging_dir),
            internal_writer: Some(TantivyIndexWriter::new(tokenizer_name, dir)?),
        })
    }

    /// Adds a document to the index.
    /// The first document will have ID = 0, the second will have ID = 1, and so on.
    /// Parameters use simple primitive types for FFI compatibility.
    pub fn add_document(&mut self, body: &str) -> Result<()> {
        if self.internal_writer.is_none() {
            bail!("IndexWriterInMemory is already finalized");
        }
        self.internal_writer.as_mut().unwrap().add_document(body)
    }

    /// Adds a null document to the index, which will also occupy an ID.
    pub fn add_null(&mut self) -> Result<()> {
        if self.internal_writer.is_none() {
            bail!("IndexWriterInMemory is already finalized");
        }
        self.internal_writer.as_mut().unwrap().add_null()
    }

    /// Finalizes the index. If this function is not called before drop, the index will be discarded
    /// and index file will not be actually created.
    pub fn finalize(&mut self) -> Result<Vec<u8>> {
        if self.internal_writer.is_none() || self.merging_directory.is_none() {
            bail!("IndexWriterInMemory is already finalized");
        }

        self.internal_writer.take().unwrap().finalize()?;

        // Merge directory into a single file.
        let buffer = self
            .merging_directory
            .take()
            .unwrap()
            .merge_files_to_buffer()?;

        Ok(buffer)
    }
}

impl Drop for IndexWriterInMemory {
    fn drop(&mut self) {
        // Ensure internal_writer is dropped before merging_directory.
        // Merging directory will take care of cleaning up.
        drop(self.internal_writer.take());
        drop(self.merging_directory.take());
    }
}

/// For FFI
fn new_disk_index_writer(tokenizer_name: &str, index_path: &str) -> Result<Box<IndexWriterOnDisk>> {
    let instance = IndexWriterOnDisk::new(tokenizer_name, index_path)?;
    Ok(Box::new(instance))
}

/// For FFI
fn new_memory_index_writer(tokenizer_name: &str) -> Result<Box<IndexWriterInMemory>> {
    let instance = IndexWriterInMemory::new(tokenizer_name)?;
    Ok(Box::new(instance))
}

#[cxx::bridge(namespace = "ClaraFTS")]
mod ffi {

    extern "Rust" {
        type IndexWriterOnDisk;

        type IndexWriterInMemory;

        fn new_disk_index_writer(
            tokenizer_name: &str,
            index_path: &str,
        ) -> Result<Box<IndexWriterOnDisk>>;

        fn add_document(self: &mut IndexWriterOnDisk, body: &str) -> Result<()>;

        fn add_null(self: &mut IndexWriterOnDisk) -> Result<()>;

        fn finalize(self: &mut IndexWriterOnDisk) -> Result<()>;

        fn new_memory_index_writer(tokenizer_name: &str) -> Result<Box<IndexWriterInMemory>>;

        fn add_document(self: &mut IndexWriterInMemory, body: &str) -> Result<()>;

        fn add_null(self: &mut IndexWriterInMemory) -> Result<()>;

        fn finalize(self: &mut IndexWriterInMemory) -> Result<Vec<u8>>;
    }
}

// Unit tests are placed in index_reader.rs
