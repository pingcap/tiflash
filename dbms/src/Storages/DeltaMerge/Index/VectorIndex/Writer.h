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

#pragma once

#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <IO/Buffer/WriteBuffer.h>
#include <Storages/DeltaMerge/Index/LocalIndexWriter.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Writer_fwd.h>
#include <TiDB/Schema/VectorIndex.h>
#include <VectorSearch/USearch.h>

namespace DB::DM
{

class VectorIndexWriterInternal : public LocalIndexWriter
{
protected:
    using USearchImplType = unum::usearch::index_dense_gt</* key_at */ UInt32, /* compressed_slot_at */ UInt32>;

public:
    /// The key is the row's offset in the DMFile.
    using Key = UInt32;

    static std::shared_ptr<VectorIndexWriterInternal> createInMemory(
        IndexID index_id,
        const TiDB::VectorIndexDefinitionPtr & definition);
    static std::shared_ptr<VectorIndexWriterInternal> createOnDisk(
        std::string_view index_file,
        IndexID index_id,
        const TiDB::VectorIndexDefinitionPtr & definition);

    explicit VectorIndexWriterInternal(IndexID index_id, const TiDB::VectorIndexDefinitionPtr & definition_);

    ~VectorIndexWriterInternal() override;

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override;

    void saveToFile(std::string_view path) const override;
    void saveToBuffer(WriteBuffer & write_buf) const override;
    void saveFilePros(dtpb::IndexFilePropsV2 * pb_idx) const override;

    dtpb::IndexFileKind kind() const override { return dtpb::IndexFileKind::VECTOR_INDEX; }

public:
    const TiDB::VectorIndexDefinitionPtr definition;

private:
    USearchImplType index;
    UInt64 added_rows = 0; // Includes nulls and deletes. Used as the index key.

    mutable double total_duration = 0;
    size_t last_reported_memory_usage = 0;
};

class VectorIndexWriterInMemory : public VectorIndexWriterInternal
{
public:
    explicit VectorIndexWriterInMemory(IndexID index_id, const TiDB::VectorIndexDefinitionPtr & definition_)
        : VectorIndexWriterInternal(index_id, definition_)
    {}
};

// FIXME: The building process is still in-memory. Only the index file is saved to disk.
class VectorIndexWriterOnDisk : public VectorIndexWriterInternal
{
public:
    explicit VectorIndexWriterOnDisk(
        std::string_view index_file_,
        IndexID index_id,
        const TiDB::VectorIndexDefinitionPtr & definition_)
        : VectorIndexWriterInternal(index_id, definition_)
        , index_file(index_file_)
    {}

private:
    const std::string index_file;
};

} // namespace DB::DM
