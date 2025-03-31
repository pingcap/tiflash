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

class VectorIndexWriterInternal
{
    friend class VectorIndexWriterInMemory;
    friend class VectorIndexWriterOnDisk;

protected:
    using USearchImplType = unum::usearch::index_dense_gt</* key_at */ UInt32, /* compressed_slot_at */ UInt32>;

public:
    /// The key is the row's offset in the DMFile.
    using Key = UInt32;

    explicit VectorIndexWriterInternal(const TiDB::VectorIndexDefinitionPtr & definition_);

    ~VectorIndexWriterInternal();

    using ProceedCheckFn = LocalIndexWriter::ProceedCheckFn;
    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed);
    void saveFileProps(dtpb::IndexFilePropsV2 * pb_idx) const;

public:
    const TiDB::VectorIndexDefinitionPtr definition;

private:
    UInt64 added_rows = 0; // Includes nulls and deletes. Used as the index key.
    size_t last_reported_memory_usage = 0;
    USearchImplType index;
    mutable double total_duration = 0;
};

class VectorIndexWriterInMemory : public LocalIndexWriterInMemory
{
public:
    explicit VectorIndexWriterInMemory(IndexID index_id, const TiDB::VectorIndexDefinitionPtr & definition)
        : LocalIndexWriterInMemory(index_id)
        , writer(definition)
    {}

    void saveToBuffer(WriteBuffer & write_buf) override;

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override
    {
        writer.addBlock(column, del_mark, should_proceed);
    }

    void saveFileProps(dtpb::IndexFilePropsV2 * pb_idx) const override { writer.saveFileProps(pb_idx); }

    dtpb::IndexFileKind kind() const override { return dtpb::IndexFileKind::VECTOR_INDEX; }

private:
    VectorIndexWriterInternal writer;
};

// FIXME: The building process is still in-memory. Only the index file is saved to disk.
class VectorIndexWriterOnDisk : public LocalIndexWriterOnDisk
{
public:
    explicit VectorIndexWriterOnDisk(
        IndexID index_id,
        std::string_view index_file,
        const TiDB::VectorIndexDefinitionPtr & definition)
        : LocalIndexWriterOnDisk(index_id, index_file)
        , writer(definition)
    {}

    void saveToFile() const override;

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override
    {
        writer.addBlock(column, del_mark, should_proceed);
    }

    void saveFileProps(dtpb::IndexFilePropsV2 * pb_idx) const override { writer.saveFileProps(pb_idx); }

    dtpb::IndexFileKind kind() const override { return dtpb::IndexFileKind::VECTOR_INDEX; }

private:
    VectorIndexWriterInternal writer;
};

} // namespace DB::DM
