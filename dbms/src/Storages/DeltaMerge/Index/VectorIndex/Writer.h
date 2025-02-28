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
#include <Storages/DeltaMerge/Index/VectorIndex/Writer_fwd.h>
#include <TiDB/Schema/VectorIndex.h>
#include <VectorSearch/USearch.h>

namespace DB::DM
{

class VectorIndexWriterInternal
{
protected:
    using USearchImplType = unum::usearch::index_dense_gt</* key_at */ UInt32, /* compressed_slot_at */ UInt32>;

    void saveToFile(std::string_view path) const;
    void saveToBuffer(WriteBuffer & write_buf) const;

public:
    /// The key is the row's offset in the DMFile.
    using Key = UInt32;
    using ProceedCheckFn = std::function<bool()>;

    explicit VectorIndexWriterInternal(const TiDB::VectorIndexDefinitionPtr & definition_);

    virtual ~VectorIndexWriterInternal();

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed);

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
    explicit VectorIndexWriterInMemory(const TiDB::VectorIndexDefinitionPtr & definition_)
        : VectorIndexWriterInternal(definition_)
    {}

    static VectorIndexWriterInMemoryPtr create(const TiDB::VectorIndexDefinitionPtr & definition)
    {
        return std::make_shared<VectorIndexWriterInMemory>(definition);
    }

    void finalize(WriteBuffer & write_buf) const { saveToBuffer(write_buf); }
};

// FIXME: The building process is still in-memory. Only the index file is saved to disk.
class VectorIndexWriterOnDisk : public VectorIndexWriterInternal
{
public:
    explicit VectorIndexWriterOnDisk(std::string_view index_file, const TiDB::VectorIndexDefinitionPtr & definition_)
        : VectorIndexWriterInternal(definition_)
        , index_file(index_file)
    {}

    static VectorIndexWriterOnDiskPtr create(
        std::string_view index_file,
        const TiDB::VectorIndexDefinitionPtr & definition)
    {
        return std::make_shared<VectorIndexWriterOnDisk>(index_file, definition);
    }

    void finalize() const { saveToFile(index_file); }

private:
    const std::string index_file;
};

} // namespace DB::DM
