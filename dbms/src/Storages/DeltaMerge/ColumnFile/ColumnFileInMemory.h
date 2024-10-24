// Copyright 2023 PingCAP, Inc.
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

#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/Remote/Serializer_fwd.h>

namespace DB
{
namespace DM
{
class ColumnFileInMemory;
using ColumnFileInMemoryPtr = std::shared_ptr<ColumnFileInMemory>;

/// A column file which is only resides in memory
class ColumnFileInMemory : public ColumnFile
{
    friend class ColumnFileInMemoryReader;
    friend struct Remote::Serializer;

private:
    ColumnFileSchemaPtr schema;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    // whether this instance can append any more data.
    bool disable_append = false;

    // The cache data in memory.
    CachePtr cache;

private:
    void fillColumns(const ColumnDefines & col_defs, size_t col_count, Columns & result) const;

    const DataTypePtr & getDataType(ColId column_id) const { return schema->getDataType(column_id); }

public:
    explicit ColumnFileInMemory(const ColumnFileSchemaPtr & schema_, const CachePtr & cache_ = nullptr)
        : schema(schema_)
        , cache(cache_ ? cache_ : std::make_shared<Cache>(schema_->getSchema()))
    {
        rows = cache->block.rows();
        bytes = cache->block.bytes();
    }

    // For deserializing a ColumnFileInMemory object without schema and data in deserializeCFInMemory.
    explicit ColumnFileInMemory(UInt64 rows_)
        : rows(rows_)
    {}

    Type getType() const override { return Type::INMEMORY_FILE; }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; }

    CachePtr getCache() { return cache; }

    /// The schema of this pack.
    ColumnFileSchemaPtr getSchema() const { return schema; }

    ColumnFileInMemoryPtr clone() { return std::make_shared<ColumnFileInMemory>(*this); }

    ColumnFileReaderPtr getReader(
        const DMContext & context,
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnDefinesPtr & col_defs,
        ReadTag) const override;

    bool isAppendable() const override { return !disable_append; }
    void disableAppend() override { disable_append = true; }
    bool append(const DMContext & dm_context, const Block & data, size_t offset, size_t limit, size_t data_bytes)
        override;

    Block readDataForFlush() const;

    bool mayBeFlushedFrom(ColumnFile *) const override { return false; }

    String toString() const override
    {
        String s = "{in_memory_file,rows:" + DB::toString(rows) //
            + ",bytes:" + DB::toString(bytes) //
            + ",disable_append:" + DB::toString(disable_append) //
            + ",schema:" + (schema ? schema->toString() : "none") //
            + ",cache_block:" + (cache ? cache->block.dumpStructure() : "none") + "}";
        return s;
    }
};


class ColumnFileInMemoryReader : public ColumnFileReader
{
private:
    const ColumnFileInMemory & memory_file;
    const ColumnDefinesPtr col_defs;

    Columns cols_data_cache;
    bool read_done = false;

public:
    ColumnFileInMemoryReader(
        const ColumnFileInMemory & memory_file_,
        const ColumnDefinesPtr & col_defs_,
        const Columns & cols_data_cache_)
        : memory_file(memory_file_)
        , col_defs(col_defs_)
        , cols_data_cache(cols_data_cache_)
    {}

    ColumnFileInMemoryReader(const ColumnFileInMemory & memory_file_, const ColumnDefinesPtr & col_defs_)
        : memory_file(memory_file_)
        , col_defs(col_defs_)
    {}

    /// This is a ugly hack to fast return PK & Version column.
    ColumnPtr getPKColumn();
    ColumnPtr getVersionColumn();

    std::pair<size_t, size_t> readRows(
        MutableColumns & output_cols,
        size_t rows_offset,
        size_t rows_limit,
        const RowKeyRange * range) override;

    Block readNextBlock() override;

    size_t skipNextBlock() override;

    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs, ReadTag) override;
};

} // namespace DM
} // namespace DB
