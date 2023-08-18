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

namespace DB
{
namespace DM
{
class ColumnFileInMemory;
using ColumnInMemoryFilePtr = std::shared_ptr<ColumnFileInMemory>;

/// A column file which is only resides in memory
class ColumnFileInMemory : public ColumnFile
{
    friend class ColumnFileInMemoryReader;

private:
    BlockPtr schema;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    // whether this instance can append any more data.
    bool disable_append = false;

    // The cache data in memory.
    CachePtr cache;
    // Used to map column id to column instance in a Block.
    ColIdToOffset colid_to_offset;

private:
    void fillColumns(const ColumnDefines & col_defs, size_t col_count, Columns & result) const;

    const DataTypePtr & getDataType(ColId column_id) const
    {
        // Note that column_id must exist
        auto index = colid_to_offset.at(column_id);
        return schema->getByPosition(index).type;
    }

public:
    explicit ColumnFileInMemory(const BlockPtr & schema_, const CachePtr & cache_ = nullptr)
        : schema(schema_)
        , cache(cache_ ? cache_ : std::make_shared<Cache>(*schema_))
    {
        colid_to_offset.clear();
        for (size_t i = 0; i < schema->columns(); ++i)
            colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
    }

    Type getType() const override { return Type::INMEMORY_FILE; }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; };

    CachePtr getCache() { return cache; }

    /// The schema of this pack.
    BlockPtr getSchema() const { return schema; }
    /// Replace the schema with a new schema, and the new schema instance should be exactly the same as the previous one.
    void resetIdenticalSchema(BlockPtr schema_) { schema = schema_; }

    ColumnInMemoryFilePtr clone()
    {
        return std::make_shared<ColumnFileInMemory>(*this);
    }

    ColumnFileReaderPtr
    getReader(const DMContext & context, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const override;

    bool isAppendable() const override
    {
        return !disable_append;
    }
    void disableAppend() override
    {
        disable_append = true;
    }
    bool append(DMContext & dm_context, const Block & data, size_t offset, size_t limit, size_t data_bytes) override;

    Block readDataForFlush() const;

    String toString() const override
    {
        String s = "{in_memory_file,rows:" + DB::toString(rows) //
            + ",bytes:" + DB::toString(bytes) //
            + ",disable_append:" + DB::toString(disable_append) //
            + ",schema:" + (schema ? schema->dumpStructure() : "none") //
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
    ColumnFileInMemoryReader(const ColumnFileInMemory & memory_file_,
                             const ColumnDefinesPtr & col_defs_,
                             const Columns & cols_data_cache_)
        : memory_file(memory_file_)
        , col_defs(col_defs_)
        , cols_data_cache(cols_data_cache_)
    {
    }

    ColumnFileInMemoryReader(const ColumnFileInMemory & memory_file_, const ColumnDefinesPtr & col_defs_)
        : memory_file(memory_file_)
        , col_defs(col_defs_)
    {
    }

    /// This is a ugly hack to fast return PK & Version column.
    ColumnPtr getPKColumn();
    ColumnPtr getVersionColumn();

    size_t readRows(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range) override;

    Block readNextBlock() override;

    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs) override;
};

} // namespace DM
} // namespace DB
