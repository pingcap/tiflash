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

#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>

namespace DB
{
namespace DM
{
class ColumnFileTiny;
using ColumnFileTinyPtr = std::shared_ptr<ColumnFileTiny>;

/// A column file which data is stored in PageStorage.
/// It may be created in two ways:
///   1. created directly when writing to storage if the data is large enough
///   2. created when flushed `ColumnFileInMemory` to disk
class ColumnFileTiny : public ColumnFilePersisted
{
    friend class ColumnFileTinyReader;

private:
    BlockPtr schema;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    /// The id of data page which stores the data of this pack.
    PageId data_page_id;

    /// The members below are not serialized.

    /// The cache data in memory.
    /// Currently this field is unused.
    CachePtr cache;
    /// Used to map column id to column instance in a Block.
    ColIdToOffset colid_to_offset;

private:
    /// Read a block of columns in `column_defines` from cache / disk,
    /// if `pack->schema` is not match with `column_defines`, take good care of ddl cast
    Columns readFromCache(const ColumnDefines & column_defines, size_t col_start, size_t col_end) const;
    Columns readFromDisk(const PageReader & page_reader, const ColumnDefines & column_defines, size_t col_start, size_t col_end) const;

    void fillColumns(const PageReader & page_reader, const ColumnDefines & col_defs, size_t col_count, Columns & result) const;

    const DataTypePtr & getDataType(ColId column_id) const
    {
        // Note that column_id must exist
        auto index = colid_to_offset.at(column_id);
        return schema->getByPosition(index).type;
    }

public:
    ColumnFileTiny(const BlockPtr & schema_, UInt64 rows_, UInt64 bytes_, PageId data_page_id_, const CachePtr & cache_ = nullptr)
        : schema(schema_)
        , rows(rows_)
        , bytes(bytes_)
        , data_page_id(data_page_id_)
        , cache(cache_)
    {
        for (size_t i = 0; i < schema->columns(); ++i)
            colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
    }

    Type getType() const override { return Type::TINY_FILE; }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; };

    auto getCache() const { return cache; }
    void clearCache() { cache = {}; }

    /// The schema of this pack. Could be empty, i.e. a DeleteRange does not have a schema.
    BlockPtr getSchema() const { return schema; }
    /// Replace the schema with a new schema, and the new schema instance should be exactly the same as the previous one.
    void resetIdenticalSchema(BlockPtr schema_) { schema = schema_; }

    ColumnFileTinyPtr cloneWith(PageId new_data_page_id)
    {
        auto new_tiny_file = std::make_shared<ColumnFileTiny>(*this);
        new_tiny_file->data_page_id = new_data_page_id;
        return new_tiny_file;
    }

    ColumnFileReaderPtr
    getReader(const DMContext & /*context*/, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const override;

    void removeData(WriteBatches & wbs) const override
    {
        wbs.removed_log.delPage(data_page_id);
    }

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;

    PageId getDataPageId() const { return data_page_id; }

    Block readBlockForMinorCompaction(const PageReader & page_reader) const;

    static ColumnFileTinyPtr writeColumnFile(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs, const BlockPtr & schema = nullptr, const CachePtr & cache = nullptr);

    static PageId writeColumnFileData(DMContext & context, const Block & block, size_t offset, size_t limit, WriteBatches & wbs);

    static std::tuple<ColumnFilePersistedPtr, BlockPtr> deserializeMetadata(ReadBuffer & buf, const BlockPtr & last_schema);

    bool mayBeFlushedFrom(ColumnFile * from_file) const override
    {
        // The current ColumnFileTiny may come from a ColumnFileInMemory (which contains data in memory)
        // or ColumnFileTiny (which contains data in PageStorage).

        if (const auto * other_tiny = from_file->tryToTinyFile(); other_tiny)
            return data_page_id == other_tiny->data_page_id;
        else if (const auto * other_in_memory = from_file->tryToInMemoryFile(); other_in_memory)
            // For ColumnFileInMemory, we just do a rough check, instead of checking byte by byte, which
            // is too expensive.
            return bytes == from_file->getBytes() && rows == from_file->getRows();
        else
            return false;
    }

    String toString() const override
    {
        String s = "{tiny_file,rows:" + DB::toString(rows) //
            + ",bytes:" + DB::toString(bytes) //
            + ",data_page_id:" + DB::toString(data_page_id) //
            + ",schema:" + (schema ? schema->dumpStructure() : "none") //
            + ",cache_block:" + (cache ? cache->block.dumpStructure() : "none") + "}";
        return s;
    }
};

class ColumnFileTinyReader : public ColumnFileReader
{
private:
    const ColumnFileTiny & tiny_file;
    const StorageSnapshotPtr storage_snap;
    const ColumnDefinesPtr col_defs;

    Columns cols_data_cache;
    bool read_done = false;

public:
    ColumnFileTinyReader(const ColumnFileTiny & tiny_file_,
                         const StorageSnapshotPtr & storage_snap_,
                         const ColumnDefinesPtr & col_defs_,
                         const Columns & cols_data_cache_)
        : tiny_file(tiny_file_)
        , storage_snap(storage_snap_)
        , col_defs(col_defs_)
        , cols_data_cache(cols_data_cache_)
    {
    }

    ColumnFileTinyReader(const ColumnFileTiny & tiny_file_, const StorageSnapshotPtr & storage_snap_, const ColumnDefinesPtr & col_defs_)
        : tiny_file(tiny_file_)
        , storage_snap(storage_snap_)
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
