// Copyright 2022 PingCAP, Ltd.
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
#include <Storages/DeltaMerge/StoragePool.h>

namespace DB
{
namespace DM
{
class ColumnFileRemote;
using ColumnFileRemotePtr = std::shared_ptr<ColumnFileRemote>;

/// A column file which is stored in remote store service like S3.
class ColumnFileRemote : public ColumnFile
{
    friend class ColumnFileRemoteReader;

private:
    BlockPtr schema;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    /// The id of data page which stores the data of this column file.
    PageId remote_data_page_id;

    // Used to map column id to column instance in a Block.
    // TODO: save it in `schema`?
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
    explicit ColumnFileRemote(const BlockPtr & schema_)
        : schema(schema_)
        , remote_data_page_id(0)
    {
        colid_to_offset.clear();
        for (size_t i = 0; i < schema->columns(); ++i)
            colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
    }

    Type getType() const override { return Type::INMEMORY_FILE; }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; };

    /// The schema of this pack.
    BlockPtr getSchema() const { return schema; }
    /// Replace the schema with a new schema, and the new schema instance should be exactly the same as the previous one.
    void resetIdenticalSchema(BlockPtr schema_) { schema = schema_; }

    ColumnFileRemotePtr clone()
    {
        return std::make_shared<ColumnFileRemote>(*this);
    }

    ColumnFileReaderPtr
    getReader(const DMContext & /*context*/, const StorageSnapshotPtr & storage_snap, const ColumnDefinesPtr & col_defs) const override;

    bool mayBeFlushedFrom(ColumnFile *) const override { return false; }

    String toString() const override
    {
        return fmt::format("{{remote_file,rows:{}"
                           ",bytes:{},schema:{}}",
                           rows,
                           bytes,
                           schema ? schema->dumpJsonStructure() : "none");
    }
};


class ColumnFileRemoteReader : public ColumnFileReader
{
private:
    const ColumnFileRemote & memory_file;
    const StorageSnapshotPtr storage_snap;
    const ColumnDefinesPtr col_defs;

    Columns cols_data_cache;
    bool read_done = false;

public:
    ColumnFileRemoteReader(const ColumnFileRemote & memory_file_,
                           const StorageSnapshotPtr & storage_snap_,
                           const ColumnDefinesPtr & col_defs_)
        : memory_file(memory_file_)
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
