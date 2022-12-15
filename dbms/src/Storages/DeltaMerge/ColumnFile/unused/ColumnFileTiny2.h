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
#include <Storages/Page/universal/UniversalPageStorage.h>

namespace DB
{
namespace DM
{
class ColumnFileTiny2;
using ColumnFileTiny2Ptr = std::shared_ptr<ColumnFileTiny2>;

/// This class is used for reading from a universal page storage.
/// Once we use universal page storage by default, this class may not be used any more.
class ColumnFileTiny2 : public ColumnFile
{
    friend class ColumnFileTiny2Reader;

private:
    BlockPtr schema;

    UInt64 rows = 0;
    UInt64 bytes = 0;

    /// The id of data page which stores the data of this column file.
    UniversalPageId data_page_id;


    /// Used to map column id to column instance in a Block.
    // TODO: save it in `schema`?
    ColIdToOffset colid_to_offset;

private:
    Columns readFromDisk(
        const IColumnFileSetStorageReaderPtr & storage_reader,
        const ColumnDefines & column_defines,
        size_t col_start,
        size_t col_end) const;

    void fillColumns(
        const IColumnFileSetStorageReaderPtr & storage_reader,
        const ColumnDefines & col_defs,
        size_t col_count,
        Columns & result) const;

    const DataTypePtr & getDataType(ColId column_id) const
    {
        // Note that column_id must exist
        auto index = colid_to_offset.at(column_id);
        return schema->getByPosition(index).type;
    }

public:
    ColumnFileTiny2(const BlockPtr & schema_, UInt64 rows_, UInt64 bytes_, UniversalPageId data_page_id_)
        : schema(schema_)
        , rows(rows_)
        , bytes(bytes_)
        , data_page_id(data_page_id_)
    {
        for (size_t i = 0; i < schema->columns(); ++i)
            colid_to_offset.emplace(schema->getByPosition(i).column_id, i);
    }

    Type getType() const override { return Type::TINY2_FILE; }

    size_t getRows() const override { return rows; }
    size_t getBytes() const override { return bytes; };

    /// The schema of this pack. Could be empty, i.e. a DeleteRange does not have a schema.
    BlockPtr getSchema() const { return schema; }

    ColumnFileReaderPtr getReader(
        const DMContext & /*context*/,
        const IColumnFileSetStorageReaderPtr & reader,
        const ColumnDefinesPtr & col_defs) const override;

    String toString() const override
    {
        String s = "{tiny_file_2,rows:" + DB::toString(rows) //
            + ",bytes:" + DB::toString(bytes) //
            + ",data_page_id:" + DB::toString(data_page_id) //
            + ",schema:" + (schema ? schema->dumpStructure() : "none") + "}";
        return s;
    }
};

class ColumnFileTiny2Reader : public ColumnFileReader
{
private:
    const ColumnFileTiny2 & tiny_file;
    const IColumnFileSetStorageReaderPtr storage_reader;
    const ColumnDefinesPtr col_defs;

    Columns cols_data_cache;
    bool read_done = false;

public:
    ColumnFileTiny2Reader(const ColumnFileTiny2 & tiny_file_,
                          const IColumnFileSetStorageReaderPtr & storage_reader_,
                          const ColumnDefinesPtr & col_defs_,
                          const Columns & cols_data_cache_ = {})
        : tiny_file(tiny_file_)
        , storage_reader(storage_reader_)
        , col_defs(col_defs_)
        , cols_data_cache(cols_data_cache_)
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
