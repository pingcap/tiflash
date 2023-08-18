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
class DMFileBlockInputStream;
using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;
class ColumnFileBig;
using ColumnBigFilePtr = std::shared_ptr<ColumnFileBig>;


/// A column file which contains a DMFile. The DMFile could have many Blocks.
class ColumnFileBig : public ColumnFilePersisted
{
    friend class ColumnFileBigReader;

private:
    DMFilePtr file;
    size_t valid_rows = 0;
    size_t valid_bytes = 0;

    RowKeyRange segment_range;

    ColumnFileBig(const DMFilePtr & file_, size_t valid_rows_, size_t valid_bytes_, const RowKeyRange & segment_range_)
        : file(file_)
        , valid_rows(valid_rows_)
        , valid_bytes(valid_bytes_)
        , segment_range(segment_range_)
    {
    }

    void calculateStat(const DMContext & context);

public:
    ColumnFileBig(const DMContext & context, const DMFilePtr & file_, const RowKeyRange & segment_range_);

    ColumnFileBig(const ColumnFileBig &) = default;

    ColumnBigFilePtr cloneWith(DMContext & context, const DMFilePtr & new_file, const RowKeyRange & new_segment_range)
    {
        auto * new_column_file = new ColumnFileBig(*this);
        new_column_file->file = new_file;
        new_column_file->segment_range = new_segment_range;
        // update `valid_rows` and `valid_bytes` by `new_segment_range`
        new_column_file->calculateStat(context);
        return std::shared_ptr<ColumnFileBig>(new_column_file);
    }

    Type getType() const override { return Type::BIG_FILE; }

    auto getFile() const { return file; }

    PageId getDataPageId() { return file->pageId(); }

    size_t getRows() const override { return valid_rows; }
    size_t getBytes() const override { return valid_bytes; };

    void removeData(WriteBatches & wbs) const override
    {
        // Here we remove the data id instead of file_id.
        // Because a dmfile could be used in several places, and only after all page ids are removed,
        // then the file_id got removed.
        wbs.removed_data.delPage(file->pageId());
    }

    ColumnFileReaderPtr
    getReader(const DMContext & context, const StorageSnapshotPtr & /*storage_snap*/, const ColumnDefinesPtr & col_defs) const override;

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;

    static ColumnFilePersistedPtr deserializeMetadata(DMContext & context, //
                                                      const RowKeyRange & segment_range,
                                                      ReadBuffer & buf);

    String toString() const override
    {
        String s = "{big_file,rows:" + DB::toString(getRows()) //
            + ",bytes:" + DB::toString(getBytes()) + "}"; //
        return s;
    }
};

class ColumnFileBigReader : public ColumnFileReader
{
private:
    const DMContext & context;
    const ColumnFileBig & column_file;
    const ColumnDefinesPtr col_defs;

    bool pk_ver_only;

    DMFileBlockInputStreamPtr file_stream;

    // The data members for reading only pk and version columns.
    // we cache them to minimize the cost.
    std::vector<Columns> cached_pk_ver_columns;
    std::vector<size_t> cached_block_rows_end;

    // The data members for reading all columns, but can only read once.
    size_t rows_before_cur_block = 0;
    size_t cur_block_offset = 0;

    Block cur_block;
    Columns cur_block_data; // The references to columns in cur_block, for faster access.

private:
    void initStream();
    size_t readRowsRepeatedly(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range);
    size_t readRowsOnce(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range);

public:
    ColumnFileBigReader(const DMContext & context_, const ColumnFileBig & column_file_, const ColumnDefinesPtr & col_defs_)
        : context(context_)
        , column_file(column_file_)
        , col_defs(col_defs_)
    {
        pk_ver_only = col_defs->size() <= 2;
    }

    size_t readRows(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range) override;

    Block readNextBlock() override;

    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs) override;
};
} // namespace DM
} // namespace DB
