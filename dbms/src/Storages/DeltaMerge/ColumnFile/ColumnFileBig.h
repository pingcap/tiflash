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
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Remote/Serializer_fwd.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{

class DMFileBlockInputStream;
using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;
class ColumnFileBig;
using ColumnFileBigPtr = std::shared_ptr<ColumnFileBig>;


/// A column file which contains a DMFile. The DMFile could have many Blocks.
class ColumnFileBig : public ColumnFilePersisted
{
    friend class ColumnFileBigReader;
    friend struct Remote::Serializer;

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
    {}

    void calculateStat(const DMContext & dm_context);

public:
    ColumnFileBig(const DMContext & dm_context, const DMFilePtr & file_, const RowKeyRange & segment_range_);

    ColumnFileBig(const ColumnFileBig &) = default;

    ColumnFileBigPtr cloneWith(
        DMContext & dm_context,
        const DMFilePtr & new_file,
        const RowKeyRange & new_segment_range)
    {
        auto * new_column_file = new ColumnFileBig(*this);
        new_column_file->file = new_file;
        new_column_file->segment_range = new_segment_range;
        // update `valid_rows` and `valid_bytes` by `new_segment_range`
        new_column_file->calculateStat(dm_context);
        return std::shared_ptr<ColumnFileBig>(new_column_file);
    }

    Type getType() const override { return Type::BIG_FILE; }

    auto getFile() const { return file; }

    PageIdU64 getDataPageId() { return file->pageId(); }

    size_t getRows() const override { return valid_rows; }
    size_t getBytes() const override { return valid_bytes; }

    void removeData(WriteBatches & wbs) const override;

    ColumnFileReaderPtr getReader(
        const DMContext & dm_context,
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnDefinesPtr & col_defs,
        ReadTag) const override;

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;
    void serializeMetadata(dtpb::ColumnFilePersisted * cf_pb, bool save_schema) const override;

    static ColumnFilePersistedPtr deserializeMetadata(
        const DMContext & dm_context,
        const RowKeyRange & segment_range,
        ReadBuffer & buf);
    static ColumnFilePersistedPtr deserializeMetadata(
        const DMContext & dm_context,
        const RowKeyRange & segment_range,
        const dtpb::ColumnFileBig & cf_pb);

    static ColumnFilePersistedPtr createFromCheckpoint(
        DMContext & dm_context,
        const RowKeyRange & target_range,
        ReadBuffer & buf,
        UniversalPageStoragePtr temp_ps,
        WriteBatches & wbs);
    static ColumnFilePersistedPtr createFromCheckpoint(
        DMContext & dm_context,
        const RowKeyRange & target_range,
        const dtpb::ColumnFileBig & cf_pb,
        UniversalPageStoragePtr temp_ps,
        WriteBatches & wbs);

    String toString() const override { return fmt::format("{{big_file,rows:{},bytes:{}}}", getRows(), getBytes()); }

    bool mayBeFlushedFrom(ColumnFile * from_file) const override
    {
        if (const auto * other = from_file->tryToBigFile(); other)
            return file->pageId() == other->file->pageId();
        else
            return false;
    }
};

class ColumnFileBigReader : public ColumnFileReader
{
private:
    const DMContext & dm_context;
    const ColumnFileBig & column_file;
    const ColumnDefinesPtr col_defs;

    Block header;

    bool pk_ver_only;

    SkippableBlockInputStreamPtr file_stream;

    // The data members for reading only pk and version columns.
    // we cache them to minimize the cost.
    std::vector<Columns> cached_pk_ver_columns;
    std::vector<size_t> cached_block_rows_end;
    size_t next_block_index_in_cache = 0;

    // The data members for reading all columns, but can only read once.
    size_t rows_before_cur_block = 0;
    size_t cur_block_offset = 0;

    Block cur_block;
    Columns cur_block_data; // The references to columns in cur_block, for faster access.

    ReadTag read_tag;

private:
    void initStream();
    std::pair<size_t, size_t> readRowsRepeatedly(
        MutableColumns & output_cols,
        size_t rows_offset,
        size_t rows_limit,
        const RowKeyRange * range);
    std::pair<size_t, size_t> readRowsOnce(
        MutableColumns & output_cols,
        size_t rows_offset,
        size_t rows_limit,
        const RowKeyRange * range);

public:
    ColumnFileBigReader(
        const DMContext & dm_context_,
        const ColumnFileBig & column_file_,
        const ColumnDefinesPtr & col_defs_,
        ReadTag read_tag_)
        : dm_context(dm_context_)
        , column_file(column_file_)
        , col_defs(col_defs_)
        , read_tag(read_tag_)
    {
        if (col_defs_->size() == 1)
        {
            if ((*col_defs)[0].id == MutSup::extra_handle_id)
            {
                pk_ver_only = true;
            }
        }
        else if (col_defs_->size() == 2)
        {
            if ((*col_defs)[0].id == MutSup::extra_handle_id && (*col_defs)[1].id == MutSup::version_col_id)
            {
                pk_ver_only = true;
            }
        }
        else
        {
            pk_ver_only = false;
        }
    }

    std::pair<size_t, size_t> readRows(
        MutableColumns & output_cols,
        size_t rows_offset,
        size_t rows_limit,
        const RowKeyRange * range) override;

    Block readNextBlock() override;

    size_t skipNextBlock() override;

    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs, ReadTag) override;
};

} // namespace DB::DM
