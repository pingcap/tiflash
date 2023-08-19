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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetSnapshot.h>

namespace DB
{
namespace DM
{
class ColumnFileSetReader
{
    friend class ColumnFileSetInputStream;

private:
    ColumnFileSetSnapshotPtr snapshot;

    // The columns expected to read. Note that we will do reading exactly in this column order.
    ColumnDefinesPtr col_defs;
    RowKeyRange segment_range;

    // The row count of each column file. Cache here to speed up checking.
    std::vector<size_t> column_file_rows;
    // The cumulative rows of column files. Used to fast locate specific column files according to rows offset by binary search.
    std::vector<size_t> column_file_rows_end;

    std::vector<ColumnFileReaderPtr> column_file_readers;

private:
    ColumnFileSetReader() = default;

    Block readPKVersion(size_t offset, size_t limit);

public:
    ColumnFileSetReader(const DMContext & context_,
                        const ColumnFileSetSnapshotPtr & snapshot_,
                        const ColumnDefinesPtr & col_defs_,
                        const RowKeyRange & segment_range_);

    // If we need to read columns besides pk and version, a ColumnFileSetReader can NOT be used more than once.
    // This method create a new reader based on the current one. It will reuse some caches in the current reader.
    ColumnFileSetReaderPtr createNewReader(const ColumnDefinesPtr & new_col_defs);

    // Use for DeltaMergeBlockInputStream to read rows from MemTableSet to do full compaction with other layer.
    // This method will check whether offset and limit are valid. It only return those valid rows.
    size_t readRows(MutableColumns & output_columns, size_t offset, size_t limit, const RowKeyRange * range);

    void getPlaceItems(BlockOrDeletes & place_items, size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end, size_t place_rows_offset = 0);

    bool shouldPlace(const DMContext & context,
                     const RowKeyRange & relevant_range,
                     UInt64 max_version,
                     size_t placed_rows);
};

class ColumnFileSetInputStream : public IBlockInputStream
{
private:
    ColumnFileSetReader reader;
    ColumnFiles & column_files;
    size_t column_files_count;

    ColumnFileReaderPtr cur_column_file_reader = {};
    size_t next_file_index = 0;

public:
    ColumnFileSetInputStream(const DMContext & context_,
                             const ColumnFileSetSnapshotPtr & delta_snap_,
                             const ColumnDefinesPtr & col_defs_,
                             const RowKeyRange & segment_range_)
        : reader(context_, delta_snap_, col_defs_, segment_range_)
        , column_files(reader.snapshot->getColumnFiles())
        , column_files_count(column_files.size())
    {}

    String getName() const override { return "ColumnFileSet"; }
    Block getHeader() const override { return toEmptyBlock(*(reader.col_defs)); }

    Block read() override
    {
        while (cur_column_file_reader || next_file_index < column_files_count)
        {
            if (!cur_column_file_reader)
            {
                if (column_files[next_file_index]->isDeleteRange())
                {
                    ++next_file_index;
                    continue;
                }
                else
                {
                    cur_column_file_reader = reader.column_file_readers[next_file_index];
                    ++next_file_index;
                }
            }
            Block block = cur_column_file_reader->readNextBlock();
            if (block)
                return block;
            else
                cur_column_file_reader = {};
        }
        return {};
    }
};
} // namespace DM
} // namespace DB