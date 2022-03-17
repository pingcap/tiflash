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

#if __clang__
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wunused-private-field"
#endif

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

class ReplacingDeletingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    ReplacingDeletingSortedBlockInputStream(const BlockInputStreams & inputs_, const SortDescription & description_,
        const String & version_column_, const String & delmark_column_, size_t max_block_size_,
        WriteBuffer * out_row_sources_buf_, bool is_optimized_ = true)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, out_row_sources_buf_, true), version_column(version_column_),
            delmark_column(delmark_column_), is_optimized(is_optimized_)
    {
    }

    String getName() const override { return "ReplacingDeletingSorted"; }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    String version_column;
    String delmark_column;

    ssize_t version_column_number = -1;
    ssize_t delmark_column_number = -1;

    Poco::Logger * log = &Poco::Logger::get("ReplacingDeletingSorted");

    /// All data has been read.
    bool finished = false;

    /// Primary key of current row.
    RowRef current_key;
    /// Primary key of next row.
    RowRef next_key;
    /// Last row with maximum version for current primary key.
    RowRef selected_row;

    /// Max version for current primary key.
    UInt64 max_version = 0;
    /// Deleted mark for current primary key.
    UInt64 max_delmark = 0;

    PODArray<RowSourcePart> current_row_sources;   /// Sources of rows with the current primary key

    bool is_optimized;
    size_t by_column = 0;
    size_t by_row = 0;

    SortCursor cur_block_cursor;
    SortCursorImpl cur_block_cursor_impl;
    SharedBlockPtr cur_block;

    template <typename TSortCursor>
    void setRowRefOptimized(RowRef & row_ref, TSortCursor & cursor)
    {
      if (cursor == cur_block_cursor)
          row_ref.shared_block = cur_block;
      else
          row_ref.shared_block = source_blocks[cursor.impl->order];

      row_ref.row_num = cursor.impl->pos;
      row_ref.columns = &row_ref.shared_block->all_columns;
    }

    template <typename TSortCursor>
    void setPrimaryKeyRefOptimized(RowRef & row_ref, TSortCursor & cursor)
    {
      if (cursor == cur_block_cursor)
          row_ref.shared_block = cur_block;
      else
          row_ref.shared_block = source_blocks[cursor.impl->order];

      row_ref.row_num = cursor.impl->pos;
      row_ref.columns = &row_ref.shared_block->sort_columns;
    }

    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    void merge_optimized(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    bool insertByColumn(SortCursor current, size_t & merged_rows, MutableColumns & merged_columns);

    /// Output into result the rows for current primary key.
    void insertRow(MutableColumns & merged_columns, size_t & merged_rows);
};
}

#if __clang__
    #pragma clang diagnostic pop
#endif
