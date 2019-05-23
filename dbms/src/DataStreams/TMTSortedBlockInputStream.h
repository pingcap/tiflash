#pragma once

#include <DataStreams/MergingSortedBlockInputStream.h>
#include <common/logger_useful.h>
#include <Core/TMTSortCursor.hpp>

namespace DB
{

class TMTSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    TMTSortedBlockInputStream(const BlockInputStreams & inputs_, const SortDescription & description_, const std::string & version_column,
        const std::string & delmark_column, size_t max_block_size_, WriteBuffer * out_row_sources_buf_ = nullptr)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, out_row_sources_buf_, true)
    {
        version_column_number = header.getPositionByName(version_column);
        delmark_column_number = header.getPositionByName(delmark_column);
    }

    String getName() const override { return "TMTSortedBlockInputStream"; }

protected:
    Block readImpl() override;
    void initQueue() override;

private:
    using TMTSortCursorPK = TMTSortCursor<true>;
    using TMTPKQueue = std::priority_queue<TMTSortCursorPK>;
    TMTPKQueue tmt_queue;

    ssize_t version_column_number = -1;
    ssize_t delmark_column_number = -1;

    Logger * log = &Logger::get("TMTSortedBlockInputStream");

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
    UInt8 max_delmark = 0;

    PODArray<RowSourcePart> current_row_sources; /// Sources of rows with the current primary key

    size_t by_column = 0;
    size_t by_row = 0;

    TMTSortCursorPK cur_block_cursor;
    SortCursorImpl cur_block_cursor_impl;
    SharedBlockPtr cur_block;

    void setRowRefOptimized(RowRef & row_ref, TMTSortCursorPK & cursor)
    {
        if (cursor.isSame(cur_block_cursor))
            row_ref.shared_block = cur_block;
        else
            row_ref.shared_block = source_blocks[cursor.impl->order];

        row_ref.row_num = cursor.impl->pos;
        row_ref.columns = &row_ref.shared_block->all_columns;
    }

    void setPrimaryKeyRefOptimized(RowRef & row_ref, TMTSortCursorPK & cursor)
    {
        if (cursor.isSame(cur_block_cursor))
            row_ref.shared_block = cur_block;
        else
            row_ref.shared_block = source_blocks[cursor.impl->order];

        row_ref.row_num = cursor.impl->pos;
        row_ref.columns = &row_ref.shared_block->sort_columns;
    }

    void merge_optimized(MutableColumns & merged_columns, std::priority_queue<TMTSortCursorPK> & queue);

    bool insertByColumn(TMTSortCursorPK current, size_t & merged_rows, MutableColumns & merged_columns);

    /// Output into result the rows for current primary key.
    void insertRow(MutableColumns & merged_columns, size_t & merged_rows);
};
} // namespace DB
