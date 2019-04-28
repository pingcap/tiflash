#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>

namespace DB
{

class MvccTMTSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    MvccTMTSortedBlockInputStream(const BlockInputStreams & inputs_,
        const SortDescription & description_,
        const String & version_column,
        const String & del_column,
        size_t max_block_size_,
        size_t read_tso_,
        bool quiet_ = true)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, nullptr, quiet_), read_tso(read_tso_)
    {
        version_column_number = header.getPositionByName(version_column);
        del_column_number = header.getPositionByName(del_column);
    }

    String getName() const override { return "MvccTMTSorted"; }

protected:
    Block readImpl() override;

private:
    ssize_t version_column_number;
    ssize_t del_column_number;
    Logger * log = &Logger::get("MvccTMTSortedBlockInputStream");

    bool finished = false;

    RowRef current_key;
    RowRef next_key;
    RowRef selected_row;

    size_t read_tso;

    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    bool hasDeleteFlag();

    void insertRow(MutableColumns &, size_t &);
};

} // namespace DB
