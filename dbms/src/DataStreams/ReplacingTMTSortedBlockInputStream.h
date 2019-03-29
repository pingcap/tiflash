#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

class ReplacingTMTSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    ReplacingTMTSortedBlockInputStream(
        const std::vector<std::pair<HandleID, HandleID>> & ranges_,
        const BlockInputStreams & inputs_,
        const SortDescription & description_,
        const String & version_column,
        const String & del_column,
        const String & pk_column,
        size_t max_block_size_,
        UInt64 gc_tso_)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, NULL), gc_tso(gc_tso_)
    {
        {
            begin_handle_ranges.resize(ranges_.size());
            for (size_t i = 0; i < ranges_.size(); ++i)
                begin_handle_ranges[i] = ranges_[i].first;
            end_handle_ranges.resize(ranges_.size());
            for (size_t i = 0; i < ranges_.size(); ++i)
                end_handle_ranges[i] = ranges_[i].second;
        }
        version_column_number = header.getPositionByName(version_column);
        del_column_number = header.getPositionByName(del_column);
        pk_column_number = header.getPositionByName(pk_column);
        deleted_by_range = 0;
    }

    String getName() const override { return "ReplacingTMTSorted"; }

protected:
    Block readImpl() override;

private:
    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);
    void insertRow(MutableColumns &, size_t &);

    bool shouldOutput();
    bool behindGcTso();
    bool nextHasDiffPk();
    bool isDefiniteDeleted();

    void logRowGoing(const std::string & reason, bool is_output);

private:
    std::vector<HandleID> begin_handle_ranges;
    std::vector<HandleID> end_handle_ranges;

    size_t version_column_number;
    size_t del_column_number;
    size_t pk_column_number;

    Logger * log = &Logger::get("ReplacingTMTStortedBlockInputStream");

    bool finished = false;

    RowRef current_key;
    RowRef next_key;
    RowRef selected_row;

    size_t deleted_by_range;

    UInt64 gc_tso;
};

}
