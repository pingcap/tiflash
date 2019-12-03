#pragma once

#include <DataStreams/MergingSortedBlockInputStream.h>
#include <Storages/Transaction/TiKVHandle.h>
#include <common/logger_useful.h>
#include <Core/TMTSortCursor.hpp>

namespace DB
{

// bottleneck is about memory copy and io
template <typename HandleType>
class ReplacingTMTSortedBlockInputStream : public MergingSortedBlockInputStream
{
    using Handle = TiKVHandle::Handle<HandleType>;

public:
    ReplacingTMTSortedBlockInputStream(const std::vector<HandleRange<HandleType>> & ranges_,
        const BlockInputStreams & inputs_,
        const SortDescription & description_,
        const String & version_column,
        const String & del_column,
        const String & pk_column,
        const size_t max_block_size_,
        const UInt64 gc_tso_,
        const TableID table_id_,
        bool final_)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, NULL),
          gc_tso(gc_tso_),
          table_id(table_id_),
          final(final_)
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
    }

    String getName() const override { return "ReplacingTMTSorted"; }

protected:
    Block readImpl() override;
    void initQueue() override;

private:
    void merge(MutableColumns & merged_columns);
    void insertRow(MutableColumns &, size_t &);

    bool shouldOutput(const TMTCmpOptimizedRes res);
    bool behindGcTso();
    bool isDefiniteDeleted();
    bool hasDeleteFlag();

private:
    std::vector<Handle> begin_handle_ranges;
    std::vector<Handle> end_handle_ranges;

    size_t version_column_number;
    size_t del_column_number;
    size_t pk_column_number;

    Logger * log = &Logger::get("ReplacingTMTSortedBlockInputStream");

    bool finished = false;

    RowRef current_key;
    RowRef next_key;
    RowRef selected_row;

    size_t deleted_by_range = 0;
    size_t diff_pk = 0;
    size_t final_del = 0;
    size_t keep_history = 0;
    size_t dis_history = 0;

    UInt64 gc_tso;
    TableID table_id;

    bool final;

    using TMTSortCursorFull = TMTSortCursor<false>;
    using TMTQueue = std::priority_queue<TMTSortCursorFull>;
    TMTQueue tmt_queue;
};

} // namespace DB
