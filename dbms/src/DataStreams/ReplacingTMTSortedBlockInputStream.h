#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>

namespace DB
{

class ReplacingTMTSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    ReplacingTMTSortedBlockInputStream(
        const BlockInputStreams & inputs_,
        const SortDescription & description_,
        const String & version_column,
        const String & del_column,
        const String & pk_column,
        size_t max_block_size_,
        size_t gc_tso_,
        bool final_,
        bool collapse_versions_)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, NULL), gc_tso(gc_tso_),
        final(final_), collapse_versions(collapse_versions_)
    {
        version_column_number = header.getPositionByName(version_column);
        del_column_number = header.getPositionByName(del_column);
        pk_column_number = header.getPositionByName(pk_column);
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
    bool isDeletedOnFinal();

    void logRowGoing(const std::string & reason, bool is_output);

private:
    size_t version_column_number;
    size_t del_column_number;
    size_t pk_column_number;

    Logger * log = &Logger::get("ReplacingTMTStortedBlockInputStream");

    bool finished = false;

    RowRef current_key;
    RowRef next_key;
    RowRef selected_row;

    size_t gc_tso;
    bool final;
    bool collapse_versions;
};

}
