#pragma once

#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{
struct RangesInDataPart
{
    MergeTreeData::DataPartPtr data_part;
    size_t part_index_in_query;
    MarkRanges ranges;

    RangesInDataPart() = default;

    RangesInDataPart(const MergeTreeData::DataPartPtr & data_part, const size_t part_index_in_query, const MarkRanges & ranges = MarkRanges{})
        : data_part{data_part}
        , part_index_in_query{part_index_in_query}
        , ranges{ranges}
    {
    }
};

using RangesInDataParts = std::vector<RangesInDataPart>;


} // namespace DB
