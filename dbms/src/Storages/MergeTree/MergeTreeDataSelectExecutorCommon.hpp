#pragma once

namespace DB
{

static inline void extendMutableEngineColumnNames(Names & column_names_to_read, const MergeTreeData & data)
{
    column_names_to_read.insert(column_names_to_read.end(), MutableSupport::version_column_name);
    column_names_to_read.insert(column_names_to_read.end(), MutableSupport::delmark_column_name);

    std::vector<String> add_columns = data.getPrimaryExpression()->getRequiredColumns();
    column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());

    std::sort(column_names_to_read.begin(), column_names_to_read.end());
    column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());
}

static inline size_t computeMinMarksForSeek(const Settings & settings, const MergeTreeData & data)
{
    size_t min_marks_for_seek = (settings.merge_tree_min_rows_for_seek + data.index_granularity - 1) / data.index_granularity;
    return min_marks_for_seek;
}

template <typename TargetType>
static inline MarkRanges markRangesFromRegionRange(const MergeTreeData::DataPart::Index & index,
    const TiKVHandle::Handle<TargetType> & handle_begin,
    const TiKVHandle::Handle<TargetType> & handle_end,
    const MarkRanges & ori_mark_ranges,
    const size_t min_marks_for_seek,
    const Settings & settings)
{
    if (handle_end <= handle_begin)
        return {};

    MarkRanges res;

    size_t marks_count = index.at(0)->size();
    if (marks_count == 0)
        return res;

    MarkRanges ranges_stack = ori_mark_ranges;
    reverse(ranges_stack.begin(), ranges_stack.end());

    while (!ranges_stack.empty())
    {
        MarkRange range = ranges_stack.back();
        ranges_stack.pop_back();

        TiKVHandle::Handle<TargetType> index_left_handle = static_cast<TargetType>(index[0]->getUInt(range.begin));
        TiKVHandle::Handle<TargetType> index_right_handle = TiKVHandle::Handle<TargetType>::max;

        if (range.end != marks_count)
            index_right_handle = static_cast<TargetType>(index[0]->getUInt(range.end));

        if (handle_begin >= index_right_handle || index_left_handle >= handle_end)
            continue;

        if (range.end == range.begin + 1)
        {
            if (res.empty() || range.begin - res.back().end > min_marks_for_seek)
                res.push_back(range);
            else
                res.back().end = range.end;
        }
        else
        {
            size_t step = (range.end - range.begin - 1) / settings.merge_tree_coarse_index_granularity + 1;
            size_t end;

            for (end = range.end; end > range.begin + step; end -= step)
                ranges_stack.push_back(MarkRange(end - step, end));

            ranges_stack.push_back(MarkRange(range.begin, end));
        }
    }

    return res;
}

template <typename TargetType>
static inline void computeHandleRenges(std::vector<std::deque<size_t>> & block_data,
    std::vector<std::pair<DB::HandleRange<TargetType>, size_t>> & handle_ranges,
    std::vector<RangesInDataParts> & region_group_range_parts,
    std::vector<DB::HandleRange<TargetType>> & region_group_handle_ranges,
    const RangesInDataParts & parts_with_ranges,
    size_t & region_sum_marks,
    size_t & region_sum_ranges,
    const Settings & settings,
    const size_t min_marks_for_seek)
{
    block_data.resize(handle_ranges.size());
    {
        size_t size = 0;

        block_data[0].emplace_back(handle_ranges[0].second);

        for (size_t i = 1; i < handle_ranges.size(); ++i)
        {
            if (handle_ranges[i].first.first == handle_ranges[size].first.second)
                handle_ranges[size].first.second = handle_ranges[i].first.second;
            else
                handle_ranges[++size] = handle_ranges[i];

            block_data[size].emplace_back(handle_ranges[i].second);
        }
        size = size + 1;
        handle_ranges.resize(size);
        block_data.resize(size);
    }

    region_group_range_parts.assign(handle_ranges.size(), {});
    region_group_handle_ranges.resize(handle_ranges.size());

    for (size_t idx = 0; idx < handle_ranges.size(); ++idx)
    {
        const auto & handle_range = handle_ranges[idx];
        for (const RangesInDataPart & ranges : parts_with_ranges)
        {
            MarkRanges mark_ranges = markRangesFromRegionRange<TargetType>(
                ranges.data_part->index, handle_range.first.first, handle_range.first.second, ranges.ranges, min_marks_for_seek, settings);

            if (mark_ranges.empty())
                continue;

            region_group_range_parts[idx].emplace_back(ranges.data_part, ranges.part_index_in_query, mark_ranges);
            region_sum_ranges += mark_ranges.size();
            for (const auto & range : mark_ranges)
                region_sum_marks += range.end - range.begin;
        }
        region_group_handle_ranges[idx] = handle_range.first;
    }
}


} // namespace DB
