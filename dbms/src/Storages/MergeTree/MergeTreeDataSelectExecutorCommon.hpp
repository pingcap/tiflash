#pragma once

namespace DB
{

static inline void extendMutableEngineColumnNames(Names & column_names_to_read, const MergeTreeData & data)
{
    column_names_to_read.insert(column_names_to_read.end(), MutableSupport::version_column_name);
    column_names_to_read.insert(column_names_to_read.end(), MutableSupport::delmark_column_name);

    std::vector<std::string> add_columns = data.getPrimaryExpression()->getRequiredColumns();
    column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());

    std::sort(column_names_to_read.begin(), column_names_to_read.end());
    column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());
}

/// make pk, version, delmark is always the first 3 columns, maybe some sample column will be added later.
static inline void extendMutableEngineColumnNames(Names & column_names_to_read, const std::string & handle_col_name)
{
    std::set<std::string> names;

    for (auto & name : column_names_to_read)
        names.emplace(std::move(name));
    column_names_to_read.clear();

    column_names_to_read.push_back(handle_col_name);
    column_names_to_read.push_back(MutableSupport::version_column_name);
    column_names_to_read.push_back(MutableSupport::delmark_column_name);

    names.erase(MutableSupport::version_column_name);
    names.erase(MutableSupport::delmark_column_name);
    names.erase(handle_col_name);

    for (auto & name : names)
        column_names_to_read.emplace_back(std::move(name));
}

static inline size_t computeMinMarksForSeek(const Settings & settings, const MergeTreeData & data)
{
    size_t min_marks_for_seek = (settings.merge_tree_min_rows_for_seek + data.index_granularity - 1) / data.index_granularity;
    return min_marks_for_seek;
}

template <typename TargetType>
static inline MarkRanges markRangesFromRegionRange(const MergeTreeData::DataPart & data_part,
    const TiKVHandle::Handle<TargetType> & handle_begin,
    const TiKVHandle::Handle<TargetType> & handle_end,
    const MarkRanges & ori_mark_ranges,
    const size_t min_marks_for_seek,
    const Settings & settings)
{
    if (handle_end <= handle_begin)
        return {};

    if (data_part.tmt_property->initialized)
    {
        TiKVHandle::Handle<TargetType> index_right_handle;
        {
            UInt64 tmp = data_part.tmt_property->max_pk.get<UInt64>();
            index_right_handle = static_cast<TargetType>(tmp);
        }
        if (handle_begin > index_right_handle)
            return {};
    }

    const auto & index = data_part.index;

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

        if (handle_begin > index_right_handle || index_left_handle >= handle_end)
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

struct ReadGroup {
    std::deque<size_t> mem_block_indexes;
    size_t start_range_index;
    size_t end_range_index;
};

/// computeHandleRanges divide the input handle ranges into one or more read groups
/// the basic rule is
/// 1. the handle ranges will be combined if adjacent handle ranges is combinable
/// 2. a mem block only exists in a single read groups
template <typename TargetType>
static inline void computeHandleRanges(std::vector<ReadGroup> & read_groups,
    std::vector<std::pair<DB::HandleRange<TargetType>, size_t>> & handle_ranges,
    std::vector<RangesInDataParts> & region_group_range_parts,
    std::vector<DB::HandleRange<TargetType>> & region_group_handle_ranges,
    const RangesInDataParts & parts_with_ranges,
    size_t & region_sum_marks,
    size_t & region_sum_ranges,
    const Settings & settings,
    const size_t min_marks_for_seek)
{
    read_groups.resize(handle_ranges.size());
    {
        size_t size = 0;
        size_t read_group_size = 0;
        std::unordered_set<size_t> current_region_indexes;

        read_groups[0].mem_block_indexes.emplace_back(handle_ranges[0].second);
        current_region_indexes.insert(handle_ranges[0].second);
        read_groups[0].start_range_index = 0;

        for (size_t i = 1; i < handle_ranges.size(); ++i)
        {
            if (handle_ranges[i].first.first == handle_ranges[size].first.second)
                handle_ranges[size].first.second = handle_ranges[i].first.second;
            else
            {
                handle_ranges[++size] = handle_ranges[i];
                /// check if it is ok to start new read group
                /// a mem block should only exists in a single read group
                if (current_region_indexes.find(handle_ranges[i].second) == current_region_indexes.end())
                {
                    /// if the region index in handle_ranges[i] is not in current_region_indexes
                    /// then it is safe to start a new read group
                    /// NOTE this assumes region index in handle_ranges is monotonous
                    read_groups[read_group_size++].end_range_index = size - 1;
                    read_groups[read_group_size].start_range_index = size;
                    current_region_indexes.clear();
                }
            }

            /// to avoid duplicate mem block in one read group
            if (current_region_indexes.find(handle_ranges[i].second) == current_region_indexes.end())
            {
                read_groups[read_group_size].mem_block_indexes.emplace_back(handle_ranges[i].second);
                current_region_indexes.insert(handle_ranges[i].second);
            }
        }
        read_groups[read_group_size].end_range_index = size;
        size = size + 1;
        read_group_size = read_group_size + 1;
        handle_ranges.resize(size);
        read_groups.resize(read_group_size);
    }

    region_group_range_parts.assign(handle_ranges.size(), {});
    region_group_handle_ranges.resize(handle_ranges.size());

    for (size_t idx = 0; idx < handle_ranges.size(); ++idx)
    {
        const auto & handle_range = handle_ranges[idx];
        for (const RangesInDataPart & ranges : parts_with_ranges)
        {
            MarkRanges mark_ranges = markRangesFromRegionRange<TargetType>(
                *ranges.data_part, handle_range.first.first, handle_range.first.second, ranges.ranges, min_marks_for_seek, settings);

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

template <TMTPKType pk_type>
BlockInputStreamPtr makeMultiWayMergeSortInput(const BlockInputStreams & inputs, const SortDescription & description,
    const size_t version_column_index, const size_t delmark_column_index, size_t max_block_size)
{
    if (pk_type != TMTPKType::UNSPECIFIED && inputs.size() == 1)
        return std::make_shared<TMTSingleSortedBlockInputStream>(inputs[0]);
    return std::make_shared<TMTSortedBlockInputStream<pk_type>>(
        inputs, description, version_column_index, delmark_column_index, max_block_size);
};

} // namespace DB
