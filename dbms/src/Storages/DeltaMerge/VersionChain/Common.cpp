// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>

namespace DB::DM
{
RSResults getRSResultsByRanges(const DMContext & dm_context, const DMFilePtr & dmfile, const RowKeyRanges & ranges)
{
    if (ranges.empty())
        return RSResults(dmfile->getPacks(), RSResult::All);

    return DMFilePackFilter::loadFrom(
               dm_context,
               dmfile,
               /*set_cache_if_miss*/ true,
               ranges,
               EMPTY_RS_OPERATOR,
               /*read_packs*/ {})
        ->getHandleRes();
}

namespace
{
template <typename T>
T getMaxValue(const MinMaxIndex & minmax_index, size_t i)
{
    if constexpr (std::is_same_v<T, Int64>) // For non-clustered index handle column
        return minmax_index.getIntMinMax(i).second;
    else if constexpr (std::is_same_v<T, String>) // For clustered index handle column
        return minmax_index.getStringMinMax(i).second.toString();
    else if constexpr (std::is_same_v<T, UInt64>) // For version column
        return minmax_index.getUInt64MinMax(i).second;
    else
        static_assert(false, "Not support type");
}

template <typename T>
T getMinValue(const MinMaxIndex & minmax_index, size_t i)
{
    if constexpr (std::is_same_v<T, Int64>) // For non-clustered index handle column
        return minmax_index.getIntMinMax(i).first;
    else if constexpr (std::is_same_v<T, String>) // For clustered index handle column
        return minmax_index.getStringMinMax(i).first.toString();
    else
        static_assert(false, "Not support type");
}
} // namespace

std::pair<RSResults, UInt32> getClippedRSResultsByRange(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range)
{
    if (!segment_range)
        return std::make_pair(RSResults(dmfile->getPacks(), RSResult::All), 0);

    const auto handle_res = getRSResultsByRanges(dm_context, dmfile, {*segment_range});

    // Clip the RSResults to remove the None at the beginning and end.
    // For example: [None, None, Some, All, All, Some, None] => {[Some, All, All, Some], start_pack_id = 2}
    // Since DMFile is sorted by handle, None can only appear at the ends, not between Some and All.
    // For example, [None, Some, None, All, Some, None] is invalid.
    const auto start = std::find_if(handle_res.begin(), handle_res.end(), [](RSResult r) { return r.isUse(); });
    if (start == handle_res.end())
        return {};

    const auto end = std::find_if(start, handle_res.end(), [](RSResult r) { return !r.isUse(); });
    const auto start_pack_id = start - handle_res.begin();
    return std::pair{RSResults(start, end), start_pack_id};
}

template <typename T>
std::vector<T> loadPackMaxValue(const DMContext & dm_context, const DMFile & dmfile, const ColId col_id)
{
    if (dmfile.getPacks() == 0)
        return {};

    const auto & global_context = dm_context.global_context;
    auto [type, minmax_index] = DMFilePackFilter::loadIndex(
        dmfile,
        global_context.getFileProvider(),
        global_context.getMinMaxIndexCache(),
        /*set_cache_if_miss*/ true,
        col_id,
        global_context.getReadLimiter(),
        dm_context.scan_context);

    auto pack_count = dmfile.getPacks();
    std::vector<T> pack_max_values;
    pack_max_values.reserve(pack_count);
    for (size_t i = 0; i < pack_count; ++i)
        pack_max_values.push_back(getMaxValue<T>(*minmax_index, i));

    return pack_max_values;
}

template std::vector<Int64> loadPackMaxValue<Int64>(
    const DMContext & dm_context,
    const DMFile & dmfile,
    const ColId col_id);
template std::vector<UInt64> loadPackMaxValue<UInt64>(
    const DMContext & dm_context,
    const DMFile & dmfile,
    const ColId col_id);
template std::vector<String> loadPackMaxValue<String>(
    const DMContext & dm_context,
    const DMFile & dmfile,
    const ColId col_id);

template <ExtraHandleType HandleType>
std::optional<std::pair<HandleType, HandleType>> loadDMFileHandleRange(
    const DMContext & dm_context,
    const DMFile & dmfile)
{
    if (dmfile.getPacks() == 0)
        return {};

    const auto & global_context = dm_context.global_context;
    auto [type, minmax_index] = DMFilePackFilter::loadIndex(
        dmfile,
        global_context.getFileProvider(),
        global_context.getMinMaxIndexCache(),
        /*set_cache_if_miss*/ true,
        MutSup::extra_handle_id,
        global_context.getReadLimiter(),
        dm_context.scan_context);

    return std::pair{
        getMinValue<HandleType>(*minmax_index, 0),
        getMaxValue<HandleType>(*minmax_index, dmfile.getPacks() - 1)};
}

template std::optional<std::pair<Int64, Int64>> loadDMFileHandleRange<Int64>(
    const DMContext & dm_context,
    const DMFile & dmfile);
template std::optional<std::pair<String, String>> loadDMFileHandleRange<String>(
    const DMContext & dm_context,
    const DMFile & dmfile);
} // namespace DB::DM
