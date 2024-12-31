// Copyright 2024 PingCAP, Inc.
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
RSResults getRSResultsByRanges(
    const Context & global_context,
    const ScanContextPtr & scan_context,
    const String & tracing_id,
    const DMFilePtr & dmfile,
    const RowKeyRanges & ranges)
{
    if (ranges.empty())
        return RSResults(dmfile->getPacks(), RSResult::All);

    auto pack_filter = DMFilePackFilter::loadFrom(
        dmfile,
        global_context.getMinMaxIndexCache(),
        true,
        ranges,
        EMPTY_RS_OPERATOR,
        {},
        global_context.getFileProvider(),
        global_context.getReadLimiter(),
        scan_context,
        tracing_id,
        ReadTag::MVCC);

    return pack_filter.getHandleRes();
}

namespace
{
template <typename T>
T getMaxValue(const MinMaxIndex & minmax_index, size_t i)
{
    if constexpr (std::is_same_v<T, Int64>)
        return minmax_index.getIntMinMax(i).second;
    else if constexpr (std::is_same_v<T, String>)
        return minmax_index.getStringMinMax(i).second;
    else if constexpr (std::is_same_v<T, UInt64>)
        return minmax_index.getUInt64MinMax(i).second;
    else
        static_assert(false, "Not support type");
}

std::pair<RSResults, UInt32> clipRSResults(const RSResults & rs_results)
{
    const auto start = std::find_if(rs_results.begin(), rs_results.end(), [](RSResult r) { return r.isUse(); });
    if (start == rs_results.end())
        return {};
    const auto end = std::find_if(start, rs_results.end(), [](RSResult r) { return !r.isUse(); });
    const auto start_pack_id = start - rs_results.begin();
    return std::make_pair(RSResults(start, end), start_pack_id);
}
} // namespace

std::pair<RSResults, UInt32> getClippedRSResultsByRanges(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range)
{
    if (!segment_range)
        return std::make_pair(RSResults(dmfile->getPacks(), RSResult::All), 0);

    const auto handle_res = getRSResultsByRanges(
        dm_context.global_context,
        dm_context.scan_context,
        dm_context.tracing_id,
        dmfile,
        {*segment_range});

    return clipRSResults(handle_res);
}

template <typename T>
std::vector<T> loadPackMaxValue(const Context & global_context, const DMFile & dmfile, const ColId col_id)
{
    if (dmfile.getPacks() == 0)
        return {};

    auto [type, minmax_index] = DMFilePackFilter::loadIndex(
        dmfile,
        global_context.getFileProvider(),
        global_context.getMinMaxIndexCache(),
        /* set cache*/ true,
        col_id,
        global_context.getReadLimiter(),
        /*scan context*/ nullptr);

    auto pack_count = dmfile.getPacks();
    std::vector<T> pack_max_values;
    pack_max_values.reserve(pack_count);
    for (size_t i = 0; i < pack_count; ++i)
    {
        pack_max_values.push_back(getMaxValue<T>(*minmax_index, i));
    }
    return pack_max_values;
}

template std::vector<Int64> loadPackMaxValue<Int64>(
    const Context & global_context,
    const DMFile & dmfile,
    const ColId col_id);
template std::vector<UInt64> loadPackMaxValue<UInt64>(
    const Context & global_context,
    const DMFile & dmfile,
    const ColId col_id);
//template std::vector<String> loadPackMaxValue<String>(const Context & global_context, const DMFile & dmfile, const ColId col_id);

} // namespace DB::DM
