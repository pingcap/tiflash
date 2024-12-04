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


#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/VersionChain/BuildBitmapFilter.h>
#include <Storages/DeltaMerge/VersionChain/DeleteMarkFilter.h>
#include <Storages/DeltaMerge/VersionChain/RowKeyFilter.h>
#include <Storages/DeltaMerge/VersionChain/VersionFilter.h>

namespace DB::DM
{
template <ExtraHandleType HandleType>
BitmapFilterPtr buildBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    const UInt64 read_ts,
    VersionChain<HandleType> & version_chain)
{
    RUNTIME_CHECK(pack_filter_results.size() == 1, pack_filter_results.size());
    RUNTIME_CHECK(snapshot.stable->getDMFiles().size() == 1, snapshot.stable->getDMFiles().size());
    const auto base_ver_snap = version_chain.replaySnapshot(dm_context, snapshot);
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    const auto & stable_filter_res = pack_filter_results[0];
    auto bitmap_filter = std::make_shared<BitmapFilter>(total_rows, true);

    const bool enable_version_chain_for_test = dm_context.global_context.getSettingsRef().enable_version_chain
        == static_cast<Int64>(VersionChainMode::EnabledForTest);

    auto version_filtered_out_rows = buildVersionFilter<HandleType>(
        dm_context,
        snapshot,
        *base_ver_snap,
        read_ts,
        stable_filter_res,
        *bitmap_filter);
    if (enable_version_chain_for_test)
        bitmap_filter->saveVersionFilterForDebug();

    auto rowkey_filtered_out_rows
        = buildRowKeyFilter<HandleType>(dm_context, snapshot, read_ranges, stable_filter_res, *bitmap_filter);
    if (enable_version_chain_for_test)
        bitmap_filter->saveRowKeyFilterForDebug();

    auto delete_filtered_out_rows = buildDeleteMarkFilter(dm_context, snapshot, stable_filter_res, *bitmap_filter);

    LOG_INFO(
        snapshot.log,
        "rowkey_filtered_out_rows={}, version_filtered_out_rows={}, delete_filtered_out_rows={}",
        rowkey_filtered_out_rows,
        version_filtered_out_rows,
        delete_filtered_out_rows);

    // The sum of `*_filtered_out_rows` may greater than the actual number of rows that are filtered out,
    // because the same row may be filtered out by multiple filters and counted multiple times.
    bitmap_filter->setAllMatch(rowkey_filtered_out_rows + version_filtered_out_rows + delete_filtered_out_rows == 0);
    return bitmap_filter;
}

template BitmapFilterPtr buildBitmapFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    const UInt64 read_ts,
    VersionChain<Int64> & version_chain);

template BitmapFilterPtr buildBitmapFilter<String>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    const UInt64 read_ts,
    VersionChain<String> & version_chain);

BitmapFilterPtr buildBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    const UInt64 read_ts,
    GenericVersionChain & generic_version_chain)
{
    return std::visit(
        [&](auto & version_chain) {
            return buildBitmapFilter(dm_context, snapshot, read_ranges, pack_filter_results, read_ts, version_chain);
        },
        generic_version_chain);
}
} // namespace DB::DM
