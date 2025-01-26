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
#include <Storages/DeltaMerge/VersionChain/DeletedFilter.h>
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
    VersionChain<HandleType> & version_chain,
    const bool force_release_cache)
{
    const auto base_ver_snap = version_chain.replaySnapshot(dm_context, snapshot, force_release_cache);
    //fmt::println("base_ver_snap={}", *base_ver_snap);
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    auto bitmap_filter = std::make_shared<BitmapFilter>(total_rows, true);
    auto & filter = bitmap_filter->getFilter();

    RUNTIME_CHECK(pack_filter_results.size() == 1, pack_filter_results.size());
    buildRowKeyFilter<HandleType>(dm_context, snapshot, read_ranges, pack_filter_results[0], filter);
    buildVersionFilter<HandleType>(dm_context, snapshot, *base_ver_snap, read_ts, filter);
    buildDeletedFilter(dm_context, snapshot, filter);
    // TODO: Make buildRowKeyFilter/buildVersionFilter/buildDeletedFilter returns filtered rows.
    bitmap_filter->setAllMatch(false);
    return bitmap_filter;
}

template BitmapFilterPtr buildBitmapFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    const UInt64 read_ts,
    VersionChain<Int64> & version_chain,
    const bool force_release_cache);

template BitmapFilterPtr buildBitmapFilter<String>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    const UInt64 read_ts,
    VersionChain<String> & version_chain,
    const bool force_release_cache);

BitmapFilterPtr buildBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    const UInt64 read_ts,
    std::variant<VersionChain<Int64>, VersionChain<String>> & variant_version_chain,
    const bool force_release_cache)
{
    return std::visit(
        [&](auto & version_chain) {
            return buildBitmapFilter(
                dm_context,
                snapshot,
                read_ranges,
                pack_filter_results,
                read_ts,
                version_chain,
                force_release_cache);
        },
        variant_version_chain);
}
} // namespace DB::DM
