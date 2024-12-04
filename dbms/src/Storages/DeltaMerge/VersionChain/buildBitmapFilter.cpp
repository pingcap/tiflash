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
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/VersionChain/DeletedFilter.h>
#include <Storages/DeltaMerge/VersionChain/RowKeyFilter.h>
#include <Storages/DeltaMerge/VersionChain/VersionFilter.h>
#include <Storages/DeltaMerge/VersionChain/buildBitmapFilter.h>
namespace DB::DM
{
template <Int64OrString Handle>
BitmapFilterPtr buildBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const UInt64 read_ts,
    VersionChain<Handle> & version_chain)
{
    const auto base_ver_snap = version_chain.replaySnapshot(dm_context, snapshot);

    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    auto bitmap_filter = std::make_shared<BitmapFilter>(total_rows, true);
    auto & filter = bitmap_filter->getFilter();

    // TODO: make these functions return filter out rows.
    buildRowKeyFilter<Handle>(dm_context, snapshot, read_ranges, filter);
    buildVersionFilter(dm_context, snapshot, *base_ver_snap, read_ts, filter);
    buildDeletedFilter(dm_context, snapshot, filter);

    bitmap_filter->runOptimize();
    return bitmap_filter;
}

template BitmapFilterPtr buildBitmapFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const UInt64 read_ts,
    VersionChain<Handle> & version_chain);
} // namespace DB::DM
