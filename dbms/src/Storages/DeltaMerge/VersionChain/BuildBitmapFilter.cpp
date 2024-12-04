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
RSResults getDMFileRSFilterResults(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const RSOperatorPtr & rs_operator)
{
    if (!rs_operator)
        return RSResults(dmfile->getPacks(), RSResult::Some);

    auto pack_filter = DMFilePackFilter::loadFrom(
        dmfile,
        dm_context.global_context.getMinMaxIndexCache(),
        true,
        {}, // read_ranges
        rs_operator,
        {},
        dm_context.global_context.getFileProvider(),
        dm_context.getReadLimiter(),
        dm_context.scan_context,
        dm_context.tracing_id,
        ReadTag::MVCC);
    return pack_filter.getPackResConst();
}

RSResults getStableRSFilterResults(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    const RSOperatorPtr & rs_operator)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    return getDMFileRSFilterResults(dm_context, dmfiles[0], rs_operator);
}

template <Int64OrString Handle>
BitmapFilterPtr buildBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & rs_operator,
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
    // TODO: send the pack res to buildVersionFilter and buildDeletedFilter to skip some packs.
    auto stable_pack_res = getStableRSFilterResults(dm_context, stable, rs_operator);
    buildRowKeyFilter<Handle>(dm_context, snapshot, read_ranges, stable_pack_res, filter);
    buildVersionFilter(dm_context, snapshot, *base_ver_snap, read_ts, filter);
    buildDeletedFilter(dm_context, snapshot, filter);

    bitmap_filter->runOptimize();
    return bitmap_filter;
}

template BitmapFilterPtr buildBitmapFilter<Int64>(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & rs_operator,
    const UInt64 read_ts,
    VersionChain<Handle> & version_chain);
} // namespace DB::DM
