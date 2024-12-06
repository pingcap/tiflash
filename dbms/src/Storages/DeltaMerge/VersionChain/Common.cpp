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
RSResults getDMFilePackFilterResultByRanges(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const RowKeyRanges & ranges)
{
    if (ranges.empty())
        return RSResults(dmfile->getPacks(), RSResult::All);

    auto pack_filter = DMFilePackFilter::loadFrom(
        dmfile,
        dm_context.global_context.getMinMaxIndexCache(),
        true,
        ranges,
        EMPTY_RS_OPERATOR,
        {},
        dm_context.global_context.getFileProvider(),
        dm_context.getReadLimiter(),
        dm_context.scan_context,
        dm_context.tracing_id,
        ReadTag::MVCC);

    return pack_filter.getHandleRes();
}

std::pair<RSResults, UInt32> getDMFilePackFilterResultBySegmentRange(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range)
{
    if (!segment_range)
        return std::make_pair(RSResults(dmfile->getPacks(), RSResult::All), 0);

    const auto handle_res = getDMFilePackFilterResultByRanges(dm_context, dmfile, {*segment_range});
    const auto valid_start_itr
        = std::find_if(handle_res.begin(), handle_res.end(), [](RSResult r) { return r.isUse(); });
    if (valid_start_itr == handle_res.end())
        return {};
    const auto valid_end_itr = std::find_if(valid_start_itr, handle_res.end(), [](RSResult r) { return !r.isUse(); });
    const auto valid_start_pack_id = valid_start_itr - handle_res.begin();
    return std::make_pair(RSResults(valid_start_itr, valid_end_itr), valid_start_pack_id);
}
} // namespace DB::DM
