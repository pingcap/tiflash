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

#pragma once

#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/VersionChain.h>

namespace DB::DM
{
struct DMContext;
struct SegmentSnapshot;
struct RowKeyRange;
using RowKeyRanges = std::vector<RowKeyRange>;

class BitmapFilter;
using BitmapFilterPtr = std::shared_ptr<BitmapFilter>;

template <ExtraHandleType HandleType>
BitmapFilterPtr buildBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    const UInt64 read_ts,
    VersionChain<HandleType> & version_chain,
    const bool force_release_cache);


BitmapFilterPtr buildBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    const UInt64 read_ts,
    std::variant<VersionChain<Int64>, VersionChain<String>> & variant_version_chain,
    const bool force_release_cache);
} // namespace DB::DM
