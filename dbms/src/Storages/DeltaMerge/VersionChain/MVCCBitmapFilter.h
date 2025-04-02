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

// buildMVCCBitmapFilter calls buildVersionFilter, buildRowKeyFilter, and buildDeleteMarkFilter,
// returning a result consistent with Segment::buildMVCCBitmapFilterNormal/Segment::buildMVCCBitmapFilterStableOnly.
template <ExtraHandleType HandleType>
BitmapFilterPtr buildMVCCBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    UInt64 read_ts,
    VersionChain<HandleType> & version_chain);

BitmapFilterPtr buildMVCCBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const RowKeyRanges & read_ranges,
    const DMFilePackFilterResults & pack_filter_results,
    UInt64 read_ts,
    GenericVersionChain & generic_version_chain);
} // namespace DB::DM
