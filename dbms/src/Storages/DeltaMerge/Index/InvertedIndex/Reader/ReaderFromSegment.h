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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/Filter/ColumnRange_fwd.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache_fwd.h>

namespace DB::DM
{

struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;

class InvertedIndexReaderFromSegment
{
private:
    const SegmentSnapshotPtr snapshot;
    const ColumnRangePtr column_range;
    // Global local index cache
    const LocalIndexCachePtr local_index_cache;

public:
    InvertedIndexReaderFromSegment(
        const SegmentSnapshotPtr & snapshot_,
        const ColumnRangePtr & column_range_,
        const LocalIndexCachePtr & local_index_cache_)
        : snapshot(snapshot_)
        , column_range(column_range_)
        , local_index_cache(local_index_cache_)
    {}

    static BitmapFilterPtr loadStable(
        const SegmentSnapshotPtr & snapshot,
        const ColumnRangePtr & column_range,
        const LocalIndexCachePtr & local_index_cache);
    static BitmapFilterPtr loadDelta(
        const SegmentSnapshotPtr & snapshot,
        const ColumnRangePtr & column_range,
        const LocalIndexCachePtr & local_index_cache);

    ~InvertedIndexReaderFromSegment() = default;

    // Load bitmap filter from segment snapshot
    BitmapFilterPtr loadStableImpl();
    BitmapFilterPtr loadDeltaImpl();
};

} // namespace DB::DM
