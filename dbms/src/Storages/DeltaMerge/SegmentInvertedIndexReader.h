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

#include <Storages/DeltaMerge/Filter/ColumnValueSet.h>
#include <Storages/DeltaMerge/Index/LocalIndex_fwd.h>

namespace DB::DM
{

struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;

class SegmentInvertedIndexReader
{
private:
    const SegmentSnapshotPtr snapshot;
    const ColumnValueSetPtr column_value_set;
    // Global local index cache
    const LocalIndexCachePtr local_index_cache;
    LoggerPtr log;

public:
    SegmentInvertedIndexReader(
        const SegmentSnapshotPtr & snapshot_,
        const ColumnValueSetPtr & column_value_set_,
        const LocalIndexCachePtr & local_index_cache_)
        : snapshot(snapshot_)
        , column_value_set(column_value_set_)
        , local_index_cache(local_index_cache_)
        , log(Logger::get())
    {}

    static BitmapFilterPtr loadStable(
        const SegmentSnapshotPtr & snapshot,
        const ColumnValueSetPtr & column_value_set,
        const LocalIndexCachePtr & local_index_cache);
    static BitmapFilterPtr loadDelta(
        const SegmentSnapshotPtr & snapshot,
        const ColumnValueSetPtr & column_value_set,
        const LocalIndexCachePtr & local_index_cache);

    ~SegmentInvertedIndexReader() = default;

    // Load bitmap filter from segment snapshot
    BitmapFilterPtr loadStableImpl();
    BitmapFilterPtr loadDeltaImpl();
};

} // namespace DB::DM
