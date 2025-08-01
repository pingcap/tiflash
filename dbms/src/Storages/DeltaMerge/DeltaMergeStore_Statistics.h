// Copyright 2023 PingCAP, Inc.
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

#include <Storages/DeltaMerge/RowKeyRange.h>
#include <common/types.h>

namespace DB::DM
{

struct SegmentStats
{
    UInt64 segment_id = 0;
    RowKeyRange range;
    UInt64 epoch = 0;
    UInt64 rows = 0;
    UInt64 size = 0;

    Float64 delta_rate = 0;
    UInt64 delta_memtable_rows = 0;
    UInt64 delta_memtable_size = 0;
    UInt64 delta_memtable_column_files = 0;
    UInt64 delta_memtable_delete_ranges = 0;
    UInt64 delta_persisted_page_id = 0;
    UInt64 delta_persisted_rows = 0;
    UInt64 delta_persisted_size = 0;
    UInt64 delta_persisted_column_files = 0;
    UInt64 delta_persisted_delete_ranges = 0;
    UInt64 delta_cache_size = 0;
    UInt64 delta_cache_alloc_size = 0;
    UInt64 delta_index_size = 0;

    UInt64 stable_page_id = 0;
    UInt64 stable_rows = 0;
    UInt64 stable_size = 0;
    UInt64 stable_dmfiles = 0;
    UInt64 stable_dmfiles_id_0 = 0;
    UInt64 stable_dmfiles_rows = 0;
    UInt64 stable_dmfiles_size = 0;
    UInt64 stable_dmfiles_size_on_disk = 0;
    UInt64 stable_dmfiles_packs = 0;
};
using SegmentsStats = std::vector<SegmentStats>;

struct StoreStats
{
    UInt64 column_count = 0;
    UInt64 segment_count = 0;

    UInt64 total_rows = 0;
    UInt64 total_size = 0;
    UInt64 total_delete_ranges = 0;

    Float64 delta_rate_rows = 0;
    Float64 delta_rate_segments = 0;

    Float64 delta_placed_rate = 0;
    UInt64 delta_cache_size = 0;
    UInt64 delta_cache_alloc_size = 0;
    Float64 delta_cache_rate = 0;
    Float64 delta_cache_wasted_rate = 0;

    UInt64 delta_index_size = 0;

    Float64 avg_segment_rows = 0;
    Float64 avg_segment_size = 0;

    UInt64 delta_count = 0;
    UInt64 total_delta_rows = 0;
    UInt64 total_delta_size = 0;
    Float64 avg_delta_rows = 0;
    Float64 avg_delta_size = 0;
    Float64 avg_delta_delete_ranges = 0;

    UInt64 stable_count = 0;
    UInt64 total_stable_rows = 0;
    UInt64 total_stable_size = 0;
    UInt64 total_stable_size_on_disk = 0;
    Float64 avg_stable_rows = 0;
    Float64 avg_stable_size = 0;

    // statistics about column file in delta
    UInt64 total_pack_count_in_delta = 0;
    UInt64 max_pack_count_in_delta = 0;
    Float64 avg_pack_count_in_delta = 0;
    Float64 avg_pack_rows_in_delta = 0;
    Float64 avg_pack_size_in_delta = 0;

    UInt64 total_pack_count_in_stable = 0;
    Float64 avg_pack_count_in_stable = 0;
    Float64 avg_pack_rows_in_stable = 0;
    Float64 avg_pack_size_in_stable = 0;

    UInt64 storage_stable_num_snapshots = 0;
    Float64 storage_stable_oldest_snapshot_lifetime = 0.0;
    UInt64 storage_stable_oldest_snapshot_thread_id = 0;
    String storage_stable_oldest_snapshot_tracing_id;

    UInt64 storage_delta_num_snapshots = 0;
    Float64 storage_delta_oldest_snapshot_lifetime = 0.0;
    UInt64 storage_delta_oldest_snapshot_thread_id = 0;
    String storage_delta_oldest_snapshot_tracing_id;

    UInt64 storage_meta_num_snapshots = 0;
    Float64 storage_meta_oldest_snapshot_lifetime = 0.0;
    UInt64 storage_meta_oldest_snapshot_thread_id = 0;
    String storage_meta_oldest_snapshot_tracing_id;

    UInt64 background_tasks_length = 0;
};
} // namespace DB::DM
