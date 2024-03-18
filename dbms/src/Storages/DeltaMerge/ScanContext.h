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

#include <Storages/DeltaMerge/ReadMode.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <common/types.h>
#include <fmt/format.h>
#include <sys/types.h>
#include <tipb/executor.pb.h>

#include <atomic>


namespace DB::DM
{
/// ScanContext is used to record statistical information in table scan for current query.
/// For each table scan(one executor id), there is only one ScanContext.
/// ScanContext helps to collect the statistical information of the table scan to show in `EXPLAIN ANALYZE`.
class ScanContext
{
public:
    /// sum of scanned packs in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_dmfile_scanned_packs{0};

    /// sum of skipped packs in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_dmfile_skipped_packs{0};

    /// sum of scanned rows in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_dmfile_scanned_rows{0};

    /// sum of skipped rows in dmfiles(both stable and ColumnFileBig) among this query
    std::atomic<uint64_t> total_dmfile_skipped_rows{0};

    std::atomic<uint64_t> total_dmfile_rough_set_index_check_time_ns{0};
    std::atomic<uint64_t> total_dmfile_read_time_ns{0};

    std::atomic<uint64_t> total_remote_region_num{0};
    std::atomic<uint64_t> total_local_region_num{0};

    // the read bytes from delta layer and stable layer (in-mem, decompressed)
    std::atomic<uint64_t> user_read_bytes{0};
    std::atomic<uint64_t> disagg_read_cache_hit_size{0};
    std::atomic<uint64_t> disagg_read_cache_miss_size{0};

    // num segments, num tasks
    std::atomic<uint64_t> num_segments{0};
    std::atomic<uint64_t> num_read_tasks{0};
    std::atomic<uint64_t> num_columns{0};

    // delta rows, bytes
    std::atomic<uint64_t> delta_rows{0};
    std::atomic<uint64_t> delta_bytes{0};

    ReadMode read_mode = ReadMode::Normal;

    // - read_mode == Normal, apply mvcc to all read blocks
    // - read_mode == Bitmap, it will apply mvcc to get the bitmap
    //   then skip rows according to the mvcc bitmap and push down filter
    //   for other columns
    // - read_mode == Fast, bypass the mvcc
    // mvcc input rows, output rows
    std::atomic<uint64_t> mvcc_input_rows{0};
    std::atomic<uint64_t> mvcc_input_bytes{0};
    std::atomic<uint64_t> mvcc_output_rows{0};
    std::atomic<uint64_t> late_materialization_skip_rows{0};

    // TODO: filter
    // Learner read
    std::atomic<uint64_t> learner_read_ns{0};
    // Create snapshot from PageStorage
    std::atomic<uint64_t> create_snapshot_time_ns{0};
    // Building bitmap
    std::atomic<uint64_t> build_bitmap_time_ns{0};

    std::atomic<uint64_t> total_vector_idx_load_from_disk{0};
    std::atomic<uint64_t> total_vector_idx_load_from_cache{0};
    std::atomic<uint64_t> total_vector_idx_load_time_ms{0};
    std::atomic<uint64_t> total_vector_idx_search_time_ms{0};
    std::atomic<uint64_t> total_vector_idx_search_visited_nodes{0};
    std::atomic<uint64_t> total_vector_idx_search_discarded_nodes{0};
    std::atomic<uint64_t> total_vector_idx_read_vec_time_ms{0};
    std::atomic<uint64_t> total_vector_idx_read_others_time_ms{0};

    const String resource_group_name;

    explicit ScanContext(const String & name = "")
        : resource_group_name(name)
    {}

    void deserialize(const tipb::TiFlashScanContext & tiflash_scan_context_pb)
    {
        total_dmfile_scanned_packs = tiflash_scan_context_pb.total_dmfile_scanned_packs();
        total_dmfile_skipped_packs = tiflash_scan_context_pb.total_dmfile_skipped_packs();
        total_dmfile_scanned_rows = tiflash_scan_context_pb.total_dmfile_scanned_rows();
        total_dmfile_skipped_rows = tiflash_scan_context_pb.total_dmfile_skipped_rows();
        total_dmfile_rough_set_index_check_time_ns
            = tiflash_scan_context_pb.total_dmfile_rough_set_index_check_time_ms() * 1000000;
        total_dmfile_read_time_ns = tiflash_scan_context_pb.total_dmfile_read_time_ms() * 1000000;
        create_snapshot_time_ns = tiflash_scan_context_pb.total_create_snapshot_time_ms() * 1000000;
        total_remote_region_num = tiflash_scan_context_pb.total_remote_region_num();
        total_local_region_num = tiflash_scan_context_pb.total_local_region_num();
        user_read_bytes = tiflash_scan_context_pb.total_user_read_bytes();
        learner_read_ns = tiflash_scan_context_pb.total_learner_read_ms() * 1000000;
        disagg_read_cache_hit_size = tiflash_scan_context_pb.total_disagg_read_cache_hit_size();
        disagg_read_cache_miss_size = tiflash_scan_context_pb.total_disagg_read_cache_miss_size();

        total_vector_idx_load_from_disk = tiflash_scan_context_pb.total_vector_idx_load_from_disk();
        total_vector_idx_load_from_cache = tiflash_scan_context_pb.total_vector_idx_load_from_cache();
        total_vector_idx_load_time_ms = tiflash_scan_context_pb.total_vector_idx_load_time_ms();
        total_vector_idx_search_time_ms = tiflash_scan_context_pb.total_vector_idx_search_time_ms();
        total_vector_idx_search_visited_nodes = tiflash_scan_context_pb.total_vector_idx_search_visited_nodes();
        total_vector_idx_search_discarded_nodes = tiflash_scan_context_pb.total_vector_idx_search_discarded_nodes();
        total_vector_idx_read_vec_time_ms = tiflash_scan_context_pb.total_vector_idx_read_vec_time_ms();
        total_vector_idx_read_others_time_ms = tiflash_scan_context_pb.total_vector_idx_read_others_time_ms();
    }

    tipb::TiFlashScanContext serialize()
    {
        tipb::TiFlashScanContext tiflash_scan_context_pb{};
        tiflash_scan_context_pb.set_total_dmfile_scanned_packs(total_dmfile_scanned_packs);
        tiflash_scan_context_pb.set_total_dmfile_skipped_packs(total_dmfile_skipped_packs);
        tiflash_scan_context_pb.set_total_dmfile_scanned_rows(total_dmfile_scanned_rows);
        tiflash_scan_context_pb.set_total_dmfile_skipped_rows(total_dmfile_skipped_rows);
        tiflash_scan_context_pb.set_total_dmfile_rough_set_index_check_time_ms(
            total_dmfile_rough_set_index_check_time_ns / 1000000);
        tiflash_scan_context_pb.set_total_dmfile_read_time_ms(total_dmfile_read_time_ns / 1000000);
        tiflash_scan_context_pb.set_total_create_snapshot_time_ms(create_snapshot_time_ns / 1000000);
        tiflash_scan_context_pb.set_total_remote_region_num(total_remote_region_num);
        tiflash_scan_context_pb.set_total_local_region_num(total_local_region_num);
        tiflash_scan_context_pb.set_total_user_read_bytes(user_read_bytes);
        tiflash_scan_context_pb.set_total_learner_read_ms(learner_read_ns / 1000000);
        tiflash_scan_context_pb.set_total_disagg_read_cache_hit_size(disagg_read_cache_hit_size);
        tiflash_scan_context_pb.set_total_disagg_read_cache_miss_size(disagg_read_cache_miss_size);

        tiflash_scan_context_pb.set_total_vector_idx_load_from_disk(total_vector_idx_load_from_disk);
        tiflash_scan_context_pb.set_total_vector_idx_load_from_cache(total_vector_idx_load_from_cache);
        tiflash_scan_context_pb.set_total_vector_idx_load_time_ms(total_vector_idx_load_time_ms);
        tiflash_scan_context_pb.set_total_vector_idx_search_time_ms(total_vector_idx_search_time_ms);
        tiflash_scan_context_pb.set_total_vector_idx_search_visited_nodes(total_vector_idx_search_visited_nodes);
        tiflash_scan_context_pb.set_total_vector_idx_search_discarded_nodes(total_vector_idx_search_discarded_nodes);
        tiflash_scan_context_pb.set_total_vector_idx_read_vec_time_ms(total_vector_idx_read_vec_time_ms);
        tiflash_scan_context_pb.set_total_vector_idx_read_others_time_ms(total_vector_idx_read_others_time_ms);

        return tiflash_scan_context_pb;
    }

    void merge(const ScanContext & other)
    {
        total_dmfile_scanned_packs += other.total_dmfile_scanned_packs;
        total_dmfile_skipped_packs += other.total_dmfile_skipped_packs;
        total_dmfile_scanned_rows += other.total_dmfile_scanned_rows;
        total_dmfile_skipped_rows += other.total_dmfile_skipped_rows;
        total_dmfile_rough_set_index_check_time_ns += other.total_dmfile_rough_set_index_check_time_ns;
        total_dmfile_read_time_ns += other.total_dmfile_read_time_ns;
        create_snapshot_time_ns += other.create_snapshot_time_ns;

        total_local_region_num += other.total_local_region_num;
        total_remote_region_num += other.total_remote_region_num;
        user_read_bytes += other.user_read_bytes;
        learner_read_ns += other.learner_read_ns;
        disagg_read_cache_hit_size += other.disagg_read_cache_hit_size;
        disagg_read_cache_miss_size += other.disagg_read_cache_miss_size;

        num_segments += other.num_segments;
        num_read_tasks += other.num_read_tasks;
        // num_columns should not sum

        delta_rows += other.delta_rows;
        delta_bytes += other.delta_bytes;

        mvcc_input_rows += other.mvcc_input_rows;
        mvcc_input_bytes += other.mvcc_input_bytes;
        mvcc_output_rows += other.mvcc_output_rows;

        total_vector_idx_load_from_disk += other.total_vector_idx_load_from_disk;
        total_vector_idx_load_from_cache += other.total_vector_idx_load_from_cache;
        total_vector_idx_load_time_ms += other.total_vector_idx_load_time_ms;
        total_vector_idx_search_time_ms += other.total_vector_idx_search_time_ms;
        total_vector_idx_search_visited_nodes += other.total_vector_idx_search_visited_nodes;
        total_vector_idx_search_discarded_nodes += other.total_vector_idx_search_discarded_nodes;
        total_vector_idx_read_vec_time_ms += other.total_vector_idx_read_vec_time_ms;
        total_vector_idx_read_others_time_ms += other.total_vector_idx_read_others_time_ms;
    }

    void merge(const tipb::TiFlashScanContext & other)
    {
        total_dmfile_scanned_packs += other.total_dmfile_scanned_packs();
        total_dmfile_skipped_packs += other.total_dmfile_skipped_packs();
        total_dmfile_scanned_rows += other.total_dmfile_scanned_rows();
        total_dmfile_skipped_rows += other.total_dmfile_skipped_rows();
        total_dmfile_rough_set_index_check_time_ns += other.total_dmfile_rough_set_index_check_time_ms() * 1000000;
        total_dmfile_read_time_ns += other.total_dmfile_read_time_ms() * 1000000;
        create_snapshot_time_ns += other.total_create_snapshot_time_ms() * 1000000;
        total_local_region_num += other.total_local_region_num();
        total_remote_region_num += other.total_remote_region_num();
        user_read_bytes += other.total_user_read_bytes();
        learner_read_ns += other.total_learner_read_ms() * 1000000;
        disagg_read_cache_hit_size += other.total_disagg_read_cache_hit_size();
        disagg_read_cache_miss_size += other.total_disagg_read_cache_miss_size();

        total_vector_idx_load_from_disk += other.total_vector_idx_load_from_disk();
        total_vector_idx_load_from_cache += other.total_vector_idx_load_from_cache();
        total_vector_idx_load_time_ms += other.total_vector_idx_load_time_ms();
        total_vector_idx_search_time_ms += other.total_vector_idx_search_time_ms();
        total_vector_idx_search_visited_nodes += other.total_vector_idx_search_visited_nodes();
        total_vector_idx_search_discarded_nodes += other.total_vector_idx_search_discarded_nodes();
        total_vector_idx_read_vec_time_ms += other.total_vector_idx_read_vec_time_ms();
        total_vector_idx_read_others_time_ms += other.total_vector_idx_read_others_time_ms();
    }

    String toJson() const;
};

} // namespace DB::DM
