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

#include <Common/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
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
    std::atomic<uint64_t> dmfile_data_scanned_rows{0};
    std::atomic<uint64_t> dmfile_data_skipped_rows{0};
    std::atomic<uint64_t> dmfile_mvcc_scanned_rows{0};
    std::atomic<uint64_t> dmfile_mvcc_skipped_rows{0};
    std::atomic<uint64_t> dmfile_lm_filter_scanned_rows{0};
    std::atomic<uint64_t> dmfile_lm_filter_skipped_rows{0};
    std::atomic<uint64_t> total_dmfile_read_time_ns{0};

    std::atomic<uint64_t> total_rs_pack_filter_check_time_ns{0};
    std::atomic<uint64_t> rs_pack_filter_none{0};
    std::atomic<uint64_t> rs_pack_filter_some{0};
    std::atomic<uint64_t> rs_pack_filter_all{0};
    std::atomic<uint64_t> rs_pack_filter_all_null{0};

    std::atomic<uint64_t> total_remote_region_num{0};
    std::atomic<uint64_t> total_local_region_num{0};
    std::atomic<uint64_t> num_stale_read{0};

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

    // Learner read
    std::atomic<uint64_t> learner_read_ns{0};
    // Create snapshot from PageStorage
    std::atomic<uint64_t> create_snapshot_time_ns{0};
    std::atomic<uint64_t> build_inputstream_time_ns{0};
    // Building bitmap
    std::atomic<uint64_t> build_bitmap_time_ns{0};

    const String resource_group_name;

    explicit ScanContext(const String & name = "")
        : resource_group_name(name)
    {}

    void deserialize(const tipb::TiFlashScanContext & tiflash_scan_context_pb)
    {
        dmfile_data_scanned_rows = tiflash_scan_context_pb.dmfile_data_scanned_rows();
        dmfile_data_skipped_rows = tiflash_scan_context_pb.dmfile_data_skipped_rows();
        dmfile_mvcc_scanned_rows = tiflash_scan_context_pb.dmfile_mvcc_scanned_rows();
        dmfile_mvcc_skipped_rows = tiflash_scan_context_pb.dmfile_mvcc_skipped_rows();
        dmfile_lm_filter_scanned_rows = tiflash_scan_context_pb.dmfile_lm_filter_scanned_rows();
        dmfile_lm_filter_skipped_rows = tiflash_scan_context_pb.dmfile_lm_filter_skipped_rows();
        total_rs_pack_filter_check_time_ns = tiflash_scan_context_pb.total_dmfile_rs_check_ms() * 1000000;
        // TODO: rs_pack_filter_none, rs_pack_filter_some, rs_pack_filter_all,rs_pack_filter_all_null
        total_dmfile_read_time_ns = tiflash_scan_context_pb.total_dmfile_read_ms() * 1000000;
        create_snapshot_time_ns = tiflash_scan_context_pb.total_build_snapshot_ms() * 1000000;
        total_remote_region_num = tiflash_scan_context_pb.remote_regions();
        total_local_region_num = tiflash_scan_context_pb.local_regions();
        user_read_bytes = tiflash_scan_context_pb.user_read_bytes();
        learner_read_ns = tiflash_scan_context_pb.total_learner_read_ms() * 1000000;
        disagg_read_cache_hit_size = tiflash_scan_context_pb.disagg_read_cache_hit_bytes();
        disagg_read_cache_miss_size = tiflash_scan_context_pb.disagg_read_cache_miss_bytes();

        num_segments = tiflash_scan_context_pb.segments();
        num_read_tasks = tiflash_scan_context_pb.read_tasks();

        delta_rows = tiflash_scan_context_pb.delta_rows();
        delta_bytes = tiflash_scan_context_pb.delta_bytes();

        mvcc_input_rows = tiflash_scan_context_pb.mvcc_input_rows();
        mvcc_input_bytes = tiflash_scan_context_pb.mvcc_input_bytes();
        mvcc_output_rows = tiflash_scan_context_pb.mvcc_output_rows();
        late_materialization_skip_rows = tiflash_scan_context_pb.lm_skip_rows();
        build_bitmap_time_ns = tiflash_scan_context_pb.total_build_bitmap_ms() * 1000000;
        num_stale_read = tiflash_scan_context_pb.stale_read_regions();
        build_inputstream_time_ns = tiflash_scan_context_pb.total_build_inputstream_ms() * 1000000;

        setStreamCost(
            tiflash_scan_context_pb.min_local_stream_ms() * 1000000,
            tiflash_scan_context_pb.max_local_stream_ms() * 1000000,
            tiflash_scan_context_pb.min_remote_stream_ms() * 1000000,
            tiflash_scan_context_pb.max_remote_stream_ms() * 1000000);

        deserializeRegionNumberOfInstance(tiflash_scan_context_pb);
    }

    tipb::TiFlashScanContext serialize()
    {
        tipb::TiFlashScanContext tiflash_scan_context_pb{};
        tiflash_scan_context_pb.set_dmfile_data_scanned_rows(dmfile_data_scanned_rows);
        tiflash_scan_context_pb.set_dmfile_data_skipped_rows(dmfile_data_skipped_rows);
        tiflash_scan_context_pb.set_dmfile_mvcc_scanned_rows(dmfile_mvcc_scanned_rows);
        tiflash_scan_context_pb.set_dmfile_mvcc_skipped_rows(dmfile_mvcc_skipped_rows);
        tiflash_scan_context_pb.set_dmfile_lm_filter_scanned_rows(dmfile_lm_filter_scanned_rows);
        tiflash_scan_context_pb.set_dmfile_lm_filter_skipped_rows(dmfile_lm_filter_skipped_rows);
        tiflash_scan_context_pb.set_total_dmfile_rs_check_ms(total_rs_pack_filter_check_time_ns / 1000000);
        // TODO: pack_filter_none, pack_filter_some, pack_filter_all
        tiflash_scan_context_pb.set_total_dmfile_read_ms(total_dmfile_read_time_ns / 1000000);
        tiflash_scan_context_pb.set_total_build_snapshot_ms(create_snapshot_time_ns / 1000000);
        tiflash_scan_context_pb.set_remote_regions(total_remote_region_num);
        tiflash_scan_context_pb.set_local_regions(total_local_region_num);
        tiflash_scan_context_pb.set_user_read_bytes(user_read_bytes);
        tiflash_scan_context_pb.set_total_learner_read_ms(learner_read_ns / 1000000);
        tiflash_scan_context_pb.set_disagg_read_cache_hit_bytes(disagg_read_cache_hit_size);
        tiflash_scan_context_pb.set_disagg_read_cache_miss_bytes(disagg_read_cache_miss_size);

        tiflash_scan_context_pb.set_segments(num_segments);
        tiflash_scan_context_pb.set_read_tasks(num_read_tasks);

        tiflash_scan_context_pb.set_delta_rows(delta_rows);
        tiflash_scan_context_pb.set_delta_bytes(delta_bytes);

        tiflash_scan_context_pb.set_mvcc_input_rows(mvcc_input_rows);
        tiflash_scan_context_pb.set_mvcc_input_bytes(mvcc_input_bytes);
        tiflash_scan_context_pb.set_mvcc_output_rows(mvcc_output_rows);
        tiflash_scan_context_pb.set_lm_skip_rows(late_materialization_skip_rows);
        tiflash_scan_context_pb.set_total_build_bitmap_ms(build_bitmap_time_ns / 1000000);
        tiflash_scan_context_pb.set_stale_read_regions(num_stale_read);
        tiflash_scan_context_pb.set_total_build_inputstream_ms(build_inputstream_time_ns / 1000000);

        tiflash_scan_context_pb.set_min_local_stream_ms(local_min_stream_cost_ns / 1000000);
        tiflash_scan_context_pb.set_max_local_stream_ms(local_max_stream_cost_ns / 1000000);
        tiflash_scan_context_pb.set_min_remote_stream_ms(remote_min_stream_cost_ns / 1000000);
        tiflash_scan_context_pb.set_max_remote_stream_ms(remote_max_stream_cost_ns / 1000000);

        serializeRegionNumOfInstance(tiflash_scan_context_pb);

        return tiflash_scan_context_pb;
    }

    void merge(const ScanContext & other)
    {
        dmfile_data_scanned_rows += other.dmfile_data_scanned_rows;
        dmfile_data_skipped_rows += other.dmfile_data_skipped_rows;
        dmfile_mvcc_scanned_rows += other.dmfile_mvcc_scanned_rows;
        dmfile_mvcc_skipped_rows += other.dmfile_mvcc_skipped_rows;
        dmfile_lm_filter_scanned_rows += other.dmfile_lm_filter_scanned_rows;
        dmfile_lm_filter_skipped_rows += other.dmfile_lm_filter_skipped_rows;
        total_rs_pack_filter_check_time_ns += other.total_rs_pack_filter_check_time_ns;
        rs_pack_filter_none += other.rs_pack_filter_none;
        rs_pack_filter_some += other.rs_pack_filter_some;
        rs_pack_filter_all += other.rs_pack_filter_all;
        rs_pack_filter_all_null += other.rs_pack_filter_all_null;
        total_dmfile_read_time_ns += other.total_dmfile_read_time_ns;

        total_local_region_num += other.total_local_region_num;
        total_remote_region_num += other.total_remote_region_num;
        user_read_bytes += other.user_read_bytes;
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
        late_materialization_skip_rows += other.late_materialization_skip_rows;

        learner_read_ns += other.learner_read_ns;
        create_snapshot_time_ns += other.create_snapshot_time_ns;
        build_inputstream_time_ns += other.build_inputstream_time_ns;
        build_bitmap_time_ns += other.build_bitmap_time_ns;

        num_stale_read += other.num_stale_read;

        mergeStreamCost(
            other.local_min_stream_cost_ns,
            other.local_max_stream_cost_ns,
            other.remote_min_stream_cost_ns,
            other.remote_max_stream_cost_ns);

        mergeRegionNumberOfInstance(other);
    }

    void merge(const tipb::TiFlashScanContext & other)
    {
        dmfile_data_scanned_rows += other.dmfile_data_scanned_rows();
        dmfile_data_skipped_rows += other.dmfile_data_skipped_rows();
        dmfile_mvcc_scanned_rows += other.dmfile_mvcc_scanned_rows();
        dmfile_mvcc_skipped_rows += other.dmfile_mvcc_skipped_rows();
        dmfile_lm_filter_scanned_rows += other.dmfile_lm_filter_scanned_rows();
        dmfile_lm_filter_skipped_rows += other.dmfile_lm_filter_skipped_rows();
        total_rs_pack_filter_check_time_ns += other.total_dmfile_rs_check_ms() * 1000000;
        // TODO: rs_pack_filter_none, rs_pack_filter_some, rs_pack_filter_all, rs_pack_filter_all_null
        total_dmfile_read_time_ns += other.total_dmfile_read_ms() * 1000000;
        create_snapshot_time_ns += other.total_build_snapshot_ms() * 1000000;
        total_local_region_num += other.local_regions();
        total_remote_region_num += other.remote_regions();
        user_read_bytes += other.user_read_bytes();
        learner_read_ns += other.total_learner_read_ms() * 1000000;
        disagg_read_cache_hit_size += other.disagg_read_cache_hit_bytes();
        disagg_read_cache_miss_size += other.disagg_read_cache_miss_bytes();

        num_segments += other.segments();
        num_read_tasks += other.read_tasks();

        delta_rows += other.delta_rows();
        delta_bytes += other.delta_bytes();

        mvcc_input_rows += other.mvcc_input_rows();
        mvcc_input_bytes += other.mvcc_input_bytes();
        mvcc_output_rows += other.mvcc_output_rows();
        late_materialization_skip_rows += other.lm_skip_rows();
        build_bitmap_time_ns += other.total_build_bitmap_ms() * 1000000;
        num_stale_read += other.stale_read_regions();
        build_inputstream_time_ns += other.total_build_inputstream_ms() * 1000000;

        mergeStreamCost(
            other.min_local_stream_ms() * 1000000,
            other.max_local_stream_ms() * 1000000,
            other.min_remote_stream_ms() * 1000000,
            other.max_remote_stream_ms() * 1000000);

        mergeRegionNumberOfInstance(other);
    }

    String toJson() const;

    void setRegionNumOfCurrentInstance(uint64_t region_num);
    void setStreamCost(uint64_t local_min_ns, uint64_t local_max_ns, uint64_t remote_min_ns, uint64_t remote_max_ns);

    static void initCurrentInstanceId(Poco::Util::AbstractConfiguration & config, const LoggerPtr & log);

private:
    void serializeRegionNumOfInstance(tipb::TiFlashScanContext & proto) const;
    void deserializeRegionNumberOfInstance(const tipb::TiFlashScanContext & proto);
    void mergeRegionNumberOfInstance(const ScanContext & other);
    void mergeRegionNumberOfInstance(const tipb::TiFlashScanContext & other);
    void mergeStreamCost(uint64_t local_min_ns, uint64_t local_max_ns, uint64_t remote_min_ns, uint64_t remote_max_ns);

    // instance_id -> number of regions.
    // `region_num_of_instance` is accessed by a single thread.
    using RegionNumOfInstance = std::unordered_map<String, uint64_t>;
    RegionNumOfInstance region_num_of_instance;

    // These members `*_stream_cost_ns` are accessed by a single thread.
    uint64_t local_min_stream_cost_ns{0};
    uint64_t local_max_stream_cost_ns{0};
    uint64_t remote_min_stream_cost_ns{0};
    uint64_t remote_max_stream_cost_ns{0};

    // `current_instance_id` is a identification of this store.
    // It only used to identify which store generated the ScanContext object.
    inline static String current_instance_id;
};

} // namespace DB::DM
