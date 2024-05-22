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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Poco/String.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <common/defines.h>
#include <common/logger_useful.h>

#include <boost/core/noncopyable.hpp>
#include <condition_variable>
#include <mutex>

namespace DB
{
#define APPLY_FOR_FAILPOINTS_ONCE(M)                              \
    M(exception_between_drop_meta_and_data)                       \
    M(exception_between_alter_data_and_meta)                      \
    M(exception_drop_table_during_remove_meta)                    \
    M(exception_between_rename_table_data_and_metadata)           \
    M(exception_between_create_database_meta_and_directory)       \
    M(exception_before_rename_table_old_meta_removed)             \
    M(exception_after_step_1_in_exchange_partition)               \
    M(exception_before_step_2_rename_in_exchange_partition)       \
    M(exception_after_step_2_in_exchange_partition)               \
    M(exception_before_step_3_rename_in_exchange_partition)       \
    M(exception_after_step_3_in_exchange_partition)               \
    M(region_exception_after_read_from_storage_some_error)        \
    M(region_exception_after_read_from_storage_all_error)         \
    M(exception_before_dmfile_remove_encryption)                  \
    M(exception_before_dmfile_remove_from_disk)                   \
    M(force_enable_region_persister_compatible_mode)              \
    M(force_disable_region_persister_compatible_mode)             \
    M(force_triggle_background_merge_delta)                       \
    M(force_triggle_foreground_flush)                             \
    M(exception_before_mpp_register_non_root_mpp_task)            \
    M(exception_before_mpp_register_tunnel_for_non_root_mpp_task) \
    M(exception_during_mpp_register_tunnel_for_non_root_mpp_task) \
    M(exception_before_mpp_non_root_task_run)                     \
    M(exception_during_mpp_non_root_task_run)                     \
    M(exception_before_mpp_register_root_mpp_task)                \
    M(exception_before_mpp_register_tunnel_for_root_mpp_task)     \
    M(exception_before_mpp_root_task_run)                         \
    M(exception_during_mpp_root_task_run)                         \
    M(exception_during_write_to_storage)                          \
    M(force_set_sst_to_dtfile_block_size)                         \
    M(force_set_sst_decode_rand)                                  \
    M(exception_before_page_file_write_sync)                      \
    M(force_set_segment_ingest_packs_fail)                        \
    M(segment_merge_after_ingest_packs)                           \
    M(force_formal_page_file_not_exists)                          \
    M(force_legacy_or_checkpoint_page_file_exists)                \
    M(exception_in_creating_set_input_stream)                     \
    M(exception_when_read_from_log)                               \
    M(exception_mpp_hash_build)                                   \
    M(exception_before_drop_segment)                              \
    M(exception_after_drop_segment)                               \
    M(exception_between_schema_change_in_the_same_diff)           \
    M(force_ps_wal_compact)                                       \
    M(pause_before_full_gc_prepare)

#define APPLY_FOR_FAILPOINTS(M)                              \
    M(skip_check_segment_update)                             \
    M(force_set_page_file_write_errno)                       \
    M(force_split_io_size_4k)                                \
    M(minimum_block_size_for_cross_join)                     \
    M(random_exception_after_dt_write_done)                  \
    M(random_slow_page_storage_write)                        \
    M(random_exception_after_page_storage_sequence_acquired) \
    M(random_slow_page_storage_remove_expired_snapshots)     \
    M(random_slow_page_storage_list_all_live_files)          \
    M(force_set_safepoint_when_decode_block)                 \
    M(force_set_page_data_compact_batch)                     \
    M(force_set_dtfile_exist_when_acquire_id)                \
    M(force_no_local_region_for_mpp_task)                    \
    M(force_remote_read_for_batch_cop)                       \
    M(force_pd_grpc_error)                                   \
    M(force_context_path)                                    \
    M(force_slow_page_storage_snapshot_release)              \
    M(force_change_all_blobs_to_read_only)                   \
    M(force_ingest_via_delta)                                \
    M(force_ingest_via_replace)                              \
    M(unblock_query_init_after_write)                        \
    M(exception_in_merged_task_init)                         \
    M(force_fail_in_flush_region_data)                       \
    M(force_schema_sync_too_old_schema)                      \
    M(exception_after_large_write_exceed)                    \
    M(delta_tree_create_node_fail)

#define APPLY_FOR_PAUSEABLE_FAILPOINTS_ONCE(M) \
    M(pause_with_alter_locks_acquired)         \
    M(hang_in_execution)                       \
    M(pause_before_dt_background_delta_merge)  \
    M(pause_until_dt_background_delta_merge)   \
    M(pause_before_apply_raft_cmd)             \
    M(pause_before_apply_raft_snapshot)        \
    M(pause_until_apply_raft_snapshot)         \
    M(pause_after_copr_streams_acquired_once)  \
    M(pause_before_register_non_root_mpp_task) \
    M(pause_when_persist_region)

#define APPLY_FOR_PAUSEABLE_FAILPOINTS(M) \
    M(pause_when_reading_from_dt_stream)  \
    M(pause_when_writing_to_dt_store)     \
    M(pause_when_ingesting_to_dt_store)   \
    M(pause_when_altering_dt_store)       \
    M(pause_after_copr_streams_acquired)  \
    M(pause_query_init)


#define APPLY_FOR_RANDOM_FAILPOINTS(M)                  \
    M(random_tunnel_wait_timeout_failpoint)             \
    M(random_tunnel_init_rpc_failure_failpoint)         \
    M(random_receiver_sync_msg_push_failure_failpoint)  \
    M(random_receiver_async_msg_push_failure_failpoint) \
    M(random_limit_check_failpoint)                     \
    M(random_join_build_failpoint)                      \
    M(random_join_prob_failpoint)                       \
    M(random_aggregate_create_state_failpoint)          \
    M(random_aggregate_merge_failpoint)                 \
    M(random_sharedquery_failpoint)                     \
    M(random_interpreter_failpoint)                     \
    M(random_task_manager_find_task_failure_failpoint)  \
    M(random_min_tso_scheduler_failpoint)               \
    M(random_region_persister_latency_failpoint)

namespace FailPoints
{
#define M(NAME) extern const char(NAME)[] = #NAME "";
APPLY_FOR_FAILPOINTS_ONCE(M)
APPLY_FOR_FAILPOINTS(M)
APPLY_FOR_PAUSEABLE_FAILPOINTS_ONCE(M)
APPLY_FOR_PAUSEABLE_FAILPOINTS(M)
APPLY_FOR_RANDOM_FAILPOINTS(M)
#undef M
} // namespace FailPoints

#ifdef FIU_ENABLE
std::unordered_map<String, std::any> FailPointHelper::fail_point_val;
std::unordered_map<String, std::shared_ptr<FailPointChannel>> FailPointHelper::fail_point_wait_channels;
class FailPointChannel : private boost::noncopyable
{
public:
    // wake up all waiting threads when destroy
    ~FailPointChannel() { notifyAll(); }

    explicit FailPointChannel(UInt64 timeout_)
        : timeout(timeout_)
    {}
    FailPointChannel()
        : timeout(0)
    {}

    void wait()
    {
        std::unique_lock lock(m);
        if (timeout == 0)
            cv.wait(lock);
        else
            cv.wait_for(lock, std::chrono::seconds(timeout));
    }

    void notifyAll()
    {
        std::unique_lock lock(m);
        cv.notify_all();
    }

private:
    UInt64 timeout;
    std::mutex m;
    std::condition_variable cv;
};

void FailPointHelper::enablePauseFailPoint(const String & fail_point_name, UInt64 time)
{
#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>(time));   \
        return;                                                                                             \
    }

#define M(NAME) SUB_M(NAME, FIU_ONETIME)
    APPLY_FOR_PAUSEABLE_FAILPOINTS_ONCE(M)
#undef M

#define M(NAME) SUB_M(NAME, 0)
    APPLY_FOR_PAUSEABLE_FAILPOINTS(M)
#undef M
#undef SUB_M

    throw Exception(fmt::format("Cannot find fail point {}", fail_point_name), ErrorCodes::FAIL_POINT_ERROR);
}

void FailPointHelper::enableFailPoint(const String & fail_point_name, std::optional<std::any> v)
{
#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        if (v.has_value())                                                                                  \
        {                                                                                                   \
            fail_point_val.try_emplace(FailPoints::NAME, v.value());                                        \
        }                                                                                                   \
        return;                                                                                             \
    }

#define M(NAME) SUB_M(NAME, FIU_ONETIME)
    APPLY_FOR_FAILPOINTS_ONCE(M)
#undef M
#define M(NAME) SUB_M(NAME, 0)
    APPLY_FOR_FAILPOINTS(M)
#undef M
#undef SUB_M

#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>());       \
        if (v.has_value())                                                                                  \
        {                                                                                                   \
            fail_point_val.try_emplace(FailPoints::NAME, v.value());                                        \
        }                                                                                                   \
        return;                                                                                             \
    }

#define M(NAME) SUB_M(NAME, FIU_ONETIME)
    APPLY_FOR_PAUSEABLE_FAILPOINTS_ONCE(M)
#undef M

#define M(NAME) SUB_M(NAME, 0)
    APPLY_FOR_PAUSEABLE_FAILPOINTS(M)
#undef M
#undef SUB_M

    throw Exception(fmt::format("Cannot find fail point {}", fail_point_name), ErrorCodes::FAIL_POINT_ERROR);
}

std::optional<std::any>
FailPointHelper::getFailPointVal(const String & fail_point_name)
{
    if (auto iter = fail_point_val.find(fail_point_name); iter != fail_point_val.end())
    {
        return iter->second;
    }
    return std::nullopt;
}

void FailPointHelper::disableFailPoint(const String & fail_point_name)
{
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
    {
        /// can not rely on deconstruction to do the notify_all things, because
        /// if someone wait on this, the deconstruct will never be called.
        iter->second->notifyAll();
        fail_point_wait_channels.erase(iter);
    }
    fail_point_val.erase(fail_point_name);
    fiu_disable(fail_point_name.c_str());
}

void FailPointHelper::wait(const String & fail_point_name)
{
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter == fail_point_wait_channels.end())
        throw Exception("Can not find channel for fail point " + fail_point_name);
    else
    {
        auto ptr = iter->second;
        ptr->wait();
    }
}

void FailPointHelper::initRandomFailPoints(Poco::Util::LayeredConfiguration & config, const LoggerPtr & log)
{
    String random_fail_point_cfg = config.getString("flash.random_fail_points", "");
    if (random_fail_point_cfg.empty())
        return;

    Poco::StringTokenizer string_tokens(random_fail_point_cfg, ",");
    for (const auto & string_token : string_tokens)
    {
        Poco::StringTokenizer pair_tokens(string_token, "-");
        RUNTIME_ASSERT((pair_tokens.count() == 2), log, "RandomFailPoints config should be FailPointA-RatioA,FailPointB-RatioB,... format");
        double rate = atof(pair_tokens[1].c_str()); //NOLINT(cert-err34-c): check conversion error manually
        RUNTIME_ASSERT((0 <= rate && rate <= 1.0), log, "RandomFailPoint trigger rate should in [0,1], while {}", rate);
        enableRandomFailPoint(pair_tokens[0], rate);
    }
    LOG_INFO(log, "Enable RandomFailPoints: {}", random_fail_point_cfg);
}

void FailPointHelper::enableRandomFailPoint(const String & fail_point_name, double rate)
{
#define SUB_M(NAME)                                               \
    if (fail_point_name == FailPoints::NAME)                      \
    {                                                             \
        fiu_enable_random(FailPoints::NAME, 1, nullptr, 0, rate); \
        return;                                                   \
    }

#define M(NAME) SUB_M(NAME)
    APPLY_FOR_RANDOM_FAILPOINTS(M)
#undef M
#undef SUB_M

    throw Exception(fmt::format("Cannot find fail point {}", fail_point_name), ErrorCodes::FAIL_POINT_ERROR);
}
#else
class FailPointChannel
{
};

void FailPointHelper::enableFailPoint(const String &, std::optional<std::any>) {}

std::optional<std::any> FailPointHelper::getFailPointVal(const String &)
{
    return std::nullopt;
}

void FailPointHelper::enablePauseFailPoint(const String &, UInt64) {}

void FailPointHelper::disableFailPoint(const String &) {}

void FailPointHelper::wait(const String &) {}

void FailPointHelper::initRandomFailPoints(Poco::Util::LayeredConfiguration &, const LoggerPtr &) {}

void FailPointHelper::enableRandomFailPoint(const String &, double) {}
#endif

} // namespace DB
