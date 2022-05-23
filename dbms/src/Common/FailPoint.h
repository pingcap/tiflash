// Copyright 2022 PingCAP, Ltd.
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
#include <Common/Exception.h>
#include <Core/Types.h>
#include <fiu-control.h>
#include <fiu-local.h>
#include <fiu.h>

#include <unordered_map>

#define __APPLY_FOR_FAILPOINTS_ONCE(M)                            \
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
    M(exception_during_mpp_write_err_to_tunnel)                   \
    M(exception_during_mpp_close_tunnel)                          \
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
    M(exception_after_drop_segment)

#define __APPLY_FOR_FAILPOINTS(M)                            \
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
    M(force_context_path)                                    \
    M(force_slow_page_storage_snapshot_release)              \
    M(force_change_all_blobs_to_read_only)

#define __APPLY_FOR_FAILPOINTS_ONCE_WITH_CHANNEL(M) \
    M(pause_with_alter_locks_acquired)              \
    M(hang_in_execution)                            \
    M(pause_before_dt_background_delta_merge)       \
    M(pause_until_dt_background_delta_merge)        \
    M(pause_before_apply_raft_cmd)                  \
    M(pause_before_apply_raft_snapshot)             \
    M(pause_until_apply_raft_snapshot)              \
    M(pause_after_copr_streams_acquired_once)

#define __APPLY_FOR_FAILPOINTS_WITH_CHANNEL(M) \
    M(pause_when_reading_from_dt_stream)       \
    M(pause_when_writing_to_dt_store)          \
    M(pause_when_ingesting_to_dt_store)        \
    M(pause_when_altering_dt_store)            \
    M(pause_after_copr_streams_acquired)       \
    M(pause_before_server_merge_one_delta)


namespace DB
{
namespace ErrorCodes
{
extern const int FAIL_POINT_ERROR;
};

namespace FailPoints
{
#define M(NAME) inline constexpr const char * NAME = #NAME "";
__APPLY_FOR_FAILPOINTS_ONCE(M)
__APPLY_FOR_FAILPOINTS(M)
__APPLY_FOR_FAILPOINTS_ONCE_WITH_CHANNEL(M)
__APPLY_FOR_FAILPOINTS_WITH_CHANNEL(M)
#undef M
} // namespace FailPoints


/// Macros to set failpoints.
// When `fail_point` is enabled, throw an exception
#define FAIL_POINT_TRIGGER_EXCEPTION(fail_point) \
    fiu_do_on(fail_point, throw Exception("Fail point " #fail_point " is triggered.", ErrorCodes::FAIL_POINT_ERROR);)
// When `fail_point` is enabled, wait till it is disabled
#define FAIL_POINT_PAUSE(fail_point) fiu_do_on(fail_point, FailPointHelper::wait(fail_point);)


class FailPointChannel;
class FailPointHelper
{
public:
    static void enableFailPoint(const String & fail_point_name);

    static void disableFailPoint(const String & fail_point_name);

    static void wait(const String & fail_point_name);

private:
    static std::unordered_map<String, std::shared_ptr<FailPointChannel>> fail_point_wait_channels;
};
} // namespace DB
