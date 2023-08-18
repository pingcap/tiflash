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

#include <Core/Types.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/Page/PageDefinesBase.h>

namespace DB
{
namespace MVCC
{
// V2 MVCC config
struct VersionSetConfig
{
    size_t compact_hint_delta_deletions = 5000;
    size_t compact_hint_delta_entries = 200 * 1000;

    void setSnapshotCleanupProb(UInt32 prob)
    {
        // Range from [0, 1000)
        prob = std::max(1U, prob);
        prob = std::min(1000U, prob);

        prob_cleanup_invalid_snapshot = prob;
    }

    bool doCleanup() const;

private:
    // Probability to cleanup invalid snapshots. 10 out of 1000 by default.
    size_t prob_cleanup_invalid_snapshot = 10;
};
} // namespace MVCC

struct PageStorageConfig
{
    //==========================================================================================
    // V2 config
    //==========================================================================================
    SettingBool sync_on_write = true;

    SettingUInt64 file_roll_size = PAGE_FILE_ROLL_SIZE;
    SettingUInt64 file_max_size = PAGE_FILE_MAX_SIZE;
    SettingUInt64 file_small_size = PAGE_FILE_SMALL_SIZE;

    SettingUInt64 file_meta_roll_size = PAGE_META_ROLL_SIZE;

    // When the value of gc_force_hardlink_rate is less than or equal to 1,
    // It means that candidates whose valid rate is greater than this value will be forced to hardlink(This will reduce the gc duration).
    // Otherwise, if gc_force_hardlink_rate is greater than 1, hardlink won't happen
    SettingDouble gc_force_hardlink_rate = 2;

    SettingDouble gc_max_valid_rate = 0.35;
    SettingUInt64 gc_min_bytes = PAGE_FILE_ROLL_SIZE;
    SettingUInt64 gc_min_files = 10;
    // Minimum number of legacy files to be selected for compaction
    SettingUInt64 gc_min_legacy_num = 3;

    SettingUInt64 gc_max_expect_legacy_files = 100;
    SettingDouble gc_max_valid_rate_bound = 1.0;

    // Maximum write concurrency. Must not be changed once the PageStorage object is created.
    SettingUInt64 num_write_slots = 1;

    // Maximum seconds of reader / writer idle time.
    // 0 for never reclaim idle file descriptor.
    SettingUInt64 open_file_max_idle_time = 15;

    // Probability to do gc when write is low.
    // The probability is `prob_do_gc_when_write_is_low` out of 1000.
    SettingUInt64 prob_do_gc_when_write_is_low = 10;

    MVCC::VersionSetConfig version_set_config;

    // Use a more easy gc config for v2 when all of its data will be transformed to v3.
    static PageStorageConfig getEasyGCConfig()
    {
        PageStorageConfig gc_config;
        gc_config.file_roll_size = PAGE_FILE_SMALL_SIZE;
        return gc_config;
    }

    //==========================================================================================
    // V3 config
    //==========================================================================================
    SettingUInt64 blob_file_limit_size = BLOBFILE_LIMIT_SIZE;
    SettingUInt64 blob_spacemap_type = 2;
    SettingDouble blob_heavy_gc_valid_rate = 0.5;
    SettingDouble blob_heavy_gc_valid_rate_raft_data = 0.05;
    SettingUInt64 blob_block_alignment_bytes = 0;

    SettingUInt64 wal_roll_size = PAGE_META_ROLL_SIZE;
    SettingUInt64 wal_max_persisted_log_files = MAX_PERSISTED_LOG_FILES;

    void reload(const PageStorageConfig & rhs)
    {
        // Reload is not atomic, but should be good enough

        // Reload gc threshold
        gc_force_hardlink_rate = rhs.gc_force_hardlink_rate;
        gc_max_valid_rate = rhs.gc_max_valid_rate;
        gc_min_bytes = rhs.gc_min_bytes;
        gc_min_files = rhs.gc_min_files;
        gc_min_legacy_num = rhs.gc_min_legacy_num;
        prob_do_gc_when_write_is_low = rhs.prob_do_gc_when_write_is_low;
        // Reload fd idle time
        open_file_max_idle_time = rhs.open_file_max_idle_time;

        // Reload V3 setting
        blob_file_limit_size = rhs.blob_file_limit_size;
        blob_spacemap_type = rhs.blob_spacemap_type;
        blob_heavy_gc_valid_rate = rhs.blob_heavy_gc_valid_rate;
        blob_heavy_gc_valid_rate_raft_data = rhs.blob_heavy_gc_valid_rate_raft_data;
        blob_block_alignment_bytes = rhs.blob_block_alignment_bytes;

        wal_roll_size = rhs.wal_roll_size;
        wal_max_persisted_log_files = rhs.wal_max_persisted_log_files;
    }

    String toDebugStringV2() const
    {
        return fmt::format(
            "PageStorageConfig V2 {{gc_min_files: {}, gc_min_bytes:{}, gc_force_hardlink_rate: {:.3f}, "
            "gc_max_valid_rate: {:.3f}, "
            "gc_min_legacy_num: {}, gc_max_expect_legacy: {}, gc_max_valid_rate_bound: {:.3f}, "
            "prob_do_gc_when_write_is_low: {}, "
            "open_file_max_idle_time: {}}}",
            gc_min_files,
            gc_min_bytes,
            gc_force_hardlink_rate.get(),
            gc_max_valid_rate.get(),
            gc_min_legacy_num,
            gc_max_expect_legacy_files.get(),
            gc_max_valid_rate_bound.get(),
            prob_do_gc_when_write_is_low,
            open_file_max_idle_time);
    }

    String toDebugStringV3() const
    {
        return fmt::format(
            "PageStorageConfig {{"
            "blob_file_limit_size: {}, blob_spacemap_type: {}, "
            "blob_heavy_gc_valid_rate: {:.3f}, blob_heavy_gc_valid_rate_raft_data: {:.3f}, "
            "blob_block_alignment_bytes: {}, wal_roll_size: {}, wal_max_persisted_log_files: {}}}",
            blob_file_limit_size.get(),
            blob_spacemap_type.get(),
            blob_heavy_gc_valid_rate.get(),
            blob_heavy_gc_valid_rate_raft_data.get(),
            blob_block_alignment_bytes.get(),
            wal_roll_size.get(),
            wal_max_persisted_log_files.get());
    }
};
} // namespace DB
