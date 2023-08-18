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

#include <Common/Exception.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/WALRecoveryMode.h>

namespace DB::PS::V3
{
struct WALConfig
{
    SettingUInt64 roll_size = PAGE_META_ROLL_SIZE;
    SettingUInt64 max_persisted_log_files = MAX_PERSISTED_LOG_FILES;

private:
    SettingUInt64 wal_recover_mode = 0;

public:
    void setRecoverMode(UInt64 recover_mode)
    {
        RUNTIME_CHECK_MSG(
            recover_mode == static_cast<UInt64>(WALRecoveryMode::TolerateCorruptedTailRecords)
                || recover_mode == static_cast<UInt64>(WALRecoveryMode::AbsoluteConsistency)
                || recover_mode == static_cast<UInt64>(WALRecoveryMode::PointInTimeRecovery)
                || recover_mode == static_cast<UInt64>(WALRecoveryMode::SkipAnyCorruptedRecords),
            "Unknow recover mode [num={}]",
            recover_mode);
        wal_recover_mode = recover_mode;
    }

    WALRecoveryMode getRecoverMode() const { return static_cast<WALRecoveryMode>(wal_recover_mode.get()); }

    static WALConfig from(const PageStorageConfig & config)
    {
        WALConfig wal_config;

        wal_config.roll_size = config.wal_roll_size;
        wal_config.max_persisted_log_files = config.wal_max_persisted_log_files;

        return wal_config;
    }
};

} // namespace DB::PS::V3
