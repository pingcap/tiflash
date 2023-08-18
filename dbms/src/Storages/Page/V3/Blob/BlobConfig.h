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

#include <Interpreters/SettingsCommon.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <common/defines.h>

namespace DB::PS::V3
{
struct BlobConfig
{
    SettingUInt64 file_limit_size = BLOBFILE_LIMIT_SIZE;
    SettingUInt64 spacemap_type = SpaceMap::SpaceMapType::SMAP64_STD_MAP;
    SettingUInt64 block_alignment_bytes = 0;
    SettingDouble heavy_gc_valid_rate = 0.2;
    SettingDouble heavy_gc_valid_rate_raft_data = 0.05;

    String toString()
    {
        return fmt::format(
            "BlobStore Config Info: "
            "[file_limit_size={}] [spacemap_type={}] "
            "[block_alignment_bytes={}] "
            "[heavy_gc_valid_rate={}]"
            "[heavy_gc_valid_rate_raft_data={}]",
            file_limit_size,
            spacemap_type,
            block_alignment_bytes,
            heavy_gc_valid_rate,
            heavy_gc_valid_rate_raft_data);
    }

    static BlobConfig from(const PageStorageConfig & config)
    {
        BlobConfig blob_config;

        blob_config.file_limit_size = config.blob_file_limit_size;
        blob_config.spacemap_type = config.blob_spacemap_type;
        blob_config.heavy_gc_valid_rate = config.blob_heavy_gc_valid_rate;
        blob_config.heavy_gc_valid_rate_raft_data = config.blob_heavy_gc_valid_rate_raft_data;
        blob_config.block_alignment_bytes = config.blob_block_alignment_bytes;

        return blob_config;
    }
};
} // namespace DB::PS::V3
