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

#include <Interpreters/SettingsCommon.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <common/defines.h>

namespace DB::PS::V3
{
struct BlobConfig
{
    SettingUInt64 file_limit_size = BLOBFILE_LIMIT_SIZE;
    SettingUInt64 spacemap_type = SpaceMap::SpaceMapType::SMAP64_STD_MAP;
    SettingUInt64 cached_fd_size = BLOBSTORE_CACHED_FD_SIZE;
    SettingUInt64 block_alignment_bytes = 0;
    SettingDouble heavy_gc_valid_rate = 0.2;

    String toString()
    {
        return fmt::format("BlobStore Config Info: "
                           "[file_limit_size={}],[spacemap_type={}],"
                           "[cached_fd_size={}],[block_alignment_bytes={}],"
                           "[heavy_gc_valid_rate={}]",
                           file_limit_size,
                           spacemap_type,
                           cached_fd_size,
                           block_alignment_bytes,
                           heavy_gc_valid_rate);
    }
};
} // namespace DB::PS::V3
