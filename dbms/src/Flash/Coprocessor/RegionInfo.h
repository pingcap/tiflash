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

#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Types.h>

namespace DB
{
struct RegionInfo
{
    RegionID region_id;
    UInt64 region_version;
    UInt64 region_conf_version;

    using RegionReadKeyRanges = std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>>;
    RegionReadKeyRanges key_ranges;
    const std::unordered_set<UInt64> * bypass_lock_ts;

    RegionInfo(
        RegionID id,
        UInt64 ver,
        UInt64 conf_ver,
        RegionReadKeyRanges && key_ranges_,
        const std::unordered_set<UInt64> * bypass_lock_ts_)
        : region_id(id)
        , region_version(ver)
        , region_conf_version(conf_ver)
        , key_ranges(std::move(key_ranges_))
        , bypass_lock_ts(bypass_lock_ts_)
    {}
};

using RegionInfoMap = std::unordered_map<RegionID, RegionInfo>;
using RegionInfoList = std::vector<RegionInfo>;
} // namespace DB
