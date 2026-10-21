// Copyright 2026 PingCAP, Inc.
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

#include <tici-search-lib/src/lib.rs.h>

namespace DB
{

enum class TiCIAdmissionRoute
{
    Local,
    Remote,
    Retry,
};

inline ShardFreshnessRequirement makeTiCIShardRequirement(
    std::uint64_t shard_id,
    std::uint64_t shard_epoch,
    std::uint64_t required_resolved_ts,
    ShardFreshnessMode mode)
{
    // TiCI ignores epoch and watermark in legacy mode and echoes zero.
    const bool legacy = mode == ShardFreshnessMode::Legacy;
    return ShardFreshnessRequirement{
        .shard_id = shard_id,
        .expected_epoch = legacy ? 0 : shard_epoch,
        .required_resolved_ts = legacy ? 0 : required_resolved_ts,
        .mode = mode,
    };
}

inline TiCIAdmissionRoute routeTiCIShard(
    const ShardFreshnessRequirement & requirement,
    const ShardAdmissionResult & result)
{
    if (result.code == ShardAdmissionCode::Ready)
    {
        if (requirement.mode == ShardFreshnessMode::Legacy
            || result.reader_applied_resolved_ts >= requirement.required_resolved_ts)
            return TiCIAdmissionRoute::Local;
        return TiCIAdmissionRoute::Retry;
    }
    // The current one-hop remote request does not serialize a freshness
    // requirement. Only legacy presence misses may take that path.
    if (result.code == ShardAdmissionCode::Missing && requirement.mode == ShardFreshnessMode::Legacy)
        return TiCIAdmissionRoute::Remote;
    return TiCIAdmissionRoute::Retry;
}

} // namespace DB
