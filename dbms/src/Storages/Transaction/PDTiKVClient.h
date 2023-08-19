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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <Core/Types.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/pd/IClient.h>
#pragma GCC diagnostic pop

#include <Core/Types.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

// We define a shared ptr here, because TMTContext / SchemaSyncer / IndexReader all need to
// `share` the resource of cluster.
using KVClusterPtr = std::shared_ptr<pingcap::kv::Cluster>;


namespace DB
{
struct PDClientHelper
{
    static constexpr int get_safepoint_maxtime = 120000; // 120s. waiting pd recover.

    static Timestamp getGCSafePointWithRetry(
        const pingcap::pd::ClientPtr & pd_client,
        bool ignore_cache = true,
        Int64 safe_point_update_interval_seconds = 30)
    {
        if (!ignore_cache)
        {
            // In case we cost too much to update safe point from PD.
            std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - safe_point_last_update_time);
            const auto min_interval = std::max(Int64(1), safe_point_update_interval_seconds); // at least one second
            if (duration.count() < min_interval)
                return cached_gc_safe_point;
        }

        pingcap::kv::Backoffer bo(get_safepoint_maxtime);
        for (;;)
        {
            try
            {
                auto safe_point = pd_client->getGCSafePoint();
                cached_gc_safe_point = safe_point;
                safe_point_last_update_time = std::chrono::system_clock::now();
                return safe_point;
            }
            catch (pingcap::Exception & e)
            {
                bo.backoff(pingcap::kv::boPDRPC, e);
            }
        }
    }

private:
    static Timestamp cached_gc_safe_point;
    static std::chrono::time_point<std::chrono::system_clock> safe_point_last_update_time;
};


} // namespace DB
