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
#include <pingcap/kv/RegionClient.h>
#include <pingcap/pd/IClient.h>
#pragma GCC diagnostic pop

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/Types.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>
#include <fiu.h>

#include <atomic>
#include <magic_enum.hpp>

namespace DB
{
namespace FailPoints
{
extern const char force_pd_grpc_error[];
} // namespace FailPoints

struct PDClientHelper
{
    static constexpr int get_safepoint_maxtime = 120000; // 120s. waiting pd recover.

    // 10 seconds timeout for getting TSO
    // https://github.com/pingcap/tidb/blob/069631e2ecfedc000ffb92c67207bea81380f020/pkg/store/mockstore/unistore/pd/client.go#L256-L276
    static constexpr int get_tso_maxtime = 10'000;

    static UInt64 getTSO(const pingcap::pd::ClientPtr & pd_client, size_t timeout_ms)
    {
        pingcap::kv::Backoffer bo(timeout_ms);
        while (true)
        {
            try
            {
                fiu_do_on(FailPoints::force_pd_grpc_error, {
                    throw pingcap::Exception("force_pd_grpc_error", pingcap::ErrorCodes::GRPCErrorCode);
                });

                return pd_client->getTS();
            }
            catch (pingcap::Exception & e)
            {
                try
                {
                    bo.backoff(pingcap::kv::boPDRPC, e);
                }
                catch (pingcap::Exception & e)
                {
                    // The backoff meets deadline exceeded
                    // Wrap the exception by DB::Exception to get the stacktrack
                    throw DB::Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "pingcap::Exception code={} msg={}",
                        magic_enum::enum_name(static_cast<pingcap::ErrorCodes>(e.code())),
                        e.message());
                }
            }
        }
    }

    static Timestamp getGCSafePointWithRetry(
        const pingcap::pd::ClientPtr & pd_client,
        bool ignore_cache = true,
        Int64 safe_point_update_interval_seconds = 30)
    {
        if (!ignore_cache)
        {
            // In case we cost too much to update safe point from PD.
            std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - safe_point_last_update_time.load());
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
    static std::atomic<Timestamp> cached_gc_safe_point;
    static std::atomic<std::chrono::time_point<std::chrono::system_clock>> safe_point_last_update_time;
};


} // namespace DB
