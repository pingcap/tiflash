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
#include <Storages/KVStore/Types.h>
#include <common/logger_useful.h>
#include <fiu.h>

#include <atomic>
#include <magic_enum.hpp>

using TimePoint = std::atomic<std::chrono::time_point<std::chrono::steady_clock>>;


namespace DB
{
namespace FailPoints
{
extern const char force_pd_grpc_error[];
} // namespace FailPoints

struct KeyspaceGCInfo
{
    DB::Timestamp ks_gc_sp{};
    TimePoint ks_gc_sp_update_time;

    KeyspaceGCInfo() { ks_gc_sp_update_time = std::chrono::steady_clock::now(); }

    KeyspaceGCInfo(const KeyspaceGCInfo & other)
    {
        ks_gc_sp = other.ks_gc_sp;
        ks_gc_sp_update_time = std::chrono::steady_clock::now();
    }

    KeyspaceGCInfo & operator=(const KeyspaceGCInfo & other)
    {
        if (this != &other)
        {
            ks_gc_sp = other.ks_gc_sp;
            ks_gc_sp_update_time = std::chrono::steady_clock::now();
        }
        return *this;
    }
};


struct PDClientHelper
{
    static constexpr int get_safepoint_maxtime = 120000; // 120s. waiting pd recover.

    // 10 seconds timeout for getting TSO
    // https://github.com/pingcap/tidb/blob/069631e2ecfedc000ffb92c67207bea81380f020/pkg/store/mockstore/unistore/pd/client.go#L256-L276
    static constexpr int get_tso_maxtime = 10'000;

    static bool enable_safepoint_v2;

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
        KeyspaceID keyspace_id,
        bool ignore_cache = true,
        Int64 safe_point_update_interval_seconds = 30)
    {
        // If keyspace id is `NullspaceID` it need to use safe point v1.
        if (enable_safepoint_v2 && keyspace_id != NullspaceID)
        {
            auto gc_safe_point
                = getGCSafePointV2WithRetry(pd_client, keyspace_id, ignore_cache, safe_point_update_interval_seconds);
            LOG_TRACE(Logger::get(), "use safe point v2, keyspace={} gc_safe_point={}", keyspace_id, gc_safe_point);
            return gc_safe_point;
        }

        if (!ignore_cache)
        {
            // In case we cost too much to update safe point from PD.
            auto now = std::chrono::steady_clock::now();
            const auto duration
                = std::chrono::duration_cast<std::chrono::seconds>(now - safe_point_last_update_time.load());
            const auto min_interval
                = std::max(static_cast<Int64>(1), safe_point_update_interval_seconds); // at least one second
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
                LOG_TRACE(Logger::get(), "use safe point v1, gc_safe_point={}", safe_point);
                safe_point_last_update_time = std::chrono::steady_clock::now();
                return safe_point;
            }
            catch (pingcap::Exception & e)
            {
                bo.backoff(pingcap::kv::boPDRPC, e);
            }
        }
    }

    static Timestamp getGCSafePointV2WithRetry(
        const pingcap::pd::ClientPtr & pd_client,
        KeyspaceID keyspace_id,
        bool ignore_cache = false,
        Int64 safe_point_update_interval_seconds = 30)
    {
        if (!ignore_cache)
        {
            // In case we cost too much to update safe point from PD.
            auto now = std::chrono::steady_clock::now();

            auto ks_gc_info = getKeyspaceGCSafepoint(keyspace_id);
            const auto duration
                = std::chrono::duration_cast<std::chrono::seconds>(now - ks_gc_info.ks_gc_sp_update_time.load());
            const auto min_interval
                = std::max(static_cast<Int64>(1), safe_point_update_interval_seconds); // at least one second
            if (duration.count() < min_interval)
            {
                return ks_gc_info.ks_gc_sp;
            }
        }

        pingcap::kv::Backoffer bo(get_safepoint_maxtime);
        for (;;)
        {
            try
            {
                auto ks_gc_sp = pd_client->getGCSafePointV2(keyspace_id);
                updateKeyspaceGCSafepointMap(keyspace_id, ks_gc_sp);
                return ks_gc_sp;
            }
            catch (pingcap::Exception & e)
            {
                bo.backoff(pingcap::kv::boPDRPC, e);
            }
        }
    }

    static void updateKeyspaceGCSafepointMap(KeyspaceID keyspace_id, Timestamp ks_gc_sp)
    {
        std::unique_lock<std::shared_mutex> lock(ks_gc_sp_mutex);
        KeyspaceGCInfo new_keyspace_gc_info;
        new_keyspace_gc_info.ks_gc_sp = ks_gc_sp;
        new_keyspace_gc_info.ks_gc_sp_update_time = std::chrono::steady_clock::now();
        ks_gc_sp_map[keyspace_id] = new_keyspace_gc_info;
    }

    static KeyspaceGCInfo getKeyspaceGCSafepoint(KeyspaceID keyspace_id)
    {
        std::shared_lock<std::shared_mutex> lock(ks_gc_sp_mutex);
        return ks_gc_sp_map[keyspace_id];
    }

    static void removeKeyspaceGCSafepoint(KeyspaceID keyspace_id)
    {
        std::unique_lock<std::shared_mutex> lock(ks_gc_sp_mutex);
        ks_gc_sp_map.erase(keyspace_id);
    }


private:
    static std::atomic<Timestamp> cached_gc_safe_point;
    static std::atomic<std::chrono::time_point<std::chrono::steady_clock>> safe_point_last_update_time;

    // Keyspace gc safepoint cache and update time.
    static std::unordered_map<KeyspaceID, KeyspaceGCInfo> ks_gc_sp_map;
    static std::shared_mutex ks_gc_sp_mutex;
};


} // namespace DB
