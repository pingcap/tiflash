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
#include <Common/TiFlashMetrics.h>
#include <Core/Types.h>
#include <Storages/KVStore/Types.h>
#include <common/logger_useful.h>
#include <fiu.h>

#include <algorithm>
#include <atomic>
#include <magic_enum.hpp>
#include <shared_mutex>
#include <unordered_map>

using TimePoint = std::atomic<std::chrono::time_point<std::chrono::steady_clock>>;


namespace DB
{
namespace FailPoints
{
extern const char force_pd_grpc_error[];
} // namespace FailPoints

enum class GCSafepointFetchStrategy
{
    // Query paths consume the last value observed by a non-query caller.
    CacheOnly,
    // Non-query paths may refresh the value from PD when needed.
    UpdateCacheIfNeeded,
};

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
    // 10 seconds timeout for getting TSO
    // https://github.com/pingcap/tidb/blob/069631e2ecfedc000ffb92c67207bea81380f020/pkg/store/mockstore/unistore/pd/client.go#L256-L276
    static constexpr int get_tso_maxtime = 10'000;
    static constexpr int get_safepoint_maxtime = 120'000;

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
        Int64 safe_point_update_interval_seconds = 30,
        Int64 safe_point_get_max_backoff_ms = 120000,
        GCSafepointFetchStrategy fetch_strategy = GCSafepointFetchStrategy::UpdateCacheIfNeeded)
    {
        UInt64 backoff_count = 0;
        auto observe_backoff_count = [&](bool success) {
            if (success)
                GET_METRIC(tiflash_gc_safepoint_backoff_count, type_success).Observe(backoff_count);
            else
                GET_METRIC(tiflash_gc_safepoint_backoff_count, type_failure).Observe(backoff_count);
        };

        if (fetch_strategy == GCSafepointFetchStrategy::CacheOnly)
        {
            if (enable_safepoint_v2 && keyspace_id != NullspaceID)
            {
                auto cached = getKeyspaceGCSafepoint(keyspace_id);
                observe_backoff_count(true);
                return cached.ks_gc_sp;
            }
            observe_backoff_count(true);
            return cached_gc_safe_point;
        }

        // If keyspace id is `NullspaceID` it needs to use safe point v1.
        if (enable_safepoint_v2 && keyspace_id != NullspaceID)
        {
            auto gc_safe_point
                = getGCSafePointV2WithRetry(
                    pd_client,
                    keyspace_id,
                    false,
                    safe_point_update_interval_seconds,
                    safe_point_get_max_backoff_ms);
            LOG_TRACE(Logger::get(), "use safe point v2, keyspace={} gc_safe_point={}", keyspace_id, gc_safe_point);
            return gc_safe_point;
        }

        if (safe_point_update_interval_seconds > 0)
        {
            // In case we cost too much to update safe point from PD.
            auto now = std::chrono::steady_clock::now();
            const auto duration
                = std::chrono::duration_cast<std::chrono::seconds>(now - safe_point_last_update_time.load());
            const auto min_interval
                = std::max(static_cast<Int64>(1), safe_point_update_interval_seconds); // at least one second
            if (duration.count() < min_interval)
            {
                observe_backoff_count(true);
                return cached_gc_safe_point;
            }
        }

        pingcap::kv::Backoffer bo(std::max(static_cast<Int64>(0), safe_point_get_max_backoff_ms));
        for (;;)
        {
            bool has_pd_response_error = false;
            try
            {
                GET_METRIC(tiflash_gc_safepoint_request_count, type_get_gc_state).Increment();
                auto safe_point = pd_client->getGCSafePoint();
                const auto cached_safe_point = cached_gc_safe_point.load(std::memory_order_acquire);
                if (safe_point < cached_safe_point)
                    GET_METRIC(tiflash_gc_safepoint_request_count, type_rewind).Increment();
                const auto merged_safe_point = std::max(cached_safe_point, safe_point);
                cached_gc_safe_point.store(merged_safe_point, std::memory_order_release);
                if (merged_safe_point == 0)
                    GET_METRIC(tiflash_gc_safepoint_request_count, type_zero_gc_safe_point).Increment();
                LOG_TRACE(Logger::get(), "use safe point v1, gc_safe_point={}", merged_safe_point);
                safe_point_last_update_time = std::chrono::steady_clock::now();
                observe_backoff_count(true);
                return merged_safe_point;
            }
            catch (pingcap::Exception & e)
            {
                if (!has_pd_response_error)
                    GET_METRIC(tiflash_gc_safepoint_request_count, type_request_exception).Increment();
                try
                {
                    ++backoff_count;
                    bo.backoff(pingcap::kv::boPDRPC, e);
                }
                catch (pingcap::Exception &)
                {
                    GET_METRIC(tiflash_gc_safepoint_request_count, type_backoff_error).Increment();
                    observe_backoff_count(false);
                    throw;
                }
            }
        }
    }

    static Timestamp getGCSafePointV2WithRetry(
        const pingcap::pd::ClientPtr & pd_client,
        KeyspaceID keyspace_id,
        bool ignore_cache = false,
        Int64 safe_point_update_interval_seconds = 30,
        Int64 safe_point_get_max_backoff_ms = 120000)
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

        pingcap::kv::Backoffer bo(std::max(static_cast<Int64>(0), safe_point_get_max_backoff_ms));
        for (;;)
        {
            try
            {
                auto ks_gc_sp = pd_client->getGCSafePointV2(keyspace_id);
                updateKeyspaceGCSafepointMap(keyspace_id, ks_gc_sp);
                return getKeyspaceGCSafepoint(keyspace_id).ks_gc_sp;
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
        const auto iter = ks_gc_sp_map.find(keyspace_id);
        if (iter != ks_gc_sp_map.end() && ks_gc_sp < iter->second.ks_gc_sp)
            GET_METRIC(tiflash_gc_safepoint_request_count, type_rewind).Increment();
        new_keyspace_gc_info.ks_gc_sp = iter == ks_gc_sp_map.end()
            ? ks_gc_sp
            : std::max(iter->second.ks_gc_sp, ks_gc_sp);
        new_keyspace_gc_info.ks_gc_sp_update_time = std::chrono::steady_clock::now();
        ks_gc_sp_map[keyspace_id] = new_keyspace_gc_info;
        if (new_keyspace_gc_info.ks_gc_sp == 0)
            GET_METRIC(tiflash_gc_safepoint_request_count, type_zero_gc_safe_point).Increment();
    }

    static KeyspaceGCInfo getKeyspaceGCSafepoint(KeyspaceID keyspace_id)
    {
        std::shared_lock<std::shared_mutex> lock(ks_gc_sp_mutex);
        const auto iter = ks_gc_sp_map.find(keyspace_id);
        return iter == ks_gc_sp_map.end() ? KeyspaceGCInfo{} : iter->second;
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
