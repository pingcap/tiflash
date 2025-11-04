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

// The GC safepoint and its update time for a keyspace.
struct KeyspaceGCInfo
{
    DB::Timestamp gc_safepoint{0};
    TimePoint update_time;

    KeyspaceGCInfo() { update_time = std::chrono::steady_clock::now(); }
    explicit KeyspaceGCInfo(Timestamp gc_safepoint_)
        : gc_safepoint(gc_safepoint_)
    {
        update_time = std::chrono::steady_clock::now();
    }

    KeyspaceGCInfo(const KeyspaceGCInfo & other)
    {
        gc_safepoint = other.gc_safepoint;
        update_time = std::chrono::steady_clock::now();
    }

    KeyspaceGCInfo & operator=(const KeyspaceGCInfo & other)
    {
        if (this != &other)
        {
            gc_safepoint = other.gc_safepoint;
            update_time = std::chrono::steady_clock::now();
        }
        return *this;
    }
};

class KeyspacesGcInfo
{
public:
    KeyspacesGcInfo() = default;

    // Update GCSafepoint for a keyspace.
    void updateGCSafepoint(KeyspaceID keyspace_id, Timestamp gc_safepoint)
    {
        // guard for invalid gc safe point
        if (gc_safepoint == 0)
            return;

        std::unique_lock lock(mtx);
        gc_safepoint_map[keyspace_id] = KeyspaceGCInfo(gc_safepoint);
    }

    // Get GCSafepoint for a keyspace.
    KeyspaceGCInfo getGCSafepoint(KeyspaceID keyspace_id)
    {
        std::shared_lock lock(mtx);
        return gc_safepoint_map[keyspace_id];
    }

    // Remove the GCSafepoint info for a keyspace.
    void removeGCSafepoint(KeyspaceID keyspace_id)
    {
        std::unique_lock lock(mtx);
        gc_safepoint_map.erase(keyspace_id);
    }

private:
    std::shared_mutex mtx;
    // keyspace_id -> KeyspaceGCInfo
    std::unordered_map<KeyspaceID, KeyspaceGCInfo> gc_safepoint_map;
};

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
        KeyspaceID keyspace_id,
        bool ignore_cache = true,
        Int64 safe_point_update_interval_seconds = 30)
    {
        if (!ignore_cache)
        {
            // In order to avoid too frequent requests to PD,
            // we cache the safe point for a while.
            auto now = std::chrono::steady_clock::now();

            auto ks_gc_info = ks_gc_sp_map.getGCSafepoint(keyspace_id);
            const auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - ks_gc_info.update_time.load());
            const auto min_interval
                = std::max(static_cast<Int64>(1), safe_point_update_interval_seconds); // at least one second
            if (duration.count() < min_interval)
                return ks_gc_info.gc_safepoint;
        }

        pingcap::kv::Backoffer bo(get_safepoint_maxtime);
        for (;;)
        {
            try
            {
                // Fetch the gc safepoint from PD.
                // - When deployed with classic cluster, the gc safepoint is cluster-based, keyspace_id=NullspaceID.
                // - When deployed with next-gen cluster, the gc safepoint is keyspace-based.
                auto gc_state = pd_client->getGCState(keyspace_id);
                auto safe_point = gc_state.gc_state().gc_safe_point();
                if (safe_point != 0)
                {
                    // add to cache
                    ks_gc_sp_map.updateGCSafepoint(keyspace_id, safe_point);
                }
#ifndef NDEBUG
                else
                {
                    LOG_WARNING(
                        Logger::get(),
                        "getGCSafePointWithRetry keyspace_id={} gc_safe_point=0 gc_state={}",
                        keyspace_id,
                        gc_state.ShortDebugString());
                }
#endif
                return safe_point;
            }
            catch (pingcap::Exception & e)
            {
                bo.backoff(pingcap::kv::boPDRPC, e);
            }
        }
    }

    static void removeKeyspaceGCSafepoint(KeyspaceID keyspace_id) { ks_gc_sp_map.removeGCSafepoint(keyspace_id); }

private:
    // Keyspace gc safepoint cache and update time.
    static KeyspacesGcInfo ks_gc_sp_map;
};


} // namespace DB
