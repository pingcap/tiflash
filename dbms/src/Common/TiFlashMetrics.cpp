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

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/TiFlashMetrics.h>
#include <common/defines.h>

namespace DB
{
TiFlashMetrics & TiFlashMetrics::instance()
{
    static TiFlashMetrics inst; // Instantiated on first use.
    return inst;
}

TiFlashMetrics::TiFlashMetrics()
{
    registered_profile_events.reserve(ProfileEvents::end());
    for (ProfileEvents::Event event = 0; event < ProfileEvents::end(); event++)
    {
        std::string name{ProfileEvents::getDescription(event)};
        auto & family = prometheus::BuildGauge()
                            .Name(profile_events_prefix + name)
                            .Help("System profile event " + name)
                            .Register(*registry);
        registered_profile_events.push_back(&family.Add({}));
    }

    registered_current_metrics.reserve(CurrentMetrics::end());
    for (CurrentMetrics::Metric metric = 0; metric < CurrentMetrics::end(); metric++)
    {
        std::string name{CurrentMetrics::getDescription(metric)};
        auto & family = prometheus::BuildGauge()
                            .Name(current_metrics_prefix + name)
                            .Help("System current metric " + name)
                            .Register(*registry);
        registered_current_metrics.push_back(&family.Add({}));
    }

    auto prometheus_name = TiFlashMetrics::current_metrics_prefix + std::string("StoreSizeUsed");
    registered_keypace_store_used_family
        = &prometheus::BuildGauge().Name(prometheus_name).Help("Store size used of keyspace").Register(*registry);
    store_used_total_metric = &registered_keypace_store_used_family->Add({{"keyspace_id", ""}, {"type", "all_used"}});

    registered_keyspace_sync_replica_ru_family = &prometheus::BuildCounter()
                                                      .Name("tiflash_storage_sync_replica_ru")
                                                      .Help("RU for synchronous replica of keyspace")
                                                      .Register(*registry);
    registered_raft_proxy_thread_memory_usage_family
        = &prometheus::BuildGauge().Name(raft_proxy_thread_memory_usage).Help("").Register(*registry);
}

void TiFlashMetrics::addReplicaSyncRU(UInt32 keyspace_id, UInt64 ru)
{
    std::unique_lock lock(replica_sync_ru_mtx);
    auto * counter = getReplicaSyncRUCounter(keyspace_id, lock);
    counter->Increment(ru);
}

UInt64 TiFlashMetrics::debugQueryReplicaSyncRU(UInt32 keyspace_id)
{
    std::unique_lock lock(replica_sync_ru_mtx);
    auto * counter = getReplicaSyncRUCounter(keyspace_id, lock);
    return counter->Value();
}

prometheus::Counter * TiFlashMetrics::getReplicaSyncRUCounter(UInt32 keyspace_id, std::unique_lock<std::mutex> &)
{
    auto itr = registered_keyspace_sync_replica_ru.find(keyspace_id);
    if (likely(itr != registered_keyspace_sync_replica_ru.end()))
    {
        return itr->second;
    }
    return registered_keyspace_sync_replica_ru[keyspace_id]
        = &registered_keyspace_sync_replica_ru_family->Add({{"keyspace_id", std::to_string(keyspace_id)}});
}

void TiFlashMetrics::removeReplicaSyncRUCounter(UInt32 keyspace_id)
{
    std::unique_lock lock(replica_sync_ru_mtx);
    auto itr = registered_keyspace_sync_replica_ru.find(keyspace_id);
    if (itr == registered_keyspace_sync_replica_ru.end())
    {
        return;
    }
    registered_keyspace_sync_replica_ru_family->Remove(itr->second);
    registered_keyspace_sync_replica_ru.erase(itr);
}

double TiFlashMetrics::getProxyThreadMemory(const std::string & k)
{
    std::shared_lock lock(proxy_thread_report_mtx);
    auto it = registered_raft_proxy_thread_memory_usage_metrics.find(k);
    RUNTIME_CHECK(it != registered_raft_proxy_thread_memory_usage_metrics.end(), k);
    return it->second->Value();
}

void TiFlashMetrics::setProxyThreadMemory(const std::string & k, Int64 v)
{
    std::shared_lock lock(proxy_thread_report_mtx);
    if unlikely (!registered_raft_proxy_thread_memory_usage_metrics.count(k))
    {
        // New metrics added through `Reset`.
        return;
    }
    registered_raft_proxy_thread_memory_usage_metrics[k]->Set(v);
}

void TiFlashMetrics::registerProxyThreadMemory(const std::string & k)
{
    std::unique_lock lock(proxy_thread_report_mtx);
    if unlikely (!registered_raft_proxy_thread_memory_usage_metrics.count(k))
    {
        registered_raft_proxy_thread_memory_usage_metrics.emplace(
            "alloc_" + k,
            &registered_raft_proxy_thread_memory_usage_family->Add({{"type", k}}));
        registered_raft_proxy_thread_memory_usage_metrics.emplace(
            "dealloc_" + k,
            &registered_raft_proxy_thread_memory_usage_family->Add({{"type", k}}));
    }
}

} // namespace DB