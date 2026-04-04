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
#include <Common/ProcessCollector.h>
#include <Common/ProfileEvents.h>
#include <Common/TiFlashMetrics.h>
#include <common/defines.h>

#include <magic_enum.hpp>

namespace DB
{
namespace
{
constexpr std::array remote_cache_file_type_labels = {"merged", "coldata", "other"};
constexpr std::array remote_cache_wait_result_labels = {"hit", "timeout", "failed"};
constexpr std::array remote_cache_reject_reason_labels = {"too_many_download"};
constexpr std::array remote_cache_download_stage_labels = {"queue_wait", "download"};
constexpr auto remote_cache_wait_on_downloading_buckets = ExpBuckets{0.0001, 2, 20};
constexpr auto remote_cache_bg_download_stage_buckets = ExpBuckets{0.0001, 2, 20};
} // namespace

TiFlashMetrics & TiFlashMetrics::instance()
{
    static TiFlashMetrics inst; // Instantiated on first use.
    return inst;
}

TiFlashMetrics::TiFlashMetrics()
{
    process_collector = std::make_shared<ProcessCollector>();

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

    registered_storage_thread_memory_usage_family
        = &prometheus::BuildGauge().Name(storages_thread_memory_usage).Help("").Register(*registry);

    registered_storage_ru_read_bytes_family = &prometheus::BuildCounter()
                                                   .Name("tiflash_storage_ru_read_bytes")
                                                   .Help("Read bytes for storage RU calculation")
                                                   .Register(*registry);

    registered_s3_store_summary_bytes_family = &prometheus::BuildGauge()
                                                    .Name("tiflash_storage_s3_store_summary_bytes")
                                                    .Help("S3 storage summary bytes by store and file type")
                                                    .Register(*registry);

    registered_remote_cache_wait_on_downloading_result_family
        = &prometheus::BuildCounter()
               .Name("tiflash_storage_remote_cache_wait_on_downloading_result")
               .Help("Bounded wait result of remote cache downloading")
               .Register(*registry);
    registered_remote_cache_wait_on_downloading_bytes_family
        = &prometheus::BuildCounter()
               .Name("tiflash_storage_remote_cache_wait_on_downloading_bytes")
               .Help("Bytes covered by remote cache bounded wait")
               .Register(*registry);
    // Timeline for one cache miss with possible follower requests:
    //
    //   req A: miss -> create Empty -> enqueue bg task ---- queue_wait ---- download ---- Complete/Failed
    //   req B:                  sees Empty -> -------- wait_on_downloading_seconds --------> hit/timeout/failed
    //   req C:                         sees Empty -> --- wait_on_downloading_seconds ---> hit/timeout/failed
    //
    // `tiflash_storage_remote_cache_bg_download_stage_seconds`
    //   - downloader-task view
    //   - measures how long the background download itself spent in `queue_wait` and `download`
    registered_remote_cache_bg_download_stage_seconds_family
        = &prometheus::BuildHistogram()
               .Name("tiflash_storage_remote_cache_bg_download_stage_seconds")
               .Help("Remote cache background download stage duration")
               .Register(*registry);
    // `tiflash_storage_remote_cache_wait_on_downloading_seconds`
    //   - follower-request view
    //   - measures how long a request waited on an existing `Empty` segment before ending as hit/timeout/failed
    registered_remote_cache_wait_on_downloading_seconds_family
        = &prometheus::BuildHistogram()
               .Name("tiflash_storage_remote_cache_wait_on_downloading_seconds")
               .Help("Bounded wait duration of remote cache downloading")
               .Register(*registry);
    registered_remote_cache_reject_family = &prometheus::BuildCounter()
                                                 .Name("tiflash_storage_remote_cache_reject")
                                                 .Help("Remote cache admission rejection by reason and file type")
                                                 .Register(*registry);

    for (size_t file_type_idx = 0; file_type_idx < remote_cache_file_type_labels.size(); ++file_type_idx)
    {
        for (size_t result_idx = 0; result_idx < remote_cache_wait_result_labels.size(); ++result_idx)
        {
            auto labels = prometheus::Labels{
                {"result", std::string(remote_cache_wait_result_labels[result_idx])},
                {"file_type", std::string(remote_cache_file_type_labels[file_type_idx])},
            };
            remote_cache_wait_on_downloading_result_metrics[file_type_idx][result_idx]
                = &registered_remote_cache_wait_on_downloading_result_family->Add(labels);
            remote_cache_wait_on_downloading_bytes_metrics[file_type_idx][result_idx]
                = &registered_remote_cache_wait_on_downloading_bytes_family->Add(labels);
            prometheus::Histogram::BucketBoundaries wait_buckets = ExpBuckets{
                remote_cache_wait_on_downloading_buckets.start,
                remote_cache_wait_on_downloading_buckets.base,
                remote_cache_wait_on_downloading_buckets.size};
            remote_cache_wait_on_downloading_seconds_metrics[file_type_idx][result_idx]
                = &registered_remote_cache_wait_on_downloading_seconds_family->Add(labels, wait_buckets);
        }
        for (size_t reason_idx = 0; reason_idx < remote_cache_reject_reason_labels.size(); ++reason_idx)
        {
            remote_cache_reject_metrics[file_type_idx][reason_idx] = &registered_remote_cache_reject_family->Add(
                {{"reason", std::string(remote_cache_reject_reason_labels[reason_idx])},
                 {"file_type", std::string(remote_cache_file_type_labels[file_type_idx])}});
        }
        for (size_t stage_idx = 0; stage_idx < remote_cache_download_stage_labels.size(); ++stage_idx)
        {
            prometheus::Histogram::BucketBoundaries buckets = ExpBuckets{
                remote_cache_bg_download_stage_buckets.start,
                remote_cache_bg_download_stage_buckets.base,
                remote_cache_bg_download_stage_buckets.size};
            remote_cache_bg_download_stage_seconds_metrics[file_type_idx][stage_idx]
                = &registered_remote_cache_bg_download_stage_seconds_family->Add(
                    {{"stage", std::string(remote_cache_download_stage_labels[stage_idx])},
                     {"file_type", std::string(remote_cache_file_type_labels[file_type_idx])}},
                    buckets);
        }
    }
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

static std::string genPrefix(TiFlashMetrics::MemoryAllocType type, const std::string & k)
{
    if (type == TiFlashMetrics::MemoryAllocType::Alloc)
    {
        return "alloc_" + k;
    }
    else
    {
        return "dealloc_" + k;
    }
}

double TiFlashMetrics::getProxyThreadMemory(TiFlashMetrics::MemoryAllocType type, const std::string & k)
{
    std::shared_lock lock(proxy_thread_report_mtx);

    auto it = registered_raft_proxy_thread_memory_usage_metrics.find(genPrefix(type, k));
    RUNTIME_CHECK(it != registered_raft_proxy_thread_memory_usage_metrics.end(), k);
    return it->second->Value();
}

void TiFlashMetrics::setProxyThreadMemory(TiFlashMetrics::MemoryAllocType type, const std::string & k, Int64 v)
{
    std::shared_lock lock(proxy_thread_report_mtx);
    auto it = registered_raft_proxy_thread_memory_usage_metrics.find(genPrefix(type, k));
    if unlikely (it == registered_raft_proxy_thread_memory_usage_metrics.end())
    {
        // New metrics added through `Reset`.
        return;
    }
    it->second->Set(v);
}

double TiFlashMetrics::getStorageThreadMemory(TiFlashMetrics::MemoryAllocType type, const std::string & k)
{
    std::shared_lock lock(proxy_thread_report_mtx);

    auto it = registered_storage_thread_memory_usage_metrics.find(genPrefix(type, k));
    RUNTIME_CHECK(it != registered_storage_thread_memory_usage_metrics.end(), k);
    return it->second->Value();
}

void TiFlashMetrics::setStorageThreadMemory(TiFlashMetrics::MemoryAllocType type, const std::string & k, Int64 v)
{
    std::shared_lock lock(proxy_thread_report_mtx);
    auto it = registered_storage_thread_memory_usage_metrics.find(genPrefix(type, k));
    if unlikely (it == registered_storage_thread_memory_usage_metrics.end())
    {
        // New metrics added through `Reset`.
        return;
    }
    it->second->Set(v);
}

void TiFlashMetrics::registerProxyThreadMemory(const std::string & k)
{
    std::unique_lock lock(proxy_thread_report_mtx);
    {
        auto prefix = genPrefix(TiFlashMetrics::MemoryAllocType::Alloc, k);
        if unlikely (!registered_raft_proxy_thread_memory_usage_metrics.contains(prefix))
        {
            registered_raft_proxy_thread_memory_usage_metrics.emplace(
                prefix,
                &registered_raft_proxy_thread_memory_usage_family->Add({{"type", prefix}}));
        }
    }
    {
        auto prefix = genPrefix(TiFlashMetrics::MemoryAllocType::Dealloc, k);
        if unlikely (!registered_raft_proxy_thread_memory_usage_metrics.contains(prefix))
        {
            registered_raft_proxy_thread_memory_usage_metrics.emplace(
                prefix,
                &registered_raft_proxy_thread_memory_usage_family->Add({{"type", prefix}}));
        }
    }
}

void TiFlashMetrics::registerStorageThreadMemory(const std::string & k)
{
    std::unique_lock lock(storage_thread_report_mtx);
    {
        auto prefix = genPrefix(TiFlashMetrics::MemoryAllocType::Alloc, k);
        if unlikely (!registered_storage_thread_memory_usage_metrics.contains(prefix))
        {
            registered_storage_thread_memory_usage_metrics.emplace(
                prefix,
                &registered_storage_thread_memory_usage_family->Add({{"type", prefix}}));
        }
    }
    {
        auto prefix = genPrefix(TiFlashMetrics::MemoryAllocType::Dealloc, k);
        if unlikely (!registered_storage_thread_memory_usage_metrics.contains(prefix))
        {
            registered_storage_thread_memory_usage_metrics.emplace(
                prefix,
                &registered_storage_thread_memory_usage_family->Add({{"type", prefix}}));
        }
    }
}

void TiFlashMetrics::setProvideProxyProcessMetrics(bool v)
{
    process_collector->include_proxy_metrics = v;
}

prometheus::Counter & TiFlashMetrics::getStorageRUReadBytesCounter(
    KeyspaceID keyspace,
    const String & resource_group,
    DM::ReadRUType type)
{
    auto key = fmt::format("{}_{}_{}", keyspace, resource_group, magic_enum::enum_name(type));

    // Fast path
    {
        std::shared_lock lock(storage_ru_read_bytes_mtx);
        auto it = registered_storage_ru_read_bytes_metrics.find(key);
        if (it != registered_storage_ru_read_bytes_metrics.end())
            return *(it->second);
    }

    // Create counter for new keyspace/resource_group/type.
    {
        std::unique_lock lock(storage_ru_read_bytes_mtx);
        // double-check: other threads may create the same counter
        auto it = registered_storage_ru_read_bytes_metrics.find(key);
        if (it != registered_storage_ru_read_bytes_metrics.end())
            return *(it->second);

        prometheus::Labels labels
            = {{"keyspace", std::to_string(keyspace)},
               {"resource_group", resource_group},
               {"type", std::string(magic_enum::enum_name(type))}};
        auto & counter = registered_storage_ru_read_bytes_family->Add(labels);
        registered_storage_ru_read_bytes_metrics[key] = &counter;
        return counter;
    }
}

void TiFlashMetrics::setS3StoreSummaryBytes(UInt64 store_id, UInt64 data_file_bytes, UInt64 dt_file_bytes)
{
    // Fast path.
    {
        std::shared_lock lock(s3_store_summary_bytes_mtx);
        auto it = registered_s3_store_summary_bytes_metrics.find(store_id);
        if (it != registered_s3_store_summary_bytes_metrics.end())
        {
            it->second.data_file_bytes->Set(data_file_bytes);
            it->second.dt_file_bytes->Set(dt_file_bytes);
            return;
        }
    }

    std::unique_lock lock(s3_store_summary_bytes_mtx);
    auto [it, inserted] = registered_s3_store_summary_bytes_metrics.try_emplace(store_id);
    if (inserted)
    {
        auto store_id_str = std::to_string(store_id);
        auto & data_file_bytes_metric
            = registered_s3_store_summary_bytes_family->Add({{"store_id", store_id_str}, {"type", "data_file_bytes"}});
        auto & dt_file_bytes_metric
            = registered_s3_store_summary_bytes_family->Add({{"store_id", store_id_str}, {"type", "dt_file_bytes"}});
        it->second = S3StoreSummaryBytesMetrics{
            .data_file_bytes = &data_file_bytes_metric,
            .dt_file_bytes = &dt_file_bytes_metric,
        };
    }

    it->second.data_file_bytes->Set(data_file_bytes);
    it->second.dt_file_bytes->Set(dt_file_bytes);
}

prometheus::Counter & TiFlashMetrics::getRemoteCacheWaitOnDownloadingResultCounter(
    TiFlashMetrics::RemoteCacheFileTypeMetric file_type,
    TiFlashMetrics::RemoteCacheWaitResultMetric result)
{
    return *remote_cache_wait_on_downloading_result_metrics[static_cast<size_t>(file_type)]
                                                           [static_cast<size_t>(result)];
}

prometheus::Counter & TiFlashMetrics::getRemoteCacheWaitOnDownloadingBytesCounter(
    TiFlashMetrics::RemoteCacheFileTypeMetric file_type,
    TiFlashMetrics::RemoteCacheWaitResultMetric result)
{
    return *remote_cache_wait_on_downloading_bytes_metrics[static_cast<size_t>(file_type)][static_cast<size_t>(result)];
}

prometheus::Histogram & TiFlashMetrics::getRemoteCacheWaitOnDownloadingSecondsHistogram(
    TiFlashMetrics::RemoteCacheFileTypeMetric file_type,
    TiFlashMetrics::RemoteCacheWaitResultMetric result)
{
    return *remote_cache_wait_on_downloading_seconds_metrics[static_cast<size_t>(file_type)]
                                                            [static_cast<size_t>(result)];
}

prometheus::Histogram & TiFlashMetrics::getRemoteCacheBgDownloadStageSecondsHistogram(
    TiFlashMetrics::RemoteCacheFileTypeMetric file_type,
    TiFlashMetrics::RemoteCacheDownloadStageMetric stage)
{
    return *remote_cache_bg_download_stage_seconds_metrics[static_cast<size_t>(file_type)][static_cast<size_t>(stage)];
}

prometheus::Counter & TiFlashMetrics::getRemoteCacheRejectCounter(
    TiFlashMetrics::RemoteCacheFileTypeMetric file_type,
    TiFlashMetrics::RemoteCacheRejectReasonMetric reason)
{
    return *remote_cache_reject_metrics[static_cast<size_t>(file_type)][static_cast<size_t>(reason)];
}
} // namespace DB
