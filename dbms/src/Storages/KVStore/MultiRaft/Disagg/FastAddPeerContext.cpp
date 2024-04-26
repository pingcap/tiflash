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

#include <Common/TiFlashMetrics.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointIngestInfo.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerContext.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/StorageDeltaMerge.h>

#include <mutex>


namespace DB
{
FastAddPeerContext::FastAddPeerContext(uint64_t thread_count)
    : log(Logger::get())
{
    if (thread_count == 0)
    {
        // Estimate this much time to handle a ffi request.
        static constexpr int ffi_handle_sec = 5;
        // Estimate this many region added in one second.
        static constexpr int region_per_sec = 5;
        thread_count = ffi_handle_sec * region_per_sec;
    }
    tasks_trace = std::make_shared<FAPAsyncTasks>(thread_count, thread_count, 1000);
}

void FastAddPeerContext::shutdown() const
{
    tasks_trace->shutdown();
}

ParsedCheckpointDataHolderPtr FastAddPeerContext::CheckpointCacheElement::getParsedCheckpointData(Context & context)
{
    std::scoped_lock<std::mutex> lock(mu);
    if (!parsed_checkpoint_data)
    {
        parsed_checkpoint_data = buildParsedCheckpointData(context, manifest_key, dir_seq);
    }
    return parsed_checkpoint_data;
}

std::pair<UInt64, ParsedCheckpointDataHolderPtr> FastAddPeerContext::getNewerCheckpointData(
    Context & context,
    UInt64 store_id,
    UInt64 required_seq)
{
    CheckpointCacheElementPtr cache_element = nullptr;
    UInt64 cache_seq = 0;
    {
        std::scoped_lock<std::mutex> lock{cache_mu};
        auto iter = checkpoint_cache_map.find(store_id);
        if (iter != checkpoint_cache_map.end() && (iter->second.first > required_seq))
        {
            cache_seq = iter->second.first;
            cache_element = iter->second.second;
        }
    }

    if (!cache_element)
    {
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        const auto manifests = S3::CheckpointManifestS3Set::getFromS3(*s3_client, store_id);
        if (manifests.empty())
        {
            LOG_DEBUG(log, "no manifest on this store, skip store_id={}", store_id);
            return std::make_pair(required_seq, nullptr);
        }
        const auto & latest_manifest_key = manifests.latestManifestKey();
        auto latest_manifest_key_view = S3::S3FilenameView::fromKey(latest_manifest_key);
        auto latest_upload_seq = latest_manifest_key_view.getUploadSequence();
        if (latest_upload_seq <= required_seq)
        {
            return std::make_pair(required_seq, nullptr);
        }
        // check whether there is some other thread downloading the current or newer manifest
        {
            std::scoped_lock lock{cache_mu};
            auto iter = checkpoint_cache_map.find(store_id);
            if (iter != checkpoint_cache_map.end() && (iter->second.first >= latest_upload_seq))
            {
                cache_seq = iter->second.first;
                cache_element = iter->second.second;
            }
            else
            {
                checkpoint_cache_map.erase(store_id);
                cache_seq = latest_upload_seq;
                cache_element = std::make_shared<CheckpointCacheElement>(latest_manifest_key, temp_ps_dir_sequence++);
                checkpoint_cache_map.emplace(store_id, std::make_pair(latest_upload_seq, cache_element));
            }
        }
    }

    // Now we try to parse the lastest manifest of this store.
    auto checkpoint_data = cache_element->getParsedCheckpointData(context);
    return std::make_pair(cache_seq, checkpoint_data);
}

CheckpointIngestInfoPtr FastAddPeerContext::getOrRestoreCheckpointIngestInfo(
    TMTContext & tmt,
    const struct TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 peer_id)
{
    {
        std::scoped_lock<std::mutex> lock(ingest_info_mu);
        if (auto iter = checkpoint_ingest_info_map.find(region_id); //
            iter != checkpoint_ingest_info_map.end())
        {
            return iter->second;
        }
    }
    {
        // The caller ensure there is no concurrency operation on the same region_id so
        // that we can call restore without locking `ingest_info_mu`
        auto info = CheckpointIngestInfo::restore(tmt, proxy_helper, region_id, peer_id);
        if (info)
        {
            GET_METRIC(tiflash_fap_task_result, type_restore).Increment();
            std::scoped_lock<std::mutex> lock(ingest_info_mu);
            checkpoint_ingest_info_map.emplace(region_id, info);
        }
        return info;
    }
}

void FastAddPeerContext::debugRemoveCheckpointIngestInfo(UInt64 region_id)
{
    std::scoped_lock<std::mutex> lock(ingest_info_mu);
    checkpoint_ingest_info_map.erase(region_id);
}

void FastAddPeerContext::cleanTask(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    CheckpointIngestInfo::CleanReason reason)
{
    bool in_memory = false;
    // TODO(fap) We use a dedicated queue and thread to clean, if this costs much.
    // However, we have to make sure the clean task will not override if a new fap snapshot of the same region comes later.
    {
        std::scoped_lock<std::mutex> lock(ingest_info_mu);
        auto iter = checkpoint_ingest_info_map.find(region_id);
        if (iter != checkpoint_ingest_info_map.end())
        {
            in_memory = true;
            checkpoint_ingest_info_map.erase(iter);
        }
    }
    if (reason == CheckpointIngestInfo::CleanReason::Success)
        CheckpointIngestInfo::cleanOnSuccess(tmt, region_id);
    else
    {
        RUNTIME_CHECK(proxy_helper != nullptr);
        CheckpointIngestInfo::forciblyClean(tmt, proxy_helper, region_id, in_memory, reason);
    }
}

std::optional<CheckpointIngestInfoPtr> FastAddPeerContext::tryGetCheckpointIngestInfo(UInt64 region_id) const
{
    std::scoped_lock<std::mutex> lock(ingest_info_mu);
    auto it = checkpoint_ingest_info_map.find(region_id);
    if (it == checkpoint_ingest_info_map.end())
        return std::nullopt;
    return it->second;
}

void FastAddPeerContext::insertCheckpointIngestInfo(
    TMTContext & tmt,
    UInt64 region_id,
    UInt64 peer_id,
    UInt64 remote_store_id,
    RegionPtr region,
    DM::Segments && segments,
    UInt64 start_time)
{
    std::shared_ptr<CheckpointIngestInfo> info;
    {
        std::scoped_lock<std::mutex> lock(ingest_info_mu);
        if (auto iter = checkpoint_ingest_info_map.find(region_id); unlikely(iter != checkpoint_ingest_info_map.end()))
        {
            // 1. Two fap task of a same snapshot take place in parallel, not possible.
            // 2. A previous fap task recovered from disk, while a new fap task is ongoing, not possible.
            // 3. A previous fap task finished with result attached to `checkpoint_ingest_info_map`, however, the ingest stage failed to be triggered/handled due to some check in proxy's part. It could be possible.
            LOG_ERROR(
                log,
                "Repeated ingest for region_id={} peer_id={} old_peer_id={}",
                region_id,
                peer_id,
                iter->second->peerId());
            GET_METRIC(tiflash_fap_task_result, type_failed_repeated).Increment();
        }

        info = std::make_shared<CheckpointIngestInfo>(
            tmt,
            region_id,
            peer_id,
            remote_store_id,
            region,
            std::move(segments),
            start_time);
        checkpoint_ingest_info_map[region_id] = info;
    }
    // persist without locking on `ingest_info_mu`
    info->persistToLocal();
}

void FastAddPeerContext::resolveFapSnapshotState(
    TMTContext & tmt,
    const TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    bool is_regular_snapshot)
{
    auto prev_state = tasks_trace->queryState(region_id);
    /// --- The regular snapshot case ---
    /// Legacy snapshot is not concurrent with FAP snapshot in both phase 1 and phase 2:
    /// Can't be InQueue/Running because:
    /// - Proxy will not actively cancel FAP, so it will not fallback if FAP phase 1 is still running.
    /// Cancel in `FastAddPeer` is blocking, so a regular snapshot won't meet a canceling snapshot.
    /// Can't be Finished because:
    /// - A finished task must be fetched by proxy on the next `FastAddPeer`.
    /// Unless
    /// - The task in worker throws without actually write anything, result in Finished state.
    /// -- The destroy region case ---
    /// When FAP goes on, it blocks all MsgAppend messages to this region peer, so the destroy won't happen.
    /// If the region is destroyed now and sent to this store later, it must be with another peer_id.
    RUNTIME_CHECK_MSG(
        prev_state != FAPAsyncTasks::TaskState::Running && prev_state != FAPAsyncTasks::TaskState::InQueue,
        "FastAddPeer: find scheduled fap task, region_id={} fap_state={} is_regular_snapshot={}",
        region_id,
        magic_enum::enum_name(prev_state),
        is_regular_snapshot);
    if (prev_state == FAPAsyncTasks::TaskState::Finished)
    {
        bool is_exception = false;
        try
        {
            tasks_trace->fetchResult(region_id);
        }
        catch (...)
        {
            is_exception = true;
        }
        LOG_INFO(
            log,
            "FastAddPeer: clean finished result region_id={} is_regular_snapshot={} is_exception={}",
            region_id,
            is_regular_snapshot,
            is_exception);
    }
    // 1. There leaves some non-ingested data on disk after restart.
    // 2. There has been no fap at all.
    // 3. FAP is enabled before, but disabled for now.
    LOG_DEBUG(
        log,
        "FastAddPeer: no find ongoing fap task, region_id={} is_regular_snapshot={}",
        region_id,
        is_regular_snapshot);
    // Still need to clean because there could be data left.
    cleanTask(
        tmt,
        proxy_helper,
        region_id,
        is_regular_snapshot ? CheckpointIngestInfo::CleanReason::ResolveStateApplySnapshot
                            : CheckpointIngestInfo::CleanReason::ResolveStateDestroy);
}

} // namespace DB
