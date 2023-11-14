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

#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointIngestInfo.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeer.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/StorageDeltaMerge.h>


namespace DB
{
FastAddPeerContext::FastAddPeerContext(uint64_t thread_count)
    : log(Logger::get())
{
    if (thread_count == 0)
    {
        static constexpr int ffi_handle_sec = 5;
        static constexpr int region_per_sec = 2;
        thread_count = ffi_handle_sec * region_per_sec;
    }
    tasks_trace = std::make_shared<FAPAsyncTasks>(thread_count);
}

ParsedCheckpointDataHolderPtr FastAddPeerContext::CheckpointCacheElement::getParsedCheckpointData(Context & context)
{
    std::unique_lock lock(mu);
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
        std::unique_lock lock{cache_mu};
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
            std::unique_lock lock{cache_mu};
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

CheckpointIngestInfoPtr FastAddPeerContext::getOrCreateCheckpointIngestInfo(
    TMTContext & tmt,
    const struct TiFlashRaftProxyHelper * proxy_helper,
    UInt64 region_id,
    UInt64 peer_id)
{
    std::unique_lock<std::mutex> lock;
    if (!checkpoint_ingest_info_map.contains(region_id))
        checkpoint_ingest_info_map[region_id]
            = std::make_shared<CheckpointIngestInfo>(tmt, proxy_helper, region_id, peer_id);
    return checkpoint_ingest_info_map.at(region_id);
}

} // namespace DB