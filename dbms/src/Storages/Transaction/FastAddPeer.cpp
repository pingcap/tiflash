// Copyright 2022 PingCAP, Ltd.
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

#include <Common/FailPoint.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/Transaction/CheckpointInfo.h>
#include <Storages/Transaction/FastAddPeerAsyncTasksImpl.h>
#include <Storages/Transaction/FastAddPeerCache.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>

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
    tasks_trace = std::make_shared<AsyncTasks>(thread_count);
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

std::pair<UInt64, ParsedCheckpointDataHolderPtr> FastAddPeerContext::getNewerCheckpointData(Context & context, UInt64 store_id, UInt64 required_seq)
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

    auto checkpoint_data = cache_element->getParsedCheckpointData(context);
    return std::make_pair(cache_seq, checkpoint_data);
}

FastAddPeerRes genFastAddPeerRes(FastAddPeerStatus status, std::string && apply_str, std::string && region_str)
{
    auto * apply = RawCppString::New(apply_str);
    auto * region = RawCppString::New(region_str);
    return FastAddPeerRes{
        .status = status,
        .apply_state = CppStrWithView{.inner = GenRawCppPtr(apply, RawCppPtrTypeImpl::String), .view = BaseBuffView{apply->data(), apply->size()}},
        .region = CppStrWithView{.inner = GenRawCppPtr(region, RawCppPtrTypeImpl::String), .view = BaseBuffView{region->data(), region->size()}},
    };
}

std::vector<StoreID> getCandidateStoreIDsForRegion(TMTContext & tmt_context, UInt64 region_id, UInt64 current_store_id)
{
    auto pd_client = tmt_context.getPDClient();
    auto [region, _] = pd_client->getRegionByID(region_id);
    std::vector<StoreID> store_ids;
    store_ids.reserve(region.peers_size());
    for (const auto & peer : region.peers())
    {
        if (peer.store_id() == current_store_id)
            continue;
        // TODO: use label on the store to determine whether it's tiflash WN
        if (peer.role() == metapb::PeerRole::Learner)
        {
            store_ids.push_back(peer.store_id());
        }
    }
    return store_ids;
}

using raft_serverpb::PeerState;
using raft_serverpb::RaftApplyState;
using raft_serverpb::RegionLocalState;

std::optional<std::tuple<CheckpointInfoPtr, RegionPtr, RaftApplyState, RegionLocalState>> tryParseRegionInfoFromCheckpointData(ParsedCheckpointDataHolderPtr checkpoint_data_holder, UInt64 remote_store_id, UInt64 region_id, TiFlashRaftProxyHelper * proxy_helper)
{
    auto * log = &Poco::Logger::get("FastAddPeer");
    RegionPtr region;
    {
        auto region_key = UniversalPageIdFormat::toKVStoreKey(region_id);
        auto page = checkpoint_data_holder->getUniversalPageStorage()->read(region_key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            ReadBufferFromMemory buf(page.data.begin(), page.data.size());
            region = Region::deserialize(buf, proxy_helper);
        }
        else
        {
            LOG_DEBUG(log, "Failed to find region key [region_id={}]", region_id);
            return std::nullopt;
        }
    }

    RaftApplyState apply_state;
    {
        auto apply_state_key = UniversalPageIdFormat::toRaftApplyStateKeyInKVEngine(region_id);
        auto page = checkpoint_data_holder->getUniversalPageStorage()->read(apply_state_key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            apply_state.ParseFromArray(page.data.begin(), page.data.size());
        }
        else
        {
            LOG_DEBUG(log, "Failed to find apply state key [region_id={}]", region_id);
            return std::nullopt;
        }
    }

    RegionLocalState region_state;
    {
        auto local_state_key = UniversalPageIdFormat::toRegionLocalStateKeyInKVEngine(region_id);
        auto page = checkpoint_data_holder->getUniversalPageStorage()->read(local_state_key, /*read_limiter*/ nullptr, {}, /*throw_on_not_exist*/ false);
        if (page.isValid())
        {
            region_state.ParseFromArray(page.data.begin(), page.data.size());
        }
        else
        {
            LOG_DEBUG(log, "Failed to find region local state key [region_id={}]", region_id);
            return std::nullopt;
        }
    }

    auto checkpoint_info = std::make_shared<CheckpointInfo>();
    checkpoint_info->remote_store_id = remote_store_id;
    checkpoint_info->region_id = region_id;
    checkpoint_info->checkpoint_data_holder = checkpoint_data_holder;
    checkpoint_info->temp_ps = checkpoint_data_holder->getUniversalPageStorage();
    return std::make_tuple(checkpoint_info, region, apply_state, region_state);
}

bool tryResetPeerIdInRegion(RegionPtr region, const RegionLocalState & region_state, uint64_t new_peer_id)
{
    auto peer_state = region_state.state();
    if (peer_state == PeerState::Tombstone || peer_state == PeerState::Applying)
    {
        return false;
    }
    for (const auto & peer : region_state.region().peers())
    {
        if (peer.id() == new_peer_id)
        {
            auto peer_copy = peer;
            region->mutMeta().setPeer(std::move(peer_copy));
            return true;
        }
    }
    return false;
}

FastAddPeerRes FastAddPeerImpl(EngineStoreServerWrap * server, uint64_t region_id, uint64_t new_peer_id)
{
    try
    {
        auto * log = &Poco::Logger::get("FastAddPeer");
        Stopwatch watch;
        CheckpointInfoPtr checkpoint_info;
        RegionPtr region;
        RaftApplyState apply_state;
        RegionLocalState region_state;
        std::unordered_map<StoreID, UInt64> checked_seq_map;
        auto fap_ctx = server->tmt->getContext().getSharedContextDisagg()->fap_context;
        const auto & settings = server->tmt->getContext().getSettingsRef();
        auto current_store_id = server->tmt->getKVStore()->getStoreMeta().id();
        auto candidate_store_ids = getCandidateStoreIDsForRegion(*(server->tmt), region_id, current_store_id);
        if (candidate_store_ids.empty())
        {
            LOG_DEBUG(log, "No suitable candidate peer for region {}", region_id);
            return genFastAddPeerRes(FastAddPeerStatus::NoSuitable, "", "");
        }
        bool success = false;
        LOG_DEBUG(log, "Begin to select checkpoint for region {}", region_id);
        while (!success)
        {
            for (const auto store_id : candidate_store_ids)
            {
                RUNTIME_CHECK(store_id != current_store_id);
                auto iter = checked_seq_map.find(store_id);
                auto checked_seq = (iter == checked_seq_map.end()) ? 0 : iter->second;
                auto [data_seq, checkpoint_data] = fap_ctx->getNewerCheckpointData(server->tmt->getContext(), store_id, checked_seq);
                checked_seq_map[store_id] = data_seq;
                if (data_seq > checked_seq)
                {
                    RUNTIME_CHECK(checkpoint_data != nullptr);
                    auto maybe_region_info = tryParseRegionInfoFromCheckpointData(checkpoint_data, store_id, region_id, server->proxy_helper);
                    if (!maybe_region_info.has_value())
                        continue;
                    std::tie(checkpoint_info, region, apply_state, region_state) = *maybe_region_info;
                    if (tryResetPeerIdInRegion(region, region_state, new_peer_id))
                    {
                        success = true;
                        LOG_INFO(log, "Select checkpoint with seq {} from store {} takes {} seconds, candidate_store_id size {} [region_id={}]", data_seq, checkpoint_info->remote_store_id, watch.elapsedSeconds(), candidate_store_ids.size(), region_id);
                        break;
                    }
                    else
                    {
                        LOG_DEBUG(log, "Checkpoint with seq {} from store {} doesn't contain reusable region info [region_id={}]", data_seq, store_id, region_id);
                    }
                }
            }
            if (!success)
            {
                if (watch.elapsedSeconds() >= settings.fap_wait_checkpoint_timeout_seconds)
                    return genFastAddPeerRes(FastAddPeerStatus::NoSuitable, "", "");
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
        }

        auto kvstore = server->tmt->getKVStore();
        kvstore->handleIngestCheckpoint(region, checkpoint_info, *server->tmt);

        // Write raft log to uni ps
        UniversalWriteBatch wb;
        RaftDataReader raft_data_reader(*(checkpoint_info->temp_ps));
        raft_data_reader.traverseRemoteRaftLogForRegion(region_id, [&](const UniversalPageId & page_id, PageSize size, const PS::V3::CheckpointLocation & location) {
            LOG_DEBUG(log, "Write raft log size {} for region {} with index {}", size, region_id, UniversalPageIdFormat::getU64ID(page_id));
            wb.putRemotePage(page_id, 0, size, location, {});
        });
        auto wn_ps = server->tmt->getContext().getWriteNodePageStorage();
        wn_ps->write(std::move(wb));

        return genFastAddPeerRes(FastAddPeerStatus::Ok, apply_state.SerializeAsString(), region_state.region().SerializeAsString());
    }
    catch (...)
    {
        DB::tryLogCurrentException("FastAddPeer", "Failed when try to restore from checkpoint");
        return genFastAddPeerRes(FastAddPeerStatus::BadData, "", "");
    }
}

FastAddPeerRes FastAddPeer(EngineStoreServerWrap * server, uint64_t region_id, uint64_t new_peer_id)
{
    try
    {
        auto * log = &Poco::Logger::get("FastAddPeer");
        auto fap_ctx = server->tmt->getContext().getSharedContextDisagg()->fap_context;
        if (fap_ctx == nullptr)
        {
            LOG_WARNING(log, "FAP Context is not initialized. Should only enable FAP in DisaggregatedStorageMode.");
            return genFastAddPeerRes(FastAddPeerStatus::NoSuitable, "", "");
        }
        if (!fap_ctx->tasks_trace->isScheduled(region_id))
        {
            // We need to schedule the task.
            auto res = fap_ctx->tasks_trace->addTask(region_id, [server, region_id, new_peer_id]() {
                return FastAddPeerImpl(server, region_id, new_peer_id);
            });
            if (res)
            {
                LOG_INFO(log, "Add new task [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
            }
            else
            {
                LOG_INFO(log, "Add new task fail(queue full) [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
                return genFastAddPeerRes(FastAddPeerStatus::WaitForData, "", "");
            }
        }

        if (fap_ctx->tasks_trace->isReady(region_id))
        {
            LOG_INFO(log, "Fetch task result [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
            return fap_ctx->tasks_trace->fetchResult(region_id);
        }
        else
        {
            LOG_DEBUG(log, "Task is still pending [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
            return genFastAddPeerRes(FastAddPeerStatus::WaitForData, "", "");
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException("FastAddPeer", fmt::format("Failed when try to restore from checkpoint {}", StackTrace().toString()));
        return genFastAddPeerRes(FastAddPeerStatus::OtherError, "", "");
    }
}
} // namespace DB
