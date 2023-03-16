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
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>

namespace DB
{

FastAddPeerContext::FastAddPeerContext(uint64_t thread_count)
{
    if (thread_count == 0)
    {
        static constexpr int ffi_handle_sec = 5;
        static constexpr int region_per_sec = 2;
        thread_count = ffi_handle_sec * region_per_sec;
    }
    tasks_trace = std::make_shared<AsyncTasks>(thread_count);
}

TempUniversalPageStoragePtr FastAddPeerContext::getTempUniversalPageStorage(UInt64 store_id, UInt64 upload_seq)
{
    std::unique_lock lock(ps_cache_mu);
    auto iter = temp_ps_cache.find(store_id);
    if (iter != temp_ps_cache.end() && iter->second.first >= upload_seq)
    {
        return iter->second.second;
    }
    return nullptr;
}

void FastAddPeerContext::updateTempUniversalPageStorage(UInt64 store_id, UInt64 upload_seq, TempUniversalPageStoragePtr temp_ps)
{
    std::unique_lock lock(ps_cache_mu);
    auto iter = temp_ps_cache.find(store_id);
    if (iter != temp_ps_cache.end() && iter->second.first >= upload_seq)
        return;

    temp_ps_cache[store_id] = std::make_pair(upload_seq, temp_ps);
}

void FastAddPeerContext::insertSegmentEndKeyInfoToCache(TableIdentifier table_identifier, const std::vector<std::pair<DM::RowKeyValue, UInt64>> & end_key_and_segment_ids)
{
    std::unique_lock lock(range_cache_mu);
    auto & end_key_to_id_map = segment_range_cache[table_identifier];
    for (const auto & [end_key, segment_id] : end_key_and_segment_ids)
    {
        end_key_to_id_map[end_key] = segment_id;
    }
}

UInt64 FastAddPeerContext::getSegmentIdContainingKey(TableIdentifier table_identifier, const DM::RowKeyValue & key)
{
    std::unique_lock lock(range_cache_mu);
    auto iter = segment_range_cache.find(table_identifier);
    if (iter != segment_range_cache.end())
    {
        auto & end_key_to_id_map = iter->second;
        auto key_iter = end_key_to_id_map.lower_bound(key);
        if (key_iter != end_key_to_id_map.end())
        {
            return key_iter->second;
        }
    }
    return 0;
}

void FastAddPeerContext::invalidateCache(TableIdentifier table_identifier)
{
    std::unique_lock lock(range_cache_mu);
    segment_range_cache.erase(table_identifier);
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

TempUniversalPageStoragePtr createTempPageStorage(Context & context, const String & manifest_key, UInt64 dir_seq)
{
    auto file_provider = context.getFileProvider();
    PageStorageConfig config;
    const auto dir_prefix = fmt::format("local_{}", dir_seq);
    auto temp_ps_wrapper = std::make_shared<TempUniversalPageStorage>();
    auto delegator = context.getPathPool().getPSDiskDelegatorGlobalMulti(dir_prefix);
    for (const auto & path : delegator->listPaths())
    {
        temp_ps_wrapper->paths.push_back(path);
        auto file = Poco::File(path);
        if (file.exists())
        {
            LOG_WARNING(Logger::get("createTempPageStorage"), "Path {} already exists, removing it", path);
            file.remove(true);
        }
    }
    auto local_ps = UniversalPageStorage::create( //
        dir_prefix,
        delegator,
        config,
        file_provider);
    local_ps->restore();
    temp_ps_wrapper->temp_ps = local_ps;
    auto * log = &Poco::Logger::get("FastAddPeer");
    LOG_DEBUG(log, "Begin to create temp ps from {}", manifest_key);

    RandomAccessFilePtr manifest_file = S3::S3RandomAccessFile::create(manifest_key);
    auto reader = PS::V3::CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = PS::V3::CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    UniversalWriteBatch wb;
    wb.disableRemoteLock();
    // insert delete records at last
    PS::V3::PageEntriesEdit<UniversalPageId>::EditRecords ref_records;
    PS::V3::PageEntriesEdit<UniversalPageId>::EditRecords delete_records;
    while (true)
    {
        auto edits = reader->readEdits(im);
        if (!edits.has_value())
            break;
        auto records = edits->getRecords();
        for (auto & record : records)
        {
            if (record.type == PS::V3::EditRecordType::VAR_ENTRY)
            {
                wb.putRemotePage(record.page_id, record.entry.tag, record.entry.checkpoint_info.data_location, std::move(record.entry.field_offsets));
            }
            else if (record.type == PS::V3::EditRecordType::VAR_REF)
            {
                ref_records.emplace_back(record);
            }
            else if (record.type == PS::V3::EditRecordType::VAR_DELETE)
            {
                delete_records.emplace_back(record);
            }
            else if (record.type == PS::V3::EditRecordType::VAR_EXTERNAL)
            {
                RUNTIME_CHECK(record.entry.checkpoint_info.has_value());
                wb.putRemoteExternal(record.page_id, record.entry.checkpoint_info.data_location);
            }
            else
            {
                RUNTIME_CHECK(false);
            }
        }
    }

    for (const auto & record : ref_records)
    {
        RUNTIME_CHECK(record.type == PS::V3::EditRecordType::VAR_REF);
        wb.putRefPage(record.page_id, record.ori_page_id);
    }
    for (const auto & record : delete_records)
    {
        RUNTIME_CHECK(record.type == PS::V3::EditRecordType::VAR_DELETE);
        wb.delPage(record.page_id);
    }
    local_ps->write(std::move(wb));
    return temp_ps_wrapper;
}

TempUniversalPageStoragePtr reuseOrCreateTempPageStorage(Context & context, const String & manifest_key)
{
    auto fap_ctx = context.getSharedContextDisagg()->fap_context;
    auto manifest_key_view = S3::S3FilenameView::fromKey(manifest_key);
    auto upload_seq = manifest_key_view.getUploadSequence();
    auto temp_ps = fap_ctx->getTempUniversalPageStorage(manifest_key_view.store_id, upload_seq);
    if (!temp_ps)
    {
        temp_ps = createTempPageStorage(context, manifest_key, fap_ctx->temp_ps_dir_sequence++);
        fap_ctx->updateTempUniversalPageStorage(manifest_key_view.store_id, upload_seq, temp_ps);
    }
    return temp_ps;
}

std::optional<CheckpointInfoPtr> tryGetCheckpointInfo(Context & context, const String & manifest_key, uint64_t region_id, TiFlashRaftProxyHelper * proxy_helper)
{
    auto * log = &Poco::Logger::get("FastAddPeer");

    auto checkpoint_info = std::make_shared<CheckpointInfo>();
    auto manifest_key_view = S3::S3FilenameView::fromKey(manifest_key);
    checkpoint_info->remote_store_id = manifest_key_view.store_id;
    checkpoint_info->temp_ps_wrapper = reuseOrCreateTempPageStorage(context, manifest_key);
    checkpoint_info->temp_ps = checkpoint_info->temp_ps_wrapper->temp_ps;

    try
    {
        auto apply_state_key = UniversalPageIdFormat::toRaftApplyStateKeyInKVEngine(region_id);
        auto page = checkpoint_info->temp_ps->read(apply_state_key);
        checkpoint_info->apply_state.ParseFromArray(page.data.begin(), page.data.size());
    }
    catch (...)
    {
        LOG_DEBUG(log, "Failed to find apply state key [region_id={}]", region_id);
        return std::nullopt;
    }

    try
    {
        auto local_state_key = UniversalPageIdFormat::toRegionLocalStateKeyInKVEngine(region_id);
        auto page = checkpoint_info->temp_ps->read(local_state_key);
        checkpoint_info->region_state.ParseFromArray(page.data.begin(), page.data.size());
    }
    catch (...)
    {
        LOG_DEBUG(log, "Failed to find region local state key [region_id={}]", region_id);
        return std::nullopt;
    }

    try
    {
        auto region_key = UniversalPageIdFormat::toKVStoreKey(region_id);
        auto page = checkpoint_info->temp_ps->read(region_key);
        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        checkpoint_info->region = Region::deserialize(buf, proxy_helper);
    }
    catch (...)
    {
        LOG_DEBUG(log, "Failed to find region key [region_id={}]", region_id);
        return std::nullopt;
    }

    return checkpoint_info;
}

std::vector<StoreID> getAllStoreIDsFromPD(TMTContext & tmt_context)
{
    auto pd_client = tmt_context.getPDClient();
    auto stores_from_pd = pd_client->getAllStores(/*exclude_tombstone*/ true);
    std::vector<StoreID> store_ids;
    store_ids.reserve(stores_from_pd.size());
    for (const auto & s : stores_from_pd)
    {
        store_ids.push_back(s.id());
    }
    return store_ids;
}

CheckpointInfoPtr selectCheckpointInfo(Context & context, uint64_t region_id, uint64_t new_peer_id, TiFlashRaftProxyHelper * proxy_helper)
{
    auto * log = &Poco::Logger::get("FastAddPeer");

    std::vector<CheckpointInfoPtr> candidates;
    std::map<uint64_t, std::string> reason;
    std::map<uint64_t, std::string> candidate_stat;

    auto & tmt_context = context.getTMTContext();
    std::vector<UInt64> all_store_ids = getAllStoreIDsFromPD(tmt_context);
    auto current_store_id = tmt_context.getKVStore()->getStoreMeta().id();
    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
    for (const auto & store_id : all_store_ids)
    {
        if (store_id == current_store_id)
            continue;
        const auto manifests = S3::CheckpointManifestS3Set::getFromS3(*s3_client, store_id);
        if (manifests.empty())
        {
            LOG_DEBUG(log, "no manifest on this store, skip store_id={}", store_id);
            continue;
        }
        const auto & latest_manifest_key = manifests.latestManifestKey();
        auto region_info = tryGetCheckpointInfo(context, latest_manifest_key, region_id, proxy_helper);
        if (region_info.has_value())
        {
            candidates.push_back(std::move(*region_info));
        }
    }

    if (candidates.empty())
    {
        LOG_INFO(log, "No candidate. [region_id={}]", region_id);
        return nullptr;
    }

    CheckpointInfoPtr winner = nullptr;
    uint64_t largest_applied_index = 0;
    for (const auto & candidate : candidates)
    {
        auto store_id = candidate->remote_store_id;
        const auto & region_state = candidate->region_state;
        const auto & apply_state = candidate->apply_state;
        const auto & peers = region_state.region().peers();
        bool ok = false;
        for (auto && pr : peers)
        {
            if (pr.id() == new_peer_id)
            {
                ok = true;
                break;
            }
        }
        if (!ok)
        {
            // Can't use this peer if it has no new_peer_id.
            reason[store_id] = fmt::format("has no peer_id {}", region_state.ShortDebugString());
            continue;
        }
        auto peer_state = region_state.state();
        if (peer_state == PeerState::Tombstone || peer_state == PeerState::Applying)
        {
            // Can't use this peer in these states.
            reason[store_id] = fmt::format("bad peer_state {}", region_state.ShortDebugString());
            continue;
        }
        auto applied_index = apply_state.applied_index();
        if (winner == nullptr || applied_index > largest_applied_index)
        {
            candidate_stat[store_id] = fmt::format("applied index {}", applied_index);
            winner = candidate;
        }
    }

    if (winner != nullptr)
    {
        return winner;
    }
    else
    {
        FmtBuffer fmt_buf;
        for (const auto & r : reason)
        {
            fmt_buf.fmtAppend("store {} reason {}, ", r.first, r.second);
        }
        std::string failed_reason = fmt_buf.toString();
        fmt_buf.clear();
        for (const auto & c : candidate_stat)
        {
            fmt_buf.fmtAppend("store {} stat {}, ", c.first, c.second);
        }
        std::string choice_stat = fmt_buf.toString();
        LOG_INFO(log, "Failed to find remote checkpoint [region_id={}] [new_peer_id={}] [total_candidates={}]; reason: {}; candidates_stat: {};", region_id, new_peer_id, candidates.size(), failed_reason, choice_stat);
        return nullptr;
    }
}

void resetPeerIdInRegion(RegionPtr region, const RegionLocalState & region_state, uint64_t new_peer_id)
{
    for (auto && pr : region_state.region().peers())
    {
        if (pr.id() == new_peer_id)
        {
            auto cpr = pr;
            region->mutMeta().setPeer(std::move(cpr));
            return;
        }
    }
    RUNTIME_CHECK(false);
}

FastAddPeerRes FastAddPeerImpl(EngineStoreServerWrap * server, uint64_t region_id, uint64_t new_peer_id)
{
    try
    {
        auto * log = &Poco::Logger::get("FastAddPeer");
        auto kvstore = server->tmt->getKVStore();
        Stopwatch watch;
        CheckpointInfoPtr checkpoint_info;
        while (true)
        {
            checkpoint_info = selectCheckpointInfo(server->tmt->getContext(), region_id, new_peer_id, server->proxy_helper);
            if (checkpoint_info == nullptr)
            {
                // TODO: make it a config
                constexpr int wait_source_apply_timeout_seconds = 60;
                if (watch.elapsedSeconds() >= wait_source_apply_timeout_seconds)
                    return genFastAddPeerRes(FastAddPeerStatus::NoSuitable, "", "");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            else
                break;
        }
        LOG_INFO(log, "Select checkpoint from store {} takes {} seconds; [region_id={}]", checkpoint_info->remote_store_id, watch.elapsedSeconds(), region_id);

        resetPeerIdInRegion(checkpoint_info->region, checkpoint_info->region_state, new_peer_id);

        kvstore->handleIngestCheckpoint(checkpoint_info, *server->tmt);

        // Write raft log to uni ps
        UniversalWriteBatch wb;
        RaftDataReader raft_data_reader(*(checkpoint_info->temp_ps));
        raft_data_reader.traverseRaftLogForRegion(region_id, [&](const UniversalPageId & page_id, DB::Page page) {
            MemoryWriteBuffer buf;
            buf.write(page.data.begin(), page.data.size());
            wb.putPage(page_id, 0, buf.tryGetReadBuffer(), page.data.size());
        });
        auto wn_ps = server->tmt->getContext().getWriteNodePageStorage();
        wn_ps->write(std::move(wb));

        return genFastAddPeerRes(FastAddPeerStatus::Ok, checkpoint_info->apply_state.SerializeAsString(), checkpoint_info->region_state.region().SerializeAsString());
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
        RUNTIME_CHECK(fap_ctx != nullptr);
        if (!fap_ctx->tasks_trace->isScheduled(region_id))
        {
            // We need to schedule the task.
            auto res = fap_ctx->tasks_trace->addTask(region_id, [server, region_id, new_peer_id]() {
                return FastAddPeerImpl(server, region_id, new_peer_id);
            });
            if (res)
            {
                LOG_INFO(log, "add new task [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
            }
            else
            {
                LOG_INFO(log, "add new task fail(queue full) [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
                return genFastAddPeerRes(FastAddPeerStatus::WaitForData, "", "");
            }
        }

        if (fap_ctx->tasks_trace->isReady(region_id))
        {
            LOG_INFO(log, "fetch task result [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
            return fap_ctx->tasks_trace->fetchResult(region_id);
        }
        else
        {
            LOG_DEBUG(log, "the task is still pending [new_peer_id={}] [region_id={}]", new_peer_id, region_id);
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
