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

#include "FastAddPeer.h"

#include <Poco/DirectoryIterator.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>
#include <Storages/Page/V3/Remote/CheckpointPageManager.h>
#include <Storages/Page/universal/Readers.h>
#include <Storages/Page/universal/UniversalPageStorage.h>
#include <Storages/Transaction/Keys.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>

#include <cstdio>


namespace DB
{
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

using PS::V3::CheckpointManifestFileReader;
using PS::V3::PageDirectory;
using PS::V3::RemoteDataLocation;
using PS::V3::Remote::WriterInfo;
using PS::V3::universal::BlobStoreTrait;
using PS::V3::universal::PageDirectoryTrait;

std::string readData(const std::string & checkpoint_data_dir, const RemoteDataLocation & location)
{
    RUNTIME_CHECK(location.offset_in_file > 0);
    RUNTIME_CHECK(location.data_file_id != nullptr && !location.data_file_id->empty());

    std::string ret;
    ret.resize(location.size_in_file);

    // Note: We will introduce a DataReader when compression is added later.
    // When there is compression, we first need to seek and read compressed blocks, decompress them, and then seek to the data we want.
    // A DataReader will encapsulate this logic.
    // Currently there is no compression, so reading data is rather easy.

    auto buf = ReadBufferFromFile(checkpoint_data_dir + *location.data_file_id);
    buf.seek(location.offset_in_file);
    auto n = buf.readBig(ret.data(), location.size_in_file);
    RUNTIME_CHECK(n == location.size_in_file);

    return ret;
}

std::optional<RemoteMeta> fetchRemotePeerMeta(Context & context, const std::string & output_directory, const std::string & checkpoint_data_dir, uint64_t store_id, uint64_t region_id, uint64_t new_peer_id, TiFlashRaftProxyHelper * proxy_helper)
{
    UNUSED(new_peer_id);
    auto * log = &Poco::Logger::get("fast add");

    Poco::DirectoryIterator it(output_directory);
    Poco::DirectoryIterator end;
    std::string optimal;
    auto optimal_index = 0;
    while (it != end)
    {
        if (it->isFile())
        {
            Poco::Path p(it.path());
            auto index = 0;
            std::istringstream(p.getBaseName()) >> index;
            if (p.getExtension() == "manifest" && index > optimal_index)
            {
                optimal_index = index;
                optimal = p.toString();
            }
        }
        ++it;
    }

    LOG_DEBUG(log, "use optimal manifest {} checkpoint_data_dir {}", optimal, checkpoint_data_dir);

    if (optimal.empty())
    {
        return std::nullopt;
    }

    auto local_ps = PS::V3::CheckpointPageManager::createTempPageStorage(context, optimal, checkpoint_data_dir);

    RemoteMeta remote_meta;
    remote_meta.remote_store_id = store_id;
    remote_meta.checkpoint_path = optimal;
    {
        auto apply_state_key = RaftLogReader::toRegionApplyStateKey(region_id);
        auto page = local_ps->read(apply_state_key, nullptr, {}, /* throw_on_not_exist */false);
        if (page.isValid())
        {
            remote_meta.apply_state.ParseFromArray(page.data.begin(), page.data.size());
        }
        else
        {
            LOG_DEBUG(log, "cannot find apply state key {}", Redact::keyToDebugString(apply_state_key.data(), apply_state_key.size()));
        }
    }
    {
        auto region_state_key = RaftLogReader::toRegionLocalStateKey(region_id);
        auto page = local_ps->read(region_state_key, nullptr, {}, /* throw_on_not_exist */false);
        if (page.isValid())
        {
            remote_meta.region_state.ParseFromArray(page.data.begin(), page.data.size());
        }
        else
        {
            LOG_DEBUG(log, "cannot find region state key {}", Redact::keyToDebugString(region_state_key.data(), region_state_key.size()));
        }
    }
    {
        auto region_key = KVStoreReader::toFullPageId(region_id);
        auto page = local_ps->read(region_key, nullptr, {}, /* throw_on_not_exist */false);
        if (page.isValid())
        {
            ReadBufferFromMemory buf(page.data.begin(), page.data.size());
            remote_meta.region = Region::deserialize(buf, proxy_helper);
        }
        else
        {
            LOG_DEBUG(log, "cannot find region key {}", Redact::keyToDebugString(region_key.data(), region_key.size()));
        }
    }
    remote_meta.temp_ps = local_ps;
    return remote_meta;
}

std::string composeOutputDirectory(const std::string & remote_dir, uint64_t store_id, const std::string & storage_name)
{
    return fmt::format(
        "{}/store_{}/ps_{}_manifest/",
        remote_dir,
        store_id,
        storage_name);
}

std::vector<uint64_t> listAllStores(const std::string & remote_dir)
{
    Poco::DirectoryIterator it(remote_dir);
    Poco::DirectoryIterator end;
    std::vector<uint64_t> res;
    while (it != end)
    {
        if (it->isDirectory())
        {
            Poco::Path p(it.path());
            auto store_id = 0;
            if (sscanf(p.getBaseName().c_str(), "store_%u", &store_id) == 1 && store_id != 0)
            {
                res.push_back(store_id);
            }
        }
        ++it;
    }
    return res;
}

std::pair<bool, std::optional<RemoteMeta>> selectRemotePeer(Context & context, UniversalPageStoragePtr page_storage, uint64_t current_store_id, uint64_t region_id, uint64_t new_peer_id, TiFlashRaftProxyHelper * proxy_helper)
{
    auto * log = &Poco::Logger::get("fast add");

    std::vector<RemoteMeta> choices;
    std::map<uint64_t, std::string> reason;
    std::map<uint64_t, std::string> candidate_stat;

    // TODO Reconsider this logic when use S3.
    const auto & remote_dir = page_storage->config.ps_remote_directory.toString();
    const auto & storage_name = page_storage->storage_name;
    auto stores = listAllStores(remote_dir);
    for (const auto & store_id : stores)
    {
        if (store_id == current_store_id)
            continue;
        auto remote_manifest_directory = composeOutputDirectory(remote_dir, store_id, storage_name);
        auto maybe_choice = fetchRemotePeerMeta(context, remote_manifest_directory, remote_dir, store_id, region_id, new_peer_id, proxy_helper);
        if (maybe_choice.has_value())
        {
            choices.push_back(std::move(maybe_choice.value()));
        }
    }

    if (choices.empty())
    {
        LOG_INFO(log, "No candidate for region {}", region_id);
        return std::make_pair(false, std::nullopt);
    }

    std::optional<RemoteMeta> choosed = std::nullopt;
    uint64_t largest_applied_index = 0;
    for (auto it = choices.begin(); it != choices.end(); it++)
    {
        auto store_id = it->remote_store_id;
        const auto & region_state = it->region_state;
        const auto & apply_state = it->apply_state;
        const auto & peers = region_state.region().peers();
        bool ok = false;
        LOG_INFO(log, "store {} region_state {} region peers {}", store_id, region_state.DebugString(), region_state.region().DebugString());
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
        if (!choosed.has_value() || applied_index > largest_applied_index)
        {
            candidate_stat[store_id] = fmt::format("applied index {}", applied_index);
            choosed = *it;
        }
    }

    FmtBuffer fmt_buf;
    for (auto iter = reason.begin(); iter != reason.end(); iter++)
    {
        fmt_buf.fmtAppend("store {} reason {}, ", iter->first, iter->second);
    }
    std::string failed_reason = fmt_buf.toString();
    fmt_buf.clear();
    for (auto iter = candidate_stat.begin(); iter != candidate_stat.end(); iter++)
    {
        fmt_buf.fmtAppend("store {} stat {}, ", iter->first, iter->second);
    }
    std::string choice_stat = fmt_buf.toString();

    LOG_INFO(log, "fast add result region_id {} new_peer_id {} remote_dir {} storage_name {} total choices {}; failed: {}; candidates: {};", region_id, new_peer_id, remote_dir, storage_name, choices.size(), failed_reason, choice_stat);
    return std::make_pair(true, choosed);
}

FastAddPeerRes FastAddPeer(EngineStoreServerWrap * server, uint64_t region_id, uint64_t new_peer_id)
{
    try
    {
        std::optional<RemoteMeta> maybe_peer = std::nullopt;
        auto wn_ps = server->tmt->getContext().getWriteNodePageStorage();
        auto kvstore = server->tmt->getKVStore();
        auto current_store_id = kvstore->getStoreMeta().id();
        Stopwatch watch;
        while (true)
        {
            bool can_retry = false;
            std::tie(can_retry, maybe_peer) = selectRemotePeer(server->tmt->getContext(), wn_ps, current_store_id, region_id, new_peer_id, server->proxy_helper);
            if (!maybe_peer.has_value())
            {
                if (!can_retry || watch.elapsedSeconds() >= 60)
                    return genFastAddPeerRes(FastAddPeerStatus::NoSuitable, "", "");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            else
                break;
        }
        auto * log = &Poco::Logger::get("fast add");
        auto & peer = maybe_peer.value();
        auto checkpoint_store_id = peer.remote_store_id;
        auto checkpoint_manifest_path = peer.checkpoint_path;
        LOG_INFO(log, "select checkpoint path {} from store {} for region {} takes {} seconds", checkpoint_manifest_path, checkpoint_store_id, region_id, watch.elapsedSeconds());
        auto region = peer.region;
        if (!region)
            return genFastAddPeerRes(FastAddPeerStatus::NoSuitable, "", "");

        const auto & remote_dir = wn_ps->config.ps_remote_directory.toString();
        auto checkpoint_data_dir = remote_dir;

        {
            bool hit = false;
            const auto & region_pb = peer.region_state.region();
            for (auto && pr : region_pb.peers())
            {
                if (pr.id() == new_peer_id)
                {
                    auto cpr = pr;
                    peer.region->mutMeta().setPeer(std::move(cpr));
                    hit = true;
                    break;
                }
            }
            if (!hit)
            {
                LOG_ERROR(log, "selected remote peer of {} has no new_peer_id {}", region_id, new_peer_id);
                return genFastAddPeerRes(FastAddPeerStatus::BadData, "", "");
            }
            else
            {
                LOG_INFO(log, "reset selected remote peer of {} to peer {}", region_id, new_peer_id);
            }
        }

        auto local_ps = peer.temp_ps;
        kvstore->handleIngestCheckpoint(region, local_ps, checkpoint_manifest_path, checkpoint_data_dir, checkpoint_store_id, *server->tmt);

        UniversalWriteBatch wb;
        RaftLogReader raft_log_reader(*local_ps);
        raft_log_reader.traverseRaftLogForRegion(region_id, [&](const UniversalPageId & page_id, const DB::Page & page) {
            MemoryWriteBuffer buf(0, page.data.size());
            buf.write(page.data.begin(), page.data.size());
            wb.putPage(page_id, 0, buf.tryGetReadBuffer(), page.data.size());
        });
        wn_ps->write(std::move(wb));

        // Generate result.
        return genFastAddPeerRes(FastAddPeerStatus::Ok, peer.apply_state.SerializeAsString(), peer.region_state.region().SerializeAsString());
    }
    catch (...)
    {
        DB::tryLogCurrentException("FastAddPeer", "Failed when try to restore from checkpoint");
        return genFastAddPeerRes(FastAddPeerStatus::BadData, "", "");
    }
}
} // namespace DB
