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

#include <Interpreters/Context.h>
#include <Poco/DirectoryIterator.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>
#include <Storages/Page/universal/UniversalPageStorage.h>
#include <Storages/Transaction/Keys.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>

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

std::string readData(const std::string & output_directory, const RemoteDataLocation & location)
{
    RUNTIME_CHECK(location.offset_in_file > 0);
    RUNTIME_CHECK(location.data_file_id != nullptr && !location.data_file_id->empty());

    std::string ret;
    ret.resize(location.size_in_file);

    // Note: We will introduce a DataReader when compression is added later.
    // When there is compression, we first need to seek and read compressed blocks, decompress them, and then seek to the data we want.
    // A DataReader will encapsulate this logic.
    // Currently there is no compression, so reading data is rather easy.

    auto buf = ReadBufferFromFile(output_directory + *location.data_file_id);
    buf.seek(location.offset_in_file);
    auto n = buf.readBig(ret.data(), location.size_in_file);
    RUNTIME_CHECK(n == location.size_in_file);

    return ret;
}

std::optional<RemoteMeta> fetchRemotePeerMeta(const std::string & output_directory, uint64_t store_id, uint64_t region_id, uint64_t new_peer_id)
{
    UNUSED(new_peer_id);
    auto log = &Poco::Logger::get("fast add");

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
                optimal = p.getFileName();
            }
            ++it;
        }
    }
    LOG_DEBUG(log, "use optimal manifest {}", optimal);

    if (optimal.empty())
    {
        return std::nullopt;
    }

    auto reader = CheckpointManifestFileReader<PageDirectoryTrait>::create(CheckpointManifestFileReader<PageDirectoryTrait>::Options{.file_path = output_directory + optimal});
    auto edit = reader->read();
    LOG_DEBUG(log, "content is {}", edit.toDebugString());

    auto records = edit.getRecords();

    raft_serverpb::RegionLocalState restored_region_state;
    raft_serverpb::RaftApplyState restored_apply_state;
    for (auto iter = records.begin(); iter != records.end(); iter++)
    {
        std::string page_id = iter->page_id;
        if (keys::validateApplyStateKey(page_id.data(), page_id.size(), region_id))
        {
            std::string decoded_data;
            auto & location = iter->entry.remote_info->data_location;
            std::string ret = readData(output_directory, location);
            restored_apply_state.ParseFromArray(ret.data(), ret.size());
        }
        else if (keys::validateRegionStateKey(page_id.data(), page_id.size(), region_id))
        {
            std::string decoded_data;
            auto & location = iter->entry.remote_info->data_location;
            std::string ret = readData(output_directory, location);
            restored_region_state.ParseFromArray(ret.data(), ret.size());
        }
        else
        {
            // Other data, such as RegionPersister.
        }
    }
    return std::make_tuple(store_id, restored_region_state, restored_apply_state, optimal);
}

std::optional<RemoteMeta> selectRemotePeer(EngineStoreServerWrap * server, uint64_t region_id, uint64_t new_peer_id)
{
    auto log = &Poco::Logger::get("fast add");

    std::vector<RemoteMeta> choices;
    std::map<uint64_t, std::string> reason;
    std::map<uint64_t, std::string> candidate_stat;

    // TODO Fetch meta from all store by fetchRemotePeerMeta.
    std::optional<RemoteMeta> choosed = std::nullopt;
    uint64_t largest_applied_index = 0;
    for (auto it = choices.begin(); it != choices.end(); it++)
    {
        auto store_id = std::get<0>(*it);
        const auto & region_state = std::get<1>(*it);
        const auto & apply_state = std::get<2>(*it);
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
            reason[store_id] = fmt::format("has no peer_id {}", region_state.DebugString());
            continue;
        }
        auto peer_state = region_state.state();
        if (peer_state == PeerState::Tombstone || peer_state == PeerState::Applying)
        {
            // Can't use this peer in these states.
            reason[store_id] = fmt::format("bad peer_state {}", region_state.DebugString());
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
    for (auto iter = reason.begin(); iter != reason.end(); iter++)
    {
        fmt_buf.fmtAppend("store {} stat {}, ", iter->first, iter->second);
    }
    std::string choice_stat = fmt_buf.toString();

    LOG_DEBUG(log, "fast add result region_id {} new peer_id {};", region_id, new_peer_id, failed_reason, choice_stat);
    return choosed;
}

FastAddPeerRes FastAddPeer(EngineStoreServerWrap * server, uint64_t region_id, uint64_t new_peer_id)
{
    UNUSED(server);
    UNUSED(region_id);
    UNUSED(new_peer_id);

    std::optional<RemoteMeta> maybe_peer = std::nullopt;
    while (true)
    {
        auto maybe_peer = selectRemotePeer(server, region_id, new_peer_id);
        if (!maybe_peer.has_value())
        {
            // TODO retry
            return genFastAddPeerRes(FastAddPeerStatus::NoSuitable, "", "");
        }
        else
            break;
    }

    // Load data from remote.

    // Generate result.
    auto & peer = maybe_peer.value();
    return genFastAddPeerRes(FastAddPeerStatus::Ok, std::get<1>(peer).SerializeAsString(), std::get<2>(peer).SerializeAsString());
}
} // namespace DB