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

#pragma once

#include <Interpreters/Context.h>
#include <Storages/Transaction/FastAddPeerContext.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LocalPageStorageCache.h>
#include <Storages/Transaction/ProxyFFI.h>

#include <future>
#include <memory>
#include <tuple>
#include <utility>

#include "Poco/Logger.h"
#include "common/logger_useful.h"

using raft_serverpb::PeerState;
using raft_serverpb::RaftApplyState;
using raft_serverpb::RegionLocalState;

namespace DB
{
struct RemoteMeta
{
    uint64_t remote_store_id;
    RegionLocalState region_state;
    RaftApplyState apply_state;
    std::string checkpoint_path;
    RegionPtr region;
    UniversalPageStoragePtr temp_ps;
    uint64_t version;
};

struct FastAddPeerContext::AsyncTasks
{
    using Key = uint64_t;
    using Func = std::function<FastAddPeerRes()>;

    explicit AsyncTasks(uint64_t pool_size)
    {
        // We use a very big queue.
        thread_pool = std::make_unique<ThreadPool>(pool_size, pool_size, 300);
    }
    bool addTask(Key k, Func f);
    bool isScheduled(Key key) const;
    bool isReady(Key key) const;
    FastAddPeerRes fetchResult(Key key);

protected:
    std::map<Key, std::future<FastAddPeerRes>> futures;
    std::unique_ptr<ThreadPool> thread_pool;
    mutable std::mutex mtx;
};

// pair<can_retry, remote_meta>
std::pair<bool, std::optional<RemoteMeta>> selectRemotePeer(Context & context, UniversalPageStoragePtr page_storage, uint64_t current_store_id, uint64_t region_id, uint64_t new_peer_id, TiFlashRaftProxyHelper * proxy_helper = nullptr);
std::string composeOutputDirectory(const std::string & remote_dir, uint64_t store_id, const std::string & storage_name);
std::optional<RemoteMeta> fetchRemotePeerMeta(Context & context, const std::string & output_directory, const std::string & checkpoint_data_dir, uint64_t store_id, uint64_t region_id, uint64_t new_peer_id, TiFlashRaftProxyHelper * proxy_helper = nullptr);
} // namespace DB