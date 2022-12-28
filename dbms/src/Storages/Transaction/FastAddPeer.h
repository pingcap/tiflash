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
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>

#include <tuple>

using raft_serverpb::PeerState;
using raft_serverpb::RaftApplyState;
using raft_serverpb::RegionLocalState;

namespace DB
{
using RemoteMeta = std::tuple<uint64_t, RegionLocalState, RaftApplyState, std::string, RegionPtr>;

std::optional<RemoteMeta> selectRemotePeer(UniversalPageStoragePtr, uint64_t region_id, uint64_t new_peer_id, TiFlashRaftProxyHelper * proxy_helper = nullptr);
std::string composeOutputDirectory(const std::string & remote_dir, uint64_t store_id, const std::string & storage_name);
std::string composeOutputDataDirectory(const std::string & remote_dir, uint64_t store_id, const std::string & storage_name);
std::optional<RemoteMeta> fetchRemotePeerMeta(const std::string & output_directory, uint64_t store_id, uint64_t region_id, uint64_t new_peer_id, TiFlashRaftProxyHelper * proxy_helper = nullptr);
} // namespace DB