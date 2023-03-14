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

#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>

using raft_serverpb::PeerState;
using raft_serverpb::RaftApplyState;
using raft_serverpb::RegionLocalState;

namespace DB
{
struct TempUniversalPageStorage;
using TempUniversalPageStoragePtr = std::shared_ptr<TempUniversalPageStorage>;

struct CheckpointInfo
{
    UInt64 remote_store_id;
    RegionLocalState region_state;
    RaftApplyState apply_state;
    RegionPtr region;
    TempUniversalPageStoragePtr temp_ps_wrapper; // a wrapper to protect the path of `temp_ps` to be deleted
    UniversalPageStoragePtr temp_ps;
};
using CheckpointInfoPtr = std::shared_ptr<CheckpointInfo>;
} // namespace DB