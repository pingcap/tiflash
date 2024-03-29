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

#pragma once

#include <Storages/DeltaMerge/Segment_fwd.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/MultiRaft/Disagg/CheckpointIngestInfo.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerCache.h>
#include <Storages/KVStore/Utils/AsyncTasks.h>

namespace DB
{
using FAPAsyncTasks = AsyncTasks<uint64_t, std::function<FastAddPeerRes()>, FastAddPeerRes>;
struct CheckpointInfo;
using CheckpointInfoPtr = std::shared_ptr<CheckpointInfo>;
class Region;
using RegionPtr = std::shared_ptr<Region>;
using CheckpointRegionInfoAndData
    = std::tuple<CheckpointInfoPtr, RegionPtr, raft_serverpb::RaftApplyState, raft_serverpb::RegionLocalState>;

class FastAddPeerContext
{
public:
    explicit FastAddPeerContext(uint64_t thread_count = 0);

    // Return parsed checkpoint data and its corresponding seq which is newer than `required_seq` if exists, otherwise return pair<required_seq, nullptr>
    std::pair<UInt64, ParsedCheckpointDataHolderPtr> getNewerCheckpointData(
        Context & context,
        UInt64 store_id,
        UInt64 required_seq);

    // Checkpoint ingest management
    CheckpointIngestInfoPtr getOrRestoreCheckpointIngestInfo(
        TMTContext & tmt,
        const struct TiFlashRaftProxyHelper * proxy_helper,
        UInt64 region_id,
        UInt64 peer_id);
    void insertCheckpointIngestInfo(
        TMTContext & tmt,
        UInt64 region_id,
        UInt64 peer_id,
        UInt64 remote_store_id,
        RegionPtr region,
        DM::Segments && segments,
        UInt64 start_time);
    std::optional<CheckpointIngestInfoPtr> tryGetCheckpointIngestInfo(UInt64 region_id) const;
    void cleanTask(
        TMTContext & tmt,
        const struct TiFlashRaftProxyHelper * proxy_helper,
        UInt64 region_id,
        CheckpointIngestInfo::CleanReason reason);
    void resolveFapSnapshotState(
        TMTContext & tmt,
        const struct TiFlashRaftProxyHelper * proxy_helper,
        UInt64 region_id,
        bool is_regular_snapshot);

    // Remove the checkpoint ingest info from memory. Only for testing.
    void debugRemoveCheckpointIngestInfo(UInt64 region_id);

public:
    std::shared_ptr<FAPAsyncTasks> tasks_trace;

private:
    class CheckpointCacheElement
    {
    public:
        explicit CheckpointCacheElement(const String & manifest_key_, UInt64 dir_seq_)
            : manifest_key(manifest_key_)
            , dir_seq(dir_seq_)
        {}

        ParsedCheckpointDataHolderPtr getParsedCheckpointData(Context & context);

    private:
        std::mutex mu;
        String manifest_key;
        UInt64 dir_seq;
        ParsedCheckpointDataHolderPtr parsed_checkpoint_data;
    };
    using CheckpointCacheElementPtr = std::shared_ptr<CheckpointCacheElement>;

    std::atomic<UInt64> temp_ps_dir_sequence = 0;

    std::mutex cache_mu;
    // Store the latest manifest data for every store
    // StoreId -> std::pair<UploadSeq, CheckpointCacheElementPtr>
    std::unordered_map<UInt64, std::pair<UInt64, CheckpointCacheElementPtr>> checkpoint_cache_map;

    // Checkpoint that is persisted, but yet to be ingested into DeltaTree.
    mutable std::mutex ingest_info_mu;
    // RegionID->CheckpointIngestInfoPtr
    std::unordered_map<UInt64, CheckpointIngestInfoPtr> checkpoint_ingest_info_map;

    LoggerPtr log;
};
} // namespace DB
