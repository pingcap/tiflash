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

#include <Common/Logger.h>
#include <Storages/DeltaMerge/Segment_fwd.h>
#include <Storages/KVStore/MultiRaft/Disagg/fast_add_peer.pb.h>
#include <common/types.h>

#include <memory>

namespace DB
{
class Region;
using RegionPtr = std::shared_ptr<Region>;
class TMTContext;
class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;
struct CheckpointIngestInfo;
using CheckpointIngestInfoPtr = std::shared_ptr<CheckpointIngestInfo>;
struct TiFlashRaftProxyHelper;
class FastAddPeerContext;

// Safety: The class is immutable once created.
struct CheckpointIngestInfo
{
    DM::Segments getRestoredSegments() const { return restored_segments; }
    UInt64 getRemoteStoreId() const { return remote_store_id; }
    RegionPtr getRegion() const { return region; }
    UInt64 regionId() const { return region_id; }
    UInt64 peerId() const { return peer_id; }
    UInt64 beginTime() const { return begin_time; }
    UInt64 createdTime() const { return begin_time; }

    CheckpointIngestInfo(
        TMTContext & tmt_,
        UInt64 region_id_,
        UInt64 peer_id_,
        UInt64 remote_store_id_,
        RegionPtr region_,
        DM::Segments && restored_segments_,
        UInt64 begin_time_)
        : tmt(tmt_)
        , region_id(region_id_)
        , peer_id(peer_id_)
        , remote_store_id(remote_store_id_)
        , region(region_)
        , restored_segments(std::move(restored_segments_))
        , begin_time(begin_time_)
        , created_time(
              std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                  .count())
        , log(DB::Logger::get("CheckpointIngestInfo"))
    {}

    CheckpointIngestInfo(const CheckpointIngestInfo &) = delete;

    static CheckpointIngestInfoPtr restore(
        TMTContext & tmt,
        const TiFlashRaftProxyHelper * proxy_helper,
        UInt64 region_id,
        UInt64 peer_id);

    enum class CleanReason
    {
        Success,
        ProxyFallback,
        TiFlashCancel,
        ResolveStateApplySnapshot,
        ResolveStateDestroy,
    };

    // Only call to clean dangling CheckpointIngestInfo.
    static bool forciblyClean(
        TMTContext & tmt,
        const TiFlashRaftProxyHelper * proxy_helper,
        UInt64 region_id,
        bool in_memory,
        CleanReason reason);
    static bool cleanOnSuccess(TMTContext & tmt, UInt64 region_id);

    FastAddPeerProto::CheckpointIngestInfoPersisted serializeMeta() const;

private:
    friend class FastAddPeerContext;
    // Safety: raftstore ensures a region is handled in a single thread.
    // `persistToLocal` is called at a fixed place in this thread.
    void persistToLocal() const;
    static void deleteWrittenData(TMTContext & tmt, RegionPtr region, const DM::Segments & segments);

private:
    TMTContext & tmt;
    const UInt64 region_id = 0;
    const UInt64 peer_id = 0;
    const UInt64 remote_store_id;
    const RegionPtr region;
    const DM::Segments restored_segments;
    // The time the FAP task is added into async tasks pool.
    // If restored, `beginTime` is no longer meaningful.
    const UInt64 begin_time;
    const UInt64 created_time;
    DB::LoggerPtr log;
};
} // namespace DB
