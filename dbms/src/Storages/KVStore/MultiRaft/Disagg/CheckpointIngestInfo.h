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

#include <common/types.h>

#include <memory>

namespace DB
{
namespace DM
{
class Segment;
using SegmentPtr = std::shared_ptr<Segment>;
using Segments = std::vector<SegmentPtr>;
struct DMContext;
using DMContextPtr = std::shared_ptr<DMContext>;
} // namespace DM
class Region;
using RegionPtr = std::shared_ptr<Region>;
class TMTContext;

struct CheckpointIngestInfo
{
    // Safety: The class is immutable once created.

    // Get segments from memory or restore from ps.
    DM::Segments getRestoredSegments() const;
    UInt64 getRemoteStoreId() const;
    void markDelete();
    RegionPtr getRegion() const;
    UInt64 regionId() const { return region_id; }
    UInt64 peerId() const { return peer_id; }
    
    // Create from build
    CheckpointIngestInfo(
        TMTContext & tmt_,
        UInt64 region_id_,
        UInt64 peer_id_,
        UInt64 remote_store_id_,
        RegionPtr region_,
        DM::Segments && restored_segments_)
        : tmt(tmt_)
        , region_id(region_id_)
        , peer_id(peer_id_)
        , remote_store_id(remote_store_id_)
        , region(region_)
        , restored_segments(std::move(restored_segments_))
        , in_memory(true)
        , clean_when_destruct(false)
    {
        persistToPS();
    }

    // Create from restore
    CheckpointIngestInfo(
        TMTContext & tmt_,
        const struct TiFlashRaftProxyHelper * proxy_helper,
        UInt64 region_id_,
        UInt64 peer_id_)
        : tmt(tmt_)
        , region_id(region_id_)
        , peer_id(peer_id_)
        , in_memory(false)
        , clean_when_destruct(false)
    {
        loadFromPS(proxy_helper);
    }

    ~CheckpointIngestInfo();

private:
    void loadFromPS(const struct TiFlashRaftProxyHelper * proxy_helper);
    // Safety: raftstore ensures a region is handled in a single thread.
    // `persistToPS` is called at a fixed place in this thread.
    void persistToPS();
    void removeFromPS();

private:
    TMTContext & tmt;
    UInt64 region_id = 0;
    UInt64 peer_id = 0;
    UInt64 remote_store_id;
    RegionPtr region;
    DM::Segments restored_segments;
    bool in_memory;
    bool clean_when_destruct;
};
using CheckpointIngestInfoPtr = std::shared_ptr<CheckpointIngestInfo>;
} // namespace DB