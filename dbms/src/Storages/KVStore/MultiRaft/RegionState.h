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

#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/metapb.pb.h>
#include <kvproto/raft_serverpb.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

using ImutRegionRangePtr = std::shared_ptr<const RegionRangeKeys>;

class RegionState : private boost::noncopyable
{
public:
    using Base = raft_serverpb::RegionLocalState;

    RegionState() = default;
    RegionState(RegionState && region_state) noexcept;
    explicit RegionState(Base && region_state);
    RegionState & operator=(RegionState && from) noexcept;

    void setRegion(metapb::Region region);
    void setVersion(const UInt64 version);
    void setConfVersion(const UInt64 version);
    UInt64 getVersion() const;
    UInt64 getConfVersion() const;
    void setStartKey(std::string key);
    void setEndKey(std::string key);
    ImutRegionRangePtr getRange() const;
    const metapb::Region & getRegion() const;
    metapb::Region & getMutRegion();
    raft_serverpb::PeerState getState() const;
    void setState(raft_serverpb::PeerState value);
    void clearMergeState();
    bool operator==(const RegionState & region_state) const;
    const Base & getBase() const;
    const raft_serverpb::MergeState & getMergeState() const;
    raft_serverpb::MergeState & getMutMergeState();

private:
    Base base;
    void updateRegionRange();

private:
    ImutRegionRangePtr region_range = nullptr;
};

HandleRange<HandleID> getHandleRangeByTable(
    const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & rawKeys,
    TableID table_id);
bool computeMappedTableID(const DecodedTiKVKey & key, TableID & table_id);

} // namespace DB
