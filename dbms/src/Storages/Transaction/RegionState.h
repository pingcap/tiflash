#pragma once

#include <Storages/Transaction/RegionRangeKeys.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/metapb.pb.h>
#include <kvproto/raft_serverpb.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

using ImutRegionRangePtr = std::shared_ptr<const RegionRangeKeys>;

class RegionState : private raft_serverpb::RegionLocalState, private boost::noncopyable
{
public:
    using Base = raft_serverpb::RegionLocalState;

    RegionState() = default;
    explicit RegionState(RegionState && region_state);
    explicit RegionState(Base && region_state);
    RegionState & operator=(RegionState && from);

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
    void updateRegionRange();

private:
    ImutRegionRangePtr region_range = nullptr;
};

HandleRange<HandleID> getHandleRangeByTable(const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & rawKeys, TableID table_id);
bool computeMappedTableID(const DecodedTiKVKey & key, TableID & table_id);

} // namespace DB
