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
    void setStartKey(std::string key);
    void setEndKey(std::string key);
    ImutRegionRangePtr getRange() const;
    const metapb::Region & getRegion() const;
    raft_serverpb::PeerState getState() const;
    void setState(raft_serverpb::PeerState value);
    void clearMergeState();
    bool operator==(const RegionState & region_state) const;
    const Base & getBase() const;

private:
    void updateRegionRange();
    metapb::Region & getMutRegion();

private:
    ImutRegionRangePtr region_range = nullptr;
};

} // namespace DB
