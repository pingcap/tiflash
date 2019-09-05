#include <Storages/Transaction/RegionState.h>
#include <Storages/Transaction/TiKVRange.h>

namespace DB
{

RegionState::RegionState(RegionState && region_state) : Base(std::move(region_state)), region_range(region_state.region_range) {}
RegionState::RegionState(Base && region_state) : Base(std::move(region_state)) { updateRegionRange(); }
RegionState & RegionState::operator=(RegionState && from)
{
    if (&from == this)
        return *this;

    (Base &)* this = (Base &&) from;
    region_range = std::move(from.region_range);
    return *this;
}

void RegionState::setRegion(metapb::Region region)
{
    getMutRegion() = std::move(region);
    updateRegionRange();
}

void RegionState::setVersion(const UInt64 version) { getMutRegion().mutable_region_epoch()->set_version(version); }

void RegionState::setConfVersion(const UInt64 version) { getMutRegion().mutable_region_epoch()->set_conf_ver(version); }

void RegionState::setStartKey(std::string key)
{
    getMutRegion().set_start_key(std::move(key));
    updateRegionRange();
}

void RegionState::setEndKey(std::string key)
{
    getMutRegion().set_end_key(std::move(key));
    updateRegionRange();
}

ImutRegionRangePtr RegionState::getRange() const { return region_range; }

void RegionState::updateRegionRange()
{
    region_range = std::make_shared<RegionRangeKeys>(TiKVKey::copyFrom(getRegion().start_key()), TiKVKey::copyFrom(getRegion().end_key()));
}

metapb::Region & RegionState::getMutRegion() { return *mutable_region(); }
const metapb::Region & RegionState::getRegion() const { return region(); }
raft_serverpb::PeerState RegionState::getState() const { return state(); }
void RegionState::setState(raft_serverpb::PeerState value) { set_state(value); }
void RegionState::clearMergeState() { clear_merge_state(); }
bool RegionState::operator==(const RegionState & region_state) const { return getBase() == region_state.getBase(); }
const RegionState::Base & RegionState::getBase() const { return *this; }

RegionRangeKeys::RegionRangeKeys(TiKVKey start_key, TiKVKey end_key)
    : ori(std::move(start_key), std::move(end_key)),
      raw(ori.first.empty() ? DecodedTiKVKey() : RecordKVFormat::decodeTiKVKey(ori.first),
          ori.second.empty() ? DecodedTiKVKey() : RecordKVFormat::decodeTiKVKey(ori.second))
{}

const std::pair<TiKVKey, TiKVKey> & RegionRangeKeys::keys() const { return ori; }

const std::pair<DecodedTiKVKey, DecodedTiKVKey> & RegionRangeKeys::rawKeys() const { return raw; }

HandleRange<HandleID> RegionRangeKeys::getHandleRangeByTable(const TableID table_id) const
{
    return TiKVRange::getHandleRangeByTable(rawKeys().first, rawKeys().second, table_id);
}

} // namespace DB
