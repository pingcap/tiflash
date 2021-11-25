#include <Storages/Transaction/RegionState.h>
#include <Storages/Transaction/TiKVRange.h>

namespace DB
{
RegionState::RegionState(RegionState && region_state) noexcept
    : base(std::move(region_state.base))
    , region_range(std::move(region_state.region_range))
{}
RegionState::RegionState(Base && region_state)
    : base(std::move(region_state))
{
    updateRegionRange();
}
RegionState & RegionState::operator=(RegionState && from) noexcept
{
    if (this == std::addressof(from))
        return *this;

    this->base = std::move(from.base);
    this->region_range = std::move(from.region_range);
    return *this;
}

void RegionState::setRegion(metapb::Region region)
{
    getMutRegion() = std::move(region);
    updateRegionRange();
}

void RegionState::setVersion(const UInt64 version)
{
    getMutRegion().mutable_region_epoch()->set_version(version);
}

void RegionState::setConfVersion(const UInt64 version)
{
    getMutRegion().mutable_region_epoch()->set_conf_ver(version);
}

UInt64 RegionState::getVersion() const
{
    return getRegion().region_epoch().version();
}
UInt64 RegionState::getConfVersion() const
{
    return getRegion().region_epoch().conf_ver();
}

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

ImutRegionRangePtr RegionState::getRange() const
{
    return region_range;
}

void RegionState::updateRegionRange()
{
    region_range = std::make_shared<RegionRangeKeys>(TiKVKey::copyFrom(getRegion().start_key()), TiKVKey::copyFrom(getRegion().end_key()));
}

metapb::Region & RegionState::getMutRegion()
{
    return *base.mutable_region();
}
const metapb::Region & RegionState::getRegion() const
{
    return base.region();
}
raft_serverpb::PeerState RegionState::getState() const
{
    return base.state();
}
void RegionState::setState(raft_serverpb::PeerState value)
{
    base.set_state(value);
}
void RegionState::clearMergeState()
{
    base.clear_merge_state();
}
bool RegionState::operator==(const RegionState & region_state) const
{
    return getBase() == region_state.getBase();
}
const RegionState::Base & RegionState::getBase() const
{
    return base;
}
const raft_serverpb::MergeState & RegionState::getMergeState() const
{
    return base.merge_state();
}
raft_serverpb::MergeState & RegionState::getMutMergeState()
{
    return *base.mutable_merge_state();
}

bool computeMappedTableID(const DecodedTiKVKey & key, TableID & table_id)
{
    // t table_id _r
    if (key.size() >= (1 + 8 + 2) && key[0] == RecordKVFormat::TABLE_PREFIX
        && memcmp(key.data() + 9, RecordKVFormat::RECORD_PREFIX_SEP, 2) == 0)
    {
        table_id = RecordKVFormat::getTableId(key);
        return true;
    }

    return false;
}

RegionRangeKeys::RegionRangeKeys(TiKVKey && start_key, TiKVKey && end_key)
    : ori(RegionRangeKeys::makeComparableKeys(std::move(start_key), std::move(end_key)))
    , raw(std::make_shared<DecodedTiKVKey>(ori.first.key.empty() ? DecodedTiKVKey() : RecordKVFormat::decodeTiKVKey(ori.first.key)),
          std::make_shared<DecodedTiKVKey>(ori.second.key.empty() ? DecodedTiKVKey() : RecordKVFormat::decodeTiKVKey(ori.second.key)))
{
    if (!computeMappedTableID(*raw.first, mapped_table_id) || ori.first.compare(ori.second) >= 0)
    {
        throw Exception("Illegal region range, should not happen, start key: " + ori.first.key.toDebugString()
                            + ", end key: " + ori.second.key.toDebugString(),
                        ErrorCodes::LOGICAL_ERROR);
    }
}

TableID RegionRangeKeys::getMappedTableID() const
{
    return mapped_table_id;
}

const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & RegionRangeKeys::rawKeys() const
{
    return raw;
}

HandleRange<HandleID> getHandleRangeByTable(const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & rawKeys, TableID table_id)
{
    return TiKVRange::getHandleRangeByTable(*rawKeys.first, *rawKeys.second, table_id);
}

const RegionRangeKeys::RegionRange & RegionRangeKeys::comparableKeys() const
{
    return ori;
}

template <bool is_start>
TiKVRangeKey TiKVRangeKey::makeTiKVRangeKey(TiKVKey && key)
{
    State state = key.empty() ? (is_start ? MIN : MAX) : NORMAL;
    return TiKVRangeKey(state, std::move(key));
}

template TiKVRangeKey TiKVRangeKey::makeTiKVRangeKey<true>(TiKVKey &&);
template TiKVRangeKey TiKVRangeKey::makeTiKVRangeKey<false>(TiKVKey &&);

RegionRangeKeys::RegionRange RegionRangeKeys::makeComparableKeys(TiKVKey && start_key, TiKVKey && end_key)
{
    return std::make_pair(
        TiKVRangeKey::makeTiKVRangeKey<true>(std::move(start_key)),
        TiKVRangeKey::makeTiKVRangeKey<false>(std::move(end_key)));
}

int TiKVRangeKey::compare(const TiKVKey & tar) const
{
    if (state != TiKVRangeKey::NORMAL)
        return state - TiKVRangeKey::NORMAL;
    return key.compare(tar);
}

int TiKVRangeKey::compare(const TiKVRangeKey & tar) const
{
    if (state != tar.state)
        return state - tar.state;
    return key.compare(tar.key);
}

TiKVRangeKey::TiKVRangeKey(State state_, TiKVKey && key_)
    : state(state_)
    , key(std::move(key_))
{}

TiKVRangeKey::TiKVRangeKey(TiKVRangeKey && src)
    : state(src.state)
    , key(std::move(src.key))
{}

TiKVRangeKey & TiKVRangeKey::operator=(TiKVRangeKey && src)
{
    if (this == &src)
        return *this;

    state = src.state;
    key = std::move(src.key);
    return *this;
}

TiKVRangeKey TiKVRangeKey::copy() const
{
    return TiKVRangeKey(state, TiKVKey::copyFrom(key));
}

} // namespace DB
