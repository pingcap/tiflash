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

#include <Storages/KVStore/Decode/TiKVRange.h>
#include <Storages/KVStore/MultiRaft/RegionState.h>

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
    region_range = std::make_shared<RegionRangeKeys>(
        TiKVKey::copyFrom(getRegion().start_key()),
        TiKVKey::copyFrom(getRegion().end_key()));
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
    auto k = key.getUserKey();
    // t table_id _r
    if (k.size() >= (1 + 8 + 2) && k[0] == RecordKVFormat::TABLE_PREFIX
        && memcmp(k.data() + 9, RecordKVFormat::RECORD_PREFIX_SEP, 2) == 0)
    {
        table_id = RecordKVFormat::getTableId(k);
        return true;
    }

    return false;
}


HandleRange<HandleID> getHandleRangeByTable(
    const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & rawKeys,
    TableID table_id)
{
    return TiKVRange::getHandleRangeByTable(*rawKeys.first, *rawKeys.second, table_id);
}

template <bool is_start>
TiKVRangeKey TiKVRangeKey::makeTiKVRangeKey(TiKVKey && key)
{
    State state = key.empty() ? (is_start ? MIN : MAX) : NORMAL;
    return TiKVRangeKey(state, std::move(key));
}

template TiKVRangeKey TiKVRangeKey::makeTiKVRangeKey<true>(TiKVKey &&);
template TiKVRangeKey TiKVRangeKey::makeTiKVRangeKey<false>(TiKVKey &&);

std::string TiKVRangeKey::toDebugString() const
{
    if (this->state == MAX)
    {
        return "inf";
    }
    else if (this->state == MIN)
    {
        return "-inf";
    }
    return this->key.toDebugString();
}

std::string RegionRangeKeys::toDebugString() const
{
    return fmt::format("[{},{}]", this->ori.first.toDebugString(), this->ori.second.toDebugString());
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
