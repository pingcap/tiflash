// Copyright 2025 PingCAP, Inc.
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

#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB
{
RegionRangeKeys::RegionRangeKeys(TiKVKey && start_key, TiKVKey && end_key)
    : ori(RegionRangeKeys::makeComparableKeys(std::move(start_key), std::move(end_key)))
    , raw(std::make_shared<DecodedTiKVKey>(
              ori.first.key.empty() ? DecodedTiKVKey() : RecordKVFormat::decodeTiKVKey(ori.first.key)),
          std::make_shared<DecodedTiKVKey>(
              ori.second.key.empty() ? DecodedTiKVKey() : RecordKVFormat::decodeTiKVKey(ori.second.key)))
{
    keyspace_id = raw.first->getKeyspaceID();
    if (!computeMappedTableID(*raw.first, mapped_table_id) || ori.first.compare(ori.second) >= 0)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Illegal region range, should not happen, start_key={} end_key={}",
            ori.first.key.toDebugString(),
            ori.second.key.toDebugString());
    }
}

TableID RegionRangeKeys::getMappedTableID() const
{
    return mapped_table_id;
}

KeyspaceID RegionRangeKeys::getKeyspaceID() const
{
    return keyspace_id;
}

const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & RegionRangeKeys::rawKeys() const
{
    return raw;
}

const RegionRangeKeys::RegionRange & RegionRangeKeys::comparableKeys() const
{
    return ori;
}

RegionRangeKeys::RegionRange RegionRangeKeys::makeComparableKeys(TiKVKey && start_key, TiKVKey && end_key)
{
    return std::make_pair(
        TiKVRangeKey::makeTiKVRangeKey<true>(std::move(start_key)),
        TiKVRangeKey::makeTiKVRangeKey<false>(std::move(end_key)));
}

RegionRangeKeys::RegionRange RegionRangeKeys::cloneRange(const RegionRange & from)
{
    return std::make_pair(from.first.copy(), from.second.copy());
}


} // namespace DB
