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

#include <Common/TiFlashMetrics.h>
#include <Storages/KVStore/MultiRaft/RegionCFDataBase.h>
#include <Storages/KVStore/MultiRaft/RegionCFDataTrait.h>
#include <Storages/KVStore/MultiRaft/RegionData.h>
#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>

namespace DB
{
using CFModifyFlag = RecordKVFormat::CFModifyFlag;

template <typename Trait>
const TiKVKey & RegionCFDataBase<Trait>::getTiKVKey(const Value & val)
{
    return *std::get<0>(val);
}

template <typename Value>
const std::shared_ptr<const TiKVValue> & getTiKVValuePtr(const Value & val)
{
    return std::get<1>(val);
}

template <typename Trait>
const TiKVValue & RegionCFDataBase<Trait>::getTiKVValue(const Value & val)
{
    return *getTiKVValuePtr<Value>(val);
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::insert(TiKVKey && key, TiKVValue && value, DupCheck mode)
{
    auto raw_key = RecordKVFormat::decodeTiKVKey(key);
    auto kv_pair_ptr = Trait::genKVPair(std::move(key), std::move(raw_key), std::move(value));
    if (!kv_pair_ptr)
        return {0, 0};

    auto & kv_pair = *kv_pair_ptr;
    auto & map = data;
    TiKVValue prev_value;
    if (mode == DupCheck::AllowSame)
    {
        prev_value = TiKVValue::copyFrom(getTiKVValue(kv_pair.second));
    }
    auto [it, ok] = map.emplace(std::move(kv_pair));

    // We support duplicated kv pairs if they are the same in snapshot.
    // This is because kvs in raftstore v2's snapshot may be overlapped.
    // However, we still not permit duplicated kvs from raft cmd.

    if (!ok)
    {
        if (mode == DupCheck::Deny)
        {
            throw Exception(
                "Found existing key in hex: " + getTiKVKey(it->second).toDebugString(),
                ErrorCodes::LOGICAL_ERROR);
        }
        else if (mode == DupCheck::AllowSame)
        {
            if (prev_value != getTiKVValue(it->second))
            {
                throw Exception(
                    "Found existing key in hex and val differs: " + getTiKVKey(it->second).toDebugString()
                        + " prev_val: " + getTiKVValue(it->second).toDebugString()
                        + " new_val: " + prev_value.toDebugString(),
                    ErrorCodes::LOGICAL_ERROR);
            }
            // Duplicated key is ignored
            return {0, 0};
        }
        else
        {
            throw Exception(
                "Found existing key in hex: " + getTiKVKey(it->second).toDebugString(),
                ErrorCodes::LOGICAL_ERROR);
        }
    }

    // No decoded data in write & default cf currently.
    return {calcTiKVKeyValueSize(it->second), 0};
}

template <>
RegionDataRes RegionCFDataBase<RegionLockCFDataTrait>::insert(TiKVKey && key, TiKVValue && value, DupCheck)
{
    RegionDataRes delta;
    Pair kv_pair = RegionLockCFDataTrait::genKVPair(std::move(key), std::move(value));
    const auto & decoded = std::get<2>(kv_pair.second);
    bool is_large_txn = decoded->isLargeTxn();
    {
        auto iter = data.find(kv_pair.first);
        if (iter != data.end())
        {
            // Could be a perssimistic lock is overwritten, or a old generation large txn lock is overwritten.
            delta.sub(calcTotalKVSize(iter->second));
            data.erase(iter);

            // In most cases, an optimistic lock replace a pessimistic lock.
            GET_METRIC(tiflash_raft_process_keys, type_lock_replaced).Increment(1);
        }
        if unlikely (is_large_txn)
        {
            GET_METRIC(tiflash_raft_process_keys, type_large_txn_lock_put).Increment(1);
        }
    }
    // According to the process of pessimistic lock, just overwrite.
    if (const auto & [it, ok] = data.emplace(std::move(kv_pair.first), std::move(kv_pair.second)); ok)
    {
        delta.add(calcTotalKVSize(it->second));
    }
    return delta;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const Value & value)
{
    return calcTiKVKeyValueSize(getTiKVKey(value), getTiKVValue(value));
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::calcTotalKVSize(const Value & value)
{
    if constexpr (std::is_same<Trait, RegionLockCFDataTrait>::value)
    {
        return {calcTiKVKeyValueSize(getTiKVKey(value), getTiKVValue(value)), std::get<2>(value)->getSize()};
    }
    else
    {
        return {calcTiKVKeyValueSize(getTiKVKey(value), getTiKVValue(value)), 0};
    }
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value)
{
    // Previously, we don't count size of Lock Cf.
    return key.dataSize() + value.dataSize();
}

template <typename Trait>
bool RegionCFDataBase<Trait>::shouldIgnoreRemove(const RegionCFDataBase::Value & value)
{
    if constexpr (std::is_same<Trait, RegionWriteCFDataTrait>::value)
    {
        return RegionWriteCFDataTrait::getWriteType(value) == CFModifyFlag::DelFlag;
    }
    else
    {
        return false;
    }
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::remove(const Key & key, bool quiet)
{
    auto & map = data;

    if (auto it = map.find(key); it != map.end())
    {
        const Value & value = it->second;

        if (shouldIgnoreRemove(value))
            return {0, 0};

        auto delta = -calcTotalKVSize(value);
        if constexpr (std::is_same<Trait, RegionLockCFDataTrait>::value)
        {
            if unlikely (std::get<2>(value)->isLargeTxn())
            {
                GET_METRIC(tiflash_raft_process_keys, type_large_txn_lock_del).Increment(1);
            }
        }
        map.erase(it);
        return delta;
    }
    else if (!quiet)
        throw Exception("Key not found", ErrorCodes::LOGICAL_ERROR);

    return {0, 0};
}

template <typename Trait>
bool RegionCFDataBase<Trait>::cmp(const Map & a, const Map & b)
{
    if (a.size() != b.size())
        return false;
    for (const auto & [key, value] : a)
    {
        if (auto it = b.find(key); it != b.end())
        {
            if (getTiKVKey(value) != getTiKVKey(it->second) || getTiKVValue(value) != getTiKVValue(it->second))
                return false;
        }
        else
            return false;
    }
    return true;
}

template <typename Trait>
bool RegionCFDataBase<Trait>::operator==(const RegionCFDataBase & cf) const
{
    return cmp(cf.data, data);
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::getSize() const
{
    return data.size();
}

template <typename Trait>
RegionCFDataBase<Trait>::RegionCFDataBase(RegionCFDataBase && region)
    : data(std::move(region.data))
{}

template <typename Trait>
RegionCFDataBase<Trait> & RegionCFDataBase<Trait>::operator=(RegionCFDataBase && region)
{
    data = std::move(region.data);
    return *this;
}

template <typename Trait>
std::string getTraitName()
{
    if constexpr (std::is_same_v<Trait, RegionWriteCFDataTrait>)
    {
        return "write";
    }
    else if constexpr (std::is_same_v<Trait, RegionDefaultCFDataTrait>)
    {
        return "default";
    }
    else if constexpr (std::is_same_v<Trait, RegionLockCFDataTrait>)
    {
        return "lock";
    }
    else
    {
        return "unknown";
    }
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::mergeFrom(const RegionCFDataBase & ori_region_data)
{
    RegionDataRes res;

    const auto & ori_map = ori_region_data.data;
    auto & tar_map = data;

    size_t ori_key_count = ori_region_data.getSize();
    for (auto it = ori_map.begin(); it != ori_map.end(); it++)
    {
        res.add(calcTotalKVSize(it->second));
        auto ok = tar_map.emplace(*it).second;
        if (!ok)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{}: got duplicate key {}, cf={}, ori_key_count={}",
                __PRETTY_FUNCTION__,
                getTiKVKey(it->second).toDebugString(),
                getTraitName<Trait>(),
                ori_key_count);
    }

    return res;
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::splitInto(const RegionRange & range, RegionCFDataBase & new_region_data)
{
    const auto & [start_key, end_key] = range;
    RegionDataRes res;

    {
        auto & ori_map = data;
        auto & tar_map = new_region_data.data;

        for (auto it = ori_map.begin(); it != ori_map.end();)
        {
            const auto & key = getTiKVKey(it->second);

            if (start_key.compare(key) <= 0 && end_key.compare(key) > 0)
            {
                res.sub(calcTotalKVSize(it->second));
                tar_map.insert(std::move(*it));
                it = ori_map.erase(it);
            }
            else
                ++it;
        }
    }
    return res;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::serialize(WriteBuffer & buf) const
{
    size_t total_size = 0;
    size_t size = getSize();
    total_size += writeBinary2(size, buf);

    for (const auto & ele : data)
    {
        const auto & key = getTiKVKey(ele.second);
        const auto & value = getTiKVValue(ele.second);
        total_size += key.serialize(buf);
        total_size += value.serialize(buf);
    }
    return total_size;
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data)
{
    auto size = readBinary2<size_t>(buf);
    size_t cf_data_size = 0;
    size_t decoded_data_size = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        auto r = new_region_data.insert(std::move(key), std::move(value));
        cf_data_size += r.payload;
        decoded_data_size += r.decoded;
    }
    return {cf_data_size, decoded_data_size};
}

template <typename Trait>
const typename RegionCFDataBase<Trait>::Data & RegionCFDataBase<Trait>::getData() const
{
    return data;
}

template <typename Trait>
typename RegionCFDataBase<Trait>::Data & RegionCFDataBase<Trait>::getDataMut()
{
    return data;
}

template struct RegionCFDataBase<RegionWriteCFDataTrait>;
template struct RegionCFDataBase<RegionDefaultCFDataTrait>;
template struct RegionCFDataBase<RegionLockCFDataTrait>;
} // namespace DB
