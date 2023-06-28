// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/Transaction/RegionCFDataBase.h>
#include <Storages/Transaction/RegionCFDataTrait.h>
#include <Storages/Transaction/RegionData.h>
#include <Storages/Transaction/RegionRangeKeys.h>

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
    const auto & raw_key = RecordKVFormat::decodeTiKVKey(key);
    auto kv_pair = Trait::genKVPair(std::move(key), raw_key, std::move(value));

    if (!kv_pair)
        return 0;

    return insert(std::move(*kv_pair), mode);
}

template <>
RegionDataRes RegionCFDataBase<RegionLockCFDataTrait>::insert(TiKVKey && key, TiKVValue && value, DupCheck mode)
{
    UNUSED(mode);
    Pair kv_pair = RegionLockCFDataTrait::genKVPair(std::move(key), std::move(value));
    // according to the process of pessimistic lock, just overwrite.
    data.insert_or_assign(std::move(kv_pair.first), std::move(kv_pair.second));
    return 0;
}

template <typename Trait>
RegionDataRes RegionCFDataBase<Trait>::insert(std::pair<Key, Value> && kv_pair, DupCheck mode)
{
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
            throw Exception("Found existing key in hex: " + getTiKVKey(it->second).toDebugString(), ErrorCodes::LOGICAL_ERROR);
        }
        else if (mode == DupCheck::AllowSame)
        {
            if (prev_value != getTiKVValue(it->second))
            {
                throw Exception("Found existing key in hex and val differs: "
                                    + getTiKVKey(it->second).toDebugString()
                                    + " prev_val: " + getTiKVValue(it->second).toDebugString()
                                    + " new_val: " + prev_value.toDebugString(),
                                ErrorCodes::LOGICAL_ERROR);
            }
        }
        else
        {
            throw Exception("Found existing key in hex: " + getTiKVKey(it->second).toDebugString(), ErrorCodes::LOGICAL_ERROR);
        }
    }

    return calcTiKVKeyValueSize(it->second);
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const Value & value)
{
    return calcTiKVKeyValueSize(getTiKVKey(value), getTiKVValue(value));
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value)
{
    if constexpr (std::is_same<Trait, RegionLockCFDataTrait>::value)
    {
        std::ignore = key;
        std::ignore = value;
        return 0;
    }
    else
        return key.dataSize() + value.dataSize();
}


template <typename Trait>
bool RegionCFDataBase<Trait>::shouldIgnoreRemove(const RegionCFDataBase::Value &)
{
    return false;
}

template <>
bool RegionCFDataBase<RegionWriteCFDataTrait>::shouldIgnoreRemove(const RegionCFDataBase::Value & value)
{
    return RegionWriteCFDataTrait::getWriteType(value) == CFModifyFlag::DelFlag;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::remove(const Key & key, bool quiet)
{
    auto & map = data;

    if (auto it = map.find(key); it != map.end())
    {
        const Value & value = it->second;

        if (shouldIgnoreRemove(value))
            return 0;

        size_t size = calcTiKVKeyValueSize(value);
        map.erase(it);
        return size;
    }
    else if (!quiet)
        throw Exception("Key not found", ErrorCodes::LOGICAL_ERROR);

    return 0;
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
size_t RegionCFDataBase<Trait>::mergeFrom(const RegionCFDataBase & ori_region_data)
{
    size_t size_changed = 0;

    const auto & ori_map = ori_region_data.data;
    auto & tar_map = data;

    for (auto it = ori_map.begin(); it != ori_map.end(); it++)
    {
        size_changed += calcTiKVKeyValueSize(it->second);
        auto ok = tar_map.emplace(*it).second;
        if (!ok)
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": got duplicate key", ErrorCodes::LOGICAL_ERROR);
    }

    return size_changed;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::splitInto(const RegionRange & range, RegionCFDataBase & new_region_data)
{
    const auto & [start_key, end_key] = range;
    size_t size_changed = 0;

    {
        auto & ori_map = data;
        auto & tar_map = new_region_data.data;

        for (auto it = ori_map.begin(); it != ori_map.end();)
        {
            const auto & key = getTiKVKey(it->second);

            if (start_key.compare(key) <= 0 && end_key.compare(key) > 0)
            {
                size_changed += calcTiKVKeyValueSize(it->second);
                tar_map.insert(std::move(*it));
                it = ori_map.erase(it);
            }
            else
                ++it;
        }
    }
    return size_changed;
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
size_t RegionCFDataBase<Trait>::deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data)
{
    auto size = readBinary2<size_t>(buf);
    size_t cf_data_size = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        cf_data_size += new_region_data.insert(std::move(key), std::move(value));
    }
    return cf_data_size;
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
