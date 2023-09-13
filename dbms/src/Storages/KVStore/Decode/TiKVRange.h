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

#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Decode/TableRowIDMinMax.h>
#include <Storages/KVStore/Decode/TiKVHandle.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <common/likely.h>

namespace DB
{

namespace TiKVRange
{

using Handle = TiKVHandle::Handle<HandleID>;

template <bool start>
inline Handle getRangeHandle(const DecodedTiKVKey & key, const TableID table_id)
{
    if (key.empty())
    {
        if constexpr (start)
            return Handle::normal_min;
        else
            return Handle::max;
    }

    const auto & table_min_max = TableRowIDMinMax::getMinMax(table_id);
    if (key <= table_min_max.handle_min)
        return Handle::normal_min;
    if (key > table_min_max.handle_max)
        return Handle::max;

    if (likely(key.size() == RecordKVFormat::RAW_KEY_SIZE))
        return RecordKVFormat::getHandle(key);
    else if (key.size() < RecordKVFormat::RAW_KEY_SIZE)
    {
        UInt64 tmp = 0;
        memcpy(
            &tmp,
            key.data() + RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE,
            key.size() - RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE);
        HandleID res = RecordKVFormat::decodeInt64(tmp);
        // the actual res is like `res - 0.x`
        return res;
    }
    else
    {
        HandleID res = RecordKVFormat::getHandle(key);
        // the actual res is like `res + 0.x`

        // this won't happen
        /*
        if (unlikely(res == max))
            return Handle::max;
        */
        return res + 1;
    }
}

template <bool start>
inline Handle getRangeHandle(const TiKVKey & tikv_key, const TableID table_id)
{
    if (tikv_key.empty())
    {
        if constexpr (start)
            return Handle::normal_min;
        else
            return Handle::max;
    }

    return getRangeHandle<start>(RecordKVFormat::decodeTiKVKey(tikv_key), table_id);
}

template <typename KeyType>
inline HandleRange<HandleID> getHandleRangeByTable(
    const KeyType & start_key,
    const KeyType & end_key,
    const TableID table_id)
{
    static_assert(std::is_same_v<KeyType, DecodedTiKVKey> || std::is_same_v<KeyType, TiKVKey>);

    auto start_handle = getRangeHandle<true>(start_key, table_id);
    auto end_handle = getRangeHandle<false>(end_key, table_id);

    return {start_handle, end_handle};
}

} // namespace TiKVRange

} // namespace DB
