#pragma once

#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <common/likely.h>

namespace DB
{

namespace TiKVRange
{

using Handle = TiKVHandle::Handle<HandleID>;

template <bool start, bool decoded = false, typename KeyType = TiKVKey>
inline Handle getRangeHandle(const KeyType & tikv_key, const TableID table_id)
{
    if constexpr (decoded)
        static_assert(std::is_same_v<KeyType, std::string>);
    else
        static_assert(std::is_same_v<KeyType, TiKVKey>);

    constexpr HandleID min = std::numeric_limits<HandleID>::min();
    constexpr HandleID max = std::numeric_limits<HandleID>::max();

    if (tikv_key.empty())
    {
        if constexpr (start)
            return Handle::normal_min;
        else
            return Handle::max;
    }

    const std::string * raw_key_ptr = nullptr;
    std::string decoded_raw_key;
    if constexpr (decoded)
        raw_key_ptr = &tikv_key;
    else
    {
        decoded_raw_key = RecordKVFormat::decodeTiKVKey(tikv_key);
        raw_key_ptr = &decoded_raw_key;
    }

    const std::string & key = *raw_key_ptr;

    if (key <= RecordKVFormat::genRawKey(table_id, min))
        return Handle::normal_min;
    if (key > RecordKVFormat::genRawKey(table_id, max))
        return Handle::max;

    if (likely(key.size() == RecordKVFormat::RAW_KEY_SIZE))
        return RecordKVFormat::getHandle(key);
    else if (key.size() < RecordKVFormat::RAW_KEY_SIZE)
    {
        UInt64 tmp = 0;
        memcpy(&tmp, key.data() + RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, key.size() - RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE);
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

inline HandleRange<HandleID> getHandleRangeByTable(const TiKVKey & start_key, const TiKVKey & end_key, TableID table_id)
{
    auto start_handle = getRangeHandle<true>(start_key, table_id);
    auto end_handle = getRangeHandle<false>(end_key, table_id);

    return {start_handle, end_handle};
}

inline HandleRange<HandleID> getHandleRangeByTable(const std::pair<TiKVKey, TiKVKey> & range, TableID table_id)
{
    return getHandleRangeByTable(range.first, range.second, table_id);
}

} // namespace TiKVRange

} // namespace DB
