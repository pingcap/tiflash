#include "region_helper.h"

#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/TiKVHelper.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <limits>

using namespace DB;

inline bool checkTableInvolveRange(const TableID table_id, const std::pair<TiKVKey, TiKVKey> & range)
{
    const TiKVKey start_key = RecordKVFormat::genKey(table_id, std::numeric_limits<HandleID>::min());
    const TiKVKey end_key = RecordKVFormat::genKey(table_id, std::numeric_limits<HandleID>::max());
    if (end_key < range.first || (!range.second.empty() && start_key >= range.second))
        return false;
    return true;
}

inline TiKVKey genIndex(const TableID tableId, const Int64 id)
{
    String key(19, 0);
    memcpy(key.data(), &RecordKVFormat::TABLE_PREFIX, 1);
    auto big_endian_table_id = RecordKVFormat::encodeInt64(tableId);
    memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id), 8);
    memcpy(key.data() + 1 + 8, "_i", 2);
    auto big_endian_handle_id = RecordKVFormat::encodeInt64(id);
    memcpy(key.data() + 1 + 8 + 2, reinterpret_cast<const char *>(&big_endian_handle_id), 8);
    return RecordKVFormat::encodeAsTiKVKey(key);
}

int main(int, char **)
{
    bool res = true;
    {
        ASSERT_CHECK(RecordKVFormat::genKey(100, 2) < RecordKVFormat::genKey(100, 3), res);
        ASSERT_CHECK(RecordKVFormat::genKey(100, 2) < RecordKVFormat::genKey(101, 2), res);
        ASSERT_CHECK(RecordKVFormat::genKey(100, 2) <= RecordKVFormat::genKey(100, 2), res);
        ASSERT_CHECK(RecordKVFormat::genKey(100, 2) <= RecordKVFormat::genKey(100, 2, 233), res);
        ASSERT_CHECK(RecordKVFormat::genKey(100, 2) < RecordKVFormat::genKey(100, 3, 233), res);
        ASSERT_CHECK(RecordKVFormat::genKey(100, 3) > RecordKVFormat::genKey(100, 2, 233), res);
        ASSERT_CHECK(RecordKVFormat::genKey(100, 2, 2) < RecordKVFormat::genKey(100, 3), res);
    }

    {
        auto key = RecordKVFormat::genKey(2222, 123, 992134);
        ASSERT_CHECK(2222 == RecordKVFormat::getTableId(key), res);
        ASSERT_CHECK(123 == RecordKVFormat::getHandle(key), res);
        ASSERT_CHECK(992134 == RecordKVFormat::getTs(key), res);

        auto bare_key = RecordKVFormat::truncateTs(key);
        ASSERT_CHECK(key == RecordKVFormat::appendTs(bare_key, 992134), res);
    }

    {
        auto lock_value
            = RecordKVFormat::encodeLockCfValue(Region::PutFlag, "primary key", 421321, std::numeric_limits<UInt64>::max(), "value");
        auto [lock_type, primary, ts, ttl, short_value] = RecordKVFormat::decodeLockCfValue(lock_value);
        assert(Region::PutFlag == lock_type);
        assert("primary key" == primary);
        assert(421321 == ts);
        assert(std::numeric_limits<UInt64>::max() == ttl);
        assert("value" == *short_value);
    }

    {
        auto lock_value = RecordKVFormat::encodeLockCfValue(Region::PutFlag, "primary key", 421321, std::numeric_limits<UInt64>::max());
        auto [lock_type, primary, ts, ttl, short_value] = RecordKVFormat::decodeLockCfValue(lock_value);
        assert(Region::PutFlag == lock_type);
        assert("primary key" == primary);
        assert(421321 == ts);
        assert(std::numeric_limits<UInt64>::max() == ttl);
        assert(nullptr == short_value);
    }

    {
        auto write_value = RecordKVFormat::encodeWriteCfValue(Region::DelFlag, std::numeric_limits<UInt64>::max(), "value");
        auto [write_type, ts, short_value] = RecordKVFormat::decodeWriteCfValue(write_value);
        assert(Region::DelFlag == write_type);
        assert(std::numeric_limits<UInt64>::max() == ts);
        assert("value" == *short_value);
    }

    {
        auto write_value = RecordKVFormat::encodeWriteCfValue(Region::DelFlag, std::numeric_limits<UInt64>::max());
        auto [write_type, ts, short_value] = RecordKVFormat::decodeWriteCfValue(write_value);
        assert(Region::DelFlag == write_type);
        assert(std::numeric_limits<UInt64>::max() == ts);
        assert(nullptr == short_value);
    }

    {

        UInt64 a = 13241432453554;
        Crc32 crc32;
        crc32.put(&a, sizeof(a));
        assert(crc32.checkSum() == 3312221216);
    }

    {
        TiKVKey start_key = RecordKVFormat::genKey(200, 123);
        TiKVKey end_key = RecordKVFormat::genKey(300, 124);

        assert(checkTableInvolveRange(200, std::make_pair(start_key, end_key)));
        assert(checkTableInvolveRange(250, std::make_pair(start_key, end_key)));
        assert(checkTableInvolveRange(300, std::make_pair(start_key, end_key)));
        assert(!checkTableInvolveRange(400, std::make_pair(start_key, end_key)));
    }
    {
        TiKVKey start_key = RecordKVFormat::genKey(200, std::numeric_limits<HandleID>::min());
        TiKVKey end_key = RecordKVFormat::genKey(200, 100);

        assert(checkTableInvolveRange(200, std::make_pair(start_key, end_key)));
        assert(!checkTableInvolveRange(100, std::make_pair(start_key, end_key)));
    }
    {
        TiKVKey start_key;
        TiKVKey end_key;

        assert(checkTableInvolveRange(200, std::make_pair(start_key, end_key)));
        assert(checkTableInvolveRange(250, std::make_pair(start_key, end_key)));
        assert(checkTableInvolveRange(300, std::make_pair(start_key, end_key)));
        assert(checkTableInvolveRange(400, std::make_pair(start_key, end_key)));
    }

    {
        TiKVKey start_key = genIndex(233, 111);
        TiKVKey end_key = RecordKVFormat::genKey(300, 124);
        auto begin = TiKVRange::getRangeHandle<true>(start_key, 233);
        auto end = TiKVRange::getRangeHandle<false>(end_key, 233);
        assert(begin == begin.normal_min);
        assert(end == end.max);
    }

    {
        TiKVKey start_key = genIndex(233, 111);
        TiKVKey end_key = RecordKVFormat::genKey(300, 124);
        auto begin = TiKVRange::getRangeHandle<true>(start_key, 300);
        auto end = TiKVRange::getRangeHandle<false>(end_key, 300);
        assert(begin == begin.normal_min);
        assert(Int64{124} == end);
    }

    {
        using HandleInt64 = TiKVHandle::Handle<Int64>;
        Int64 int64_min = std::numeric_limits<Int64>::min();
        Int64 int64_max = std::numeric_limits<Int64>::max();
        assert(HandleInt64(int64_min) < HandleInt64(int64_max));
        assert(HandleInt64(int64_min) <= HandleInt64(int64_max));
        assert(HandleInt64(int64_max) > HandleInt64(int64_min));
        assert(HandleInt64(int64_max) >= HandleInt64(int64_min));
        assert(HandleInt64(int64_min) == HandleInt64(int64_min));
        assert(HandleInt64(int64_max) == HandleInt64(int64_max));

        assert(int64_min < HandleInt64(int64_max));
        assert(int64_min <= HandleInt64(int64_max));
        assert(int64_max > HandleInt64(int64_min));
        assert(int64_max >= HandleInt64(int64_min));
        assert(int64_min == HandleInt64(int64_min));
        assert(int64_max == HandleInt64(int64_max));

        assert(int64_max < HandleInt64::max);
        assert(int64_max <= HandleInt64::max);

        assert(HandleInt64::max > int64_max);
        assert(HandleInt64::max >= int64_max);

        assert(HandleInt64::max == HandleInt64::max);
    }

    {
        assert(TiKVRange::getRangeHandle<true>(TiKVKey(""), 1000) == TiKVRange::Handle::normal_min);
        assert(TiKVRange::getRangeHandle<false>(TiKVKey(""), 1000) == TiKVRange::Handle::max);
    }

    {
        TiKVKey start_key = RecordKVFormat::genKey(123, std::numeric_limits<Int64>::min());
        TiKVKey end_key = RecordKVFormat::genKey(123, std::numeric_limits<Int64>::max());
        assert(TiKVRange::getRangeHandle<true>(start_key, 123) == TiKVRange::Handle(std::numeric_limits<Int64>::min()));
        assert(TiKVRange::getRangeHandle<false>(end_key, 123) == TiKVRange::Handle(std::numeric_limits<Int64>::max()));

        assert(TiKVRange::getRangeHandle<true>(start_key, 123) >= TiKVRange::Handle::normal_min);
        assert(TiKVRange::getRangeHandle<false>(end_key, 123) < TiKVRange::Handle::max);

        start_key = RecordKVFormat::encodeAsTiKVKey(RecordKVFormat::decodeTiKVKey(start_key) + "123");
        assert(TiKVRange::getRangeHandle<true>(start_key, 123) == TiKVRange::Handle(std::numeric_limits<Int64>::min() + 1));
        assert(RecordKVFormat::genKey(123, std::numeric_limits<Int64>::min() + 2) >= start_key);
        assert(RecordKVFormat::genKey(123, std::numeric_limits<Int64>::min()) < start_key);

        end_key = RecordKVFormat::encodeAsTiKVKey(RecordKVFormat::decodeTiKVKey(end_key) + "123");
        assert(TiKVRange::getRangeHandle<false>(end_key, 123) == TiKVRange::Handle::max);

        auto s = RecordKVFormat::genRawKey(123, -1);
        s.resize(17);
        assert(s.size() == 17);
        start_key = RecordKVFormat::encodeAsTiKVKey(s);
        auto o1 = TiKVRange::getRangeHandle<true>(start_key, 123);

        s = RecordKVFormat::genRawKey(123, -1);
        s[17] = s[18] = 0;
        assert(s.size() == 19);
        auto o2 = RecordKVFormat::getHandle(s);
        assert(o2 == o1);
    }

    {
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({TiKVRange::Handle::normal_min, TiKVRange::Handle::normal_min});
        assert(n == 1);
        assert(new_range[0].first == new_range[0].second);
    }

    {
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({TiKVRange::Handle::max, TiKVRange::Handle::max});
        assert(n == 1);
        assert(new_range[0].first == new_range[0].second);
    }

    {
        // 100000... , -1 (111111...), 0, 011111... ==> 0 ~ 111111...
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({TiKVRange::Handle::normal_min, TiKVRange::Handle::max});
        assert(n == 1);
        assert(CHTableHandle::UInt64TableHandle::normal_min == new_range[0].first);
        assert(CHTableHandle::UInt64TableHandle::max == new_range[0].second);
    }

    {
        // 100000... , 111111...
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({TiKVRange::Handle::normal_min, -1});
        assert(n == 1);
        assert(CHTableHandle::UInt64TableHandle{static_cast<UInt64>(TiKVRange::Handle::normal_min.handle_id)} == new_range[0].first);
        assert(CHTableHandle::UInt64TableHandle{static_cast<UInt64>(-1)} == new_range[0].second);
        assert((new_range[0].second.handle_id - new_range[0].first.handle_id) == UInt64(-1 - TiKVRange::Handle::normal_min.handle_id));
    }

    {
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({{2333ll}, {2334ll}});
        assert(n == 1);
        assert(UInt64{2333} == new_range[0].first);
        assert(UInt64{2334} == new_range[0].second);
    }

    {
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({-1, 10});
        assert(n == 2);

        assert(UInt64{0} == new_range[0].first);
        assert(UInt64{10} == new_range[0].second);

        assert(CHTableHandle::UInt64TableHandle{static_cast<UInt64>(-1)} == new_range[1].first);
        assert(CHTableHandle::UInt64TableHandle::max == new_range[1].second);
    }

    return res ? 0 : 1;
}
