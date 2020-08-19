#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TiKVHelper.h>
#include <Storages/Transaction/TiKVRange.h>
#include <gtest/gtest.h>

#include "region_helper.h"

using namespace DB;

using RangeRef = std::pair<const TiKVKey &, const TiKVKey &>;

inline bool checkTableInvolveRange(const TableID table_id, const RangeRef & range)
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

TEST(TiKVKeyValue_test, PortedTests)
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
        auto [lock_type, primary, ts, ttl, min_commit_ts] = RecordKVFormat::decodeLockCfValue(lock_value);
        ASSERT_TRUE(Region::PutFlag == lock_type);
        ASSERT_TRUE("primary key" == primary);
        ASSERT_TRUE(421321 == ts);
        ASSERT_TRUE(std::numeric_limits<UInt64>::max() == ttl);
        ASSERT_TRUE(0 == min_commit_ts);
    }

    {
        auto lock_value = RecordKVFormat::encodeLockCfValue(Region::PutFlag, "primary key", 421321, std::numeric_limits<UInt64>::max());
        auto [lock_type, primary, ts, ttl, min_commit_ts] = RecordKVFormat::decodeLockCfValue(lock_value);
        ASSERT_TRUE(Region::PutFlag == lock_type);
        ASSERT_TRUE("primary key" == primary);
        ASSERT_TRUE(421321 == ts);
        ASSERT_TRUE(std::numeric_limits<UInt64>::max() == ttl);
        ASSERT_TRUE(0 == min_commit_ts);
    }

    {
        auto write_value = RecordKVFormat::encodeWriteCfValue(Region::DelFlag, std::numeric_limits<UInt64>::max(), "value");
        auto [write_type, ts, short_value] = RecordKVFormat::decodeWriteCfValue(write_value);
        ASSERT_TRUE(Region::DelFlag == write_type);
        ASSERT_TRUE(std::numeric_limits<UInt64>::max() == ts);
        ASSERT_TRUE("value" == *short_value);
    }

    {
        auto write_value = RecordKVFormat::encodeWriteCfValue(Region::DelFlag, std::numeric_limits<UInt64>::max());
        auto [write_type, ts, short_value] = RecordKVFormat::decodeWriteCfValue(write_value);
        ASSERT_TRUE(Region::DelFlag == write_type);
        ASSERT_TRUE(std::numeric_limits<UInt64>::max() == ts);
        ASSERT_TRUE(nullptr == short_value);
    }

    {

        UInt64 a = 13241432453554;
        Crc32 crc32;
        crc32.put(&a, sizeof(a));
        ASSERT_TRUE(crc32.checkSum() == 3312221216);
    }

    {
        TiKVKey start_key = RecordKVFormat::genKey(200, 123);
        TiKVKey end_key = RecordKVFormat::genKey(300, 124);

        ASSERT_TRUE(checkTableInvolveRange(200, RangeRef{start_key, end_key}));
        ASSERT_TRUE(checkTableInvolveRange(250, RangeRef{start_key, end_key}));
        ASSERT_TRUE(checkTableInvolveRange(300, RangeRef{start_key, end_key}));
        ASSERT_TRUE(!checkTableInvolveRange(400, RangeRef{start_key, end_key}));
    }
    {
        TiKVKey start_key = RecordKVFormat::genKey(200, std::numeric_limits<HandleID>::min());
        TiKVKey end_key = RecordKVFormat::genKey(200, 100);

        ASSERT_TRUE(checkTableInvolveRange(200, RangeRef{start_key, end_key}));
        ASSERT_TRUE(!checkTableInvolveRange(100, RangeRef{start_key, end_key}));
    }
    {
        TiKVKey start_key;
        TiKVKey end_key;

        ASSERT_TRUE(checkTableInvolveRange(200, RangeRef{start_key, end_key}));
        ASSERT_TRUE(checkTableInvolveRange(250, RangeRef{start_key, end_key}));
        ASSERT_TRUE(checkTableInvolveRange(300, RangeRef{start_key, end_key}));
        ASSERT_TRUE(checkTableInvolveRange(400, RangeRef{start_key, end_key}));
    }

    {
        TiKVKey start_key = genIndex(233, 111);
        TiKVKey end_key = RecordKVFormat::genKey(300, 124);
        auto begin = TiKVRange::getRangeHandle<true>(start_key, 233);
        auto end = TiKVRange::getRangeHandle<false>(end_key, 233);
        ASSERT_TRUE(begin == begin.normal_min);
        ASSERT_TRUE(end == end.max);
    }

    {
        TiKVKey start_key = genIndex(233, 111);
        TiKVKey end_key = RecordKVFormat::genKey(300, 124);
        auto begin = TiKVRange::getRangeHandle<true>(start_key, 300);
        auto end = TiKVRange::getRangeHandle<false>(end_key, 300);
        ASSERT_TRUE(begin == begin.normal_min);
        ASSERT_TRUE(Int64{124} == end);
    }

    {
        using HandleInt64 = TiKVHandle::Handle<Int64>;
        Int64 int64_min = std::numeric_limits<Int64>::min();
        Int64 int64_max = std::numeric_limits<Int64>::max();
        ASSERT_TRUE(HandleInt64(int64_min) < HandleInt64(int64_max));
        ASSERT_TRUE(HandleInt64(int64_min) <= HandleInt64(int64_max));
        ASSERT_TRUE(HandleInt64(int64_max) > HandleInt64(int64_min));
        ASSERT_TRUE(HandleInt64(int64_max) >= HandleInt64(int64_min));
        ASSERT_TRUE(HandleInt64(int64_min) == HandleInt64(int64_min));
        ASSERT_TRUE(HandleInt64(int64_max) == HandleInt64(int64_max));

        ASSERT_TRUE(int64_min < HandleInt64(int64_max));
        ASSERT_TRUE(int64_min <= HandleInt64(int64_max));
        ASSERT_TRUE(int64_max > HandleInt64(int64_min));
        ASSERT_TRUE(int64_max >= HandleInt64(int64_min));
        ASSERT_TRUE(int64_min == HandleInt64(int64_min));
        ASSERT_TRUE(int64_max == HandleInt64(int64_max));

        ASSERT_TRUE(int64_max < HandleInt64::max);
        ASSERT_TRUE(int64_max <= HandleInt64::max);

        ASSERT_TRUE(HandleInt64::max > int64_max);
        ASSERT_TRUE(HandleInt64::max >= int64_max);

        ASSERT_TRUE(HandleInt64::max == HandleInt64::max);
    }

    {
        ASSERT_TRUE(TiKVRange::getRangeHandle<true>(TiKVKey(""), 1000) == TiKVRange::Handle::normal_min);
        ASSERT_TRUE(TiKVRange::getRangeHandle<false>(TiKVKey(""), 1000) == TiKVRange::Handle::max);
    }

    {
        TiKVKey start_key = RecordKVFormat::genKey(123, std::numeric_limits<Int64>::min());
        TiKVKey end_key = RecordKVFormat::genKey(123, std::numeric_limits<Int64>::max());
        ASSERT_TRUE(TiKVRange::getRangeHandle<true>(start_key, 123) == TiKVRange::Handle(std::numeric_limits<Int64>::min()));
        ASSERT_TRUE(TiKVRange::getRangeHandle<false>(end_key, 123) == TiKVRange::Handle(std::numeric_limits<Int64>::max()));

        ASSERT_TRUE(TiKVRange::getRangeHandle<true>(start_key, 123) >= TiKVRange::Handle::normal_min);
        ASSERT_TRUE(TiKVRange::getRangeHandle<false>(end_key, 123) < TiKVRange::Handle::max);

        start_key = RecordKVFormat::encodeAsTiKVKey(RecordKVFormat::decodeTiKVKey(start_key) + "123");
        ASSERT_TRUE(TiKVRange::getRangeHandle<true>(start_key, 123) == TiKVRange::Handle(std::numeric_limits<Int64>::min() + 1));
        ASSERT_TRUE(RecordKVFormat::genKey(123, std::numeric_limits<Int64>::min() + 2) >= start_key);
        ASSERT_TRUE(RecordKVFormat::genKey(123, std::numeric_limits<Int64>::min()) < start_key);

        end_key = RecordKVFormat::encodeAsTiKVKey(RecordKVFormat::decodeTiKVKey(end_key) + "123");
        ASSERT_TRUE(TiKVRange::getRangeHandle<false>(end_key, 123) == TiKVRange::Handle::max);

        auto s = RecordKVFormat::genRawKey(123, -1);
        s.resize(17);
        ASSERT_TRUE(s.size() == 17);
        start_key = RecordKVFormat::encodeAsTiKVKey(s);
        auto o1 = TiKVRange::getRangeHandle<true>(start_key, 123);

        s = RecordKVFormat::genRawKey(123, -1);
        s[17] = s[18] = 0;
        ASSERT_TRUE(s.size() == 19);
        auto o2 = RecordKVFormat::getHandle(s);
        ASSERT_TRUE(o2 == o1);
    }

    {
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({TiKVRange::Handle::normal_min, TiKVRange::Handle::normal_min});
        ASSERT_TRUE(n == 1);
        ASSERT_TRUE(new_range[0].first == new_range[0].second);
    }

    {
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({TiKVRange::Handle::max, TiKVRange::Handle::max});
        ASSERT_TRUE(n == 1);
        ASSERT_TRUE(new_range[0].first == new_range[0].second);
    }

    {
        // 100000... , -1 (111111...), 0, 011111... ==> 0 ~ 111111...
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({TiKVRange::Handle::normal_min, TiKVRange::Handle::max});
        ASSERT_TRUE(n == 1);
        ASSERT_TRUE(CHTableHandle::UInt64TableHandle::normal_min == new_range[0].first);
        ASSERT_TRUE(CHTableHandle::UInt64TableHandle::max == new_range[0].second);
    }

    {
        // 100000... , 111111...
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({TiKVRange::Handle::normal_min, -1});
        ASSERT_TRUE(n == 1);
        ASSERT_TRUE(CHTableHandle::UInt64TableHandle{static_cast<UInt64>(TiKVRange::Handle::normal_min.handle_id)} == new_range[0].first);
        ASSERT_TRUE(CHTableHandle::UInt64TableHandle{static_cast<UInt64>(-1)} == new_range[0].second);
        ASSERT_TRUE((new_range[0].second.handle_id - new_range[0].first.handle_id) == UInt64(-1 - TiKVRange::Handle::normal_min.handle_id));
    }

    {
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({{2333ll}, {2334ll}});
        ASSERT_TRUE(n == 1);
        ASSERT_TRUE(UInt64{2333} == new_range[0].first);
        ASSERT_TRUE(UInt64{2334} == new_range[0].second);
    }

    {
        auto [n, new_range] = CHTableHandle::splitForUInt64TableHandle({-1, 10});
        ASSERT_TRUE(n == 2);

        ASSERT_TRUE(UInt64{0} == new_range[0].first);
        ASSERT_TRUE(UInt64{10} == new_range[0].second);

        ASSERT_TRUE(CHTableHandle::UInt64TableHandle{static_cast<UInt64>(-1)} == new_range[1].first);
        ASSERT_TRUE(CHTableHandle::UInt64TableHandle::max == new_range[1].second);
    }

    {
        std::string s = "1234";
        s[0] = char(1);
        s[3] = char(111);
        const auto & key = TiKVKey(s.data(), s.size());
        ASSERT_EQ(key.toHex(), "[0132336F]");
    }

    {
        std::string s(12, 1);
        s[8] = s[9] = s[10] = 0;
        ASSERT_TRUE(RecordKVFormat::checkKeyPaddingValid(s.data() + 1, 1));
        ASSERT_TRUE(RecordKVFormat::checkKeyPaddingValid(s.data() + 2, 2));
        ASSERT_TRUE(RecordKVFormat::checkKeyPaddingValid(s.data() + 3, 3));
        for (auto i = 1; i <= 8; ++i)
            ASSERT_TRUE(!RecordKVFormat::checkKeyPaddingValid(s.data() + 4, i));
    }

    {
        RegionRangeKeys range(RecordKVFormat::genKey(1, 2, 3), RecordKVFormat::genKey(2, 4, 100));
        ASSERT_TRUE(RecordKVFormat::getTs(range.comparableKeys().first.key) == 3);
        ASSERT_TRUE(RecordKVFormat::getTs(range.comparableKeys().second.key) == 100);
        ASSERT_TRUE(RecordKVFormat::getTableId(range.rawKeys().first) == 1);
        ASSERT_TRUE(RecordKVFormat::getTableId(range.rawKeys().second) == 2);
        ASSERT_TRUE(RecordKVFormat::getHandle(range.rawKeys().first) == 2);
        ASSERT_TRUE(RecordKVFormat::getHandle(range.rawKeys().second) == 4);

        ASSERT_EQ(range.comparableKeys().first.state, TiKVRangeKey::NORMAL);
        ASSERT_EQ(range.comparableKeys().second.state, TiKVRangeKey::NORMAL);

        auto range2 = RegionRangeKeys::makeComparableKeys(TiKVKey{}, TiKVKey{});
        ASSERT_TRUE(range2.first.state == TiKVRangeKey::MIN);
        ASSERT_TRUE(range2.second.state == TiKVRangeKey::MAX);

        ASSERT_TRUE(range2.first.compare(range2.second) < 0);
        ASSERT_TRUE(range2.first.compare(range.comparableKeys().second) < 0);
        ASSERT_TRUE(range.comparableKeys().first.compare(range.comparableKeys().second) < 0);
        ASSERT_TRUE(range.comparableKeys().second.compare(range2.second) < 0);

        ASSERT_TRUE(range.comparableKeys().first.compare(RecordKVFormat::genKey(1, 2, 3)) == 0);
    }

    {
        const Int64 tableId = 2333;
        const Timestamp ts = 66666;
        std::string key(RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, 0);
        memcpy(key.data(), &RecordKVFormat::TABLE_PREFIX, 1);
        auto big_endian_table_id = RecordKVFormat::encodeInt64(tableId);
        memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id), 8);
        memcpy(key.data() + 1 + 8, RecordKVFormat::RECORD_PREFIX_SEP, 2);
        std::string pk = "12345678...";
        key += pk;
        auto tikv_key = RecordKVFormat::encodeAsTiKVKey(key);
        RecordKVFormat::appendTs(tikv_key, ts);
        {
            auto decoded_key = RecordKVFormat::decodeTiKVKey(tikv_key);
            ASSERT_EQ(RecordKVFormat::getTableId(decoded_key), tableId);
            auto tidb_pk = RecordKVFormat::getRawTiDBPK(decoded_key);
            ASSERT_EQ(*tidb_pk, pk);
        }
    }

    ASSERT_TRUE(res);
}

TEST(TiKVKeyValue_test, ToHex)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wc++11-narrowing"
    std::string key{
        0x74, 0x80, 0x00, 0x00, //
        0x00, 0x00, 0x00, 0x00, //
        0x35, 0x5f, 0x72, 0xff, //
        0xff, 0xff, 0xff, 0xff, //
        0xff, 0xff, 0xff, 0x00, //
    };
#pragma GCC diagnostic pop
    auto hex_key = DB::ToHex(key.data(), key.size());
    EXPECT_EQ(hex_key.size(), 2 + 40UL);
    EXPECT_EQ(hex_key, "[7480000000000000355F72FFFFFFFFFFFFFFFF00]");
}

namespace
{

// In python, we can convert a test case from `s`
// 'range = parseTestCase({{{}}});\nASSERT_EQ(range, expected_range);'.format(','.join(map(lambda x: '{{{}}}'.format(','.join(map(lambda y: '0x{:02x}'.format(int(y, 16)), x.strip('[').strip(']').split()))), s.split(','))))

HandleRange<HandleID> parseTestCase(std::vector<std::vector<u_char>> && seq)
{
    std::string start_key_s, end_key_s;
    for (const auto ch : seq[0])
        start_key_s += ch;
    for (const auto ch : seq[1])
        end_key_s += ch;
    RegionRangeKeys range{RecordKVFormat::encodeAsTiKVKey(start_key_s), RecordKVFormat::encodeAsTiKVKey(end_key_s)};
    return range.getHandleRangeByTable(45);
}

HandleRange<HandleID> parseTestCase2(std::vector<std::vector<u_char>> && seq)
{
    std::string start_key_s, end_key_s;
    for (const auto ch : seq[0])
        start_key_s += ch;
    for (const auto ch : seq[1])
        end_key_s += ch;
    RegionRangeKeys range{TiKVKey::copyFrom(start_key_s), TiKVKey::copyFrom(end_key_s)};
    return range.getHandleRangeByTable(45);
}

std::string rangeToString(const HandleRange<HandleID> & r)
{
    std::stringstream ss;
    ss << "[" << r.first.toString() << "," << r.second.toString() << ")";
    return ss.str();
}

} // namespace

TEST(RegionRange_test, DISABLED_GetHandleRangeByTableID)
try
{
    HandleRange<HandleID> range;
    HandleRange<HandleID> expected_range;

    // clang-format off
    range = parseTestCase({{0x74,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x2D,},{}});
    expected_range = {TiKVHandle::Handle<HandleID>::normal_min, TiKVHandle::Handle<HandleID>::max};
    EXPECT_EQ(range, expected_range) << rangeToString(range) << " <-> " << rangeToString(expected_range);

    range = parseTestCase({{0x74,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x2d,0x5f,0x69,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x01,0x03,0x80,0x00,0x00,0x00,0x00,0x5a,0x0f,0x00,0x03,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x02},{0x74,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x2d,0x5f,0x72,0x80,0x00,0x00,0x00,0x00,0x00,0xaa,0x40}});
    expected_range = {TiKVHandle::Handle<HandleID>::normal_min, 43584};
    EXPECT_EQ(range, expected_range) << rangeToString(range) << " <-> " << rangeToString(expected_range);

    range = parseTestCase({{0x74,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x2d,0x5f,0x72,0x80,0x00,0x00,0x00,0x00,0x00,0xaa,0x40},{0x74,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x2d,0x5f,0x72,0x80,0x00,0x00,0x00,0x00,0x02,0x21,0x40}});
    expected_range = {43584, 139584};
    EXPECT_EQ(range, expected_range) << rangeToString(range) << " <-> " << rangeToString(expected_range);

    range = parseTestCase({{0x74,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x2d,0x5f,0x72,0x80,0x00,0x00,0x00,0x00,0x10,0xc7,0x40},{0x74,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x2d,0x5f,0x72,0x80,0x00,0x00,0x00,0x00,0x12,0x3e,0x40}});
    expected_range = {1099584, 1195584};
    EXPECT_EQ(range, expected_range) << rangeToString(range) << " <-> " << rangeToString(expected_range);


    // [74 80 0 0 0 0 0 0 ff 2d 5f 69 80 0 0 0 0 ff 0 0 1 3 80 0 0 0 ff 0 5a cf 64 3 80 0 0 ff 0 0 0 0 2 0 0 0 fc],[74 80 0 0 0 0 0 0 ff 2d 5f 72 80 0 0 0 0 ff 0 b8 b 0 0 0 0 0 fa]
    range = parseTestCase2({{0x74,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0xff,0x2d,0x5f,0x69,0x80,0x00,0x00,0x00,0x00,0xff,0x00,0x00,0x01,0x03,0x80,0x00,0x00,0x00,0xff,0x00,0x5a,0xcf,0x64,0x03,0x80,0x00,0x00,0xff,0x00,0x00,0x00,0x00,0x02,0x00,0x00,0x00,0xfc},{0x74,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0xff,0x2d,0x5f,0x72,0x80,0x00,0x00,0x00,0x00,0xff,0x00,0xb8,0x0b,0x00,0x00,0x00,0x00,0x00,0xfa}});
    expected_range = {TiKVHandle::Handle<HandleID>::normal_min, 47115};
    EXPECT_EQ(range, expected_range) << rangeToString(range) << " <-> " << rangeToString(expected_range);

    // clang-format on
}
catch (const Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl;

    throw;
}
