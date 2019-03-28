#include "region_helper.h"

#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/TiKVHelper.h>
#include <limits>


using namespace DB;

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
        auto lock_value = RecordKVFormat::encodeLockCfValue(Region::PutFlag, "primary key", 421321, std::numeric_limits<UInt64>::max(), "value");
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

    auto key = RecordKVFormat::genKey(1222, 100023, 452321);
    std::cout << escapeString(key.toString()) << std::endl;
    // std::cout << escapeString(DataKVFormat::enc_start_key(key)) << std::endl;
    // std::cout << escapeString(DataKVFormat::enc_end_key(key)) << std::endl;
    // std::cout << escapeString(DataKVFormat::region_state_key(331231)) << std::endl;

    {

        UInt64 a = 13241432453554;
        Crc32 crc32;
        crc32.put(&a, sizeof(a));
        assert(crc32.checkSum() == 3312221216);
    }

    {
        TiKVKey start_key = RecordKVFormat::genKey(200, 123);
        TiKVKey end_key = RecordKVFormat::genKey(300, 124);

        assert(TiKVRange::checkTableInvolveRange(200, std::make_pair(start_key, end_key)));
        assert(TiKVRange::checkTableInvolveRange(250, std::make_pair(start_key, end_key)));
        assert(TiKVRange::checkTableInvolveRange(300, std::make_pair(start_key, end_key)));
        assert(!TiKVRange::checkTableInvolveRange(400, std::make_pair(start_key, end_key)));
    }
    {
        TiKVKey start_key = RecordKVFormat::genKey(200, std::numeric_limits<HandleID>::min());
        TiKVKey end_key = RecordKVFormat::genKey(200, 100);

        assert(TiKVRange::checkTableInvolveRange(200, std::make_pair(start_key, end_key)));
        assert(!TiKVRange::checkTableInvolveRange(100, std::make_pair(start_key, end_key)));
    }
    {
        TiKVKey start_key;
        TiKVKey end_key;

        assert(TiKVRange::checkTableInvolveRange(200, std::make_pair(start_key, end_key)));
        assert(TiKVRange::checkTableInvolveRange(250, std::make_pair(start_key, end_key)));
        assert(TiKVRange::checkTableInvolveRange(300, std::make_pair(start_key, end_key)));
        assert(TiKVRange::checkTableInvolveRange(400, std::make_pair(start_key, end_key)));
    }

    return res ? 0 : 1;
}
