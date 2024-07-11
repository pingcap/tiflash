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

#include <IO/VarInt.h>
#include <Storages/Transaction/CHTableHandle.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TiKVHelper.h>
#include <Storages/Transaction/TiKVRange.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <TestUtils/TiFlashTestBasic.h>

#include "region_helper.h"

namespace DB
{
TiKVValue encode_lock_cf_value(
    UInt8 lock_type,
    const String & primary,
    Timestamp ts,
    UInt64 ttl,
    const String * short_value,
    Timestamp min_commit_ts,
    Timestamp for_update_ts,
    uint64_t txn_size,
    const std::vector<std::string> & async_commit,
    const std::vector<uint64_t> & rollback)
{
    auto lock_value = RecordKVFormat::encodeLockCfValue(lock_type, primary, ts, ttl, short_value, min_commit_ts);
    WriteBufferFromOwnString res;
    res.write(lock_value.getStr().data(), lock_value.getStr().size());
    {
        res.write(RecordKVFormat::MIN_COMMIT_TS_PREFIX);
        RecordKVFormat::encodeUInt64(min_commit_ts, res);
    }
    {
        res.write(RecordKVFormat::FOR_UPDATE_TS_PREFIX);
        RecordKVFormat::encodeUInt64(for_update_ts, res);
    }
    {
        res.write(RecordKVFormat::TXN_SIZE_PREFIX);
        RecordKVFormat::encodeUInt64(txn_size, res);
    }
    {
        res.write(RecordKVFormat::ROLLBACK_TS_PREFIX);
        TiKV::writeVarUInt(rollback.size(), res);
        for (auto ts : rollback)
        {
            RecordKVFormat::encodeUInt64(ts, res);
        }
    }
    {
        res.write(RecordKVFormat::ASYNC_COMMIT_PREFIX);
        TiKV::writeVarUInt(async_commit.size(), res);
        for (const auto & s : async_commit)
        {
            writeVarInt(s.size(), res);
            res.write(s.data(), s.size());
        }
    }
    {
        res.write(RecordKVFormat::LAST_CHANGE_PREFIX);
        RecordKVFormat::encodeUInt64(12345678, res);
        TiKV::writeVarUInt(87654321, res);
    }
    {
        res.write(RecordKVFormat::TXN_SOURCE_PREFIX_FOR_LOCK);
        TiKV::writeVarUInt(876543, res);
    }
    {
        res.write(RecordKVFormat::PESSIMISTIC_LOCK_WITH_CONFLICT_PREFIX);
    }
    return TiKVValue(res.releaseStr());
}

} // namespace DB

using namespace DB;

using RangeRef = std::pair<const TiKVKey &, const TiKVKey &>;

inline bool checkTableInvolveRange(const TableID table_id, const RangeRef & range)
{
    const TiKVKey start_key = RecordKVFormat::genKey(table_id, std::numeric_limits<HandleID>::min());
    const TiKVKey end_key = RecordKVFormat::genKey(table_id, std::numeric_limits<HandleID>::max());
    return !(end_key < range.first || (!range.second.empty() && start_key >= range.second));
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

TEST(TiKVKeyValueTest, PortedTests)
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
        std::string shor_value = "value";
        auto lock_for_update_ts = 7777, txn_size = 1;
        const std::vector<std::string> & async_commit = {"s1", "s2"};
        const std::vector<uint64_t> & rollback = {3, 4};
        auto lock_value = encode_lock_cf_value(
            Region::DelFlag,
            "primary key",
            421321,
            std::numeric_limits<UInt64>::max(),
            &shor_value,
            66666,
            lock_for_update_ts,
            txn_size,
            async_commit,
            rollback);
        auto ori_key = std::make_shared<const TiKVKey>(RecordKVFormat::genKey(1, 88888));
        auto lock = RecordKVFormat::DecodedLockCFValue(ori_key, std::make_shared<TiKVValue>(std::move(lock_value)));
        {
            auto & lock_info = lock;
            ASSERT_TRUE(kvrpcpb::Op::Del == lock_info.lock_type);
            ASSERT_TRUE("primary key" == lock_info.primary_lock);
            ASSERT_TRUE(421321 == lock_info.lock_version);
            ASSERT_TRUE(std::numeric_limits<UInt64>::max() == lock_info.lock_ttl);
            ASSERT_TRUE(66666 == lock_info.min_commit_ts);
            ASSERT_TRUE(ori_key == lock_info.key);
            ASSERT_EQ(lock_for_update_ts, lock_info.lock_for_update_ts);
            ASSERT_EQ(txn_size, lock_info.txn_size);

            ASSERT_EQ(true, lock_info.use_async_commit);
        }
        {
            auto lock_info = lock.intoLockInfo();
            ASSERT_TRUE(kvrpcpb::Op::Del == lock_info->lock_type());
            ASSERT_TRUE("primary key" == lock_info->primary_lock());
            ASSERT_TRUE(421321 == lock_info->lock_version());
            ASSERT_TRUE(std::numeric_limits<UInt64>::max() == lock_info->lock_ttl());
            ASSERT_TRUE(66666 == lock_info->min_commit_ts());
            ASSERT_TRUE(RecordKVFormat::decodeTiKVKey(*ori_key) == lock_info->key());
            ASSERT_EQ(true, lock_info->use_async_commit());
            ASSERT_EQ(lock_for_update_ts, lock_info->lock_for_update_ts());
            ASSERT_EQ(txn_size, lock_info->txn_size());
            {
                auto secondaries = lock_info->secondaries();
                ASSERT_EQ(2, secondaries.size());
                ASSERT_EQ(secondaries[0], async_commit[0]);
                ASSERT_EQ(secondaries[1], async_commit[1]);
            }
        }

        {
            RegionLockCFData d;
            auto k1 = RecordKVFormat::genKey(1, 123);
            auto k2 = RecordKVFormat::genKey(1, 124);
            d.insert(TiKVKey::copyFrom(k1),
                     RecordKVFormat::encodeLockCfValue(
                         Region::PutFlag,
                         "primary key",
                         8765,
                         std::numeric_limits<UInt64>::max(),
                         nullptr,
                         66666));
            d.insert(TiKVKey::copyFrom(k2),
                     RecordKVFormat::encodeLockCfValue(
                         RecordKVFormat::LockType::Lock,
                         "",
                         8,
                         20));
            d.insert(TiKVKey::copyFrom(k2),
                     RecordKVFormat::encodeLockCfValue(
                         RecordKVFormat::LockType::Pessimistic,
                         "",
                         8,
                         20));
            d.insert(TiKVKey::copyFrom(k2),
                     RecordKVFormat::encodeLockCfValue(
                         Region::DelFlag,
                         "primary key",
                         5678,
                         std::numeric_limits<UInt64>::max(),
                         nullptr,
                         66666));
            ASSERT_TRUE(d.getSize() == 2);
            ASSERT_TRUE(
                std::get<2>(d.getData().find(RegionLockCFDataTrait::Key{nullptr, std::string_view(k2.data(), k2.dataSize())})->second)
                    ->lock_version
                == 5678);
            d.remove(RegionLockCFDataTrait::Key{nullptr, std::string_view(k1.data(), k1.dataSize())}, true);
            ASSERT_TRUE(d.getSize() == 1);
            d.remove(RegionLockCFDataTrait::Key{nullptr, std::string_view(k2.data(), k2.dataSize())}, true);
            ASSERT_TRUE(d.getSize() == 0);
        }
    }

    {
        auto write_value = RecordKVFormat::encodeWriteCfValue(Region::DelFlag, std::numeric_limits<UInt64>::max(), "value");
        auto write_record = RecordKVFormat::decodeWriteCfValue(write_value);
        ASSERT_TRUE(write_record);
        ASSERT_TRUE(Region::DelFlag == write_record->write_type);
        ASSERT_TRUE(std::numeric_limits<UInt64>::max() == write_record->prewrite_ts);
        ASSERT_TRUE("value" == *write_record->short_value);
        RegionWriteCFData d;
        d.insert(RecordKVFormat::genKey(1, 2, 3), RecordKVFormat::encodeWriteCfValue(Region::PutFlag, 4, "value"));
        ASSERT_TRUE(d.getSize() == 1);

        ASSERT_TRUE(d.insert(RecordKVFormat::genKey(1, 2, 3), RecordKVFormat::encodeWriteCfValue(Region::PutFlag, 4, "value", true)) == 0);
        ASSERT_TRUE(d.getSize() == 1);

        ASSERT_TRUE(d.insert(RecordKVFormat::genKey(1, 2, 3),
                             RecordKVFormat::encodeWriteCfValue(RecordKVFormat::UselessCFModifyFlag::LockFlag, 4, "value"))
                    == 0);
        ASSERT_TRUE(d.getSize() == 1);

        auto pk = RecordKVFormat::getRawTiDBPK(RecordKVFormat::genRawKey(1, 2));
        d.remove(RegionWriteCFData::Key{pk, 3});
        ASSERT_TRUE(d.getSize() == 0);
    }

    {
        auto write_value = RecordKVFormat::encodeWriteCfValue(Region::DelFlag, std::numeric_limits<UInt64>::max());
        auto write_record = RecordKVFormat::decodeWriteCfValue(write_value);
        ASSERT_TRUE(write_record);
        ASSERT_TRUE(Region::DelFlag == write_record->write_type);
        ASSERT_TRUE(std::numeric_limits<UInt64>::max() == write_record->prewrite_ts);
        ASSERT_TRUE(nullptr == write_record->short_value);
    }

    {
        auto write_value = RecordKVFormat::encodeWriteCfValue(RecordKVFormat::UselessCFModifyFlag::RollbackFlag, 8888, "test");
        auto write_record = RecordKVFormat::decodeWriteCfValue(write_value);
        ASSERT_TRUE(!write_record);
    }

    {
        auto write_value = RecordKVFormat::encodeWriteCfValue(Region::PutFlag, 8888, "qwer", true);
        auto write_record = RecordKVFormat::decodeWriteCfValue(write_value);
        ASSERT_TRUE(!write_record);
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
        ASSERT_EQ(key.toDebugString(), "0132336F");
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
        ASSERT_TRUE(RecordKVFormat::getTableId(*range.rawKeys().first) == 1);
        ASSERT_TRUE(RecordKVFormat::getTableId(*range.rawKeys().second) == 2);
        ASSERT_TRUE(RecordKVFormat::getHandle(*range.rawKeys().first) == 2);
        ASSERT_TRUE(RecordKVFormat::getHandle(*range.rawKeys().second) == 4);

        ASSERT_TRUE(range.comparableKeys().first.state == TiKVRangeKey::NORMAL);
        ASSERT_TRUE(range.comparableKeys().second.state == TiKVRangeKey::NORMAL);

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
        const Int64 table_id = 2333;
        const Timestamp ts = 66666;
        std::string key(RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, 0);
        memcpy(key.data(), &RecordKVFormat::TABLE_PREFIX, 1);
        auto big_endian_table_id = RecordKVFormat::encodeInt64(table_id);
        memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id), 8);
        memcpy(key.data() + 1 + 8, RecordKVFormat::RECORD_PREFIX_SEP, 2);
        std::string pk = "12345678...";
        key += pk;
        auto tikv_key = RecordKVFormat::encodeAsTiKVKey(key);
        RecordKVFormat::appendTs(tikv_key, ts);
        {
            auto decoded_key = RecordKVFormat::decodeTiKVKey(tikv_key);
            ASSERT_EQ(RecordKVFormat::getTableId(decoded_key), table_id);
            auto tidb_pk = RecordKVFormat::getRawTiDBPK(decoded_key);
            ASSERT_EQ(*tidb_pk, pk);
        }
    }

    ASSERT_TRUE(res);
}

TEST(TiKVKeyValueTest, ParseLockValue)
try
{
    // prepare
    std::string shor_value = "value";
    auto lock_for_update_ts = 7777, txn_size = 1;
    const std::vector<std::string> & async_commit = {"s1", "s2"};
    const std::vector<uint64_t> & rollback = {3, 4};
    auto lock_value = encode_lock_cf_value(
        Region::DelFlag,
        "primary key",
        421321,
        std::numeric_limits<UInt64>::max(),
        &shor_value,
        66666,
        lock_for_update_ts,
        txn_size,
        async_commit,
        rollback);

    // parse
    auto ori_key = std::make_shared<const TiKVKey>(RecordKVFormat::genKey(1, 88888));
    auto lock = RecordKVFormat::DecodedLockCFValue(ori_key, std::make_shared<TiKVValue>(std::move(lock_value)));

    // check the parsed result
    {
        auto & lock_info = lock;
        ASSERT_TRUE(kvrpcpb::Op::Del == lock_info.lock_type);
        ASSERT_TRUE("primary key" == lock_info.primary_lock);
        ASSERT_TRUE(421321 == lock_info.lock_version);
        ASSERT_TRUE(std::numeric_limits<UInt64>::max() == lock_info.lock_ttl);
        ASSERT_TRUE(66666 == lock_info.min_commit_ts);
        ASSERT_TRUE(ori_key == lock_info.key);
        ASSERT_EQ(lock_for_update_ts, lock_info.lock_for_update_ts);
        ASSERT_EQ(txn_size, lock_info.txn_size);

        ASSERT_EQ(true, lock_info.use_async_commit);
    }
}
CATCH

TEST(TiKVKeyValueTest, Redact)
try
{
    String table_info_json
        = R"json({"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"a","O":"a"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":3,"Flen":10,"Tp":15}},{"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"b","O":"b"},"offset":1,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":3,"Flen":20,"Tp":15}},{"comment":"","default":null,"default_bit":null,"id":3,"name":{"L":"c","O":"c"},"offset":2,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":49,"index_info":[{"id":1,"idx_cols":[{"length":-1,"name":{"L":"a","O":"a"},"offset":0},{"length":-1,"name":{"L":"b","O":"b"},"offset":1}],"idx_name":{"L":"primary","O":"primary"},"index_type":1,"is_global":false,"is_invisible":false,"is_primary":true,"is_unique":true,"state":5,"tbl_name":{"L":"","O":""}}],"is_common_handle":true,"name":{"L":"pt","O":"pt"},"partition":null,"pk_is_handle":false,"schema_version":25,"state":5,"update_timestamp":421444995366518789})json";
    TiDB::TableInfo table_info(table_info_json, NullspaceID);
    ASSERT_TRUE(table_info.is_common_handle);

    TiKVKey start, end;
    {
        start = RecordKVFormat::genKey(table_info, std::vector{Field{"aaa", strlen("aaa")}, Field{"abc", strlen("abc")}});
        end = RecordKVFormat::genKey(table_info, std::vector{Field{"bbb", strlen("bbb")}, Field{"abc", strlen("abc")}});
    }
    RegionRangeKeys range(std::move(start), std::move(end));
    const auto & raw_keys = range.rawKeys();
    ASSERT_EQ(RecordKVFormat::getTableId(*raw_keys.first), 49);
    ASSERT_EQ(RecordKVFormat::getTableId(*raw_keys.second), 49);

    auto raw_pk1 = RecordKVFormat::getRawTiDBPK(*raw_keys.first);
    auto raw_pk2 = RecordKVFormat::getRawTiDBPK(*raw_keys.second);

    Redact::setRedactLog(false);
    // These will print the value
    EXPECT_NE(raw_pk1.toDebugString(), "?");
    EXPECT_NE(raw_pk2.toDebugString(), "?");
    EXPECT_NE(RecordKVFormat::DecodedTiKVKeyRangeToDebugString(raw_keys), "[?, ?)");

    Redact::setRedactLog(true);
    // These will print '?' instead of value
    EXPECT_EQ(raw_pk1.toDebugString(), "?");
    EXPECT_EQ(raw_pk2.toDebugString(), "?");
    EXPECT_EQ(RecordKVFormat::DecodedTiKVKeyRangeToDebugString(raw_keys), "[?, ?)");

    Redact::setRedactLog(false); // restore flags
}
CATCH

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
    return getHandleRangeByTable(range.rawKeys(), 45);
}

HandleRange<HandleID> parseTestCase2(std::vector<std::vector<u_char>> && seq)
{
    std::string start_key_s, end_key_s;
    for (const auto ch : seq[0])
        start_key_s += ch;
    for (const auto ch : seq[1])
        end_key_s += ch;
    RegionRangeKeys range{TiKVKey::copyFrom(start_key_s), TiKVKey::copyFrom(end_key_s)};
    return getHandleRangeByTable(range.rawKeys(), 45);
}

std::string rangeToString(const HandleRange<HandleID> & r)
{
    std::stringstream ss;
    ss << "[" << r.first.toString() << "," << r.second.toString() << ")";
    return ss.str();
}

} // namespace

TEST(RegionRangeTest, DISABLED_GetHandleRangeByTableID)
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
CATCH
