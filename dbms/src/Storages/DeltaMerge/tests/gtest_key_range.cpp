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

#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/TiDB.h>

namespace DB::DM::tests
{

TEST(HandleRangeTest, Redact)
{
    HandleRange range(20, 400);

    Redact::setRedactLog(RedactMode::Disable);
    EXPECT_EQ(range.toDebugString(), "[20,400)");

    Redact::setRedactLog(RedactMode::Enable);
    EXPECT_EQ(range.toDebugString(), "[?,?)");

    Redact::setRedactLog(RedactMode::Marker);
    EXPECT_EQ(range.toDebugString(), "[‹20›,‹400›)");

    Redact::setRedactLog(RedactMode::Disable); // restore flags
}

namespace
{
std::shared_ptr<RegionRangeKeys> genTestRegionRangeKeys()
{
    TiKVKey start, end;
    {
        String table_info_json
            = R"json({"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"a","O":"a"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":3,"Flen":10,"Tp":15}},{"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"b","O":"b"},"offset":1,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":3,"Flen":20,"Tp":15}},{"comment":"","default":null,"default_bit":null,"id":3,"name":{"L":"c","O":"c"},"offset":2,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":49,"index_info":[{"id":1,"idx_cols":[{"length":-1,"name":{"L":"a","O":"a"},"offset":0},{"length":-1,"name":{"L":"b","O":"b"},"offset":1}],"idx_name":{"L":"primary","O":"primary"},"index_type":1,"is_global":false,"is_invisible":false,"is_primary":true,"is_unique":true,"state":5,"tbl_name":{"L":"","O":""}}],"is_common_handle":true,"name":{"L":"pt","O":"pt"},"partition":null,"pk_is_handle":false,"schema_version":25,"state":5,"update_timestamp":421444995366518789})json";
        TiDB::TableInfo table_info(table_info_json, NullspaceID);

        start
            = RecordKVFormat::genKey(table_info, std::vector{Field{"aaa", strlen("aaa")}, Field{"abc", strlen("abc")}});
        end = RecordKVFormat::genKey(table_info, std::vector{Field{"bbb", strlen("bbb")}, Field{"abc", strlen("abc")}});
    }
    return std::make_shared<RegionRangeKeys>(std::move(start), std::move(end));
}
} // namespace

TEST(HandleRangeTest, Basic)
{
    RowKeyRange all_range = RowKeyRange::newAll(true, 3);
    EXPECT_TRUE(all_range.isStartInfinite());
    EXPECT_TRUE(all_range.isEndInfinite());
    EXPECT_TRUE(all_range.all());
    RowKeyRange none_range = RowKeyRange::newNone(true, 3);
    EXPECT_TRUE(none_range.none());
}

TEST(HandleRangeTest, RedactRangeFromHandle)
{
    RowKeyRange range = RowKeyRange::fromHandleRange(HandleRange{20, 400});

    Redact::setRedactLog(RedactMode::Disable);
    EXPECT_EQ(range.toDebugString(), "[20,400)");

    Redact::setRedactLog(RedactMode::Enable);
    EXPECT_EQ(range.toDebugString(), "[?,?)");

    Redact::setRedactLog(RedactMode::Marker);
    EXPECT_EQ(range.toDebugString(), "[‹20›,‹400›)");

    Redact::setRedactLog(RedactMode::Disable); // restore flags
}

TEST(HandleRangeTest, RedactRangeFromCommonHandle)
{
    auto region_range = genTestRegionRangeKeys();
    TableID table_id = 49;
    RowKeyRange range = RowKeyRange::fromRegionRange(region_range, table_id, true, 3);
    RowKeyRange all_range = RowKeyRange::newAll(true, 3);
    RowKeyRange none_range = RowKeyRange::newNone(true, 3);

    // print some values
    Redact::setRedactLog(RedactMode::Disable);
    EXPECT_EQ(range.toDebugString(), "[02066161610206616263,02066262620206616263)");
    EXPECT_EQ(all_range.toDebugString(), "[01,FA)");
    EXPECT_EQ(none_range.toDebugString(), "[FA,01)");

    // print placeholder(?) instead of values
    Redact::setRedactLog(RedactMode::Enable);
    EXPECT_EQ(range.toDebugString(), "[?,?)");
    EXPECT_EQ(all_range.toDebugString(), "[?,?)");
    EXPECT_EQ(none_range.toDebugString(), "[?,?)");

    // print values with marker
    Redact::setRedactLog(RedactMode::Marker);
    EXPECT_EQ(range.toDebugString(), "[‹02066161610206616263›,‹02066262620206616263›)");
    EXPECT_EQ(all_range.toDebugString(), "[‹01›,‹FA›)");
    EXPECT_EQ(none_range.toDebugString(), "[‹FA›,‹01›)");

    Redact::setRedactLog(RedactMode::Disable); // restore flags
}

TEST(RowKey, ToNextKeyIntHandle)
{
    const auto key = RowKeyValue::fromHandle(20);
    const auto next = key.toNext();
    EXPECT_EQ("21", next.toDebugString());

    {
        const auto expected_next_int = RowKeyValue::fromHandle(21);
        EXPECT_EQ(next.toRowKeyValueRef(), expected_next_int.toRowKeyValueRef());
    }
    {
        const auto range_keys
            = std::make_shared<RegionRangeKeys>(RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 21));
        const auto range = RowKeyRange::fromRegionRange(
            range_keys,
            /* table_id */ 1,
            /* is_common_handle */ false,
            /* row_key_column_size */ 1);
        EXPECT_EQ(next.toRowKeyValueRef(), range.getEnd());
    }
    // Note: {20,00} will be regarded as Key=21 in RowKeyRange::fromRegionRange.
    {
        auto key_end = RecordKVFormat::genRawKey(1, 20);
        key_end.push_back(0);
        auto tikv_key_end = RecordKVFormat::encodeAsTiKVKey(key_end);
        const auto range_keys
            = std::make_shared<RegionRangeKeys>(RecordKVFormat::genKey(1, 0), std::move(tikv_key_end));
        const auto range = RowKeyRange::fromRegionRange(
            range_keys,
            /* table_id */ 1,
            /* is_common_handle */ false,
            /* row_key_column_size */ 1);
        EXPECT_EQ(next.toRowKeyValueRef(), range.getEnd());
    }
}

TEST(RowKey, ToNextKeyCommonHandle)
{
    using namespace std::literals::string_literals;

    const auto key = RowKeyValue(/* is_common_handle */ true, std::make_shared<String>("\xcc\xab"s), 0);
    const auto next = key.toNext();
    EXPECT_EQ("CCAB00", next.toDebugString());

    const auto my_next = RowKeyValue(/* is_common_handle */ true, std::make_shared<String>("\xcc\xab\x00"s), 0);
    EXPECT_EQ(my_next.toRowKeyValueRef(), next.toRowKeyValueRef());
}

TEST(RowKey, NextIntHandleCompare)
{
    auto int_max = RowKeyValue::INT_HANDLE_MAX_KEY;
    auto int_max_i64 = RowKeyValue::fromHandle(static_cast<Handle>(std::numeric_limits<HandleID>::max()));

    EXPECT_GT(int_max.toRowKeyValueRef(), int_max_i64.toRowKeyValueRef());

    auto int_max_i64_pnext = int_max_i64.toPrefixNext();
    EXPECT_EQ(int_max, int_max_i64_pnext);
    EXPECT_EQ(int_max.toRowKeyValueRef(), int_max_i64_pnext.toRowKeyValueRef());
    EXPECT_EQ(int_max_i64_pnext.toRowKeyValueRef(), int_max.toRowKeyValueRef());

    auto int_max_i64_next = int_max_i64.toNext();
    EXPECT_EQ(int_max, int_max_i64_next);
    EXPECT_EQ(int_max.toRowKeyValueRef(), int_max_i64_next.toRowKeyValueRef());
    EXPECT_EQ(int_max_i64_next.toRowKeyValueRef(), int_max.toRowKeyValueRef());
}

TEST(RowKey, NextIntHandleMinMax)
{
    auto v0 = RowKeyValue::fromHandle(static_cast<Handle>(1178400));
    auto v0_next = v0.toNext();
    auto v1 = RowKeyValue::fromHandle(static_cast<Handle>(1178401));

    EXPECT_EQ(v0, std::min(v0, v1));
    EXPECT_EQ(v0, std::min(v0, v0_next));

    EXPECT_EQ(v1, std::max(v0, v1));
    EXPECT_EQ(v1, std::max(v0, v0_next));
}

} // namespace DB::DM::tests
