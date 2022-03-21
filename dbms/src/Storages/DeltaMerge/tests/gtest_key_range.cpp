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

#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace DM
{
namespace tests
{
TEST(HandleRange_test, Redact)
{
    HandleRange range(20, 400);

    Redact::setRedactLog(false);
    EXPECT_EQ(range.toDebugString(), "[20,400)");

    Redact::setRedactLog(true);
    EXPECT_EQ(range.toDebugString(), "[?,?)");

    Redact::setRedactLog(false); // restore flags
}

namespace
{
std::shared_ptr<RegionRangeKeys> genTestRegionRangeKeys()
{
    TiKVKey start, end;
    {
        String table_info_json
            = R"json({"cols":[{"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"a","O":"a"},"offset":0,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":3,"Flen":10,"Tp":15}},{"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"b","O":"b"},"offset":1,"origin_default":null,"state":5,"type":{"Charset":"utf8mb4","Collate":"utf8mb4_bin","Decimal":0,"Elems":null,"Flag":3,"Flen":20,"Tp":15}},{"comment":"","default":null,"default_bit":null,"id":3,"name":{"L":"c","O":"c"},"offset":2,"origin_default":null,"state":5,"type":{"Charset":"binary","Collate":"binary","Decimal":0,"Elems":null,"Flag":0,"Flen":11,"Tp":3}}],"comment":"","id":49,"index_info":[{"id":1,"idx_cols":[{"length":-1,"name":{"L":"a","O":"a"},"offset":0},{"length":-1,"name":{"L":"b","O":"b"},"offset":1}],"idx_name":{"L":"primary","O":"primary"},"index_type":1,"is_global":false,"is_invisible":false,"is_primary":true,"is_unique":true,"state":5,"tbl_name":{"L":"","O":""}}],"is_common_handle":true,"name":{"L":"pt","O":"pt"},"partition":null,"pk_is_handle":false,"schema_version":25,"state":5,"update_timestamp":421444995366518789})json";
        TiDB::TableInfo table_info(table_info_json);

        start = RecordKVFormat::genKey(table_info, std::vector{Field{"aaa", strlen("aaa")}, Field{"abc", strlen("abc")}});
        end = RecordKVFormat::genKey(table_info, std::vector{Field{"bbb", strlen("bbb")}, Field{"abc", strlen("abc")}});
    }
    return std::make_shared<RegionRangeKeys>(std::move(start), std::move(end));
}
} // namespace

TEST(RowKeyRange_test, Basic)
{
    RowKeyRange all_range = RowKeyRange::newAll(true, 3);
    EXPECT_TRUE(all_range.isStartInfinite());
    EXPECT_TRUE(all_range.isEndInfinite());
    EXPECT_TRUE(all_range.all());
    RowKeyRange none_range = RowKeyRange::newNone(true, 3);
    EXPECT_TRUE(none_range.none());
}

TEST(RowKeyRange_test, RedactRangeFromHandle)
{
    RowKeyRange range = RowKeyRange::fromHandleRange(HandleRange{20, 400});

    Redact::setRedactLog(false);
    EXPECT_EQ(range.toDebugString(), "[20,400)");

    Redact::setRedactLog(true);
    EXPECT_EQ(range.toDebugString(), "[?,?)");

    Redact::setRedactLog(false); // restore flags
}

TEST(RowKeyRange_test, RedactRangeFromCommonHandle)
{
    auto region_range = genTestRegionRangeKeys();
    TableID table_id = 49;
    RowKeyRange range = RowKeyRange::fromRegionRange(region_range, table_id, true, 3);
    RowKeyRange all_range = RowKeyRange::newAll(true, 3);
    RowKeyRange none_range = RowKeyRange::newNone(true, 3);

    // print some values
    Redact::setRedactLog(false);
    EXPECT_NE(range.toDebugString(), "[?,?)");
    EXPECT_NE(all_range.toDebugString(), "[?,?)");
    EXPECT_NE(none_range.toDebugString(), "[?,?)");

    // print placeholder(?) instead of values
    Redact::setRedactLog(true);
    EXPECT_EQ(range.toDebugString(), "[?,?)");
    EXPECT_EQ(all_range.toDebugString(), "[?,?)");
    EXPECT_EQ(none_range.toDebugString(), "[?,?)");

    Redact::setRedactLog(false); // restore flags
}

} // namespace tests
} // namespace DM
} // namespace DB
