// Copyright 2026 PingCAP, Inc.
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

#include <Common/MyTime.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <Storages/DeltaMerge/Filter/SessionTimezone.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::DM::tests
{
namespace
{
UInt64 packed(const String & text)
{
    ReadBufferFromString buffer(text);
    UInt64 value;
    readMyDateTimeText(value, 6, buffer);
    return value;
}

RSCheckParam packs(const Attr & attr, const std::vector<std::pair<String, String>> & bounds)
{
    auto index = std::make_shared<MinMaxIndex>(*attr.type);
    for (const auto & [min, max] : bounds)
    {
        auto column = attr.type->createColumn();
        column->insert(Field(packed(min)));
        column->insert(Field(packed(max)));
        index->addPack(*column, nullptr);
    }
    return RSCheckParam{.indexes = {{attr.col_id, RSIndex(attr.type, index)}}};
}
} // namespace

TEST(SessionTimezoneTest, ConvertsBoundsAndKeepsFold)
{
    const Attr attr{"ts", 1, std::make_shared<DataTypeMyDateTime>(6)};
    const auto param = packs(
        attr,
        {
            {"2021-11-07 04:00:00", "2021-11-07 04:10:00"},
            {"2021-11-07 05:20:00", "2021-11-07 05:40:00"},
            {"2021-11-07 06:20:00", "2021-11-07 06:40:00"},
            {"2021-11-07 04:00:00", "2021-11-07 08:00:00"},
            {"2021-11-07 08:00:00", "2021-11-07 08:10:00"},
        });
    TimezoneInfo tz;
    tz.resetByTimezoneName("America/New_York");
    auto op = std::make_shared<SessionTimezone>(createEqual(attr, Field(packed("2021-11-07 01:30:00"))), attr, tz);
    EXPECT_EQ(
        op->roughCheck(0, 5, param),
        (RSResults{RSResult::None, RSResult::SomeNull, RSResult::SomeNull, RSResult::SomeNull, RSResult::None}));
    EXPECT_EQ(createNot(op)->roughCheck(1, 3, param), RSResults(3, RSResult::SomeNull));
    EXPECT_EQ(createOr({op, op})->roughCheck(0, 5, param), op->roughCheck(0, 5, param));
    EXPECT_EQ(
        createAnd({op, createEqual(attr, Field(packed("2022-01-01 00:00:00")))})->roughCheck(0, 5, param),
        (RSResults{RSResult::None, RSResult::NoneNull, RSResult::NoneNull, RSResult::NoneNull, RSResult::None}));

    tz.resetByTimezoneOffset(8 * 3600);
    SessionTimezone fixed(createEqual(attr, Field(packed("2021-11-07 12:05:00"))), attr, tz);
    EXPECT_EQ(fixed.roughCheck(0, 1, param), RSResults(1, RSResult::Some));
    EXPECT_EQ(fixed.roughCheck(1, 1, param), RSResults(1, RSResult::None));
    EXPECT_EQ(param.indexes.at(1).minmax->getUInt64MinMax(0).first, packed("2021-11-07 04:00:00"));
}

TEST(SessionTimezoneTest, RetainsTransitionsZeroAndNullableBounds)
{
    const Attr attr{"ts", 1, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMyDateTime>(6))};
    auto param = packs(
        attr,
        {
            {"2021-03-14 06:59:59", "2021-03-14 07:00:01"},
            {"2021-01-01 00:00:00", "2021-12-31 00:00:00"},
            {"0000-00-00 00:00:00", "2021-12-31 00:00:00"},
            {"2039-01-01 00:00:00", "2039-01-02 00:00:00"},
        });
    auto nulls = attr.type->createColumn();
    nulls->insert(Field());
    param.indexes.at(1).minmax->addPack(*nulls, nullptr);
    TimezoneInfo tz;
    tz.resetByTimezoneName("America/New_York");
    SessionTimezone op(createGreater(attr, Field(packed("2022-01-01 00:00:00"))), attr, tz);
    EXPECT_EQ(op.roughCheck(0, 5, param), RSResults(5, RSResult::SomeNull));
}

TEST(SessionTimezoneTest, ParserProjectsTimestampLiteralsToSessionTime)
{
    const Attr attr{"ts", 1, std::make_shared<DataTypeMyDateTime>(6)};
    const auto param = packs(attr, {{"2021-11-08 12:00:00", "2021-11-08 12:00:00"}});
    TiDB::ColumnInfos columns(1);
    columns[0].id = 1;
    google::protobuf::RepeatedPtrField<tipb::Expr> filters;
    auto * expr = filters.Add();
    expr->set_tp(tipb::ExprType::ScalarFunc);
    expr->set_sig(tipb::ScalarFuncSig::EQTime);
    auto * column = expr->add_children();
    column->set_tp(tipb::ExprType::ColumnRef);
    WriteBufferFromOwnString offset;
    encodeDAGInt64(0, offset);
    column->set_val(offset.releaseStr());
    column->mutable_field_type()->set_tp(TiDB::TypeTimestamp);
    auto * literal = expr->add_children();
    *literal = constructDateTimeLiteralTiExpr(packed("2021-11-08 07:00:00"));
    TimezoneInfo tz;
    tz.resetByTimezoneName("America/New_York");
    const tipb::ANNQueryInfo ann;
    const tipb::FTSQueryInfo fts;
    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed;
    const google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> indexes;
    const std::vector<int> runtime_filters;
    DAGQueryInfo dag(filters, ann, fts, pushed, indexes, columns, runtime_filters, 0, tz);
    auto check = [&] {
        return FilterParser::parseDAGQuery(dag, columns, {{1, attr}}, Logger::get())->roughCheck(0, 1, param);
    };
    EXPECT_EQ(check(), RSResults(1, RSResult::All));
    *literal = constructDateTimeLiteralTiExpr(packed("2021-11-08 12:00:00"));
    literal->mutable_field_type()->set_tp(TiDB::TypeTimestamp);
    EXPECT_EQ(check(), RSResults(1, RSResult::All));
    *literal = constructDateTimeLiteralTiExpr(packed("1969-12-31 23:59:59"));
    literal->mutable_field_type()->set_tp(TiDB::TypeTimestamp);
    EXPECT_TRUE(check()[0].isUse());
}

TEST(SessionTimezoneTest, RuntimeInRetainsBothFoldOccurrences)
{
    const Attr attr{"ts", 1, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMyDateTime>(6))};
    auto index = std::make_shared<MinMaxIndex>(*attr.type);
    for (const auto & utc : {"2026-11-01 04:30:00", "2026-11-01 05:30:00", "2026-11-01 06:30:00"})
    {
        auto column = attr.type->createColumn();
        column->insert(Field(packed(utc)));
        index->addPack(*column, nullptr);
    }
    auto nulls = attr.type->createColumn();
    nulls->insert(Field());
    index->addPack(*nulls, nullptr);
    const RSCheckParam param{.indexes = {{attr.col_id, RSIndex(attr.type, index)}}};
    tipb::Expr target;
    target.set_tp(tipb::ExprType::ColumnRef);
    target.mutable_field_type()->set_tp(TiDB::TypeTimestamp);
    TimezoneInfo tz;
    tz.resetByTimezoneName("America/New_York");
    std::set<Field> values{Field(packed("2026-11-01 01:30:00"))};
    for (const bool include_null : {false, true})
    {
        SCOPED_TRACE(include_null);
        if (include_null)
            values.insert(Field());
        auto op = FilterParser::parseRFInExpr(tipb::IN, target, attr, values, tz);
        const auto result = op->roughCheck(0, 4, param);
        EXPECT_FALSE(result[0].isUse());
        // Both UTC occurrences match the same session value; neither may be pruned.
        EXPECT_TRUE(result[1].isUse());
        EXPECT_TRUE(result[2].isUse());
        EXPECT_TRUE(result[3].isUse());
        EXPECT_EQ(op->roughCheck(1, 2, param), (RSResults{result[1], result[2]}));
        EXPECT_EQ(op->buildSets({})->toDebugString(), "Unsupported");
    }
}

TEST(SessionTimezoneTest, RuntimeInPrunesSafeTimezoneBounds)
{
    const Attr attr{"ts", 1, std::make_shared<DataTypeMyDateTime>(6)};
    const auto param
        = packs(attr, {{"2026-11-02 12:00:00", "2026-11-02 12:00:00"}, {"2026-11-02 13:00:00", "2026-11-02 13:00:00"}});
    tipb::Expr target;
    target.set_tp(tipb::ExprType::ColumnRef);
    target.mutable_field_type()->set_tp(TiDB::TypeTimestamp);
    TimezoneInfo tz;
    const auto check = [&](const String & session_value) {
        auto op = FilterParser::parseRFInExpr(tipb::IN, target, attr, {Field(packed(session_value))}, tz);
        const auto result = op->roughCheck(0, 2, param);
        EXPECT_TRUE(result[0].isUse());
        EXPECT_FALSE(result[1].isUse());
    };
    tz.resetByTimezoneName("America/New_York");
    check("2026-11-02 07:00:00");
    tz.resetByTimezoneOffset(8 * 3600);
    check("2026-11-02 20:00:00");
    tz.resetByTimezoneName("UTC");
    check("2026-11-02 12:00:00");
    // DATETIME values must not be shifted even in a non-UTC session.
    target.mutable_field_type()->set_tp(TiDB::TypeDatetime);
    tz.resetByTimezoneName("America/New_York");
    check("2026-11-02 12:00:00");
}
} // namespace DB::DM::tests
