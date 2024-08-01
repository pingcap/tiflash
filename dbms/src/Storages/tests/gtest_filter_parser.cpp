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

#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessorUtils.h>
#include <Debug/dbgQueryCompiler.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <Storages/AlterCommands.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/KVStore/TMTContext.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <common/logger_useful.h>

#include <optional>
#include <regex>

namespace DB
{
namespace tests
{
class FilterParserTest : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }

    FilterParserTest()
        : log(Logger::get())
        , ctx(TiFlashTestEnv::getContext())
    {
        default_timezone_info = ctx->getTimezoneInfo();
    }

protected:
    LoggerPtr log;
    ContextPtr ctx;
    static TimezoneInfo default_timezone_info;
    DM::RSOperatorPtr generateRsOperator(String table_info_json, const String & query, TimezoneInfo & timezone_info);
};

TimezoneInfo FilterParserTest::default_timezone_info;

DM::RSOperatorPtr FilterParserTest::generateRsOperator(
    const String table_info_json,
    const String & query,
    TimezoneInfo & timezone_info = default_timezone_info)
{
    const TiDB::TableInfo table_info(table_info_json, NullspaceID);

    QueryTasks query_tasks;
    std::tie(query_tasks, std::ignore) = compileQuery(
        *ctx,
        query,
        [&](const String &, const String &) { return table_info; },
        getDAGProperties(""));
    auto & dag_request = *query_tasks[0].dag_request;
    DAGContext dag_context(dag_request, {}, NullspaceID, "", DAGRequestKind::Cop, "", 0, "", log);
    ctx->setDAGContext(&dag_context);
    // Don't care about regions information in this test
    google::protobuf::RepeatedPtrField<tipb::Expr> conditions;
    traverseExecutors(&dag_request, [&](const tipb::Executor & executor) {
        if (executor.has_selection())
        {
            conditions = executor.selection().conditions();
            return false;
        }
        return true;
    });

    DM::ColumnDefines columns_to_read;
    columns_to_read.reserve(table_info.columns.size());
    for (const auto & column : table_info.columns)
    {
        columns_to_read.push_back(DM::ColumnDefine(column.id, column.name, getDataTypeByColumnInfo(column)));
    }
    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters{}; // don't care pushed down filters
    std::unique_ptr<DAGQueryInfo> dag_query = std::make_unique<DAGQueryInfo>(
        conditions,
        tipb::ANNQueryInfo{},
        pushed_down_filters,
        table_info.columns,
        std::vector<int>(), // don't care runtime filter
        0,
        timezone_info);
    auto create_attr_by_column_id = [&columns_to_read](ColumnID column_id) -> DM::Attr {
        auto iter = std::find_if(
            columns_to_read.begin(),
            columns_to_read.end(),
            [column_id](const DM::ColumnDefine & d) -> bool { return d.id == column_id; });
        if (iter != columns_to_read.end())
            return DM::Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
        // Maybe throw an exception? Or check if `type` is nullptr before creating filter?
        return DM::Attr{.col_name = "", .col_id = column_id, .type = DataTypePtr{}};
    };

    return DM::FilterParser::parseDAGQuery(*dag_query, table_info.columns, std::move(create_attr_by_column_id), log);
}

// Test cases for col and literal
TEST_F(FilterParserTest, ColAndLiteral)
try
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"col_2","O":"col_2"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}}
    ],
    "pk_is_handle":false,"index_info":[],"is_common_handle":false,
    "name":{"L":"t_111","O":"t_111"},"partition":null,
    "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
})json";

    {
        // Equal between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 = 666");
        EXPECT_EQ(rs_operator->name(), "equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"666\"}");
    }

    {
        // Greater between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 > 666");
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater\",\"col\":\"col_2\",\"value\":\"666\"}");
    }

    {
        // GreaterEqual between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 >= 667");
        EXPECT_EQ(rs_operator->name(), "greater_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater_equal\",\"col\":\"col_2\",\"value\":\"667\"}");
    }

    {
        // Less between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 < 777");
        EXPECT_EQ(rs_operator->name(), "less");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less\",\"col\":\"col_2\",\"value\":\"777\"}");
    }

    {
        // LessEqual between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 <= 776");
        EXPECT_EQ(rs_operator->name(), "less_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less_equal\",\"col\":\"col_2\",\"value\":\"776\"}");
    }
}
CATCH

TEST_F(FilterParserTest, LiteralAndCol)
try
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"col_2","O":"col_2"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}}
    ],
    "pk_is_handle":false,"index_info":[],"is_common_handle":false,
    "name":{"L":"t_111","O":"t_111"},"partition":null,
    "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
})json";
    // Test cases for literal and col (inverse direction)
    {
        // Equal between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 667 = col_2");
        EXPECT_EQ(rs_operator->name(), "equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"667\"}");
    }

    {
        // NotEqual between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 667 != col_2");
        EXPECT_EQ(rs_operator->name(), "not_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"not_equal\",\"col\":\"col_2\",\"value\":\"667\"}");
    }

    {
        // Greater between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 667 < col_2");
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater\",\"col\":\"col_2\",\"value\":\"667\"}");
    }

    {
        // GreaterEqual between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 667 <= col_2");
        EXPECT_EQ(rs_operator->name(), "greater_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater_equal\",\"col\":\"col_2\",\"value\":\"667\"}");
    }

    {
        // Less between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 777 > col_2");
        EXPECT_EQ(rs_operator->name(), "less");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less\",\"col\":\"col_2\",\"value\":\"777\"}");
    }

    {
        // LessEqual between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 777 >= col_2");
        EXPECT_EQ(rs_operator->name(), "less_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less_equal\",\"col\":\"col_2\",\"value\":\"777\"}");
    }
}
CATCH

// Test cases for Logic operator
TEST_F(FilterParserTest, LogicOperator)
try
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"col_1","O":"col_1"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":254}},
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"col_2","O":"col_2"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}},
        {"comment":"","default":null,"default_bit":null,"id":3,"name":{"L":"col_3","O":"col_3"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}}
    ],
    "pk_is_handle":false,"index_info":[],"is_common_handle":false,
    "name":{"L":"t_111","O":"t_111"},"partition":null,
    "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
})json";
    {
        // Not
        auto rs_operator
            = generateRsOperator(table_info_json, "select col_1, col_2 from default.t_111 where NOT col_2=666");
        EXPECT_EQ(rs_operator->name(), "not");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"not\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"666\"}]}");
    }

    {
        // And
        auto rs_operator
            = generateRsOperator(table_info_json, "select * from default.t_111 where col_1 = 'test1' and col_2 = 666");
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        std::regex rx(
            R"(\{"op":"and","children":\[\{"op":"unsupported",.*\},\{"op":"equal","col":"col_2","value":"666"\}\]\})");
        EXPECT_TRUE(std::regex_search(rs_operator->toDebugString(), rx));
    }

    {
        // OR
        auto rs_operator
            = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 = 789 or col_2 = 777");
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 2);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->getColumnIDs()[1], 2);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"or\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"789\"},{\"op\":\"equal\","
            "\"col\":\"col_2\",\"value\":\"777\"}]}");
    }

    // More complicated
    {
        // And with "not supported"
        auto rs_operator = generateRsOperator(
            table_info_json,
            "select * from default.t_111 where col_1 = 'test1' and not col_2 = 666");
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        std::regex rx(
            R"(\{"op":"and","children":\[\{"op":"unsupported",.*\},\{"op":"not","children":\[\{"op":"equal","col":"col_2","value":"666"\}\]\}\]\})");
        EXPECT_TRUE(std::regex_search(rs_operator->toDebugString(), rx));
    }

    {
        // And with not
        auto rs_operator
            = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 = 789 and not col_3 = 666");
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 2);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->getColumnIDs()[1], 3);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"and\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"789\"},{\"op\":\"not\","
            "\"children\":[{\"op\":\"equal\",\"col\":\"col_3\",\"value\":\"666\"}]}]}");
    }

    {
        // And with or
        auto rs_operator = generateRsOperator(
            table_info_json,
            "select * from default.t_111 where col_2 = 789 and (col_3 = 666 or col_3 = 678)");
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 3);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->getColumnIDs()[1], 3);
        EXPECT_EQ(rs_operator->getColumnIDs()[2], 3);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"and\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"789\"},{\"op\":\"or\","
            "\"children\":[{\"op\":\"equal\",\"col\":\"col_3\",\"value\":\"666\"},{\"op\":\"equal\",\"col\":\"col_3\","
            "\"value\":\"678\"}]}]}");
    }

    {
        // Or with "not supported"
        auto rs_operator
            = generateRsOperator(table_info_json, "select * from default.t_111 where col_1 = 'test1' or col_2 = 666");
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        std::regex rx(
            R"(\{"op":"or","children":\[\{"op":"unsupported",.*\},\{"op":"equal","col":"col_2","value":"666"\}\]\})");
        EXPECT_TRUE(std::regex_search(rs_operator->toDebugString(), rx));
    }

    {
        // Or with not
        auto rs_operator = generateRsOperator(
            table_info_json,
            "select * from default.t_111 where col_1 = 'test1' or not col_2 = 666");
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        std::regex rx(
            R"(\{"op":"or","children":\[\{"op":"unsupported",.*\},\{"op":"not","children":\[\{"op":"equal","col":"col_2","value":"666"\}\]\}\]\})");
        EXPECT_TRUE(std::regex_search(rs_operator->toDebugString(), rx));
    }

    {
        // And with IsNULL
        auto rs_operator
            = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 = 789 and col_3 is null");
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 2);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->getColumnIDs()[1], 3);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"and\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"789\"},{\"op\":\"isnull\","
            "\"col\":\"col_3\"}]}");
    }

    {
        // And between col and literal (not supported since And only support when child is ColumnExpr)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 and 1");
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(
            rs_operator->toDebugString(),
            R"raw({"op":"and","children":[{"op":"unsupported","reason":"child of logical and is not function, expr.tp=ColumnRef"},{"op":"unsupported","reason":"child of logical and is not function, expr.tp=Uint64"}]})raw");
    }

    {
        // Or between col and literal (not supported since Or only support when child is ColumnExpr)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 or 1");
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(
            rs_operator->toDebugString(),
            R"raw({"op":"or","children":[{"op":"unsupported","reason":"child of logical operator is not function, child_type=ColumnRef"},{"op":"unsupported","reason":"child of logical operator is not function, child_type=Uint64"}]})raw");
    }

    {
        // IsNull with FunctionExpr (not supported since IsNull only support when child is ColumnExpr)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where (col_2 > 1) is null");
        EXPECT_EQ(rs_operator->name(), "unsupported");
    }
}
CATCH

// Test cases for date,datetime,timestamp column
TEST_F(FilterParserTest, TimestampColumn)
try
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":4,"name":{"L":"col_timestamp","O":"col_time"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":5,"Elems":null,"Flag":1,"Flen":0,"Tp":7}},
        {"comment":"","default":null,"default_bit":null,"id":5,"name":{"L":"col_datetime","O":"col_datetime"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":5,"Elems":null,"Flag":1,"Flen":0,"Tp":12}},
        {"comment":"","default":null,"default_bit":null,"id":6,"name":{"L":"col_date","O":"col_date"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":5,"Elems":null,"Flag":1,"Flen":0,"Tp":14}}
    ],
    "pk_is_handle":false,"index_info":[],"is_common_handle":false,
    "name":{"L":"t_111","O":"t_111"},"partition":null,
    "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
})json";

    String datetime = "2021-10-26 17:00:00.00000";
    ReadBufferFromMemory read_buffer(datetime.c_str(), datetime.size());
    UInt64 origin_time_stamp;
    ASSERT_TRUE(tryReadMyDateTimeText(origin_time_stamp, 6, read_buffer));
    const auto & time_zone_utc = DateLUT::instance("UTC");
    UInt64 converted_time = origin_time_stamp;

    {
        // Greater between TimeStamp col and Datetime literal, use local timezone
        auto ctx = TiFlashTestEnv::getContext();
        auto & timezone_info = ctx->getTimezoneInfo();
        convertTimeZone(origin_time_stamp, converted_time, *timezone_info.timezone, time_zone_utc);

        auto rs_operator = generateRsOperator(
            table_info_json,
            String("select * from default.t_111 where col_timestamp > cast_string_datetime('") + datetime
                + String("')"),
            timezone_info);
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_timestamp\",\"value\":\"") + toString(converted_time)
                + String("\"}"));
    }

    {
        // Greater between TimeStamp col and Datetime literal, use Chicago timezone
        auto ctx = TiFlashTestEnv::getContext();
        auto & timezone_info = ctx->getTimezoneInfo();
        timezone_info.resetByTimezoneName("America/Chicago");
        convertTimeZone(origin_time_stamp, converted_time, *timezone_info.timezone, time_zone_utc);

        auto rs_operator = generateRsOperator(
            table_info_json,
            String("select * from default.t_111 where col_timestamp > cast_string_datetime('") + datetime
                + String("')"),
            timezone_info);
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_timestamp\",\"value\":\"") + toString(converted_time)
                + String("\"}"));
    }

    {
        // Greater between TimeStamp col and Datetime literal, use Chicago timezone
        auto ctx = TiFlashTestEnv::getContext();
        auto & timezone_info = ctx->getTimezoneInfo();
        timezone_info.resetByTimezoneOffset(28800);
        convertTimeZoneByOffset(origin_time_stamp, converted_time, false, timezone_info.timezone_offset);

        auto rs_operator = generateRsOperator(
            table_info_json,
            String("select * from default.t_111 where col_timestamp > cast_string_datetime('") + datetime
                + String("')"),
            timezone_info);
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_timestamp\",\"value\":\"") + toString(converted_time)
                + String("\"}"));
    }

    {
        // Greater between Datetime col and Datetime literal
        auto rs_operator = generateRsOperator(
            table_info_json,
            String("select * from default.t_111 where col_datetime > cast_string_datetime('") + datetime
                + String("')"));
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 5);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_datetime\",\"value\":\"") + toString(origin_time_stamp)
                + String("\"}"));
    }

    {
        // Greater between Date col and Datetime literal
        auto rs_operator = generateRsOperator(
            table_info_json,
            String("select * from default.t_111 where col_date > cast_string_datetime('") + datetime + String("')"));
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 6);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_date\",\"value\":\"") + toString(origin_time_stamp)
                + String("\"}"));
    }
}
CATCH

// Test cases for unsupported column type
TEST_F(FilterParserTest, UnsupportedColumnType)
try
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"col_1","O":"col_1"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":254}},
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"col_2","O":"col_2"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}},
        {"comment":"","default":null,"default_bit":null,"id":3,"name":{"L":"col_3","O":"col_3"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":5}},
        {"comment":"","default":null,"default_bit":null,"id":5,"name":{"L":"col_5","O":"col_5"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":1,"Elems":null,"Flag":4097,"Flen":9,"Tp":0}}
    ],
    "pk_is_handle":false,"index_info":[],"is_common_handle":false,
    "name":{"L":"t_111","O":"t_111"},"partition":null,
    "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
})json";
    {
        // Greater between col and literal (not supported since the type of col_3 is floating point)
        auto rs_operator
            = generateRsOperator(table_info_json, "select * from default.t_111 where col_3 > 1234568.890123");
        EXPECT_EQ(rs_operator->name(), "unsupported");
    }

    {
        // Greater between col and literal (not supported since the type of col_1 is string)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_1 > '123'");
        EXPECT_EQ(rs_operator->name(), "unsupported");
    }

    {
        // Greater between col and literal (not supported since the type of col_5 is decimal)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_5 > 1");
        EXPECT_EQ(rs_operator->name(), "unsupported");
    }

    {
        // Not with literal (not supported since Not only support when child is ColumnExpr)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where not 1");
        EXPECT_EQ(rs_operator->name(), "unsupported");
    }
}
CATCH

// Test cases for not satisfy `column` `op` `literal`
TEST_F(FilterParserTest, ComplicatedFilters)
try
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":1,"name":{"L":"col_1","O":"col_1"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":254}},
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"col_2","O":"col_2"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}},
        {"comment":"","default":null,"default_bit":null,"id":3,"name":{"L":"col_3","O":"col_3"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":5}},
        {"comment":"","default":null,"default_bit":null,"id":5,"name":{"L":"col_5","O":"col_5"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}}
    ],
    "pk_is_handle":false,"index_info":[],"is_common_handle":false,
    "name":{"L":"t_111","O":"t_111"},"partition":null,
    "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
})json";

    for (const auto & test_case : Strings{
             "select * from default.t_111 where col_2 = col_5", // col and col
             "select * from default.t_111 where 666 = 666", // literal and literal
             "select * from default.t_111 where bitand(col_2, 1) > 100",
             "select * from default.t_111 where col_2 > bitand(100, 1)",
             "select * from default.t_111 where 100 < bitand(col_2, 1)",
             "select * from default.t_111 where bitand(100,1) < col_2",
             "select * from default.t_111 where round_int(col_2) < 1",
             "select * from default.t_111 where bitand(col_2, 1) = col_5",
             "select * from default.t_111 where bitor(bitand(col_2, 1), col_2) > col_5",
         })
    {
        auto rs_operator = generateRsOperator(table_info_json, test_case);
        EXPECT_EQ(rs_operator->name(), "unsupported");
    }
}
CATCH

} // namespace tests
} // namespace DB
