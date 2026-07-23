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

#include <Common/MyTime.h>
#include <DataTypes/DataTypeMyDateTime.h>
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
#include <Storages/DeltaMerge/Filter/And.h>
#include <Storages/DeltaMerge/Filter/DateQueryDomain.h>
#include <Storages/DeltaMerge/Filter/Or.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/DeltaMerge/Index/TrimMinMaxIndex.h>
#include <Storages/KVStore/TMTContext.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <common/logger_useful.h>

#include <regex>


namespace DB::tests
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
            // Maybe another test has already registered, ignore exception here.
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
    DM::RSOperatorPtr generateRsOperator(
        String table_info_json,
        const String & query,
        TimezoneInfo & timezone_info = default_timezone_info,
        bool enable_trim_minmax = false);
};

TimezoneInfo FilterParserTest::default_timezone_info;

DM::RSOperatorPtr FilterParserTest::generateRsOperator(
    const String table_info_json,
    const String & query,
    TimezoneInfo & timezone_info,
    bool enable_trim_minmax)
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
    // these variables need to live long enough as it is kept as reference in `dag_query`
    const auto ann_query_info = tipb::ANNQueryInfo{};
    const auto fts_query_info = tipb::FTSQueryInfo{};
    const auto runtime_filter_ids = std::vector<int>();
    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters{}; // don't care pushed down filters
    const google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> empty_used_indexes{}; // don't care used indexes
    std::unique_ptr<DAGQueryInfo> dag_query = std::make_unique<DAGQueryInfo>(
        conditions,
        ann_query_info,
        fts_query_info,
        pushed_down_filters,
        empty_used_indexes,
        table_info.columns,
        runtime_filter_ids, // don't care runtime filter
        0,
        timezone_info);
    DM::FilterParser::ColumnIDToAttrMap column_id_to_attr;
    for (const auto & cd : columns_to_read)
    {
        column_id_to_attr[cd.id] = DM::Attr{.col_name = cd.name, .col_id = cd.id, .type = cd.type};
    }

    return DM::FilterParser::parseDAGQuery(
        *dag_query, table_info.columns, column_id_to_attr, log, enable_trim_minmax);
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

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_inverted_query_info();
        inverted->set_index_id(1);
        inverted->set_column_id(2);
        used_indexes.Add(std::move(columnar_info));
    }

    {
        // Equal between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 = 666");
        EXPECT_EQ(rs_operator->name(), "equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"666\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: {666}");
    }

    {
        // Greater between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 > 666");
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater\",\"col\":\"col_2\",\"value\":\"666\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: [667, 9223372036854775807]");
    }

    {
        // GreaterEqual between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 >= 667");
        EXPECT_EQ(rs_operator->name(), "greater_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater_equal\",\"col\":\"col_2\",\"value\":\"667\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: [667, 9223372036854775807]");
    }

    {
        // Less between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 < 777");
        EXPECT_EQ(rs_operator->name(), "less");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less\",\"col\":\"col_2\",\"value\":\"777\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: [-9223372036854775808, 776]");
    }

    {
        // LessEqual between col and literal
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 <= 776");
        EXPECT_EQ(rs_operator->name(), "less_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less_equal\",\"col\":\"col_2\",\"value\":\"776\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: [-9223372036854775808, 776]");
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

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_inverted_query_info();
        inverted->set_index_id(1);
        inverted->set_column_id(2);
        used_indexes.Add(std::move(columnar_info));
    }

    // Test cases for literal and col (inverse direction)
    {
        // Equal between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 667 = col_2");
        EXPECT_EQ(rs_operator->name(), "equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"667\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: {667}");
    }

    {
        // NotEqual Equal between literal and col
        auto rs_operator
            = generateRsOperator(table_info_json, "select * from default.t_111 where -9223372036854775808 != col_2");
        EXPECT_EQ(rs_operator->name(), "not_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"not_equal\",\"col\":\"col_2\",\"value\":\"-9223372036854775808\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: [-9223372036854775807, 9223372036854775807]");
    }

    {
        // NotEqual between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 667 != col_2");
        EXPECT_EQ(rs_operator->name(), "not_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"not_equal\",\"col\":\"col_2\",\"value\":\"667\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: {[-9223372036854775808, 666], [668, 9223372036854775807]}");
    }

    {
        // Greater between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 667 < col_2");
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater\",\"col\":\"col_2\",\"value\":\"667\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: [668, 9223372036854775807]");
    }

    {
        // GreaterEqual between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 667 <= col_2");
        EXPECT_EQ(rs_operator->name(), "greater_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater_equal\",\"col\":\"col_2\",\"value\":\"667\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: [667, 9223372036854775807]");
    }

    {
        // Less between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 777 > col_2");
        EXPECT_EQ(rs_operator->name(), "less");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less\",\"col\":\"col_2\",\"value\":\"777\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: [-9223372036854775808, 776]");
    }

    {
        // LessEqual between literal and col (take care of direction)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where 777 >= col_2");
        EXPECT_EQ(rs_operator->name(), "less_equal");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less_equal\",\"col\":\"col_2\",\"value\":\"777\"}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: [-9223372036854775808, 777]");
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

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_inverted_query_info();
        inverted->set_index_id(1);
        inverted->set_column_id(2);
        used_indexes.Add(std::move(columnar_info));
    }
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_inverted_query_info();
        inverted->set_index_id(2);
        inverted->set_column_id(3);
        used_indexes.Add(std::move(columnar_info));
    }

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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: {[-9223372036854775808, 665], [667, 9223372036854775807]}");
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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: {666}");
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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: {777, 789}");
    }

    {
        // OR
        auto rs_operator
            = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 > 789 or col_2 < 791");
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 2);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 2);
        EXPECT_EQ(rs_operator->getColumnIDs()[1], 2);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"or\",\"children\":[{\"op\":\"greater\",\"col\":\"col_2\",\"value\":\"789\"},{\"op\":\"less\","
            "\"col\":\"col_2\",\"value\":\"791\"}]}");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: ALL");
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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: {[-9223372036854775808, 665], [667, 9223372036854775807]}");
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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "And[3: {[-9223372036854775808, 665], [667, 9223372036854775807]}, 2: {789}]");
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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "And[3: {666, 678}, 2: {789}]");
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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "2: {789}");
    }

    {
        // And between col and literal (not supported since And only support when child is ColumnExpr)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 and 1");
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(
            rs_operator->toDebugString(),
            R"raw({"op":"and","children":[{"op":"unsupported","reason":"child of logical and is not function, expr.tp=ColumnRef"},{"op":"unsupported","reason":"child of logical and is not function, expr.tp=Uint64"}]})raw");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
    }

    {
        // Or between col and literal (not supported since Or only support when child is ColumnExpr)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_2 or 1");
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(
            rs_operator->toDebugString(),
            R"raw({"op":"or","children":[{"op":"unsupported","reason":"child of logical operator is not function, child_type=ColumnRef"},{"op":"unsupported","reason":"child of logical operator is not function, child_type=Uint64"}]})raw");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
    }

    {
        // IsNull with FunctionExpr (not supported since IsNull only support when child is ColumnExpr)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where (col_2 > 1) is null");
        EXPECT_EQ(rs_operator->name(), "unsupported");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
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

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_inverted_query_info();
        inverted->set_index_id(1);
        inverted->set_column_id(4);
        used_indexes.Add(std::move(columnar_info));
    }
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_inverted_query_info();
        inverted->set_index_id(2);
        inverted->set_column_id(5);
        used_indexes.Add(std::move(columnar_info));
    }
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_inverted_query_info();
        inverted->set_index_id(3);
        inverted->set_column_id(6);
        used_indexes.Add(std::move(columnar_info));
    }

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
        const auto query = String("select * from default.t_111 where col_timestamp > cast_string_datetime('") + datetime
            + String("')");

        auto rs_operator = generateRsOperator(table_info_json, query, timezone_info, /*enable_trim_minmax*/ false);
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(R"json({{"op":"greater","col":"col_timestamp","value":"{}"}})json", toString(converted_time)));

        rs_operator = generateRsOperator(table_info_json, query, timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "date_range");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(
                R"json({{"op":"date_range","col":"col_timestamp","class":"LowerBounded","lower":"{}","lower_inclusive":false,"upper":"","upper_inclusive":true}})json",
                converted_time));
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), fmt::format("4: [{}, 18446744073709551615]", converted_time + 1));
    }

    {
        // Greater between TimeStamp col and Datetime literal, use Chicago timezone
        auto ctx = TiFlashTestEnv::getContext();
        auto & timezone_info = ctx->getTimezoneInfo();
        timezone_info.resetByTimezoneName("America/Chicago");
        convertTimeZone(origin_time_stamp, converted_time, *timezone_info.timezone, time_zone_utc);
        const auto query = String("select * from default.t_111 where col_timestamp > cast_string_datetime('") + datetime
            + String("')");

        auto rs_operator = generateRsOperator(table_info_json, query, timezone_info, /*enable_trim_minmax*/ false);
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(R"json({{"op":"greater","col":"col_timestamp","value":"{}"}})json", toString(converted_time)));

        rs_operator = generateRsOperator(table_info_json, query, timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "date_range");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(
                R"json({{"op":"date_range","col":"col_timestamp","class":"LowerBounded","lower":"{}","lower_inclusive":false,"upper":"","upper_inclusive":true}})json",
                converted_time));
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), fmt::format("4: [{}, 18446744073709551615]", converted_time + 1));
    }

    {
        // Greater between TimeStamp col and Datetime literal, use timezone offset
        auto ctx = TiFlashTestEnv::getContext();
        auto & timezone_info = ctx->getTimezoneInfo();
        timezone_info.resetByTimezoneOffset(28800);
        convertTimeZoneByOffset(origin_time_stamp, converted_time, false, timezone_info.timezone_offset);
        const auto query = String("select * from default.t_111 where col_timestamp > cast_string_datetime('") + datetime
            + String("')");

        auto rs_operator = generateRsOperator(table_info_json, query, timezone_info, /*enable_trim_minmax*/ false);
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(R"json({{"op":"greater","col":"col_timestamp","value":"{}"}})json", toString(converted_time)));

        rs_operator = generateRsOperator(table_info_json, query, timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "date_range");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(
                R"json({{"op":"date_range","col":"col_timestamp","class":"LowerBounded","lower":"{}","lower_inclusive":false,"upper":"","upper_inclusive":true}})json",
                converted_time));
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), fmt::format("4: [{}, 18446744073709551615]", converted_time + 1));
    }

    {
        // Greater between Datetime col and Datetime literal
        const auto query
            = fmt::format("select * from default.t_111 where col_datetime > cast_string_datetime('{}')", datetime);

        auto rs_operator
            = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ false);
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 5);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(
                R"json({{"op":"greater","col":"col_datetime","value":"{}"}})json",
                toString(origin_time_stamp)));

        rs_operator = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "date_range");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 5);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(
                R"json({{"op":"date_range","col":"col_datetime","class":"LowerBounded","lower":"{}","lower_inclusive":false,"upper":"","upper_inclusive":true}})json",
                origin_time_stamp));
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), fmt::format("5: [{}, 18446744073709551615]", origin_time_stamp + 1));
    }

    {
        // Greater between Date col and Datetime literal
        const auto query
            = fmt::format("select * from default.t_111 where col_date > cast_string_datetime('{}')", datetime);

        auto rs_operator
            = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ false);
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 6);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(R"json({{"op":"greater","col":"col_date","value":"{}"}})json", toString(origin_time_stamp)));

        rs_operator = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "date_range");
        EXPECT_EQ(rs_operator->getColumnIDs().size(), 1);
        EXPECT_EQ(rs_operator->getColumnIDs()[0], 6);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(
                R"json({{"op":"date_range","col":"col_date","class":"LowerBounded","lower":"{}","lower_inclusive":false,"upper":"","upper_inclusive":true}})json",
                origin_time_stamp));
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), fmt::format("6: [{}, 18446744073709551615]", origin_time_stamp + 1));
    }
}
CATCH

// Temporal-Type Tests: TIMESTAMP DST vs standard time, and DATE PreferTrim equality.
TEST_F(FilterParserTest, TimestampTrimNormalizeDSTAndDateEqual)
try
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":4,"name":{"L":"col_timestamp","O":"col_time"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":6,"Elems":null,"Flag":1,"Flen":0,"Tp":7}},
        {"comment":"","default":null,"default_bit":null,"id":6,"name":{"L":"col_date","O":"col_date"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":1,"Flen":0,"Tp":14}}
    ],
    "pk_is_handle":false,"index_info":[],"is_common_handle":false,
    "name":{"L":"t_111","O":"t_111"},"partition":null,
    "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
})json";

    const auto & time_zone_utc = DateLUT::instance("UTC");
    auto ctx = TiFlashTestEnv::getContext();
    auto & timezone_info = ctx->getTimezoneInfo();
    timezone_info.resetByTimezoneName("America/Chicago");

    auto parse_local = [](const String & datetime) {
        ReadBufferFromMemory read_buffer(datetime.c_str(), datetime.size());
        UInt64 origin = 0;
        EXPECT_TRUE(tryReadMyDateTimeText(origin, 6, read_buffer));
        return origin;
    };

    const String winter = "2021-01-15 12:00:00.000000"; // CST = UTC-6
    const String summer = "2021-07-15 12:00:00.000000"; // CDT = UTC-5
    const UInt64 winter_local = parse_local(winter);
    const UInt64 summer_local = parse_local(summer);

    UInt64 winter_chicago_utc = 0;
    UInt64 summer_chicago_utc = 0;
    convertTimeZone(winter_local, winter_chicago_utc, *timezone_info.timezone, time_zone_utc);
    convertTimeZone(summer_local, summer_chicago_utc, *timezone_info.timezone, time_zone_utc);

    // Fixed CST offset (-6h): matches Chicago in winter, differs in summer under DST.
    constexpr Int64 cst_offset = -6 * 3600;
    UInt64 winter_cst_utc = 0;
    UInt64 summer_cst_utc = 0;
    convertTimeZoneByOffset(winter_local, winter_cst_utc, /*from_utc*/ false, cst_offset);
    convertTimeZoneByOffset(summer_local, summer_cst_utc, /*from_utc*/ false, cst_offset);
    EXPECT_EQ(winter_chicago_utc, winter_cst_utc);
    EXPECT_NE(summer_chicago_utc, summer_cst_utc);

    for (const auto & [label, datetime, expected_utc] :
         {std::tuple<const char *, String, UInt64>{"winter", winter, winter_chicago_utc},
          {"summer", summer, summer_chicago_utc}})
    {
        // Use >= so normalize emits inclusive LowerBounded DateRange.
        const auto query = String("select * from default.t_111 where col_timestamp >= cast_string_datetime('")
            + datetime + String("')");
        auto rs_operator = generateRsOperator(table_info_json, query, timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "date_range") << label;
        auto reqs = rs_operator->getIndexRequests();
        ASSERT_EQ(reqs.size(), 1u) << label;
        EXPECT_EQ(reqs[0].preferred_kind, DM::RSIndexKind::PreferTrim) << label;
        ASSERT_TRUE(reqs[0].query_domain.has_value()) << label;
        EXPECT_EQ(reqs[0].query_domain->predicate_class, DM::TrimPredicateClass::LowerBounded) << label;
        ASSERT_TRUE(reqs[0].query_domain->lower.has_value()) << label;
        EXPECT_EQ(reqs[0].query_domain->lower->safeGet<UInt64>(), expected_utc) << label;
        EXPECT_TRUE(reqs[0].query_domain->lower_inclusive) << label;
    }

    // DATE equality -> PreferTrim (no timezone conversion).
    {
        const String date_lit = "2020-01-01";
        const auto query
            = fmt::format("select * from default.t_111 where col_date = cast_string_datetime('{}')", date_lit);
        auto rs_operator
            = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "equal");
        auto reqs = rs_operator->getIndexRequests();
        ASSERT_EQ(reqs.size(), 1u);
        EXPECT_EQ(reqs[0].preferred_kind, DM::RSIndexKind::PreferTrim);
        ASSERT_TRUE(reqs[0].query_domain.has_value());
        EXPECT_EQ(reqs[0].query_domain->predicate_class, DM::TrimPredicateClass::EqualityOrInOrBounded);
    }
}
CATCH

// normalizeTemporalRangesForTrim via FilterParser (gated by enable_trim_minmax).
TEST_F(FilterParserTest, NormalizeTemporalRangesForTrim)
try
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"col_2","O":"col_2"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}},
        {"comment":"","default":null,"default_bit":null,"id":5,"name":{"L":"col_datetime","O":"col_datetime"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":5,"Elems":null,"Flag":1,"Flen":0,"Tp":12}}
    ],
    "pk_is_handle":false,"index_info":[],"is_common_handle":false,
    "name":{"L":"t_111","O":"t_111"},"partition":null,
    "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
})json";

    const String lo_str = "2021-10-26 17:00:00.00000";
    const String hi_str = "2021-10-27 17:00:00.00000";
    UInt64 lo = 0;
    UInt64 hi = 0;
    {
        ReadBufferFromMemory lo_buf(lo_str.c_str(), lo_str.size());
        ASSERT_TRUE(tryReadMyDateTimeText(lo, 6, lo_buf));
        ReadBufferFromMemory hi_buf(hi_str.c_str(), hi_str.size());
        ASSERT_TRUE(tryReadMyDateTimeText(hi, 6, hi_buf));
    }

    {
        // Bounded range: GE+LE merge into a single DateRange only when normalize is enabled.
        const auto query = fmt::format(
            "select * from default.t_111 where col_datetime >= cast_string_datetime('{}') "
            "and col_datetime <= cast_string_datetime('{}')",
            lo_str,
            hi_str);

        auto rs_operator
            = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ false);
        EXPECT_EQ(rs_operator->name(), "and");
        auto and_op = std::dynamic_pointer_cast<DM::And>(rs_operator);
        ASSERT_NE(and_op, nullptr);
        ASSERT_EQ(and_op->getChildren().size(), 2u);
        EXPECT_EQ(and_op->getChildren()[0]->name(), "greater_equal");
        EXPECT_EQ(and_op->getChildren()[1]->name(), "less_equal");

        rs_operator = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "date_range");
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(
                R"json({{"op":"date_range","col":"col_datetime","class":"EqualityOrInOrBounded","lower":"{}","lower_inclusive":true,"upper":"{}","upper_inclusive":true}})json",
                lo,
                hi));
        auto reqs = rs_operator->getIndexRequests();
        ASSERT_EQ(reqs.size(), 1u);
        EXPECT_EQ(reqs[0].preferred_kind, DM::RSIndexKind::PreferTrim);
        ASSERT_TRUE(reqs[0].query_domain.has_value());
        EXPECT_EQ(reqs[0].query_domain->predicate_class, DM::TrimPredicateClass::EqualityOrInOrBounded);
    }

    {
        // Upper-only bound becomes UpperBounded DateRange when normalize is enabled.
        const auto query
            = fmt::format("select * from default.t_111 where col_datetime <= cast_string_datetime('{}')", hi_str);

        auto rs_operator
            = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ false);
        EXPECT_EQ(rs_operator->name(), "less_equal");

        rs_operator = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "date_range");
        EXPECT_EQ(
            rs_operator->toDebugString(),
            fmt::format(
                R"json({{"op":"date_range","col":"col_datetime","class":"UpperBounded","lower":"","lower_inclusive":true,"upper":"{}","upper_inclusive":true}})json",
                hi));
    }

    {
        // OR must not rewrite children into DateRange, even with normalize enabled.
        const auto query = fmt::format(
            "select * from default.t_111 where col_datetime >= cast_string_datetime('{}') "
            "or col_datetime = cast_string_datetime('{}')",
            lo_str,
            hi_str);

        auto rs_operator
            = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "or");
        auto or_op = std::dynamic_pointer_cast<DM::Or>(rs_operator);
        ASSERT_NE(or_op, nullptr);
        ASSERT_EQ(or_op->getChildren().size(), 2u);
        EXPECT_EQ(or_op->getChildren()[0]->name(), "greater_equal");
        EXPECT_EQ(or_op->getChildren()[1]->name(), "equal");
    }

    {
        // Temporal range AND non-temporal predicate: only the temporal part is rewritten.
        const auto query = fmt::format(
            "select * from default.t_111 where col_datetime >= cast_string_datetime('{}') "
            "and col_datetime <= cast_string_datetime('{}') and col_2 = 666",
            lo_str,
            hi_str);

        auto rs_operator
            = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "and");
        auto and_op = std::dynamic_pointer_cast<DM::And>(rs_operator);
        ASSERT_NE(and_op, nullptr);
        ASSERT_EQ(and_op->getChildren().size(), 2u);

        // Child order follows flatten then rewrite: non-temporal kept first, then DateRange.
        bool found_date_range = false;
        bool found_equal = false;
        for (const auto & child : and_op->getChildren())
        {
            if (child->name() == "date_range")
            {
                found_date_range = true;
                EXPECT_EQ(
                    child->toDebugString(),
                    fmt::format(
                        R"json({{"op":"date_range","col":"col_datetime","class":"EqualityOrInOrBounded","lower":"{}","lower_inclusive":true,"upper":"{}","upper_inclusive":true}})json",
                        lo,
                        hi));
            }
            else if (child->name() == "equal")
            {
                found_equal = true;
                EXPECT_EQ(child->toDebugString(), R"json({"op":"equal","col":"col_2","value":"666"})json");
            }
        }
        EXPECT_TRUE(found_date_range);
        EXPECT_TRUE(found_equal);
    }

    {
        // Top-level AND with an OR leaf: only the exposed GE/LE bounds are rewritten.
        // t >= L AND t <= U AND (t = L OR t = U)  =>  DateRange AND Or(equal, equal)
        const auto query = fmt::format(
            "select * from default.t_111 where col_datetime >= cast_string_datetime('{}') "
            "and col_datetime <= cast_string_datetime('{}') "
            "and (col_datetime = cast_string_datetime('{}') or col_datetime = cast_string_datetime('{}'))",
            lo_str,
            hi_str,
            lo_str,
            hi_str);

        auto rs_operator
            = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "and");
        auto and_op = std::dynamic_pointer_cast<DM::And>(rs_operator);
        ASSERT_NE(and_op, nullptr);
        ASSERT_EQ(and_op->getChildren().size(), 2u);

        bool found_date_range = false;
        bool found_or = false;
        for (const auto & child : and_op->getChildren())
        {
            if (child->name() == "date_range")
            {
                found_date_range = true;
                EXPECT_EQ(
                    child->toDebugString(),
                    fmt::format(
                        R"json({{"op":"date_range","col":"col_datetime","class":"EqualityOrInOrBounded","lower":"{}","lower_inclusive":true,"upper":"{}","upper_inclusive":true}})json",
                        lo,
                        hi));
            }
            else if (child->name() == "or")
            {
                found_or = true;
                auto or_op = std::dynamic_pointer_cast<DM::Or>(child);
                ASSERT_NE(or_op, nullptr);
                ASSERT_EQ(or_op->getChildren().size(), 2u);
                EXPECT_EQ(or_op->getChildren()[0]->name(), "equal");
                EXPECT_EQ(or_op->getChildren()[1]->name(), "equal");
            }
        }
        EXPECT_TRUE(found_date_range);
        EXPECT_TRUE(found_or);
    }

    {
        // Top-level OR whose child contains AND: do not rewrite the AND inside OR into DateRange.
        // (t >= L AND t <= U) OR t = U  =>  still Or(And(...), equal), no date_range
        const auto query = fmt::format(
            "select * from default.t_111 where (col_datetime >= cast_string_datetime('{}') "
            "and col_datetime <= cast_string_datetime('{}')) "
            "or col_datetime = cast_string_datetime('{}')",
            lo_str,
            hi_str,
            hi_str);

        auto rs_operator
            = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(rs_operator->name(), "or");
        auto or_op = std::dynamic_pointer_cast<DM::Or>(rs_operator);
        ASSERT_NE(or_op, nullptr);
        ASSERT_EQ(or_op->getChildren().size(), 2u);

        bool found_and = false;
        bool found_equal = false;
        for (const auto & child : or_op->getChildren())
        {
            EXPECT_NE(child->name(), "date_range");
            if (child->name() == "and")
            {
                found_and = true;
                auto and_op = std::dynamic_pointer_cast<DM::And>(child);
                ASSERT_NE(and_op, nullptr);
                ASSERT_EQ(and_op->getChildren().size(), 2u);
                EXPECT_EQ(and_op->getChildren()[0]->name(), "greater_equal");
                EXPECT_EQ(and_op->getChildren()[1]->name(), "less_equal");
            }
            else if (child->name() == "equal")
            {
                found_equal = true;
            }
        }
        EXPECT_TRUE(found_and);
        EXPECT_TRUE(found_equal);
    }
}
CATCH

// Design "Must fall back": distinguish shape-level (no PreferTrim) vs eligibility-level
// (PreferTrim DateRange whose domain is outside stored E).
TEST_F(FilterParserTest, TrimMustFallBackShapes)
try
{
    const String table_info_json = R"json({
    "cols":[
        {"comment":"","default":null,"default_bit":null,"id":2,"name":{"L":"col_2","O":"col_2"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":0,"Elems":null,"Flag":4097,"Flen":0,"Tp":8}},
        {"comment":"","default":null,"default_bit":null,"id":5,"name":{"L":"col_datetime","O":"col_datetime"},"offset":-1,"origin_default":null,"state":0,"type":{"Charset":null,"Collate":null,"Decimal":5,"Elems":null,"Flag":1,"Flen":0,"Tp":12}}
    ],
    "pk_is_handle":false,"index_info":[],"is_common_handle":false,
    "name":{"L":"t_111","O":"t_111"},"partition":null,
    "comment":"Mocked.","id":30,"schema_version":-1,"state":0,"tiflash_replica":{"Count":0},"update_timestamp":1636471547239654
})json";

    const String in_e = "2020-01-01 00:00:00.00000";
    const String out_high = "2200-01-01 00:00:00.00000";
    const String out_low = "1800-01-01 00:00:00.00000";
    const String hi_in_e = "2020-01-02 00:00:00.00000";

    auto has_prefer_trim = [](const DM::RSOperatorPtr & op) {
        for (const auto & req : op->getIndexRequests())
        {
            if (req.preferred_kind == DM::RSIndexKind::PreferTrim)
                return true;
        }
        return false;
    };

    auto expect_no_prefer_trim = [&](const DM::RSOperatorPtr & op, const char * label) {
        ASSERT_NE(op, nullptr) << label;
        EXPECT_FALSE(has_prefer_trim(op)) << label << " tree=" << op->toDebugString();
    };

    auto expect_prefer_trim_ineligible = [&](const DM::RSOperatorPtr & op, const char * label) {
        ASSERT_NE(op, nullptr) << label;
        EXPECT_EQ(op->name(), "date_range") << label;
        ASSERT_TRUE(has_prefer_trim(op)) << label;
        auto reqs = op->getIndexRequests();
        ASSERT_EQ(reqs.size(), 1u) << label;
        ASSERT_TRUE(reqs[0].query_domain.has_value()) << label;
        auto dt = std::make_shared<DataTypeMyDateTime>(0);
        const UInt64 e_lo = DM::TrimMinMax::defaultLowerBoundPacked(*dt);
        const UInt64 e_hi = DM::TrimMinMax::defaultUpperBoundPacked(*dt);
        EXPECT_FALSE(reqs[0].query_domain->isTrimEligible(e_lo, e_hi)) << label;
    };

    // Shape: col != ...
    {
        const auto query
            = fmt::format("select * from default.t_111 where col_datetime != cast_string_datetime('{}')", in_e);
        auto op = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(op->name(), "not_equal");
        expect_no_prefer_trim(op, "col != literal");
    }

    // Shape: NOT (BETWEEN ...)  <=>  NOT (GE AND LE)
    // Tipb may keep Not(...) or rewrite; either way must not PreferTrim / emit DateRange.
    {
        const auto query = fmt::format(
            "select * from default.t_111 where not (col_datetime >= cast_string_datetime('{}') "
            "and col_datetime <= cast_string_datetime('{}'))",
            in_e,
            hi_in_e);
        auto op = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        expect_no_prefer_trim(op, "NOT (BETWEEN)");
        EXPECT_EQ(op->toDebugString().find("date_range"), String::npos) << op->toDebugString();
    }

    // Shape: BETWEEN OR status = 1 (non-temporal Equal stays Normal)
    {
        const auto query = fmt::format(
            "select * from default.t_111 where (col_datetime >= cast_string_datetime('{}') "
            "and col_datetime <= cast_string_datetime('{}')) or col_2 = 1",
            in_e,
            hi_in_e);
        auto op = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(op->name(), "or");
        expect_no_prefer_trim(op, "BETWEEN OR status=1");
        EXPECT_EQ(op->toDebugString().find("date_range"), String::npos);
    }

    // Shape: BETWEEN OR col IS NULL
    {
        const auto query = fmt::format(
            "select * from default.t_111 where (col_datetime >= cast_string_datetime('{}') "
            "and col_datetime <= cast_string_datetime('{}')) or col_datetime is null",
            in_e,
            hi_in_e);
        auto op = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        EXPECT_EQ(op->name(), "or");
        expect_no_prefer_trim(op, "BETWEEN OR IS NULL");
        EXPECT_EQ(op->toDebugString().find("date_range"), String::npos);
    }

    // Shape: non-ColumnRef predicate (CAST stand-in). MockExecutor cannot compile `cast`,
    // so use bitand(...) which FilterParser likewise turns into Unsupported / no PreferTrim.
    {
        auto op = generateRsOperator(
            table_info_json,
            "select * from default.t_111 where bitand(col_2, 1) > 100",
            default_timezone_info,
            /*enable_trim_minmax*/ true);
        EXPECT_EQ(op->name(), "unsupported") << op->toDebugString();
        expect_no_prefer_trim(op, "function(col) compare literal (CAST stand-in)");
    }

    // Eligibility: col >= 2200 may PreferTrim, but domain is outside default E.
    {
        const auto query
            = fmt::format("select * from default.t_111 where col_datetime >= cast_string_datetime('{}')", out_high);
        auto op = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        expect_prefer_trim_ineligible(op, "col >= 2200");
    }

    // Eligibility: col <= 1800
    {
        const auto query
            = fmt::format("select * from default.t_111 where col_datetime <= cast_string_datetime('{}')", out_low);
        auto op = generateRsOperator(table_info_json, query, default_timezone_info, /*enable_trim_minmax*/ true);
        expect_prefer_trim_ineligible(op, "col <= 1800");
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

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;

    {
        // Greater between col and literal (not supported since the type of col_3 is floating point)
        auto rs_operator
            = generateRsOperator(table_info_json, "select * from default.t_111 where col_3 > 1234568.890123");
        EXPECT_EQ(rs_operator->name(), "unsupported");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
    }

    {
        // Greater between col and literal (not supported since the type of col_1 is string)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_1 > '123'");
        EXPECT_EQ(rs_operator->name(), "unsupported");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
    }

    {
        // Greater between col and literal (not supported since the type of col_5 is decimal)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where col_5 > 1");
        EXPECT_EQ(rs_operator->name(), "unsupported");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
    }

    {
        // Not with literal (not supported since Not only support when child is ColumnExpr)
        auto rs_operator = generateRsOperator(table_info_json, "select * from default.t_111 where not 1");
        EXPECT_EQ(rs_operator->name(), "unsupported");
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
    }
}
CATCH

// ColumnIDToAttrMap may hold empty type when column id is absent from defines.
// parseTiCompareExpr must turn that into Unsupported instead of Equal/In.
TEST_F(FilterParserTest, MissingAttrTypeBecomesUnsupported)
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

    const TiDB::TableInfo table_info(table_info_json, NullspaceID);
    QueryTasks query_tasks;
    std::tie(query_tasks, std::ignore) = compileQuery(
        *ctx,
        "select * from default.t_111 where col_2 = 666",
        [&](const String &, const String &) { return table_info; },
        getDAGProperties(""));
    auto & dag_request = *query_tasks[0].dag_request;
    DAGContext dag_context(dag_request, {}, NullspaceID, "", DAGRequestKind::Cop, "", 0, "", log);
    ctx->setDAGContext(&dag_context);

    google::protobuf::RepeatedPtrField<tipb::Expr> conditions;
    traverseExecutors(&dag_request, [&](const tipb::Executor & executor) {
        if (executor.has_selection())
        {
            conditions = executor.selection().conditions();
            return false;
        }
        return true;
    });

    const auto ann_query_info = tipb::ANNQueryInfo{};
    const auto fts_query_info = tipb::FTSQueryInfo{};
    const auto runtime_filter_ids = std::vector<int>();
    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters{};
    const google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> empty_used_indexes{};
    std::unique_ptr<DAGQueryInfo> dag_query = std::make_unique<DAGQueryInfo>(
        conditions,
        ann_query_info,
        fts_query_info,
        pushed_down_filters,
        empty_used_indexes,
        table_info.columns,
        runtime_filter_ids,
        0,
        default_timezone_info);

    // Simulate missing column defines: Attr.type is empty for the referenced column.
    DM::FilterParser::ColumnIDToAttrMap column_id_to_attr;
    column_id_to_attr[2] = DM::Attr{.col_name = "", .col_id = 2, .type = DataTypePtr{}};

    auto rs_operator = DM::FilterParser::parseDAGQuery(
        *dag_query,
        table_info.columns,
        column_id_to_attr,
        log,
        /*enable_trim_minmax*/ false);
    EXPECT_EQ(rs_operator->name(), "unsupported");
    // getIndexRequests is called by DMFilePackFilter regardless of trim settings.
    EXPECT_NO_THROW(std::ignore = rs_operator->getIndexRequests());
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

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;

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
        auto sets = rs_operator->buildSets(used_indexes);
        EXPECT_TRUE(sets != nullptr);
        EXPECT_EQ(sets->toDebugString(), "Unsupported");
    }
}
CATCH

} // namespace DB::tests
