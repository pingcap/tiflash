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

#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <Debug/dbgQueryCompiler.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/StorageDeltaMerge.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>

#include <regex>


namespace DB::tests
{

class ParsePushDownFilterTest : public ::testing::Test
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

protected:
    LoggerPtr log = Logger::get();
    ContextPtr ctx = DB::tests::TiFlashTestEnv::getContext();
    TimezoneInfo default_timezone_info = DB::tests::TiFlashTestEnv::getContext()->getTimezoneInfo();
    DM::PushDownFilterPtr generatePushDownFilter(
        String table_info_json,
        const String & query,
        TimezoneInfo & timezone_info);
};


DM::PushDownFilterPtr ParsePushDownFilterTest::generatePushDownFilter(
    const String table_info_json,
    const String & query,
    TimezoneInfo & timezone_info)
{
    const TiDB::TableInfo table_info(table_info_json, NullspaceID);
    QueryTasks query_tasks;
    std::tie(query_tasks, std::ignore) = compileQuery(
        *ctx,
        query,
        [&](const String &, const String &) { return table_info; },
        getDAGProperties(""));
    auto & dag_request = *query_tasks[0].dag_request;
    DAGContext dag_context(dag_request, {}, NullspaceID, "", DAGRequestKind::Cop, "", log);
    ctx->setDAGContext(&dag_context);
    // Don't care about regions information in this test
    DAGQuerySource dag(*ctx);
    auto query_block = *dag.getRootQueryBlock();
    google::protobuf::RepeatedPtrField<tipb::Expr> empty_condition;
    // Push down all filters
    const google::protobuf::RepeatedPtrField<tipb::Expr> & pushed_down_filters
        = query_block.children[0]->selection != nullptr ? query_block.children[0]->selection->selection().conditions()
                                                        : empty_condition;
    const google::protobuf::RepeatedPtrField<tipb::Expr> & conditions = empty_condition;

    std::unique_ptr<DAGQueryInfo> dag_query;
    DM::ColumnDefines columns_to_read;
    columns_to_read.reserve(table_info.columns.size());
    {
        for (const auto & column : table_info.columns)
        {
            columns_to_read.push_back(DM::ColumnDefine(column.id, column.name, getDataTypeByColumnInfo(column)));
        }
        dag_query = std::make_unique<DAGQueryInfo>(
            conditions,
            pushed_down_filters,
            table_info.columns,
            std::vector<int>(), // don't care runtime filter
            0,
            timezone_info);
    }

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

    auto rs_operator
<<<<<<< HEAD
        = DM::FilterParser::parseDAGQuery(*dag_query, columns_to_read, std::move(create_attr_by_column_id), log);
    auto push_down_filter = StorageDeltaMerge::buildPushDownFilter(
        rs_operator,
        table_info.columns,
        pushed_down_filters,
        columns_to_read,
        *ctx,
        log);
=======
        = DM::FilterParser::parseDAGQuery(*dag_query, table_info.columns, std::move(create_attr_by_column_id), log);
    auto push_down_filter
        = DM::PushDownFilter::build(rs_operator, table_info.columns, pushed_down_filters, columns_to_read, *ctx, log);
>>>>>>> e6fc04addf (Storages: Fix obtaining incorrect column information when there are virtual columns in the query (#9189))
    return push_down_filter;
}

// Test cases for col and literal
TEST_F(ParsePushDownFilterTest, ColAndLiteral)
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
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 = 666",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "equal");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"666\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 1);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // Greater between col and literal
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 > 666",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater\",\"col\":\"col_2\",\"value\":\"666\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 2);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // GreaterEqual between col and literal
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 >= 667",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "greater_equal");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater_equal\",\"col\":\"col_2\",\"value\":\"667\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 2);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // Less between col and literal
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 < 777",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "less");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less\",\"col\":\"col_2\",\"value\":\"777\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 7);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // LessEqual between col and literal
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 <= 776",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "less_equal");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less_equal\",\"col\":\"col_2\",\"value\":\"776\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 7);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }
}
CATCH

TEST_F(ParsePushDownFilterTest, LiteralAndCol)
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
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where 667 = col_2",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "equal");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"667\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 1);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // NotEqual between literal and col (take care of direction)
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where 667 != col_2",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "not_equal");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"not_equal\",\"col\":\"col_2\",\"value\":\"667\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 7);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // Greater between literal and col (take care of direction)
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where 667 < col_2",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater\",\"col\":\"col_2\",\"value\":\"667\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 1);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // GreaterEqual between literal and col (take care of direction)
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where 667 <= col_2",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "greater_equal");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"greater_equal\",\"col\":\"col_2\",\"value\":\"667\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 2);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // Less between literal and col (take care of direction)
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where 777 > col_2",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "less");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less\",\"col\":\"col_2\",\"value\":\"777\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 7);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // LessEqual between literal and col (take care of direction)
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where 777 >= col_2",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "less_equal");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->toDebugString(), "{\"op\":\"less_equal\",\"col\":\"col_2\",\"value\":\"777\"}");

        Block before_where_block = Block{toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439})};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 7);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }
}
CATCH

// Test cases for Logic operator
TEST_F(ParsePushDownFilterTest, LogicOperator)
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
        auto filter = generatePushDownFilter(
            table_info_json,
            "select col_1, col_2 from default.t_111 where NOT col_2=666",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "not");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"not\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"666\"}]}");

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 7);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // And
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_1 = 'test1' and col_2 = 666",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        std::regex rx(
            R"(\{"op":"and","children":\[\{"op":"unsupported",.*\},\{"op":"equal","col":"col_2","value":"666"\}\]\})");
        EXPECT_TRUE(std::regex_search(rs_operator->toDebugString(), rx));

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 1);
        EXPECT_EQ(filter->filter_columns->size(), 2);
    }

    {
        // OR
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 = 789 or col_2 = 777",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(rs_operator->getAttrs().size(), 2);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->getAttrs()[1].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[1].col_id, 2);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"or\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"789\"},{\"op\":\"equal\","
            "\"col\":\"col_2\",\"value\":\"777\"}]}");

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 0);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    // More complicated
    {
        // And with "not supported"
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_1 = 'test1' and not col_2 = 666",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        std::regex rx(
            R"(\{"op":"and","children":\[\{"op":"unsupported",.*\},\{"op":"not","children":\[\{"op":"equal","col":"col_2","value":"666"\}\]\}\]\})");
        EXPECT_TRUE(std::regex_search(rs_operator->toDebugString(), rx));

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 1);
        EXPECT_EQ(filter->filter_columns->size(), 2);
    }

    {
        // And with not
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 = 789 and not col_3 = 666",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(rs_operator->getAttrs().size(), 2);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->getAttrs()[1].col_name, "col_3");
        EXPECT_EQ(rs_operator->getAttrs()[1].col_id, 3);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"and\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"789\"},{\"op\":\"not\","
            "\"children\":[{\"op\":\"equal\",\"col\":\"col_3\",\"value\":\"666\"}]}]}");

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 0);
        EXPECT_EQ(filter->filter_columns->size(), 2);
    }

    {
        // And with or
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 = 789 and (col_3 = 666 or col_3 = 678)",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(rs_operator->getAttrs().size(), 3);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        EXPECT_EQ(rs_operator->getAttrs()[1].col_name, "col_3");
        EXPECT_EQ(rs_operator->getAttrs()[1].col_id, 3);
        EXPECT_EQ(rs_operator->getAttrs()[2].col_name, "col_3");
        EXPECT_EQ(rs_operator->getAttrs()[2].col_id, 3);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"and\",\"children\":[{\"op\":\"equal\",\"col\":\"col_2\",\"value\":\"789\"},{\"op\":\"or\","
            "\"children\":[{\"op\":\"equal\",\"col\":\"col_3\",\"value\":\"666\"},{\"op\":\"equal\",\"col\":\"col_3\","
            "\"value\":\"678\"}]}]}");

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 1, 0, 1, 121, 789, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 1);
        EXPECT_EQ(filter->filter_columns->size(), 2);
    }

    {
        // Or with "not supported"
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_1 = 'test1' or col_2 = 666",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        std::regex rx(
            R"(\{"op":"or","children":\[\{"op":"unsupported",.*\},\{"op":"equal","col":"col_2","value":"666"\}\]\})");
        EXPECT_TRUE(std::regex_search(rs_operator->toDebugString(), rx));

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 1, 0, 1, 121, 666, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 2);
        EXPECT_EQ(filter->filter_columns->size(), 2);
    }

    {
        // Or with not
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_1 = 'test1' or not col_2 = 666",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_2");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 2);
        std::regex rx(
            R"(\{"op":"or","children":\[\{"op":"unsupported",.*\},\{"op":"not","children":\[\{"op":"equal","col":"col_2","value":"666"\}\]\}\]\})");
        EXPECT_TRUE(std::regex_search(rs_operator->toDebugString(), rx));

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 666, 0, 1, 121, 666, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 7);
        EXPECT_EQ(filter->filter_columns->size(), 2);
    }

    {
        // And between col and literal (not supported since And only support when child is ColumnExpr)
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 and 1",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "and");
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"and\",\"children\":[{\"op\":\"unsupported\",\"reason\":\"child of logical and is not "
            "function\",\"content\":\"tp: ColumnRef val: \"\\200\\000\\000\\000\\000\\000\\000\\001\" field_type { tp: "
            "8 flag: 4097 flen: 0 decimal: 0 collate: 0 "
            "}\",\"is_not\":\"0\"},{\"op\":\"unsupported\",\"reason\":\"child of logical and is not "
            "function\",\"content\":\"tp: Uint64 val: \"\\000\\000\\000\\000\\000\\000\\000\\001\" field_type { tp: 1 "
            "flag: 4129 flen: 0 decimal: 0 collate: 0 }\",\"is_not\":\"0\"}]}");

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 666, 0, 1, 121, 666, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 6);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    std::cout << " do query select * from default.t_111 where col_2 or 1 " << std::endl;
    {
        // Or between col and literal (not supported since Or only support when child is ColumnExpr)
        auto filter = generatePushDownFilter(
            table_info_json,
            "select * from default.t_111 where col_2 or 1",
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "or");
        EXPECT_EQ(
            rs_operator->toDebugString(),
            "{\"op\":\"or\",\"children\":[{\"op\":\"unsupported\",\"reason\":\"child of logical operator is not "
            "function\",\"content\":\"tp: ColumnRef val: \"\\200\\000\\000\\000\\000\\000\\000\\001\" field_type { tp: "
            "8 flag: 4097 flen: 0 decimal: 0 collate: 0 "
            "}\",\"is_not\":\"0\"},{\"op\":\"unsupported\",\"reason\":\"child of logical operator is not "
            "function\",\"content\":\"tp: Uint64 val: \"\\000\\000\\000\\000\\000\\000\\000\\001\" field_type { tp: 1 "
            "flag: 4129 flen: 0 decimal: 0 collate: 0 }\",\"is_not\":\"0\"}]}");

        Block before_where_block = Block{
            {toVec<String>("col_1", {"a", "b", "c", "test1", "d", "test1", "pingcap", "tiflash"}),
             toVec<Int64>("col_2", {0, 666, 0, 1, 121, 666, 667, 888439}),
             toVec<Int64>("col_3", {3, 121, 0, 121, 121, 666, 667, 888439})}};
        EXPECT_EQ(filter->extra_cast, nullptr);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        EXPECT_TRUE(col->isColumnConst()); // always true, so filter column is const column
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    // TODO: add is null and is not null test case
    // after test framework support nullable column
}
CATCH

// Test cases for date,datetime,timestamp column
TEST_F(ParsePushDownFilterTest, TimestampColumn)
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

    String datetime = "1970-01-01 00:00:01.000000";
    ReadBufferFromMemory read_buffer(datetime.c_str(), datetime.size());
    UInt64 origin_time_stamp;
    tryReadMyDateTimeText(origin_time_stamp, 6, read_buffer);
    const auto & time_zone_utc = DateLUT::instance("UTC");
    UInt64 converted_time = origin_time_stamp;
    std::cout << "origin_time_stamp: " << origin_time_stamp << std::endl;
    // origin_time_stamp: 1802216106174185472

    {
        // Greater between TimeStamp col and Datetime literal, use local timezone
        auto ctx = TiFlashTestEnv::getContext();
        auto & timezone_info = ctx->getTimezoneInfo();
        convertTimeZone(origin_time_stamp, converted_time, *timezone_info.timezone, time_zone_utc);
        // converted_time: 0

        auto filter = generatePushDownFilter(
            table_info_json,
            String("select * from default.t_111 where col_timestamp > cast_string_datetime('") + datetime
                + String("')"),
            timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_timestamp");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_timestamp\",\"value\":\"") + toString(converted_time)
                + String("\"}"));

        Block before_where_block = Block{
            {toVec<UInt64>(
                 "col_timestamp",
                 {12, 1, 1802216106174185472, 1802216106174185472, 1, 43, 1802216106174185472, 888439}),
             toVec<UInt64>(
                 "col_datetime",
                 {1849259496301477883,
                  1849559496301477888,
                  0,
                  1,
                  1849259496301477888,
                  1849559496301477888,
                  667,
                  888439}),
             toVec<Int64>(
                 "col_date",
                 {-1849559496301477888, 1849259496301477888, 0, 121, 121, 1849259496301477888, 667, 888439})}};
        EXPECT_TRUE(filter->extra_cast);
        filter->extra_cast->execute(before_where_block);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 3);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // Greater between TimeStamp col and Datetime literal, use Chicago timezone
        auto ctx = TiFlashTestEnv::getContext();
        auto & timezone_info = ctx->getTimezoneInfo();
        timezone_info.resetByTimezoneName("America/Chicago");
        convertTimeZone(origin_time_stamp, converted_time, *timezone_info.timezone, time_zone_utc);
        // converted_time: 1802216518491045888

        auto filter = generatePushDownFilter(
            table_info_json,
            String("select * from default.t_111 where col_timestamp > cast_string_datetime('") + datetime
                + String("')"),
            timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_timestamp");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_timestamp\",\"value\":\"") + toString(converted_time)
                + String("\"}"));

        Block before_where_block = Block{
            {toVec<UInt64>(
                 "col_timestamp",
                 {1849559496301477888,
                  1849560389654675456,
                  1949560389654675456,
                  1849259496301477888,
                  1849560389654675452,
                  1849559416301477888,
                  1849559496301477833,
                  888439}),
             toVec<UInt64>(
                 "col_datetime",
                 {1849259496301477883,
                  1849559496301477888,
                  0,
                  1,
                  1849259496301477888,
                  1849559496301477888,
                  667,
                  888439}),
             toVec<Int64>(
                 "col_date",
                 {-1849559496301477888, 1849259496301477888, 0, 121, 121, 1849259496301477888, 667, 888439})}};
        EXPECT_TRUE(filter->extra_cast);
        filter->extra_cast->execute(before_where_block);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 7);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // Greater between TimeStamp col and Datetime literal, use Chicago timezone
        auto ctx = TiFlashTestEnv::getContext();
        auto & timezone_info = ctx->getTimezoneInfo();
        timezone_info.resetByTimezoneOffset(28800);
        convertTimeZoneByOffset(origin_time_stamp, converted_time, false, timezone_info.timezone_offset);
        // converted_time: 0

        auto filter = generatePushDownFilter(
            table_info_json,
            String("select * from default.t_111 where col_timestamp > cast_string_datetime('") + datetime
                + String("')"),
            timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_timestamp");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 4);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_timestamp\",\"value\":\"") + toString(converted_time)
                + String("\"}"));

        Block before_where_block = Block{
            {toVec<UInt64>(
                 "col_timestamp",
                 {1849559496301477888,
                  1849560389654675456,
                  1949560389654675456,
                  1849259496301477888,
                  1849560389654675452,
                  1849559416301477888,
                  1849559496301477833,
                  888439}),
             toVec<UInt64>(
                 "col_datetime",
                 {1849259496301477883,
                  1849559496301477888,
                  0,
                  1,
                  1849259496301477888,
                  1849559496301477888,
                  667,
                  888439}),
             toVec<Int64>(
                 "col_date",
                 {-1849559496301477888, 1849259496301477888, 0, 121, 121, 1849259496301477888, 667, 888439})}};
        EXPECT_TRUE(filter->extra_cast);
        filter->extra_cast->execute(before_where_block);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 7);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // Greater between Datetime col and Datetime literal
        auto filter = generatePushDownFilter(
            table_info_json,
            String("select * from default.t_111 where col_datetime > cast_string_datetime('") + datetime + String("')"),
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_datetime");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 5);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_datetime\",\"value\":\"") + toString(origin_time_stamp)
                + String("\"}"));

        Block before_where_block = Block{
            {toVec<UInt64>(
                 "col_timestamp",
                 {1849559496301477888,
                  1849560389654675456,
                  1949560389654675456,
                  1849259496301477888,
                  1849560389654675452,
                  1849559416301477888,
                  1849559496301477833,
                  888439}),
             toVec<UInt64>(
                 "col_datetime",
                 {1849259496301477883,
                  1849559496301477888,
                  0,
                  1,
                  1849259496301477888,
                  1849559496301477888,
                  667,
                  888439}),
             toVec<Int64>(
                 "col_date",
                 {-1849559496301477888, 1849259496301477888, 0, 121, 121, 1849259496301477888, 667, 888439})}};
        EXPECT_TRUE(!filter->extra_cast);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 4);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }

    {
        // Greater between Date col and Datetime literal
        auto filter = generatePushDownFilter(
            table_info_json,
            String("select * from default.t_111 where col_date > cast_string_datetime('") + datetime + String("')"),
            default_timezone_info);
        const auto & rs_operator = filter->rs_operator;
        EXPECT_EQ(rs_operator->name(), "greater");
        EXPECT_EQ(rs_operator->getAttrs().size(), 1);
        EXPECT_EQ(rs_operator->getAttrs()[0].col_name, "col_date");
        EXPECT_EQ(rs_operator->getAttrs()[0].col_id, 6);
        EXPECT_EQ(
            rs_operator->toDebugString(),
            String("{\"op\":\"greater\",\"col\":\"col_date\",\"value\":\"") + toString(origin_time_stamp)
                + String("\"}"));

        Block before_where_block = Block{
            {toVec<UInt64>(
                 "col_timestamp",
                 {1849559496301477888,
                  1849560389654675456,
                  1949560389654675456,
                  1849259496301477888,
                  1849560389654675452,
                  1849559416301477888,
                  1849559496301477833,
                  888439}),
             toVec<UInt64>(
                 "col_datetime",
                 {1849259496301477883,
                  1849559496301477888,
                  0,
                  1,
                  1849259496301477888,
                  1849559496301477888,
                  667,
                  888439}),
             toVec<Int64>(
                 "col_date",
                 {-1849559496301477888,
                  1849560046057291779,
                  0,
                  121,
                  1849560046057291798,
                  1849259496301477888,
                  667,
                  888439})}};
        EXPECT_TRUE(!filter->extra_cast);
        filter->before_where->execute(before_where_block);
        EXPECT_EQ(before_where_block.rows(), 8);
        auto & col = before_where_block.getByName(filter->filter_column_name).column;
        const auto * concrete_column = typeid_cast<const ColumnUInt8 *>(&(*col));
        EXPECT_EQ(countBytesInFilter(concrete_column->getData()), 3);
        EXPECT_EQ(filter->filter_columns->size(), 1);
    }
}
CATCH

} // namespace DB::tests
