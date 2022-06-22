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

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class ExecutorTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable({"test_db", "test_table"},
                             {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                              toNullableVec<String>("s2", {"apple", {}, "banana"})});
        context.addExchangeReceiver("exchange1",
                                    {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                                     toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addExchangeReceiver("exchange_r_table",
                                    {{"s1", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});

        context.addExchangeReceiver("exchange_l_table",
                                    {{"s1", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});

        context.addMockTable({"test_db", "r_table"},
                             {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                             {toVec<String>("s", {"banana", "banana"}),
                              toVec<String>("join_c", {"apple", "banana"})});

        context.addMockTable({"test_db", "r_table_2"},
                             {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                             {toVec<String>("s", {"banana", "banana", "banana"}),
                              toVec<String>("join_c", {"apple", "apple", "apple"})});

        context.addMockTable({"test_db", "l_table"},
                             {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                             {toVec<String>("s", {"banana", "banana"}),
                              toVec<String>("join_c", {"apple", "banana"})});
    }
};

TEST_F(ExecutorTestRunner, Filter)
try
{
    auto request = context
                       .scan("test_db", "test_table")
                       .filter(eq(col("s1"), col("s2")))
                       .build(context);
    {
        executeStreams(request,
                       {toNullableVec<String>({"banana"}),
                        toNullableVec<String>({"banana"})});
    }

    request = context.receive("exchange1")
                  .filter(eq(col("s1"), col("s2")))
                  .build(context);
    {
        executeStreams(request,
                       {toNullableVec<String>({"banana"}),
                        toNullableVec<String>({"banana"})});
    }
}
CATCH

TEST_F(ExecutorTestRunner, Limit)
try
{
    /// Prepare column data
    using ColDataType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColumnWithData = std::vector<ColDataType>;
    const ColumnWithData col0{"col0-0", {}, "col0-2", "col0-3", {}, "col0-5", "col0-6", "col0-7"};
    const size_t col_data_num = col0.size();

    /// Prepare some names
    const String db_name("test_db");
    const String table_name("projection_test_table");
    const String col_name("limit_col");

    /// Create table for the test
    context.addMockTable({db_name, table_name},
                         {{col_name, TiDB::TP::TypeString}},
                         {toNullableVec<String>(col_name, col0)});
    
    size_t concurrency = 1;
    std::shared_ptr<tipb::DAGRequest> request;
    ColumnsWithTypeAndName expect_cols;

    /// Check limit result with various parameters
    for (size_t limit_num = 0; limit_num <= col_data_num + 3; limit_num++)
    {
        request = context
                        .scan(db_name, table_name)
                        .limit(limit_num)
                        .build(context);

        if (limit_num == 0)
            expect_cols = {};
        else if (limit_num > col_data_num)
            expect_cols = {toNullableVec<String>(col_name, ColumnWithData(col0.begin(), col0.end()))};
        else
            expect_cols = {toNullableVec<String>(col_name, ColumnWithData(col0.begin(), col0.begin() + limit_num))};
        
        executeStreams(request, expect_cols, concurrency);
    }
}
CATCH

TEST_F(ExecutorTestRunner, TopN)
try
{
    /// Prepare column data
    using ColDataType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColumnWithData = std::vector<ColDataType>;
    ColumnWithData col0{"col0-0", "col0-1", "col0-2", {}, "col0-4", {}, "col0-6", "col0-7"};
    size_t col_data_num = col0.size();

    /// Prepare some names
    const String db_name("test_db");
    const String table_name("topn_test_table");
    const String col_name("topn_col");

    /// Create table for the test
    context.addMockTable({db_name, table_name},
                         {{col_name, TiDB::TP::TypeString}},
                         {toNullableVec<String>(col_name, col0)});
    
    size_t concurrency = 1;
    std::shared_ptr<tipb::DAGRequest> request;
    ColumnsWithTypeAndName expect_cols;
    
    bool is_desc;

    /// Check topn result with various parameters
    for (size_t i = 1; i <= 1; i++)
    {
        is_desc = static_cast<bool>(i); /// Set descent or ascent
        if (is_desc)
            sort(col0.begin(), col0.end(), std::greater<ColDataType>()); /// Sort col0 for the following comparison
        else
            sort(col0.begin(), col0.end());

        for (size_t limit_num = 0; limit_num <= col_data_num + 3; limit_num++)        
        {
            request = context
                            .scan(db_name, table_name)
                            .topN(col_name, is_desc, limit_num)
                            .build(context);
            
            if (limit_num == 0 || limit_num > col_data_num)
                expect_cols = {toNullableVec<String>(col_name, ColumnWithData(col0.begin(), col0.end()))};
            else
                expect_cols = {toNullableVec<String>(col_name, ColumnWithData(col0.begin(), col0.begin() + limit_num))};

            executeStreams(request, expect_cols, concurrency);
        }
    }
}
CATCH

TEST_F(ExecutorTestRunner, Projection)
try
{
    /// Prepare column data
    using ColDataType = std::vector<std::optional<typename TypeTraits<String>::FieldType>>;
    const ColDataType col0{"col0-0", "col0-1", "col0-2", {}, "col0-4"};
    const ColDataType col1{"col1-0", {}, "col1-2", "col1-3", "col1-4"};
    const ColDataType col2{"col2-0", "col2-1", {}, "col2-3", "col2-4"};

    /// Prepare some names
    std::vector<String> col_names{"proj_col0", "proj_col1", "proj_col2"};
    const String db_name("test_db");
    const String table_name("projection_test_table");

    /// Create table for the test
    context.addMockTable({db_name, table_name},
                         {{col_names[0], TiDB::TP::TypeString},
                          {col_names[1], TiDB::TP::TypeString},
                          {col_names[2], TiDB::TP::TypeString}},
                         {toNullableVec<String>(col_names[0], col0),
                          toNullableVec<String>(col_names[1], col1),
                          toNullableVec<String>(col_names[2], col2)});

    const size_t concurrency = 1;
    auto get_request = [&](MockColumnNames col_names)
    {
        return context.scan(db_name, table_name).project(col_names).build(context);
    };

    /// Start to test projection

    auto request = get_request({col_names[2]});
    executeStreams(request, {toNullableVec<String>(col_names[2], col2)}, concurrency);

    request = get_request({col_names[0], col_names[1]});
    executeStreams(request,
                   {toNullableVec<String>(col_names[0], col0),
                    toNullableVec<String>(col_names[1], col1),},
                    concurrency);
    
    request = get_request({col_names[0], col_names[1], col_names[2]});
    executeStreams(request,
                   {toNullableVec<String>(col_names[0], col0),
                    toNullableVec<String>(col_names[1], col1),
                    toNullableVec<String>(col_names[2], col2)},
                    concurrency);
}
CATCH

TEST_F(ExecutorTestRunner, JoinWithTableScan)
try
{
    auto request = context
                       .scan("test_db", "l_table")
                       .join(context.scan("test_db", "r_table"), {col("join_c")}, ASTTableJoin::Kind::Left)
                       .topN("join_c", false, 2)
                       .build(context);
    {
        String expected = "topn_3 | order_by: {(<1, String>, desc: false)}, limit: 2\n"
                          " Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
                          "  table_scan_0 | {<0, String>, <1, String>}\n"
                          "  table_scan_1 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"}),
                        toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"})},
                       2);

        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"}),
                        toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"})},
                       5);

        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"}),
                        toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"})});
    }
    request = context
                  .scan("test_db", "l_table")
                  .join(context.scan("test_db", "r_table"), {col("join_c")}, ASTTableJoin::Kind::Left)
                  .project({"s", "join_c"})
                  .topN("join_c", false, 2)
                  .build(context);
    {
        String expected = "topn_4 | order_by: {(<1, String>, desc: false)}, limit: 2\n"
                          " project_3 | {<0, String>, <1, String>}\n"
                          "  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
                          "   table_scan_0 | {<0, String>, <1, String>}\n"
                          "   table_scan_1 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"})},
                       2);
    }

    request = context
                  .scan("test_db", "l_table")
                  .join(context.scan("test_db", "r_table_2"), {col("join_c")}, ASTTableJoin::Kind::Left)
                  .topN("join_c", false, 4)
                  .build(context);
    {
        String expected = "topn_3 | order_by: {(<1, String>, desc: false)}, limit: 4\n"
                          " Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
                          "  table_scan_0 | {<0, String>, <1, String>}\n"
                          "  table_scan_1 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana", "banana", "banana"}),
                        toNullableVec<String>({"apple", "apple", "apple", "banana"}),
                        toNullableVec<String>({"banana", "banana", "banana", {}}),
                        toNullableVec<String>({"apple", "apple", "apple", {}})},
                       2);
        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana", "banana", "banana"}),
                        toNullableVec<String>({"apple", "apple", "apple", "banana"}),
                        toNullableVec<String>({"banana", "banana", "banana", {}}),
                        toNullableVec<String>({"apple", "apple", "apple", {}})},
                       3);
    }
}
CATCH

TEST_F(ExecutorTestRunner, JoinWithExchangeReceiver)
try
{
    auto request = context
                       .receive("exchange_l_table")
                       .join(context.receive("exchange_r_table"), {col("join_c")}, ASTTableJoin::Kind::Left)
                       .topN("join_c", false, 2)
                       .build(context);
    {
        String expected = "topn_3 | order_by: {(<1, String>, desc: false)}, limit: 2\n"
                          " Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
                          "  exchange_receiver_0 | type:PassThrough, {<0, String>, <1, String>}\n"
                          "  exchange_receiver_1 | type:PassThrough, {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"}),
                        toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"})},
                       2);

        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"}),
                        toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"})},
                       5);

        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"}),
                        toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"})});
    }
}
CATCH

TEST_F(ExecutorTestRunner, JoinWithTableScanAndReceiver)
try
{
    auto request = context
                       .scan("test_db", "l_table")
                       .join(context.receive("exchange_r_table"), {col("join_c")}, ASTTableJoin::Kind::Left)
                       .topN("join_c", false, 2)
                       .build(context);
    {
        String expected = "topn_3 | order_by: {(<1, String>, desc: false)}, limit: 2\n"
                          " Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
                          "  table_scan_0 | {<0, String>, <1, String>}\n"
                          "  exchange_receiver_1 | type:PassThrough, {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
        executeStreams(request,
                       {toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"}),
                        toNullableVec<String>({"banana", "banana"}),
                        toNullableVec<String>({"apple", "banana"})},
                       2);
    }
}
CATCH

} // namespace tests
} // namespace DB