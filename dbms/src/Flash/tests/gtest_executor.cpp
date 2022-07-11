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
        ASSERT_COLUMNS_EQ_R(executeStreams(request),
                            createColumns({toNullableVec<String>({"banana"}),
                                           toNullableVec<String>({"banana"})}));
    }

    request = context.receive("exchange1")
                  .filter(eq(col("s1"), col("s2")))
                  .build(context);
    {
        ASSERT_COLUMNS_EQ_R(executeStreams(request),
                            createColumns({toNullableVec<String>({"banana"}),
                                           toNullableVec<String>({"banana"})}));
    }
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
        ASSERT_COLUMNS_EQ_R(executeStreams(request, 2),
                            createColumns({toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"}),
                                           toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"})}));

        ASSERT_COLUMNS_EQ_R(executeStreams(request, 5),
                            createColumns({toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"}),
                                           toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"})}));

        ASSERT_COLUMNS_EQ_R(executeStreams(request),
                            createColumns({toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"}),
                                           toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"})}));
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
        ASSERT_COLUMNS_EQ_R(executeStreams(request, 2),
                            createColumns({toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"})}));
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
        ASSERT_COLUMNS_EQ_R(executeStreams(request, 2),
                            createColumns({toNullableVec<String>({"banana", "banana", "banana", "banana"}),
                                           toNullableVec<String>({"apple", "apple", "apple", "banana"}),
                                           toNullableVec<String>({"banana", "banana", "banana", {}}),
                                           toNullableVec<String>({"apple", "apple", "apple", {}})}));
        ASSERT_COLUMNS_EQ_R(executeStreams(request, 3),
                            createColumns({toNullableVec<String>({"banana", "banana", "banana", "banana"}),
                                           toNullableVec<String>({"apple", "apple", "apple", "banana"}),
                                           toNullableVec<String>({"banana", "banana", "banana", {}}),
                                           toNullableVec<String>({"apple", "apple", "apple", {}})}));
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
        ASSERT_COLUMNS_EQ_R(executeStreams(request, 2),
                            createColumns({toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"}),
                                           toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"})}));

        ASSERT_COLUMNS_EQ_R(executeStreams(request, 5),
                            createColumns({toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"}),
                                           toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"})}));

        ASSERT_COLUMNS_EQ_R(executeStreams(request),
                            createColumns({toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"}),
                                           toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"})}));
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
        ASSERT_COLUMNS_EQ_R(executeStreams(request, 2),
                            createColumns({toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"}),
                                           toNullableVec<String>({"banana", "banana"}),
                                           toNullableVec<String>({"apple", "banana"})}));
    }
}
CATCH

} // namespace tests
} // namespace DB
