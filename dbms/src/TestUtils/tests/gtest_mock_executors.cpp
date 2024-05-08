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

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class MockDAGRequestTest : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}});
        context.addMockTable(
            {"test_db", "test_table_1"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addMockTable(
            {"test_db", "r_table"},
            {{"r_a", TiDB::TP::TypeLong}, {"r_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addMockTable(
            {"test_db", "l_table"},
            {{"l_a", TiDB::TP::TypeLong}, {"l_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addExchangeReceiver(
            "sender_1",
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
    }
};

TEST_F(MockDAGRequestTest, MockTable)
try
{
    auto request = context.scan("test_db", "test_table").build(context);
    {
        String expected = "table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }


    request = context.scan("test_db", "test_table_1").build(context);
    {
        String expected = "table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Filter)
try
{
    auto request = context.scan("test_db", "test_table").filter(eq(col("s1"), col("s2"))).build(context);
    {
        String expected = "selection_1 | equals(<0, String>, <1, String>)}\n"
                          " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table_1")
                  .filter(And(eq(col("s1"), col("s2")), lt(col("s2"), col("s2")))) // type in lt must be same
                  .build(context);
    {
        String expected = "selection_1 | equals(<0, Long>, <1, String>) and less(<1, String>, <1, String>)}\n"
                          " table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table_1")
                  .filter(
                      And(eq(col("test_table_1.s1"), col("test_table_1.s2")),
                          lt(col("test_table_1.s2"), col("test_table_1.s2"))))
                  .build(context);
    {
        String expected = "selection_1 | equals(<0, Long>, <1, String>) and less(<1, String>, <1, String>)}\n"
                          " table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Projection)
try
{
    auto request = context.scan("test_db", "test_table").project({"s1"}).build(context);
    {
        String expected = "project_1 | {<0, String>}\n"
                          " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table_1").project({col("s3"), eq(col("s1"), col("s2"))}).build(context);
    {
        String expected = "project_1 | {<2, String>, equals(<0, Long>, <1, String>)}\n"
                          " table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table_1").project({"s1", "s2"}).build(context);
    {
        String expected = "project_1 | {<0, Long>, <1, String>}\n"
                          " table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table_1").project({"test_table_1.s1", "test_table_1.s2"}).build(context);
    {
        String expected = "project_1 | {<0, Long>, <1, String>}\n"
                          " table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Limit)
try
{
    auto request = context.scan("test_db", "test_table").limit(10).build(context);
    {
        String expected = "limit_1 | 10\n"
                          " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table_1").limit(lit(Field(static_cast<UInt64>(10)))).build(context);
    {
        String expected = "limit_1 | 10\n"
                          " table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, TopN)
try
{
    auto request = context.scan("test_db", "test_table").topN({{"s1", false}, {"s2", true}}, 10).build(context);
    {
        String expected = "topn_1 | order_by: {(<0, String>, desc: false), (<1, String>, desc: true)}, limit: 10\n"
                          " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table").topN("s1", false, 10).build(context);
    {
        String expected = "topn_1 | order_by: {(<0, String>, desc: false)}, limit: 10\n"
                          " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table").topN("test_table.s1", false, 10).build(context);
    {
        String expected = "topn_1 | order_by: {(<0, String>, desc: false)}, limit: 10\n"
                          " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Aggregation)
try
{
    auto request = context.scan("test_db", "test_table").aggregation(Max(col("s1")), col("s2")).build(context);
    {
        String expected = "aggregation_1 | group_by: {<1, String>}, agg_func: {max(<0, String>)}\n"
                          " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table")
                  .aggregation({Max(col("s1"))}, {col("s2"), lt(col("s1"), col("s2"))})
                  .build(context);
    {
        String expected
            = "aggregation_1 | group_by: {<1, String>, less(<0, String>, <1, String>)}, agg_func: {max(<0, String>)}\n"
              " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    request = context.scan("test_db", "test_table")
                  .aggregation(
                      {Max(col("test_table.s1"))},
                      {col("test_table.s2"), lt(col("test_table.s1"), col("test_table.s2"))})
                  .build(context);
    {
        String expected
            = "aggregation_1 | group_by: {<1, String>, less(<0, String>, <1, String>)}, agg_func: {max(<0, String>)}\n"
              " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Join)
try
{
    DAGRequestBuilder right_builder = context.scan("test_db", "r_table")
                                          .filter(And(eq(col("r_a"), col("r_b")), eq(col("r_a"), col("r_b"))))
                                          .project({col("r_a"), col("r_b"), col("join_c")})
                                          .aggregation({Max(col("r_a"))}, {col("join_c"), col("r_b")})
                                          .topN({{"r_b", false}}, 10);

    DAGRequestBuilder left_builder = context.scan("test_db", "l_table")
                                         .topN({{"l_a", false}}, 10)
                                         .join(right_builder, tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                                         .limit(10);
    auto request = left_builder.build(context);
    {
        String expected
            = "limit_8 | 10\n"
              " Join_7 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
              "  topn_6 | order_by: {(<0, Long>, desc: false)}, limit: 10\n"
              "   table_scan_5 | {<0, Long>, <1, String>, <2, String>}\n"
              "  topn_4 | order_by: {(<2, String>, desc: false)}, limit: 10\n"
              "   aggregation_3 | group_by: {<2, String>, <1, String>}, agg_func: {max(<0, Long>)}\n"
              "    project_2 | {<0, Long>, <1, String>, <2, String>}\n"
              "     selection_1 | equals(<0, Long>, <1, String>) and equals(<0, Long>, <1, String>)}\n"
              "      table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    {
        auto right_builder
            = context.scan("test_db", "r_table")
                  .filter(And(eq(col("r_table.r_a"), col("r_table.r_b")), eq(col("r_table.r_a"), col("r_table.r_b"))))
                  .project({col("r_table.r_a"), col("r_table.r_b"), col("join_c")})
                  .aggregation({Max(col("r_table.r_a"))}, {col("join_c"), col("r_table.r_b")})
                  .topN({{"r_table.r_b", false}}, 10);

        auto left_builder = context.scan("test_db", "l_table")
                                .topN({{"l_table.l_a", false}}, 10)
                                .join(
                                    right_builder,
                                    tipb::JoinType::TypeLeftOuterJoin,
                                    {col("join_c")}) // todo ensure the join is legal.
                                .limit(10);
        request = left_builder.build(context);
        {
            String expected
                = "limit_8 | 10\n"
                  " Join_7 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
                  "  topn_6 | order_by: {(<0, Long>, desc: false)}, limit: 10\n"
                  "   table_scan_5 | {<0, Long>, <1, String>, <2, String>}\n"
                  "  topn_4 | order_by: {(<2, String>, desc: false)}, limit: 10\n"
                  "   aggregation_3 | group_by: {<2, String>, <1, String>}, agg_func: {max(<0, Long>)}\n"
                  "    project_2 | {<0, Long>, <1, String>, <2, String>}\n"
                  "     selection_1 | equals(<0, Long>, <1, String>) and equals(<0, Long>, <1, String>)}\n"
                  "      table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n";
            ASSERT_DAGREQUEST_EQAUL(expected, request);
        }
    }
}
CATCH

TEST_F(MockDAGRequestTest, ExchangeSender)
try
{
    auto request = context.scan("test_db", "test_table").exchangeSender(tipb::PassThrough).build(context);
    {
        String expected = "exchange_sender_1 | type:PassThrough, {<0, String>, <1, String>}\n"
                          " table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
    request
        = context.scan("test_db", "test_table").topN("s1", false, 10).exchangeSender(tipb::Broadcast).build(context);
    {
        String expected = "exchange_sender_2 | type:Broadcast, {<0, String>, <1, String>}\n"
                          " topn_1 | order_by: {(<0, String>, desc: false)}, limit: 10\n"
                          "  table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
    request = context.scan("test_db", "test_table")
                  .project({col("s1"), col("s2")})
                  .exchangeSender(tipb::Hash)
                  .build(context);
    {
        String expected = "exchange_sender_2 | type:Hash, {<0, String>, <1, String>}\n"
                          " project_1 | {<0, String>, <1, String>}\n"
                          "  table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, ExchangeReceiver)
try
{
    auto request = context.receive("sender_1").build(context);
    {
        String expected = "exchange_receiver_0 | type:PassThrough, {<0, String>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
    request = context.receive("sender_1").topN("s1", false, 10).build(context);
    {
        String expected = "topn_1 | order_by: {(<0, String>, desc: false)}, limit: 10\n"
                          " exchange_receiver_0 | type:PassThrough, {<0, String>, <1, String>, <2, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Expand)
try
{
    auto request = context.scan("test_db", "test_table")
                       .expand(MockVVecColumnNameVec{
                           MockVecColumnNameVec{
                               MockColumnNameVec{"s1"},
                           },
                           MockVecColumnNameVec{
                               MockColumnNameVec{"s2"},
                           },
                       })
                       .build(context);
    {
        String expected = "expand_1 | expanded_by: [<{<0, String>}><{<1, String>}>]\n"
                          " table_scan_0 | {<0, String>, <1, String>}";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
    request = context.receive("sender_1")
                  .expand(MockVVecColumnNameVec{
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s1"},
                      },
                      MockVecColumnNameVec{
                          MockColumnNameVec{"s2"},
                      },
                  })
                  .build(context);
    {
        String expected = "expand_1 | expanded_by: [<{<0, String>}><{<1, String>}>]\n"
                          " exchange_receiver_0 | type:PassThrough, {<0, String>, <1, String>, <2, String>}";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, MockWindow)
try
{
    auto request = context.scan("test_db", "test_table")
                       .sort({"s1", false}, true)
                       .window(RowNumber(), {"s1", true}, {"s2", false}, buildDefaultRowsFrame())
                       .build(context);
    {
        String expected
            = "window_2 | partition_by: {(<1, String>, desc: false)}}, order_by: {(<0, String>, desc: "
              "true)}, func_desc: {row_number()}, frame: {start<CurrentRow, false, 0>, end<CurrentRow, false, 0>}\n"
              " sort_1 | isPartialSort: true, partition_by: {(<0, String>, desc: false)}\n"
              "  table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }


    request = context.scan("test_db", "test_table")
                  .sort({"test_table.s1", false}, true)
                  .window(RowNumber(), {"test_table.s1", true}, {"test_table.s2", false}, buildDefaultRowsFrame())
                  .build(context);
    {
        String expected
            = "window_2 | partition_by: {(<1, String>, desc: false)}}, order_by: {(<0, String>, desc: "
              "true)}, func_desc: {row_number()}, frame: {start<CurrentRow, false, 0>, end<CurrentRow, false, 0>}\n"
              " sort_1 | isPartialSort: true, partition_by: {(<0, String>, desc: false)}\n"
              "  table_scan_0 | {<0, String>, <1, String>}\n";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, ListBase)
try
{
    {
        auto request = context.scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .aggregation(Max(col("s1")), col("s2"))
                           .filter(eq(col("s2"), lit(Field("1", 1))))
                           .limit(10)
                           .build(context, DAGRequestType::list);
        String expected = R"(
Limit | 10
 Selection | equals(<1, String>, <'1', String>)}
  Aggregation | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   Selection | equals(<0, String>, <1, String>)}
    TableScan | {<0, String>, <1, String>})";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }

    {
        auto request = context.scan("test_db", "test_table")
                           .filter(eq(col("s1"), col("s2")))
                           .aggregation(Max(col("s1")), col("s2"))
                           .filter(eq(col("s2"), lit(Field("1", 1))))
                           .topN("s2", false, 10)
                           .build(context, DAGRequestType::list);
        String expected = R"(
TopN | order_by: {(<1, String>, desc: false)}, limit: 10
 Selection | equals(<1, String>, <'1', String>)}
  Aggregation | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   Selection | equals(<0, String>, <1, String>)}
    TableScan | {<0, String>, <1, String>})";
        ASSERT_DAGREQUEST_EQAUL(expected, request);
    }
}
CATCH
} // namespace tests
} // namespace DB
