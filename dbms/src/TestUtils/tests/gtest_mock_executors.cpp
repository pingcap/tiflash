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

#include <Interpreters/Context.h>
#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <tipb/executor.pb.h>

namespace DB
{
namespace tests
{
class MockDAGRequestTest : public DB::tests::MockExecutorTest
{
public:
    void initializeContext() override
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        context = MockDAGRequestContext(TiFlashTestEnv::getContext());

        context.addMockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "test_table_1"}, {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "r_table"}, {{"r_a", TiDB::TP::TypeLong}, {"r_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "l_table"}, {{"l_a", TiDB::TP::TypeLong}, {"l_b", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addExchangeRelationSchema("sender_1", {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
    }
};

TEST_F(MockDAGRequestTest, MockTable)
try
{
    auto request = context.scan("test_db", "test_table").build(context);
    {
        String expected_string = "table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
    request = context.scan("test_db", "test_table_1").build(context);
    {
        String expected_string = "table_scan_0 | columns:{index: 0, type: Long, index: 1, type: String, index: 2, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Filter)
try
{
    auto request = context.scan("test_db", "test_table").filter(eq(col("s1"), col("s2"))).build(context);
    {
        String expected_string = "selection_1 | equals(index: 0, type: String, index: 1, type: String)}\n"
                                 " table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
    request = context.scan("test_db", "test_table_1")
                  .filter(And(eq(col("s1"), col("s2")), lt(col("s2"), lt(col("s1"), col("s2")))))
                  .build(context);
    {
        String expected_string = "selection_1 | equals(index: 0, type: Long, index: 1, type: String) and less(index: 1, type: String, less(index: 0, type: Long, index: 1, type: String))}\n"
                                 " table_scan_0 | columns:{index: 0, type: Long, index: 1, type: String, index: 2, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Projection)
try
{
    auto request = context.scan("test_db", "test_table")
                       .project("s1")
                       .build(context);
    {
        String expected_string = "project_1 | columns:{index: 0, type: String}\n"
                                 " table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
    request = context.scan("test_db", "test_table_1")
                  .project({col("s3"), eq(col("s1"), col("s2"))})
                  .build(context);
    {
        String expected_string = "project_1 | columns:{index: 2, type: String, equals(index: 0, type: Long, index: 1, type: String)}\n"
                                 " table_scan_0 | columns:{index: 0, type: Long, index: 1, type: String, index: 2, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
    request = context.scan("test_db", "test_table_1")
                  .project({"s1", "s2"})
                  .build(context);
    {
        String expected_string = "project_1 | columns:{index: 0, type: Long, index: 1, type: String}\n"
                                 " table_scan_0 | columns:{index: 0, type: Long, index: 1, type: String, index: 2, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Limit)
try
{
    auto request = context.scan("test_db", "test_table")
                       .limit(10)
                       .build(context);
    {
        String expected_string = "limit_1 | 10\n"
                                 " table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
    request = context.scan("test_db", "test_table_1")
                  .limit(lit(Field(static_cast<UInt64>(10))))
                  .build(context);
    {
        String expected_string = "limit_1 | 10\n"
                                 " table_scan_0 | columns:{index: 0, type: Long, index: 1, type: String, index: 2, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, TopN)
try
{
    auto request = context.scan("test_db", "test_table")
                       .topN({{"s1", false}}, 10)
                       .build(context);
    {
        String expected_string = "topn_1 | order_by: columns{index: 0, type: String, desc: true}, limit: 10\n"
                                 " table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
    request = context.scan("test_db", "test_table")
                  .topN("s1", false, 10)
                  .build(context);
    {
        String expected_string = "topn_1 | order_by: columns{index: 0, type: String, desc: true}, limit: 10\n"
                                 " table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, Aggregation)
try
{
    auto request = context.scan("test_db", "test_table")
                       .aggregation(Max(col("s1")), col("s2"))
                       .build(context);
    {
        String expected_string = "aggregation_1 | group_by: columns:{index: 1, type: String}, agg_func:{max(index: 0, type: String)}\n"
                                 " table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
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
                                         .join(right_builder, {col("join_c")}, ASTTableJoin::Kind::Left) // todo ensure the join is legal.
                                         .limit(10);
    auto request = left_builder.build(context);
    {
        String expected_string = "limit_8 | 10\n"
                                 " Join_7 | LeftOuterJoin,HashJoin. left_join_keys: {type: String}, right_join_keys: {type: String}\n"
                                 "  topn_6 | order_by: columns{index: 0, type: Long, desc: true}, limit: 10\n"
                                 "   table_scan_5 | columns:{index: 0, type: Long, index: 1, type: String, index: 2, type: String}\n"
                                 "  topn_4 | order_by: columns{index: 2, type: String, desc: true}, limit: 10\n"
                                 "   aggregation_3 | group_by: columns:{index: 2, type: String, index: 1, type: String}, agg_func:{max(index: 0, type: Long)}\n"
                                 "    project_2 | columns:{index: 0, type: Long, index: 1, type: String, index: 2, type: String}\n"
                                 "     selection_1 | equals(index: 0, type: Long, index: 1, type: String) and equals(index: 0, type: Long, index: 1, type: String)}\n"
                                 "      table_scan_0 | columns:{index: 0, type: Long, index: 1, type: String, index: 2, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, ExchangeSender)
try
{
    auto request = context.scan("test_db", "test_table")
                       .exchangeSender(tipb::PassThrough)
                       .build(context);
    {
        String expected_string = "exchange_sender_1 | type:PassThrough, fields:{type: String, type: String}\n"
                                 " table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
    request = context.scan("test_db", "test_table")
                  .topN("s1", false, 10)
                  .exchangeSender(tipb::Broadcast)
                  .build(context);
    {
        String expected_string = "exchange_sender_2 | type:Broadcast, fields:{type: String, type: String}\n"
                                 " topn_1 | order_by: columns{index: 0, type: String, desc: true}, limit: 10\n"
                                 "  table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
    request = context.scan("test_db", "test_table")
                  .project({col("s1"), col("s2")})
                  .exchangeSender(tipb::Hash)
                  .build(context);
    {
        String expected_string = "exchange_sender_2 | type:Hash, fields:{type: String, type: String}\n"
                                 " project_1 | columns:{index: 0, type: String, index: 1, type: String}\n"
                                 "  table_scan_0 | columns:{index: 0, type: String, index: 1, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
}
CATCH

TEST_F(MockDAGRequestTest, ExchangeReceiver)
try
{
    auto request = context.receive("sender_1")
                       .build(context);
    {
        String expected_string = "exchange_receiver_0 | type:PassThrough, fields:{type: String, type: String, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
    request = context.receive("sender_1")
                  .topN("s1", false, 10)
                  .build(context);
    {
        String expected_string = "topn_1 | order_by: columns{index: 0, type: String, desc: true}, limit: 10\n"
                                 " exchange_receiver_0 | type:PassThrough, fields:{type: String, type: String, type: String}\n";
        ASSERT_DAGREQUEST_EQAUL(expected_string, request);
    }
}
CATCH

} // namespace tests
} // namespace DB