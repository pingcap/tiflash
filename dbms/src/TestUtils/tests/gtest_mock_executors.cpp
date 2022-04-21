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
        context.addMockTable({"test_db", "r_table"}, {{"r_a", TiDB::TP::TypeLong}, {"r_b", TiDB::TP::TypeString}, {"r_c", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "l_table"}, {{"l_a", TiDB::TP::TypeLong}, {"l_b", TiDB::TP::TypeString}, {"l_c", TiDB::TP::TypeString}});
        context.addExchangeRelationSchema("sender_1", {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
    }
};

TEST_F(MockDAGRequestTest, MockTable)
try
{
    auto request = context.scan("test_db", "test_table").build(context);
    String expected_string_1 = "table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string_1, request);

    request = context.scan("test_db", "test_table_1").build(context);
    String expected_string_2 = "table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string_2, request);
}
CATCH

TEST_F(MockDAGRequestTest, Filter)
try
{
    auto request = context.scan("test_db", "test_table").filter(eq(col("s1"), col("s2"))).build(context);
    String expected_string = "selection_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = context.scan("test_db", "test_table_1")
                  .filter(And(eq(col("s1"), col("s2")), lt(col("s2"), col("s3"))))
                  .build(context);
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, Projection)
try
{
    auto request = context.scan("test_db", "test_table")
                       .project("s1")
                       .build(context);
    String expected_string = "project_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = context.scan("test_db", "test_table_1")
                  .project({col("s3"), eq(col("s1"), col("s2"))})
                  .build(context);
    String expected_string_2 = "project_1\n"
                               " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string_2, request);

    request = context.scan("test_db", "test_table_1")
                  .project({"s1", "s2"})
                  .build(context);
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, Limit)
try
{
    auto request = context.scan("test_db", "test_table")
                       .limit(10)
                       .build(context);
    String expected_string = "limit_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = context.scan("test_db", "test_table_1")
                  .limit(lit(Field(static_cast<UInt64>(10))))
                  .build(context);
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, TopN)
try
{
    auto request = context.scan("test_db", "test_table")
                       .topN({{"s1", false}}, 10)
                       .build(context);
    String expected_string = "topn_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = context.scan("test_db", "test_table")
                  .topN("s1", false, 10)
                  .build(context);
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, Aggregation)
try
{
    auto request = context.scan("test_db", "test_table")
                       .aggregation(Max(col("s1")), col("s2"))
                       .build(context);
    String expected_string = "aggregation_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, Join)
try
{
    DAGRequestBuilder right_builder = context.scan("test_db", "r_table")
                                          .filter(eq(col("r_a"), col("r_b")))
                                          .project({col("r_a"), col("r_b")})
                                          .aggregation(Max(col("r_a")), col("r_b"));


    DAGRequestBuilder left_builder = context.scan("test_db", "l_table")
                                         .topN({{"l_a", false}}, 10)
                                         .join(right_builder, col("l_a"), ASTTableJoin::Kind::Left)
                                         .limit(10);

    auto request = left_builder.build(context);
    String expected_string = "limit_7\n"
                             " Join_6\n"
                             "  topn_5\n"
                             "   table_scan_4\n"
                             "  aggregation_3\n"
                             "   project_2\n"
                             "    selection_1\n"
                             "     table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, ExchangeSender)
try
{
    auto request = context.scan("test_db", "test_table")
                       .exchangeSender(tipb::PassThrough)
                       .build(context);
    String expected_string = "exchange_sender_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = context.scan("test_db", "test_table")
                  .topN("s1", false, 10)
                  .exchangeSender(tipb::Broadcast)
                  .build(context);
    String expected_string_2 = "exchange_sender_2\n"
                               " topn_1\n"
                               "  table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string_2, request);

    request = context.scan("test_db", "test_table")
                  .project({col("s1"), col("s2")})
                  .exchangeSender(tipb::Hash)
                  .build(context);
    String expected_string_3 = "exchange_sender_2\n"
                               " project_1\n"
                               "  table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string_3, request);
}
CATCH

TEST_F(MockDAGRequestTest, ExchangeReceiver)
try
{
    auto request = context.receive("sender_1")
                       .build(context);
    String expected_string = "exchange_receiver_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = context.receive("sender_1")
                  .topN("s1", false, 10)
                  .build(context);
    String expected_string_2 = "topn_1\n"
                               " exchange_receiver_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string_2, request);
}
CATCH

} // namespace tests
} // namespace DB