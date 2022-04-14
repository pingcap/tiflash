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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class MockDAGRequestTest : public DB::tests::MockExecutorTest
{
};

TEST_F(MockDAGRequestTest, MockTable)
try
{
    auto builder = DAGRequestBuilderFactory().createDAGRequestBuilder();
    MockTableName table_name("test_db", "test"); // db_name-->table_name
    std::vector<MockColumnInfo> table_columns;
    table_columns.emplace_back("t_l", TiDB::TP::TypeLong);
    table_columns.emplace_back("t_s", TiDB::TP::TypeString);
    builder.mockTable(table_name, table_columns);
    auto dag_request_1 = builder.build(context);
    String expected_string_1 = "table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string_1, dag_request_1);

    builder.mockTable({"test_db", "test_table"}, {{"t_l", TiDB::TP::TypeLong}, {"t_s", TiDB::TP::TypeString}});
    auto dag_request_2 = builder.build(context);
    String expected_string_2 = "table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string_2, dag_request_2);
}
CATCH

TEST_F(MockDAGRequestTest, Filter)
try
{
    auto builder = DAGRequestBuilderFactory().createDAGRequestBuilder();
    auto request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}})
                       .filter(eq(col("s1"), col("s2")))
                       .build(context);
    String expected_string = "selection_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}})
                  .filter(And(eq(col("s1"), col("s2")), lt(col("s2"), col("s3"))))
                  .build(context);
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, Projection)
try
{
    auto builder = DAGRequestBuilderFactory().createDAGRequestBuilder();
    auto request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}})
                       .project("s1")
                       .build(context);
    String expected_string = "project_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}})
                  .project({col("s3")})
                  .build(context);
    String expected_string_2 = "project_1\n"
                               " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string_2, request);

    request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}})
                  .project({"s1", "s2"})
                  .build(context);
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, Limit)
try
{
    auto builder = DAGRequestBuilderFactory().createDAGRequestBuilder();
    auto request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}})
                       .limit(10)
                       .build(context);
    String expected_string = "limit_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}})
                  .limit(lit(Field(static_cast<UInt64>(10))))
                  .build(context);
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, TopN)
try
{
    auto builder = DAGRequestBuilderFactory().createDAGRequestBuilder();
    auto request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}})
                       .topN({{"s1", false}}, 10)
                       .build(context);
    String expected_string = "topn_1\n"
                             " table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);

    request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}})
                  .topN("s1", false, 10)
                  .build(context);
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH

TEST_F(MockDAGRequestTest, Aggregation)
try
{
    auto builder = DAGRequestBuilderFactory().createDAGRequestBuilder();
    auto request = builder.mockTable({"test_db", "test_table"}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}})
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
    auto builder_factory = DAGRequestBuilderFactory();
    DAGRequestBuilder right_builder = builder_factory.createDAGRequestBuilder();
    right_builder
        .mockTable({"r_db", "r_table"}, {{"r_a", TiDB::TP::TypeString}, {"r_b", TiDB::TP::TypeString}})
        .filter(eq(col("r_a"), col("r_b")))
        .project({col("r_a"), col("r_b")})
        .aggregation(Max(col("r_a")), col("r_b"));


    DAGRequestBuilder left_builder = builder_factory.createDAGRequestBuilder();
    left_builder
        .mockTable({"l_db", "l_table"}, {{"l_a", TiDB::TP::TypeString}, {"l_b", TiDB::TP::TypeString}})
        .topN({{"l_a", false}}, 10)
        .join(right_builder, col("l_a"), ASTTableJoin::Kind::Left)
        .limit(10);

    auto request = left_builder.build(context);
    String expected_string = "limit_7\n"
                             " topn_5\n"
                             "  table_scan_4\n"
                             " aggregation_3\n"
                             "  project_2\n"
                             "   selection_1\n"
                             "    table_scan_0\n";
    ASSERT_DAGREQUEST_EQAUL(expected_string, request);
}
CATCH


} // namespace tests
} // namespace DB