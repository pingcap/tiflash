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

TEST_F(MockDAGRequestTest, Test1)
try
{
    MockTableName right_table{"r_db", "r_table"};
    std::vector<MockColumnInfo> r_columns;
    r_columns.emplace_back("r_a", TiDB::TP::TypeString);
    r_columns.emplace_back("r_b", TiDB::TP::TypeString);

    MockTableName left_table{"l_db", "l_table"};
    std::vector<MockColumnInfo> l_columns;
    l_columns.emplace_back("l_a", TiDB::TP::TypeString);
    l_columns.emplace_back("l_b", TiDB::TP::TypeLong);

    DAGRequestBuilder right_builder;
    right_builder
        .mockTable(right_table, r_columns)
        .filter(eq(col("r_a"), col("r_b")))
        .project({col("r_a")})
        .topN("r_a", false, 20);


    DAGRequestBuilder left_builder;
    left_builder
        .mockTable(left_table, l_columns)
        .topN({{"l_a", false}}, 10)
        .join(right_builder, col("l_a"), ASTTableJoin::Kind::Left)
        .limit(10);

    auto dag_request = left_builder.build(context);
    String to_tree_string = toTreeString(dag_request.get());
    String expected_string = "limit_7\n"
                             " topn_5\n"
                             "  table_scan_4\n"
                             " topn_3\n"
                             "  project_2\n"
                             "   selection_1\n"
                             "    table_scan_0\n";
    ASSERT_EQ(trim(to_tree_string), trim(expected_string));
}
CATCH

TEST_F(MockDAGRequestTest, Test2)
try
{
    MockTableName right_table{"r_db", "r_table"};
    std::vector<MockColumnInfo> r_columns;
    r_columns.emplace_back("r_a", TiDB::TP::TypeString);
    r_columns.emplace_back("r_b", TiDB::TP::TypeString);

    MockTableName left_table{"l_db", "l_table"};
    std::vector<MockColumnInfo> l_columns;
    l_columns.emplace_back("l_a", TiDB::TP::TypeString);
    l_columns.emplace_back("l_b", TiDB::TP::TypeLong);

    DAGRequestBuilder right_builder;
    right_builder
        .mockTable(right_table, r_columns)
        .filter(eq(col("r_a"), col("r_b")))
        .project({col("r_a")})
        .topN("r_a", false, 20);


    DAGRequestBuilder join_builder;
    join_builder
        .mockTable(left_table, l_columns)
        .topN({{"l_a", false}}, 10)
        .aggregation({Max(col("l_a"), col("l_b"))}, {col("l_a")})
        .join(right_builder, col("l_a"), ASTTableJoin::Kind::Left)
        .limit(10);
    auto dag_request = join_builder.build(context);
    auto to_tree_string = toTreeString(dag_request.get());
    String expected_string = "limit_8\n"
                             " aggregation_6\n"
                             "  topn_5\n"
                             "   table_scan_4\n"
                             " topn_3\n"
                             "  project_2\n"
                             "   selection_1\n"
                             "    table_scan_0\n";
    ASSERT_EQ(trim(to_tree_string), trim(expected_string));
}
CATCH

} // namespace tests
} // namespace DB