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
#include <TestUtils/mockExecutor.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{

    namespace
{
String toTreeString(const tipb::Executor & root_executor, size_t level = 0);

String toTreeString(const tipb::DAGRequest * dag_request)
{
    assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
    if (dag_request->has_root_executor())
    {
        return toTreeString(dag_request->root_executor());
    }
    else
    {
        FmtBuffer buffer;
        String prefix;
        traverseExecutors(dag_request, [&buffer, &prefix](const tipb::Executor & executor) {
            assert(executor.has_executor_id());
            buffer.fmtAppend("{}{}\n", prefix, executor.executor_id());
            prefix.append(" ");
            return true;
        });
        return buffer.toString();
    }
}

String toTreeString(const tipb::Executor & root_executor, size_t level)
{
    FmtBuffer buffer;

    auto append_str = [&buffer, &level](const tipb::Executor & executor) {
        assert(executor.has_executor_id());
        for (size_t i = 0; i < level; ++i)
            buffer.append(" ");
        buffer.append(executor.executor_id()).append("\n");
    };

    traverseExecutorTree(root_executor, [&](const tipb::Executor & executor) {
        if (executor.has_join())
        {
            for (const auto & child : executor.join().children())
                buffer.append(toTreeString(child, level));
            return false;
        }
        else
        {
            append_str(executor);
            ++level;
            return true;
        }
    });

    return buffer.toString();
}

// String & trim(String & str)
// {
//     if (str.empty())
//     {
//         return str;
//     }

//     str.erase(0, str.find_first_not_of(' '));
//     str.erase(str.find_last_not_of(' ') + 1);
//     return str;
// }
} // namespace
class MockTiPBDAGRequestTest : public DB::tests::FunctionTest
{
};

TEST_F(MockTiPBDAGRequestTest, Test1)
try
{
    String empty_str;

    MockTableName right_table{"r_db", "r_table"};
    std::vector<MockColumnInfo> r_columns;
    r_columns.emplace_back("l_a", TiDB::TP::TypeString);
    r_columns.emplace_back("l_b", TiDB::TP::TypeString);

    MockTableName left_table{"l_db", "l_table"};
    std::vector<MockColumnInfo> l_columns;
    l_columns.emplace_back("l_a", TiDB::TP::TypeString);
    l_columns.emplace_back("l_b", TiDB::TP::TypeLong);

    DAGRequestBuilder right_builder;
    right_builder
        .mockTable(right_table, r_columns)
        .filter(eq(col("l_a"), col("l_b")))
        .project({col("l_a")})
        .topN("l_a", false, 20);


    DAGRequestBuilder left_builder;
    left_builder
        .mockTable(left_table, l_columns)
        .topN({{"l_a",false}}, 10)
        .join(right_builder, col("l_a")) // ywq todo more types of join, should make sure have both field
        .limit(10);

    auto dag_request = left_builder.build(context);
    String to_tree_string = toTreeString(dag_request.get());
    std::cout << to_tree_string << std::endl;
}
CATCH

TEST_F(MockTiPBDAGRequestTest, Test2)
try
{
    String empty_str;

    MockTableName right_table{"r_db", "r_table"};
    std::vector<MockColumnInfo> r_columns;
    r_columns.emplace_back("l_a", TiDB::TP::TypeString);
    r_columns.emplace_back("l_b", TiDB::TP::TypeString);

    MockTableName left_table{"l_db", "l_table"};
    std::vector<MockColumnInfo> l_columns;
    l_columns.emplace_back("l_a", TiDB::TP::TypeString);
    l_columns.emplace_back("l_b", TiDB::TP::TypeLong);

    DAGRequestBuilder right_builder;
    right_builder
        .mockTable(right_table, r_columns)
        .filter(eq(col("l_a"), col("l_b")))
        .project({col("l_a")})
        .topN("l_a", false, 20);


    DAGRequestBuilder left_builder;
    left_builder
        .mockTable(left_table, l_columns)
        .topN("l_a", false, 10)
        .join(right_builder, col("l_a"))
        .limit(10);

    auto dag_request = left_builder.build(context);
    String to_tree_string = toTreeString(dag_request.get());
    std::cout << to_tree_string << std::endl;

}
CATCH


} // namespace tests
} // namespace DB