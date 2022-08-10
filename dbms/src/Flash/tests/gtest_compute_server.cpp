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

#include <TestUtils/MPPTaskTestUtils.h>

namespace DB
{
namespace tests
{
class ComputeServerRunner : public DB::tests::MPPTaskTestUtils
{
public:
    static void SetUpTestCase()
    {
        MPPTaskTestUtils::SetUpTestCase();
        MockComputeServerManager::instance().addServer("0.0.0.0:3930");
        MockComputeServerManager::instance().addServer("0.0.0.0:3931");
        MockComputeServerManager::instance().addServer("0.0.0.0:3932");
        MockComputeServerManager::instance().addServer("0.0.0.0:3933");
        MockComputeServerManager::instance().startServers(log_ptr, TiFlashTestEnv::getGlobalContext());
    }
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        /// for agg
        context.addMockTable(
            {"test_db", "test_table_1"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", {1, {}, 10000000, 10000000}), toNullableVec<String>("s2", {"apple", {}, "banana", "test"}), toNullableVec<String>("s3", {"apple", {}, "banana", "test"})});

        /// for join
        context.addMockTable(
            {"test_db", "l_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});
        context.addMockTable(
            {"test_db", "r_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});
    }
};

TEST_F(ComputeServerRunner, runAggTasks)
try
{
    {
        auto tasks = context.scan("test_db", "test_table_1")
                         .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                         .project({"max(s1)"})
                         .buildMPPTasksForMultipleServer(context);

        auto expected_cols = {toNullableVec<Int32>({1, {}, 10000000, 10000000})};
        ASSERT_MPPTASK_EQUAL(tasks, expected_cols);
    }
}
CATCH

TEST_F(ComputeServerRunner, runJoinTasks)
try
{
    auto tasks = context
                     .scan("test_db", "l_table")
                     .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                     .buildMPPTasksForMultipleServer(context);

    auto expected_cols = {
        toNullableVec<String>({{}, "banana", "banana"}),
        toNullableVec<String>({{}, "apple", "banana"}),
        toNullableVec<String>({{}, "banana", "banana"}),
        toNullableVec<String>({{}, "apple", "banana"})};

    ASSERT_MPPTASK_EQUAL(tasks, expected_cols);
}
CATCH

} // namespace tests
} // namespace DB
