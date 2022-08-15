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
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        /// for agg
        context.addMockTable(
            {"test_db", "test_table_1"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", {1, {}, 10000000}), toNullableVec<String>("s2", {"apple", {}, "banana"}), toNullableVec<String>("s3", {"apple", {}, "banana"})});

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
                         .buildMPPTasks(context);

        size_t task_size = tasks.size();

        std::vector<String> expected_strings = {
            "exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}\n"
            " aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}\n"
            "  table_scan_0 | {<0, Long>, <1, String>, <2, String>}\n",
            "exchange_sender_3 | type:PassThrough, {<0, Long>}\n"
            " project_2 | {<0, Long>}\n"
            "  aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}\n"
            "   exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}\n"};
        for (size_t i = 0; i < task_size; ++i)
        {
            ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
        }

        auto expected_cols = {toNullableVec<Int32>({1, {}, 10000000})};
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
                     .topN("join_c", false, 2)
                     .buildMPPTasks(context);

    size_t task_size = tasks.size();
    std::vector<String> expected_strings = {
        "exchange_sender_6 | type:Hash, {<0, String>, <1, String>}\n"
        " table_scan_1 | {<0, String>, <1, String>}",
        "exchange_sender_5 | type:Hash, {<0, String>, <1, String>}\n"
        " table_scan_0 | {<0, String>, <1, String>}",
        "exchange_sender_4 | type:PassThrough, {<0, String>, <1, String>, <2, String>}\n"
        " topn_3 | order_by: {(<1, String>, desc: false)}, limit: 2\n"
        "  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
        "   exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>}\n"
        "   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>}"};
    for (size_t i = 0; i < task_size; ++i)
    {
        ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
    }

    auto expected_cols = {
        toNullableVec<String>({{}, "banana"}),
        toNullableVec<String>({{}, "apple"}),
        toNullableVec<String>({{}, {}})};
    ASSERT_MPPTASK_EQUAL(tasks, expected_cols);
}
CATCH

} // namespace tests
} // namespace DB
