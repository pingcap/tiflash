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
class SplitTaskTest : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        enablePipeline(false);
        context.addMockTable(
            {"test_db", "test_table_1"},
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "l_table"}, {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
        context.addMockTable({"test_db", "r_table"}, {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}});
    }
};

TEST_F(SplitTaskTest, aggregation)
try
{
    auto tasks = context.scan("test_db", "test_table_1")
                     .filter(eq(col("s2"), col("s3")))
                     .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                     .filter(eq(col("s2"), col("s3")))
                     .topN("s2", false, 10)
                     .buildMPPTasks(context);

    const auto task_size = tasks.size();
    std::vector<String> executors
        = {"exchange_sender_7 | type:Hash, {<0, String>, <1, String>, <2, String>}\n"
           " aggregation_6 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, String>)}\n"
           "  selection_1 | equals(<1, String>, <2, String>)}\n"
           "   table_scan_0 | {<0, String>, <1, String>, <2, String>}",
           "exchange_sender_5 | type:PassThrough, {<0, String>, <1, String>, <2, String>}\n"
           " topn_4 | order_by: {(<1, String>, desc: false)}, limit: 10\n"
           "  selection_3 | equals(<1, String>, <2, String>)}\n"
           "   aggregation_2 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, String>)}\n"
           "    exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>, <2, String>}"};
    for (size_t i = 0; i < task_size; ++i)
    {
        ASSERT_DAGREQUEST_EQAUL(executors[i], tasks[i].dag_request);
    }

    std::vector<String> streams
        = {"MockExchangeSender\n"
           " Expression: <final projection>\n"
           "  Expression: <expr after aggregation>\n"
           "   Aggregating\n"
           "    Expression: <before aggregation>\n"
           "     Filter\n"
           "      MockTableScan",
           "MockExchangeSender\n"
           " Expression: <final projection>\n"
           "  MergeSorting, limit = 10\n"
           "   PartialSorting: limit = 10\n"
           "    Expression: <before TopN>\n"
           "     Filter\n"
           "      Expression: <expr after aggregation>\n"
           "       Aggregating\n"
           "        MockExchangeReceiver"};
    for (size_t i = 0; i < task_size; ++i)
    {
        ASSERT_BLOCKINPUTSTREAM_EQAUL(streams[i], tasks[i].dag_request, 1);
    }
}
CATCH

TEST_F(SplitTaskTest, join)
try
{
    auto tasks = context.scan("test_db", "l_table")
                     .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                     .topN("join_c", false, 2)
                     .buildMPPTasks(context);

    const auto task_size = tasks.size();
    std::vector<String> executors
        = {"exchange_sender_6 | type:Hash, {<0, String>, <1, String>}\n"
           " table_scan_1 | {<0, String>, <1, String>}",
           "exchange_sender_5 | type:Hash, {<0, String>, <1, String>}\n"
           " table_scan_0 | {<0, String>, <1, String>}",
           "exchange_sender_4 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}\n"
           " topn_3 | order_by: {(<1, String>, desc: false)}, limit: 2\n"
           "  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}\n"
           "   exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>}\n"
           "   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>}"};
    for (size_t i = 0; i < task_size; ++i)
    {
        ASSERT_DAGREQUEST_EQAUL(executors[i], tasks[i].dag_request);
    }

    std::vector<String> res
        = {"MockExchangeSender\n"
           " Expression: <final projection>\n"
           "  MockTableScan",
           "MockExchangeSender\n"
           " Expression: <final projection>\n"
           "  MockTableScan",
           "CreatingSets\n"
           " HashJoinBuild: <join build, build_side_root_executor_id = exchange_receiver_8>, join_kind = Left\n"
           "  Expression: <final projection>\n"
           "   MockExchangeReceiver\n"
           " MockExchangeSender\n"
           "  Expression: <final projection>\n"
           "   MergeSorting, limit = 2\n"
           "    PartialSorting: limit = 2\n"
           "     HashJoinProbe: <join probe, join_executor_id = Join_2, scan_hash_map_after_probe = false>\n"
           "      Expression: <final projection>\n"
           "       MockExchangeReceiver"};
    for (size_t i = 0; i < task_size; ++i)
    {
        ASSERT_BLOCKINPUTSTREAM_EQAUL(res[i], tasks[i].dag_request, 1);
    }
}
CATCH

} // namespace tests
} // namespace DB
