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

#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <TestUtils/MPPTaskTestUtils.h>

namespace DB
{
namespace tests
{
LoggerPtr MPPTaskTestUtils::log_ptr = nullptr;
size_t MPPTaskTestUtils::server_num = 0;
MPPTestMeta MPPTaskTestUtils::test_meta = {};

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
            {toNullableVec<Int32>("s1", {1, {}, 10000000, 10000000}), toNullableVec<String>("s2", {"apple", {}, "banana", "test"}), toNullableVec<String>("s3", {"apple", {}, "banana", "test"})});

        /// agg table with 200 rows
        std::vector<std::optional<TypeTraits<int>::FieldType>> agg_s1(200);
        std::vector<std::optional<String>> agg_s2(200);
        std::vector<std::optional<String>> agg_s3(200);
        for (size_t i = 0; i < 200; ++i)
        {
            if (i % 30 != 0)
            {
                agg_s1[i] = i % 20;
                agg_s2[i] = {fmt::format("val_{}", i % 10)};
                agg_s3[i] = {fmt::format("val_{}", i)};
            }
        }
        context.addMockTable(
            {"test_db", "test_table_2"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", agg_s1), toNullableVec<String>("s2", agg_s2), toNullableVec<String>("s3", agg_s3)});

        /// for join
        context.addMockTable(
            {"test_db", "l_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});
        context.addMockTable(
            {"test_db", "r_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});

        /// join left table with 200 rows
        std::vector<std::optional<TypeTraits<int>::FieldType>> join_s1(200);
        std::vector<std::optional<String>> join_s2(200);
        std::vector<std::optional<String>> join_s3(200);
        for (size_t i = 0; i < 200; ++i)
        {
            if (i % 20 != 0)
            {
                agg_s1[i] = i % 5;
                agg_s2[i] = {fmt::format("val_{}", i % 6)};
                agg_s3[i] = {fmt::format("val_{}", i)};
            }
        }
        context.addMockTable(
            {"test_db", "l_table_2"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", agg_s1), toNullableVec<String>("s2", agg_s2), toNullableVec<String>("s3", agg_s3)});

        /// join right table with 100 rows
        std::vector<std::optional<TypeTraits<int>::FieldType>> join_r_s1(100);
        std::vector<std::optional<String>> join_r_s2(100);
        std::vector<std::optional<String>> join_r_s3(100);
        for (size_t i = 0; i < 100; ++i)
        {
            if (i % 20 != 0)
            {
                join_r_s1[i] = i % 6;
                join_r_s2[i] = {fmt::format("val_{}", i % 7)};
                join_r_s3[i] = {fmt::format("val_{}", i)};
            }
        }
        context.addMockTable(
            {"test_db", "r_table_2"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", join_r_s1), toNullableVec<String>("s2", join_r_s2), toNullableVec<String>("s3", join_r_s3)});
    }
};


TEST_F(ComputeServerRunner, runAggTasks)
try
{
    startServers(4);
    {
        std::vector<String> expected_strings = {
            R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
 aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
  table_scan_0 | {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_3 | type:PassThrough, {<0, Long>}
 project_2 | {<0, Long>}
  aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
   exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_3 | type:PassThrough, {<0, Long>}
 project_2 | {<0, Long>}
  aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
   exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)",
            R"(
exchange_sender_3 | type:PassThrough, {<0, Long>}
 project_2 | {<0, Long>}
  aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
   exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)",
            R"(exchange_sender_3 | type:PassThrough, {<0, Long>}
 project_2 | {<0, Long>}
  aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
   exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
)"};
        auto expected_cols = {toNullableVec<Int32>({1, {}, 10000000, 10000000})};

        ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
            context
                .scan("test_db", "test_table_1")
                .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                .project({"max(s1)"}),
            expected_strings,
            expected_cols);
    }

    {
        auto properties = getDAGPropertiesForTest(1);
        auto tasks = context
                         .scan("test_db", "test_table_1")
                         .aggregation({Count(col("s1"))}, {})
                         .project({"count(s1)"})
                         .buildMPPTasks(context, properties);
        std::vector<String> expected_strings = {
            R"(exchange_sender_5 | type:PassThrough, {<0, Longlong>}
 aggregation_4 | group_by: {}, agg_func: {count(<0, Long>)}
  table_scan_0 | {<0, Long>}
            )",
            R"(exchange_sender_3 | type:PassThrough, {<0, Longlong>}
 project_2 | {<0, Longlong>}
  aggregation_1 | group_by: {}, agg_func: {sum(<0, Longlong>)}
   exchange_receiver_6 | type:PassThrough, {<0, Longlong>})"};

        size_t task_size = tasks.size();
        for (size_t i = 0; i < task_size; ++i)
        {
            ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
        }
    }
}
CATCH

TEST_F(ComputeServerRunner, runJoinTasks)
try
{
    startServers(3);
    {
        auto expected_cols = {
            toNullableVec<String>({{}, "banana", "banana"}),
            toNullableVec<String>({{}, "apple", "banana"}),
            toNullableVec<String>({{}, "banana", "banana"}),
            toNullableVec<String>({{}, "apple", "banana"})};

        std::vector<String> expected_strings = {
            R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
 table_scan_1 | {<0, String>, <1, String>})",
            R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
 table_scan_1 | {<0, String>, <1, String>})",
            R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
 table_scan_1 | {<0, String>, <1, String>})",
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
 Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
  exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
  exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})",
            R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
 Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
  exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
  exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})",
            R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
 Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
  exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
  exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})"};

        ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(context
                                                 .scan("test_db", "l_table")
                                                 .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}),
                                             expected_strings,
                                             expect_cols);
    }

    {
        auto properties = getDAGPropertiesForTest(1);
        auto tasks = context
                         .scan("test_db", "l_table")
                         .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                         .buildMPPTasks(context, properties);

        std::vector<String> expected_strings = {
            R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
 table_scan_1 | {<0, String>, <1, String>})",
            R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
 Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
  exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
  exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})"};

        size_t task_size = tasks.size();
        for (size_t i = 0; i < task_size; ++i)
        {
            ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
        }
    }
}
CATCH

TEST_F(ComputeServerRunner, runJoinThenAggTasks)
try
{
    startServers(3);
    {
        std::vector<String> expected_strings = {
            R"(exchange_sender_10 | type:Hash, {<0, String>}
 table_scan_1 | {<0, String>})",
            R"(exchange_sender_10 | type:Hash, {<0, String>}
 table_scan_1 | {<0, String>})",
            R"(exchange_sender_10 | type:Hash, {<0, String>}
 table_scan_1 | {<0, String>})",
            R"(exchange_sender_9 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_9 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_9 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_7 | type:Hash, {<0, String>, <1, String>}
 aggregation_6 | group_by: {<0, String>}, agg_func: {max(<0, String>)}
  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
   exchange_receiver_11 | type:PassThrough, {<0, String>, <1, String>}
   exchange_receiver_12 | type:PassThrough, {<0, String>})",
            R"(exchange_sender_7 | type:Hash, {<0, String>, <1, String>}
 aggregation_6 | group_by: {<0, String>}, agg_func: {max(<0, String>)}
  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
   exchange_receiver_11 | type:PassThrough, {<0, String>, <1, String>}
   exchange_receiver_12 | type:PassThrough, {<0, String>})",
            R"(exchange_sender_7 | type:Hash, {<0, String>, <1, String>}
 aggregation_6 | group_by: {<0, String>}, agg_func: {max(<0, String>)}
  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
   exchange_receiver_11 | type:PassThrough, {<0, String>, <1, String>}
   exchange_receiver_12 | type:PassThrough, {<0, String>})",
            R"(exchange_sender_5 | type:PassThrough, {<0, String>, <1, String>}
 project_4 | {<0, String>, <1, String>}
  aggregation_3 | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>})",
            R"(exchange_sender_5 | type:PassThrough, {<0, String>, <1, String>}
 project_4 | {<0, String>, <1, String>}
  aggregation_3 | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>})",
            R"(exchange_sender_5 | type:PassThrough, {<0, String>, <1, String>}
 project_4 | {<0, String>, <1, String>}
  aggregation_3 | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>})"};

        auto expected_cols = {
            toNullableVec<String>({{}, "banana"}),
            toNullableVec<String>({{}, "banana"})};

        ASSERT_MPPTASK_EQUAL_PLAN_AND_RESULT(
            context
                .scan("test_db", "l_table")
                .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                .project({col("max(l_table.s)"), col("l_table.s")}),
            expected_strings,
            expect_cols);
    }

    {
        auto properties = getDAGPropertiesForTest(1);
        auto tasks = context
                         .scan("test_db", "l_table")
                         .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                         .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                         .project({col("max(l_table.s)"), col("l_table.s")})
                         .buildMPPTasks(context, properties);

        std::vector<String> expected_strings = {
            R"(exchange_sender_10 | type:Hash, {<0, String>}
 table_scan_1 | {<0, String>})",
            R"(exchange_sender_9 | type:Hash, {<0, String>, <1, String>}
 table_scan_0 | {<0, String>, <1, String>})",
            R"(exchange_sender_7 | type:Hash, {<0, String>, <1, String>}
 aggregation_6 | group_by: {<0, String>}, agg_func: {max(<0, String>)}
  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
   exchange_receiver_11 | type:PassThrough, {<0, String>, <1, String>}
   exchange_receiver_12 | type:PassThrough, {<0, String>})",
            R"(exchange_sender_5 | type:PassThrough, {<0, String>, <1, String>}
 project_4 | {<0, String>, <1, String>}
  aggregation_3 | group_by: {<1, String>}, agg_func: {max(<0, String>)}
   exchange_receiver_8 | type:PassThrough, {<0, String>, <1, String>})",
        };

        size_t task_size = tasks.size();
        for (size_t i = 0; i < task_size; ++i)
        {
            ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
        }
    }
}
CATCH

TEST_F(ComputeServerRunner, aggWithColumnPrune)
try
{
    startServers(3);

    context.addMockTable(
        {"test_db", "test_table_2"},
        {{"i1", TiDB::TP::TypeLong}, {"i2", TiDB::TP::TypeLong}, {"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}, {"s4", TiDB::TP::TypeString}, {"s5", TiDB::TP::TypeString}},
        {toNullableVec<Int32>("i1", {0, 0, 0}), toNullableVec<Int32>("i2", {1, 1, 1}), toNullableVec<String>("s1", {"1", "9", "8"}), toNullableVec<String>("s2", {"1", "9", "8"}), toNullableVec<String>("s3", {"4", "9", "99"}), toNullableVec<String>("s4", {"4", "9", "999"}), toNullableVec<String>("s5", {"4", "9", "9999"})});
    std::vector<String> res{"9", "9", "99", "999", "9999"};
    std::vector<String> max_cols{"s1", "s2", "s3", "s4", "s5"};
    for (size_t i = 0; i < 1; ++i)
    {
        {
            auto request = context
                               .scan("test_db", "test_table_2")
                               .aggregation({Max(col(max_cols[i]))}, {col("i1")});
            auto expected_cols = {
                toNullableVec<String>({res[i]}),
                toNullableVec<Int32>({{0}})};
            ASSERT_COLUMNS_EQ_UR(expected_cols, buildAndExecuteMPPTasks(request));
        }

        {
            auto request = context
                               .scan("test_db", "test_table_2")
                               .aggregation({Max(col(max_cols[i]))}, {col("i2")});
            auto expected_cols = {
                toNullableVec<String>({res[i]}),
                toNullableVec<Int32>({{1}})};
            ASSERT_COLUMNS_EQ_UR(expected_cols, buildAndExecuteMPPTasks(request));
        }

        {
            auto request = context
                               .scan("test_db", "test_table_2")
                               .aggregation({Max(col(max_cols[i]))}, {col("i1"), col("i2")});
            auto expected_cols = {
                toNullableVec<String>({res[i]}),
                toNullableVec<Int32>({{0}}),
                toNullableVec<Int32>({{1}})};
            ASSERT_COLUMNS_EQ_UR(expected_cols, buildAndExecuteMPPTasks(request));
        }
    }
}
CATCH

TEST_F(ComputeServerRunner, cancelAggTasks)
try
{
    startServers(4);
    {
        auto [query_id, res] = prepareMPPStreams(context
                                                     .scan("test_db", "test_table_1")
                                                     .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                                                     .project({"max(s1)"}));
        EXPECT_TRUE(assertQueryActive(query_id));
        MockComputeServerManager::instance().cancelQuery(query_id);
        EXPECT_TRUE(assertQueryCancelled(query_id));
    }
}
CATCH

TEST_F(ComputeServerRunner, cancelJoinTasks)
try
{
    startServers(4);
    {
        auto [query_id, res] = prepareMPPStreams(context
                                                     .scan("test_db", "l_table")
                                                     .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}));
        EXPECT_TRUE(assertQueryActive(query_id));
        MockComputeServerManager::instance().cancelQuery(query_id);
        EXPECT_TRUE(assertQueryCancelled(query_id));
    }
}
CATCH

TEST_F(ComputeServerRunner, cancelJoinThenAggTasks)
try
{
    startServers(4);
    {
        auto [query_id, _] = prepareMPPStreams(context
                                                   .scan("test_db", "l_table")
                                                   .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                                                   .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                                                   .project({col("max(l_table.s)"), col("l_table.s")}));
        EXPECT_TRUE(assertQueryActive(query_id));
        MockComputeServerManager::instance().cancelQuery(query_id);
        EXPECT_TRUE(assertQueryCancelled(query_id));
    }
}
CATCH

TEST_F(ComputeServerRunner, multipleQuery)
try
{
    startServers(4);
    {
        auto [query_id1, res1] = prepareMPPStreams(context
                                                       .scan("test_db", "l_table")
                                                       .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}));
        auto [query_id2, res2] = prepareMPPStreams(context
                                                       .scan("test_db", "l_table")
                                                       .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                                                       .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                                                       .project({col("max(l_table.s)"), col("l_table.s")}));

        EXPECT_TRUE(assertQueryActive(query_id1));
        MockComputeServerManager::instance().cancelQuery(query_id1);
        EXPECT_TRUE(assertQueryCancelled(query_id1));

        EXPECT_TRUE(assertQueryActive(query_id2));
        MockComputeServerManager::instance().cancelQuery(query_id2);
        EXPECT_TRUE(assertQueryCancelled(query_id2));
    }

    // start 10 queries
    {
        std::vector<std::tuple<MPPQueryId, std::vector<BlockInputStreamPtr>>> queries;
        for (size_t i = 0; i < 10; ++i)
        {
            queries.push_back(prepareMPPStreams(context
                                                    .scan("test_db", "l_table")
                                                    .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})));
        }
        for (size_t i = 0; i < 10; ++i)
        {
            auto query_id = std::get<0>(queries[i]);
            EXPECT_TRUE(assertQueryActive(query_id));
            MockComputeServerManager::instance().cancelQuery(query_id);
            EXPECT_TRUE(assertQueryCancelled(query_id));
        }
    }
}
CATCH

TEST_F(ComputeServerRunner, runCoprocessor)
try
{
    // In coprocessor test, we only need to start 1 server.
    startServers(1);
    {
        auto request = context
                           .scan("test_db", "l_table")
                           .build(context);

        auto expected_cols = {
            toNullableVec<String>({{"banana", {}, "banana"}}),
            toNullableVec<String>({{"apple", {}, "banana"}})};
        ASSERT_COLUMNS_EQ_UR(expected_cols, executeCoprocessorTask(request));
    }
}
CATCH

TEST_F(ComputeServerRunner, runFineGrainedShuffleJoinTest)
try
{
    startServers(3);
    constexpr size_t join_type_num = 7;
    constexpr tipb::JoinType join_types[join_type_num] = {
        tipb::JoinType::TypeInnerJoin,
        tipb::JoinType::TypeLeftOuterJoin,
        tipb::JoinType::TypeRightOuterJoin,
        tipb::JoinType::TypeSemiJoin,
        tipb::JoinType::TypeAntiSemiJoin,
        tipb::JoinType::TypeLeftOuterSemiJoin,
        tipb::JoinType::TypeAntiLeftOuterSemiJoin,
    };
    // fine-grained shuffle is enabled.
    constexpr uint64_t enable = 8;
    constexpr uint64_t disable = 0;

    for (auto join_type : join_types)
    {
        std::cout << "JoinType: " << static_cast<int>(join_type) << std::endl;
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
        auto request = context
                           .scan("test_db", "l_table_2")
                           .join(context.scan("test_db", "r_table_2"), join_type, {col("s1"), col("s2")}, disable)
                           .project({col("l_table_2.s1"), col("l_table_2.s2"), col("l_table_2.s3")});
        const auto expected_cols = buildAndExecuteMPPTasks(request);

        auto request2 = context
                            .scan("test_db", "l_table_2")
                            .join(context.scan("test_db", "r_table_2"), join_type, {col("s1"), col("s2")}, enable)
                            .project({col("l_table_2.s1"), col("l_table_2.s2"), col("l_table_2.s3")});
        auto tasks = request2.buildMPPTasks(context, properties);
        const auto actual_cols = executeMPPTasks(tasks, properties, MockComputeServerManager::instance().getServerConfigMap());
        ASSERT_COLUMNS_EQ_UR(expected_cols, actual_cols);
    }
}
CATCH

TEST_F(ComputeServerRunner, runFineGrainedShuffleAggTest)
try
{
    startServers(3);
    // fine-grained shuffle is enabled.
    constexpr uint64_t enable = 8;
    constexpr uint64_t disable = 0;
    {
        auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
        auto request = context
                           .scan("test_db", "test_table_2")
                           .aggregation({Max(col("s3"))}, {col("s1"), col("s2")}, disable);
        const auto expected_cols = buildAndExecuteMPPTasks(request);

        auto request2 = context
                            .scan("test_db", "test_table_2")
                            .aggregation({Max(col("s3"))}, {col("s1"), col("s2")}, enable);
        auto tasks = request2.buildMPPTasks(context, properties);
        const auto actual_cols = executeMPPTasks(tasks, properties, MockComputeServerManager::instance().getServerConfigMap());
        ASSERT_COLUMNS_EQ_UR(expected_cols, actual_cols);
    }
}
CATCH

} // namespace tests
} // namespace DB
