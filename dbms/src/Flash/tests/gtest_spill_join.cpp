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

#include <Flash/tests/gtest_join.h>

#include <magic_enum.hpp>

namespace DB
{
namespace tests
{
class SpillJoinTestRunner : public DB::tests::JoinTestRunner
{
public:
    void initializeContext() override
    {
        JoinTestRunner::initializeContext();
        dag_context_ptr->log = Logger::get("JoinSpillTest");
    }
};

#define WRAP_FOR_SPILL_TEST_BEGIN                  \
    std::vector<bool> pipeline_bools{false, true}; \
    for (auto enable_pipeline : pipeline_bools)    \
    {                                              \
        enablePipeline(enable_pipeline);

#define WRAP_FOR_SPILL_TEST_END }

TEST_F(SpillJoinTestRunner, SimpleJoinSpill)
try
{
    constexpr size_t simple_test_num = 4;

    context.addMockTable(
        "simple_test",
        "t1",
        {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}},
        {toNullableVec<String>("a", {"1", "2", {}, "1", {}}), toNullableVec<String>("b", {"3", "4", "3", {}, {}})});
    context.addMockTable(
        "simple_test",
        "t2",
        {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}},
        {toNullableVec<String>("a", {"1", "3", {}, "1", {}}), toNullableVec<String>("b", {"3", "4", "3", {}, {}})});

    // names of left table, right table and join key column
    const std::tuple<String, String, String> join_cases[simple_test_num] = {
        std::make_tuple("t1", "t2", "a"),
        std::make_tuple("t2", "t1", "a"),
        std::make_tuple("t1", "t2", "b"),
        std::make_tuple("t2", "t1", "b"),
    };

    const ColumnsWithTypeAndName expected_cols[simple_test_num * join_type_num] = {
        // inner join
        {toNullableVec<String>({"1", "1", "1", "1"}),
         toNullableVec<String>({{}, "3", {}, "3"}),
         toNullableVec<String>({"1", "1", "1", "1"}),
         toNullableVec<String>({"3", "3", {}, {}})},
        {toNullableVec<String>({"1", "1", "1", "1"}),
         toNullableVec<String>({{}, "3", {}, "3"}),
         toNullableVec<String>({"1", "1", "1", "1"}),
         toNullableVec<String>({"3", "3", {}, {}})},
        {toNullableVec<String>({{}, "1", "2", {}, "1"}),
         toNullableVec<String>({"3", "3", "4", "3", "3"}),
         toNullableVec<String>({"1", "1", "3", {}, {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3"})},
        {toNullableVec<String>({{}, "1", "3", {}, "1"}),
         toNullableVec<String>({"3", "3", "4", "3", "3"}),
         toNullableVec<String>({"1", "1", "2", {}, {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3"})},
        // left join
        {toNullableVec<String>({"1", "1", "2", {}, "1", "1", {}}),
         toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}}),
         toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}),
         toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}})},
        {toNullableVec<String>({"1", "1", "3", {}, "1", "1", {}}),
         toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}}),
         toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}),
         toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}})},
        {toNullableVec<String>({"1", "1", "2", {}, {}, "1", {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}),
         toNullableVec<String>({{}, "1", "3", {}, "1", {}, {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})},
        {toNullableVec<String>({"1", "1", "3", {}, {}, "1", {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}),
         toNullableVec<String>({{}, "1", "2", {}, "1", {}, {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})},
        // right join
        {toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}),
         toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}}),
         toNullableVec<String>({"1", "1", "3", {}, "1", "1", {}}),
         toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}})},
        {toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}),
         toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}}),
         toNullableVec<String>({"1", "1", "2", {}, "1", "1", {}}),
         toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}})},
        {toNullableVec<String>({{}, "1", "2", {}, "1", {}, {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}),
         toNullableVec<String>({"1", "1", "3", {}, {}, "1", {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})},
        {toNullableVec<String>({{}, "1", "3", {}, "1", {}, {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}),
         toNullableVec<String>({"1", "1", "2", {}, {}, "1", {}}),
         toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})},
        // semi join
        {toNullableVec<String>({"1", "1"}), toNullableVec<String>({"3", {}})},
        {toNullableVec<String>({"1", "1"}), toNullableVec<String>({"3", {}})},
        {toNullableVec<String>({"1", "2", {}}), toNullableVec<String>({"3", "4", "3"})},
        {toNullableVec<String>({"1", "3", {}}), toNullableVec<String>({"3", "4", "3"})},
        // anti semi join
        {toNullableVec<String>({"2", {}, {}}), toNullableVec<String>({"4", "3", {}})},
        {toNullableVec<String>({"3", {}, {}}), toNullableVec<String>({"4", "3", {}})},
        {toNullableVec<String>({"1", {}}), toNullableVec<String>({{}, {}})},
        {toNullableVec<String>({"1", {}}), toNullableVec<String>({{}, {}})},
        // left outer semi join
        {toNullableVec<String>({"1", "2", {}, "1", {}}),
         toNullableVec<String>({"3", "4", "3", {}, {}}),
         toNullableVec<Int8>({1, 0, 0, 1, 0})},
        {toNullableVec<String>({"1", "3", {}, "1", {}}),
         toNullableVec<String>({"3", "4", "3", {}, {}}),
         toNullableVec<Int8>({1, 0, 0, 1, 0})},
        {toNullableVec<String>({"1", "2", {}, "1", {}}),
         toNullableVec<String>({"3", "4", "3", {}, {}}),
         toNullableVec<Int8>({1, 1, 1, 0, 0})},
        {toNullableVec<String>({"1", "3", {}, "1", {}}),
         toNullableVec<String>({"3", "4", "3", {}, {}}),
         toNullableVec<Int8>({1, 1, 1, 0, 0})},
        // anti left outer semi join
        {toNullableVec<String>({"1", "2", {}, "1", {}}),
         toNullableVec<String>({"3", "4", "3", {}, {}}),
         toNullableVec<Int8>({0, 1, 1, 0, 1})},
        {toNullableVec<String>({"1", "3", {}, "1", {}}),
         toNullableVec<String>({"3", "4", "3", {}, {}}),
         toNullableVec<Int8>({0, 1, 1, 0, 1})},
        {toNullableVec<String>({"1", "2", {}, "1", {}}),
         toNullableVec<String>({"3", "4", "3", {}, {}}),
         toNullableVec<Int8>({0, 0, 0, 1, 1})},
        {toNullableVec<String>({"1", "3", {}, "1", {}}),
         toNullableVec<String>({"3", "4", "3", {}, {}}),
         toNullableVec<Int8>({0, 0, 0, 1, 1})},
    };

    WRAP_FOR_SPILL_TEST_BEGIN
    for (size_t i = 0; i < join_type_num; ++i)
    {
        for (size_t j = 0; j < simple_test_num; ++j)
        {
            const auto & [l, r, k] = join_cases[j];
            auto join_type = join_types[i];
            auto request = context.scan("simple_test", l)
                               .join(context.scan("simple_test", r), join_type, {col(k)})
                               .build(context);
            auto request_column_prune = context.scan("simple_test", l)
                                            .join(context.scan("simple_test", r), join_type, {col(k)})
                                            .aggregation({Count(lit(static_cast<UInt64>(1)))}, {})
                                            .build(context);

            {
                context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(10000)));
                ASSERT_THROW(executeStreams(request), Exception)
                    << "join_type = " << magic_enum::enum_name(join_type) << ", simple_test_index = " << j;
                auto concurrences = {2, 5, 10};
                for (auto concurrency : concurrences)
                {
                    ASSERT_COLUMNS_EQ_UR(expected_cols[i * simple_test_num + j], executeStreams(request, concurrency))
                        << "join_type = " << magic_enum::enum_name(join_type) << ", simple_test_index = " << j
                        << ", concurrency = " << concurrency;
                }
                ASSERT_COLUMNS_EQ_UR(
                    genScalarCountResults(expected_cols[i * simple_test_num + j]),
                    executeStreams(request_column_prune, 2));
            }
        }
    }
    WRAP_FOR_SPILL_TEST_END
}
CATCH

TEST_F(SpillJoinTestRunner, SpillToDisk)
try
{
    context.addMockTable(
        "split_test",
        "t1",
        {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}},
        {toVec<Int32>("a", {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), toVec<Int32>("b", {2, 2, 2, 2, 2, 2, 2, 2, 2, 2})});
    context.addMockTable("split_test", "t2", {{"a", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 1, 1, 2, 2, 2, 3, 3,
                                                                                              3, 4, 4, 4, 5, 5, 5, 6,
                                                                                              6, 6, 7, 7, 7, 8, 8, 8,
                                                                                              9, 9, 9, 0, 0, 0})});

    auto request = context.scan("split_test", "t1")
                       .join(context.scan("split_test", "t2"), tipb::JoinType::TypeInnerJoin, {col("a")})
                       .build(context);
    auto request_column_prune = context.scan("split_test", "t1")
                                    .join(context.scan("split_test", "t2"), tipb::JoinType::TypeInnerJoin, {col("a")})
                                    .aggregation({Count(lit(static_cast<UInt64>(1)))}, {})
                                    .build(context);

    auto join_restore_concurrences = {-1, 0, 1, 5};
    auto concurrences = {2, 5, 10};
    const ColumnsWithTypeAndName expect
        = {toNullableVec<Int32>(
               {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 0, 0, 0}),
           toNullableVec<Int32>(
               {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}),
           toNullableVec<Int32>(
               {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 0, 0, 0})};
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(10000)));

    WRAP_FOR_SPILL_TEST_BEGIN
    for (const auto & join_restore_concurrency : join_restore_concurrences)
    {
        context.context->setSetting("join_restore_concurrency", Field(static_cast<Int64>(join_restore_concurrency)));
        ASSERT_THROW(executeStreams(request), Exception);
        for (auto concurrency : concurrences)
        {
            ASSERT_COLUMNS_EQ_UR(expect, executeStreams(request, concurrency));
        }
        ASSERT_COLUMNS_EQ_UR(genScalarCountResults(expect), executeStreams(request_column_prune, 2));
    }
    WRAP_FOR_SPILL_TEST_END
}
CATCH

TEST_F(SpillJoinTestRunner, ScanHashMapAfterProbeDataWithSpillEnabledAndSpillTriggered)
try
{
    UInt64 max_block_size = 800;
    size_t original_max_streams = 20;
    /// used to test the case that max_stream less than fine_grained_stream_count
    size_t original_max_streams_small = 4;
    std::vector<String> left_table_names
        = {"left_table_1_concurrency",
           "left_table_3_concurrency",
           "left_table_5_concurrency",
           "left_table_10_concurrency"};
    std::vector<String> right_table_names
        = {"right_table_1_concurrency",
           "right_table_3_concurrency",
           "right_table_5_concurrency",
           "right_table_10_concurrency"};
    std::vector<size_t> right_exchange_receiver_concurrency = {1, 3, 5, 10};
    UInt64 max_bytes_before_external_join = 20000;

    WRAP_FOR_SPILL_TEST_BEGIN
    /// case 1, right join without right condition
    auto request
        = context.scan("outer_join_test", right_table_names[0])
              .join(context.scan("outer_join_test", left_table_names[0]), tipb::JoinType::TypeLeftOuterJoin, {col("a")})
              .project(
                  {fmt::format("{}.a", left_table_names[0]),
                   fmt::format("{}.b", left_table_names[0]),
                   fmt::format("{}.a", right_table_names[0]),
                   fmt::format("{}.b", right_table_names[0])})
              .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    /// use right_table left join left_table as the reference
    auto ref_columns = executeStreams(request, original_max_streams);

    /// case 1.1 table scan join table scan
    context.context->setSetting(
        "max_bytes_before_external_join",
        Field(static_cast<UInt64>(max_bytes_before_external_join)));
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context.scan("outer_join_test", left_table_name)
                          .join(
                              context.scan("outer_join_test", right_table_name),
                              tipb::JoinType::TypeRightOuterJoin,
                              {col("a")})
                          .project({fmt::format("{}.a", left_table_name), fmt::format("{}.b", right_table_name)})
                          .build(context);
            if (right_table_name == "right_table_1_concurrency")
            {
                ASSERT_THROW(executeStreams(request, original_max_streams), Exception)
                    << "left_table_name = " << left_table_name << ", right_table_name = " << right_table_name;
            }
            else
            {
                ColumnsWithTypeAndName ref;
                ref.push_back(ref_columns[0]);
                ref.push_back(ref_columns[3]);
                ASSERT_COLUMNS_EQ_UR(ref, executeStreams(request, original_max_streams))
                    << "left_table_name = " << left_table_name << ", right_table_name = " << right_table_name;
            }
        }
    }
    /// case 1.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            auto right_name = fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency);
            request = context.scan("outer_join_test", left_table_name)
                          .join(
                              context.receive(right_name, exchange_concurrency),
                              tipb::JoinType::TypeRightOuterJoin,
                              {col("a")},
                              {},
                              {},
                              {},
                              {},
                              exchange_concurrency)
                          .project({fmt::format("{}.b", left_table_name), fmt::format("{}.a", right_name)})
                          .build(context);
            if (exchange_concurrency == 1)
            {
                ASSERT_THROW(executeStreams(request, original_max_streams), Exception);
            }
            else
            {
                ColumnsWithTypeAndName ref;
                ref.push_back(ref_columns[1]);
                ref.push_back(ref_columns[2]);
                ASSERT_COLUMNS_EQ_UR(ref, executeStreams(request, original_max_streams));
                if (original_max_streams_small < exchange_concurrency)
                    ASSERT_COLUMNS_EQ_UR(ref, executeStreams(request, original_max_streams_small));
            }
        }
    }
    /// case 2, right join with right condition
    request = context.scan("outer_join_test", right_table_names[0])
                  .join(
                      context.scan("outer_join_test", left_table_names[0]),
                      tipb::JoinType::TypeLeftOuterJoin,
                      {col("a")},
                      {gt(col(right_table_names[0] + ".b"), lit(Field(static_cast<Int64>(1000))))},
                      {},
                      {},
                      {},
                      0)
                  .project(
                      {fmt::format("{}.a", left_table_names[0]),
                       fmt::format("{}.b", left_table_names[0]),
                       fmt::format("{}.a", right_table_names[0]),
                       fmt::format("{}.b", right_table_names[0])})
                  .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// use right_table left join left_table as the reference
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    ref_columns = executeStreams(request, original_max_streams);

    context.context->setSetting(
        "max_bytes_before_external_join",
        Field(static_cast<UInt64>(max_bytes_before_external_join)));
    /// case 2.1 table scan join table scan
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context.scan("outer_join_test", left_table_name)
                          .join(
                              context.scan("outer_join_test", right_table_name),
                              tipb::JoinType::TypeRightOuterJoin,
                              {col("a")},
                              {},
                              {gt(col(right_table_name + ".b"), lit(Field(static_cast<Int64>(1000))))},
                              {},
                              {},
                              0)
                          .project({fmt::format("{}.a", left_table_name), fmt::format("{}.b", right_table_name)})
                          .build(context);
            if (right_table_name == "right_table_1_concurrency")
            {
                ASSERT_THROW(executeStreams(request, original_max_streams), Exception)
                    << "left_table_name = " << left_table_name << ", right_table_name = " << right_table_name;
            }
            else
            {
                ColumnsWithTypeAndName ref;
                ref.push_back(ref_columns[0]);
                ref.push_back(ref_columns[3]);
                ASSERT_COLUMNS_EQ_UR(ref, executeStreams(request, original_max_streams))
                    << "left_table_name = " << left_table_name << ", right_table_name = " << right_table_name;
            }
        }
    }
    /// case 2.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            String exchange_name = fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency);
            request = context.scan("outer_join_test", left_table_name)
                          .join(
                              context.receive(exchange_name, exchange_concurrency),
                              tipb::JoinType::TypeRightOuterJoin,
                              {col("a")},
                              {},
                              {gt(col(exchange_name + ".b"), lit(Field(static_cast<Int64>(1000))))},
                              {},
                              {},
                              exchange_concurrency)
                          .project({fmt::format("{}.b", left_table_name), fmt::format("{}.a", exchange_name)})
                          .build(context);
            if (exchange_concurrency == 1)
            {
                ASSERT_THROW(executeStreams(request, original_max_streams), Exception);
            }
            else
            {
                ColumnsWithTypeAndName ref;
                ref.push_back(ref_columns[1]);
                ref.push_back(ref_columns[2]);
                ASSERT_COLUMNS_EQ_UR(ref, executeStreams(request, original_max_streams));
                if (original_max_streams_small < exchange_concurrency)
                    ASSERT_COLUMNS_EQ_UR(ref, executeStreams(request, original_max_streams_small));
            }
        }
    }
    WRAP_FOR_SPILL_TEST_END
}
CATCH

TEST_F(SpillJoinTestRunner, ScanHashMapAfterProbeDataWithSpillEnabledAndSpillNotTriggered)
try
{
    UInt64 max_block_size = 800;
    size_t original_max_streams = 20;
    /// used to test the case that max_stream less than fine_grained_stream_count
    size_t original_max_streams_small = 4;
    std::vector<String> left_table_names
        = {"left_table_1_concurrency",
           "left_table_3_concurrency",
           "left_table_5_concurrency",
           "left_table_10_concurrency"};
    std::vector<String> right_table_names
        = {"right_table_1_concurrency",
           "right_table_3_concurrency",
           "right_table_5_concurrency",
           "right_table_10_concurrency"};
    std::vector<size_t> right_exchange_receiver_concurrency = {1, 3, 5, 10};
    UInt64 max_bytes_before_external_join_will_no_spill_happens = 1024ULL * 1024 * 1024 * 1024;

    WRAP_FOR_SPILL_TEST_BEGIN
    /// case 1, right join without right condition
    auto request
        = context.scan("outer_join_test", right_table_names[0])
              .join(context.scan("outer_join_test", left_table_names[0]), tipb::JoinType::TypeLeftOuterJoin, {col("a")})
              .project(
                  {fmt::format("{}.a", left_table_names[0]),
                   fmt::format("{}.b", left_table_names[0]),
                   fmt::format("{}.a", right_table_names[0]),
                   fmt::format("{}.b", right_table_names[0])})
              .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    /// use right_table left join left_table as the reference
    auto ref_columns = executeStreams(request, original_max_streams);

    context.context->setSetting(
        "max_bytes_before_external_join",
        Field(static_cast<UInt64>(max_bytes_before_external_join_will_no_spill_happens)));
    /// case 1.1 table scan join table scan
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context.scan("outer_join_test", left_table_name)
                          .join(
                              context.scan("outer_join_test", right_table_name),
                              tipb::JoinType::TypeRightOuterJoin,
                              {col("a")})
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
        }
    }
    /// case 1.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            request = context.scan("outer_join_test", left_table_name)
                          .join(
                              context.receive(
                                  fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency),
                                  exchange_concurrency),
                              tipb::JoinType::TypeRightOuterJoin,
                              {col("a")},
                              {},
                              {},
                              {},
                              {},
                              exchange_concurrency)
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
            if (original_max_streams_small < exchange_concurrency)
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams_small));
        }
    }
    /// case 2, right join with right condition
    request = context.scan("outer_join_test", right_table_names[0])
                  .join(
                      context.scan("outer_join_test", left_table_names[0]),
                      tipb::JoinType::TypeLeftOuterJoin,
                      {col("a")},
                      {gt(col(right_table_names[0] + ".b"), lit(Field(static_cast<Int64>(1000))))},
                      {},
                      {},
                      {},
                      0)
                  .project(
                      {fmt::format("{}.a", left_table_names[0]),
                       fmt::format("{}.b", left_table_names[0]),
                       fmt::format("{}.a", right_table_names[0]),
                       fmt::format("{}.b", right_table_names[0])})
                  .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// use right_table left join left_table as the reference
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    ref_columns = executeStreams(request, original_max_streams);
    context.context->setSetting(
        "max_bytes_before_external_join",
        Field(static_cast<UInt64>(max_bytes_before_external_join_will_no_spill_happens)));
    /// case 2.1 table scan join table scan
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context.scan("outer_join_test", left_table_name)
                          .join(
                              context.scan("outer_join_test", right_table_name),
                              tipb::JoinType::TypeRightOuterJoin,
                              {col("a")},
                              {},
                              {gt(col(right_table_name + ".b"), lit(Field(static_cast<Int64>(1000))))},
                              {},
                              {},
                              0)
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
        }
    }
    /// case 2.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            String exchange_name = fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency);
            request = context.scan("outer_join_test", left_table_name)
                          .join(
                              context.receive(
                                  fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency),
                                  exchange_concurrency),
                              tipb::JoinType::TypeRightOuterJoin,
                              {col("a")},
                              {},
                              {gt(col(exchange_name + ".b"), lit(Field(static_cast<Int64>(1000))))},
                              {},
                              {},
                              exchange_concurrency)
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
            if (original_max_streams_small < exchange_concurrency)
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams_small));
        }
    }
    WRAP_FOR_SPILL_TEST_END
}
CATCH

#undef WRAP_FOR_SPILL_TEST_BEGIN
#undef WRAP_FOR_SPILL_TEST_END

} // namespace tests
} // namespace DB
