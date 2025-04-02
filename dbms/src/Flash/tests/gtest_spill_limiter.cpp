// Copyright 2025 PingCAP, Inc.
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
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class SpillLimiterTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        dag_context_ptr->log = Logger::get("SpillLimiterTest");

        total_rows = 2000;
        DB::MockColumnInfoVec table_column_infos{
            {"key_64", TiDB::TP::TypeLongLong, false},
            {"key_string", TiDB::TP::TypeString, false},
            {"key_nullable_string", TiDB::TP::TypeString, true},
            {"key_decimal256", TiDB::TP::TypeString, false},
            {"value", TiDB::TP::TypeLong, false}};
        ColumnsWithTypeAndName table_column_data;
        for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(table_column_infos))
        {
            ColumnGeneratorOpts opts{
                total_rows,
                getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
                RANDOM,
                column_info.name};
            table_column_data.push_back(ColumnGenerator::instance().generate(opts));
        }
        context.addMockTable("test_db", "t1", table_column_infos, table_column_data);
    }

    size_t total_rows = 0;
};

TEST_F(SpillLimiterTestRunner, simpleAgg)
try
{
    std::vector<std::vector<String>> group_by_keys{
        {"key_64", "key_nullable_string", "key_decimal256"},
    };
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(10)));

    // <max_spilled_bytes, expect_error>
    std::vector<std::pair<uint64_t, bool>> max_spilled_bytes = {{1, true}, {800, true}, {0, false}};

    for (const auto & keys : group_by_keys)
    {
        MockAstVec key_vec;
        for (const auto & key : keys)
            key_vec.push_back(col(key));
        auto request = context.scan("test_db", "t1").aggregation({Max(col("value"))}, key_vec).build(context);

        enablePipeline(false);
        context.context->setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(0)));
        context.context->setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(0)));
        context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(0)));
        auto reference = executeStreams(request);

        context.context->setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(1)));
        context.context->setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(1)));
        context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(1)));
        SPILL_LIMITER_TEST_BEGIN
        enablePipeline(true);
        ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 1));
        ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 10));
        enablePipeline(false);
        ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 1));
        ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 10));
        SPILL_LIMITER_TEST_END
    }
}
CATCH

TEST_F(SpillLimiterTestRunner, simpleSort)
try
{
    MockOrderByItemVec order_by_items{
        std::make_pair("key_64", true),
        std::make_pair("key_string", true),
        std::make_pair("key_decimal256", true)};

    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(10)));

    // <max_spilled_bytes, expect_error>
    std::vector<std::pair<uint64_t, bool>> max_spilled_bytes = {{1, true}, {800, true}, {0, false}};

    auto request = context.scan("test_db", "t1").topN(order_by_items, total_rows).build(context);

    enablePipeline(false);
    context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(0)));
    auto reference = executeStreams(request);

    context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(1)));
    SPILL_LIMITER_TEST_BEGIN
    enablePipeline(true);
    ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 1));
    ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 10));
    enablePipeline(false);
    ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 1));
    ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 10));
    SPILL_LIMITER_TEST_END
}
CATCH

TEST_F(SpillLimiterTestRunner, simpleJoin)
try
{
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(10)));
    // <max_spilled_bytes, expect_error>
    std::vector<std::pair<uint64_t, bool>> max_spilled_bytes = {{1, true}, {800, true}, {0, false}};

    auto request = context.scan("test_db", "t1")
                       .join(context.scan("test_db", "t1"), tipb::JoinType::TypeLeftOuterJoin, {col("key_64")})
                       .project({fmt::format("t1.key_decimal256")})
                       .build(context);

    enablePipeline(false);
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    auto reference = executeStreams(request);

    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(1)));
    SPILL_LIMITER_TEST_BEGIN
    enablePipeline(true);
    ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 10));
    enablePipeline(false);
    ASSERT_COLUMNS_EQ_UR(reference, executeStreams(request, 10));
    SPILL_LIMITER_TEST_END
}
CATCH
} // namespace tests
} // namespace DB
