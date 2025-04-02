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

#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <common/types.h>

namespace DB
{
namespace tests
{
class SpillSortTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        dag_context_ptr->log = Logger::get("SortSpillTest");
    }
};

/// todo add more tests
TEST_F(SpillSortTestRunner, SimpleCase)
try
{
    DB::MockColumnInfoVec column_infos{
        {"a", TiDB::TP::TypeLongLong},
        {"b", TiDB::TP::TypeLongLong},
        {"c", TiDB::TP::TypeLongLong},
        {"d", TiDB::TP::TypeLongLong},
        {"e", TiDB::TP::TypeLongLong}};
    ColumnsWithTypeAndName column_data;
    size_t table_rows = 102400;
    UInt64 max_block_size = 500;
    size_t original_max_streams = 20;
    size_t total_data_size = 0;
    size_t limit_size = table_rows / 10 * 9;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            table_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        column_data.push_back(ColumnGenerator::instance().generate(opts));
    }
    for (const auto & column : column_data)
        total_data_size += column.column->estimateByteSizeForSpill();

    context.addMockTable("spill_sort_test", "simple_table", column_infos, column_data, 8);

    // <max_spilled_bytes, expect_error>
    std::vector<std::pair<int64_t, bool>> max_spilled_bytes
        = {{-1, false}, {1, true}, {100000, true}, {total_data_size, false}};

    MockOrderByItemVec order_by_items{
        std::make_pair("a", true),
        std::make_pair("b", true),
        std::make_pair("c", true),
        std::make_pair("d", true),
        std::make_pair("e", true)};

    auto request = context.scan("spill_sort_test", "simple_table").topN(order_by_items, limit_size).build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));

    enablePipeline(false);
    /// disable spill
    context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(0)));
    auto ref_columns = executeStreams(request, 1);

    /// enable spill
    context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(total_data_size / 10)));
    // don't use `executeAndAssertColumnsEqual` since it takes too long to run
    /// todo use ASSERT_COLUMNS_EQ_R once TiFlash support final TopN
    SPILL_LIMITER_TEST_BEGIN
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
    SPILL_LIMITER_TEST_END

    /// enable spill and use small max_cached_data_bytes_in_spiller
    context.context->setSetting("max_cached_data_bytes_in_spiller", Field(static_cast<UInt64>(total_data_size / 100)));
    SPILL_LIMITER_TEST_BEGIN
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
    SPILL_LIMITER_TEST_END

    // The implementation of topN in the pipeline model is LocalSort, and the result of using multiple threads is unstable. Therefore, a single thread is used here instead.
    enablePipeline(true);
    context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(total_data_size / 10)));
    SPILL_LIMITER_TEST_BEGIN
    ASSERT_COLUMNS_EQ_R(ref_columns, executeStreams(request, 1));
    SPILL_LIMITER_TEST_END

    context.context->setSetting("max_cached_data_bytes_in_spiller", Field(static_cast<UInt64>(total_data_size / 100)));
    SPILL_LIMITER_TEST_BEGIN
    ASSERT_COLUMNS_EQ_R(ref_columns, executeStreams(request, 1));
    SPILL_LIMITER_TEST_END
}
CATCH

TEST_F(SpillSortTestRunner, CollatorTest)
try
{
    DB::MockColumnInfoVec column_infos{
        {"a", TiDB::TP::TypeString, false},
        {"b", TiDB::TP::TypeString, false},
        {"c", TiDB::TP::TypeString, false},
        {"d", TiDB::TP::TypeString, false},
        {"e", TiDB::TP::TypeString, false}};
    ColumnsWithTypeAndName column_data;
    size_t table_rows = 102400;
    UInt64 max_block_size = 500;
    size_t original_max_streams = 20;
    size_t total_data_size = 0;
    size_t limit_size = table_rows / 10 * 8;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            table_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name,
            5};
        column_data.push_back(ColumnGenerator::instance().generate(opts));
        total_data_size += column_data.back().column->byteSize();
    }
    context.addMockTable("spill_sort_test", "collation_table", column_infos, column_data, 8);

    MockOrderByItemVec order_by_items{
        std::make_pair("a", true),
        std::make_pair("b", true),
        std::make_pair("c", true),
        std::make_pair("d", true),
        std::make_pair("e", true)};
    std::vector<Int64> collators{
        TiDB::ITiDBCollator::UTF8MB4_BIN,
        TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI,
        TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI,
        TiDB::ITiDBCollator::UTF8MB4_0900_AI_CI,
        TiDB::ITiDBCollator::UTF8MB4_0900_BIN};
    for (const auto & collator_id : collators)
    {
        context.setCollation(collator_id);
        auto request
            = context.scan("spill_sort_test", "collation_table").topN(order_by_items, limit_size).build(context);
        context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));

        enablePipeline(false);
        /// disable spill
        context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(0)));
        auto ref_columns = executeStreams(request, original_max_streams);
        /// enable spill
        context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(total_data_size / 10)));
        // don't use `executeAndAssertColumnsEqual` since it takes too long to run
        /// todo use ASSERT_COLUMNS_EQ_R once TiFlash support final TopN
        ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
        /// enable spill and use small max_cached_data_bytes_in_spiller
        context.context->setSetting(
            "max_cached_data_bytes_in_spiller",
            Field(static_cast<UInt64>(total_data_size / 100)));
        ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));

        // The implementation of topN in the pipeline model is LocalSort, and the result of using multiple threads is unstable. Therefore, a single thread is used here instead.
        // Because `UTF8MB4_GENERAL_CI` ignores case sensitivity, leading to unstable results, `ASSERT_COLUMNS_EQ_UR` is still used here.
        enablePipeline(true);
        context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(total_data_size / 10)));
        ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, 1));
        context.context->setSetting(
            "max_cached_data_bytes_in_spiller",
            Field(static_cast<UInt64>(total_data_size / 100)));
        ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, 1));
    }
}
CATCH

TEST_F(SpillSortTestRunner, SpillAfterFilter)
try
{
    DB::MockColumnInfoVec column_infos{
        {"a", TiDB::TP::TypeTiny},
        {"b", TiDB::TP::TypeTiny},
        {"c", TiDB::TP::TypeTiny},
        {"d", TiDB::TP::TypeTiny},
        {"e", TiDB::TP::TypeTiny}};
    ColumnsWithTypeAndName column_data;
    size_t table_rows = 102400;
    UInt64 max_block_size = 64;
    size_t total_data_size = 0;
    size_t limit_size = table_rows / 10 * 9;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            table_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        column_data.push_back(ColumnGenerator::instance().generate(opts));
        total_data_size += column_data.back().column->byteSize();
    }
    context.addMockTable("spill_sort_test", "simple_table", column_infos, column_data, 8);

    MockOrderByItemVec order_by_items{
        std::make_pair("a", true),
        std::make_pair("b", true),
        std::make_pair("c", true),
        std::make_pair("d", true),
        std::make_pair("e", true)};

    auto request = context.scan("spill_sort_test", "simple_table")
                       .filter(gt(col("d"), lit(toField(static_cast<Int8>(-128)))))
                       .topN(order_by_items, limit_size)
                       .project({gt(col("d"), lit(toField(static_cast<Int8>(-128))))})
                       .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));

    /// disable spill
    context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(0)));
    auto ref_columns = executeStreams(request, 1);

    // The implementation of topN in the pipeline model is LocalSort, and the result of using multiple threads is unstable. Therefore, a single thread is used here instead.
    enablePipeline(true);
    context.context->setSetting("max_bytes_before_external_sort", Field(static_cast<UInt64>(total_data_size / 10)));
    ASSERT_COLUMNS_EQ_R(ref_columns, executeStreams(request, 1));
    context.context->setSetting("max_cached_data_bytes_in_spiller", Field(static_cast<UInt64>(total_data_size / 100)));
    ASSERT_COLUMNS_EQ_R(ref_columns, executeStreams(request, 1));
}
CATCH

} // namespace tests
} // namespace DB
