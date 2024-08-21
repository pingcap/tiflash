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

#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB
{
namespace FailPoints
{
extern const char random_marked_for_auto_spill[];
extern const char random_fail_in_resize_callback[];
} // namespace FailPoints

namespace tests
{
class AutoSpillAggregationTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        dag_context_ptr->log = Logger::get("AutoSpillAggTest");
    }
};

#define WRAP_FOR_SPILL_TEST_BEGIN                  \
    std::vector<bool> pipeline_bools{false, true}; \
    for (auto enable_pipeline : pipeline_bools)    \
    {                                              \
        enablePipeline(enable_pipeline);

#define WRAP_FOR_SPILL_TEST_END }

/// todo add more tests
TEST_F(AutoSpillAggregationTestRunner, TriggerByRandomMarkForSpill)
try
{
    DB::MockColumnInfoVec column_infos{
        {"a", TiDB::TP::TypeLongLong},
        {"b", TiDB::TP::TypeLongLong},
        {"c", TiDB::TP::TypeLongLong},
        {"d", TiDB::TP::TypeLongLong},
        {"e", TiDB::TP::TypeLongLong}};
    ColumnsWithTypeAndName column_datas;
    size_t table_rows = 102400;
    size_t duplicated_rows = 51200;
    UInt64 max_block_size = 500;
    size_t original_max_streams = 20;
    size_t total_data_size = 0;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            table_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        column_datas.push_back(ColumnGenerator::instance().generate(opts));
        total_data_size += column_datas.back().column->byteSize();
    }
    for (auto & column_data : column_datas)
        column_data.column->assumeMutable()->insertRangeFrom(*column_data.column, 0, duplicated_rows);
    context.addMockTable("spill_sort_test", "simple_table", column_infos, column_datas, 8);

    auto request = context.scan("spill_sort_test", "simple_table")
                       .aggregation({Min(col("c")), Max(col("d")), Count(col("e"))}, {col("a"), col("b")})
                       .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// disable spill
    context.context->setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(0)));
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(0)));
    enablePipeline(false);
    auto ref_columns = executeStreams(request, original_max_streams);
    /// enable spill
    DB::FailPointHelper::enableRandomFailPoint(DB::FailPoints::random_marked_for_auto_spill, 0.5);
    WRAP_FOR_SPILL_TEST_BEGIN
    context.context->setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(1)));
    context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(1)));
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(total_data_size * 1000)));
    context.context->setSetting("auto_memory_revoke_trigger_threshold", Field(0.7));
    /// don't use `executeAndAssertColumnsEqual` since it takes too long to run
    /// test parallel aggregation
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreamsWithMemoryTracker(request, original_max_streams));
    WRAP_FOR_SPILL_TEST_END
    DB::FailPointHelper::disableFailPoint(DB::FailPoints::random_marked_for_auto_spill);
}
CATCH

TEST_F(AutoSpillAggregationTestRunner, TriggerByRandomFailInResizeCallback)
try
{
    DB::MockColumnInfoVec column_infos{
        {"a", TiDB::TP::TypeLongLong},
        {"b", TiDB::TP::TypeLongLong},
        {"c", TiDB::TP::TypeLongLong},
        {"d", TiDB::TP::TypeLongLong},
        {"e", TiDB::TP::TypeLongLong}};
    ColumnsWithTypeAndName column_datas;
    size_t table_rows = 1024000;
    size_t duplicated_rows = 51200;
    UInt64 max_block_size = 500;
    size_t original_max_streams = 20;
    size_t total_data_size = 0;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            table_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        column_datas.push_back(ColumnGenerator::instance().generate(opts));
        total_data_size += column_datas.back().column->byteSize();
    }
    for (auto & column_data : column_datas)
        column_data.column->assumeMutable()->insertRangeFrom(*column_data.column, 0, duplicated_rows);
    context.addMockTable("spill_sort_test", "simple_table", column_infos, column_datas, 8);

    auto request = context.scan("spill_sort_test", "simple_table")
                       .aggregation({Min(col("c")), Max(col("d")), Count(col("e"))}, {col("a"), col("b")})
                       .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// disable spill
    context.context->setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(0)));
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(0)));
    enablePipeline(false);
    auto ref_columns = executeStreams(request, original_max_streams);
    /// enable spill
    DB::FailPointHelper::enableRandomFailPoint(DB::FailPoints::random_fail_in_resize_callback, 0.5);
    WRAP_FOR_SPILL_TEST_BEGIN
    context.context->setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(1)));
    context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(1)));
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(total_data_size * 1000)));
    context.context->setSetting("auto_memory_revoke_trigger_threshold", Field(0.7));
    /// don't use `executeAndAssertColumnsEqual` since it takes too long to run
    /// test parallel aggregation
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreamsWithMemoryTracker(request, original_max_streams));
    WRAP_FOR_SPILL_TEST_END
    DB::FailPointHelper::disableFailPoint(DB::FailPoints::random_fail_in_resize_callback);
}
CATCH

#undef WRAP_FOR_SPILL_TEST_BEGIN
#undef WRAP_FOR_SPILL_TEST_END
#undef WRAP_FOR_AGG_PARTIAL_BLOCK_START
#undef WRAP_FOR_AGG_PARTIAL_BLOCK_END

} // namespace tests
} // namespace DB
