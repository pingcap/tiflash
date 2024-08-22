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
} // namespace FailPoints
namespace tests
{
class AutoSpillSortTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        dag_context_ptr->log = Logger::get("AutoSpillSortTest");
    }
};

/// todo add more tests
TEST_F(AutoSpillSortTestRunner, TriggerByRandomMarkForSpill)
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
        total_data_size += column_data.back().column->byteSize();
    }
    context.addMockTable("spill_sort_test", "simple_table", column_infos, column_data, 8);

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
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(0)));
    auto ref_columns = executeStreams(request, 1);
    /// enable auto spill
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(total_data_size * 1000)));
    context.context->setSetting("auto_memory_revoke_trigger_threshold", Field(0.7));
    DB::FailPointHelper::enableRandomFailPoint(DB::FailPoints::random_marked_for_auto_spill, 0.5);
    // don't use `executeAndAssertColumnsEqual` since it takes too long to run
    /// todo use ASSERT_COLUMNS_EQ_R once TiFlash support final TopN
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreamsWithMemoryTracker(request, original_max_streams));

    // The implementation of topN in the pipeline model is LocalSort, and the result of using multiple threads is unstable. Therefore, a single thread is used here instead.
    enablePipeline(true);
    ASSERT_COLUMNS_EQ_R(ref_columns, executeStreamsWithMemoryTracker(request, 1));
    DB::FailPointHelper::disableFailPoint(DB::FailPoints::random_marked_for_auto_spill);
}
CATCH

} // namespace tests
} // namespace DB
