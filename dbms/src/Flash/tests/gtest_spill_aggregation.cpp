// Copyright 2023 PingCAP, Ltd.
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

#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{

class SpillAggregationTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
    }
};

/// todo add more tests
TEST_F(SpillAggregationTestRunner, SimpleCase)
try
{
    DB::MockColumnInfoVec column_infos{{"a", TiDB::TP::TypeLongLong}, {"b", TiDB::TP::TypeLongLong}, {"c", TiDB::TP::TypeLongLong}, {"d", TiDB::TP::TypeLongLong}, {"e", TiDB::TP::TypeLongLong}};
    ColumnsWithTypeAndName column_datas;
    size_t table_rows = 102400;
    size_t duplicated_rows = 51200;
    UInt64 max_block_size = 500;
    size_t original_max_streams = 20;
    size_t total_data_size = 0;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{table_rows, getDataTypeByColumnInfoForComputingLayer(column_info)->getName(), RANDOM, column_info.name};
        column_datas.push_back(ColumnGenerator::instance().generate(opts));
        total_data_size += column_datas.back().column->byteSize();
    }
    for (auto & column_data : column_datas)
        column_data.column->assumeMutable()->insertRangeFrom(*column_data.column, 0, duplicated_rows);
    context.addMockTable("spill_sort_test", "simple_table", column_infos, column_datas, 8);

    auto request = context
                       .scan("spill_sort_test", "simple_table")
                       .aggregation({Min(col("c")), Max(col("d")), Count(col("e"))}, {col("a"), col("b")})
                       .build(context);
    context.context.setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// disable spill
    context.context.setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(0)));
    auto ref_columns = executeStreams(request, original_max_streams);
    /// enable spill
    context.context.setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(total_data_size / 200)));
    context.context.setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(1)));
    context.context.setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(1)));
    /// don't use `executeAndAssertColumnsEqual` since it takes too long to run
    /// test single thread aggregation
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, 1));
    /// test parallel aggregation
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
    /// enable spill and use small max_spilled_size_per_spill
    context.context.setSetting("max_spilled_size_per_spill", Field(static_cast<UInt64>(total_data_size / 200)));
    /// test single thread aggregation
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, 1));
    /// test parallel aggregation
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
    /// test spill with small max_block_size
    /// the avg rows in one bucket is ~10240/256 = 400, so set the small_max_block_size to 300
    /// is enough to test the output spilt
    size_t small_max_block_size = 300;
    context.context.setSetting("max_block_size", Field(static_cast<UInt64>(small_max_block_size)));
    auto blocks = getExecuteStreamsReturnBlocks(request, 1);
    for (auto & block : blocks)
    {
        ASSERT_EQ(block.rows() <= small_max_block_size, true);
    }
    ASSERT_COLUMNS_EQ_UR(ref_columns, mergeBlocks(std::move(blocks)).getColumnsWithTypeAndName());
    blocks = getExecuteStreamsReturnBlocks(request, original_max_streams);
    for (auto & block : blocks)
    {
        ASSERT_EQ(block.rows() <= small_max_block_size, true);
    }
    ASSERT_COLUMNS_EQ_UR(ref_columns, mergeBlocks(std::move(blocks)).getColumnsWithTypeAndName());
}
CATCH
} // namespace tests
} // namespace DB
