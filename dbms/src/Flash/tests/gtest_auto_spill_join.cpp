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
#include <TiDB/Decode/TypeMapping.h>

#include <magic_enum.hpp>

namespace DB
{
namespace FailPoints
{
extern const char random_marked_for_auto_spill[];
extern const char random_fail_in_resize_callback[];
} // namespace FailPoints
namespace tests
{
class AutoSpillJoinTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        dag_context_ptr->log = Logger::get("AutoSpillJoinTest");
    }
};

#define WRAP_FOR_SPILL_TEST_BEGIN                  \
    std::vector<bool> pipeline_bools{false, true}; \
    for (auto enable_pipeline : pipeline_bools)    \
    {                                              \
        enablePipeline(enable_pipeline);

#define WRAP_FOR_SPILL_TEST_END }

TEST_F(AutoSpillJoinTestRunner, TriggerByRandomMarkForSpill)
try
{
    DB::MockColumnInfoVec column_infos{
        {"a", TiDB::TP::TypeLongLong},
        {"b", TiDB::TP::TypeLongLong},
        {"c", TiDB::TP::TypeLongLong}};
    ColumnsWithTypeAndName t1_column_datas;
    ColumnsWithTypeAndName t2_column_datas;
    size_t table_rows = 102400;
    size_t duplicated_rows = 51200;
    UInt64 max_block_size = 500;
    size_t original_max_streams = 20;
    size_t total_data_size = 0;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            table_rows - duplicated_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        t1_column_datas.push_back(ColumnGenerator::instance().generate(opts));
        t2_column_datas.push_back(ColumnGenerator::instance().generate(opts));
        total_data_size += t1_column_datas.back().column->byteSize();
        total_data_size += t2_column_datas.back().column->byteSize();
    }
    size_t column_index = 0;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            duplicated_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        auto column_data = ColumnGenerator::instance().generate(opts);
        t1_column_datas[column_index].column->assumeMutable()->insertRangeFrom(*column_data.column, 0, duplicated_rows);
        t2_column_datas[column_index].column->assumeMutable()->insertRangeFrom(*column_data.column, 0, duplicated_rows);
        total_data_size += column_data.column->byteSize() * 2;
        ++column_index;
    }
    context.addMockTable("auto_spill_test", "t1", column_infos, t1_column_datas, 8);
    context.addMockTable("auto_spill_test", "t2", column_infos, t1_column_datas, 8);
    auto request = context.scan("auto_spill_test", "t1")
                       .join(context.scan("auto_spill_test", "t2"), tipb::JoinType::TypeLeftOuterJoin, {col("a")})
                       .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// disable spill
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(0)));
    enablePipeline(false);
    auto ref_columns = executeStreams(request, original_max_streams);
    /// enable spill
    DB::FailPointHelper::enableRandomFailPoint(DB::FailPoints::random_marked_for_auto_spill, 0.5);
    WRAP_FOR_SPILL_TEST_BEGIN
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(total_data_size * 1000)));
    context.context->setSetting("auto_memory_revoke_trigger_threshold", Field(0.7));
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreamsWithMemoryTracker(request, original_max_streams));
    WRAP_FOR_SPILL_TEST_END
    DB::FailPointHelper::disableFailPoint(DB::FailPoints::random_marked_for_auto_spill);
}
CATCH

TEST_F(AutoSpillJoinTestRunner, TriggerByRandomFailInResizeCallback)
try
{
    DB::MockColumnInfoVec column_infos{
        {"a", TiDB::TP::TypeLongLong},
        {"b", TiDB::TP::TypeLongLong},
        {"c", TiDB::TP::TypeLongLong}};
    ColumnsWithTypeAndName t1_column_datas;
    ColumnsWithTypeAndName t2_column_datas;
    size_t table_rows = 102400;
    size_t duplicated_rows = 51200;
    UInt64 max_block_size = 500;
    size_t original_max_streams = 20;
    size_t total_data_size = 0;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            table_rows - duplicated_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        t1_column_datas.push_back(ColumnGenerator::instance().generate(opts));
        t2_column_datas.push_back(ColumnGenerator::instance().generate(opts));
        total_data_size += t1_column_datas.back().column->byteSize();
        total_data_size += t2_column_datas.back().column->byteSize();
    }
    size_t column_index = 0;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            duplicated_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        auto column_data = ColumnGenerator::instance().generate(opts);
        t1_column_datas[column_index].column->assumeMutable()->insertRangeFrom(*column_data.column, 0, duplicated_rows);
        t2_column_datas[column_index].column->assumeMutable()->insertRangeFrom(*column_data.column, 0, duplicated_rows);
        total_data_size += column_data.column->byteSize() * 2;
        ++column_index;
    }
    context.addMockTable("auto_spill_test", "t1", column_infos, t1_column_datas, 8);
    context.addMockTable("auto_spill_test", "t2", column_infos, t1_column_datas, 8);
    auto request = context.scan("auto_spill_test", "t1")
                       .join(context.scan("auto_spill_test", "t2"), tipb::JoinType::TypeLeftOuterJoin, {col("a")})
                       .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// disable spill
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(0)));
    enablePipeline(false);
    auto ref_columns = executeStreams(request, original_max_streams);
    /// enable spill
    DB::FailPointHelper::enableRandomFailPoint(DB::FailPoints::random_fail_in_resize_callback, 0.5);
    WRAP_FOR_SPILL_TEST_BEGIN
    context.context->setSetting("max_memory_usage", Field(static_cast<UInt64>(total_data_size * 1000)));
    context.context->setSetting("auto_memory_revoke_trigger_threshold", Field(0.7));
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreamsWithMemoryTracker(request, original_max_streams));
    WRAP_FOR_SPILL_TEST_END
    DB::FailPointHelper::disableFailPoint(DB::FailPoints::random_fail_in_resize_callback);
}
CATCH

#undef WRAP_FOR_SPILL_TEST_BEGIN
#undef WRAP_FOR_SPILL_TEST_END

} // namespace tests
} // namespace DB
