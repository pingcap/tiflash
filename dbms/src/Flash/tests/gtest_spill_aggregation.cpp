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

#include <Interpreters/Context.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace FailPoints
{
extern const char force_agg_on_partial_block[];
extern const char force_thread_0_no_agg_spill[];
extern const char force_agg_prefetch[];
} // namespace FailPoints

namespace tests
{
class SpillAggregationTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        dag_context_ptr->log = Logger::get("AggSpillTest");
    }
};

#define WRAP_FOR_AGG_FAILPOINTS_START                                                  \
    std::vector<bool> enables{true, false};                                            \
    for (auto enable : enables)                                                        \
    {                                                                                  \
        if (enable)                                                                    \
        {                                                                              \
            FailPointHelper::enableFailPoint(FailPoints::force_agg_on_partial_block);  \
            FailPointHelper::enableFailPoint(FailPoints::force_agg_prefetch);          \
        }                                                                              \
        else                                                                           \
        {                                                                              \
            FailPointHelper::disableFailPoint(FailPoints::force_agg_on_partial_block); \
            FailPointHelper::disableFailPoint(FailPoints::force_agg_prefetch);         \
        }

#define WRAP_FOR_AGG_FAILPOINTS_END }

#define WRAP_FOR_AGG_THREAD_0_NO_SPILL_START                                           \
    for (auto thread_0_no_spill : {true, false})                                       \
    {                                                                                  \
        if (thread_0_no_spill)                                                         \
            FailPointHelper::enableFailPoint(FailPoints::force_thread_0_no_agg_spill); \
        else                                                                           \
            FailPointHelper::disableFailPoint(FailPoints::force_thread_0_no_agg_spill);

#define WRAP_FOR_AGG_THREAD_0_NO_SPILL_END }


#define WRAP_FOR_SPILL_TEST_BEGIN                  \
    std::vector<bool> pipeline_bools{false, true}; \
    for (auto enable_pipeline : pipeline_bools)    \
    {                                              \
        enablePipeline(enable_pipeline);

#define WRAP_FOR_SPILL_TEST_END }

/// todo add more tests
TEST_F(SpillAggregationTestRunner, SimpleCase)
try
{
    DB::MockColumnInfoVec column_infos{
        {"a", TiDB::TP::TypeLongLong},
        {"b", TiDB::TP::TypeLongLong},
        {"c", TiDB::TP::TypeLongLong},
        {"d", TiDB::TP::TypeLongLong},
        {"e", TiDB::TP::TypeLongLong}};
    ColumnsWithTypeAndName column_datas;
    size_t table_rows = 12800;
    size_t duplicated_rows = 6400;
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
    enablePipeline(false);
    auto ref_columns = executeStreams(request, original_max_streams);
    /// enable spill
    WRAP_FOR_SPILL_TEST_BEGIN
    context.context->setSetting(
        "max_bytes_before_external_group_by",
        Field(static_cast<UInt64>(total_data_size / 200)));
    context.context->setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(1)));
    context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(1)));
    /// don't use `executeAndAssertColumnsEqual` since it takes too long to run
    /// test single thread aggregation
    WRAP_FOR_AGG_FAILPOINTS_START
    WRAP_FOR_AGG_THREAD_0_NO_SPILL_START
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, 1));
    /// test parallel aggregation
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
    WRAP_FOR_AGG_THREAD_0_NO_SPILL_END
    WRAP_FOR_AGG_FAILPOINTS_END
    /// enable spill and use small max_cached_data_bytes_in_spiller
    context.context->setSetting("max_cached_data_bytes_in_spiller", Field(static_cast<UInt64>(total_data_size / 200)));
    /// test single thread aggregation
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, 1));
    /// test parallel aggregation
    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
    /// test spill with small max_block_size
    /// the avg rows in one bucket is ~10240/256 = 400, so set the small_max_block_size to 100
    /// is enough to test the output spilt
    size_t small_max_block_size = 100;
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(small_max_block_size)));
    auto blocks = getExecuteStreamsReturnBlocks(request, 1);
    for (auto & block : blocks)
    {
        ASSERT_EQ(block.rows() <= small_max_block_size, true);
    }
    ASSERT_COLUMNS_EQ_UR(ref_columns, vstackBlocks(std::move(blocks)).getColumnsWithTypeAndName());
    blocks = getExecuteStreamsReturnBlocks(request, original_max_streams);
    for (auto & block : blocks)
    {
        ASSERT_EQ(block.rows() <= small_max_block_size, true);
    }
    ASSERT_COLUMNS_EQ_UR(ref_columns, vstackBlocks(std::move(blocks)).getColumnsWithTypeAndName());
    WRAP_FOR_SPILL_TEST_END
}
CATCH

TEST_F(SpillAggregationTestRunner, AggWithSpecialGroupKey)
try
{
    /// prepare data
    size_t unique_rows = 3000;
    DB::MockColumnInfoVec table_column_infos{
        {"key_8", TiDB::TP::TypeTiny, false},
        {"key_16", TiDB::TP::TypeShort, false},
        {"key_32", TiDB::TP::TypeLong, false},
        {"key_64", TiDB::TP::TypeLongLong, false},
        {"key_string_1", TiDB::TP::TypeString, false},
        {"key_string_2", TiDB::TP::TypeString, false},
        {"value", TiDB::TP::TypeLong, false}};
    ColumnsWithTypeAndName table_column_data;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(table_column_infos))
    {
        ColumnGeneratorOpts opts{
            unique_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        table_column_data.push_back(ColumnGenerator::instance().generate(opts));
    }
    for (auto & table_column : table_column_data)
    {
        if (table_column.name != "value")
            table_column.column->assumeMutable()->insertRangeFrom(*table_column.column, 0, unique_rows / 2);
        else
        {
            ColumnGeneratorOpts opts{unique_rows / 2, table_column.type->getName(), RANDOM, table_column.name};
            auto column = ColumnGenerator::instance().generate(opts);
            table_column.column->assumeMutable()->insertRangeFrom(*column.column, 0, unique_rows / 2);
        }
    }
    ColumnWithTypeAndName shuffle_column
        = ColumnGenerator::instance().generate({unique_rows + unique_rows / 2, "UInt64", RANDOM});
    IColumn::Permutation perm;
    shuffle_column.column->getPermutation(false, 0, -1, perm);
    for (auto & column : table_column_data)
    {
        column.column = column.column->permute(perm, 0);
    }

    context.addMockTable("test_db", "agg_table_with_special_key", table_column_infos, table_column_data);

    size_t max_block_size = 800;
    size_t max_bytes_before_external_agg = 100;
    std::vector<size_t> concurrences{1, 8};
    std::vector<Int64> collators{TiDB::ITiDBCollator::UTF8MB4_BIN, TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI};
    std::vector<std::vector<String>> group_by_keys{
        /// fast path with one int and one string in bin collation
        {"key_64", "key_string_1"},
        /// fast path with two string in bin collation
        {"key_string_1", "key_string_2"},
        /// fast path with one string in bin collation
        {"key_string_1"},
        /// keys need to be shuffled
        {"key_8", "key_16", "key_32", "key_64"},
    };
    std::vector<std::vector<ASTPtr>> agg_funcs{{Max(col("value"))}, {Max(col("value")), Min(col("value"))}};
    for (auto collator_id : collators)
    {
        for (const auto & keys : group_by_keys)
        {
            for (const auto & agg_func : agg_funcs)
            {
                context.setCollation(collator_id);
                const auto * current_collator = TiDB::ITiDBCollator::getCollator(collator_id);
                ASSERT_TRUE(current_collator != nullptr);
                bool has_string_key = false;
                MockAstVec key_vec;
                for (const auto & key : keys)
                    key_vec.push_back(col(key));
                auto request = context.scan("test_db", "agg_table_with_special_key")
                                   .aggregation(agg_func, key_vec)
                                   .build(context);
                /// use one level, no block split, no spill as the reference
                context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(0)));
                context.context->setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(0)));
                context.context->setSetting("max_block_size", Field(static_cast<UInt64>(unique_rows * 2)));
                enablePipeline(false);
                auto reference = executeStreams(request, 1);
                SortDescription sd;
                if (current_collator->isCI())
                {
                    /// for ci collation, need to sort and compare the result manually
                    for (const auto & result_col : reference)
                    {
                        if (!removeNullable(result_col.type)->isString())
                        {
                            sd.push_back(SortColumnDescription(result_col.name, 1, 1, nullptr));
                        }
                        else
                        {
                            sd.push_back(SortColumnDescription(result_col.name, 1, 1, current_collator));
                            has_string_key = true;
                        }
                    }
                    /// don't run ci test if there is no string key
                    if (!has_string_key)
                        continue;
                    Block tmp_block(reference);
                    sortBlock(tmp_block, sd);
                    reference = tmp_block.getColumnsWithTypeAndName();
                }
                for (auto concurrency : concurrences)
                {
                    context.context->setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(1)));
                    context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(1)));
                    context.context->setSetting(
                        "max_bytes_before_external_group_by",
                        Field(static_cast<UInt64>(max_bytes_before_external_agg)));
                    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
                    WRAP_FOR_SPILL_TEST_BEGIN
                    WRAP_FOR_AGG_FAILPOINTS_START
                    WRAP_FOR_AGG_THREAD_0_NO_SPILL_START
                    auto blocks = getExecuteStreamsReturnBlocks(request, concurrency);
                    for (auto & block : blocks)
                    {
                        block.checkNumberOfRows();
                        ASSERT(block.rows() <= max_block_size);
                    }
                    if (current_collator->isCI())
                    {
                        auto merged_block = vstackBlocks(std::move(blocks));
                        sortBlock(merged_block, sd);
                        auto merged_columns = merged_block.getColumnsWithTypeAndName();
                        for (size_t col_index = 0; col_index < reference.size(); col_index++)
                            ASSERT_TRUE(columnEqual(
                                reference[col_index].column,
                                merged_columns[col_index].column,
                                sd[col_index].collator));
                    }
                    else
                    {
                        ASSERT_TRUE(columnsEqual(
                            reference,
                            vstackBlocks(std::move(blocks)).getColumnsWithTypeAndName(),
                            false));
                    }
                    WRAP_FOR_AGG_THREAD_0_NO_SPILL_END
                    WRAP_FOR_AGG_FAILPOINTS_END
                    WRAP_FOR_SPILL_TEST_END
                }
            }
        }
    }
}
CATCH

TEST_F(SpillAggregationTestRunner, AggWithDistinctAggFunc)
try
{
    /// prepare data
    size_t unique_rows = 3000;
    DB::MockColumnInfoVec table_column_infos{
        {"key_8", TiDB::TP::TypeTiny, false},
        {"key_16", TiDB::TP::TypeShort, false},
        {"key_32", TiDB::TP::TypeLong, false},
        {"key_64", TiDB::TP::TypeLongLong, false},
        {"key_string_1", TiDB::TP::TypeString, false},
        {"key_string_2", TiDB::TP::TypeString, false},
        {"value_1", TiDB::TP::TypeString, false},
        {"value_2", TiDB::TP::TypeLong, false},
    };
    size_t key_column = 6;
    ColumnsWithTypeAndName table_column_data;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(table_column_infos))
    {
        ColumnGeneratorOpts opts{
            unique_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        table_column_data.push_back(ColumnGenerator::instance().generate(opts));
    }
    for (size_t i = 0; i < key_column; i++)
        table_column_data[i].column->assumeMutable()->insertRangeFrom(*table_column_data[i].column, 0, unique_rows / 2);
    for (size_t i = key_column; i < table_column_data.size(); i++)
    {
        auto & table_column = table_column_data[i];
        ColumnGeneratorOpts opts{unique_rows / 2, table_column.type->getName(), RANDOM, table_column.name};
        auto column = ColumnGenerator::instance().generate(opts);
        table_column.column->assumeMutable()->insertRangeFrom(*column.column, 0, unique_rows / 2);
    }

    ColumnWithTypeAndName shuffle_column
        = ColumnGenerator::instance().generate({unique_rows + unique_rows / 2, "UInt64", RANDOM});
    IColumn::Permutation perm;
    shuffle_column.column->getPermutation(false, 0, -1, perm);
    for (auto & column : table_column_data)
    {
        column.column = column.column->permute(perm, 0);
    }

    context.addMockTable("test_db", "agg_table_with_special_key", table_column_infos, table_column_data);

    size_t max_block_size = 800;
    size_t max_bytes_before_external_agg = 100;
    std::vector<size_t> concurrences{1, 8};
    std::vector<Int64> collators{TiDB::ITiDBCollator::UTF8MB4_BIN, TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI};
    std::vector<std::vector<String>> group_by_keys{
        /// fast path with one int and one string
        {"key_64", "key_string_1"},
        /// fast path with two string
        {"key_string_1", "key_string_2"},
        /// fast path with one string
        {"key_string_1"},
        /// keys need to be shuffled
        {"key_8", "key_16", "key_32", "key_64"},
    };
    std::vector<std::vector<ASTPtr>> agg_funcs{
        {Max(col("value_1")), CountDistinct(col("value_2"))},
        {CountDistinct(col("value_1")), CountDistinct(col("value_2"))},
        {CountDistinct(col("value_1"))}};
    for (auto collator_id : collators)
    {
        for (const auto & keys : group_by_keys)
        {
            for (const auto & agg_func : agg_funcs)
            {
                context.setCollation(collator_id);
                const auto * current_collator = TiDB::ITiDBCollator::getCollator(collator_id);
                ASSERT_TRUE(current_collator != nullptr);
                bool has_string_key = false;
                MockAstVec key_vec;
                for (const auto & key : keys)
                    key_vec.push_back(col(key));
                auto request = context.scan("test_db", "agg_table_with_special_key")
                                   .aggregation(agg_func, key_vec)
                                   .build(context);
                /// use one level, no block split, no spill as the reference
                context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(0)));
                context.context->setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(0)));
                context.context->setSetting("max_block_size", Field(static_cast<UInt64>(unique_rows * 2)));
                enablePipeline(false);
                auto reference = executeStreams(request, 1);
                SortDescription sd;
                if (current_collator->isCI())
                {
                    /// for ci collation, need to sort and compare the result manually
                    for (const auto & result_col : reference)
                    {
                        if (!removeNullable(result_col.type)->isString())
                        {
                            sd.push_back(SortColumnDescription(result_col.name, 1, 1, nullptr));
                        }
                        else
                        {
                            sd.push_back(SortColumnDescription(result_col.name, 1, 1, current_collator));
                            has_string_key = true;
                        }
                    }
                    /// don't run ci test if there is no string key
                    if (!has_string_key)
                        continue;
                    Block tmp_block(reference);
                    sortBlock(tmp_block, sd);
                    reference = tmp_block.getColumnsWithTypeAndName();
                }
                for (auto concurrency : concurrences)
                {
                    context.context->setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(1)));
                    context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(1)));
                    context.context->setSetting(
                        "max_bytes_before_external_group_by",
                        Field(static_cast<UInt64>(max_bytes_before_external_agg)));
                    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
                    WRAP_FOR_SPILL_TEST_BEGIN
                    WRAP_FOR_AGG_FAILPOINTS_START
                    WRAP_FOR_AGG_THREAD_0_NO_SPILL_START
                    auto blocks = getExecuteStreamsReturnBlocks(request, concurrency);
                    for (auto & block : blocks)
                    {
                        block.checkNumberOfRows();
                        ASSERT(block.rows() <= max_block_size);
                    }
                    if (current_collator->isCI())
                    {
                        auto merged_block = vstackBlocks(std::move(blocks));
                        sortBlock(merged_block, sd);
                        auto merged_columns = merged_block.getColumnsWithTypeAndName();
                        for (size_t col_index = 0; col_index < reference.size(); col_index++)
                            ASSERT_TRUE(columnEqual(
                                reference[col_index].column,
                                merged_columns[col_index].column,
                                sd[col_index].collator));
                    }
                    else
                    {
                        ASSERT_TRUE(columnsEqual(
                            reference,
                            vstackBlocks(std::move(blocks)).getColumnsWithTypeAndName(),
                            false));
                    }
                    WRAP_FOR_AGG_THREAD_0_NO_SPILL_END
                    WRAP_FOR_AGG_FAILPOINTS_END
                    WRAP_FOR_SPILL_TEST_END
                }
            }
        }
    }
}
CATCH

TEST_F(SpillAggregationTestRunner, FineGrainedShuffle)
try
{
    DB::MockColumnInfoVec column_infos{
        {"a", TiDB::TP::TypeLongLong},
        {"b", TiDB::TP::TypeLongLong},
        {"c", TiDB::TP::TypeLongLong},
        {"d", TiDB::TP::TypeLongLong},
        {"e", TiDB::TP::TypeLongLong}};
    DB::MockColumnInfoVec partition_column_infos{{"a", TiDB::TP::TypeLongLong}, {"b", TiDB::TP::TypeLongLong}};
    ColumnsWithTypeAndName column_datas;
    size_t table_rows = 5120;
    size_t duplicated_rows = 2560;
    UInt64 max_block_size = 100;
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
    context
        .addExchangeReceiver("exchange_receiver_1_concurrency", column_infos, column_datas, 1, partition_column_infos);
    context
        .addExchangeReceiver("exchange_receiver_3_concurrency", column_infos, column_datas, 3, partition_column_infos);
    context
        .addExchangeReceiver("exchange_receiver_5_concurrency", column_infos, column_datas, 5, partition_column_infos);
    context.addExchangeReceiver(
        "exchange_receiver_10_concurrency",
        column_infos,
        column_datas,
        10,
        partition_column_infos);
    std::vector<size_t> exchange_receiver_concurrency = {1, 3, 5, 10};

    auto gen_request = [&](size_t exchange_concurrency) {
        return context
            .receive(fmt::format("exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency)
            .aggregation({Min(col("c")), Max(col("d")), Count(col("e"))}, {col("a"), col("b")}, exchange_concurrency)
            .build(context);
    };
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));

    /// disable spill
    context.context->setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(0)));
    enablePipeline(false);
    auto baseline = executeStreams(gen_request(1), 1);

    /// enable spill
    context.context->setSetting(
        "max_bytes_before_external_group_by",
        Field(static_cast<UInt64>(total_data_size / 200)));
    context.context->setSetting("group_by_two_level_threshold", Field(static_cast<UInt64>(1)));
    context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(1)));
    for (size_t exchange_concurrency : exchange_receiver_concurrency)
    {
        /// don't use `executeAndAssertColumnsEqual` since it takes too long to run
        auto request = gen_request(exchange_concurrency);
        WRAP_FOR_SPILL_TEST_BEGIN
        WRAP_FOR_AGG_FAILPOINTS_START
        ASSERT_COLUMNS_EQ_UR(baseline, executeStreams(request, exchange_concurrency));
        WRAP_FOR_AGG_FAILPOINTS_END
        WRAP_FOR_SPILL_TEST_END
    }
}
CATCH

#undef WRAP_FOR_SPILL_TEST_BEGIN
#undef WRAP_FOR_SPILL_TEST_END
#undef WRAP_FOR_AGG_FAILPOINTS_START
#undef WRAP_FOR_AGG_FAILPOINTS_END

} // namespace tests
} // namespace DB
