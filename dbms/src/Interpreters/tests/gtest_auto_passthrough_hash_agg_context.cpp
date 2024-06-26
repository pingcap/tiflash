// Copyright 2024 PingCAP, Inc.
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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Interpreters/Context.h>
#include <Operators/AutoPassThroughHashAggContext.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/MPPTaskTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/mockExecutor.h>
#include <gtest/gtest.h>

#include <random>

namespace DB
{

namespace tests
{
// todo using func in DAGExpressionAnalyzer
void appendAggDescription(
    const Names & arg_names,
    const DataTypes & arg_types,
    TiDB::TiDBCollators & arg_collators,
    const String & agg_func_name,
    AggregateDescriptions & aggregate_descriptions,
    NamesAndTypes & aggregated_columns,
    bool empty_input_as_null,
    const Context & context)
{
    assert(arg_names.size() == arg_collators.size() && arg_names.size() == arg_types.size());

    AggregateDescription aggregate;
    aggregate.argument_names = arg_names;
    String func_string = genFuncString(agg_func_name, aggregate.argument_names, arg_collators);

    aggregate.column_name = func_string;
    aggregate.parameters = Array();
    aggregate.function
        = AggregateFunctionFactory::instance().get(context, agg_func_name, arg_types, {}, 0, empty_input_as_null);
    aggregate.function->setCollators(arg_collators);

    aggregated_columns.emplace_back(func_string, aggregate.function->getReturnType());

    aggregate_descriptions.emplace_back(std::move(aggregate));
}

class TestAutoPassThroughAggContext : public MPPTaskTestUtils
{
protected:
    void initializeContext() override
    try
    {
        ExecutorTest::initializeContext();

        if (inited)
            return;

        inited = true;

        col1_data_type = std::make_shared<DataTypeString>();
        col2_data_type = std::make_shared<DataTypeInt64>();

        auto_pass_through_context = buildAutoPassHashAggThroughContext();

        std::tie(low_ndv_blocks, low_ndv_block) = buildBlocks(/*block_num*/ 20, /*distinct_num*/ 10);
        std::tie(high_ndv_blocks, high_ndv_block) = buildBlocks(/*block_num*/ 20, /*distinct_num*/ 20 * block_size);
        std::tie(medium_ndv_blocks, medium_ndv_block) = buildBlocks(/*block_num*/ 20, /*distinct_num*/ block_size / 2);
        std::tie(random_blocks, random_block) = buildBlocks(/*block_num*/ 1, /*distinct_num*/ 100);

        context.addMockTable(
            {db_name, high_ndv_tbl_name},
            {{col1_name, TiDB::TP::TypeString, false}, {col2_name, TiDB::TP::TypeLongLong, false}},
            high_ndv_block.getColumnsWithTypeAndName());
        context.addMockTable(
            {db_name, low_ndv_tbl_name},
            {{col1_name, TiDB::TP::TypeString, false}, {col2_name, TiDB::TP::TypeLongLong, false}},
            low_ndv_block.getColumnsWithTypeAndName());
        context.addMockTable(
            {db_name, medium_ndv_tbl_name},
            {{col1_name, TiDB::TP::TypeString, false}, {col2_name, TiDB::TP::TypeLongLong, false}},
            medium_ndv_block.getColumnsWithTypeAndName());
    }
    CATCH

    // select repo_name, sum(commit_num) from repo_history group by repo_name;
    std::unique_ptr<AutoPassThroughHashAggContext> buildAutoPassHashAggThroughContext()
    {
        const String req_id("TestAutoPassThroughAggContext");

        Block before_agg_header(NamesAndTypes{{col1_name, col1_data_type}, {col2_name, col2_data_type}});

        size_t agg_streams_size = 1;
        size_t before_agg_stream_size = 1;
        Names key_names{col1_name};

        KeyRefAggFuncMap key_ref_agg_func;
        AggFuncRefKeyMap agg_func_ref_key;

        // Assume no collator
        std::unordered_map<String, TiDB::TiDBCollatorPtr> collators{{col1_name, nullptr}};

        TiDB::TiDBCollators arg_collators{nullptr};

        AggregateDescriptions aggregate_descriptions;
        NamesAndTypes aggregated_columns;
        appendAggDescription(
            Names{col1_name},
            DataTypes{col1_data_type},
            arg_collators,
            "first_row",
            aggregate_descriptions,
            aggregated_columns,
            /*empty_input_as_null*/ true,
            *context.context);
        appendAggDescription(
            Names{col2_name},
            DataTypes{col2_data_type},
            arg_collators,
            "sum",
            aggregate_descriptions,
            aggregated_columns,
            /*empty_input_as_null*/ true, // todo?
            *context.context);

        AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);

        SpillConfig spill_config(
            context.context->getTemporaryPath(),
            req_id,
            context.context->getSettingsRef().max_cached_data_bytes_in_spiller,
            context.context->getSettingsRef().max_spilled_rows_per_file,
            context.context->getSettingsRef().max_spilled_bytes_per_file,
            context.context->getFileProvider(),
            context.context->getSettingsRef().max_threads,
            context.context->getSettingsRef().max_block_size);

        auto params = AggregationInterpreterHelper::buildParams(
            *context.context,
            before_agg_header,
            before_agg_stream_size,
            agg_streams_size,
            key_names,
            key_ref_agg_func,
            agg_func_ref_key,
            collators,
            aggregate_descriptions,
            /*final=*/true,
            spill_config);
        return std::make_unique<AutoPassThroughHashAggContext>(
            *params,
            [&]() { return false; },
            req_id);
    }

    std::pair<BlocksList, Block> buildBlocks(size_t block_num, size_t distinct_num)
    {
        auto distinct_repo_names = generateDistinctStrings(distinct_num);
        auto distinct_commit_nums = generateDistinctIntegers(distinct_num);

        auto col1_in_one = col1_data_type->createColumn();
        auto col2_in_one = col2_data_type->createColumn();

        BlocksList res;
        for (size_t i = 0; i < block_num; ++i)
        {
            auto col1 = col1_data_type->createColumn();
            auto col2 = col2_data_type->createColumn();

            for (size_t j = 0; j < block_size; ++j)
            {
                size_t idx = (i * block_size + j) % distinct_num;
                auto repo_name = distinct_repo_names[idx];
                auto commit_num = distinct_commit_nums[idx];
                col1->insert(Field(repo_name.data(), repo_name.size()));
                col2->insert(Field(static_cast<Int64>(commit_num)));

                col1_in_one->insert(Field(repo_name.data(), repo_name.size()));
                col2_in_one->insert(Field(static_cast<Int64>(commit_num)));
            }
            ColumnsWithTypeAndName cols{
                {std::move(col1), col1_data_type, col1_name},
                {std::move(col2), col2_data_type, col2_name}};
            res.push_back(Block(cols));
        }
        ColumnsWithTypeAndName cols{
            {std::move(col1_in_one), col1_data_type, col1_name},
            {std::move(col2_in_one), col2_data_type, col2_name}};
        Block res_in_one(cols);
        return {res, res_in_one};
    }

    static std::string generateRandomString(
        size_t length,
        std::mt19937 & gen,
        std::uniform_int_distribution<> & charDist)
    {
        const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        std::string result;
        for (size_t i = 0; i < length; ++i)
        {
            result += chars[charDist(gen)];
        }
        return result;
    }

    static std::vector<std::string> generateDistinctStrings(size_t n)
    {
        std::unordered_set<std::string> unique_strings;
        std::vector<std::string> result;

        std::random_device rd;
        std::mt19937 gen(rd());

        std::normal_distribution<> length_dist(50, 15);
        std::uniform_int_distribution<> char_dist(0, 61);

        while (unique_strings.size() < n)
        {
            size_t length;
            do
            {
                length = static_cast<size_t>(std::round(length_dist(gen)));
            } while (length < 1 || length > 100);

            std::string new_string = generateRandomString(length, gen, char_dist);

            if (unique_strings.find(new_string) == unique_strings.end())
            {
                unique_strings.insert(new_string);
                result.push_back(new_string);
            }
        }

        return result;
    }

    static std::vector<Int32> generateDistinctIntegers(size_t n)
    {
        std::unordered_set<int> unique_ints;
        std::vector<int> result;

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dist(0, std::numeric_limits<Int32>::max());

        while (unique_ints.size() < n)
        {
            int new_int = dist(gen);
            if (unique_ints.find(new_int) == unique_ints.end())
            {
                unique_ints.insert(new_int);
                result.push_back(new_int);
            }
        }

        return result;
    }

    const size_t block_size = 8096;
    std::unique_ptr<AutoPassThroughHashAggContext> auto_pass_through_context;
    const String col1_name = "repo_name";
    const String col2_name = "commit_num";
    const String db_name = "test_db";
    const String high_ndv_tbl_name = "test_tbl_high_ndv";
    const String low_ndv_tbl_name = "test_tbl_low_ndv";
    const String medium_ndv_tbl_name = "test_tbl_medium_ndv";

    DataTypePtr col1_data_type;
    DataTypePtr col2_data_type;

    BlocksList high_ndv_blocks;
    BlocksList low_ndv_blocks;
    BlocksList medium_ndv_blocks;
    BlocksList random_blocks;

    Block high_ndv_block;
    Block low_ndv_block;
    Block medium_ndv_block;
    Block random_block;

    bool inited = false;
};

TEST_F(TestAutoPassThroughAggContext, stateSwitch)
try
{
    size_t state_processed_rows = 0;
    const auto state_processed_row_limit = auto_pass_through_context->getNonAdjustRowLimit();
    const auto adjust_state_rows_limit = auto_pass_through_context->getAdjustRowLimit();

    // Expect InitState
    EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Init);

    // Expect switch to AdjustState
    auto_pass_through_context->onBlock(high_ndv_blocks.front());
    high_ndv_blocks.pop_front();
    EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Adjust);

    // Expect switch to SelectiveState
    while (true)
    {
        auto & block = medium_ndv_blocks.front();
        auto_pass_through_context->onBlock(block);
        state_processed_rows += block.rows();
        medium_ndv_blocks.pop_front();
        if (state_processed_rows >= adjust_state_rows_limit)
            break;

        EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Adjust);
    }
    EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Selective);

    // Expect switch to AdjustState
    state_processed_rows = 0;
    while (true)
    {
        auto_pass_through_context->onBlock(random_blocks.front());
        state_processed_rows += random_blocks.front().rows();

        if (state_processed_rows >= state_processed_row_limit)
            break;

        EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Selective);
    }
    EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Adjust);

    // Expect switch to PreAggState
    state_processed_rows = 0;
    while (true)
    {
        auto & block = low_ndv_blocks.front();
        auto_pass_through_context->onBlock(block);
        state_processed_rows += block.rows();
        low_ndv_blocks.pop_front();

        if (state_processed_rows >= adjust_state_rows_limit)
            break;

        EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Adjust);
    }
    EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::PreHashAgg);

    // Expect switch to AdjustState
    state_processed_rows = 0;
    while (true)
    {
        auto_pass_through_context->onBlock(random_blocks.front());
        state_processed_rows += random_blocks.front().rows();

        if (state_processed_rows >= state_processed_row_limit)
            break;

        EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::PreHashAgg);
    }
    EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Adjust);

    // Expect switch to PassThroughState
    state_processed_rows = 0;
    while (true)
    {
        auto & block = high_ndv_blocks.front();
        auto_pass_through_context->onBlock(block);
        state_processed_rows += block.rows();
        high_ndv_blocks.pop_front();

        if (state_processed_rows >= adjust_state_rows_limit)
            break;

        EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::Adjust);
    }
    EXPECT_EQ(auto_pass_through_context->getCurState(), AutoPassThroughHashAggContext::State::PassThrough);
}
CATCH

// Using different workload(low/high/medium ndv) to run auto_pass_through hashagg,
// which cover logic of interpretion, execution and exchange.
TEST_F(TestAutoPassThroughAggContext, integration)
try
{
    auto workloads = std::vector{high_ndv_tbl_name, low_ndv_tbl_name, medium_ndv_tbl_name};
    for (const auto & tbl_name : workloads)
    {
        LOG_DEBUG(Logger::get(), "TestAutoPassThroughAggContext iteration, tbl_name: {}", tbl_name);
        auto req_auto_pass_through
            = context.scan(db_name, tbl_name)
                  .aggregation(
                      {makeASTFunction("first_row", col(col1_name)), makeASTFunction("max", col(col2_name))},
                      {col(col1_name)},
                      0,
                      true)
                  .build(context);

        auto req_no_pass_through
            = context.scan(db_name, tbl_name)
                  .aggregation(
                      {makeASTFunction("first_row", col(col1_name)), makeASTFunction("max", col(col2_name))},
                      {col(col1_name)},
                      0,
                      false)
                  .build(context);

        const size_t concurrency = 1;
        auto res_no_pass_through = executeStreams(req_no_pass_through, concurrency);

        // Only run 1-st hashagg for auto_pass_through hashagg, so the result is not same with non-auto_pass_through.
        // But the result size should be same.
        enablePipeline(false);
        auto res_auto_pass_through = executeStreams(req_auto_pass_through, concurrency);
        ASSERT_EQ(res_no_pass_through.size(), res_auto_pass_through.size());

        enablePipeline(true);
        res_auto_pass_through = executeStreams(req_auto_pass_through, concurrency);
        ASSERT_EQ(res_no_pass_through.size(), res_auto_pass_through.size());

        // 2-staged Aggregation to test. Expect auto_pass_through hashagg result equal to non-auto_pass_through hashagg.
        {
            startServers(4);
            auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
            auto req_auto_pass_through
                = context.scan(db_name, tbl_name)
                      .aggregation(
                          {makeASTFunction("first_row", col(col1_name)), makeASTFunction("max", col(col2_name))},
                          {col(col1_name)},
                          0,
                          true)
                      .buildMPPTasks(context, properties);

            enablePipeline(false);
            auto res_auto_pass_through = executeMPPTasks(req_auto_pass_through, properties);
            ASSERT_COLUMNS_EQ_UR(res_no_pass_through, res_auto_pass_through);

            enablePipeline(true);
            res_auto_pass_through = executeMPPTasks(req_auto_pass_through, properties);
            ASSERT_COLUMNS_EQ_UR(res_no_pass_through, res_auto_pass_through);
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
