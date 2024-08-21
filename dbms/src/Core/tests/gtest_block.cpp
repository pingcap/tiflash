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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnString.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <Functions/FunctionHelpers.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

using namespace DB::DM;
using namespace DB::DM::tests;

namespace DB
{
namespace tests
{
class BlockTest : public testing::Test
{
};

TEST_F(BlockTest, TestEstimateBytesForSpillNormalColumn)
try
{
    ColumnsWithTypeAndName columns;
    std::vector<String> all_types{
        "Int64",
        "Int32",
        "UInt64",
        "UInt32",
        "Decimal(5,2)",
        "Decimal(10,2)",
        "Decimal(20,2)",
        "Decimal(40,2)",
        "MyDate",
        "MyDateTime",
        "String",
        "FixedString(10)"};
    for (auto & type_name : all_types)
    {
        DataTypePtr types[2];
        types[0] = DataTypeFactory::instance().get(type_name);
        types[1] = makeNullable(types[0]);
        for (auto & type : types)
        {
            auto column = type->createColumn();
            for (size_t i = 0; i < 10; i++)
                column->insertDefault();
            columns.emplace_back(std::move(column), type);
        }
    }
    Block block(columns);
    ASSERT_TRUE(block.allocatedBytes() == block.estimateBytesForSpill());
}
CATCH

TEST_F(BlockTest, TestEstimateBytesForSpillColumnAggregateFunction)
try
{
    DB::registerAggregateFunctions();
    ArenaPtr pool = std::make_shared<Arena>();
    pool->alloc(1024 * 1024);
    /// case 1, agg function not allocate memory in arena
    std::vector<String> types{"Int64", "String", "Nullable(Int64)", "Nullable(String)"};
    std::vector<size_t> data_size{
        16,
        ColumnString::APPROX_STRING_SIZE * 2,
        24,
        ColumnString::APPROX_STRING_SIZE * 2 + 8};
    for (size_t i = 0; i < types.size(); ++i)
    {
        auto agg_data_type = DataTypeFactory::instance().get(fmt::format("AggregateFunction(Min, {})", types[i]));
        auto agg_column = agg_data_type->createColumn();
        auto agg_func = typeid_cast<ColumnAggregateFunction *>(agg_column.get())->getAggregateFunction();
        auto size_of_aggregate_states = agg_func->sizeOfData();
        auto align_aggregate_states = agg_func->alignOfData();
        typeid_cast<ColumnAggregateFunction *>(agg_column.get())->addArena(pool);
        for (size_t j = 0; j < 10; ++j)
        {
            auto * aggregate_data = pool->alignedAlloc(size_of_aggregate_states, align_aggregate_states);
            agg_func->create(aggregate_data);
            agg_column->insertData(reinterpret_cast<const char *>(&aggregate_data), data_size[i]);
        }
        ColumnsWithTypeAndName columns;
        columns.emplace_back(std::move(agg_column), agg_data_type);
        Block block(columns);
        ASSERT_NE(block.estimateBytesForSpill(), block.allocatedBytes());
        ASSERT_EQ(block.estimateBytesForSpill(), data_size[i] * 10);
    }
    /// case 2, agg function allocate memory in arena
    for (size_t i = 0; i < types.size(); ++i)
    {
        auto agg_data_type = DataTypeFactory::instance().get(fmt::format("AggregateFunction(uniqExact, {})", types[i]));
        auto agg_column = agg_data_type->createColumn();
        auto agg_func = typeid_cast<ColumnAggregateFunction *>(agg_column.get())->getAggregateFunction();
        typeid_cast<ColumnAggregateFunction *>(agg_column.get())->addArena(pool);
        auto size_of_aggregate_states = agg_func->sizeOfData();
        auto align_aggregate_states = agg_func->alignOfData();
        for (size_t j = 0; j < 10; ++j)
        {
            auto * aggregate_data = pool->alignedAlloc(size_of_aggregate_states, align_aggregate_states);
            agg_func->create(aggregate_data);
            agg_column->insertData(reinterpret_cast<const char *>(&aggregate_data), data_size[i]);
        }
        ColumnsWithTypeAndName columns;
        columns.emplace_back(std::move(agg_column), agg_data_type);
        Block block(columns);
        ASSERT_EQ(block.estimateBytesForSpill(), block.allocatedBytes());
    }
}
CATCH

TEST_F(BlockTest, TestReserveInVstackBlocks)
try
{
    size_t block_size = 50;
    size_t rows_per_block = 100;
    /// for both big and small string, the reserve in vstackBlock is expected to reserve enough memory before actually insert
    /// for small string, the reserve in vstackBlock is expected to not reserve too much memory
    String long_str(ColumnString::APPROX_STRING_SIZE * 5, 'a');
    String short_str(std::max(1, ColumnString::APPROX_STRING_SIZE / 10), 'a');
    std::vector<String> string_values{short_str, long_str};
    std::vector<String> types{"String", "Nullable(String)"};
    for (const auto & string_value : string_values)
    {
        for (const auto & type_string : types)
        {
            auto data_type = DataTypeFactory::instance().get(type_string);
            Blocks blocks;
            for (size_t i = 0; i < block_size; i++)
            {
                auto column = data_type->createColumn();
                for (size_t j = 0; j < rows_per_block; j++)
                {
                    if (data_type->isNullable() && j % 2 == 0)
                        column->insert(Field());
                    else
                        column->insert(string_value);
                }
                ColumnsWithTypeAndName columns;
                columns.emplace_back(std::move(column), data_type);
                blocks.emplace_back(std::move(columns));
            }
            auto result_block = vstackBlocks<true>(std::move(blocks));
            ASSERT_EQ(result_block.columns(), 1);
            if (string_value.size() < ColumnString::APPROX_STRING_SIZE)
            {
                size_t allocated_bytes = result_block.getByPosition(0).column->allocatedBytes();
                ASSERT_TRUE(result_block.rows() * ColumnString::APPROX_STRING_SIZE > allocated_bytes);
            }
        }
    }
}
CATCH

template <typename F>
void permutationRSResults(F && check)
{
    const RSResults candidate_rs_results{RSResult::All, RSResult::AllNull, RSResult::Some, RSResult::SomeNull};
    for (auto a : candidate_rs_results)
        for (auto b : candidate_rs_results)
            for (auto c : candidate_rs_results)
                for (auto d : candidate_rs_results)
                    check({a, b, c, d});
}

RSResult logicalAnd(const RSResults & rs_results)
{
    auto res = RSResult::All;
    for (auto rs_result : rs_results)
        res = res && rs_result;
    return res;
}

TEST_F(BlockTest, VstackBlocksRSResult)
try
{
    auto check_vstack_blocks = [](const RSResults & rs_results) {
        Blocks blocks;
        size_t start = 0;
        constexpr size_t num_rows = 10;
        for (auto rs_result : rs_results)
        {
            blocks.push_back(DMTestEnv::prepareSimpleWriteBlock(start, start + num_rows, false));
            blocks.back().setRSResult(rs_result);
            start += num_rows;
        }
        auto b = vstackBlocks(std::move(blocks));
        ASSERT_EQ(b.getRSResult(), logicalAnd(rs_results));
    };
    permutationRSResults(std::move(check_vstack_blocks));
}
CATCH

TEST_F(BlockTest, HstackBlocksRSResult)
try
{
    auto create_column_defines = [](int count) {
        ColumnDefines column_defines;
        for (int i = 0; i < count; ++i)
            column_defines.emplace_back(i + 1, std::to_string(i), std::make_shared<DataTypeInt64>());
        return column_defines;
    };
    auto check_hstack_blocks = [&](const RSResults & rs_results) {
        auto column_defines = create_column_defines(rs_results.size());
        auto header = toEmptyBlock(column_defines);
        Blocks blocks;
        constexpr size_t num_rows = 10;
        for (size_t i = 0; i < rs_results.size(); ++i)
        {
            const auto & cd = column_defines[i];
            Block block;
            block.insert(createColumn<Int64>(createSignedNumbers(0, num_rows), cd.name, cd.id));
            block.setRSResult(rs_results[i]);
            blocks.push_back(std::move(block));
        }
        auto b = hstackBlocks(std::move(blocks), header);
        ASSERT_EQ(b.getRSResult(), logicalAnd(rs_results));
    };
    permutationRSResults(std::move(check_hstack_blocks));
}
CATCH
} // namespace tests
} // namespace DB
