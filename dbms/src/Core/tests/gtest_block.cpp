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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnString.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <Encryption/MockKeyManager.h>
#include <Functions/FunctionHelpers.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>


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
    std::vector<String> all_types{"Int64", "Int32", "UInt64", "UInt32", "Decimal(5,2)", "Decimal(10,2)", "Decimal(20,2)", "Decimal(40,2)", "MyDate", "MyDateTime", "String", "FixedString(10)"};
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
    std::vector<size_t> data_size{16, ColumnString::APPROX_STRING_SIZE * 2, 24, ColumnString::APPROX_STRING_SIZE * 2 + 8};
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

} // namespace tests
} // namespace DB
