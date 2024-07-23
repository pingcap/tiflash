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
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Operators/AutoPassThroughHashAggContext.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>

namespace DB
{
namespace tests
{
class BenchAutoPassThroughColumnGenerator : public ::benchmark::Fixture
{
public:
    // todo refactor with buildAutoPassHashAggThroughContext()
    void SetUp(const ::benchmark::State &) override
    try
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName("sum"))
            DB::registerAggregateFunctions();

        const size_t block_size = 65536;
        const size_t block_num = 100;
        auto data_type_int64 = std::make_shared<DataTypeInt64>();
        auto data_type_uint8 = std::make_shared<DataTypeUInt8>();
        auto data_type_int64_nullable = std::make_shared<DataTypeNullable>(data_type_int64);

        const size_t prec = 15;
        const size_t scale = 2;
        auto data_type_decimal = std::make_shared<DataTypeDecimal<Decimal64>>(prec, scale);

        auto data_type_decimal_nullable = std::make_shared<DataTypeNullable>(data_type_decimal);

        if (blocks.empty())
        {
            for (size_t i = 0; i < block_num; ++i)
            {
                {
                    auto col_int64 = ColumnGenerator::instance().generate(
                        {block_size, data_type_int64->getName(), RANDOM, col_int64_name});
                    auto col_decimal = ColumnGenerator::instance().generate(
                        {block_size, data_type_decimal->getName(), RANDOM, col_decimal_name});
                    blocks.push_back({col_int64, col_decimal});
                }

                {
                    auto col_int64 = ColumnGenerator::instance().generate(
                        {block_size, data_type_int64->getName(), RANDOM, col_int64_name});
                    auto col_decimal = ColumnGenerator::instance().generate(
                        {block_size, data_type_decimal->getName(), RANDOM, col_decimal_name});

                    auto col_nullmap = ColumnUInt8::create(block_size / 2, 0);
                    col_nullmap->reserve(block_size);
                    for (size_t i = 0; i < block_size / 2; ++i)
                        col_nullmap->insert(Field(static_cast<UInt64>(1)));

                    auto col_nullmap_1 = col_nullmap->clone();

                    auto col_int64_nullable
                        = ColumnNullable::create(std::move(col_int64.column), std::move(col_nullmap));
                    auto col_decimal_nullable
                        = ColumnNullable::create(std::move(col_decimal.column), std::move(col_nullmap_1));
                    nullable_blocks.push_back(
                        {{std::move(col_int64_nullable), data_type_int64_nullable, "int64_nullable"},
                         {std::move(col_decimal_nullable), data_type_decimal_nullable, "decimal_nullable"}});
                }
            }
        }

        auto context = TiFlashTestEnv::getContext();
        count_not_null_desc = AggregateDescription{
            .function = AggregateFunctionFactory::instance().get(*context, "count", {data_type_decimal}),
            .parameters = {},
            .arguments = {1},
            .argument_names = {col_decimal_name},
            .column_name = "out_col",
        };

        count_nullable_desc = AggregateDescription{
            .function = AggregateFunctionFactory::instance().get(*context, "count", {data_type_decimal_nullable}),
            .parameters = {},
            .arguments = {1},
            .argument_names = {col_decimal_name},
            .column_name = "out_col",
        };

        sum_not_null_desc = AggregateDescription{
            .function = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_decimal}),
            .parameters = {},
            .arguments = {1},
            .argument_names = {col_decimal_name},
            .column_name = "out_col",
        };

        sum_nullable_desc = AggregateDescription{
            .function = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_decimal_nullable}),
            .parameters = {},
            .arguments = {1},
            .argument_names = {col_decimal_name},
            .column_name = "out_col",
        };

        child_not_null_header = Block({{data_type_int64, col_int64_name}, {data_type_decimal, col_decimal_name}});
        child_nullable_header
            = Block({{data_type_int64_nullable, col_int64_name}, {data_type_decimal_nullable, col_decimal_name}});

        auto sum_agg_func = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_decimal});
        sum_not_null_header = Block({
            {sum_agg_func->getReturnType(), "out_col"},
        });
        sum_agg_func = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_decimal_nullable});
        sum_nullable_header = Block({
            {sum_agg_func->getReturnType(), "out_col"},
        });
        auto count_agg_func = AggregateFunctionFactory::instance().get(*context, "count", {data_type_decimal});
        count_not_null_header = Block({
            {count_agg_func->getReturnType(), "out_col"},
        });
        count_agg_func = AggregateFunctionFactory::instance().get(*context, "count", {data_type_decimal_nullable});
        count_nullable_header = Block({
            {count_agg_func->getReturnType(), "out_col"},
        });
    }
    CATCH

    const String col_decimal_name = "l_quantity";
    const String col_int64_name = "l_orderkey";

    AggregateDescription count_not_null_desc;
    AggregateDescription count_nullable_desc;
    AggregateDescription sum_nullable_desc;
    AggregateDescription sum_not_null_desc;

    Block sum_not_null_header;
    Block sum_nullable_header;
    Block count_not_null_header;
    Block count_nullable_header;
    Block child_not_null_header;
    Block child_nullable_header;

    // Make it static to avoid generate dataset multiple times.
    // Because a new BenchAutoPassThroughColumnGenerator object will be created
    // for each sub-benchmark.
    static std::vector<Block> blocks;
    static std::vector<Block> nullable_blocks;
};

std::vector<Block> BenchAutoPassThroughColumnGenerator::blocks;
std::vector<Block> BenchAutoPassThroughColumnGenerator::nullable_blocks;

#define DEFINE_GENERIC_BENCH(NAME, DESC, BLOCKS)                                            \
    BENCHMARK_DEFINE_F(BenchAutoPassThroughColumnGenerator, NAME)(benchmark::State & state) \
    try                                                                                     \
    {                                                                                       \
        for (const auto & _ : state)                                                        \
        {                                                                                   \
            std::vector<ColumnPtr> out_cols;                                                \
            out_cols.reserve(BenchAutoPassThroughColumnGenerator::BLOCKS.size());           \
            for (const auto & block : BenchAutoPassThroughColumnGenerator::BLOCKS)          \
            {                                                                               \
                out_cols.push_back(::DB::genPassThroughColumnGeneric(DESC, block));         \
            }                                                                               \
        }                                                                                   \
    }                                                                                       \
    CATCH                                                                                   \
    BENCHMARK_REGISTER_F(BenchAutoPassThroughColumnGenerator, NAME);

DEFINE_GENERIC_BENCH(generic_count_notnull, count_not_null_desc, blocks)
DEFINE_GENERIC_BENCH(generic_count_nullable, count_nullable_desc, nullable_blocks)
DEFINE_GENERIC_BENCH(generic_sum_notnull, sum_not_null_desc, blocks)
DEFINE_GENERIC_BENCH(generic_sum_nullable, sum_nullable_desc, nullable_blocks)

#define DEFINE_FAST_BENCH(NAME, DESC, HEADER, CHILD_HEADER, BLOCKS)                                         \
    BENCHMARK_DEFINE_F(BenchAutoPassThroughColumnGenerator, NAME)(benchmark::State & state)                 \
    try                                                                                                     \
    {                                                                                                       \
        auto generators = setupAutoPassThroughColumnGenerator(HEADER, CHILD_HEADER, {DESC}, Logger::get()); \
        assert(generators.size() == 1);                                                                     \
        for (const auto & _ : state)                                                                        \
        {                                                                                                   \
            std::vector<ColumnPtr> out_cols;                                                                \
            out_cols.reserve(BenchAutoPassThroughColumnGenerator::BLOCKS.size());                           \
            for (const auto & block : BenchAutoPassThroughColumnGenerator::BLOCKS)                          \
            {                                                                                               \
                out_cols.push_back(generators[0](block));                                                   \
            }                                                                                               \
        }                                                                                                   \
    }                                                                                                       \
    CATCH                                                                                                   \
    BENCHMARK_REGISTER_F(BenchAutoPassThroughColumnGenerator, NAME);

DEFINE_FAST_BENCH(fast_count_notnull, count_not_null_desc, count_not_null_header, child_not_null_header, blocks)
DEFINE_FAST_BENCH(
    fast_count_nullable,
    count_nullable_desc,
    count_nullable_header,
    child_nullable_header,
    nullable_blocks)
DEFINE_FAST_BENCH(fast_sum_notnull, sum_not_null_desc, sum_not_null_header, child_not_null_header, blocks)
DEFINE_FAST_BENCH(fast_sum_nullable, sum_nullable_desc, sum_nullable_header, child_nullable_header, nullable_blocks)

#undef DEFINE_GENERIC_BENCH
#undef DEFINE_FAST_BENCH
} // namespace tests
} // namespace DB
