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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Operators/AutoPassThroughHashAggContext.h>
#include <Core/Block.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>

#include <benchmark/benchmark.h>

namespace DB
{
namespace tests
{
class BenchGetPassThroughBlock : public ::benchmark::Fixture
{
public:
    // todo refactor with buildAutoPassHashAggThroughContext()
    void SetUp(const ::benchmark::State &) override
    {
        if (!blocks.empty())
            return;

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

        for (size_t i = 0; i < block_num; ++i)
        {
            {
                auto col_int64 = ColumnGenerator::instance().generate({block_size, data_type_int64->getName(), RANDOM, col_int64_name});
                auto col_decimal = ColumnGenerator::instance().generate({block_size, data_type_decimal->getName(), RANDOM, col_decimal_name});
                blocks.push_back({col_int64, col_decimal});
            }

            {
                auto col_int64 = ColumnGenerator::instance().generate({block_size, data_type_int64->getName(), RANDOM, col_int64_name});
                auto col_decimal = ColumnGenerator::instance().generate({block_size, data_type_decimal->getName(), RANDOM, col_decimal_name});

                auto col_nullmap = ColumnUInt8::create(block_size/2, 0);
                col_nullmap->reserve(block_size);
                for (size_t i = 0; i < block_size/2; ++i)
                    col_nullmap->insert(Field(static_cast<UInt64>(1)));

                auto col_nullmap_1 = col_nullmap->clone();

                auto col_int64_nullable = ColumnNullable::create(std::move(col_int64.column), std::move(col_nullmap));
                auto col_decimal_nullable = ColumnNullable::create(std::move(col_decimal.column), std::move(col_nullmap_1));
                nullable_blocks.push_back({
                        {std::move(col_int64_nullable), data_type_int64_nullable, "int64_nullable"},
                        {std::move(col_decimal_nullable), data_type_decimal_nullable, "decimal_nullable"}});
            }
        }

        // todo sum or sumWithOverflow
        auto context = TiFlashTestEnv::getContext();
        sum_agg_func = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_decimal});

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

        sum_nullable_desc = AggregateDescription{
            // todo sum sumWithOverflow?
            .function = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_decimal_nullable}),
            .parameters = {},
            .arguments = {1},
            .argument_names = {col_decimal_name},
            .column_name = "out_col",
        };
    }

    const String col_decimal_name = "l_quantity";
    const String col_int64_name = "l_orderkey";
    std::vector<Block> blocks;
    std::vector<Block> nullable_blocks;

    AggregateFunctionPtr sum_agg_func;
    AggregateDescription count_not_null_desc;
    AggregateDescription count_nullable_desc;
    AggregateDescription sum_nullable_desc;
};

BENCHMARK_DEFINE_F(BenchGetPassThroughBlock, genByStaticCast)(benchmark::State & state)
try
{
    for (const auto & _ : state)
    {
        std::vector<ColumnPtr> out_cols;
        for (const auto & block : blocks)
        {
            const auto & col_decimal = block.getByName(col_decimal_name);
            const auto * col_decimal_ptr = checkAndGetColumn<ColumnDecimal<Decimal64>>(col_decimal.column.get());
            const auto & datas = col_decimal_ptr->getData();

            const size_t scale = col_decimal_ptr->getScale();
            
            auto out_col = ColumnDecimal<Decimal128>::create(0, scale);
            out_col->reserve(datas.size());
            for (const auto & val : datas)
            {
                out_col->insert(Field(DecimalField(static_cast<Decimal128>(val), scale)));
            }
            out_cols.push_back(std::move(out_col));
        }
    }
}
CATCH
BENCHMARK_REGISTER_F(BenchGetPassThroughBlock, genByStaticCast);

BENCHMARK_DEFINE_F(BenchGetPassThroughBlock, genByAggFunc)(benchmark::State & state)
try
{
    for (const auto & _ : state)
    {
        std::vector<ColumnPtr> out_cols;
        out_cols.reserve(blocks.size());
        for (const auto & block : blocks)
        {
            Arena arena;
            auto * place = arena.alignedAlloc(sum_agg_func->sizeOfData(), sum_agg_func->alignOfData());

            ColumnRawPtrs argument_columns(1);
            argument_columns[0] = block.getByName(col_decimal_name).column.get();

            const auto * col_decimal_ptr = checkAndGetColumn<ColumnDecimal<Decimal64>>(block.getByName(col_decimal_name).column.get());
            auto out_col = ColumnDecimal<Decimal128>::create(0, col_decimal_ptr->getScale());
            for (size_t row = 0; row < block.rows(); ++row)
            {
                sum_agg_func->create(place);
                sum_agg_func->add(place, argument_columns.data(), row, &arena);
                sum_agg_func->insertResultInto(place, *out_col, &arena);
            }
            out_cols.push_back(std::move(out_col));
        }
    }
}
CATCH
BENCHMARK_REGISTER_F(BenchGetPassThroughBlock, genByAggFunc);

BENCHMARK_DEFINE_F(BenchGetPassThroughBlock, genCountNotNullFast)(benchmark::State & state)
try
{
    for (const auto & _ : state)
    {
        std::vector<ColumnPtr> out_cols;
        out_cols.reserve(blocks.size());
        for (const auto & block : blocks)
        {
            out_cols.push_back(::DB::getPassThroughColumnForCount(count_not_null_desc, block));
        }
    }
}
CATCH
BENCHMARK_REGISTER_F(BenchGetPassThroughBlock, genCountNotNullFast);

BENCHMARK_DEFINE_F(BenchGetPassThroughBlock, genCountNotNullGeneric)(benchmark::State & state)
try
{
    for (const auto & _ : state)
    {
        std::vector<ColumnPtr> out_cols;
        out_cols.reserve(blocks.size());
        Arena arena;
        for (const auto & block : blocks)
        {
            out_cols.push_back(::DB::getPassThroughColumnGeneric(count_not_null_desc, block, arena));
        }
    }
}
CATCH
BENCHMARK_REGISTER_F(BenchGetPassThroughBlock, genCountNotNullGeneric);

// todo macro
BENCHMARK_DEFINE_F(BenchGetPassThroughBlock, genCountNullableFast)(benchmark::State & state)
try
{
    for (const auto & _ : state)
    {
        std::vector<ColumnPtr> out_cols;
        out_cols.reserve(blocks.size());
        for (const auto & block : nullable_blocks)
        {
            out_cols.push_back(::DB::getPassThroughColumnForCount(count_nullable_desc, block));
        }
    }
}
CATCH
BENCHMARK_REGISTER_F(BenchGetPassThroughBlock, genCountNullableFast);

BENCHMARK_DEFINE_F(BenchGetPassThroughBlock, genCountNullableGeneric)(benchmark::State & state)
try
{
    for (const auto & _ : state)
    {
        std::vector<ColumnPtr> out_cols;
        out_cols.reserve(blocks.size());
        Arena arena;
        for (const auto & block : nullable_blocks)
        {
            out_cols.push_back(::DB::getPassThroughColumnGeneric(count_nullable_desc, block, arena));
        }
    }
}
CATCH
BENCHMARK_REGISTER_F(BenchGetPassThroughBlock, genCountNullableGeneric);

// todo macro
BENCHMARK_DEFINE_F(BenchGetPassThroughBlock, genSumNullableFast)(benchmark::State & state)
try
{
    for (const auto & _ : state)
    {
        std::vector<ColumnPtr> out_cols;
        out_cols.reserve(blocks.size());
        for (const auto & block : nullable_blocks)
        {
            out_cols.push_back(::DB::getPassThroughColumnForSum(sum_nullable_desc, block));
        }
    }
}
CATCH
BENCHMARK_REGISTER_F(BenchGetPassThroughBlock, genSumNullableFast);

BENCHMARK_DEFINE_F(BenchGetPassThroughBlock, genSumNullableGeneric)(benchmark::State & state)
try
{
    for (const auto & _ : state)
    {
        std::vector<ColumnPtr> out_cols;
        out_cols.reserve(blocks.size());
        Arena arena;
        for (const auto & block : nullable_blocks)
        {
            out_cols.push_back(::DB::getPassThroughColumnGeneric(sum_nullable_desc, block, arena));
        }
    }
}
CATCH
BENCHMARK_REGISTER_F(BenchGetPassThroughBlock, genSumNullableGeneric);
} // namespace tests
} // namespace DB
