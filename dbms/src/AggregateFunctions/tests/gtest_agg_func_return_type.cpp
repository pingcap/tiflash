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

#include <AggregateFunctions/AggregateFunctionSum.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <TestUtils/AggregationTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/mockExecutor.h>

#include <tuple>
#include <vector>

namespace DB
{
namespace tests
{

class ExecutorAggFuncReturnTypeTestRunner : public DB::tests::AggregationTest
{
public:
    template <typename T>
    ::testing::AssertionResult testSumOnPartialResult()
    {
        for (size_t nullable = 0; nullable <= 1; ++nullable)
        {
            PrecType max_precision = maxDecimalPrecision<T>();
            std::vector<std::pair<PrecType, ScaleType>> cases{
                {max_precision, 0},
                {max_precision - 1, 1},
                {max_precision - 2, 2},
                {max_precision - 3, 3},
            };

            for (const auto & p : cases)
            {
                PrecType precision = p.first;
                ScaleType scale = p.second;
                std::shared_ptr<DataTypeDecimal<T>> input_nested_type_ptr
                    = std::make_shared<DataTypeDecimal<T>>(precision, scale);
                std::shared_ptr<DataTypeDecimal<T>> expect_output_nested_type_ptr
                    = std::make_shared<DataTypeDecimal<T>>(precision, scale);
                DataTypePtr input_type_ptr = nullable == 0
                    ? static_cast<DataTypePtr>(input_nested_type_ptr)
                    : static_cast<DataTypePtr>(std::make_shared<DataTypeNullable>(input_nested_type_ptr));
                DataTypePtr expect_output_type_ptr = nullable == 0
                    ? static_cast<DataTypePtr>(expect_output_nested_type_ptr)
                    : static_cast<DataTypePtr>(std::make_shared<DataTypeNullable>(expect_output_nested_type_ptr));
                auto result
                    = checkAggReturnType(NameSumOnPartialResult::name, {input_type_ptr}, expect_output_type_ptr);
                if (result)
                    continue;
                return result;
            }
        }

        return ::testing::AssertionSuccess();
    }

    UInt64 evalCountAgg(const String & agg_name, const DataTypePtr & data_type, const std::vector<Field> & values)
    {
        AggregateFunctionPtr agg_ptr
            = DB::AggregateFunctionFactory::instance().get(*TiFlashTestEnv::getContext(), agg_name, {data_type}, {});
        auto data_col = data_type->createColumn();
        for (const auto & value : values)
            data_col->insert(value);

        Arena arena;
        auto * place = arena.alignedAlloc(agg_ptr->sizeOfData(), agg_ptr->alignOfData());
        agg_ptr->create(place);
        const IColumn * cols[] = {data_col.get()};
        for (size_t i = 0; i < values.size(); ++i)
            agg_ptr->add(place, cols, i, &arena);

        auto result_col = agg_ptr->getReturnType()->createColumn();
        agg_ptr->insertResultInto(place, *result_col, &arena);
        agg_ptr->destroy(place);
        return typeid_cast<const ColumnUInt64 &>(*result_col).getData()[0];
    }
};

TEST_F(ExecutorAggFuncReturnTypeTestRunner, AggregationSum)
try
{
    // Test output type of the sum aggregation function which receives partial result
    ASSERT_TRUE(testSumOnPartialResult<Decimal32>());
    ASSERT_TRUE(testSumOnPartialResult<Decimal64>());
    ASSERT_TRUE(testSumOnPartialResult<Decimal128>());
    ASSERT_TRUE(testSumOnPartialResult<Decimal256>());
}
CATCH

TEST_F(ExecutorAggFuncReturnTypeTestRunner, AggregationMinMaxCount)
try
{
    auto int_type = std::make_shared<DataTypeInt64>();
    auto nullable_int_type = makeNullable(int_type);

    ASSERT_EQ(evalCountAgg("max_count", int_type, {Field(Int64(1)), Field(Int64(3)), Field(Int64(3)), Field(Int64(2))}), 2);
    ASSERT_EQ(
        evalCountAgg(
            "min_count",
            nullable_int_type,
            {Field(), Field(Int64(2)), Field(Int64(1)), Field(Int64(1)), Field()}),
        2);
    ASSERT_EQ(evalCountAgg("max_count", nullable_int_type, {Field(), Field(), Field()}), 0);
    ASSERT_EQ(evalCountAgg("min_count", nullable_int_type, {Field(), Field(), Field()}), 0);
}
CATCH

} // namespace tests
} // namespace DB
