// Copyright 2022 PingCAP, Ltd.
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
#include <AggregateFunctions/AggregateFunctionSum.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <TestUtils/AggregationTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <tuple>
#include <vector>

namespace DB
{
namespace tests
{

class ExecutorAggFuncReturnTypeTestRunner : public DB::tests::AggregationTest
{
public:
    ::testing::AssertionResult checkAggReturnType(const String & agg_name, const DataTypes & data_types, const DataTypePtr & expect_type)
    {
        AggregateFunctionPtr agg_ptr = DB::AggregateFunctionFactory::instance().get(agg_name, data_types, {});
        const DataTypePtr & ret_type = agg_ptr->getReturnType();
        if (ret_type->getName() == expect_type->getName())
            return ::testing::AssertionSuccess();
        return ::testing::AssertionFailure() << "Expect type: " << expect_type->getName() << " Actual type: " << ret_type->getName();
    }

    template <typename T>
    ::testing::AssertionResult testSumOnPartialResult()
    {
        for (size_t nullable = 0; nullable <= 1; ++nullable)
        {
            PrecType precision = maxDecimalPrecision<T>() - 1;
            ScaleType scale = 5;
            std::shared_ptr<DataTypeDecimal<T>> input_nested_type_ptr = std::make_shared<DataTypeDecimal<T>>(precision, scale);
            std::shared_ptr<DataTypeDecimal<T>> expect_output_nested_type_ptr = std::make_shared<DataTypeDecimal<T>>(precision, scale);
            DataTypePtr input_type_ptr = nullable == 0 ? static_cast<DataTypePtr>(input_nested_type_ptr) : static_cast<DataTypePtr>(std::make_shared<DataTypeNullable>(input_nested_type_ptr));
            DataTypePtr expect_output_type_ptr = nullable == 0 ? static_cast<DataTypePtr>(expect_output_nested_type_ptr) : static_cast<DataTypePtr>(std::make_shared<DataTypeNullable>(expect_output_type_ptr));
            auto result = checkAggReturnType(NameSumOnPartialResult::name, {input_type_ptr}, expect_output_type_ptr);
            if (result)
                continue;
            return result;
        }

        return ::testing::AssertionSuccess();
    }
};

TEST_F(ExecutorAggFuncReturnTypeTestRunner, AggregationSum)
try
{
    // Test output type of the second stage sum aggregation function
    ASSERT_TRUE(testSumOnPartialResult<Decimal32>());
    ASSERT_TRUE(testSumOnPartialResult<Decimal64>());
    ASSERT_TRUE(testSumOnPartialResult<Decimal128>());
    ASSERT_TRUE(testSumOnPartialResult<Decimal256>());
}
CATCH

} // namespace tests
} // namespace DB
