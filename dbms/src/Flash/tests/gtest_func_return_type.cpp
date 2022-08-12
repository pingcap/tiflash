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
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <vector>
#include <tuple>

namespace DB
{
namespace tests
{

class ExecutorFuncReturnTypeTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
    }

    ::testing::AssertionResult checkAggReturnType(const String & agg_name, const DataTypes & data_types, const DataTypePtr & expect_type)
    {
        AggregateFunctionPtr agg_ptr = DB::AggregateFunctionFactory::instance().get(agg_name, data_types, {});
        const DataTypePtr & ret_type = agg_ptr->getReturnType();
        if (ret_type->getName() == expect_type->getName())
            return ::testing::AssertionSuccess();
        return ::testing::AssertionFailure() << "Expect type: " << expect_type->getName() << " Actual type: " << ret_type->getName();
    }

    template <typename T>
    ::testing::AssertionResult testSecondStageSumAgg()
    {
        PrecType precision = maxDecimalPrecision<T>() - 1;
        ScaleType scale = 5;
        std::shared_ptr<IDataType> input_type_ptr = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
        std::shared_ptr<IDataType> expect_output_type_ptr = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
        return checkAggReturnType(NameFinalSumStage::name, {input_type_ptr}, expect_output_type_ptr);
    }
};

TEST_F(ExecutorFuncReturnTypeTestRunner, AggregationSum)
try
{
    // Test output type of the second stage sum aggregation function
    ASSERT_TRUE(testSecondStageSumAgg<Decimal32>());
    ASSERT_TRUE(testSecondStageSumAgg<Decimal64>());
    ASSERT_TRUE(testSecondStageSumAgg<Decimal128>());
    ASSERT_TRUE(testSecondStageSumAgg<Decimal256>());
}
CATCH

} // namespace tests
} // namespace DB
