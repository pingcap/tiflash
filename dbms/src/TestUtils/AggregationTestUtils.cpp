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

#include <DataTypes/IDataType.h>
#include <TestUtils/AggregationTestUtils.h>

namespace DB::tests
{
void AggregationTest::SetUpTestCase()
{
    auto register_func = [](std::function<void()> func) {
        try
        {
            func();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    };

    register_func(DB::registerAggregateFunctions);
}

::testing::AssertionResult AggregationTest::checkAggReturnType(
    const String & agg_name,
    const DataTypes & data_types,
    const DataTypePtr & expect_type)
{
    AggregateFunctionPtr agg_ptr
        = DB::AggregateFunctionFactory::instance().get(*TiFlashTestEnv::getContext(), agg_name, data_types, {});
    const DataTypePtr & ret_type = agg_ptr->getReturnType();
    if (ret_type->equals(*expect_type))
        return ::testing::AssertionSuccess();
    return ::testing::AssertionFailure() << "Expect type: " << expect_type->getName()
                                         << " Actual type: " << ret_type->getName();
}

} // namespace DB::tests
