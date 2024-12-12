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

#include <TestUtils/AggregationTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace tests
{

class ExecutorWindowAgg : public DB::tests::AggregationTest {};

TEST_F(ExecutorWindowAgg, Avg)
try
{
    auto data_type_uint8 = std::make_shared<DataTypeUInt8>();
    auto context = TiFlashTestEnv::getContext();
    auto agg_func = AggregateFunctionFactory::instance().get(*context, "sum", {data_type_uint8}, {}, 0, true);
        // result_type = window_function_description.aggregate_function->getReturnType();
}
CATCH

TEST_F(ExecutorWindowAgg, Count)
try
{

}
CATCH

TEST_F(ExecutorWindowAgg, Sum)
try
{

}
CATCH

TEST_F(ExecutorWindowAgg, Min)
try
{

}
CATCH

TEST_F(ExecutorWindowAgg, Max)
try
{

}
CATCH

} // namespace tests
} // namespace DB
