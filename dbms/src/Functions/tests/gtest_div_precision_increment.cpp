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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Logger.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <TestUtils/AggregationTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
const std::string func_name = "tidbDivide";
class TestDivPrecisionIncre : public DB::tests::FunctionTest
{
public:
    static void SetUpTestCase()
    {
        FunctionTest::SetUpTestCase();
        AggregationTest::SetUpTestCase();
    }
};

using DecimalField32 = DecimalField<Decimal32>;

TEST_F(TestDivPrecisionIncre, RejectsNegativeIncrement)
{
    // Bypass request validation to exercise the AVG and division scale protections directly.
    context->getDAGContext()->setDivPrecisionIncrement(-1);
    const DataTypes argument_types{createDecimal(10, 2)};
    EXPECT_THROW(AggregateFunctionFactory::instance().get(*context, "avg", argument_types, {}), TiFlashException);
    EXPECT_THROW(
        DB::tests::executeFunction(
            *context,
            func_name,
            {createColumn<Decimal64>(std::make_tuple(10, 2), {"1.00", "2.00"}),
             createColumn<Decimal64>(std::make_tuple(10, 0), {"2", "1"})}),
        TiFlashException);
}

TEST_F(TestDivPrecisionIncre, castDecimalAsRealBasic)
try
{
    context->getDAGContext()->setDivPrecisionIncrement(4);
    /// Decimal32
    ASSERT_COLUMN_EQ_V2(
        createColumn<Nullable<Decimal64>>(std::make_tuple(12, 5), {"0.73333", "1.65000"}),
        DB::tests::executeFunction(
            *context,
            func_name,
            {createColumn<Decimal32>(std::make_tuple(8, 1), {"2.2", "3.3"}),
             createColumn<Decimal32>(std::make_tuple(2, 0), {"3", "2"})}));

    /// div_precision_increment 5
    context->getDAGContext()->setDivPrecisionIncrement(5);
    ASSERT_COLUMN_EQ_V2(
        createColumn<Nullable<Decimal64>>(std::make_tuple(13, 6), {"0.733333", "1.650000"}),
        DB::tests::executeFunction(
            *context,
            func_name,
            {createColumn<Decimal32>(std::make_tuple(8, 1), {"2.2", "3.3"}),
             createColumn<Decimal32>(std::make_tuple(2, 0), {"3", "2"})}));

    /// div_precision_increment 0
    context->getDAGContext()->setDivPrecisionIncrement(0);
    ASSERT_COLUMN_EQ_V2(
        createColumn<Nullable<Decimal32>>(std::make_tuple(8, 1), {"0.7", "1.7"}),
        DB::tests::executeFunction(
            *context,
            func_name,
            {createColumn<Decimal32>(std::make_tuple(8, 1), {"2.2", "3.3"}),
             createColumn<Decimal32>(std::make_tuple(2, 0), {"3", "2"})}));

    /// div_precision_increment 30
    context->getDAGContext()->setDivPrecisionIncrement(30);
    ASSERT_COLUMN_EQ_V2(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 30),
            {"0.733333333333333333333333333333", "1.650000000000000000000000000000"}),
        DB::tests::executeFunction(
            *context,
            func_name,
            {createColumn<Decimal32>(std::make_tuple(8, 1), {"2.2", "3.3"}),
             createColumn<Decimal32>(std::make_tuple(2, 0), {"3", "2"})}));
}
CATCH

} // namespace
} // namespace DB::tests
