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

#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class TestFunctionLog : public DB::tests::FunctionTest
{
};

TEST_F(TestFunctionLog, Log)
try
{
    String func_name = "log";
    /// not null column
    auto input = createColumn<Float64>({-1, 0, 0.5, 1, 2});
    auto ref = std::log(0.5);
    auto output = createColumn<Nullable<Float64>>({{}, {}, ref, 0, -ref});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// nullable column
    input = createColumn<Nullable<Float64>>({{}, -1, 0, 0.5, 1, 2});
    output = createColumn<Nullable<Float64>>({{}, {}, {}, ref, 0, -ref});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// not null constant
    input = createConstColumn<Float64>(5, 0);
    output = createConstColumn<Nullable<Float64>>(5, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    input = createConstColumn<Float64>(5, 1);
    output = createConstColumn<Nullable<Float64>>(5, 0);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// nullable constant with not null value
    input = createConstColumn<Nullable<Float64>>(5, 0);
    output = createConstColumn<Nullable<Float64>>(5, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    input = createConstColumn<Nullable<Float64>>(5, 1);
    output = createConstColumn<Nullable<Float64>>(5, 0);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// nullable constant with null value
    input = createConstColumn<Nullable<Float64>>(5, {});
    output = createConstColumn<Nullable<Float64>>(5, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// typeNothing
    input = createOnlyNullColumnConst(5);
    output = createOnlyNullColumnConst(5);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// don't need to test other data tpe like int/decimal since TiDB will ensure the input must be float64
}
CATCH

TEST_F(TestFunctionLog, Log2Args)
try
{
    String func_name = "log2args";
    /// func(column,column)
    auto input1 = createColumn<Float64>({1, 0, -1, 2, 2, 2});
    auto input2 = createColumn<Float64>({2, 2, 2, 0, -1, 2});
    auto output = createColumn<Nullable<Float64>>({{}, {}, {}, {}, {}, 1});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    input1 = createColumn<Nullable<Float64>>({1, 0, -1, 2, 2, 2, {}});
    input2 = createColumn<Float64>({2, 2, 2, 0, -1, 2, 2});
    output = createColumn<Nullable<Float64>>({{}, {}, {}, {}, {}, 1, {}});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    input1 = createColumn<Float64>({1, 0, -1, 2, 2, 2, 2});
    input2 = createColumn<Nullable<Float64>>({2, 2, 2, 0, -1, 2, {}});
    output = createColumn<Nullable<Float64>>({{}, {}, {}, {}, {}, 1, {}});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    input1 = createColumn<Nullable<Float64>>({1, 0, -1, 2, 2, 2, 2});
    input2 = createColumn<Nullable<Float64>>({2, 2, 2, 0, -1, 2, {}});
    output = createColumn<Nullable<Float64>>({{}, {}, {}, {}, {}, 1, {}});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    /// func(const,column)
    input1 = createConstColumn<Float64>(7, 2);
    input2 = createColumn<Float64>({2, 2, 2, 0, -1, 2, 2});
    output = createColumn<Nullable<Float64>>({1, 1, 1, {}, {}, 1, 1});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    /// func(column,const)
    input1 = createColumn<Nullable<Float64>>({1, 0, -1, 2, 2, 2, {}});
    input2 = createConstColumn<Float64>(7, 2);
    output = createColumn<Nullable<Float64>>({{}, {}, {}, 1, 1, 1, {}});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    /// func(const,const)
    input1 = createConstColumn<Float64>(7, 2);
    input2 = createConstColumn<Float64>(7, 2);
    output = createConstColumn<Nullable<Float64>>(7, 1);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    input1 = createConstColumn<Float64>(7, 1);
    input2 = createConstColumn<Float64>(7, 2);
    output = createConstColumn<Nullable<Float64>>(7, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    /// typeNothing
    input1 = createOnlyNullColumnConst(5);
    input2 = createConstColumn<Float64>(5, 2);
    output = createOnlyNullColumnConst(5);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    input1 = createConstColumn<Float64>(5, 2);
    input2 = createOnlyNullColumnConst(5);
    output = createOnlyNullColumnConst(5);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
    input1 = createOnlyNullColumnConst(5);
    input2 = createOnlyNullColumnConst(5);
    output = createOnlyNullColumnConst(5);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input1, input2));
}
CATCH

TEST_F(TestFunctionLog, Log2)
try
{
    String func_name = "log2";
    /// not null column
    auto input = createColumn<Float64>({-1, 0, 0.5, 1, 2});
    auto ref = std::log2(0.5);
    auto output = createColumn<Nullable<Float64>>({{}, {}, ref, 0, -ref});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// nullable column
    input = createColumn<Nullable<Float64>>({{}, -1, 0, 0.5, 1, 2});
    output = createColumn<Nullable<Float64>>({{}, {}, {}, ref, 0, -ref});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// not null constant
    input = createConstColumn<Float64>(5, 0);
    output = createConstColumn<Nullable<Float64>>(5, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    input = createConstColumn<Float64>(5, 1);
    output = createConstColumn<Nullable<Float64>>(5, 0);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// nullable constant with not null value
    input = createConstColumn<Nullable<Float64>>(5, 0);
    output = createConstColumn<Nullable<Float64>>(5, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    input = createConstColumn<Nullable<Float64>>(5, 1);
    output = createConstColumn<Nullable<Float64>>(5, 0);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// nullable constant with null value
    input = createConstColumn<Nullable<Float64>>(5, {});
    output = createConstColumn<Nullable<Float64>>(5, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// typeNothing
    input = createOnlyNullColumnConst(5);
    output = createOnlyNullColumnConst(5);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// don't need to test other data tpe like int/decimal since TiDB will ensure the input must be float64
}
CATCH

TEST_F(TestFunctionLog, Log10)
try
{
    String func_name = "log10";
    /// not null column
    auto input = createColumn<Float64>({-1, 0, 0.5, 1, 2});
    auto ref = std::log10(0.5);
    auto output = createColumn<Nullable<Float64>>({{}, {}, ref, 0, -ref});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// nullable column
    input = createColumn<Nullable<Float64>>({{}, -1, 0, 0.5, 1, 2});
    output = createColumn<Nullable<Float64>>({{}, {}, {}, ref, 0, -ref});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// not null constant
    input = createConstColumn<Float64>(5, 0);
    output = createConstColumn<Nullable<Float64>>(5, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    input = createConstColumn<Float64>(5, 1);
    output = createConstColumn<Nullable<Float64>>(5, 0);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// nullable constant with not null value
    input = createConstColumn<Nullable<Float64>>(5, 0);
    output = createConstColumn<Nullable<Float64>>(5, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    input = createConstColumn<Nullable<Float64>>(5, 1);
    output = createConstColumn<Nullable<Float64>>(5, 0);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// nullable constant with null value
    input = createConstColumn<Nullable<Float64>>(5, {});
    output = createConstColumn<Nullable<Float64>>(5, {});
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// typeNothing
    input = createOnlyNullColumnConst(5);
    output = createOnlyNullColumnConst(5);
    ASSERT_COLUMN_EQ(output, executeFunction(func_name, input));
    /// don't need to test other data tpe like int/decimal since TiDB will ensure the input must be float64
}
CATCH

} // namespace tests

} // namespace DB
