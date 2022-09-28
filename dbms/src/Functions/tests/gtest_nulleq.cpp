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

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Common/MyDuration.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionsDateTime.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class TestNullEq : public DB::tests::FunctionTest
{
protected:
    DataTypePtr getReturnTypeForNullEq(const DataTypePtr & type_1, const DataTypePtr & type_2)
    {
        ColumnsWithTypeAndName input_columns{
            {nullptr, type_1, ""},
            {nullptr, type_2, ""},
        };
        return getReturnTypeForFunction(context, "nullEq", input_columns);
    }
};

TEST_F(TestNullEq, TestTypeInfer)
try
{
    auto test_type = [&](const String & col_1_type_name, const String & col_2_type_name, const String & result_type_name) {
        auto result_type = DataTypeFactory::instance().get(result_type_name);
        auto col_1_type = DataTypeFactory::instance().get(col_1_type_name);
        auto col_2_type = DataTypeFactory::instance().get(col_2_type_name);
        ASSERT_TRUE(result_type->equals(*getReturnTypeForNullEq(col_1_type, col_2_type)));
    };
    /// test integer type Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64
    const auto result_type_name_nulleq = "UInt8";
    test_type("Int8", "Int8", result_type_name_nulleq);
    test_type("Int8", "Int16", result_type_name_nulleq);
    test_type("Int8", "Int32", result_type_name_nulleq);
    test_type("Int8", "Int64", result_type_name_nulleq);
    test_type("Int8", "UInt8", result_type_name_nulleq);
    test_type("Int8", "UInt16", result_type_name_nulleq);
    test_type("Int8", "UInt32", result_type_name_nulleq);
    test_type("Int8", "UInt64", result_type_name_nulleq);

    test_type("UInt8", "Int8", result_type_name_nulleq);
    test_type("UInt8", "Int16", result_type_name_nulleq);
    test_type("UInt8", "Int32", result_type_name_nulleq);
    test_type("UInt8", "Int64", result_type_name_nulleq);
    test_type("UInt8", "UInt8", result_type_name_nulleq);
    test_type("UInt8", "UInt16", result_type_name_nulleq);
    test_type("UInt8", "UInt32", result_type_name_nulleq);
    test_type("UInt8", "UInt64", result_type_name_nulleq);

    test_type("Int16", "Int8", result_type_name_nulleq);
    test_type("Int16", "Int16", result_type_name_nulleq);
    test_type("Int16", "Int32", result_type_name_nulleq);
    test_type("Int16", "Int64", result_type_name_nulleq);
    test_type("Int16", "UInt8", result_type_name_nulleq);
    test_type("Int16", "UInt16", result_type_name_nulleq);
    test_type("Int16", "UInt32", result_type_name_nulleq);
    test_type("Int16", "UInt64", result_type_name_nulleq);

    test_type("UInt16", "Int8", result_type_name_nulleq);
    test_type("UInt16", "Int16", result_type_name_nulleq);
    test_type("UInt16", "Int32", result_type_name_nulleq);
    test_type("UInt16", "Int64", result_type_name_nulleq);
    test_type("UInt16", "UInt8", result_type_name_nulleq);
    test_type("UInt16", "UInt16", result_type_name_nulleq);
    test_type("UInt16", "UInt32", result_type_name_nulleq);
    test_type("UInt16", "UInt64", result_type_name_nulleq);

    test_type("Int32", "Int8", result_type_name_nulleq);
    test_type("Int32", "Int16", result_type_name_nulleq);
    test_type("Int32", "Int32", result_type_name_nulleq);
    test_type("Int32", "Int64", result_type_name_nulleq);
    test_type("Int32", "UInt8", result_type_name_nulleq);
    test_type("Int32", "UInt16", result_type_name_nulleq);
    test_type("Int32", "UInt32", result_type_name_nulleq);
    test_type("Int32", "UInt64", result_type_name_nulleq);

    test_type("UInt32", "Int8", result_type_name_nulleq);
    test_type("UInt32", "Int16", result_type_name_nulleq);
    test_type("UInt32", "Int32", result_type_name_nulleq);
    test_type("UInt32", "Int64", result_type_name_nulleq);
    test_type("UInt32", "UInt8", result_type_name_nulleq);
    test_type("UInt32", "UInt16", result_type_name_nulleq);
    test_type("UInt32", "UInt32", result_type_name_nulleq);
    test_type("UInt32", "UInt64", result_type_name_nulleq);

    test_type("Int64", "Int8", result_type_name_nulleq);
    test_type("Int64", "Int16", result_type_name_nulleq);
    test_type("Int64", "Int32", result_type_name_nulleq);
    test_type("Int64", "Int64", result_type_name_nulleq);
    test_type("Int64", "UInt8", result_type_name_nulleq);
    test_type("Int64", "UInt16", result_type_name_nulleq);
    test_type("Int64", "UInt32", result_type_name_nulleq);
    test_type("Int64", "UInt64", result_type_name_nulleq);

    test_type("UInt64", "Int8", result_type_name_nulleq);
    test_type("UInt64", "Int16", result_type_name_nulleq);
    test_type("UInt64", "Int32", result_type_name_nulleq);
    test_type("UInt64", "Int64", result_type_name_nulleq);
    test_type("UInt64", "UInt8", result_type_name_nulleq);
    test_type("UInt64", "UInt16", result_type_name_nulleq);
    test_type("UInt64", "UInt32", result_type_name_nulleq);
    test_type("UInt64", "UInt64", result_type_name_nulleq);

    /// test type infer for real
    test_type("Float32", "Float32", result_type_name_nulleq);
    test_type("Float32", "Float64", result_type_name_nulleq);
    test_type("Float64", "Float32", result_type_name_nulleq);
    test_type("Float64", "Float64", result_type_name_nulleq);

    /// test type infer for string
    test_type("String", "String", result_type_name_nulleq);

    /// test type infer for decimal
    test_type("Decimal(5,3)", "Decimal(5,3)", result_type_name_nulleq);
    test_type("Decimal(5,3)", "Decimal(12,4)", result_type_name_nulleq);
    test_type("Decimal(5,3)", "Decimal(20,2)", result_type_name_nulleq);
    test_type("Decimal(5,3)", "Decimal(40,6)", result_type_name_nulleq);

    test_type("Decimal(12,4)", "Decimal(5,3)", result_type_name_nulleq);
    test_type("Decimal(12,4)", "Decimal(12,4)", result_type_name_nulleq);
    test_type("Decimal(12,4)", "Decimal(20,2)", result_type_name_nulleq);
    test_type("Decimal(12,4)", "Decimal(40,6)", result_type_name_nulleq);

    test_type("Decimal(20,2)", "Decimal(5,3)", result_type_name_nulleq);
    test_type("Decimal(20,2)", "Decimal(12,4)", result_type_name_nulleq);
    test_type("Decimal(20,2)", "Decimal(20,2)", result_type_name_nulleq);
    test_type("Decimal(20,2)", "Decimal(40,6)", result_type_name_nulleq);

    test_type("Decimal(40,6)", "Decimal(5,3)", result_type_name_nulleq);
    test_type("Decimal(40,6)", "Decimal(12,4)", result_type_name_nulleq);
    test_type("Decimal(40,6)", "Decimal(20,2)", result_type_name_nulleq);
    test_type("Decimal(40,6)", "Decimal(40,6)", result_type_name_nulleq);

    /// test type infer for time
    test_type("MyDate", "MyDate", result_type_name_nulleq);
    test_type("MyDate", "MyDateTime(0)", result_type_name_nulleq);
    test_type("MyDate", "MyDateTime(3)", result_type_name_nulleq);
    test_type("MyDate", "MyDateTime(6)", result_type_name_nulleq);

    test_type("MyDateTime(0)", "MyDate", result_type_name_nulleq);
    test_type("MyDateTime(0)", "MyDateTime(0)", result_type_name_nulleq);
    test_type("MyDateTime(0)", "MyDateTime(3)", result_type_name_nulleq);
    test_type("MyDateTime(0)", "MyDateTime(6)", result_type_name_nulleq);

    test_type("MyDateTime(3)", "MyDate", result_type_name_nulleq);
    test_type("MyDateTime(3)", "MyDateTime(0)", result_type_name_nulleq);
    test_type("MyDateTime(3)", "MyDateTime(3)", result_type_name_nulleq);
    test_type("MyDateTime(3)", "MyDateTime(6)", result_type_name_nulleq);

    test_type("MyDateTime(6)", "MyDate", result_type_name_nulleq);
    test_type("MyDateTime(6)", "MyDateTime(0)", result_type_name_nulleq);
    test_type("MyDateTime(6)", "MyDateTime(3)", result_type_name_nulleq);
    test_type("MyDateTime(6)", "MyDateTime(6)", result_type_name_nulleq);

    /// test type infer for Duration
    test_type("MyDuration(0)", "MyDuration(0)", result_type_name_nulleq);
    test_type("MyDuration(0)", "MyDuration(3)", result_type_name_nulleq);
    test_type("MyDuration(0)", "MyDuration(6)", result_type_name_nulleq);

    test_type("MyDuration(3)", "MyDuration(0)", result_type_name_nulleq);
    test_type("MyDuration(3)", "MyDuration(3)", result_type_name_nulleq);
    test_type("MyDuration(3)", "MyDuration(6)", result_type_name_nulleq);

    test_type("MyDuration(6)", "MyDuration(0)", result_type_name_nulleq);
    test_type("MyDuration(6)", "MyDuration(3)", result_type_name_nulleq);
    test_type("MyDuration(6)", "MyDuration(6)", result_type_name_nulleq);

    /// test nullable related
    test_type("Nullable(Int8)", "Nullable(Int8)", result_type_name_nulleq);
    test_type("Nullable(Int8)", "Int8", result_type_name_nulleq);
    test_type("Int8", "Nullable(Int8)", result_type_name_nulleq);

    test_type("Nullable(UInt8)", "Nullable(UInt8)", result_type_name_nulleq);
    test_type("Nullable(UInt8)", "UInt8", result_type_name_nulleq);
    test_type("UInt8", "Nullable(UInt8)", result_type_name_nulleq);

    test_type("Nullable(Int16)", "Nullable(Int16)", result_type_name_nulleq);
    test_type("Nullable(Int16)", "Int16", result_type_name_nulleq);
    test_type("Int16", "Nullable(Int16)", result_type_name_nulleq);

    test_type("Nullable(UInt16)", "Nullable(UInt16)", result_type_name_nulleq);
    test_type("Nullable(UInt16)", "UInt16", result_type_name_nulleq);
    test_type("UInt16", "Nullable(UInt16)", result_type_name_nulleq);

    test_type("Nullable(Int32)", "Nullable(Int32)", result_type_name_nulleq);
    test_type("Nullable(Int32)", "Int32", result_type_name_nulleq);
    test_type("Int32", "Nullable(Int32)", result_type_name_nulleq);

    test_type("Nullable(UInt32)", "Nullable(UInt32)", result_type_name_nulleq);
    test_type("Nullable(UInt32)", "UInt32", result_type_name_nulleq);
    test_type("UInt32", "Nullable(UInt32)", result_type_name_nulleq);

    test_type("Nullable(Int64)", "Nullable(Int64)", result_type_name_nulleq);
    test_type("Nullable(Int64)", "Int64", result_type_name_nulleq);
    test_type("Int64", "Nullable(Int64)", result_type_name_nulleq);

    test_type("Nullable(UInt64)", "Nullable(UInt64)", result_type_name_nulleq);
    test_type("Nullable(UInt64)", "UInt64", result_type_name_nulleq);
    test_type("UInt64", "Nullable(UInt64)", result_type_name_nulleq);

    test_type("Nullable(Float32)", "Nullable(Float32)", result_type_name_nulleq);
    test_type("Nullable(Float32)", "Float32", result_type_name_nulleq);
    test_type("Float32", "Nullable(Float32)", result_type_name_nulleq);

    test_type("Nullable(String)", "Nullable(String)", result_type_name_nulleq);
    test_type("Nullable(String)", "String", result_type_name_nulleq);
    test_type("String", "Nullable(String)", result_type_name_nulleq);

    test_type("Nullable(Decimal(5,3))", "Nullable(Decimal(5,3))", result_type_name_nulleq);
    test_type("Nullable(Decimal(5,3))", "Decimal(5,3)", result_type_name_nulleq);
    test_type("Decimal(5,3)", "Nullable(Decimal(5,3))", result_type_name_nulleq);

    test_type("Nullable(MyDate)", "Nullable(MyDate)", result_type_name_nulleq);
    test_type("Nullable(MyDate)", "MyDate", result_type_name_nulleq);
    test_type("MyDate", "Nullable(MyDate)", result_type_name_nulleq);

    test_type("Nullable(MyDateTime(0))", "Nullable(MyDateTime(0))", result_type_name_nulleq);
    test_type("Nullable(MyDateTime(0))", "MyDateTime(0)", result_type_name_nulleq);
    test_type("MyDateTime(0)", "Nullable(MyDateTime(0))", result_type_name_nulleq);

    test_type("Nullable(MyDuration(0))", "Nullable(MyDuration(0))", result_type_name_nulleq);
    test_type("Nullable(MyDuration(0))", "MyDuration(0)", result_type_name_nulleq);
    test_type("MyDuration(0)", "Nullable(MyDuration(0))", result_type_name_nulleq);
}
CATCH
} // namespace tests
} // namespace DB
