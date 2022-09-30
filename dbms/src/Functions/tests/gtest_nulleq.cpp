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
    ColumnWithTypeAndName executeNullEq(const ColumnWithTypeAndName & first_column, const ColumnWithTypeAndName & second_column)
    {
        return executeFunction("nullEq", first_column, second_column);
    }
    DataTypePtr getReturnTypeForNullEq(const DataTypePtr & type_1, const DataTypePtr & type_2)
    {
        ColumnsWithTypeAndName input_columns{
            {nullptr, type_1, ""},
            {nullptr, type_2, ""},
        };
        return getReturnTypeForFunction(context, "nullEq", input_columns);
    }
    template <class IntegerType>
    ColumnWithTypeAndName createIntegerColumnInternal(const std::vector<Int64> & signed_input, const std::vector<UInt64> unsigned_input, const std::vector<Int32> & null_map)
    {
        static_assert(std::is_integral_v<IntegerType>);
        InferredDataVector<IntegerType> data_vector;
        if constexpr (std::is_signed_v<IntegerType>)
        {
            for (auto v : signed_input)
                data_vector.push_back(static_cast<IntegerType>(v));
        }
        else
        {
            for (auto v : unsigned_input)
                data_vector.push_back(static_cast<IntegerType>(v));
        }
        return null_map.empty() ? createColumn<IntegerType>(data_vector) : createNullableColumn<IntegerType>(data_vector, null_map);
    }
    template <class FloatType>
    ColumnWithTypeAndName createFloatColumnInternal(const std::vector<Float64> & float_input, const std::vector<Int32> & null_map)
    {
        static_assert(std::is_floating_point_v<FloatType>);
        InferredDataVector<FloatType> data_vector;
        for (auto v : float_input)
            data_vector.push_back(static_cast<FloatType>(v));
        return null_map.empty() ? createColumn<FloatType>(data_vector) : createNullableColumn<FloatType>(data_vector, null_map);
    }
    void testNullEqFunction(const ColumnsWithTypeAndName & input_columns, const std::vector<Int32> & null_map)
    {
        auto col_result = createColumn<UInt8>({0, 1});
        for (const auto & col_1 : input_columns)
        {
            for (const auto & col_2 : input_columns)
            {
                const auto * const result_type_name_nulleq = "UInt8";
                auto result_type = DataTypeFactory::instance().get(result_type_name_nulleq);
                auto result_column = result_type->createColumn();
                for (size_t i = 0; i < col_1.column->size(); i++)
                {
                    /// NullEq logic implementation.
                    Field result;
                    if (col_1.type->isNullable() && null_map[i] && col_2.type->isNullable() && null_map[i])
                    {
                        /// Both Null.
                        col_result.column->get(1, result);
                        result_column->insert(result);
                    }
                    else if (col_1.type->isNullable() && null_map[i])
                    {
                        /// Only one side is Null
                        col_result.column->get(0, result);
                        result_column->insert(result);
                    }
                    else if (col_2.type->isNullable() && null_map[i])
                    {
                        /// Only one side is Null
                        col_result.column->get(0, result);
                        result_column->insert(result);
                    }
                    else
                    {
                        /// Both not Null. Make a comparison then.
                        Field col1_field;
                        col_1.column->get(i, col1_field);
                        Field col2_field;
                        col_2.column->get(i, col2_field);
                        bool equals = (col1_field == col2_field);
                        if (col1_field.toString().find("Decimal") != std::string::npos && col2_field.toString().find("Decimal") != std::string::npos)
                        {
                            /// Ugly Fix of Decimal.
                            auto decimal_string = [&](const Field & value) -> DB::String {
                                switch (value.getType())
                                {
                                case Field::Types::Which::Decimal32:
                                {
                                    auto v = safeGet<DecimalField<Decimal32>>(value);
                                    return v.toString();
                                }
                                case Field::Types::Which::Decimal64:
                                {
                                    auto v = safeGet<DecimalField<Decimal64>>(value);
                                    return v.toString();
                                }
                                case Field::Types::Which::Decimal128:
                                {
                                    auto v = safeGet<DecimalField<Decimal128>>(value);
                                    return v.toString();
                                }
                                case Field::Types::Which::Decimal256:
                                {
                                    auto v = safeGet<DecimalField<Decimal256>>(value);
                                    return v.toString();
                                }
                                default:
                                    throw Exception("Unsupported with data type.");
                                }
                            };

                            /// I know all the tested decimal have actually the scale of 2.
                            /// So they are just substring-relationship.
                            const auto decimal_string_col1 = decimal_string(col1_field);
                            const auto decimal_string_col2 = decimal_string(col2_field);
                            if (decimal_string_col1.size() > decimal_string_col2.size()) {
                                equals = (decimal_string_col1.find(decimal_string_col2) != std::string::npos);
                            }
                            else
                            {
                                equals = (decimal_string_col2.find(decimal_string_col1) != std::string::npos);
                            }
                        }
                        col_result.column->get(equals, result);
                        result_column->insert(result);
                    }
                }
                ColumnWithTypeAndName expected{std::move(result_column), result_type, ""};
                ASSERT_COLUMN_EQ(expected, executeNullEq(col_1, col_2));
            }
        }
    }
};

TEST_F(TestNullEq, TestInputType)
try
{
    std::vector<Int32> null_map{1, 0, 0, 0, 1};
    /// case 1 test NullEqInt
    std::vector<Int64> signed_ints{-2, -1, 0, 1, 2};
    std::vector<UInt64> unsigned_ints{0, 1, 2, 3, 4};
    ColumnsWithTypeAndName int_input{
        /// not null column
        createIntegerColumnInternal<UInt8>(signed_ints, unsigned_ints, {}),
        createIntegerColumnInternal<Int8>(signed_ints, unsigned_ints, {}),
        createIntegerColumnInternal<UInt16>(signed_ints, unsigned_ints, {}),
        createIntegerColumnInternal<Int16>(signed_ints, unsigned_ints, {}),
        createIntegerColumnInternal<UInt32>(signed_ints, unsigned_ints, {}),
        createIntegerColumnInternal<Int32>(signed_ints, unsigned_ints, {}),
        createIntegerColumnInternal<UInt64>(signed_ints, unsigned_ints, {}),
        createIntegerColumnInternal<Int64>(signed_ints, unsigned_ints, {}),
        /// nullable column
        createIntegerColumnInternal<UInt8>(signed_ints, unsigned_ints, null_map),
        createIntegerColumnInternal<Int8>(signed_ints, unsigned_ints, null_map),
        createIntegerColumnInternal<UInt16>(signed_ints, unsigned_ints, null_map),
        createIntegerColumnInternal<Int16>(signed_ints, unsigned_ints, null_map),
        createIntegerColumnInternal<UInt32>(signed_ints, unsigned_ints, null_map),
        createIntegerColumnInternal<Int32>(signed_ints, unsigned_ints, null_map),
        createIntegerColumnInternal<UInt64>(signed_ints, unsigned_ints, null_map),
        createIntegerColumnInternal<Int64>(signed_ints, unsigned_ints, null_map),
    };
    testNullEqFunction(int_input, null_map);
    /// case 2 test NullEqReal
    std::vector<Float64> float_data{-2, -1, 0, 1, 2};
    ColumnsWithTypeAndName float_input{
        createFloatColumnInternal<Float32>(float_data, {}),
        createFloatColumnInternal<Float64>(float_data, {}),
        createFloatColumnInternal<Float32>(float_data, null_map),
        createFloatColumnInternal<Float64>(float_data, null_map),
    };
    testNullEqFunction(float_input, null_map);
    /// case 3 test NullEqString
    std::vector<String> string_data{"abc", "bcd", "cde", "def", "efg"};
    ColumnsWithTypeAndName string_input{
        createColumn<String>(string_data),
        createNullableColumn<String>(string_data, null_map),
    };
    testNullEqFunction(string_input, null_map);
    /// case 4 test NullEqDecimal
        std::vector<String> decimal_data{"-12.34", "-12.12", "0.00", "12.12", "12.34"};
        ColumnsWithTypeAndName decimal_input{
            createColumn<Decimal32>(std::make_tuple(5, 3), decimal_data),
            createNullableColumn<Decimal32>(std::make_tuple(5, 3), decimal_data, null_map),
            createColumn<Decimal64>(std::make_tuple(12, 4), decimal_data),
            createNullableColumn<Decimal64>(std::make_tuple(12, 4), decimal_data, null_map),
            createColumn<Decimal128>(std::make_tuple(20, 2), decimal_data),
            createNullableColumn<Decimal128>(std::make_tuple(20, 2), decimal_data, null_map),
            createColumn<Decimal256>(std::make_tuple(40, 6), decimal_data),
            createNullableColumn<Decimal256>(std::make_tuple(40, 6), decimal_data, null_map),
        };
        testNullEqFunction(decimal_input, null_map);
    /// case 5 test NullEqTime
    InferredDataVector<MyDate> date_data{
        MyDate(0, 0, 0).toPackedUInt(),
        MyDate(1960, 1, 1).toPackedUInt(),
        MyDate(1999, 1, 1).toPackedUInt(),
        MyDate(2000, 1, 1).toPackedUInt(),
        MyDate(2400, 1, 1).toPackedUInt(),
    };
    InferredDataVector<MyDate> datetime_data_fsp_0{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1960, 1, 1, 11, 11, 11, 0).toPackedUInt(),
        MyDateTime(1999, 1, 1, 22, 22, 22, 0).toPackedUInt(),
        MyDateTime(2000, 1, 1, 1, 1, 1, 0).toPackedUInt(),
        MyDateTime(2400, 1, 1, 2, 2, 2, 0).toPackedUInt(),
    };
    InferredDataVector<MyDate> datetime_data_fsp_3{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1960, 1, 1, 11, 11, 11, 123000).toPackedUInt(),
        MyDateTime(1999, 1, 1, 22, 22, 22, 234000).toPackedUInt(),
        MyDateTime(2000, 1, 1, 1, 1, 1, 345000).toPackedUInt(),
        MyDateTime(2400, 1, 1, 2, 2, 2, 456000).toPackedUInt(),
    };
    InferredDataVector<MyDate> datetime_data_fsp_6{
        MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
        MyDateTime(1960, 1, 1, 11, 11, 11, 123456).toPackedUInt(),
        MyDateTime(1999, 1, 1, 22, 22, 22, 234561).toPackedUInt(),
        MyDateTime(2000, 1, 1, 1, 1, 1, 345612).toPackedUInt(),
        MyDateTime(2400, 1, 1, 2, 2, 2, 456123).toPackedUInt(),
    };
    ColumnsWithTypeAndName time_input{
        createColumn<MyDate>(date_data),
        createNullableColumn<MyDate>(date_data, null_map),
        createColumn<MyDateTime>(std::make_tuple(0), datetime_data_fsp_0),
        createNullableColumn<MyDateTime>(std::make_tuple(0), datetime_data_fsp_0, null_map),
        createColumn<MyDateTime>(std::make_tuple(3), datetime_data_fsp_3),
        createNullableColumn<MyDateTime>(std::make_tuple(3), datetime_data_fsp_3, null_map),
        createColumn<MyDateTime>(std::make_tuple(6), datetime_data_fsp_6),
        createNullableColumn<MyDateTime>(std::make_tuple(6), datetime_data_fsp_6, null_map),
    };
    testNullEqFunction(time_input, null_map);
    /// case 6 test NullEqDuration
    InferredDataVector<MyDuration> duration_data_fsp_0{
        MyDuration(-1, 10, 10, 10, 0, 0).nanoSecond(),
        MyDuration(-1, 11, 11, 11, 0, 0).nanoSecond(),
        MyDuration(1, 10, 10, 10, 0, 0).nanoSecond(),
        MyDuration(1, 11, 11, 11, 0, 0).nanoSecond(),
        MyDuration(1, 12, 12, 12, 0, 0).nanoSecond(),
    };
    InferredDataVector<MyDuration> duration_data_fsp_3{
        MyDuration(-1, 10, 10, 10, 123000, 3).nanoSecond(),
        MyDuration(-1, 11, 11, 11, 234000, 3).nanoSecond(),
        MyDuration(1, 10, 10, 10, 345000, 3).nanoSecond(),
        MyDuration(1, 11, 11, 11, 456000, 3).nanoSecond(),
        MyDuration(1, 12, 12, 12, 561000, 3).nanoSecond(),
    };
    InferredDataVector<MyDuration> duration_data_fsp_6{
        MyDuration(-1, 10, 10, 10, 123456, 6).nanoSecond(),
        MyDuration(-1, 11, 11, 11, 234561, 6).nanoSecond(),
        MyDuration(1, 10, 10, 10, 345612, 6).nanoSecond(),
        MyDuration(1, 11, 11, 11, 456123, 6).nanoSecond(),
        MyDuration(1, 12, 12, 12, 561234, 6).nanoSecond(),
    };
    ColumnsWithTypeAndName duration_input{
        createColumn<MyDuration>(std::make_tuple(0), duration_data_fsp_0),
        createNullableColumn<MyDuration>(std::make_tuple(0), duration_data_fsp_0, null_map),
        createColumn<MyDuration>(std::make_tuple(3), duration_data_fsp_3),
        createNullableColumn<MyDuration>(std::make_tuple(3), duration_data_fsp_3, null_map),
        createColumn<MyDuration>(std::make_tuple(6), duration_data_fsp_6),
        createNullableColumn<MyDuration>(std::make_tuple(6), duration_data_fsp_6, null_map),
    };
    testNullEqFunction(duration_input, null_map);
}
CATCH
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
    const auto * const result_type_name_nulleq = "UInt8";
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
