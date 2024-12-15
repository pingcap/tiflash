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

#include <Common/Exception.h>
#include <Common/MyDuration.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/convertFieldToType.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class TestIfNull : public DB::tests::FunctionTest
{
protected:
    ColumnWithTypeAndName executeIfNull(
        const ColumnWithTypeAndName & first_column,
        const ColumnWithTypeAndName & second_column)
    {
        return executeFunction("ifNull", first_column, second_column);
    }
    DataTypePtr getReturnTypeForIfNull(const DataTypePtr & type_1, const DataTypePtr & type_2)
    {
        ColumnsWithTypeAndName input_columns{
            {nullptr, type_1, ""},
            {nullptr, type_2, ""},
        };
        return getReturnTypeForFunction(*context, "ifNull", input_columns);
    }
    template <class IntegerType>
    ColumnWithTypeAndName createIntegerColumnInternal(
        const std::vector<Int64> & signed_input,
        const std::vector<UInt64> unsigned_input,
        const std::vector<Int32> & null_map)
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
        return null_map.empty() ? createColumn<IntegerType>(data_vector)
                                : createNullableColumn<IntegerType>(data_vector, null_map);
    }
    template <class FloatType>
    ColumnWithTypeAndName createFloatColumnInternal(
        const std::vector<Float64> & float_input,
        const std::vector<Int32> & null_map)
    {
        static_assert(std::is_floating_point_v<FloatType>);
        InferredDataVector<FloatType> data_vector;
        for (auto v : float_input)
            data_vector.push_back(static_cast<FloatType>(v));
        return null_map.empty() ? createColumn<FloatType>(data_vector)
                                : createNullableColumn<FloatType>(data_vector, null_map);
    }
    void testIfNull(const ColumnsWithTypeAndName & input_columns, const std::vector<Int32> & null_map)
    {
        for (const auto & col_1 : input_columns)
        {
            for (const auto & col_2 : input_columns)
            {
                auto result_type = getLeastSupertype({removeNullable(col_1.type), col_2.type});
                auto result_column = result_type->createColumn();
                for (size_t i = 0; i < col_1.column->size(); i++)
                {
                    Field result;
                    if (col_1.type->isNullable() && null_map[i])
                    {
                        col_2.column->get(i, result);
                    }
                    else
                    {
                        col_1.column->get(i, result);
                    }
                    result_column->insert(convertFieldToType(result, *result_type));
                }
                ColumnWithTypeAndName expected{std::move(result_column), result_type, ""};
                ASSERT_COLUMN_EQ(expected, executeIfNull(col_1, col_2));
            }
        }
    }
};

TEST_F(TestIfNull, TestInputPattern)
try
{
    InferredDataVector<Int64> expr1_data{1, 2, 3, 4, 5};
    InferredDataVector<Int64> expr2_data{5, 4, 3, 2, 1};
    std::vector<Int32> null_map{1, 0, 0, 0, 1};
    InferredDataVector<Int64> vector_vector_result{5, 2, 3, 4, 1};
    InferredDataVector<Int64> vector_const_result{5, 2, 3, 4, 5};

    ColumnsWithTypeAndName first_const_arguments = {
        /// constant
        createConstColumn<Int64>(5, expr1_data[0]),
        /// nullable(not null constant)
        createConstColumn<Nullable<Int64>>(5, expr1_data[0]),
        /// nullable(null constant)
        createConstColumn<Nullable<Int64>>(5, {}),
        /// onlyNull constant
        createConstColumn<Nullable<Null>>(5, {})};
    ColumnsWithTypeAndName second_const_arguments = {
        createConstColumn<Int64>(5, expr2_data[0]),
        createConstColumn<Nullable<Int64>>(5, expr2_data[0]),
        createConstColumn<Nullable<Int64>>(5, {}),
        createConstColumn<Nullable<Null>>(5, {}),
    };
    /// ifNull will call `multiIf`, which never returns constant column even all the input column is constant
    auto expr_data_1_vector = createColumn<Int64>({1, 1, 1, 1, 1});
    auto expr_data_1_nullable_vector = createNullableColumn<Int64>({1, 1, 1, 1, 1}, {0, 0, 0, 0, 0});
    auto expr_data_2_vector = createColumn<Int64>({5, 5, 5, 5, 5});
    auto expr_data_2_nullable_vector = createNullableColumn<Int64>({5, 5, 5, 5, 5}, {0, 0, 0, 0, 0});
    auto nullable_vector = createNullableColumn<Int64>({5, 5, 5, 5, 5}, {1, 1, 1, 1, 1});
    auto nothing_vector = createNullableColumn<Null>({Null(), Null(), Null(), Null(), Null()}, {1, 1, 1, 1, 1});
    /// case 1 all kinds of constant
    for (auto & col_1 : first_const_arguments)
    {
        for (auto & col_2 : second_const_arguments)
        {
            if (col_1.type->onlyNull() && col_2.type->onlyNull())
            {
                ASSERT_COLUMN_EQ(nothing_vector, executeIfNull(col_1, col_2));
            }
            else if (col_1.column->isNullAt(0))
            {
                if (col_2.column->isNullAt(0))
                {
                    ASSERT_COLUMN_EQ(nullable_vector, executeIfNull(col_1, col_2));
                }
                else
                {
                    ASSERT_COLUMN_EQ(expr_data_2_vector, executeIfNull(col_1, col_2));
                }
            }
            else
            {
                if (col_2.column->isNullAt(0))
                {
                    ASSERT_COLUMN_EQ(expr_data_1_nullable_vector, executeIfNull(col_1, col_2));
                }
                else
                {
                    ASSERT_COLUMN_EQ(expr_data_1_vector, executeIfNull(col_1, col_2));
                }
            }
        }
    }
    ColumnsWithTypeAndName first_vector_arguments{
        //createColumn<Int64>(expr1_data),
        createNullableColumn<Int64>(expr1_data, null_map),
    };
    ColumnsWithTypeAndName second_vector_arguments{
        createColumn<Int64>(expr2_data),
        createNullableColumn<Int64>(expr2_data, null_map),
    };
    /// case 2 func(vector/nullable<vector>, constant)
    for (const auto & col_1 : first_vector_arguments)
    {
        for (const auto & col_2 : second_const_arguments)
        {
            if (!col_1.type->isNullable() || col_2.column->isNullAt(0))
            {
                ASSERT_COLUMN_EQ(col_1, executeIfNull(col_1, col_2));
            }
            else
            {
                ASSERT_COLUMN_EQ(createColumn<Int64>(vector_const_result), executeIfNull(col_1, col_2));
            }
        }
    }
    /// case 3 func(constant, vector/nullable<vector>
    for (const auto & col_1 : first_const_arguments)
    {
        for (const auto & col_2 : second_vector_arguments)
        {
            if (col_1.column->isNullAt(0))
            {
                ASSERT_COLUMN_EQ(col_2, executeIfNull(col_1, col_2));
            }
            else
            {
                if (col_2.type->isNullable())
                {
                    ASSERT_COLUMN_EQ(expr_data_1_nullable_vector, executeIfNull(col_1, col_2));
                }
                else
                {
                    ASSERT_COLUMN_EQ(expr_data_1_vector, executeIfNull(col_1, col_2));
                }
            }
        }
    }
    /// case 4 func(vector/nullable<vector>, vector/nullable<vector>)
    for (const auto & col_1 : first_vector_arguments)
    {
        for (const auto & col_2 : second_vector_arguments)
        {
            if (!col_1.type->isNullable())
            {
                if (col_2.type->isNullable())
                {
                    ASSERT_COLUMN_EQ(
                        createNullableColumn<Int64>(expr1_data, {0, 0, 0, 0, 0}),
                        executeIfNull(col_1, col_2));
                }
                else
                {
                    ASSERT_COLUMN_EQ(createColumn<Int64>(expr1_data), executeIfNull(col_1, col_2));
                }
            }
            else
            {
                /// col_1 has null value
                if (col_2.type->isNullable())
                {
                    ASSERT_COLUMN_EQ(
                        createNullableColumn<Int64>(vector_vector_result, {1, 0, 0, 0, 1}),
                        executeIfNull(col_1, col_2));
                }
                else
                {
                    ASSERT_COLUMN_EQ(createColumn<Int64>(vector_vector_result), executeIfNull(col_1, col_2));
                }
            }
        }
    }
}
CATCH
TEST_F(TestIfNull, TestInputType)
try
{
    std::vector<Int32> null_map{1, 0, 0, 0, 1};
    /// case 1 test IfNullInt
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
    testIfNull(int_input, null_map);
    /// case 2 test ifNullReal
    std::vector<Float64> float_data{-2, -1, 0, 1, 2};
    ColumnsWithTypeAndName float_input{
        createFloatColumnInternal<Float32>(float_data, {}),
        createFloatColumnInternal<Float64>(float_data, {}),
        createFloatColumnInternal<Float32>(float_data, null_map),
        createFloatColumnInternal<Float64>(float_data, null_map),
    };
    testIfNull(float_input, null_map);
    /// case 3 test ifNullString
    std::vector<String> string_data{"abc", "bcd", "cde", "def", "efg"};
    ColumnsWithTypeAndName string_input{
        createColumn<String>(string_data),
        createNullableColumn<String>(string_data, null_map),
    };
    testIfNull(string_input, null_map);
    /// case 4 test ifNullDecimal
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
    testIfNull(decimal_input, null_map);
    /// case 5 test ifNullTime
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
    testIfNull(time_input, null_map);
    /// case 6 test ifNullDuration
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
    /// DataTypeMyDuration is not supported yet
    //testIfNull(duration_input, null_map);
}
CATCH
TEST_F(TestIfNull, TestTypeInfer)
try
{
    auto test_type
        = [&](const String & col_1_type_name, const String & col_2_type_name, const String & result_type_name) {
              auto result_type = DataTypeFactory::instance().get(result_type_name);
              auto col_1_type = DataTypeFactory::instance().get(col_1_type_name);
              auto col_2_type = DataTypeFactory::instance().get(col_2_type_name);
              ASSERT_TRUE(result_type->equals(*getReturnTypeForIfNull(col_1_type, col_2_type)));
          };
    /// test integer type Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64
    test_type("Int8", "Int8", "Int8");
    test_type("Int8", "Int16", "Int16");
    test_type("Int8", "Int32", "Int32");
    test_type("Int8", "Int64", "Int64");
    test_type("Int8", "UInt8", "Int16");
    test_type("Int8", "UInt16", "Int32");
    test_type("Int8", "UInt32", "Int64");
    test_type("Int8", "UInt64", "Decimal(20,0)");

    test_type("UInt8", "Int8", "Int16");
    test_type("UInt8", "Int16", "Int16");
    test_type("UInt8", "Int32", "Int32");
    test_type("UInt8", "Int64", "Int64");
    test_type("UInt8", "UInt8", "UInt8");
    test_type("UInt8", "UInt16", "UInt16");
    test_type("UInt8", "UInt32", "UInt32");
    test_type("UInt8", "UInt64", "UInt64");

    test_type("Int16", "Int8", "Int16");
    test_type("Int16", "Int16", "Int16");
    test_type("Int16", "Int32", "Int32");
    test_type("Int16", "Int64", "Int64");
    test_type("Int16", "UInt8", "Int16");
    test_type("Int16", "UInt16", "Int32");
    test_type("Int16", "UInt32", "Int64");
    test_type("Int16", "UInt64", "Decimal(20,0)");

    test_type("UInt16", "Int8", "Int32");
    test_type("UInt16", "Int16", "Int32");
    test_type("UInt16", "Int32", "Int32");
    test_type("UInt16", "Int64", "Int64");
    test_type("UInt16", "UInt8", "UInt16");
    test_type("UInt16", "UInt16", "UInt16");
    test_type("UInt16", "UInt32", "UInt32");
    test_type("UInt16", "UInt64", "UInt64");

    test_type("Int32", "Int8", "Int32");
    test_type("Int32", "Int16", "Int32");
    test_type("Int32", "Int32", "Int32");
    test_type("Int32", "Int64", "Int64");
    test_type("Int32", "UInt8", "Int32");
    test_type("Int32", "UInt16", "Int32");
    test_type("Int32", "UInt32", "Int64");
    test_type("Int32", "UInt64", "Decimal(20,0)");

    test_type("UInt32", "Int8", "Int64");
    test_type("UInt32", "Int16", "Int64");
    test_type("UInt32", "Int32", "Int64");
    test_type("UInt32", "Int64", "Int64");
    test_type("UInt32", "UInt8", "UInt32");
    test_type("UInt32", "UInt16", "UInt32");
    test_type("UInt32", "UInt32", "UInt32");
    test_type("UInt32", "UInt64", "UInt64");

    test_type("Int64", "Int8", "Int64");
    test_type("Int64", "Int16", "Int64");
    test_type("Int64", "Int32", "Int64");
    test_type("Int64", "Int64", "Int64");
    test_type("Int64", "UInt8", "Int64");
    test_type("Int64", "UInt16", "Int64");
    test_type("Int64", "UInt32", "Int64");
    test_type("Int64", "UInt64", "Decimal(20,0)");

    test_type("UInt64", "Int8", "Decimal(20,0)");
    test_type("UInt64", "Int16", "Decimal(20,0)");
    test_type("UInt64", "Int32", "Decimal(20,0)");
    test_type("UInt64", "Int64", "Decimal(20,0)");
    test_type("UInt64", "UInt8", "UInt64");
    test_type("UInt64", "UInt16", "UInt64");
    test_type("UInt64", "UInt32", "UInt64");
    test_type("UInt64", "UInt64", "UInt64");

    /// test type infer for real
    test_type("Float32", "Float32", "Float32");
    test_type("Float32", "Float64", "Float64");
    test_type("Float64", "Float32", "Float64");
    test_type("Float64", "Float64", "Float64");

    /// test type infer for string
    test_type(DataTypeString::getDefaultName(), DataTypeString::getDefaultName(), DataTypeString::getDefaultName());

    /// test type infer for decimal
    test_type("Decimal(5,3)", "Decimal(5,3)", "Decimal(5,3)");
    test_type("Decimal(5,3)", "Decimal(12,4)", "Decimal(12,4)");
    test_type("Decimal(5,3)", "Decimal(20,2)", "Decimal(21,3)");
    test_type("Decimal(5,3)", "Decimal(40,6)", "Decimal(40,6)");

    test_type("Decimal(12,4)", "Decimal(5,3)", "Decimal(12,4)");
    test_type("Decimal(12,4)", "Decimal(12,4)", "Decimal(12,4)");
    test_type("Decimal(12,4)", "Decimal(20,2)", "Decimal(22,4)");
    test_type("Decimal(12,4)", "Decimal(40,6)", "Decimal(40,6)");

    test_type("Decimal(20,2)", "Decimal(5,3)", "Decimal(21,3)");
    test_type("Decimal(20,2)", "Decimal(12,4)", "Decimal(22,4)");
    test_type("Decimal(20,2)", "Decimal(20,2)", "Decimal(20,2)");
    test_type("Decimal(20,2)", "Decimal(40,6)", "Decimal(40,6)");

    test_type("Decimal(40,6)", "Decimal(5,3)", "Decimal(40,6)");
    test_type("Decimal(40,6)", "Decimal(12,4)", "Decimal(40,6)");
    test_type("Decimal(40,6)", "Decimal(20,2)", "Decimal(40,6)");
    test_type("Decimal(40,6)", "Decimal(40,6)", "Decimal(40,6)");

    /// test type infer for time
    test_type("MyDate", "MyDate", "MyDate");
    test_type("MyDate", "MyDateTime(0)", "MyDateTime(0)");
    test_type("MyDate", "MyDateTime(3)", "MyDateTime(3)");
    test_type("MyDate", "MyDateTime(6)", "MyDateTime(6)");

    test_type("MyDateTime(0)", "MyDate", "MyDateTime(0)");
    test_type("MyDateTime(0)", "MyDateTime(0)", "MyDateTime(0)");
    test_type("MyDateTime(0)", "MyDateTime(3)", "MyDateTime(3)");
    test_type("MyDateTime(0)", "MyDateTime(6)", "MyDateTime(6)");

    test_type("MyDateTime(3)", "MyDate", "MyDateTime(3)");
    test_type("MyDateTime(3)", "MyDateTime(0)", "MyDateTime(3)");
    test_type("MyDateTime(3)", "MyDateTime(3)", "MyDateTime(3)");
    test_type("MyDateTime(3)", "MyDateTime(6)", "MyDateTime(6)");

    test_type("MyDateTime(6)", "MyDate", "MyDateTime(6)");
    test_type("MyDateTime(6)", "MyDateTime(0)", "MyDateTime(6)");
    test_type("MyDateTime(6)", "MyDateTime(3)", "MyDateTime(6)");
    test_type("MyDateTime(6)", "MyDateTime(6)", "MyDateTime(6)");

    /// test type infer for Duration
    test_type("MyDuration(0)", "MyDuration(0)", "MyDuration(0)");
    test_type("MyDuration(0)", "MyDuration(3)", "MyDuration(3)");
    test_type("MyDuration(0)", "MyDuration(6)", "MyDuration(6)");

    test_type("MyDuration(3)", "MyDuration(0)", "MyDuration(3)");
    test_type("MyDuration(3)", "MyDuration(3)", "MyDuration(3)");
    test_type("MyDuration(3)", "MyDuration(6)", "MyDuration(6)");

    test_type("MyDuration(6)", "MyDuration(0)", "MyDuration(6)");
    test_type("MyDuration(6)", "MyDuration(3)", "MyDuration(6)");
    test_type("MyDuration(6)", "MyDuration(6)", "MyDuration(6)");

    /// test nullable related
    test_type("Nullable(Int8)", "Nullable(Int8)", "Nullable(Int8)");
    test_type("Nullable(Int8)", "Int8", "Int8");
    /// the result type should be Int8, currently, we rewrite ifnull(c1, c2) => multiIf(isnull(c1), assumeNotNull(c1), c2)
    /// so the return type is Nullable(Int8)
    test_type("Int8", "Nullable(Int8)", "Nullable(Int8)");
    test_type("Int8", "Int8", "Int8");
}
CATCH
} // namespace tests
} // namespace DB
