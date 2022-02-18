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
class TestIfNull : public DB::tests::FunctionTest
{
protected:
    ColumnWithTypeAndName executeIfNull(const ColumnWithTypeAndName & first_column, const ColumnWithTypeAndName & second_column)
    {
        auto is_null_column = executeFunction("isNull", first_column);
        auto not_null_column = executeFunction("assumeNotNull", first_column);
        return executeFunction("multiIf", is_null_column, second_column, not_null_column);
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
                    ASSERT_COLUMN_EQ(col_2.type->isNullable() ? expr_data_2_nullable_vector : expr_data_2_vector, executeIfNull(col_1, col_2));
                }
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
                if (col_2.type->isNullable())
                {
                    ASSERT_COLUMN_EQ(createNullableColumn<Int64>(vector_const_result, {0, 0, 0, 0, 0}), executeIfNull(col_1, col_2));
                }
                else
                {
                    ASSERT_COLUMN_EQ(createColumn<Int64>(vector_const_result), executeIfNull(col_1, col_2));
                }
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
                    ASSERT_COLUMN_EQ(createNullableColumn<Int64>(expr1_data, {0, 0, 0, 0, 0}), executeIfNull(col_1, col_2));
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
                    ASSERT_COLUMN_EQ(createNullableColumn<Int64>(vector_vector_result, {1, 0, 0, 0, 1}), executeIfNull(col_1, col_2));
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
        createColumn<Decimal32>(std::make_tuple(4, 2), decimal_data),
        createNullableColumn<Decimal32>(std::make_tuple(4, 2), decimal_data, null_map),
        createColumn<Decimal64>(std::make_tuple(12, 4), decimal_data),
        createNullableColumn<Decimal64>(std::make_tuple(12, 4), decimal_data, null_map),
        createColumn<Decimal128>(std::make_tuple(20, 3), decimal_data),
        createNullableColumn<Decimal128>(std::make_tuple(20, 3), decimal_data, null_map),
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
    InferredDataVector<MyDuration> duration_input{111111, 222222, 101010, 202020, 212121};
    /// case 6 test ifNullDuration
}
CATCH
} // namespace tests
} // namespace DB
