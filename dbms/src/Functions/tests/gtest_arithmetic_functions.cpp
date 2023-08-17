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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <Functions/divide.cpp>
#include <string>
#include <unordered_map>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class TestBinaryArithmeticFunctions : public DB::tests::FunctionTest
{
protected:
    template <typename T>
    using DataVector = std::vector<std::optional<T>>;

    using DecimalField32 = DecimalField<Decimal32>;
    using DecimalField64 = DecimalField<Decimal64>;
    using DecimalField128 = DecimalField<Decimal128>;
    using DecimalField256 = DecimalField<Decimal256>;

    template <typename T, ScaleType scale>
    struct GetValue
    {
        // only decimals have scale.
        static_assert(scale == 0);

        static constexpr T Max() { return std::numeric_limits<T>::max(); }
        static constexpr T Zero() { return 0; }
        static constexpr T One() { return 1; }
    };

    template <typename T, ScaleType scale>
    struct GetValue<Decimal<T>, scale>
    {
        using TDecimal = Decimal<T>;
        using ReturnType = DecimalField<TDecimal>;

        static constexpr PrecType MaxPrecision = maxDecimalPrecision<TDecimal>();
        static_assert(MaxPrecision > 0);

        static constexpr ReturnType Max()
        {
            TDecimal value(0);
            for (PrecType i = 0; i < MaxPrecision; ++i)
            {
                value *= TDecimal(10);
                value += TDecimal(9);
            }
            return ReturnType(value, scale);
        }

        static constexpr ReturnType Zero() { return ReturnType(TDecimal(0), scale); }

        static constexpr ReturnType One()
        {
            TDecimal value(1);
            for (ScaleType i = 0; i < scale; ++i)
                value *= TDecimal(10);
            return ReturnType(value, scale);
        }
    };

    void checkNullConstantResult(const ColumnWithTypeAndName & result, size_t size)
    {
        const IColumn * res_col = result.column.get();
        ASSERT_TRUE(size == res_col->size());
        Field res_field;
        for (size_t i = 0; i < size; i++)
        {
            res_col->get(i, res_field);
            ASSERT_TRUE(res_field.isNull());
        }
    }
};

template <typename TYPE>
void doTiDBDivideDecimalRoundInternalTest()
{
    auto apply = static_cast<TYPE (*)(TYPE, TYPE)>(&TiDBDivideFloatingImpl<TYPE, TYPE, false>::apply);

    constexpr TYPE max = std::numeric_limits<TYPE>::max();
    // note: Int256's min is not equal to -max-1
    // according to https://www.boost.org/doc/libs/1_60_0/libs/multiprecision/doc/html/boost_multiprecision/tut/ints/cpp_int.html
    constexpr TYPE min = std::numeric_limits<TYPE>::min();

    // clang-format off
    const std::vector<std::array<TYPE, 3>> cases = {
        {1, 2, 1}, {1, -2, -1}, {-1, 2, -1}, {-1, -2, 1},

        {0, 3, 0}, {0, -3, 0}, {0, 3, 0}, {0, -3, 0},
        {1, 3, 0}, {1, -3, 0}, {-1, 3, 0}, {-1, -3, 0},
        {2, 3, 1}, {2, -3, -1}, {-2, 3, -1}, {-2, -3, 1},
        {3, 3, 1}, {3, -3, -1}, {-3, 3, -1}, {-3, -3, 1},
        {4, 3, 1}, {4, -3, -1}, {-4, 3, -1}, {-4, -3, 1},
        {5, 3, 2}, {5, -3, -2}, {-5, 3, -2}, {-5, -3, 2},

        // ±max as divisor
        {0, max, 0}, {max/2-1, max, 0}, {max/2, max, 0}, {max/2+1, max, 1}, {max-1, max, 1}, {max, max, 1},
        {-1, max, 0}, {-max/2+1, max, 0},  {-max/2, max, 0}, {-max/2-1, max, -1}, {-max+1, max, -1}, {-max, max, -1}, {min, max, -1},
        {0, -max, 0}, {max/2-1, -max, 0}, {max/2, -max, 0}, {max/2+1, -max, -1}, {max-1, -max, -1}, {max, -max, -1},
        {-1, -max, 0}, {-max/2+1, -max, 0},  {-max/2, -max, 0}, {-max/2-1, -max, 1}, {-max+1, -max, 1}, {-max, -max, 1}, {min, -max, 1},

        // ±max as dividend
        {max, 1, max}, {max, 2, max/2+1}, {max, max/2-1, 2}, {max, max/2, 2}, {max, max/2+1, 2}, {max, max-1, 1},
        {max, -1, -max}, {max, -2, -max/2-1}, {max, -max/2+1, -2}, {max, -max/2, -2}, {max, -max/2-1, -2}, {max, -max+1, -1},
        {-max, 1, -max}, {-max, 2, -max/2-1}, {-max, max/2+1, -2}, {-max, max/2, -2}, {-max, max/2-1, -2}, {-max, max-1, -1},
        {-max, -1, max}, {-max, -2, max/2+1}, {-max, -max/2-1, 2}, {-max, -max/2, 2}, {-max, -max/2+1, 2}, {-max, -max+1, 1},
    };
    // clang-format on

    for (const auto & expect : cases)
    {
        std::array<TYPE, 3> actual = {expect[0], expect[1], apply(expect[0], expect[1])};
        ASSERT_EQ(expect, actual);
    }
}

TEST_F(TestBinaryArithmeticFunctions, TiDBDivideDecimalRoundInternal)
try
{
    doTiDBDivideDecimalRoundInternalTest<Int32>();
    doTiDBDivideDecimalRoundInternalTest<Int64>();
    doTiDBDivideDecimalRoundInternalTest<Int128>();
    doTiDBDivideDecimalRoundInternalTest<Int256>();
    doTiDBDivideDecimalRoundInternalTest<Int512>();
}
CATCH

TEST_F(TestBinaryArithmeticFunctions, TiDBDivideDecimalRound)
try
{
    const String func_name = "tidbDivide";

    // decimal32
    {
        // int and decimal
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal64>>(
                std::make_tuple(18, 4),
                {DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(0, 4)}),
            executeFunction(
                func_name,
                createColumn<Int32>({1, 1, 1, 1, 1}),
                createColumn<Decimal32>(
                    std::make_tuple(20, 4),
                    {DecimalField32(100000000, 4),
                     DecimalField32(100010000, 4),
                     DecimalField32(199990000, 4),
                     DecimalField32(200000000, 4),
                     DecimalField32(200010000, 4)})));

        // decimal and decimal
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(26, 8),
                {DecimalField128(10000, 8),
                 DecimalField128(9999, 8),
                 DecimalField128(5000, 8),
                 DecimalField128(5000, 8),
                 DecimalField128(5000, 8)}),
            executeFunction(
                func_name,
                createColumn<Decimal32>(
                    std::make_tuple(18, 4),
                    {DecimalField32(10000, 4),
                     DecimalField32(10000, 4),
                     DecimalField32(10000, 4),
                     DecimalField32(10000, 4),
                     DecimalField32(10000, 4)}),
                createColumn<Decimal32>(
                    std::make_tuple(18, 4),
                    {DecimalField32(100000000, 4),
                     DecimalField32(100010000, 4),
                     DecimalField32(199990000, 4),
                     DecimalField32(200000000, 4),
                     DecimalField32(200010000, 4)})));
    }

    // decimal64
    {
        // int and decimal
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal64>>(
                std::make_tuple(18, 4),
                {DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(0, 4)}),
            executeFunction(
                func_name,
                createColumn<Int32>({1, 1, 1, 1, 1}),
                createColumn<Decimal64>(
                    std::make_tuple(20, 4),
                    {DecimalField64(100000000, 4),
                     DecimalField64(100010000, 4),
                     DecimalField64(199990000, 4),
                     DecimalField64(200000000, 4),
                     DecimalField64(200010000, 4)})));

        // decimal and decimal
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(26, 8),
                {DecimalField128(10000, 8),
                 DecimalField128(9999, 8),
                 DecimalField128(5000, 8),
                 DecimalField128(5000, 8),
                 DecimalField128(5000, 8)}),
            executeFunction(
                func_name,
                createColumn<Decimal64>(
                    std::make_tuple(18, 4),
                    {DecimalField64(10000, 4),
                     DecimalField64(10000, 4),
                     DecimalField64(10000, 4),
                     DecimalField64(10000, 4),
                     DecimalField64(10000, 4)}),
                createColumn<Decimal64>(
                    std::make_tuple(18, 4),
                    {DecimalField64(100000000, 4),
                     DecimalField64(100010000, 4),
                     DecimalField64(199990000, 4),
                     DecimalField64(200000000, 4),
                     DecimalField64(200010000, 4)})));
    }

    // decimal128
    {
        // int and decimal
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal64>>(
                std::make_tuple(18, 4),
                {DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(0, 4)}),
            executeFunction(
                func_name,
                createColumn<Int32>({1, 1, 1, 1, 1}),
                createColumn<Decimal128>(
                    std::make_tuple(20, 4),
                    {DecimalField128(100000000, 4),
                     DecimalField128(100010000, 4),
                     DecimalField128(199990000, 4),
                     DecimalField128(200000000, 4),
                     DecimalField128(200010000, 4)})));

        // decimal and decimal
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(26, 8),
                {DecimalField128(10000, 8),
                 DecimalField128(9999, 8),
                 DecimalField128(5000, 8),
                 DecimalField128(5000, 8),
                 DecimalField128(5000, 8)}),
            executeFunction(
                func_name,
                createColumn<Decimal128>(
                    std::make_tuple(18, 4),
                    {DecimalField128(10000, 4),
                     DecimalField128(10000, 4),
                     DecimalField128(10000, 4),
                     DecimalField128(10000, 4),
                     DecimalField128(10000, 4)}),
                createColumn<Decimal128>(
                    std::make_tuple(18, 4),
                    {DecimalField128(100000000, 4),
                     DecimalField128(100010000, 4),
                     DecimalField128(199990000, 4),
                     DecimalField128(200000000, 4),
                     DecimalField128(200010000, 4)})));
    }

    // decimal256
    {
        // int and decimal
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal64>>(
                std::make_tuple(18, 4),
                {DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(1, 4),
                 DecimalField64(0, 4)}),
            executeFunction(
                func_name,
                createColumn<Int32>({1, 1, 1, 1, 1}),
                createColumn<Decimal256>(
                    std::make_tuple(20, 4),
                    {DecimalField256(Int256(100000000), 4),
                     DecimalField256(Int256(100010000), 4),
                     DecimalField256(Int256(199990000), 4),
                     DecimalField256(Int256(200000000), 4),
                     DecimalField256(Int256(200010000), 4)})));

        // decimal and decimal
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(26, 8),
                {DecimalField128(10000, 8),
                 DecimalField128(9999, 8),
                 DecimalField128(5000, 8),
                 DecimalField128(5000, 8),
                 DecimalField128(5000, 8)}),
            executeFunction(
                func_name,
                createColumn<Decimal256>(
                    std::make_tuple(18, 4),
                    {DecimalField256(Int256(10000), 4),
                     DecimalField256(Int256(10000), 4),
                     DecimalField256(Int256(10000), 4),
                     DecimalField256(Int256(10000), 4),
                     DecimalField256(Int256(10000), 4)}),
                createColumn<Decimal256>(
                    std::make_tuple(18, 4),
                    {DecimalField256(Int256(100000000), 4),
                     DecimalField256(Int256(100010000), 4),
                     DecimalField256(Int256(199990000), 4),
                     DecimalField256(Int256(200000000), 4),
                     DecimalField256(Int256(200010000), 4)})));
    }
}
CATCH

TEST_F(TestBinaryArithmeticFunctions, TiDBDivideDecimal)
try
{
    const String func_name = "tidbDivide";

    DataTypePtr decimal_type_1 = std::make_shared<DataTypeDecimal<Decimal128>>(20, 2);
    DataTypePtr decimal_type_2 = std::make_shared<DataTypeDecimal<Decimal128>>(20, 0);
    DataTypePtr nullable_decimal_type_1 = makeNullable(decimal_type_1);
    DataTypePtr nullable_decimal_type_2 = makeNullable(decimal_type_2);
    DataTypePtr null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());

    std::vector<DataTypePtr> col1_types;
    std::vector<DataTypePtr> col2_types;
    col1_types.push_back(decimal_type_1);
    col1_types.push_back(nullable_decimal_type_1);
    col1_types.push_back(null_type);
    col2_types.push_back(decimal_type_2);
    col2_types.push_back(nullable_decimal_type_2);
    col2_types.push_back(null_type);

    std::vector<Field> null_or_zero_field;
    null_or_zero_field.push_back(Null());
    null_or_zero_field.push_back(Field(DecimalField<Decimal128>(0, 0)));

    std::vector<Int64> values{10, 2, 20, 8, 10, 0, 30, 8, 16, 4};
    /// The precision of a non-zero DecimalField<Decimal128> must not less than minDecimalPrecision<Decimal128>
    /// the decimal_128_factor is used to make sure the precision is big enough
    Int128 decimal_128_factor = 10000000000;
    decimal_128_factor *= 1000000000;

    const size_t size = 10;

    std::vector<UInt8> col1_null_map{0, 1, 0, 1, 0, 1, 1, 0, 1, 0};
    ASSERT_TRUE(size == col1_null_map.size());

    std::vector<UInt8> col2_null_map{1, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    ASSERT_TRUE(size == col2_null_map.size());

    /// case 1, constant / constant
    /// 1.1 null / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (!col1_type->isNullable())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1 = col1_type->createColumnConst(size, Null());
                auto c2 = col2_type->createColumnConst(size, col2_value);
                auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
                auto result = executeFunction(func_name, {col1, col2});
                checkNullConstantResult(result, size);
            }
        }
    }

    /// 1.2 null / non_null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (!col1_type->isNullable())
                continue;
            if (col2_type->onlyNull())
                continue;
            auto c1 = nullable_decimal_type_1->createColumnConst(size, Null());
            auto c2 = col2_type->createColumnConst(size, Field(DecimalField<Decimal128>(2 * decimal_128_factor, 0)));
            auto col1 = ColumnWithTypeAndName(std::move(c1), nullable_decimal_type_1, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
            auto result = executeFunction(func_name, {col1, col2});
            checkNullConstantResult(result, size);
        }
    }

    /// 1.3 non_null / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (col1_type->onlyNull())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1
                    = col1_type->createColumnConst(size, Field(DecimalField<Decimal128>(100 * decimal_128_factor, 2)));
                auto c2 = col2_type->createColumnConst(size, col2_value);
                auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
                auto result = executeFunction(func_name, {col1, col2});
                checkNullConstantResult(result, size);
            }
        }
    }

    /// 1.4 non_null / non_null_or_zero
    DecimalField<Decimal128> ref(5, 0);
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Field(DecimalField<Decimal128>(1000 * decimal_128_factor, 2)));
            auto c2 = col2_type->createColumnConst(size, Field(DecimalField<Decimal128>(2 * decimal_128_factor, 0)));
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
            auto res_col = executeFunction(func_name, {col1, col2}, nullptr, false).column;
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                ASSERT_TRUE(!res_field.isNull());
                ASSERT_TRUE(res_field.safeGet<DecimalField<Decimal128>>() == ref);
            }
        }
    }
    /// case 2, vector / constant
    /// 2.1 vector / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (col1_type->onlyNull())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1_mutable = col1_type->createColumn();
                for (size_t i = 0; i < values.size(); i++)
                {
                    if (col1_type->isNullable() && col1_null_map[i])
                        c1_mutable->insert(Null());
                    else
                        c1_mutable->insert(Field(DecimalField<Decimal128>(values[i] * decimal_128_factor, 2)));
                }
                auto c2 = col2_type->createColumnConst(values.size(), col2_value);

                auto col1 = ColumnWithTypeAndName(std::move(c1_mutable), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

                auto result = executeFunction(func_name, {col1, col2});
                checkNullConstantResult(result, size);
            }
        }
    }
    /// 2.1 vector / non_null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1_mutable = col1_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col1_type->isNullable() && col1_null_map[i])
                    c1_mutable->insert(Null());
                else
                    c1_mutable->insert(Field(DecimalField<Decimal128>(values[i] * decimal_128_factor, 2)));
            }
            auto c2 = col2_type->createColumnConst(
                values.size(),
                Field(DecimalField<Decimal128>(2 * decimal_128_factor, 0)));

            auto col1 = ColumnWithTypeAndName(std::move(c1_mutable), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            auto res_col = executeFunction(func_name, {col1, col2}).column;
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                DecimalField<Decimal128> result(values[i] / 2, 2);
                res_col->get(i, res_field);
                if (col1_type->isNullable() && col1_null_map[i])
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<DecimalField<Decimal128>>() == result);
                }
            }
        }
    }
    /// 3 constant / vector
    /// 3.1 null / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (!col1_type->isNullable())
                continue;
            if (col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Null());
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field(DecimalField<Decimal128>(values[i] * decimal_128_factor, 2)));
            }
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            auto result = executeFunction(func_name, {col1, col2});
            checkNullConstantResult(result, size);
        }
    }
    /// 3.2 non_null / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            typename Decimal128::NativeType value = 1;
            for (size_t i = 0; i < values.size(); i++)
            {
                if (values[i] != 0)
                    value *= values[i];
            }
            auto c1
                = col1_type->createColumnConst(size, Field(DecimalField<Decimal128>(value * decimal_128_factor, 2)));
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field(DecimalField<Decimal128>(values[i] * decimal_128_factor, 0)));
            }
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            auto res_col = executeFunction(func_name, {col1, col2}).column;
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if ((col2_type->isNullable() && col2_null_map[i]) || values[i] == 0)
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    DecimalField<Decimal128> result(value / values[i], 2);
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<DecimalField<Decimal128>>() == result);
                }
            }
        }
    }
    /// case 4 vector / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumn();
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col1_type->isNullable() && col1_null_map[i])
                    c1->insert(Null());
                else
                    c1->insert(Field(DecimalField<Decimal128>(values[i] * decimal_128_factor, 2)));
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field(DecimalField<Decimal128>(values[i] * decimal_128_factor, 0)));
            }
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            auto res_col = executeFunction(func_name, {col1, col2}).column;
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if ((col1_type->isNullable() && col1_null_map[i]) || (col2_type->isNullable() && col2_null_map[i])
                    || values[i] == 0)
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    DecimalField<Decimal128> result(1, 2);
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<DecimalField<Decimal128>>() == result);
                }
            }
        }
    }
}
CATCH

TEST_F(TestBinaryArithmeticFunctions, TiDBDivideDouble)
try
{
    const String func_name = "tidbDivide";

    DataTypePtr double_type = std::make_shared<DataTypeFloat64>();
    DataTypePtr nullable_double_type = makeNullable(double_type);
    DataTypePtr null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());

    std::vector<DataTypePtr> col1_types;
    std::vector<DataTypePtr> col2_types;
    col1_types.push_back(double_type);
    col1_types.push_back(nullable_double_type);
    col1_types.push_back(null_type);
    col2_types.push_back(double_type);
    col2_types.push_back(nullable_double_type);
    col2_types.push_back(null_type);

    std::vector<Field> null_or_zero_field;
    null_or_zero_field.push_back(Null());
    null_or_zero_field.push_back(Field((Float64)0));


    std::vector<Int64> values{10, 2, 20, 8, 10, 0, 30, 8, 16, 4};

    const size_t size = 10;

    std::vector<UInt8> col1_null_map{0, 1, 0, 1, 0, 1, 1, 0, 1, 0};
    ASSERT_TRUE(size == col1_null_map.size());

    std::vector<UInt8> col2_null_map{1, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    ASSERT_TRUE(size == col2_null_map.size());

    /// case 1, constant / constant
    /// 1.1 null / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (!col1_type->isNullable())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1 = col1_type->createColumnConst(size, Null());
                auto c2 = col2_type->createColumnConst(size, col2_value);
                auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

                auto result = executeFunction(func_name, {col1, col2});
                checkNullConstantResult(result, size);
            }
        }
    }

    /// 1.2 null / non_null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (!col1_type->isNullable())
                continue;
            if (col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Null());
            auto c2 = col2_type->createColumnConst(size, Field((Float64)2));
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            auto result = executeFunction(func_name, {col1, col2});
            checkNullConstantResult(result, size);
        }
    }

    /// 1.3 non_null / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (col1_type->onlyNull())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1 = col1_type->createColumnConst(size, Field((Float64)1));
                auto c2 = col2_type->createColumnConst(size, col2_value);
                auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

                auto result = executeFunction(func_name, {col1, col2});
                checkNullConstantResult(result, size);
            }
        }
    }

    /// 1.4 non_null / non_null_or_zero
    Float64 ref = 5;
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Field((Float64)10));
            auto c2 = col2_type->createColumnConst(size, Field((Float64)2));
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
            auto res_col = executeFunction(func_name, {col1, col2}).column;
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                ASSERT_TRUE(!res_field.isNull());
                ASSERT_TRUE(res_field.safeGet<Float64>() == ref);
            }
        }
    }
    /// case 2, vector / constant
    /// 2.1 vector / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (col1_type->onlyNull())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1_mutable = col1_type->createColumn();
                for (size_t i = 0; i < values.size(); i++)
                {
                    if (col1_type->isNullable() && col1_null_map[i])
                        c1_mutable->insert(Null());
                    else
                        c1_mutable->insert(Field((Float64)values[i]));
                }
                auto c2 = col2_type->createColumnConst(values.size(), col2_value);

                auto col1 = ColumnWithTypeAndName(std::move(c1_mutable), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

                auto result = executeFunction(func_name, {col1, col2});
                checkNullConstantResult(result, values.size());
            }
        }
    }
    /// 2.1 vector / non_null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1_mutable = col1_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col1_type->isNullable() && col1_null_map[i])
                    c1_mutable->insert(Null());
                else
                    c1_mutable->insert(Field((Float64)values[i]));
            }
            auto c2 = col2_type->createColumnConst(values.size(), Field((Float64)2));

            auto col1 = ColumnWithTypeAndName(std::move(c1_mutable), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            auto res_col = executeFunction(func_name, {col1, col2}).column;
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if (col1_type->isNullable() && col1_null_map[i])
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<Float64>() == values[i] / 2.0);
                }
            }
        }
    }
    /// 3 constant / vector
    /// 3.1 null / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (!col1_type->isNullable())
                continue;
            if (col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Null());
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field((Float64)values[i]));
            }
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            auto result = executeFunction(func_name, {col1, col2});
            checkNullConstantResult(result, size);
        }
    }
    /// 3.2 non_null / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            Float64 value = 1;
            for (size_t i = 0; i < values.size(); i++)
            {
                if (values[i] != 0)
                    value *= values[i];
            }
            auto c1 = col1_type->createColumnConst(size, Field((Float64)value));
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field((Float64)values[i]));
            }
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            auto res_col = executeFunction(func_name, {col1, col2}).column;
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if ((col2_type->isNullable() && col2_null_map[i]) || values[i] == 0)
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<Float64>() == value / values[i]);
                }
            }
        }
    }
    /// case 4 vector / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumn();
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col1_type->isNullable() && col1_null_map[i])
                    c1->insert(Null());
                else
                    c1->insert(Field((Float64)values[i]));

                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field((Float64)values[i]));
            }
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            auto res_col = executeFunction(func_name, {col1, col2}).column;
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if ((col1_type->isNullable() && col1_null_map[i]) || (col2_type->isNullable() && col2_null_map[i])
                    || values[i] == 0)
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<Float64>() == 1);
                }
            }
        }
    }
}
CATCH

TEST_F(TestBinaryArithmeticFunctions, Modulo)
try
{
    const String func_name = "modulo";

    using uint64_limits = std::numeric_limits<UInt64>;
    using int64_limits = std::numeric_limits<Int64>;

    // "{}" is similar to std::nullopt.

    // integer modulo

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({2, 3, 1, {}, 0, {}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>({5, 3, uint64_limits::max(), 1, 0, 0, {}, 0, {}}),
            createColumn<Nullable<UInt64>>({3, 5, uint64_limits::max() - 1, 0, 1, 0, 0, {}, {}})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({2, 2, 1, 3, int64_limits::max(), {}, 0, {}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>(
                {5, 5, uint64_limits::max(), uint64_limits::max(), uint64_limits::max(), 1, 0, 0, {}, 0, {}}),
            createColumn<Nullable<Int64>>(
                {3, -3, int64_limits::max(), int64_limits::max() - 1, int64_limits::min(), 0, 1, 0, 0, {}, {}})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({2, -2, 466025954, -466025955, {}, 0, {}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({5, -5, int64_limits::max(), int64_limits::min(), 1, 0, 0, {}, 0, {}}),
            createColumn<Nullable<UInt64>>({3, 3, 998244353, 998244353, 0, 1, 0, 0, {}, {}})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({2, -2, 2, -2, int64_limits::max(), -1, {}, 0, {}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({5, -5, 5, -5, int64_limits::max(), int64_limits::min(), 1, 0, 0, {}, 0, {}}),
            createColumn<Nullable<Int64>>(
                {3, 3, -3, -3, int64_limits::min(), int64_limits::max(), 0, 1, 0, 0, {}, {}})));

    // decimal modulo

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(7, 3),
            {DecimalField32(700, 3),
             DecimalField32(-700, 3),
             DecimalField32(700, 3),
             DecimalField32(-700, 3),
             {},
             {},
             {},
             {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(3300, 3),
                 DecimalField32(-3300, 3),
                 DecimalField32(3300, 3),
                 DecimalField32(-3300, 3),
                 DecimalField32(1000, 3),
                 {},
                 DecimalField32(0, 3),
                 {}}),
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(1300, 3),
                 DecimalField32(1300, 3),
                 DecimalField32(-1300, 3),
                 DecimalField32(-1300, 3),
                 DecimalField32(0, 3),
                 DecimalField32(0, 3),
                 {},
                 {}})));

    // decimal overflow test.

    // for example, 999'999'999 % 1.0000'0000 can check whether Decimal32 is doing arithmetic on Int64.
    // scaling 999'999'999 (Decimal(9, 0)) to Decimal(9, 8) needs to multiply it with 1'0000'0000.
    // if it uses Int32, it will overflow and get wrong result (something like 0.69325056).

#define MODULO_OVERFLOW_TESTCASE(Decimal, precision)                                                                  \
    do                                                                                                                \
    {                                                                                                                 \
        using NativeType = Decimal::NativeType;                                                                       \
        using FieldType = DecimalField<Decimal>;                                                                      \
        auto prec = (precision);                                                                                      \
        auto max_scale = std::min(decimal_max_scale, static_cast<ScaleType>(prec) - 1);                               \
        auto exp10_x = static_cast<NativeType>(DecimalMaxValue::get(max_scale)) + 1; /* exp10_x: 10^x */              \
        auto decimal_max = exp10_x * 10 - 1;                                                                          \
        auto zero = static_cast<NativeType>(0); /* for Int256 */                                                      \
        ASSERT_COLUMN_EQ(                                                                                             \
            createColumn<Nullable<Decimal>>(std::make_tuple(prec, max_scale), {FieldType(zero, max_scale)}),          \
            executeFunction(                                                                                          \
                func_name,                                                                                            \
                createColumn<Nullable<Decimal>>(std::make_tuple(prec, 0), {FieldType(decimal_max, 0)}),               \
                createColumn<Nullable<Decimal>>(std::make_tuple(prec, max_scale), {FieldType(exp10_x, max_scale)}))); \
        ASSERT_COLUMN_EQ(                                                                                             \
            createColumn<Nullable<Decimal>>(std::make_tuple(prec, max_scale), {FieldType(exp10_x, max_scale)}),       \
            executeFunction(                                                                                          \
                func_name,                                                                                            \
                createColumn<Nullable<Decimal>>(std::make_tuple(prec, max_scale), {FieldType(exp10_x, max_scale)}),   \
                createColumn<Nullable<Decimal>>(std::make_tuple(prec, 0), {FieldType(decimal_max, 0)})));             \
    } while (false)

    MODULO_OVERFLOW_TESTCASE(Decimal32, 9);
    MODULO_OVERFLOW_TESTCASE(Decimal64, 18);
    MODULO_OVERFLOW_TESTCASE(Decimal128, 38);
    MODULO_OVERFLOW_TESTCASE(Decimal256, 65);

#undef MODULO_OVERFLOW_TESTCASE

    Int128 large_number_1 = static_cast<Int128>(std::numeric_limits<UInt64>::max()) * 100000;
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 5),
            {DecimalField128(large_number_1 % 100000, 5),
             DecimalField128(1, 5),
             DecimalField128(large_number_1 / 2 - 1, 5)}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(38, 5),
                {DecimalField128(large_number_1, 5),
                 DecimalField128(large_number_1, 5),
                 DecimalField128(large_number_1, 5)}),
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(38, 5),
                {DecimalField128(100000, 5),
                 DecimalField128(large_number_1 - 1, 5),
                 DecimalField128(large_number_1 / 2 + 1, 5)})));

    Int256 large_number_2 = static_cast<Int256>(large_number_1) * large_number_1;
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 5),
            {DecimalField256(large_number_2 % 100000, 5),
             DecimalField256(static_cast<Int256>(1), 5),
             DecimalField256(large_number_2 / 2 - 1, 5)}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal256>>(
                std::make_tuple(65, 5),
                {DecimalField256(large_number_2, 5),
                 DecimalField256(large_number_2, 5),
                 DecimalField256(large_number_2, 5)}),
            createColumn<Nullable<Decimal256>>(
                std::make_tuple(65, 5),
                {DecimalField256(static_cast<Int256>(100000), 5),
                 DecimalField256(large_number_2 - 1, 5),
                 DecimalField256(large_number_2 / 2 + 1, 5)})));

    // Int64 has a precision of 20, which is larger than Decimal64.
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(std::make_tuple(19, 3), {DecimalField128(300, 3), {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(3300, 3), DecimalField32(3300, 3), {}}),
            createColumn<Nullable<Int64>>({1, 0, {}})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(std::make_tuple(15, 5), {DecimalField64(450256, 5)}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(std::make_tuple(7, 5), {DecimalField32(3223456, 5)}),
            createColumn<Nullable<Decimal64>>(std::make_tuple(15, 3), {DecimalField64(9244, 3)})));

    // real modulo

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0.19999999999999996,
             -0.19999999999999996,
             0.19999999999999996,
             -0.19999999999999996,
             1.0999999999999996,
             -1.0999999999999996,
             1.0999999999999996,
             -1.0999999999999996,
             {},
             0.0,
             {},
             {},
             {},
             {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.3, -1.3, 1.3, -1.3, 3.3, -3.3, 3.3, -3.3, 12.34, 0.0, 0.0, 0.0, {}, {}}),
            createColumn<Nullable<Float64>>(
                {1.1, 1.1, -1.1, -1.1, 1.1, 1.1, -1.1, -1.1, 0.0, 12.34, 0.0, {}, 0.0, {}})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({0.55, {}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.55, 1.55, {}, 0.0, {}}),
            createColumn<Nullable<Int64>>({-1, 0, 0, {}, {}})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({0.25, {}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(1250, 3), DecimalField32(1250, 3), {}, DecimalField32(0, 3), {}}),
            createColumn<Nullable<Float64>>({1.0, 0.0, 0.0, {}, {}})));

    // const-vector modulo

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({{}, 0, 1, 0, 3, 3, 3}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(7, 3),
            createColumn<Nullable<Int64>>({0, 1, 2, 3, 4, 5, 6})));

    // vector-const modulo

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({{}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({0, 1, 2, 3}),
            createConstColumn<Nullable<Int64>>(4, 0)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, 1, 2, 0, 1, 2, 0}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({0, 1, 2, 3, 4, 5, 6}),
            createConstColumn<Nullable<Int64>>(7, 3)));

    // const-const modulo

    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, 2),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(1, 5),
            createConstColumn<Nullable<Int64>>(1, -3)));

    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, {}),
        executeFunction(func_name, createConstColumn<Nullable<Int64>>(1, 0), createConstColumn<Nullable<Int64>>(1, 0)));

    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, {}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(1, {}),
            createConstColumn<Nullable<Int64>>(1, 0)));

    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, {}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(1, 0),
            createConstColumn<Nullable<Int64>>(1, {})));

    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, {}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(1, {}),
            createConstColumn<Nullable<Int64>>(1, {})));
}
CATCH

TEST_F(TestBinaryArithmeticFunctions, ModuloExtra)
try
{
    std::unordered_map<String, DataTypePtr> data_type_map = {
        {"Int64", makeDataType<Nullable<Int64>>()},
        {"UInt64", makeDataType<Nullable<UInt64>>()},
        {"Float64", makeDataType<Nullable<Float64>>()},
        {"Decimal32", makeDataType<Nullable<Decimal32>>(9, 3)},
        {"Decimal64", makeDataType<Nullable<Decimal64>>(18, 6)},
        {"Decimal128", makeDataType<Nullable<Decimal128>>(38, 10)},
        {"Decimal256", makeDataType<Nullable<Decimal256>>(65, 20)},
    };

    auto makeResultDataType
        = [&](const String & typeName, size_t precision [[maybe_unused]], size_t scale [[maybe_unused]]) {
              if (typeName.find("Decimal") != String::npos)
                  return makeNullable(createDecimal(precision, scale));
              return data_type_map[typeName];
          };

#define MODULO_TESTCASE(Left, Right, Result, precision, left_scale, right_scale, result_scale) \
    do                                                                                         \
    {                                                                                          \
        auto prec = (precision);                                                               \
        auto result_data_type = makeResultDataType(#Result, prec, result_scale);               \
        auto left_data_type = data_type_map[#Left];                                            \
        auto right_data_type = data_type_map[#Right];                                          \
        ColumnWithTypeAndName expect_column{                                                   \
            makeColumn<Nullable<Result>>(                                                      \
                result_data_type,                                                              \
                InferredDataVector<Nullable<Result>>(                                          \
                    {{},                                                                       \
                     GetValue<Result, result_scale>::Zero(),                                   \
                     GetValue<Result, result_scale>::Zero(),                                   \
                     {},                                                                       \
                     {},                                                                       \
                     {},                                                                       \
                     {}})),                                                                    \
            result_data_type,                                                                  \
            "result"};                                                                         \
        ColumnWithTypeAndName left_column{                                                     \
            makeColumn<Nullable<Left>>(                                                        \
                left_data_type,                                                                \
                InferredDataVector<Nullable<Left>>(                                            \
                    {GetValue<Left, left_scale>::Max(),                                        \
                     GetValue<Left, left_scale>::Zero(),                                       \
                     GetValue<Left, left_scale>::One(),                                        \
                     GetValue<Left, left_scale>::Zero(),                                       \
                     {},                                                                       \
                     {},                                                                       \
                     {}})),                                                                    \
            left_data_type,                                                                    \
            "left"};                                                                           \
        ColumnWithTypeAndName right_column{                                                    \
            makeColumn<Nullable<Right>>(                                                       \
                right_data_type,                                                               \
                InferredDataVector<Nullable<Right>>(                                           \
                    {GetValue<Right, right_scale>::Zero(),                                     \
                     GetValue<Right, right_scale>::Max(),                                      \
                     GetValue<Right, right_scale>::One(),                                      \
                     {},                                                                       \
                     GetValue<Right, right_scale>::Zero(),                                     \
                     GetValue<Right, right_scale>::Max(),                                      \
                     {}})),                                                                    \
            right_data_type,                                                                   \
            "right"};                                                                          \
        ASSERT_COLUMN_EQ(expect_column, executeFunction("modulo", left_column, right_column)); \
    } while (false)

    MODULO_TESTCASE(Int64, Int64, Int64, 0, 0, 0, 0);
    MODULO_TESTCASE(Int64, UInt64, Int64, 0, 0, 0, 0);
    MODULO_TESTCASE(Int64, Float64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(Int64, Decimal32, Decimal128, 19, 0, 3, 3);
    MODULO_TESTCASE(Int64, Decimal64, Decimal128, 19, 0, 6, 6);
    MODULO_TESTCASE(Int64, Decimal128, Decimal128, 38, 0, 10, 10);
    MODULO_TESTCASE(Int64, Decimal256, Decimal256, 65, 0, 20, 20);

    MODULO_TESTCASE(UInt64, Int64, UInt64, 0, 0, 0, 0);
    MODULO_TESTCASE(UInt64, UInt64, UInt64, 0, 0, 0, 0);
    MODULO_TESTCASE(UInt64, Float64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(UInt64, Decimal32, Decimal128, 20, 0, 3, 3);
    MODULO_TESTCASE(UInt64, Decimal64, Decimal128, 20, 0, 6, 6);
    MODULO_TESTCASE(UInt64, Decimal128, Decimal128, 38, 0, 10, 10);
    MODULO_TESTCASE(UInt64, Decimal256, Decimal256, 65, 0, 20, 20);

    MODULO_TESTCASE(Float64, Int64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(Float64, UInt64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(Float64, Float64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(Float64, Decimal32, Float64, 0, 0, 3, 0);
    MODULO_TESTCASE(Float64, Decimal64, Float64, 0, 0, 6, 0);
    MODULO_TESTCASE(Float64, Decimal128, Float64, 0, 0, 10, 0);
    MODULO_TESTCASE(Float64, Decimal256, Float64, 0, 0, 20, 0);

    MODULO_TESTCASE(Decimal32, Int64, Decimal128, 19, 3, 0, 3);
    MODULO_TESTCASE(Decimal32, UInt64, Decimal128, 20, 3, 0, 3);
    MODULO_TESTCASE(Decimal32, Float64, Float64, 0, 3, 0, 0);
    MODULO_TESTCASE(Decimal32, Decimal32, Decimal32, 9, 3, 3, 3);
    MODULO_TESTCASE(Decimal32, Decimal64, Decimal64, 18, 3, 6, 6);
    MODULO_TESTCASE(Decimal32, Decimal128, Decimal128, 38, 3, 10, 10);
    MODULO_TESTCASE(Decimal32, Decimal256, Decimal256, 65, 3, 20, 20);

    MODULO_TESTCASE(Decimal64, Int64, Decimal128, 19, 6, 0, 6);
    MODULO_TESTCASE(Decimal64, UInt64, Decimal128, 20, 6, 0, 6);
    MODULO_TESTCASE(Decimal64, Float64, Float64, 0, 6, 0, 0);
    MODULO_TESTCASE(Decimal64, Decimal32, Decimal64, 18, 6, 3, 6);
    MODULO_TESTCASE(Decimal64, Decimal64, Decimal64, 18, 6, 6, 6);
    MODULO_TESTCASE(Decimal64, Decimal128, Decimal128, 38, 6, 10, 10);
    MODULO_TESTCASE(Decimal64, Decimal256, Decimal256, 65, 6, 20, 20);

    MODULO_TESTCASE(Decimal128, Int64, Decimal128, 38, 10, 0, 10);
    MODULO_TESTCASE(Decimal128, UInt64, Decimal128, 38, 10, 0, 10);
    MODULO_TESTCASE(Decimal128, Float64, Float64, 0, 10, 0, 0);
    MODULO_TESTCASE(Decimal128, Decimal32, Decimal128, 38, 10, 3, 10);
    MODULO_TESTCASE(Decimal128, Decimal64, Decimal128, 38, 10, 6, 10);
    MODULO_TESTCASE(Decimal128, Decimal128, Decimal128, 38, 10, 10, 10);
    MODULO_TESTCASE(Decimal128, Decimal256, Decimal256, 65, 10, 20, 20);

    MODULO_TESTCASE(Decimal256, Int64, Decimal256, 65, 20, 0, 20);
    MODULO_TESTCASE(Decimal256, UInt64, Decimal256, 65, 20, 0, 20);
    MODULO_TESTCASE(Decimal256, Float64, Float64, 0, 20, 0, 0);
    MODULO_TESTCASE(Decimal256, Decimal32, Decimal256, 65, 20, 3, 20);
    MODULO_TESTCASE(Decimal256, Decimal64, Decimal256, 65, 20, 6, 20);
    MODULO_TESTCASE(Decimal256, Decimal128, Decimal256, 65, 20, 10, 20);
    MODULO_TESTCASE(Decimal256, Decimal256, Decimal256, 65, 20, 20, 20);

#undef MODULO_TESTCASE
}
CATCH


} // namespace tests
} // namespace DB
