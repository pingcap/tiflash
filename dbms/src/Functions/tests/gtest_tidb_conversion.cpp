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

#include <Columns/ColumnsNumber.h>
#include <Common/Logger.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <TestUtils/FunctionTestUtils.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <gtest/gtest.h>

namespace DB
{
template <typename T>
void writeFloatTextNoExp(T x, WriteBuffer & buf);
}

namespace DB::tests
{
namespace
{

template <typename T>
std::string formatFloat(const T x)
{
    std::string res;
    WriteBufferFromString buf(res);
    writeFloatTextNoExp(x, buf);
    res.resize(buf.count());
    return res;
}

String genFloatStr(std::string_view val, int zero_n)
{
    assert(zero_n > 0);

    String s;
    s.resize(val.size() + zero_n, '0');
    std::memcpy(s.data(), val.data(), val.size());
    return s;
}

String genFloatStr(int zero_n, std::string_view val)
{
    assert(zero_n > 0);

    String s;
    s.resize(val.size() + zero_n + 1, '0');
    s[1] = '.';
    std::memcpy(s.data() + zero_n + 1, val.data(), val.size());
    return s;
}

auto getDatetimeColumn(bool single_field = false)
{
    MyDateTime datetime(2021, 10, 26, 16, 8, 59, 0);
    MyDateTime datetime_frac(2021, 10, 26, 16, 8, 59, 123456);

    auto col_datetime = ColumnUInt64::create();
    col_datetime->insert(Field(datetime.toPackedUInt()));
    if (!single_field)
        col_datetime->insert(Field(datetime_frac.toPackedUInt()));
    return col_datetime;
}

auto createCastTypeConstColumn(String str)
{
    return createConstColumn<String>(1, str);
}

const std::string func_name = "tidb_cast";

const Int8 MAX_INT8 = std::numeric_limits<Int8>::max();
const Int8 MIN_INT8 = std::numeric_limits<Int8>::min();
const Int16 MAX_INT16 = std::numeric_limits<Int16>::max();
const Int16 MIN_INT16 = std::numeric_limits<Int16>::min();
const Int32 MAX_INT32 = std::numeric_limits<Int32>::max();
const Int32 MIN_INT32 = std::numeric_limits<Int32>::min();
const Int64 MAX_INT64 = std::numeric_limits<Int64>::max();
const Int64 MIN_INT64 = std::numeric_limits<Int64>::min();
const UInt8 MAX_UINT8 = std::numeric_limits<UInt8>::max();
const UInt16 MAX_UINT16 = std::numeric_limits<UInt16>::max();
const UInt32 MAX_UINT32 = std::numeric_limits<UInt32>::max();
const UInt64 MAX_UINT64 = std::numeric_limits<UInt64>::max();

const Float32 MAX_FLOAT32 = std::numeric_limits<Float32>::max();
const Float32 MIN_FLOAT32 = std::numeric_limits<Float32>::min();
const Float64 MAX_FLOAT64 = std::numeric_limits<Float64>::max();
const Float64 MIN_FLOAT64 = std::numeric_limits<Float64>::min();

class TestTidbConversion : public DB::tests::FunctionTest
{
public:
    template <typename Input, typename Output>
    void testNotOnlyNull(const Input & input, const Output & output)
    {
        static_assert(!IsDecimal<Output> && !std::is_same_v<Output, MyDateTime>);
        auto inner_test = [&](bool is_const) {
            ASSERT_COLUMN_EQ(
                is_const ? createConstColumn<Nullable<Output>>(1, output) : createColumn<Nullable<Output>>({output}),
                executeFunction(
                    func_name,
                    {is_const ? createConstColumn<Nullable<Input>>(1, input) : createColumn<Nullable<Input>>({input}),
                     createCastTypeConstColumn(fmt::format("Nullable({})", TypeName<Output>::get()))}));
        };
        inner_test(true);
        inner_test(false);
    }

    template <typename Input, typename Output>
    typename std::enable_if<IsDecimal<Output>, void>::type testNotOnlyNull(
        const Input & input,
        const DecimalField<Output> & output,
        const std::tuple<UInt32, UInt32> & meta)
    {
        auto inner_test = [&](bool is_const) {
            ASSERT_COLUMN_EQ(
                is_const ? createConstColumn<Nullable<Output>>(meta, 1, output)
                         : createColumn<Nullable<Output>>(meta, {output}),
                executeFunction(
                    func_name,
                    {is_const ? createConstColumn<Nullable<Input>>(1, input) : createColumn<Nullable<Input>>({input}),
                     createCastTypeConstColumn(
                         fmt::format("Nullable(Decimal({},{}))", std::get<0>(meta), std::get<1>(meta)))}));
        };
        inner_test(true);
        inner_test(false);
    }

    template <typename Input, typename Output>
    typename std::enable_if<std::is_same_v<Output, MyDateTime>, void>::type testNotOnlyNull(
        const DecimalField<Decimal64> & input,
        const MyDateTime & output,
        int fraction)
    {
        auto meta = std::make_tuple(19, input.getScale());
        auto inner_test = [&](bool is_const) {
            ASSERT_COLUMN_EQ(
                is_const ? createDateTimeColumnConst(1, output, fraction) : createDateTimeColumn({output}, fraction),
                executeFunction(
                    func_name,
                    {is_const ? createConstColumn<Nullable<Input>>(meta, 1, input)
                              : createColumn<Nullable<Input>>(meta, {input}),
                     createCastTypeConstColumn(fmt::format("Nullable(MyDateTime({}))", fraction))}));
        };
        inner_test(true);
        inner_test(false);
    }

    template <typename Input, typename Output>
    typename std::enable_if<std::is_same_v<Output, MyDateTime>, void>::type testNotOnlyNull(
        const Input & input,
        const MyDateTime & output,
        int fraction)
    {
        auto inner_test = [&](bool is_const) {
            ASSERT_COLUMN_EQ(
                is_const ? createDateTimeColumnConst(1, output, fraction) : createDateTimeColumn({output}, fraction),
                executeFunction(
                    func_name,
                    {is_const ? createConstColumn<Nullable<Input>>(1, input) : createColumn<Nullable<Input>>({input}),
                     createCastTypeConstColumn(fmt::format("Nullable(MyDateTime({}))", fraction))}));
        };
        inner_test(true);
        inner_test(false);
    }

    template <typename Input, typename Output>
    void testThrowException(const Input & input)
    {
        static_assert(!IsDecimal<Output> && !std::is_same_v<Output, MyDateTime>);
        auto inner_test = [&](bool is_const) {
            ASSERT_THROW(
                executeFunction(
                    func_name,
                    {is_const ? createConstColumn<Nullable<Input>>(1, input) : createColumn<Nullable<Input>>({input}),
                     createCastTypeConstColumn(fmt::format("Nullable({})", TypeName<Output>::get()))}),
                TiFlashException);
        };
        inner_test(true);
        inner_test(false);
    }

    template <typename Input, typename Output>
    typename std::enable_if<IsDecimal<Output>, void>::type testThrowException(
        const Input & input,
        const std::tuple<UInt32, UInt32> & meta)
    {
        auto inner_test = [&](bool is_const) {
            ASSERT_THROW(
                executeFunction(
                    func_name,
                    {is_const ? createConstColumn<Nullable<Input>>(1, input) : createColumn<Nullable<Input>>({input}),
                     createCastTypeConstColumn(
                         fmt::format("Nullable(Decimal({},{}))", std::get<0>(meta), std::get<1>(meta)))}),
                TiFlashException);
        };
        inner_test(true);
        inner_test(false);
    }

    template <typename Input, typename Output>
    typename std::enable_if<std::is_same_v<Output, MyDateTime>, void>::type testThrowException(
        const Input & input,
        int fraction)
    {
        auto inner_test = [&](bool is_const) {
            ASSERT_THROW(
                executeFunction(
                    func_name,
                    {is_const ? createConstColumn<Nullable<Input>>(1, input) : createColumn<Nullable<Input>>({input}),
                     createCastTypeConstColumn(fmt::format("Nullable(MyDateTime({}))", fraction))}),
                TiFlashException);
        };
        inner_test(true);
        inner_test(false);
    }

    template <typename Input, typename Output>
    typename std::enable_if<std::is_same_v<Output, MyDateTime>, void>::type testReturnNull(
        const Input & input,
        int fraction)
    {
        auto inner_test = [&](bool is_const) {
            ASSERT_COLUMN_EQ(
                is_const ? createDateTimeColumnConst(1, {}, fraction) : createDateTimeColumn({{}}, fraction),
                executeFunction(
                    func_name,
                    {is_const ? createConstColumn<Nullable<Input>>(1, input) : createColumn<Nullable<Input>>({input}),
                     createCastTypeConstColumn(fmt::format("Nullable(MyDateTime({}))", fraction))}));
        };
        inner_test(true);
        inner_test(false);
    }

    template <typename Input, typename Output>
    typename std::enable_if<std::is_same_v<Output, MyDateTime>, void>::type testReturnNull(
        const DecimalField<Input> & input,
        const std::tuple<UInt32, UInt32> & meta,
        int fraction)
    {
        auto inner_test = [&](bool is_const) {
            ASSERT_COLUMN_EQ(
                is_const ? createDateTimeColumnConst(1, {}, fraction) : createDateTimeColumn({{}}, fraction),
                executeFunction(
                    func_name,
                    {is_const ? createConstColumn<Nullable<Input>>(meta, 1, input)
                              : createColumn<Nullable<Input>>(meta, {input}),
                     createCastTypeConstColumn(fmt::format("Nullable(MyDateTime({}))", fraction))}));
        };
        inner_test(true);
        inner_test(false);
    }

    template <typename Input, typename Output>
    void testOnlyNull()
    {
        std::vector<ColumnWithTypeAndName> nulls
            = {createOnlyNullColumnConst(1),
               createOnlyNullColumn(1),
               createColumn<Nullable<Input>>({{}}),
               createConstColumn<Nullable<Input>>(1, {})};

        auto inner_test = [&](const ColumnWithTypeAndName & null_one) {
            if constexpr (IsDecimal<Output>)
            {
                auto precision = 0;
                if constexpr (std::is_same_v<Output, Decimal32>)
                {
                    precision = 9;
                }
                else if constexpr (std::is_same_v<Output, Decimal64>)
                {
                    precision = 18;
                }
                else if constexpr (std::is_same_v<Output, Decimal128>)
                {
                    precision = 38;
                }
                else
                {
                    static_assert(std::is_same_v<Output, Decimal256>);
                    precision = 65;
                }
                auto meta = std::make_tuple(precision, 0);
                auto res = null_one.column->isColumnConst()
                    ? createConstColumn<Nullable<Output>>(meta, 1, std::optional<DecimalField<Output>>{})
                    : createColumn<Nullable<Output>>(meta, {std::optional<DecimalField<Output>>{}});
                ASSERT_COLUMN_EQ(
                    res,
                    executeFunction(
                        func_name,
                        {null_one, createCastTypeConstColumn(fmt::format("Nullable(Decimal({},0))", precision))}));
            }
            else if constexpr (std::is_same_v<Output, MyDateTime>)
            {
                auto res = null_one.column->isColumnConst() ? createDateTimeColumnConst(1, {}, 6)
                                                            : createDateTimeColumn({{}}, 6);
                ASSERT_COLUMN_EQ(
                    res,
                    executeFunction(func_name, {null_one, createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
            }
            else
            {
                auto res = null_one.column->isColumnConst() ? createConstColumn<Nullable<Output>>(1, {})
                                                            : createColumn<Nullable<Output>>({{}});
                ASSERT_COLUMN_EQ(
                    res,
                    executeFunction(
                        func_name,
                        {null_one, createCastTypeConstColumn(fmt::format("Nullable({})", TypeName<Output>::get()))}));
            }
        };
        for (const auto & null_one : nulls)
        {
            inner_test(null_one);
        }
    }

    void parseAndTestDecimalToFloat64(String input_decimal, double ref)
    {
        ASSERT_TRUE(!input_decimal.empty());
        size_t prec = input_decimal.length();
        size_t scale = 0;
        auto pos = input_decimal.find('.');
        if (pos != std::string::npos)
        {
            ASSERT_TRUE(input_decimal.length() >= pos + 1);
            scale = input_decimal.length() - pos - 1;
            --prec;
        }
        bool negative = input_decimal.starts_with("-");
        if (negative)
        {
            ASSERT_TRUE(prec >= 1);
            --prec;
        }
        ASSERT_TRUE(prec >= scale);
        ASSERT_COLUMN_EQ_V2(
            createColumn<Float64>({ref}),
            executeFunction(
                func_name,
                {createColumn<Decimal128>(std::make_tuple(prec, scale), {input_decimal}),
                 createCastTypeConstColumn("Float64")}));
        ASSERT_COLUMN_EQ_V2(
            createColumn<Float64>({ref}),
            executeFunction(
                func_name,
                {createColumn<Decimal256>(std::make_tuple(prec, scale), {input_decimal}),
                 createCastTypeConstColumn("Float64")}));
    }
};

using DecimalField32 = DecimalField<Decimal32>;
using DecimalField64 = DecimalField<Decimal64>;
using DecimalField128 = DecimalField<Decimal128>;
using DecimalField256 = DecimalField<Decimal256>;

TEST_F(TestTidbConversion, castIntAsInt)
try
{
    /// null only cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({{}}),
        executeFunction(func_name, {createOnlyNullColumn(1), createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({{}}),
        executeFunction(func_name, {createOnlyNullColumn(1), createCastTypeConstColumn("Nullable(Int64)")}));

    /// const cases
    // uint8/16/32/64 -> uint64, no overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_UINT8),
        executeFunction(func_name, {createConstColumn<UInt8>(1, MAX_UINT8), createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_UINT16),
        executeFunction(func_name, {createConstColumn<UInt16>(1, MAX_UINT16), createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_UINT32),
        executeFunction(func_name, {createConstColumn<UInt32>(1, MAX_UINT32), createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_UINT64),
        executeFunction(func_name, {createConstColumn<UInt64>(1, MAX_UINT64), createCastTypeConstColumn("UInt64")}));
    // int8/16/32/64 -> uint64, no overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_INT8),
        executeFunction(func_name, {createConstColumn<Int8>(1, MAX_INT8), createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_INT16),
        executeFunction(func_name, {createConstColumn<Int16>(1, MAX_INT16), createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_INT32),
        executeFunction(func_name, {createConstColumn<Int32>(1, MAX_INT32), createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_INT64),
        executeFunction(func_name, {createConstColumn<Int64>(1, MAX_INT64), createCastTypeConstColumn("UInt64")}));
    // uint8/16/32 -> int64, no overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_UINT8),
        executeFunction(func_name, {createConstColumn<UInt8>(1, MAX_UINT8), createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_UINT16),
        executeFunction(func_name, {createConstColumn<UInt16>(1, MAX_UINT16), createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_UINT32),
        executeFunction(func_name, {createConstColumn<UInt32>(1, MAX_UINT32), createCastTypeConstColumn("Int64")}));
    //  uint64 -> int64, will overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, -1),
        executeFunction(func_name, {createConstColumn<UInt64>(1, MAX_UINT64), createCastTypeConstColumn("Int64")}));
    // int8/16/32/64 -> int64, no overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_INT8),
        executeFunction(func_name, {createConstColumn<Int8>(1, MAX_INT8), createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_INT16),
        executeFunction(func_name, {createConstColumn<Int16>(1, MAX_INT16), createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_INT32),
        executeFunction(func_name, {createConstColumn<Int32>(1, MAX_INT32), createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_INT64),
        executeFunction(func_name, {createConstColumn<Int64>(1, MAX_INT64), createCastTypeConstColumn("Int64")}));

    /// normal cases
    // uint8/16/32/64 -> uint64, no overflow
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, 1, MAX_UINT8, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt8>>({0, 1, MAX_UINT8, {}}), createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, 1, MAX_UINT16, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt16>>({0, 1, MAX_UINT16, {}}), createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, 1, MAX_UINT32, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({0, 1, MAX_UINT32, {}}), createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, 1, MAX_UINT64, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({0, 1, MAX_UINT64, {}}), createCastTypeConstColumn("Nullable(UInt64)")}));
    // int8/16/32/64 -> uint64, no overflow
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, MAX_INT8, MAX_UINT64, MAX_UINT64 - MAX_INT8, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({0, MAX_INT8, -1, MIN_INT8, {}}),
             createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, MAX_INT16, MAX_UINT64, MAX_UINT64 - MAX_INT16, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int16>>({0, MAX_INT16, -1, MIN_INT16, {}}),
             createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, MAX_INT32, MAX_UINT64, MAX_UINT64 - MAX_INT32, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({0, MAX_INT32, -1, MIN_INT32, {}}),
             createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, MAX_INT64, MAX_UINT64, MAX_UINT64 - MAX_INT64, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({0, MAX_INT64, -1, MIN_INT64, {}}),
             createCastTypeConstColumn("Nullable(UInt64)")}));
    // uint8/16/32 -> int64, no overflow
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT8, MAX_UINT8, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt8>>({0, MAX_INT8, MAX_UINT8, {}}),
             createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT16, MAX_UINT16, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt16>>({0, MAX_INT16, MAX_UINT16, {}}),
             createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT32, MAX_UINT32, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({0, MAX_INT32, MAX_UINT32, {}}),
             createCastTypeConstColumn("Nullable(Int64)")}));
    //  uint64 -> int64, overflow may happen
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT64, -1, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({0, MAX_INT64, MAX_UINT64, {}}),
             createCastTypeConstColumn("Nullable(Int64)")}));
    // int8/16/32/64 -> int64, no overflow
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT8, -1, MIN_INT8, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({0, MAX_INT8, -1, MIN_INT8, {}}),
             createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT16, -1, MIN_INT16, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int16>>({0, MAX_INT16, -1, MIN_INT16, {}}),
             createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT32, -1, MIN_INT32, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({0, MAX_INT32, -1, MIN_INT32, {}}),
             createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT64, -1, MIN_INT64, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({0, MAX_INT64, -1, MIN_INT64, {}}),
             createCastTypeConstColumn("Nullable(Int64)")}));
}
CATCH

TEST_F(TestTidbConversion, castIntAsReal)
try
{
    // uint64/int64 -> float64, may be not precise
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({1234567890.0, 123456789012345680.0, 0.0, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>(
                 {1234567890, // this is fine
                  123456789012345678, // but this cannot be represented precisely in the IEEE 754 64-bit float format
                  0,
                  {}}),
             createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({1234567890.0, 123456789012345680.0, 0.0, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>(
                 {1234567890, // this is fine
                  123456789012345678, // but this cannot be represented precisely in the IEEE 754 64-bit float format
                  0,
                  {}}),
             createCastTypeConstColumn("Nullable(Float64)")}));
    // uint32/16/8 and int32/16/8 -> float64, precise
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_UINT32, 0, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({MAX_UINT32, 0, {}}), createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_UINT16, 0, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt16>>({MAX_UINT16, 0, {}}), createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_UINT8, 0, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt8>>({MAX_UINT8, 0, {}}), createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_INT32, MIN_INT32, 0, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, 0, {}}),
             createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_INT16, MIN_INT16, 0, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, 0, {}}),
             createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_INT8, MIN_INT8, 0, {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, 0, {}}),
             createCastTypeConstColumn("Nullable(Float64)")}));
}
CATCH

TEST_F(TestTidbConversion, castIntAsString)
try
{
    /// null only cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}}),
        executeFunction(func_name, {createOnlyNullColumn(1), createCastTypeConstColumn("Nullable(String)")}));

    /// const cases
    // uint64/32/16/8 -> string
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "18446744073709551615"),
        executeFunction(func_name, {createConstColumn<UInt64>(1, MAX_UINT64), createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "4294967295"),
        executeFunction(func_name, {createConstColumn<UInt32>(1, MAX_UINT32), createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "65535"),
        executeFunction(func_name, {createConstColumn<UInt16>(1, MAX_UINT16), createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "255"),
        executeFunction(func_name, {createConstColumn<UInt8>(1, MAX_UINT8), createCastTypeConstColumn("String")}));
    // int64/32/16/8 -> string
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "9223372036854775807"),
        executeFunction(func_name, {createConstColumn<Int64>(1, MAX_INT64), createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "2147483647"),
        executeFunction(func_name, {createConstColumn<Int32>(1, MAX_INT32), createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "32767"),
        executeFunction(func_name, {createConstColumn<Int16>(1, MAX_INT16), createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "127"),
        executeFunction(func_name, {createConstColumn<Int8>(1, MAX_INT8), createCastTypeConstColumn("String")}));

    /// normal cases
    // uint64/32/16/8 -> string
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"18446744073709551615", "0", {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({MAX_UINT64, 0, {}}), createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"4294967295", "0", {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({MAX_UINT32, 0, {}}), createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"65535", "0", {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt16>>({MAX_UINT16, 0, {}}), createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"255", "0", {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt8>>({MAX_UINT8, 0, {}}), createCastTypeConstColumn("Nullable(String)")}));
    // int64/32/16/8 -> string
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"9223372036854775807", "-9223372036854775808", "0", {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({MAX_INT64, MIN_INT64, 0, {}}),
             createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"2147483647", "-2147483648", "0", {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, 0, {}}),
             createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"32767", "-32768", "0", {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, 0, {}}),
             createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"127", "-128", "0", {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, 0, {}}),
             createCastTypeConstColumn("Nullable(String)")}));
}
CATCH

TEST_F(TestTidbConversion, castIntAsDecimal)
try
{
    // int8 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(MAX_INT8, 0), DecimalField32(MIN_INT8, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
             createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(MAX_INT8, 0), DecimalField64(MIN_INT8, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
             createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_INT8, 0), DecimalField128(MIN_INT8, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
             createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT8), 0), DecimalField256(static_cast<Int256>(MIN_INT8), 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
             createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // int16 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(MAX_INT16, 0), DecimalField32(MIN_INT16, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, {}}),
             createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(MAX_INT16, 0), DecimalField64(MIN_INT16, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, {}}),
             createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_INT16, 0), DecimalField128(MIN_INT16, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, {}}),
             createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT16), 0),
             DecimalField256(static_cast<Int256>(MIN_INT16), 0),
             {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, {}}),
             createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // int32 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(999999999, 0), DecimalField32(-999999999, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({999999999, -999999999, {}}),
             createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_THROW(
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({1000000000, -1000000000, {}}),
             createCastTypeConstColumn("Nullable(Decimal(9,0))")}),
        TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(MAX_INT32, 0), DecimalField64(MIN_INT32, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, {}}),
             createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_INT32, 0), DecimalField128(MIN_INT32, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, {}}),
             createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT32), 0),
             DecimalField256(static_cast<Int256>(MIN_INT32), 0),
             {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, {}}),
             createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // int64 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(999999999, 0), DecimalField32(-999999999, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({999999999, -999999999, {}}),
             createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_THROW(
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({1000000000, -1000000000, {}}),
             createCastTypeConstColumn("Nullable(Decimal(9,0))")}),
        TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(999999999999999999, 0), DecimalField64(-999999999999999999, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({999999999999999999, -999999999999999999, {}}),
             createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_THROW(
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({1000000000000000000, -1000000000000000000, {}}),
             createCastTypeConstColumn("Nullable(Decimal(18,0))")}),
        TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_INT64, 0), DecimalField128(MIN_INT64, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({MAX_INT64, MIN_INT64, {}}),
             createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT64), 0),
             DecimalField256(static_cast<Int256>(MIN_INT64), 0),
             {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({MAX_INT64, MIN_INT64, {}}),
             createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // uint8 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(9, 0), {DecimalField32(MAX_UINT8, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt8>>({MAX_UINT8, {}}), createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(std::make_tuple(18, 0), {DecimalField64(MAX_UINT8, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt8>>({MAX_UINT8, {}}), createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(std::make_tuple(38, 0), {DecimalField128(MAX_UINT8, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt8>>({MAX_UINT8, {}}), createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_UINT8), 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt8>>({MAX_UINT8, {}}), createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // uint16 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(9, 0), {DecimalField32(MAX_UINT16, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt16>>({MAX_UINT16, {}}), createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(std::make_tuple(18, 0), {DecimalField64(MAX_UINT16, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt16>>({MAX_UINT16, {}}), createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(std::make_tuple(38, 0), {DecimalField128(MAX_UINT16, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt16>>({MAX_UINT16, {}}), createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_UINT16), 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt16>>({MAX_UINT16, {}}), createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // uint32 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(9, 0), {DecimalField32(999999999, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({999999999, {}}), createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_THROW(
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({1000000000, {}}), createCastTypeConstColumn("Nullable(Decimal(9,0))")}),
        TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(std::make_tuple(18, 0), {DecimalField64(MAX_UINT32, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({MAX_UINT32, {}}), createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(std::make_tuple(38, 0), {DecimalField128(MAX_UINT32, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({MAX_UINT32, {}}), createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_UINT32), 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({MAX_UINT32, {}}), createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // uint64 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(9, 0), {DecimalField32(999999999, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({999999999, {}}), createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_THROW(
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({1000000000, {}}), createCastTypeConstColumn("Nullable(Decimal(9,0))")}),
        TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(std::make_tuple(18, 0), {DecimalField64(999999999999999999, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({999999999999999999, {}}),
             createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_THROW(
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({1000000000000000000, {}}),
             createCastTypeConstColumn("Nullable(Decimal(18,0))")}),
        TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(std::make_tuple(38, 0), {DecimalField128(MAX_INT64, 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({MAX_INT64, {}}), createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT64), 0), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({MAX_INT64, {}}), createCastTypeConstColumn("Nullable(Decimal(65,0))")}));

    ASSERT_THROW(
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({9999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}),
        TiFlashException);

    ASSERT_THROW(
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({-9999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}),
        TiFlashException);

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(4, 1), {DecimalField32(static_cast<Int32>(9990), 1)}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(4, 1), {DecimalField32(static_cast<Int32>(-9990), 1)}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({-999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}));

    auto & dag_context = getDAGContext();
    UInt64 ori_flags = dag_context.getFlags();
    dag_context.addFlag(TiDBSQLFlags::OVERFLOW_AS_WARNING);
    dag_context.clearWarnings();

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(4, 1), {DecimalField32(static_cast<Int32>(9999), 1)}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({9999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(4, 1), {DecimalField32(static_cast<Int32>(-9999), 1)}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({-9999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(2, 2), {DecimalField32(static_cast<Int32>(99), 2)}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({9999}), createCastTypeConstColumn("Nullable(Decimal(2, 2))")}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(std::make_tuple(2, 2), {DecimalField32(static_cast<Int32>(-99), 2)}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int32>>({-9999}), createCastTypeConstColumn("Nullable(Decimal(2, 2))")}));

    dag_context.setFlags(ori_flags);
}
CATCH

TEST_F(TestTidbConversion, castIntAsTime)
try
{
    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{}, {{2021, 10, 26, 16, 8, 59, 0}}}, 6),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({{}, 20211026160859}),
             createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{}, {{2021, 10, 26, 16, 8, 59, 0}}}, 6),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({{}, 20211026160859}),
             createCastTypeConstColumn("Nullable(MyDateTime(6))")}));

    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{}}, 6),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt8>>({MAX_UINT8}), createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{}}, 6),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt16>>({MAX_UINT16}), createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{}}, 6),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt32>>({MAX_UINT32}), createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{}}, 6),
        executeFunction(
            func_name,
            {createColumn<Nullable<UInt64>>({0}), createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{}, {}}, 6),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({{}, -20211026160859}),
             createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
}
CATCH

TEST_F(TestTidbConversion, castRealAsInt)
try
{
    testOnlyNull<Float32, Int64>();
    testOnlyNull<Float32, UInt64>();
    testOnlyNull<Float64, Int64>();
    testOnlyNull<Float64, UInt64>();

    testNotOnlyNull<Float32, Int64>(0, 0);
    testThrowException<Float32, Int64>(MAX_FLOAT32);
    testNotOnlyNull<Float32, Int64>(MIN_FLOAT32, 0);
    testNotOnlyNull<Float32, Int64>(12.213f, 12);
    testNotOnlyNull<Float32, Int64>(-12.213f, -12);
    testNotOnlyNull<Float32, Int64>(12.513f, 13);
    testNotOnlyNull<Float32, Int64>(-12.513f, -13);

    testNotOnlyNull<Float32, UInt64>(0, 0);
    testThrowException<Float32, UInt64>(MAX_FLOAT32);
    testNotOnlyNull<Float32, UInt64>(MIN_FLOAT32, 0);
    testNotOnlyNull<Float32, UInt64>(12.213f, 12);
    testThrowException<Float32, UInt64>(-12.213f);
    testNotOnlyNull<Float32, UInt64>(12.513f, 13);
    testThrowException<Float32, UInt64>(-12.513f);

    testNotOnlyNull<Float64, Int64>(0, 0);
    testThrowException<Float64, Int64>(MAX_FLOAT64);
    testNotOnlyNull<Float64, Int64>(MIN_FLOAT64, 0);
    testNotOnlyNull<Float64, Int64>(12.213, 12);
    testNotOnlyNull<Float64, Int64>(-12.213, -12);
    testNotOnlyNull<Float64, Int64>(12.513, 13);
    testNotOnlyNull<Float64, Int64>(-12.513, -13);

    testNotOnlyNull<Float64, UInt64>(0, 0);
    testThrowException<Float64, UInt64>(MAX_FLOAT64);
    testNotOnlyNull<Float64, UInt64>(MIN_FLOAT64, 0);
    testNotOnlyNull<Float64, UInt64>(12.213, 12);
    testThrowException<Float64, UInt64>(-12.213);
    testNotOnlyNull<Float64, Int64>(12.513, 13);
    testNotOnlyNull<Float64, Int64>(-12.513, -13);
}
CATCH

TEST_F(TestTidbConversion, castRealAsReal)
try
{
    testOnlyNull<Float32, Float64>();
    testOnlyNull<Float64, Float64>();

    testNotOnlyNull<Float32, Float64>(0, 0);
    testNotOnlyNull<Float32, Float64>(12.213, 12.213000297546387);
    testNotOnlyNull<Float32, Float64>(-12.213, -12.213000297546387);
    testNotOnlyNull<Float32, Float64>(MIN_FLOAT32, MIN_FLOAT32);
    testNotOnlyNull<Float32, Float64>(MAX_FLOAT32, MAX_FLOAT32);

    testNotOnlyNull<Float64, Float64>(0, 0);
    testNotOnlyNull<Float64, Float64>(12.213, 12.213);
    testNotOnlyNull<Float64, Float64>(-12.213, -12.213);
    testNotOnlyNull<Float64, Float64>(MIN_FLOAT64, MIN_FLOAT64);
    testNotOnlyNull<Float64, Float64>(MAX_FLOAT64, MAX_FLOAT64);
}
CATCH

TEST_F(TestTidbConversion, castRealAsString)
try
{
    const String str_max_float32 = genFloatStr("34028235", 31);
    const String str_min_float32 = genFloatStr(38, "11754944");
    const String str_max_float64 = genFloatStr("17976931348623157", 292);
    const String str_min_float64 = genFloatStr(308, "22250738585072014");

    ASSERT_EQ(formatFloat(MAX_FLOAT32), str_max_float32);
    ASSERT_EQ(formatFloat(MIN_FLOAT32), str_min_float32);
    ASSERT_EQ(formatFloat(-MAX_FLOAT32), "-" + str_max_float32);
    ASSERT_EQ(formatFloat(-MIN_FLOAT32), "-" + str_min_float32);
    ASSERT_EQ(formatFloat(std::numeric_limits<Float32>::infinity()), "+Inf");
    ASSERT_EQ(formatFloat(-std::numeric_limits<Float32>::infinity()), "-Inf");
    ASSERT_EQ(formatFloat(-std::numeric_limits<Float32>::quiet_NaN()), "NaN");

    ASSERT_EQ(formatFloat(MAX_FLOAT64), str_max_float64);
    ASSERT_EQ(formatFloat(MIN_FLOAT64), str_min_float64);
    ASSERT_EQ(formatFloat(-MAX_FLOAT64), "-" + str_max_float64);
    ASSERT_EQ(formatFloat(-MIN_FLOAT64), "-" + str_min_float64);
    ASSERT_EQ(formatFloat(std::numeric_limits<Float64>::infinity()), "+Inf");
    ASSERT_EQ(formatFloat(-std::numeric_limits<Float64>::infinity()), "-Inf");
    ASSERT_EQ(formatFloat(-std::numeric_limits<Float64>::quiet_NaN()), "NaN");

    testOnlyNull<Float32, String>();
    testOnlyNull<Float64, String>();

    testNotOnlyNull<Float32, String>(0, "0");
    testNotOnlyNull<Float32, String>(12.213, "12.213");
    testNotOnlyNull<Float32, String>(-12.213, "-12.213");
    testNotOnlyNull<Float64, String>(0, "0");
    testNotOnlyNull<Float64, String>(12.213, "12.213");
    testNotOnlyNull<Float64, String>(-12.213, "-12.213");

    testNotOnlyNull<Float32, String>(MAX_FLOAT32, str_max_float32);
    testNotOnlyNull<Float32, String>(MIN_FLOAT32, str_min_float32);
    testNotOnlyNull<Float32, String>(-MAX_FLOAT32, "-" + str_max_float32);
    testNotOnlyNull<Float32, String>(-MIN_FLOAT32, "-" + str_min_float32);
    testNotOnlyNull<Float32, String>(std::numeric_limits<Float32>::infinity(), "+Inf");
    testNotOnlyNull<Float32, String>(-std::numeric_limits<Float32>::infinity(), "-Inf");
    testNotOnlyNull<Float32, String>(-std::numeric_limits<Float32>::quiet_NaN(), "NaN");

    testNotOnlyNull<Float64, String>(MAX_FLOAT64, str_max_float64);
    testNotOnlyNull<Float64, String>(MIN_FLOAT64, str_min_float64);
    testNotOnlyNull<Float64, String>(-MAX_FLOAT64, "-" + str_max_float64);
    testNotOnlyNull<Float64, String>(-MIN_FLOAT64, "-" + str_min_float64);
    testNotOnlyNull<Float64, String>(std::numeric_limits<Float64>::infinity(), "+Inf");
    testNotOnlyNull<Float64, String>(-std::numeric_limits<Float64>::infinity(), "-Inf");
    testNotOnlyNull<Float64, String>(-std::numeric_limits<Float64>::quiet_NaN(), "NaN");
}
CATCH

TEST_F(TestTidbConversion, castRealAsDecimal)
try
{
    testOnlyNull<Float32, Decimal32>();
    testOnlyNull<Float32, Decimal64>();
    testOnlyNull<Float32, Decimal128>();
    testOnlyNull<Float32, Decimal256>();
    testOnlyNull<Float64, Decimal32>();
    testOnlyNull<Float64, Decimal64>();
    testOnlyNull<Float64, Decimal128>();
    testOnlyNull<Float64, Decimal256>();

    // TODO fix:
    // for tidb, cast(12.213f as decimal(x, x)) throw warnings: Truncated incorrect DECIMAL value: '-12.21300029754638.
    // tiflash is same as mysql, don't throw warnings.

    testNotOnlyNull<Float32, Decimal32>(0, DecimalField32(0, 0), std::make_tuple(9, 0));
    testNotOnlyNull<Float32, Decimal32>(12.213f, DecimalField32(12213, 3), std::make_tuple(9, 3));
    testNotOnlyNull<Float32, Decimal32>(-12.213f, DecimalField32(-12213, 3), std::make_tuple(9, 3));
    testThrowException<Float32, Decimal32>(MAX_FLOAT32, std::make_tuple(9, 0));
    testNotOnlyNull<Float32, Decimal32>(MIN_FLOAT64, DecimalField32(0, 9), std::make_tuple(9, 9));

    testNotOnlyNull<Float32, Decimal64>(0, DecimalField64(0, 0), std::make_tuple(18, 0));
    testNotOnlyNull<Float32, Decimal64>(12.213f, DecimalField64(12213, 3), std::make_tuple(18, 3));
    testNotOnlyNull<Float32, Decimal64>(-12.213f, DecimalField64(-12213, 3), std::make_tuple(18, 3));
    testThrowException<Float32, Decimal64>(MAX_FLOAT32, std::make_tuple(18, 0));
    testNotOnlyNull<Float32, Decimal64>(MIN_FLOAT64, DecimalField64(0, 18), std::make_tuple(18, 18));

    testNotOnlyNull<Float32, Decimal128>(0, DecimalField128(0, 0), std::make_tuple(38, 0));
    testNotOnlyNull<Float32, Decimal128>(12.213f, DecimalField128(12213, 3), std::make_tuple(38, 3));
    testNotOnlyNull<Float32, Decimal128>(-12.213f, DecimalField128(-12213, 3), std::make_tuple(38, 3));
    testThrowException<Float32, Decimal128>(MAX_FLOAT32, std::make_tuple(38, 0));
    testNotOnlyNull<Float32, Decimal128>(MIN_FLOAT64, DecimalField128(0, 30), std::make_tuple(38, 30));

    testNotOnlyNull<Float32, Decimal256>(0, DecimalField256(static_cast<Int256>(0), 0), std::make_tuple(65, 0));
    testNotOnlyNull<Float32, Decimal256>(
        12.213f,
        DecimalField256(static_cast<Int256>(12213), 3),
        std::make_tuple(65, 3));
    testNotOnlyNull<Float32, Decimal256>(
        -12.213f,
        DecimalField256(static_cast<Int256>(-12213), 3),
        std::make_tuple(65, 3));
    // TODO add test after bug fixed
    // ERROR 1105 (HY000): other error for mpp stream: Cannot convert a non-finite number to an integer.
    // testNotOnlyNull<Float32, Decimal256>(MAX_FLOAT32, DecimalField256(Int256("340282346638528860000000000000000000000"), 0), std::make_tuple(65, 0));
    testNotOnlyNull<Float32, Decimal256>(
        MIN_FLOAT64,
        DecimalField256(static_cast<Int256>(0), 30),
        std::make_tuple(65, 30));

    testNotOnlyNull<Float64, Decimal32>(0, DecimalField32(0, 0), std::make_tuple(9, 0));
    testNotOnlyNull<Float64, Decimal32>(12.213, DecimalField32(12213, 3), std::make_tuple(9, 3));
    testNotOnlyNull<Float64, Decimal32>(-12.213, DecimalField32(-12213, 3), std::make_tuple(9, 3));
    testThrowException<Float64, Decimal32>(MAX_FLOAT64, std::make_tuple(9, 0));
    testNotOnlyNull<Float64, Decimal32>(MIN_FLOAT64, DecimalField32(0, 9), std::make_tuple(9, 9));

    testNotOnlyNull<Float64, Decimal64>(0, DecimalField64(0, 0), std::make_tuple(18, 0));
    testNotOnlyNull<Float64, Decimal64>(12.213, DecimalField64(12213, 3), std::make_tuple(18, 3));
    testNotOnlyNull<Float64, Decimal64>(-12.213, DecimalField64(-12213, 3), std::make_tuple(18, 3));
    testThrowException<Float64, Decimal64>(MAX_FLOAT64, std::make_tuple(18, 0));
    testNotOnlyNull<Float64, Decimal64>(MIN_FLOAT64, DecimalField64(0, 18), std::make_tuple(18, 18));

    testNotOnlyNull<Float64, Decimal128>(0, DecimalField128(0, 0), std::make_tuple(38, 0));
    testNotOnlyNull<Float64, Decimal128>(12.213, DecimalField128(12213, 3), std::make_tuple(38, 3));
    testNotOnlyNull<Float64, Decimal128>(-12.213, DecimalField128(-12213, 3), std::make_tuple(38, 3));
    testThrowException<Float64, Decimal128>(MAX_FLOAT64, std::make_tuple(38, 0));
    testNotOnlyNull<Float64, Decimal128>(MIN_FLOAT64, DecimalField128(0, 30), std::make_tuple(38, 30));

    testNotOnlyNull<Float64, Decimal256>(0, DecimalField256(static_cast<Int256>(0), 0), std::make_tuple(65, 0));
    testNotOnlyNull<Float64, Decimal256>(
        12.213,
        DecimalField256(static_cast<Int256>(12213), 3),
        std::make_tuple(65, 3));
    testNotOnlyNull<Float64, Decimal256>(
        -12.213,
        DecimalField256(static_cast<Int256>(-12213), 3),
        std::make_tuple(65, 3));
    testThrowException<Float64, Decimal256>(MAX_FLOAT64, std::make_tuple(65, 0));
    testNotOnlyNull<Float64, Decimal256>(
        MIN_FLOAT64,
        DecimalField256(static_cast<Int256>(0), 30),
        std::make_tuple(65, 30));


    // test round
    // TODO fix:
    // in default mode
    // for round test, tidb throw warnings: Truncated incorrect DECIMAL value: xxx
    // tiflash is same as mysql, don't throw warnings.
    auto & dag_context = getDAGContext();
    UInt64 ori_flags = dag_context.getFlags();
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    dag_context.clearWarnings();

    testNotOnlyNull<Float32, Decimal32>(12.213f, DecimalField32(1221, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float32, Decimal32>(-12.213f, DecimalField32(-1221, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float32, Decimal32>(12.215f, DecimalField32(1222, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float32, Decimal32>(-12.215f, DecimalField32(-1222, 2), std::make_tuple(9, 2));

    testNotOnlyNull<Float32, Decimal64>(12.213f, DecimalField64(1221, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float32, Decimal64>(-12.213f, DecimalField64(-1221, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float32, Decimal64>(12.215f, DecimalField64(1222, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float32, Decimal64>(-12.215f, DecimalField64(-1222, 2), std::make_tuple(18, 2));

    testNotOnlyNull<Float32, Decimal128>(12.213f, DecimalField128(1221, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float32, Decimal128>(-12.213f, DecimalField128(-1221, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float32, Decimal128>(12.215f, DecimalField128(1222, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float32, Decimal128>(-12.215f, DecimalField128(-1222, 2), std::make_tuple(38, 2));

    testNotOnlyNull<Float32, Decimal256>(
        12.213f,
        DecimalField256(static_cast<Int256>(1221), 2),
        std::make_tuple(65, 2));
    testNotOnlyNull<Float32, Decimal256>(
        -12.213f,
        DecimalField256(static_cast<Int256>(-1221), 2),
        std::make_tuple(65, 2));
    testNotOnlyNull<Float32, Decimal256>(
        12.215f,
        DecimalField256(static_cast<Int256>(1222), 2),
        std::make_tuple(65, 2));
    testNotOnlyNull<Float32, Decimal256>(
        -12.215f,
        DecimalField256(static_cast<Int256>(-1222), 2),
        std::make_tuple(65, 2));

    testNotOnlyNull<Float64, Decimal32>(12.213, DecimalField32(1221, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float64, Decimal32>(-12.213, DecimalField32(-1221, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float64, Decimal32>(12.215, DecimalField32(1222, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float64, Decimal32>(-12.215, DecimalField32(-1222, 2), std::make_tuple(9, 2));

    testNotOnlyNull<Float64, Decimal64>(12.213, DecimalField64(1221, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float64, Decimal64>(-12.213, DecimalField64(-1221, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float64, Decimal64>(12.215, DecimalField64(1222, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float64, Decimal64>(-12.215, DecimalField64(-1222, 2), std::make_tuple(18, 2));

    testNotOnlyNull<Float64, Decimal128>(12.213, DecimalField128(1221, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float64, Decimal128>(-12.213, DecimalField128(-1221, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float64, Decimal128>(12.215, DecimalField128(1222, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float64, Decimal128>(-12.215, DecimalField128(-1222, 2), std::make_tuple(38, 2));

    testNotOnlyNull<Float64, Decimal256>(12.213, DecimalField256(static_cast<Int256>(1221), 2), std::make_tuple(65, 2));
    testNotOnlyNull<Float64, Decimal256>(
        -12.213,
        DecimalField256(static_cast<Int256>(-1221), 2),
        std::make_tuple(65, 2));
    testNotOnlyNull<Float64, Decimal256>(12.215, DecimalField256(static_cast<Int256>(1222), 2), std::make_tuple(65, 2));
    testNotOnlyNull<Float64, Decimal256>(
        -12.215,
        DecimalField256(static_cast<Int256>(-1222), 2),
        std::make_tuple(65, 2));

    // Not compatible with MySQL/TiDB.
    // MySQL/TiDB: 34028199169636080000000000000000000000.00
    // TiFlash:    34028199169636079590747176440761942016.00
    testNotOnlyNull<Float32, Decimal256>(
        3.40282e+37f,
        DecimalField256(Decimal256(Int256("3402819916963607959074717644076194201600")), 2),
        std::make_tuple(50, 2));
    // MySQL/TiDB: 34028200000000000000000000000000000000.00
    // TiFlash:    34028200000000004441521809130870213181.44
    testNotOnlyNull<Float64, Decimal256>(
        3.40282e+37,
        DecimalField256(Decimal256(Int256("3402820000000000444152180913087021318144")), 2),
        std::make_tuple(50, 2));

    // MySQL/TiDB: 123.12345886230469000000
    // TiFlash:    123.12345886230470197248
    testNotOnlyNull<Float32, Decimal256>(
        123.123456789123456789f,
        DecimalField256(Decimal256(Int256("12312345886230470197248")), 20),
        std::make_tuple(50, 20));
    // MySQL/TiDB: 123.12345886230469000000
    // TiFlash:    123.12345678912344293376
    testNotOnlyNull<Float64, Decimal256>(
        123.123456789123456789,
        DecimalField256(Decimal256(Int256("12312345678912344293376")), 20),
        std::make_tuple(50, 20));

    dag_context.setFlags(ori_flags);
    dag_context.clearWarnings();
}
CATCH

TEST_F(TestTidbConversion, castRealAsTime)
try
{
    testOnlyNull<Float32, MyDateTime>();
    testOnlyNull<Float64, MyDateTime>();

    // TODO add tests after non-expected results fixed
    testReturnNull<Float32, MyDateTime>(12.213, 6);
    testReturnNull<Float32, MyDateTime>(-12.213, 6);
    testReturnNull<Float32, MyDateTime>(MAX_FLOAT32, 6);
    testReturnNull<Float32, MyDateTime>(MIN_FLOAT32, 6);

    testNotOnlyNull<Float32, MyDateTime>(0, {0, 0, 0, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float32, MyDateTime>(111, {2000, 1, 11, 0, 0, 0, 0}, 6);
    testReturnNull<Float32, MyDateTime>(-111, 6);
    testNotOnlyNull<Float32, MyDateTime>(111.1, {2000, 1, 11, 0, 0, 0, 0}, 6);

    testReturnNull<Float64, MyDateTime>(12.213, 6);
    testReturnNull<Float64, MyDateTime>(-12.213, 6);
    testReturnNull<Float64, MyDateTime>(MAX_FLOAT64, 6);
    testReturnNull<Float64, MyDateTime>(MIN_FLOAT64, 6);
    testReturnNull<Float64, MyDateTime>(1.1, 6);
    testReturnNull<Float64, MyDateTime>(48.1, 6);
    testReturnNull<Float64, MyDateTime>(100.1, 6);
    testReturnNull<Float64, MyDateTime>(1301.11, 6);
    testReturnNull<Float64, MyDateTime>(1131.111, 6);
    testReturnNull<Float64, MyDateTime>(100001111.111, 6);
    testReturnNull<Float64, MyDateTime>(20121212121260.1111111, 6);
    testReturnNull<Float64, MyDateTime>(20121212126012.1111111, 6);
    testReturnNull<Float64, MyDateTime>(20121212241212.1111111, 6);
    testNotOnlyNull<Float64, MyDateTime>(111, {2000, 1, 11, 0, 0, 0, 0}, 6);
    testReturnNull<Float64, MyDateTime>(-111, 6);

    testNotOnlyNull<Float64, MyDateTime>(0, {0, 0, 0, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(20210201, {2021, 2, 1, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(20210201.1, {2021, 2, 1, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(20210000.1, {2021, 0, 0, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(120012.1, {2012, 0, 12, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(121200.1, {2012, 12, 00, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(101.1, {2000, 1, 1, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(111.1, {2000, 1, 11, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(1122.1, {2000, 11, 22, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(31212.111, {2003, 12, 12, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(121212.1111, {2012, 12, 12, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(1121212.111111, {112, 12, 12, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(11121212.111111, {1112, 12, 12, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(99991111.1111111, {9999, 11, 11, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(1212121212.111111, {2000, 12, 12, 12, 12, 12, 111111}, 6);
}
CATCH

TEST_F(TestTidbConversion, castDecimalAsTime)
try
{
    testReturnNull<Decimal64, MyDateTime>(DecimalField64(11, 1), std::make_tuple(19, 1), 6);
    testReturnNull<Decimal64, MyDateTime>(DecimalField64(481, 1), std::make_tuple(19, 1), 6);
    testReturnNull<Decimal64, MyDateTime>(DecimalField64(1001, 1), std::make_tuple(19, 1), 6);
    testReturnNull<Decimal64, MyDateTime>(DecimalField64(130111, 2), std::make_tuple(19, 2), 6);
    testReturnNull<Decimal64, MyDateTime>(DecimalField64(1131111, 3), std::make_tuple(19, 3), 6);
    testReturnNull<Decimal64, MyDateTime>(DecimalField64(100001111111, 3), std::make_tuple(19, 3), 6);
    testReturnNull<Decimal64, MyDateTime>(DecimalField64(12121212126011111, 5), std::make_tuple(19, 6), 6);
    testReturnNull<Decimal64, MyDateTime>(DecimalField64(121212126012111111, 5), std::make_tuple(19, 4), 6);
    testReturnNull<Decimal64, MyDateTime>(DecimalField64(12121224121211111, 5), std::make_tuple(19, 4), 6);

    testNotOnlyNull<Decimal64, MyDateTime>(DecimalField64(1011, 1), {2000, 1, 1, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Decimal64, MyDateTime>(DecimalField64(1111, 1), {2000, 1, 11, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Decimal64, MyDateTime>(DecimalField64(11221, 1), {2000, 11, 22, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Decimal64, MyDateTime>(DecimalField64(31212111, 3), {2003, 12, 12, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Decimal64, MyDateTime>(DecimalField64(30000111, 3), {2003, 0, 0, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Decimal64, MyDateTime>(DecimalField64(1212121111, 4), {2012, 12, 12, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Decimal64, MyDateTime>(DecimalField64(1121212111111, 6), {112, 12, 12, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Decimal64, MyDateTime>(DecimalField64(11121212111111, 6), {1112, 12, 12, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Decimal64, MyDateTime>(DecimalField64(99991111111111, 6), {9999, 11, 11, 0, 0, 0, 0}, 6);
}
CATCH

TEST_F(TestTidbConversion, truncateInCastDecimalAsDecimal)
try
{
    DAGContext * dag_context = context->getDAGContext();
    UInt64 ori_flags = dag_context->getFlags();
    dag_context->addFlag(TiDBSQLFlags::IN_INSERT_STMT | TiDBSQLFlags::IN_UPDATE_OR_DELETE_STMT);
    dag_context->clearWarnings();

    ASSERT_COLUMN_EQ(
        createColumn<Decimal32>(
            std::make_tuple(5, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal32>(
                 std::make_tuple(5, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(5,2)")}));
    ASSERT_EQ(dag_context->getWarningCount(), 4);
    dag_context->setFlags(ori_flags);
    dag_context->clearWarnings();
}
CATCH

TEST_F(TestTidbConversion, castDecimalAsDecimalWithRound)
try
{
    DAGContext * dag_context = context->getDAGContext();
    UInt64 ori_flags = dag_context->getFlags();
    dag_context->addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    dag_context->clearWarnings();

    /// decimal32 to decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Decimal32>(
            std::make_tuple(5, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal32>(
                 std::make_tuple(5, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(5,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(
            std::make_tuple(15, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal32>(
                 std::make_tuple(5, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(15,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal128>(
            std::make_tuple(25, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal32>(
                 std::make_tuple(5, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(25,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal256>(
            std::make_tuple(45, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal32>(
                 std::make_tuple(5, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(45,2)")}));

    /// decimal64 to decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Decimal32>(
            std::make_tuple(5, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(
                 std::make_tuple(15, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(5,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(
            std::make_tuple(15, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(
                 std::make_tuple(15, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(15,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal128>(
            std::make_tuple(25, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(
                 std::make_tuple(15, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(25,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal256>(
            std::make_tuple(45, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(
                 std::make_tuple(15, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(45,2)")}));

    /// decimal128 to decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Decimal32>(
            std::make_tuple(5, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal128>(
                 std::make_tuple(25, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(5,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(
            std::make_tuple(15, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal128>(
                 std::make_tuple(25, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(15,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal128>(
            std::make_tuple(25, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal128>(
                 std::make_tuple(25, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(25,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal256>(
            std::make_tuple(45, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal128>(
                 std::make_tuple(25, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(45,2)")}));

    /// decimal256 to decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Decimal32>(
            std::make_tuple(5, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal256>(
                 std::make_tuple(45, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(5,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal64>(
            std::make_tuple(15, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal256>(
                 std::make_tuple(45, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(15,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal128>(
            std::make_tuple(25, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal256>(
                 std::make_tuple(45, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(25,2)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Decimal256>(
            std::make_tuple(45, 2),
            {"1.23", "1.56", "1.01", "1.00", "-1.23", "-1.56", "-1.01", "-1.00"}),
        executeFunction(
            func_name,
            {createColumn<Decimal256>(
                 std::make_tuple(45, 4),
                 {"1.2300", "1.5600", "1.0056", "1.0023", "-1.2300", "-1.5600", "-1.0056", "-1.0023"}),
             createCastTypeConstColumn("Decimal(45,2)")}));

    dag_context->setFlags(ori_flags);
    dag_context->clearWarnings();
}
CATCH

TEST_F(TestTidbConversion, castDecimalAsRealBasic)
try
{
    /// null only cases
    ASSERT_COLUMN_EQ_V2(
        createColumn<Nullable<Float64>>({{}}),
        executeFunction(func_name, {createOnlyNullColumn(1), createCastTypeConstColumn("Nullable(Float64)")}));

    /// ColumnVector(const nullable)
    ASSERT_COLUMN_EQ_V2(
        createConstColumn<Nullable<Float64>>(1, {3.0}),
        executeFunction(
            func_name,
            {createConstColumn<Nullable<Decimal64>>(std::make_tuple(22, 1), 1, {"3.0"}),
             createCastTypeConstColumn("Nullable(Float64)")}));

    ASSERT_COLUMN_EQ_V2(
        createConstColumn<Nullable<Float64>>(1, {}),
        executeFunction(
            func_name,
            {createConstColumn<Nullable<Decimal64>>(std::make_tuple(22, 1), 1, std::nullopt),
             createCastTypeConstColumn("Nullable(Float64)")}));

    /// ColumnVector(const non-nullable)
    ASSERT_COLUMN_EQ_V2(
        createConstColumn<Float64>(1, {1230.345}),
        executeFunction(
            func_name,
            {createConstColumn<Decimal64>(std::make_tuple(22, 3), 1, {"1230.345"}),
             createCastTypeConstColumn("Float64")}));

    /// ColumnVector(nullable)
    ASSERT_COLUMN_EQ_V2(
        createColumn<Nullable<Float64>>({345.002, std::nullopt, 1.02}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Decimal64>>(std::make_tuple(22, 3), {"345.002", std::nullopt, "1.02"}),
             createCastTypeConstColumn("Nullable(Float64)")}));

    /// ColumnVector(non-nullable)
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({3.14159, 1.0234567}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(std::make_tuple(22, 7), {"3.14159", "1.0234567"}),
             createCastTypeConstColumn("Float64")}));

    /// Negative
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({-0.14159, -1.0234567}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(std::make_tuple(22, 7), {"-0.14159", "-1.0234567"}),
             createCastTypeConstColumn("Float64")}));

    /// Decimal32
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({0.141, 1.02}),
        executeFunction(
            func_name,
            {createColumn<Decimal32>(std::make_tuple(8, 3), {"0.141", "1.02"}), createCastTypeConstColumn("Float64")}));

    /// Decimal64
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({0.14159, 1.0234567}),
        executeFunction(
            func_name,
            {createColumn<Decimal64>(std::make_tuple(22, 7), {"0.14159", "1.0234567"}),
             createCastTypeConstColumn("Float64")}));

    /// Decimal128
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({0.141593452, 19387.023456789}),
        executeFunction(
            func_name,
            {createColumn<Decimal128>(std::make_tuple(25, 9), {"0.141593452", "19387.023456789"}),
             createCastTypeConstColumn("Float64")}));

    /// Decimal256
    ASSERT_COLUMN_EQ_V2(
        createColumn<Float64>({0.141593452, 193857.02356789}),
        executeFunction(
            func_name,
            {createColumn<Decimal256>(std::make_tuple(32, 9), {"0.141593452", "193857.02356789"}),
             createCastTypeConstColumn("Float64")}));
}
CATCH

TEST_F(TestTidbConversion, castDecimalAsRealNumeric)
try
{
    std::vector<std::pair<String, double>> test_data = {
        {"9007199254740993.0", 9007199254740992.0},
        {"9007199254740994.0", 9007199254740994.0},
        {"9007199254740995.0", 9007199254740996.0},
        {"9007199254740996.0", 9007199254740996.0},
        {"12345", 12345},
        {"123.45", 123.45},
        {"-123.45", -123.45},
        {"0.00012345000098765", 0.00012345000098765},
        {"1234500009876.5", 1234500009876.5},
        {"-9223372036854775807", -9223372036854775807.0},
        {"-9223372036854775808", -9223372036854775808.0},
        {"18446744073709551615", 18446744073709551615.0},
        {"123456789.987654321", 123456789.987654321},
        {"1", 1},
        {"+1", 1},
        {"100000000000000000000000", 1e+23},
        {"123456700", 1.234567e+08},
        {"99999999999999974834176", 9.999999999999997e+22},
        {"100000000000000000000001", 1.0000000000000001e+23},
        {"100000000000000008388608", 1.0000000000000001e+23},
        {"100000000000000016777215", 1.0000000000000001e+23},
        {"100000000000000016777216", 1.0000000000000003e+23},
        {"-1", -1},
        {"-0.1", -0.1},
        {"0", 0},
        {"22.222222222222222", 22.22222222222222},
        {"3.12415900000000035241", 3.124159000000000130370},
        {"3.12415900000000035242", 3.124159000000000574460},
        {"3.124158999999999908325", 3.124158999999999686281},
        {"3.124158999999999908326", 3.124159000000000130370},
        {"1090544144181609348671888949248", 1.0905441441816093e+30},
        {"1090544144181609348835077142190", 1.0905441441816094e+30},
    };
    for (const auto & data : test_data)
    {
        parseAndTestDecimalToFloat64(data.first, data.second);
    }
}
CATCH

TEST_F(TestTidbConversion, castTimeAsReal)
try
{
    const auto data_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    const Float64 datetime_float = 20211026160859;
    const Float64 datetime_frac_float = 20211026160859.125;

    // cast datetime to float
    auto col_datetime1 = getDatetimeColumn();
    auto ctn_datetime1 = ColumnWithTypeAndName(std::move(col_datetime1), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({datetime_float, datetime_frac_float}),
        executeFunction(func_name, {ctn_datetime1, createCastTypeConstColumn("Float64")}));

    // cast datetime to nullable float
    auto col_datetime2 = getDatetimeColumn();
    auto ctn_datetime2 = ColumnWithTypeAndName(std::move(col_datetime2), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({datetime_float, datetime_frac_float}),
        executeFunction(func_name, {ctn_datetime2, createCastTypeConstColumn("Nullable(Float64)")}));

    // cast nullable datetime to nullable float
    auto col_datetime3 = getDatetimeColumn();
    auto datetime3_null_map = ColumnUInt8::create(2, 0);
    datetime3_null_map->getData()[1] = 1;
    auto col_datetime3_nullable = ColumnNullable::create(std::move(col_datetime3), std::move(datetime3_null_map));
    auto ctn_datetime3_nullable
        = ColumnWithTypeAndName(std::move(col_datetime3_nullable), makeNullable(data_type_ptr), "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({datetime_float, {}}),
        executeFunction(func_name, {ctn_datetime3_nullable, createCastTypeConstColumn("Nullable(Float64)")}));

    // cast const datetime to float
    auto col_datetime4_const = ColumnConst::create(getDatetimeColumn(true), 1);
    auto ctn_datetime4_const = ColumnWithTypeAndName(std::move(col_datetime4_const), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createConstColumn<Float64>(1, datetime_float),
        executeFunction(func_name, {ctn_datetime4_const, createCastTypeConstColumn("Float64")}));

    // cast nullable const datetime to float
    auto col_datetime5 = getDatetimeColumn(true);
    auto datetime5_null_map = ColumnUInt8::create(1, 0);
    auto col_datetime5_nullable = ColumnNullable::create(std::move(col_datetime5), std::move(datetime5_null_map));
    auto col_datetime5_nullable_const = ColumnConst::create(std::move(col_datetime5_nullable), 1);
    auto ctn_datetime5_nullable_const
        = ColumnWithTypeAndName(std::move(col_datetime5_nullable_const), makeNullable(data_type_ptr), "datetime");
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(1, datetime_float),
        executeFunction(func_name, {ctn_datetime5_nullable_const, createCastTypeConstColumn("Nullable(Float64)")}));
}
CATCH

TEST_F(TestTidbConversion, castDurationAsDuration)
try
{
    const auto from_type = std::make_shared<DataTypeMyDuration>(3);
    const auto to_type_1 = std::make_shared<DataTypeMyDuration>(5); // from_fsp <  to_fsp
    const auto to_type_2 = std::make_shared<DataTypeMyDuration>(3); // from_fsp == to_fsp
    const auto to_type_3 = std::make_shared<DataTypeMyDuration>(2); // from_fsp >  to_fsp

    ColumnWithTypeAndName input(
        createColumn<DataTypeMyDuration::FieldType>({(20 * 3600 + 20 * 60 + 20) * 1000000000L + 555000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 555000000L,
                                                     (20 * 3600 + 20 * 60 + 20) * 1000000000L + 554000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 554000000L,
                                                     (20 * 3600 + 20 * 60 + 20) * 1000000000L + 999000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 999000000L})
            .column,
        from_type,
        "input");

    ColumnWithTypeAndName output1(input.column, to_type_1, "output1");
    ColumnWithTypeAndName output2(input.column, to_type_2, "output2");
    ColumnWithTypeAndName output3(
        createColumn<DataTypeMyDuration::FieldType>({(20 * 3600 + 20 * 60 + 20) * 1000000000L + 560000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 560000000L,
                                                     (20 * 3600 + 20 * 60 + 20) * 1000000000L + 550000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 550000000L,
                                                     (20 * 3600 + 20 * 60 + 21) * 1000000000L + 000000000L,
                                                     -(20 * 3600 + 20 * 60 + 21) * 1000000000L - 000000000L})
            .column,
        to_type_3,
        "output3");

    ASSERT_COLUMN_EQ(output1, executeFunction(func_name, {input, createCastTypeConstColumn(to_type_1->getName())}));
    ASSERT_COLUMN_EQ(output2, executeFunction(func_name, {input, createCastTypeConstColumn(to_type_2->getName())}));
    ASSERT_COLUMN_EQ(output3, executeFunction(func_name, {input, createCastTypeConstColumn(to_type_3->getName())}));

    // Test Nullable
    ColumnWithTypeAndName input_nullable(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(20 * 3600 + 20 * 60 + 20) * 1000000000L + 555000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 555000000L,
                                                               {},
                                                               (20 * 3600 + 20 * 60 + 20) * 1000000000L + 554000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 554000000L,
                                                               {},
                                                               (20 * 3600 + 20 * 60 + 20) * 1000000000L + 999000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 999000000L})
            .column,
        makeNullable(input.type),
        "input_nullable");
    ColumnWithTypeAndName output1_nullable(input_nullable.column, makeNullable(to_type_1), "output1_nullable");
    ColumnWithTypeAndName output2_nullable(input_nullable.column, makeNullable(to_type_2), "output2_nullable");
    ColumnWithTypeAndName output3_nullable(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(20 * 3600 + 20 * 60 + 20) * 1000000000L + 560000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 560000000L,
                                                               {},
                                                               (20 * 3600 + 20 * 60 + 20) * 1000000000L + 550000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 550000000L,
                                                               {},
                                                               (20 * 3600 + 20 * 60 + 21) * 1000000000L + 000000000L,
                                                               -(20 * 3600 + 20 * 60 + 21) * 1000000000L - 000000000L})
            .column,
        makeNullable(to_type_3),
        "output3_output");

    ASSERT_COLUMN_EQ(
        output1_nullable,
        executeFunction(func_name, {input_nullable, createCastTypeConstColumn(makeNullable(to_type_1)->getName())}));
    ASSERT_COLUMN_EQ(
        output2_nullable,
        executeFunction(func_name, {input_nullable, createCastTypeConstColumn(makeNullable(to_type_2)->getName())}));
    ASSERT_COLUMN_EQ(
        output3_nullable,
        executeFunction(func_name, {input_nullable, createCastTypeConstColumn(makeNullable(to_type_3)->getName())}));

    // Test Const
    ColumnWithTypeAndName input_const(
        createConstColumn<DataTypeMyDuration::FieldType>(1, (20 * 3600 + 20 * 60 + 20) * 1000000000L + 999000000L)
            .column,
        from_type,
        "input_const");
    ColumnWithTypeAndName output1_const(input_const.column, to_type_1, "output1_const");
    ColumnWithTypeAndName output2_const(input_const.column, to_type_2, "output2_const");
    ColumnWithTypeAndName output3_const(
        createConstColumn<DataTypeMyDuration::FieldType>(1, (20 * 3600 + 20 * 60 + 21) * 1000000000L + 000000000L)
            .column,
        to_type_3,
        "output3_const");

    ASSERT_COLUMN_EQ(
        output1_const,
        executeFunction(func_name, {input_const, createCastTypeConstColumn(to_type_1->getName())}));
    ASSERT_COLUMN_EQ(
        output2_const,
        executeFunction(func_name, {input_const, createCastTypeConstColumn(to_type_2->getName())}));
    ASSERT_COLUMN_EQ(
        output3_const,
        executeFunction(func_name, {input_const, createCastTypeConstColumn(to_type_3->getName())}));
}
CATCH

TEST_F(TestTidbConversion, StrToDateTypeTest)
try
{
    // Arg1 is ColumnVector, Arg2 is ColumnVector
    auto arg1_column = createColumn<Nullable<String>>({{}, "1/12/2020", "00:59:60 ", "1/12/2020"});
    auto arg2_column = createColumn<Nullable<String>>({"%d/%c/%Y", {}, "%H:%i:%S ", "%d/%c/%Y"});
    ColumnWithTypeAndName result_column(
        createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
            {{}, {}, {}, MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt()})
            .column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(non-null value)), Arg2 is ColumnVector
    arg1_column = createConstColumn<Nullable<String>>(2, {"1/12/2020"});
    arg2_column = createColumn<Nullable<String>>({"%d/%c/%Y", "%d/%c/%Y"});
    result_column = ColumnWithTypeAndName(
        createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
            {MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt(), MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt()})
            .column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(null value)), Arg2 is ColumnVector
    arg1_column = createConstColumn<Nullable<String>>(2, {});
    arg2_column = createColumn<Nullable<String>>({"%d/%c/%Y", "%d/%c/%Y"});
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnVector, Arg2 is ColumnConst(ColumnNullable(non-null value))
    arg1_column = createColumn<Nullable<String>>({"1/12/2020", "1/12/2020"});
    arg2_column = createConstColumn<Nullable<String>>(2, "%d/%c/%Y");
    result_column = ColumnWithTypeAndName(
        createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
            {MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt(), MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt()})
            .column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(non-null value)), Arg2 is ColumnConst(ColumnNullable(non-null value))
    arg1_column = createConstColumn<Nullable<String>>(2, "1/12/2020");
    arg2_column = createConstColumn<Nullable<String>>(2, "%d/%c/%Y");
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(
            2,
            {MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt()})
            .column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(null value)), Arg2 is ColumnConst(ColumnNullable(non-null value))
    arg1_column = createConstColumn<Nullable<String>>(2, {});
    arg2_column = createConstColumn<Nullable<String>>(2, "%d/%c/%Y");
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnVector, Arg2 is ColumnConst(ColumnNullable(null value))
    arg1_column = createColumn<Nullable<String>>({"1/12/2020", "1/12/2020"});
    arg2_column = createConstColumn<Nullable<String>>(2, {});
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(non-null value)), Arg2 is ColumnConst(ColumnNullable(null value))
    arg1_column = createConstColumn<Nullable<String>>(2, {"1/12/2020"});
    arg2_column = createConstColumn<Nullable<String>>(2, {});
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(null value)), Arg2 is ColumnConst(ColumnNullable(null value))
    arg1_column = createConstColumn<Nullable<String>>(2, {});
    arg2_column = createConstColumn<Nullable<String>>(2, {});
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));
}
CATCH

TEST_F(TestTidbConversion, skipCheckOverflowIntToDeciaml)
{
    DataTypePtr int8_ptr = makeDataType<Int8>();
    DataTypePtr int16_ptr = makeDataType<Int16>();
    DataTypePtr int32_ptr = makeDataType<Int32>();
    DataTypePtr int64_ptr = makeDataType<Int64>();
    DataTypePtr uint8_ptr = makeDataType<UInt8>();
    DataTypePtr uint16_ptr = makeDataType<UInt16>();
    DataTypePtr uint32_ptr = makeDataType<UInt32>();
    DataTypePtr uint64_ptr = makeDataType<UInt64>();

    const PrecType prec_decimal32 = 8;
    const PrecType prec_decimal64 = 17;
    const PrecType prec_decimal128 = 37;
    const PrecType prec_decimal256 = 65;
    const ScaleType scale = 0;

    // int8(max_prec: 3) -> decimal32(max_prec: 9)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt8>(int8_ptr, prec_decimal32, scale));
    // int16(max_prec: 5) -> decimal32(max_prec: 9)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt16>(int16_ptr, prec_decimal32, scale));
    // int32(max_prec: 10) -> decimal32(max_prec: 9)
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt32>(int32_ptr, prec_decimal32, scale));
    // int64(max_prec: 20) -> decimal32(max_prec: 9)
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt64>(int64_ptr, prec_decimal32, scale));

    // uint8(max_prec: 3) -> decimal32(max_prec: 9)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt8>(uint8_ptr, prec_decimal32, scale));
    // uint16(max_prec: 5) -> decimal32(max_prec: 9)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt16>(uint16_ptr, prec_decimal32, scale));
    // uint32(max_prec: 10) -> decimal32(max_prec: 9)
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt32>(uint32_ptr, prec_decimal32, scale));
    // uint64(max_prec: 20) -> decimal32(max_prec: 9)
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt64>(uint64_ptr, prec_decimal32, scale));

    // int8(max_prec: 3) -> decimal64(max_prec: 18)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt8>(int8_ptr, prec_decimal64, scale));
    // int16(max_prec: 5) -> decimal64(max_prec: 18)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt16>(int16_ptr, prec_decimal64, scale));
    // int32(max_prec: 10) -> decimal64(max_prec: 18)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt32>(int32_ptr, prec_decimal64, scale));
    // int64(max_prec: 20) -> decimal64(max_prec: 18)
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt64>(int64_ptr, prec_decimal64, scale));

    // uint8(max_prec: 3) -> decimal64(max_prec: 18)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt8>(uint8_ptr, prec_decimal64, scale));
    // uint16(max_prec: 5) -> decimal64(max_prec: 18)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt16>(uint16_ptr, prec_decimal64, scale));
    // uint32(max_prec: 10) -> decimal64(max_prec: 18)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt32>(uint32_ptr, prec_decimal64, scale));
    // uint64(max_prec: 20) -> decimal64(max_prec: 18)
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt64>(uint64_ptr, prec_decimal64, scale));

    // int8(max_prec: 3) -> decimal128(max_prec: 38)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt8>(int8_ptr, prec_decimal128, scale));
    // int16(max_prec: 5) -> decimal128(max_prec: 38)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt16>(int16_ptr, prec_decimal128, scale));
    // int32(max_prec: 10) -> decimal128(max_prec: 38)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt32>(int32_ptr, prec_decimal128, scale));
    // int64(max_prec: 20) -> decimal128(max_prec: 38)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt64>(int64_ptr, prec_decimal128, scale));

    // uint8(max_prec: 3) -> decimal128(max_prec: 38)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt8>(uint8_ptr, prec_decimal128, scale));
    // uint16(max_prec: 5) -> decimal128(max_prec: 38)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt16>(uint16_ptr, prec_decimal128, scale));
    // uint32(max_prec: 10) -> decimal128(max_prec: 38)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt32>(uint32_ptr, prec_decimal128, scale));
    // uint64(max_prec: 20) -> decimal128(max_prec: 38)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt64>(uint64_ptr, prec_decimal128, scale));

    // int8(max_prec: 3) -> decimal256(max_prec: 65)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt8>(int8_ptr, prec_decimal256, scale));
    // int16(max_prec: 5) -> decimal256(max_prec: 65)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt16>(int16_ptr, prec_decimal256, scale));
    // int32(max_prec: 10) -> decimal256(max_prec: 65)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt32>(int32_ptr, prec_decimal256, scale));
    // int64(max_prec: 20) -> decimal256(max_prec: 65)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt64>(int64_ptr, prec_decimal256, scale));

    // uint8(max_prec: 3) -> decimal256(max_prec: 65)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt8>(uint8_ptr, prec_decimal256, scale));
    // uint16(max_prec: 5) -> decimal256(max_prec: 65)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt16>(uint16_ptr, prec_decimal256, scale));
    // uint32(max_prec: 10) -> decimal256(max_prec: 65)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt32>(uint32_ptr, prec_decimal256, scale));
    // uint64(max_prec: 20) -> decimal256(max_prec: 65)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeUInt64>(uint64_ptr, prec_decimal256, scale));
}

TEST_F(TestTidbConversion, skipCheckOverflowDecimalToDecimal)
{
    DataTypePtr decimal32_ptr_8_3 = createDecimal(8, 3);
    DataTypePtr decimal32_ptr_8_2 = createDecimal(8, 2);

    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeDecimal32>(decimal32_ptr_8_2, 8, 3));
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeDecimal32>(decimal32_ptr_8_3, 8, 2));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeDecimal32>(decimal32_ptr_8_2, 7, 5));

    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeDecimal32>(decimal32_ptr_8_2, 9, 3));
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeDecimal32>(decimal32_ptr_8_2, 9, 1));

    DataTypePtr decimal32_ptr_6_4 = createDecimal(6, 4);
    // decimal(6, 4) -> decimal(5, 3)
    // because select cast(99.9999 as decimal(5, 3)); -> 100.000 is greater than 99.999.
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeDecimal32>(decimal32_ptr_6_4, 5, 3));
    // decimal(6, 4) -> decimal(7, 5)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeDecimal32>(decimal32_ptr_6_4, 7, 5));

    // decimal(6, 4) -> decimal(6, 5)
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeDecimal32>(decimal32_ptr_6_4, 6, 5));
    // decimal(6, 4) -> decimal(8, 5)
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeDecimal32>(decimal32_ptr_6_4, 8, 5));
}

TEST_F(TestTidbConversion, skipCheckOverflowEnumToDecimal)
{
    DataTypeEnum8::Values enum8_values;
    enum8_values.push_back({"a", 10});
    enum8_values.push_back({"b", 20});
    DataTypePtr enum8_ptr = std::make_shared<DataTypeEnum8>(enum8_values);

    DataTypeEnum16::Values enum16_values;
    enum16_values.push_back({"a1", 1000});
    enum16_values.push_back({"b1", 2000});
    DataTypePtr enum16_ptr = std::make_shared<DataTypeEnum16>(enum16_values);

    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeEnum8>(enum8_ptr, 3, 0));
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeEnum8>(enum8_ptr, 4, 1));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeEnum8>(enum8_ptr, 2, 0));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeEnum8>(enum8_ptr, 4, 2));

    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeEnum16>(enum16_ptr, 5, 0));
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeEnum16>(enum16_ptr, 6, 1));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeEnum16>(enum16_ptr, 4, 0));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeEnum16>(enum16_ptr, 6, 2));
}

TEST_F(TestTidbConversion, skipCheckOverflowMyDateTimeToDeciaml)
{
    DataTypePtr datetime_ptr_no_fsp = std::make_shared<DataTypeMyDateTime>();
    DataTypePtr datetime_ptr_fsp_5 = std::make_shared<DataTypeMyDateTime>(5);

    // rule for no fsp: 14 + to_scale <= to_prec.
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDateTime>(datetime_ptr_no_fsp, 5, 3));
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDateTime>(datetime_ptr_no_fsp, 18, 3));
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDateTime>(datetime_ptr_no_fsp, 17, 3));
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDateTime>(datetime_ptr_no_fsp, 18, 4));
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDateTime>(datetime_ptr_no_fsp, 14, 0));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDateTime>(datetime_ptr_no_fsp, 14, 1));

    // rule for fsp: 20 + scale_diff <= to_prec.
    // 20 + (3 - 6 + 1) = 18
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDateTime>(datetime_ptr_fsp_5, 19, 3));
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDateTime>(datetime_ptr_fsp_5, 18, 3));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDateTime>(datetime_ptr_fsp_5, 17, 3));
}

TEST_F(TestTidbConversion, skipCheckOverflowMyDateToDeciaml)
{
    DataTypePtr date_ptr = std::make_shared<DataTypeMyDate>();

    // rule: 8 + to_scale <= to_prec.
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDate>(date_ptr, 11, 3));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDate>(date_ptr, 11, 4));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDate>(date_ptr, 10, 3));
}

TEST_F(TestTidbConversion, skipCheckOverflowOtherToDecimal)
{
    // float and string not support skip overflow check.
    DataTypePtr string_ptr = std::make_shared<DataTypeString>();
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeString>(string_ptr, 1, 0));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeString>(string_ptr, 60, 1));

    DataTypePtr float32_ptr = std::make_shared<DataTypeFloat32>();
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeFloat32>(float32_ptr, 1, 0));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeFloat32>(float32_ptr, 60, 1));

    DataTypePtr float64_ptr = std::make_shared<DataTypeFloat64>();
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeFloat64>(float64_ptr, 1, 0));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeFloat64>(float64_ptr, 60, 1));

    // cast duration to decimal is not supported to push down to tiflash for now.
    DataTypePtr duration_ptr = std::make_shared<DataTypeMyDuration>();
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDuration>(duration_ptr, 1, 0));
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeMyDuration>(duration_ptr, 60, 1));
}

// check if template argument of CastInternalType is correct or not.
TEST_F(TestTidbConversion, checkCastInternalType)
try
{
    // case1: cast(tinyint as decimal(7, 3))
    PrecType to_prec = 7;
    ScaleType to_scale = 3;
    DataTypePtr int8_ptr = std::make_shared<DataTypeInt8>();
    // from_prec(3) + to_scale(3) <= Decimal32::prec(9), so we **CAN** skip check overflow.
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt8>(int8_ptr, to_prec, to_scale));

    // from_prec(3) + to_scale(3) <= Int32::real_prec(10) - 1, so CastInternalType should be **Int32**.
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(to_prec, to_scale),
            {DecimalField32(MAX_INT8 * 1000, to_scale), DecimalField32(MIN_INT8 * 1000, to_scale), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
             createCastTypeConstColumn("Nullable(Decimal(7,3))")}));

    // case2: cast(tinyint as decimal(9, 7))
    to_prec = 9;
    to_scale = 7;
    // from_prec(3) + to_scale(7) > Decimal32::prec(9), so we **CANNOT** skip check overflow.
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt8>(int8_ptr, to_prec, to_scale));

    // from_prec(3) + to_scale(7) > Int32::real_prec(10) - 1, so CastInternalType should be **Int64**.
    auto & dag_context = getDAGContext();
    UInt64 ori_flags = dag_context.getFlags();
    dag_context.addFlag(TiDBSQLFlags::OVERFLOW_AS_WARNING);
    dag_context.clearWarnings();
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(to_prec, to_scale),
            {DecimalField32(999999999, to_scale), DecimalField32(-999999999, to_scale), {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
             createCastTypeConstColumn("Nullable(Decimal(9,7))")}));
    dag_context.setFlags(ori_flags);

    // case3: cast(bigint as decimal(40, 20))
    // from_prec(19) + to_scale(20) <= Decimal256::prec(40), so we **CAN** skip check overflow.
    to_prec = 40;
    to_scale = 20;
    DataTypePtr int64_ptr = std::make_shared<DataTypeInt64>();
    ASSERT_TRUE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt64>(int64_ptr, to_prec, to_scale));

    // from_prec(19) + to_scale(20) > Int128::real_prec(39) - 1, so CastInternalType should be **Int256**.
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(to_prec, to_scale),
            {DecimalField256(1024 * static_cast<Int256>(pow(10, to_scale)), to_scale),
             DecimalField256(-1024 * static_cast<Int256>(pow(10, to_scale)), to_scale),
             {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({1024, -1024, {}}), createCastTypeConstColumn("Nullable(Decimal(40,20))")}));

    // case4: cast(bigint as decimal(38, 20))
    // from_prec(19) + to_scale(20) > Decimal256::prec(38), so we **CANNOT** skip check overflow.
    to_prec = 38;
    to_scale = 20;
    ASSERT_FALSE(FunctionTiDBCast<>::canSkipCheckOverflowForDecimal<DataTypeInt64>(int64_ptr, to_prec, to_scale));

    // from_prec(19) + to_scale(20) > Int128::real_prec(39) - 1, so CastInternalType should be **Int256**.
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(to_prec, to_scale),
            {DecimalField128(1024 * static_cast<Int128>(pow(10, to_scale)), to_scale),
             DecimalField128(-1024 * static_cast<Int128>(pow(10, to_scale)), to_scale),
             {}}),
        executeFunction(
            func_name,
            {createColumn<Nullable<Int64>>({1024, -1024, {}}), createCastTypeConstColumn("Nullable(Decimal(38,20))")}));
}
CATCH

// for https://github.com/pingcap/tics/issues/4036
TEST_F(TestTidbConversion, castStringAsDateTime)
try
{
    auto input = std::vector<String>{
        "2012-12-12 12:12:12",
        "2012-12-12\t12:12:12",
        "2012-12-12\n12:12:12",
        "2012-12-12\v12:12:12",
        "2012-12-12\f12:12:12",
        "2012-12-12\r12:12:12"};
    auto to_column = createConstColumn<String>(1, "MyDateTime(6)");

    // vector
    auto from_column = createColumn<String>(input);
    UInt64 except_packed = MyDateTime(2012, 12, 12, 12, 12, 12, 0).toPackedUInt();
    auto vector_result = executeFunction("tidb_cast", {from_column, to_column});
    for (size_t i = 0; i < input.size(); i++)
    {
        ASSERT_EQ(except_packed, vector_result.column.get()->get64(i));
    }

    // const
    auto const_from_column = createConstColumn<String>(1, "2012-12-12\n12:12:12");
    auto const_result = executeFunction("tidb_cast", {from_column, to_column});
    ASSERT_EQ(except_packed, const_result.column.get()->get64(0));

    // nullable
    auto nullable_from_column = createColumn<Nullable<String>>(
        {"2012-12-12 12:12:12",
         "2012-12-12\t12:12:12",
         "2012-12-12\n12:12:12",
         "2012-12-12\v12:12:12",
         "2012-12-12\f12:12:12",
         "2012-12-12\r12:12:12"});
    auto nullable_result = executeFunction("tidb_cast", {from_column, to_column});
    for (size_t i = 0; i < input.size(); i++)
    {
        ASSERT_EQ(except_packed, nullable_result.column.get()->get64(i));
    }
}
CATCH

TEST_F(TestTidbConversion, castTimeAsDuration)
try
{
    const auto to_type_1 = std::make_shared<DataTypeMyDuration>(5); // from_fsp <  to_fsp
    const auto to_type_2 = std::make_shared<DataTypeMyDuration>(4); // from_fsp == to_fsp
    const auto to_type_3 = std::make_shared<DataTypeMyDuration>(2); // from_fsp >  to_fsp
    // cast datetime to duration
    const auto datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(4);
    MyDateTime date(2021, 10, 26, 0, 0, 0, 0);
    MyDateTime datetime(2021, 10, 26, 11, 11, 11, 0);
    MyDateTime datetime_frac1(2021, 10, 26, 11, 11, 11, 111100);
    MyDateTime datetime_frac2(2021, 10, 26, 11, 11, 11, 123500);
    MyDateTime datetime_frac3(2021, 10, 26, 11, 11, 11, 999900);

    auto col_datetime = ColumnUInt64::create();
    col_datetime->insert(Field(date.toPackedUInt()));
    col_datetime->insert(Field(datetime.toPackedUInt()));
    col_datetime->insert(Field(datetime_frac1.toPackedUInt()));
    col_datetime->insert(Field(datetime_frac2.toPackedUInt()));
    col_datetime->insert(Field(datetime_frac3.toPackedUInt()));

    auto ctn_datetime = ColumnWithTypeAndName(std::move(col_datetime), datetime_type_ptr, "datetime");
    ColumnWithTypeAndName datetime_output1(
        createColumn<DataTypeMyDuration::FieldType>({(0 * 3600 + 0 * 60 + 0) * 1000000000L + 000000000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 000000000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 111100000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 123500000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 999900000L})
            .column,
        to_type_1,
        "datetime_output1");
    ColumnWithTypeAndName datetime_output2(
        createColumn<DataTypeMyDuration::FieldType>({(0 * 3600 + 0 * 60 + 0) * 1000000000L + 000000000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 000000000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 111100000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 123500000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 999900000L})
            .column,
        to_type_2,
        "datetime_output2");

    ColumnWithTypeAndName datetime_output3(
        createColumn<DataTypeMyDuration::FieldType>({(0 * 3600 + 0 * 60 + 0) * 1000000000L + 000000000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 000000000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 110000000L,
                                                     (11 * 3600 + 11 * 60 + 11) * 1000000000L + 120000000L,
                                                     (11 * 3600 + 11 * 60 + 12) * 1000000000L + 000000000L})
            .column,
        to_type_3,
        "datetime_output3");


    ASSERT_COLUMN_EQ(
        datetime_output1,
        executeFunction(func_name, {ctn_datetime, createCastTypeConstColumn(to_type_1->getName())}));
    ASSERT_COLUMN_EQ(
        datetime_output2,
        executeFunction(func_name, {ctn_datetime, createCastTypeConstColumn(to_type_2->getName())}));
    ASSERT_COLUMN_EQ(
        datetime_output3,
        executeFunction(func_name, {ctn_datetime, createCastTypeConstColumn(to_type_3->getName())}));


    // Test Const
    ColumnWithTypeAndName input_const(
        createConstColumn<DataTypeMyDateTime::FieldType>(1, datetime_frac2.toPackedUInt()).column,
        datetime_type_ptr,
        "input_const");
    ColumnWithTypeAndName output1_const(
        createConstColumn<DataTypeMyDuration::FieldType>(1, (11 * 3600 + 11 * 60 + 11) * 1000000000L + 123500000L)
            .column,
        to_type_1,
        "output1_const");
    ColumnWithTypeAndName output2_const(
        createConstColumn<DataTypeMyDuration::FieldType>(1, (11 * 3600 + 11 * 60 + 11) * 1000000000L + 123500000L)
            .column,
        to_type_2,
        "output2_const");
    ColumnWithTypeAndName output3_const(
        createConstColumn<DataTypeMyDuration::FieldType>(1, (11 * 3600 + 11 * 60 + 11) * 1000000000L + 120000000L)
            .column,
        to_type_3,
        "output3_const");

    ASSERT_COLUMN_EQ(
        output1_const,
        executeFunction(func_name, {input_const, createCastTypeConstColumn(to_type_1->getName())}));
    ASSERT_COLUMN_EQ(
        output2_const,
        executeFunction(func_name, {input_const, createCastTypeConstColumn(to_type_2->getName())}));
    ASSERT_COLUMN_EQ(
        output3_const,
        executeFunction(func_name, {input_const, createCastTypeConstColumn(to_type_3->getName())}));

    // Test Nullable
    ColumnWithTypeAndName input_nullable(
        createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
            {datetime_frac1.toPackedUInt(), {}, datetime_frac2.toPackedUInt(), {}, datetime_frac3.toPackedUInt()})
            .column,
        makeNullable(datetime_type_ptr),
        "input_nullable");
    ColumnWithTypeAndName output1_nullable(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(11 * 3600 + 11 * 60 + 11) * 1000000000L + 111100000L,
                                                               {},
                                                               (11 * 3600 + 11 * 60 + 11) * 1000000000L + 123500000L,
                                                               {},
                                                               (11 * 3600 + 11 * 60 + 11) * 1000000000L + 999900000L})
            .column,
        makeNullable(to_type_1),
        "output1_output");
    ColumnWithTypeAndName output2_nullable(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(11 * 3600 + 11 * 60 + 11) * 1000000000L + 111100000L,
                                                               {},
                                                               (11 * 3600 + 11 * 60 + 11) * 1000000000L + 123500000L,
                                                               {},
                                                               (11 * 3600 + 11 * 60 + 11) * 1000000000L + 999900000L})
            .column,
        makeNullable(to_type_2),
        "output2_output");
    ColumnWithTypeAndName output3_nullable(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(11 * 3600 + 11 * 60 + 11) * 1000000000L + 110000000L,
                                                               {},
                                                               (11 * 3600 + 11 * 60 + 11) * 1000000000L + 120000000L,
                                                               {},
                                                               (11 * 3600 + 11 * 60 + 12) * 1000000000L + 000000000L})
            .column,
        makeNullable(to_type_3),
        "output3_output");

    ASSERT_COLUMN_EQ(
        output1_nullable,
        executeFunction(func_name, {input_nullable, createCastTypeConstColumn(makeNullable(to_type_1)->getName())}));
    ASSERT_COLUMN_EQ(
        output2_nullable,
        executeFunction(func_name, {input_nullable, createCastTypeConstColumn(makeNullable(to_type_2)->getName())}));
    ASSERT_COLUMN_EQ(
        output3_nullable,
        executeFunction(func_name, {input_nullable, createCastTypeConstColumn(makeNullable(to_type_3)->getName())}));
}
CATCH

// for https://github.com/pingcap/tics/issues/3595
TEST_F(TestTidbConversion, castStringAsDateTime3595)
try
{
    auto & dag_context = getDAGContext();
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    auto to_datetime_column = createConstColumn<String>(1, "Nullable(MyDateTime(6))");
    ColumnWithTypeAndName expect_datetime_column(
        createColumn<Nullable<DataTypeMyDateTime::FieldType>>({{}}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(6)),
        "result");
    auto to_date_column = createConstColumn<String>(1, "Nullable(MyDate)");
    ColumnWithTypeAndName expect_date_column(
        createColumn<Nullable<DataTypeMyDate::FieldType>>({{}}).column,
        makeNullable(std::make_shared<DataTypeMyDate>()),
        "result");

    auto from_column = createColumn<String>({"08:45:16"});
    auto vector_result = executeFunction("tidb_cast", {from_column, to_datetime_column});
    for (size_t i = 0; i < from_column.column->size(); i++)
    {
        ASSERT_COLUMN_EQ(expect_datetime_column, vector_result);
    }
    vector_result = executeFunction("tidb_cast", {from_column, to_date_column});
    for (size_t i = 0; i < from_column.column->size(); i++)
    {
        ASSERT_COLUMN_EQ(expect_date_column, vector_result);
    }

    auto from_decimal_column = createColumn<Decimal32>(std::make_tuple(9, 3), {"102310.023"});
    vector_result = executeFunction("tidb_cast", {from_decimal_column, to_datetime_column});
    for (size_t i = 0; i < from_decimal_column.column->size(); i++)
    {
        ASSERT_COLUMN_EQ(expect_datetime_column, vector_result);
    }
    vector_result = executeFunction("tidb_cast", {from_decimal_column, to_date_column});
    for (size_t i = 0; i < from_decimal_column.column->size(); i++)
    {
        ASSERT_COLUMN_EQ(expect_date_column, vector_result);
    }

    auto from_float_column = createColumn<DataTypeFloat64::FieldType>({102310.023});
    vector_result = executeFunction("tidb_cast", {from_float_column, to_datetime_column});
    for (size_t i = 0; i < from_float_column.column->size(); i++)
    {
        ASSERT_COLUMN_EQ(expect_datetime_column, vector_result);
    }
    vector_result = executeFunction("tidb_cast", {from_float_column, to_date_column});
    for (size_t i = 0; i < from_float_column.column->size(); i++)
    {
        ASSERT_COLUMN_EQ(expect_date_column, vector_result);
    }

    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{{2012, 0, 0, 0, 0, 0, 0}}}, 6),
        executeFunction(
            func_name,
            {createColumn<Nullable<String>>({"20120000"}), createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
}
CATCH

} // namespace
} // namespace DB::tests
