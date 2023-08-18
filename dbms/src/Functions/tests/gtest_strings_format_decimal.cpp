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

#include <DataTypes/DataTypeNullable.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class FormatDecimal : public DB::tests::FunctionTest
{
public:
    // `abc.def` ==> 'fed.cba'
    // `-abc.def` ==> '-fed.cba'
    // `abc.def0000` ==> 'fed.cba'
    // `-abc.def0000` ==> '-fed.cba'
    // `abc.de0000f` ==> 'f0000ed.cba'
    // `-abc.de0000f` ==> '-f0000ed.cba'
    // `abc.de0000f0000` ==> 'f0000ed.cba'
    // `-abc.de0000f0000` ==> '-f0000ed.cba'
    // `0.def` ==> 'fed.'
    // `-0.def` ==> '-fed.'
    // `0.def0000` ==> 'fed.'
    // `-0.def0000` ==> '-fed.'
    // `0.de0000f` ==> 'f0000ed.'
    // `-0.de0000f` ==> '-f0000ed.'
    // `0.de0000f0000` ==> 'f0000ed.'
    // `-0.de0000f0000` ==> '-f0000ed.'
    // `abc` ==> 'cba'
    // `-abc` ==> '-cba'
    // `abc.00` ==> 'cba'
    // `-abc.00` ==> '-cba'
    // `abc0000` ==> '0000cba'
    // `-abc0000` ==> '-0000cba'
    // `abc0000.00` ==> '0000cba'
    // `-abc0000.00` ==> '-0000cba'
    // `0` ==> ''
    // `0.00` ==> ''
    template <typename Decimal>
    void formatTest()
    {
        static const std::string func_name = "formatDecimal";
        using Native = typename Decimal::NativeType;
        using FieldType = DecimalField<Decimal>;
        using NullableDecimal = Nullable<Decimal>;
        auto precision = maxDecimalPrecision<Decimal>();

        auto execute_func = [&](const ColumnWithTypeAndName & column) {
            return executeFunction(func_name, {column}, {}, true);
        };

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"654.321", "-654.321"}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 3),
                {
                    FieldType(static_cast<Native>(123456), 3), // 123.456
                    FieldType(static_cast<Native>(-123456), 3), // -123.456
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"654.321", "-654.321", "600054.321", "-600054.321"}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 6),
                {
                    FieldType(static_cast<Native>(123456000), 6), // 123.456000
                    FieldType(static_cast<Native>(-123456000), 6), // -123.456000
                    FieldType(static_cast<Native>(123450006), 6), // -123.450006
                    FieldType(static_cast<Native>(-123450006), 6), // -123.450006
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"60054.321", "-60054.321"}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 7),
                {
                    FieldType(static_cast<Native>(1234500600), 7), // 123.4500600
                    FieldType(static_cast<Native>(-1234500600), 7), // -123.4500600
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"321.", "-321."}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 3),
                {
                    FieldType(static_cast<Native>(123), 3), // 0.123
                    FieldType(static_cast<Native>(-123), 3), // -0.123
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"321.", "-321.", "400321.", "-400321."}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 6),
                {
                    FieldType(static_cast<Native>(123000), 6), // 0.123000
                    FieldType(static_cast<Native>(-123000), 6), // -0.123000
                    FieldType(static_cast<Native>(123004), 6), // 0.123004
                    FieldType(static_cast<Native>(-123004), 6), // -0.123004
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"400321.", "-400321."}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 8),
                {
                    FieldType(static_cast<Native>(12300400), 8), // 0.12300400
                    FieldType(static_cast<Native>(-12300400), 8), // -0.12300400
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"321", "-321"}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 0),
                {
                    FieldType(static_cast<Native>(123), 0), // 123
                    FieldType(static_cast<Native>(-123), 0), // -123
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"321", "-321"}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 2),
                {
                    FieldType(static_cast<Native>(12300), 2), // 123.00
                    FieldType(static_cast<Native>(-12300), 2), // -123.00
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"00321", "-00321"}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 0),
                {
                    FieldType(static_cast<Native>(12300), 0), // 12300
                    FieldType(static_cast<Native>(-12300), 0), // -12300
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"00321", "-00321"}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 2),
                {
                    FieldType(static_cast<Native>(1230000), 2), // 12300.00
                    FieldType(static_cast<Native>(-1230000), 2), // -12300.00
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({""}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 0),
                {
                    FieldType(static_cast<Native>(0), 0), // 0
                })));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({""}),
            execute_func(createColumn<NullableDecimal>(
                std::make_tuple(precision, 2),
                {
                    FieldType(static_cast<Native>(0), 2), // 0.00
                })));

        auto test_for_min_max = [&](const ColumnWithTypeAndName & expected) {
            ASSERT_COLUMN_EQ(
                expected,
                execute_func(createColumn<NullableDecimal>(
                    std::make_tuple(precision, 2),
                    {
                        FieldType(static_cast<Native>(std::numeric_limits<Native>::max()), 2),
                        FieldType(static_cast<Native>(std::numeric_limits<Native>::max() - 1), 2),
                        FieldType(static_cast<Native>(std::numeric_limits<Native>::min()), 2),
                        FieldType(static_cast<Native>(std::numeric_limits<Native>::min() + 1), 2),
                    })));
        };
        if constexpr (std::is_same_v<Decimal, Decimal32>)
        {
            test_for_min_max(
                createColumn<Nullable<String>>({"74.63847412", "64.63847412", "-84.63847412", "-74.63847412"}));
        }
        else if constexpr (std::is_same_v<Decimal, Decimal64>)
        {
            test_for_min_max(createColumn<Nullable<String>>(
                {"70.85774586302733229", "60.85774586302733229", "-80.85774586302733229", "-70.85774586302733229"}));
        }
        else if constexpr (std::is_same_v<Decimal, Decimal128>)
        {
            test_for_min_max(createColumn<Nullable<String>>(
                {"72.7501488517303786137132964064381141071",
                 "62.7501488517303786137132964064381141071",
                 "-82.7501488517303786137132964064381141071",
                 "-72.7501488517303786137132964064381141071"}));
        }
        else
        {
            static_assert(std::is_same_v<Decimal, Decimal256>);
            test_for_min_max(createColumn<Nullable<String>>(
                {"53.9936921319700485754930465046566489962358709786800589075324591613732980297511",
                 "43.9936921319700485754930465046566489962358709786800589075324591613732980297511",
                 "-53.9936921319700485754930465046566489962358709786800589075324591613732980297511",
                 "-43.9936921319700485754930465046566489962358709786800589075324591613732980297511"}));
        }
    }
};

TEST_F(FormatDecimal, test)
try
{
    formatTest<Decimal32>();
    formatTest<Decimal64>();
    formatTest<Decimal128>();
    formatTest<Decimal256>();
}
CATCH

} // namespace tests
} // namespace DB
