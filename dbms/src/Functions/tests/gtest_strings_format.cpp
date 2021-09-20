#include <DataTypes/DataTypeNullable.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class StringFormat : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }
};


TEST_F(StringFormat, FormatWithLocaleAllUnitTest)
try
{
    const std::string func_name = "formatWithLocale";
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"12,332.1235", "12,332.1235", "12,332.1235", "12,332.1235", "12,332.1235", {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({12332.123456, 12332.123456, 12332.123456, 12332.123456, 12332.123456, 12332.123456, {}, {}}),
            createColumn<Nullable<Int64>>({4, 4, 4, 4, 4, {}, 4, {}}),
            createColumn<Nullable<String>>({"en_US", "en_us", "EN_US", "xxx", {}, "xx", "xx", "xx"})));
}
CATCH

TEST_F(StringFormat, StringFormatAllUnitTest)
try
{
    const std::string func_name = "format";

    // float64, int64
    // vector, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"12,332.1235", "12,332.1000", "12,332", "12,332", "12,332.300000000000000000000000000000", "-12,332.30000", "-1,000.0", "-333.33", {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({12332.123456, 12332.1, 12332.2, 12332.3, 12332.3, -12332.3, -999.9999, -333.333, 0}),
            createColumn<Nullable<Int64>>({4, 4, 0, -1, 31, 5, 1, 2, {}})));
    // vector, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"12,332.123", "12,332.100", "-12,332.300", "-1,000.000", "-333.333"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({12332.123456, 12332.1, -12332.3, -999.9999, -333.333}),
            createConstColumn<Nullable<Int64>>(5, 3)));
    // const, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"-999.9999", "-1,000", "-1,000", "-999.999900000000000000000000000000", "-999.99990", "-1,000.0", "-1,000.00"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Float64>>(7, -999.9999),
            createColumn<Nullable<Int64>>({4, 0, -1, 31, 5, 1, 2})));
    // const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, "-1,000.000"),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Float64>>(1, -999.9999),
            createConstColumn<Nullable<Int64>>(1, 3)));

    // float64, uint64
    // vector, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"12,332.1235", "12,332.1000", "12,332", "12,332.300000000000000000000000000000", "-12,332.30000", "-1,000.0", "-333.33"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({12332.123456, 12332.1, 12332.2, 12332.3, -12332.3, -999.9999, -333.333}),
            createColumn<Nullable<UInt64>>({4, 4, 0, 31, 5, 1, 2})));
    // vector, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"12,332.123", "12,332.100", "-12,332.300", "-1,000.000", "-333.333"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({12332.123456, 12332.1, -12332.3, -999.9999, -333.333}),
            createConstColumn<Nullable<UInt64>>(8, 3)));
    // const, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"-999.9999", "-1,000", "-999.999900000000000000000000000000", "-999.99990", "-1,000.0", "-1,000.00"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Float64>>(6, -999.9999),
            createColumn<Nullable<UInt64>>({4, 0, 31, 5, 1, 2})));
    // const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, "-1,000.000"),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Float64>>(1, -999.9999),
            createConstColumn<Nullable<UInt64>>(1, 3)));

#define DECIMAL_TESTCASE(Decimal, Origin, precision)                                                                                                                  \
    do                                                                                                                                                                \
    {                                                                                                                                                                 \
        using FieldType = DecimalField<Decimal>;                                                                                                                      \
        using NullableDecimal = Nullable<Decimal>;                                                                                                                    \
        auto prec = (precision);                                                                                                                                      \
        ASSERT_COLUMN_EQ(                                                                                                                                             \
            createColumn<Nullable<String>>({"12,332.1000", "12,332", "12,332", "12,332.300000000000000000000000000000", "-12,332.30000", "-1,000.0", "-333.33", {}}), \
            executeFunction(                                                                                                                                          \
                func_name,                                                                                                                                            \
                createColumn<NullableDecimal>(                                                                                                                        \
                    std::make_tuple(prec, 4),                                                                                                                         \
                    {FieldType(static_cast<Origin>(123321000), 4),                                                                                                    \
                     FieldType(static_cast<Origin>(123322000), 4),                                                                                                    \
                     FieldType(static_cast<Origin>(123323000), 4),                                                                                                    \
                     FieldType(static_cast<Origin>(123323000), 4),                                                                                                    \
                     FieldType(static_cast<Origin>(-123323000), 4),                                                                                                   \
                     FieldType(static_cast<Origin>(-9999999), 4),                                                                                                     \
                     FieldType(static_cast<Origin>(-3333330), 4),                                                                                                     \
                     FieldType(static_cast<Origin>(0), 0)}),                                                                                                          \
                createColumn<Nullable<Int64>>({4, 0, -1, 31, 5, 1, 2, {}})));                                                                                         \
        ASSERT_COLUMN_EQ(                                                                                                                                             \
            createColumn<Nullable<String>>({"12,332.100", "-12,332.300", "-1,000.000", "-333.333"}),                                                                  \
            executeFunction(                                                                                                                                          \
                func_name,                                                                                                                                            \
                createColumn<NullableDecimal>(                                                                                                                        \
                    std::make_tuple(prec, 4),                                                                                                                         \
                    {FieldType(static_cast<Origin>(123321000), 4),                                                                                                    \
                     FieldType(static_cast<Origin>(-123323000), 4),                                                                                                   \
                     FieldType(static_cast<Origin>(-9999999), 4),                                                                                                     \
                     FieldType(static_cast<Origin>(-3333330), 4)}),                                                                                                   \
                createConstColumn<Nullable<Int64>>(4, 3)));                                                                                                           \
        ASSERT_COLUMN_EQ(                                                                                                                                             \
            createColumn<Nullable<String>>({"-999.9999", "-1,000", "-1,000", "-999.999900000000000000000000000000", "-999.99990", "-1,000.0", "-1,000.00"}),          \
            executeFunction(                                                                                                                                          \
                func_name,                                                                                                                                            \
                createConstColumn<NullableDecimal>(                                                                                                                   \
                    std::make_tuple(prec, 4),                                                                                                                         \
                    7,                                                                                                                                                \
                    FieldType(static_cast<Origin>(-9999999), 4)),                                                                                                     \
                createColumn<Nullable<Int64>>({4, 0, -1, 31, 5, 1, 2})));                                                                                             \
        ASSERT_COLUMN_EQ(                                                                                                                                             \
            createConstColumn<Nullable<String>>(1, "-1,000.000"),                                                                                                     \
            executeFunction(                                                                                                                                          \
                func_name,                                                                                                                                            \
                createConstColumn<NullableDecimal>(                                                                                                                   \
                    std::make_tuple(prec, 4),                                                                                                                         \
                    1,                                                                                                                                                \
                    FieldType(static_cast<Origin>(-9999999), 4)),                                                                                                     \
                createConstColumn<Nullable<Int64>>(1, 3)));                                                                                                           \
        ASSERT_COLUMN_EQ(                                                                                                                                             \
            createColumn<Nullable<String>>({"12,332.1000", "12,332", "12,332.300000000000000000000000000000", "-12,332.30000", "-1,000.0", "-333.33", {}}),           \
            executeFunction(                                                                                                                                          \
                func_name,                                                                                                                                            \
                createColumn<NullableDecimal>(                                                                                                                        \
                    std::make_tuple(prec, 4),                                                                                                                         \
                    {FieldType(static_cast<Origin>(123321000), 4),                                                                                                    \
                     FieldType(static_cast<Origin>(123323000), 4),                                                                                                    \
                     FieldType(static_cast<Origin>(123323000), 4),                                                                                                    \
                     FieldType(static_cast<Origin>(-123323000), 4),                                                                                                   \
                     FieldType(static_cast<Origin>(-9999999), 4),                                                                                                     \
                     FieldType(static_cast<Origin>(-3333330), 4),                                                                                                     \
                     FieldType(static_cast<Origin>(0), 0)}),                                                                                                          \
                createColumn<Nullable<UInt64>>({4, 0, 31, 5, 1, 2, {}})));                                                                                            \
        ASSERT_COLUMN_EQ(                                                                                                                                             \
            createColumn<Nullable<String>>({"12,332.100", "-12,332.300", "-1,000.000", "-333.333"}),                                                                  \
            executeFunction(                                                                                                                                          \
                func_name,                                                                                                                                            \
                createColumn<NullableDecimal>(                                                                                                                        \
                    std::make_tuple(prec, 4),                                                                                                                         \
                    {FieldType(static_cast<Origin>(123321000), 4),                                                                                                    \
                     FieldType(static_cast<Origin>(-123323000), 4),                                                                                                   \
                     FieldType(static_cast<Origin>(-9999999), 4),                                                                                                     \
                     FieldType(static_cast<Origin>(-3333330), 4)}),                                                                                                   \
                createConstColumn<Nullable<UInt64>>(4, 3)));                                                                                                          \
        ASSERT_COLUMN_EQ(                                                                                                                                             \
            createColumn<Nullable<String>>({"-999.9999", "-1,000", "-999.999900000000000000000000000000", "-999.99990", "-1,000.0", "-1,000.00"}),                    \
            executeFunction(                                                                                                                                          \
                func_name,                                                                                                                                            \
                createConstColumn<NullableDecimal>(                                                                                                                   \
                    std::make_tuple(prec, 4),                                                                                                                         \
                    6,                                                                                                                                                \
                    FieldType(static_cast<Origin>(-9999999), 4)),                                                                                                     \
                createColumn<Nullable<UInt64>>({4, 0, 31, 5, 1, 2})));                                                                                                \
        ASSERT_COLUMN_EQ(                                                                                                                                             \
            createConstColumn<Nullable<String>>(1, "-1,000.000"),                                                                                                     \
            executeFunction(                                                                                                                                          \
                func_name,                                                                                                                                            \
                createConstColumn<NullableDecimal>(                                                                                                                   \
                    std::make_tuple(prec, 4),                                                                                                                         \
                    1,                                                                                                                                                \
                    FieldType(static_cast<Origin>(-9999999), 4)),                                                                                                     \
                createConstColumn<Nullable<UInt64>>(1, 3)));                                                                                                          \
    } while (false)

    DECIMAL_TESTCASE(Decimal32, Int32, 9);
    DECIMAL_TESTCASE(Decimal64, Int64, 18);
    DECIMAL_TESTCASE(Decimal128, Int128, 38);
    DECIMAL_TESTCASE(Decimal256, Int256, 65);

#undef DECIMAL_TESTCASE
}
CATCH

} // namespace tests
} // namespace DB
