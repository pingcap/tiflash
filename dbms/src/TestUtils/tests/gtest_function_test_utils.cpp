#include <TestUtils/FunctionTestUtils.h>

namespace DB
{

namespace tests
{

class TestFunctionTestUtils : public ::testing::Test
{
};

TEST_F(TestFunctionTestUtils, ParseDecimal)
try
{
    using DecimalField64 = DecimalField<Decimal64>;

    ASSERT_EQ(parseDecimal<Nullable<Decimal64>>(std::nullopt), std::nullopt);
    ASSERT_EQ(parseDecimal<Nullable<Decimal64>>("123", 3, 0), DecimalField64(123, 0));

    constexpr auto parse = parseDecimal<Decimal64>;
    ASSERT_EQ(parse("123.123", 6, 3), DecimalField64(123123, 3));
    ASSERT_THROW(parse("123.123", 3, 3), TiFlashTestException);
    ASSERT_THROW(parse("123.123", 6, 0), TiFlashTestException);
    ASSERT_NO_THROW(parse("123.123", 10, 3));
    ASSERT_THROW(parse(" 123.123", 6, 3), TiFlashTestException);
    ASSERT_NO_THROW(parse("123.123", 60, 3));
    ASSERT_THROW(parse("123123123123123123.123", 60, 3), TiFlashTestException);
    ASSERT_THROW(parse("+.123", 3, 3), TiFlashTestException);
    ASSERT_EQ(parse("+0.123", 3, 3), DecimalField64(123, 3));
    ASSERT_EQ(parse("-0.123", 4, 3), DecimalField64(-123, 3));
    ASSERT_EQ(parse("", 1, 0), DecimalField64(0, 0));
    ASSERT_THROW(parse(".", 1, 0), TiFlashTestException);
    ASSERT_EQ(parse("0", 1, 0), DecimalField64(0, 0));
    ASSERT_EQ(parse("00", 2, 0), DecimalField64(0, 0));
    ASSERT_EQ(parse("0.", 1, 0), DecimalField64(0, 0));
    ASSERT_EQ(parse("0.0", 2, 1), DecimalField64(0, 1));
    ASSERT_EQ(parse("000000.000000", 12, 6), DecimalField64(0, 6));
    ASSERT_THROW(parse("0..", 1, 0), TiFlashTestException);
    ASSERT_THROW(parse("abc", 3, 0), TiFlashTestException);
}
CATCH

TEST_F(TestFunctionTestUtils, CreateDecimalColumn)
try
{
    using DecimalField64 = DecimalField<Decimal64>;

    auto args = std::make_tuple(4, 2);
    auto field = DecimalField64(4200, 2);
    auto null = Null();

    {
        std::vector<DecimalField64> vec = {field};
        auto column = createColumn<Decimal64>(args, vec).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 1);
        ASSERT_EQ((*column)[0], field);
    }

    {
        std::vector<std::optional<DecimalField64>> vec = {field, std::nullopt};
        auto column = createColumn<Nullable<Decimal64>>(args, vec).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ((*column)[0], field);
        ASSERT_EQ((*column)[1], null);
    }

    {
        std::vector<std::optional<DecimalField64>> vec = {std::nullopt, field};
        auto column = createColumn<Nullable<Decimal64>>(args, vec).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ((*column)[0], null);
        ASSERT_EQ((*column)[1], field);
    }

    {
        auto column = createColumn<Decimal64>(args, {field}).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 1);
        ASSERT_EQ((*column)[0], field);
    }

    {
        auto column = createColumn<Nullable<Decimal64>>(args, {field, {}}).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ((*column)[0], field);
        ASSERT_EQ((*column)[1], null);
    }

    {
        auto column = createColumn<Nullable<Decimal64>>(args, {{}, field}).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ((*column)[0], null);
        ASSERT_EQ((*column)[1], field);
    }

    {
        std::vector<String> vec = {"42.00"};
        auto column = createColumn<Decimal64>(args, vec).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 1);
        ASSERT_EQ((*column)[0], field);
    }

    {
        std::vector<std::optional<String>> vec = {"42.00", {}};
        auto column = createColumn<Nullable<Decimal64>>(args, vec).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ((*column)[0], field);
        ASSERT_EQ((*column)[1], null);
    }

    {
        std::vector<std::optional<String>> vec = {{}, "42.00"};
        auto column = createColumn<Nullable<Decimal64>>(args, vec).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ((*column)[0], null);
        ASSERT_EQ((*column)[1], field);
    }

    {
        auto column = createColumn<Nullable<Decimal64>>(args, {"42.00"}).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 1);
        ASSERT_EQ((*column)[0], field);
    }

    {
        auto column = createColumn<Nullable<Decimal64>>(args, {"42.00", {}}).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ((*column)[0], field);
        ASSERT_EQ((*column)[1], null);
    }

    {
        auto column = createColumn<Nullable<Decimal64>>(args, {{}, "42.00"}).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ((*column)[0], null);
        ASSERT_EQ((*column)[1], field);
    }

    // passing an initializer list of "{}" is ambiguous.
    // > createColumn<Nullable<Decimal64>>(args, {{}})
    // > createColumn<Nullable<Decimal64>>(args, {{}, {}})
    //
    // workaround: use `std::nullopt` instead.

    {
        auto column = createColumn<Nullable<Decimal64>>(args, {std::nullopt}).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 1);
        ASSERT_EQ((*column)[0], null);
    }

    {
        auto column = createColumn<Nullable<Decimal64>>(args, {std::nullopt, std::nullopt}).column;
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), 2);
        ASSERT_EQ((*column)[0], null);
        ASSERT_EQ((*column)[1], null);
    }
}
CATCH

} // namespace tests

} // namespace DB
