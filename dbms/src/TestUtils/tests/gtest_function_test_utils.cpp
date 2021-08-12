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
    ASSERT_EQ(parse("+.123", 3, 3), DecimalField64(123, 3));
    ASSERT_EQ(parse("-0.123", 4, 3), DecimalField64(-123, 3));
    ASSERT_EQ(parse("", 0, 0), DecimalField64(0, 0));
    ASSERT_EQ(parse("'''", 0, 0), DecimalField64(0, 0));
    ASSERT_THROW(parse(".", 0, 0), TiFlashTestException);
    ASSERT_EQ(parse("0", 1, 0), DecimalField64(0, 0));
    ASSERT_EQ(parse("0''''0", 2, 0), DecimalField64(0, 0));
    ASSERT_THROW(parse("0.", 1, 0), TiFlashTestException);
    ASSERT_EQ(parse("0.0", 2, 1), DecimalField64(0, 1));
    ASSERT_EQ(parse("000'000.000'000", 12, 6), DecimalField64(0, 6));
    ASSERT_THROW(parse("0..", 1, 0), TiFlashTestException);
    ASSERT_THROW(parse("abc", 3, 0), TiFlashTestException);
}
CATCH

} // namespace tests

} // namespace DB
