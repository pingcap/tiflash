#include <Functions/FunctionsRound.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{

namespace tests
{

class TestFunctionsRoundWithFrac : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
        }
        catch (Exception &)
        {
            // maybe other tests have already registered.
        }
    }

    String func_name = "tidbRoundWithFrac";

    auto getFrac(size_t size, const std::optional<Int64> & frac) { return createConstColumn<Nullable<Int64>>(size, {frac}); }

    template <typename... Args>
    auto executeWithName(Args &&... args)
    {
        return executeFunction(func_name, {std::forward<Args>(args)...});
    }

    template <typename Input>
    auto execute(const Input & input, std::optional<Int64> frac)
    {
        return executeWithName(input, getFrac(input.column->size(), frac));
    }
};

TEST_F(TestFunctionsRoundWithFrac, PrecisionInfer)
{
    constexpr auto infer = TiDBRoundPrecisionInferer::infer;
    using Result = std::tuple<PrecType, ScaleType>;

    EXPECT_EQ(infer(9, 3, 2, true), Result(9, 2));
    EXPECT_EQ(infer(9, 3, 1, true), Result(8, 1));
    EXPECT_EQ(infer(9, 3, 0, true), Result(7, 0));
    EXPECT_EQ(infer(9, 3, -1, true), Result(7, 0));
    EXPECT_EQ(infer(9, 3, -2, true), Result(7, 0));
    EXPECT_EQ(infer(9, 3, 40, true), Result(36, 30));
    EXPECT_EQ(infer(9, 3, -100, true), Result(7, 0));
    EXPECT_EQ(infer(9, 3, 0, false), Result(10, 3));
    EXPECT_EQ(infer(9, 3, 233, false), Result(10, 3));

    EXPECT_EQ(infer(18, 6, 2, true), Result(15, 2));
    EXPECT_EQ(infer(18, 6, 1, true), Result(14, 1));
    EXPECT_EQ(infer(18, 6, 0, true), Result(13, 0));
    EXPECT_EQ(infer(18, 6, -1, true), Result(13, 0));
    EXPECT_EQ(infer(18, 6, -2, true), Result(13, 0));
    EXPECT_EQ(infer(18, 6, 40, true), Result(42, 30));
    EXPECT_EQ(infer(18, 6, -100, true), Result(13, 0));
    EXPECT_EQ(infer(18, 6, 0, false), Result(19, 6));
    EXPECT_EQ(infer(18, 6, -233, false), Result(19, 6));

    EXPECT_EQ(infer(30, 30, 2, true), Result(3, 2));
    EXPECT_EQ(infer(30, 30, 1, true), Result(2, 1));
    EXPECT_EQ(infer(30, 30, 0, true), Result(1, 0));
    EXPECT_EQ(infer(30, 30, -1, true), Result(1, 0));
    EXPECT_EQ(infer(30, 30, -2, true), Result(1, 0));
    EXPECT_EQ(infer(30, 30, 40, true), Result(30, 30));
    EXPECT_EQ(infer(30, 30, -100, true), Result(1, 0));
    EXPECT_EQ(infer(30, 30, 0, false), Result(31, 30));
    EXPECT_EQ(infer(30, 30, 233, false), Result(31, 30));

    EXPECT_EQ(infer(64, 0, 40, true), Result(65, 30));
    EXPECT_EQ(infer(64, 0, -100, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -62, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -63, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -64, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -65, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, -66, true), Result(65, 0));
    EXPECT_EQ(infer(64, 0, 0, false), Result(65, 0));
    EXPECT_EQ(infer(64, 0, 233, false), Result(65, 0));

    EXPECT_EQ(infer(65, 30, 40, true), Result(65, 30));
    EXPECT_EQ(infer(65, 30, -100, true), Result(36, 0));
    EXPECT_EQ(infer(65, 30, 0, false), Result(65, 30));
    EXPECT_EQ(infer(65, 30, 233, false), Result(65, 30));

    EXPECT_EQ(infer(65, 0, 233, false), Result(65, 0));
    EXPECT_EQ(infer(65, 1, 233, false), Result(65, 1));
    EXPECT_EQ(infer(64, 0, 233, false), Result(65, 0));

    EXPECT_EQ(infer(18, 6, 4, true), Result(17, 4));
    EXPECT_EQ(infer(18, 6, 5, true), Result(18, 5));
    EXPECT_EQ(infer(18, 6, 6, true), Result(18, 6));
    EXPECT_EQ(infer(18, 6, 7, true), Result(19, 7));
    EXPECT_EQ(infer(18, 6, 8, true), Result(20, 8));
}

template <typename T>
class TestFunctionsRoundWithFracSigned : public TestFunctionsRoundWithFrac
{
};

using TestFunctionsRoundWithFracSignedTypes = ::testing::Types<Int8, Int16, Int32, Int64>;
TYPED_TEST_CASE(TestFunctionsRoundWithFracSigned, TestFunctionsRoundWithFracSignedTypes);

TYPED_TEST(TestFunctionsRoundWithFracSigned, ConstFrac)
try
{
    using Int = TypeParam;

    int digits = std::numeric_limits<Int>::digits10;
    Int large = 5;
    for (int i = 0; i < digits - 1; ++i)
        large *= 10;

#define DATA                                                                                 \
    {                                                                                        \
        0, 1, -1, 4, 5, -4, -5, 49, 50, -49, -50, large - 1, large, -(large - 1), -large, {} \
    }

    auto input = createColumn<Nullable<Int>>(DATA);
    auto output = createColumn<Nullable<Int64>>(DATA);
#undef DATA

    ASSERT_COLUMN_EQ(this->execute(input, std::numeric_limits<Int64>::max()), output);
    for (int i = 0; i <= 100; ++i)
        ASSERT_COLUMN_EQ(this->execute(input, i), output) << "i = " << i;

    ASSERT_COLUMN_EQ(this->execute(input, -1),
        createColumn<Nullable<Int64>>({0, 0, 0, 0, 10, 0, -10, 50, 50, -50, -50, large, large, -large, -large, {}}));

    int start = -2;
    if (digits > 2)
    {
        ASSERT_COLUMN_EQ(this->execute(input, -2),
            createColumn<Nullable<Int64>>({0, 0, 0, 0, 0, 0, 0, 0, 100, 0, -100, large, large, -large, -large, {}}));

        start = -3;
    }
    for (int i = start; i >= -(digits - 1); --i)
        ASSERT_COLUMN_EQ(
            this->execute(input, i), createColumn<Nullable<Int64>>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, large, large, -large, -large, {}}))
            << "i = " << i;

    if (digits > 2)
    {
        ASSERT_COLUMN_EQ(this->execute(input, -digits),
            createColumn<Nullable<Int64>>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, large * 2, 0, -large * 2, {}}));
    }

    auto zeroes = createColumn<Nullable<Int64>>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, {}});
    for (int i = -(digits + 1); i >= -100; --i)
        ASSERT_COLUMN_EQ(this->execute(input, i), zeroes) << "i = " << i;
    ASSERT_COLUMN_EQ(this->execute(input, std::numeric_limits<Int64>::min()), zeroes);
}
CATCH

template <typename T>
class TestFunctionsRoundWithFracUnsigned : public TestFunctionsRoundWithFrac
{
};

using TestFunctionsRoundWithFracUnsignedTypes = ::testing::Types<UInt8, UInt16, UInt32, UInt64>;
TYPED_TEST_CASE(TestFunctionsRoundWithFracUnsigned, TestFunctionsRoundWithFracUnsignedTypes);

TYPED_TEST(TestFunctionsRoundWithFracUnsigned, ConstFrac)
try
{
    using UInt = TypeParam;

    UInt large = 5;
    int digits = std::numeric_limits<UInt>::digits10;
    for (int i = 0; i < digits - 1; ++i)
        large *= 10;

#define DATA                                     \
    {                                            \
        0, 1, 4, 5, 49, 50, large - 1, large, {} \
    }

    auto input = createColumn<Nullable<UInt>>(DATA);
    auto output = createColumn<Nullable<UInt64>>(DATA);
#undef DATA

    ASSERT_COLUMN_EQ(this->execute(input, std::numeric_limits<Int64>::max()), output);
    for (int i = 0; i <= 100; ++i)
        ASSERT_COLUMN_EQ(this->execute(input, i), output) << "i = " << i;

    ASSERT_COLUMN_EQ(this->execute(input, -1), createColumn<Nullable<UInt64>>({0, 0, 0, 10, 50, 50, large, large, {}}));

    int start = -2;
    if (digits > 2)
    {
        ASSERT_COLUMN_EQ(this->execute(input, -2), createColumn<Nullable<UInt64>>({0, 0, 0, 0, 0, 100, large, large, {}}));

        start = -3;
    }
    for (int i = start; i >= -(digits - 1); --i)
        ASSERT_COLUMN_EQ(this->execute(input, i), createColumn<Nullable<UInt64>>({0, 0, 0, 0, 0, 0, large, large, {}})) << "i = " << i;

    if (digits > 2)
    {
        ASSERT_COLUMN_EQ(this->execute(input, -digits), createColumn<Nullable<UInt64>>({0, 0, 0, 0, 0, 0, 0, large * 2, {}}));
    }

    auto zeroes = createColumn<Nullable<UInt64>>({0, 0, 0, 0, 0, 0, 0, 0, {}});
    for (int i = -(digits + 1); i >= -100; --i)
        ASSERT_COLUMN_EQ(this->execute(input, i), zeroes) << "i = " << i;
    ASSERT_COLUMN_EQ(this->execute(input, std::numeric_limits<Int64>::min()), zeroes);
}
CATCH

TEST_F(TestFunctionsRoundWithFrac, Int64ConstFracOverflow)
{
    using Limits = std::numeric_limits<Int64>;

    {
        auto input = createColumn<Int64>({Limits::max(), Limits::min()});

        EXPECT_NO_THROW(execute(input, Limits::max()));
        for (int i = 0; i <= 100; ++i)
            EXPECT_NO_THROW(execute(input, 0)) << "i = " << i;
    }

    for (Int64 v : {Limits::max(), Limits::min()})
    {
        auto input = createColumn<Int64>({v});
        UInt64 value = v;

        for (int i = 0; i <= 100; ++i)
        {
            UInt64 digit = value % 10;
            value /= 10;

            auto message = fmt::format("value = {}, i = {}", value, i);
            if (digit >= 5)
                EXPECT_THROW(execute(input, -(i + 1)), Exception) << message;
            else
                EXPECT_NO_THROW(execute(input, -(i + 1))) << message;
        }

        EXPECT_NO_THROW(execute(input, Limits::min()));
    }
}

TEST_F(TestFunctionsRoundWithFrac, UInt64ConstFracOverflow)
{
    UInt64 value = std::numeric_limits<UInt64>::max();
    auto input = createColumn<UInt64>({value});

    EXPECT_NO_THROW(execute(input, std::numeric_limits<Int64>::max()));
    for (int i = 0; i <= 100; ++i)
        EXPECT_NO_THROW(execute(input, 0)) << "i = " << i;

    for (int i = 0; i <= 100; ++i)
    {
        UInt64 digit = value % 10;
        value /= 10;

        auto message = fmt::format("value = {}, i = {}", value, i);
        if (digit >= 5)
            EXPECT_THROW(execute(input, -(i + 1)), Exception) << message;
        else
            EXPECT_NO_THROW(execute(input, -(i + 1))) << message;
    }

    EXPECT_NO_THROW(execute(input, std::numeric_limits<Int64>::min()));
}

} // namespace tests

} // namespace DB
