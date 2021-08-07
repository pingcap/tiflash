#include <Functions/FunctionsRound.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{

namespace tests
{

class TestFunctionsRoundWithFrac : public ::testing::Test
{
protected:
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
};

TEST_F(TestFunctionsRoundWithFrac, PrecisionInfer) {
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

} // namespace tests

} // namespace DB
