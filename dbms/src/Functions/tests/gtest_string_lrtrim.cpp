#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>

namespace DB
{
namespace tests
{
class StringLRTrim : public DB::tests::FunctionTest
{
protected:
    ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<String>(1, s);
    }
};

TEST_F(StringLRTrim, strLRTrimTest)
try
{
    // ltrim(const)
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "x "),
        executeFunction("tidbLTrim", createConstColumn<Nullable<String>>(5, " x ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "测试 "),
        executeFunction("tidbLTrim", createConstColumn<Nullable<String>>(5, " 测试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "x x x"),
        executeFunction("tidbLTrim", createConstColumn<Nullable<String>>(5, "x x x")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "测 试 "),
        executeFunction("tidbLTrim", createConstColumn<Nullable<String>>(5, "测 试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "x "),
        executeFunction("tidbLTrim", createConstColumn<String>(5, " x ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "测试 "),
        executeFunction("tidbLTrim", createConstColumn<String>(5, " 测试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "x x x"),
        executeFunction("tidbLTrim", createConstColumn<String>(5, "x x x")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "测 试 "),
        executeFunction("tidbLTrim", createConstColumn<String>(5, "测 试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(0, ""),
        executeFunction("tidbLTrim", createConstColumn<String>(0, "测 试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, ""),
        executeFunction("tidbLTrim", createConstColumn<String>(5, "   ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, ""),
        executeFunction("tidbLTrim", createConstColumn<String>(5, "")));
    ASSERT_COLUMN_EQ(
        toConst("+Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^   "),
        executeFunction("tidbLTrim", toConst("   +Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^   ")));
    ASSERT_COLUMN_EQ(
        toConst("▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕   "),
        executeFunction("tidbLTrim", toConst("   ▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕   ")));
    ASSERT_COLUMN_EQ(
        toConst("թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ   "),
        executeFunction("tidbLTrim", toConst("   թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ   ")));

    // rtrim(const)
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, " x"),
        executeFunction("tidbRTrim", createConstColumn<Nullable<String>>(5, " x ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, " 测试"),
        executeFunction("tidbRTrim", createConstColumn<Nullable<String>>(5, " 测试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "x x x"),
        executeFunction("tidbRTrim", createConstColumn<Nullable<String>>(5, "x x x")));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(5, "测 试"),
        executeFunction("tidbRTrim", createConstColumn<Nullable<String>>(5, "测 试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, " x"),
        executeFunction("tidbRTrim", createConstColumn<String>(5, " x ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, " 测试"),
        executeFunction("tidbRTrim", createConstColumn<String>(5, " 测试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "x x x"),
        executeFunction("tidbRTrim", createConstColumn<String>(5, "x x x")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, "测 试"),
        executeFunction("tidbRTrim", createConstColumn<String>(5, "测 试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(0, ""),
        executeFunction("tidbRTrim", createConstColumn<String>(0, "测 试 ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, ""),
        executeFunction("tidbRTrim", createConstColumn<String>(5, "   ")));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(5, ""),
        executeFunction("tidbRTrim", createConstColumn<String>(5, "")));
    ASSERT_COLUMN_EQ(
        toConst("   +Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^"),
        executeFunction("tidbRTrim", toConst("   +Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^   ")));
    ASSERT_COLUMN_EQ(
        toConst("   ▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕"),
        executeFunction("tidbRTrim", toConst("   ▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕   ")));
    ASSERT_COLUMN_EQ(
        toConst("   թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"),
        executeFunction("tidbRTrim", toConst("   թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ   ")));

    // ltrim(column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"xx aa", "xxaa xx ", "\t aa \t", "", {}, "", "\n\t"}),
        executeFunction("tidbLTrim", createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}, " ", "\n\t"})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({"xx aa", "xxaa xx ", "\t aa \t", "", {}, "", "\n\t"}),
        executeFunction("tidbLTrim", createColumn<String>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}, " ", "\n\t"})));
    // rtrim(column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"  xx aa", "  xxaa xx", "\t aa \t", "", {}, "", "\n\t"}),
        executeFunction("tidbRTrim", createColumn<Nullable<String>>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}, " ", "\n\t"})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({"  xx aa", "  xxaa xx", "\t aa \t", "", {}, "", "\n\t"}),
        executeFunction("tidbRTrim", createColumn<String>({"  xx aa", "  xxaa xx ", "\t aa \t", "", {}, " ", "\n\t"})));


    // ltrim(column) Nullable<String> ASCII group and non-ASCII group
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"a", "b", "c ", "d ", "e f", "g h", "i j ", "k l "}),
        executeFunction("tidbLTrim", createColumn<Nullable<String>>({"a", " b", "c ", " d ", "e f", " g h", "i j ", " k l "})));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"你", "好", "平 ", "凯 ", "星 辰", "啊 波", "次 得 ", "额 佛 "}),
        executeFunction("tidbLTrim", createColumn<Nullable<String>>({"你", " 好", "平 ", " 凯 ", "星 辰", " 啊 波", "次 得 ", " 额 佛 "})));
    // ltrim(column) String ASCII group and non-ASCII group
    ASSERT_COLUMN_EQ(
        createColumn<String>({"a", "b", "c ", "d ", "e f", "g h", "i j ", "k l "}),
        executeFunction("tidbLTrim", createColumn<String>({"a", " b", "c ", " d ", "e f", " g h", "i j ", " k l "})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({"你", "好", "平 ", "凯 ", "星 辰", "啊 波", "次 得 ", "额 佛 "}),
        executeFunction("tidbLTrim", createColumn<String>({"你", " 好", "平 ", " 凯 ", "星 辰", " 啊 波", "次 得 ", " 额 佛 "})));
    // rtrim(column)  Nullable<String> ASCII group and non-ASCII group
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"a", " b", "c", " d", "e f", " g h", "i j", " k l"}),
        executeFunction("tidbRTrim", createColumn<Nullable<String>>({"a", " b", "c ", " d ", "e f", " g h", "i j ", " k l "})));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"你", " 好", "平", " 凯", "星 辰", " 啊 波", "次 得", " 额 佛"}),
        executeFunction("tidbRTrim", createColumn<Nullable<String>>({"你", " 好", "平 ", " 凯 ", "星 辰", " 啊 波", "次 得 ", " 额 佛 "})));
    // rtrim(column)  String ASCII group and non-ASCII group
    ASSERT_COLUMN_EQ(
        createColumn<String>({"a", " b", "c", " d", "e f", " g h", "i j", " k l"}),
        executeFunction("tidbRTrim", createColumn<String>({"a", " b", "c ", " d ", "e f", " g h", "i j ", " k l "})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({"你", " 好", "平", " 凯", "星 辰", " 啊 波", "次 得", " 额 佛"}),
        executeFunction("tidbRTrim", createColumn<String>({"你", " 好", "平 ", " 凯 ", "星 辰", " 啊 波", "次 得 ", " 额 佛 "})));
}
CATCH

} // namespace tests
} // namespace DB
