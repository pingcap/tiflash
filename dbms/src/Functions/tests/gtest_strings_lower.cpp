#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB::tests
{
class StringUpper : public DB::tests::FunctionTest
{
protected:
    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    static ColumnWithTypeAndName toVec(const std::vector<std::optional<String>> & v)
    {
        std::vector<String> strings;
        strings.reserve(v.size());
        for (std::optional<String> s : v)
        {
            strings.push_back(s.value());
        }

        return createColumn<String>(strings);
    }

    static ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<String>(1, s);
    }
};

TEST_F(StringUpper, lowerAll)
{
    std::vector<std::optional<String>> candidate_strings = {"one WEEK’S time TEST", "abc测试def", "ABCテストabc", "ЀЁЂѓЄЅІїЈЉЊЋЌѝЎЏ", "+Ѐ-ё*Ђ/ѓ!Є@Ѕ#І$@Ї%Ј……љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^", "ΑΒΓΔΕΖΗΘικΛΜΝΞΟΠΡΣτΥΦΧΨωΣ", "▲Α▼Βγ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕", "թՓՁՋՐՉՃԺԾՔՈԵՌՏԸՒԻՕՊԱՍԴՖԳՀՅԿԼԽԶՂՑՎԲՆմՇ"};
    std::vector<std::optional<String>> lower_case_strings = {"one week’s time test", "abc测试def", "abcテストabc", "ѐёђѓєѕіїјљњћќѝўџ", "+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……љ&њ（ћ）ќ￥ѝ#ў@џ！^", "αβγδεζηθικλμνξοπρστυφχψωσ", "▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★σ✕", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"};


    ASSERT_COLUMN_EQ(
        toNullableVec(lower_case_strings),
        executeFunction(
            "lowerUTF8",
            toNullableVec(candidate_strings)));

    ASSERT_COLUMN_EQ(
        toVec(lower_case_strings),
        executeFunction(
            "lowerUTF8",
            toVec(candidate_strings)));

    ASSERT_COLUMN_EQ(
        toNullableVec(candidate_strings),
        executeFunction(
            "lowerBinary",
            toNullableVec(candidate_strings)));

    ASSERT_COLUMN_EQ(
        toVec(candidate_strings),
        executeFunction(
            "lowerBinary",
            toVec(candidate_strings)));

    ASSERT_COLUMN_EQ(
        toConst("one week’s time test"),
        executeFunction(
            "lowerUTF8",
            toConst("ONE WEEK’S TIME TEST")));

    ASSERT_COLUMN_EQ(
        toConst("+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……љ&њ（ћ）ќ￥ѝ#ў@џ！^"),
        executeFunction(
            "lowerUTF8",
            toConst("+Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^")));

    ASSERT_COLUMN_EQ(
        toConst("▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★σ✕"),
        executeFunction(
            "lowerUTF8",
            toConst("▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕")));

    ASSERT_COLUMN_EQ(
        toConst("թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"),
        executeFunction(
            "lowerBinary",
            toConst("թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ")));
}


} // namespace DB::tests
