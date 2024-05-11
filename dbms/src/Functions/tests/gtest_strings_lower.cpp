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
class StringLower : public DB::tests::FunctionTest
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

TEST_F(StringLower, lowerAll)
{
    std::vector<std::optional<String>> candidate_strings
        = {"one WEEK'S time TEST",
           "abc测试def",
           "ABCテストabc",
           "ЀЁЂѓЄЅІїЈЉЊЋЌѝЎЏ",
           "+Ѐ-ё*Ђ/ѓ!Є@Ѕ#І$@Ї%Ј……љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^",
           "İaSdİİİİdDS",
           "ΑΒΓΔΕΖΗΘικΛΜΝΞΟΠΡΣτΥΦΧΨωΣ",
           "ȺDȺİȺaȺȾOİȺ",
           "TEST_WRONG_UTF8_1\x80\xe0\x21",
           "▲Α▼Βγ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕",
           "ⱮⱭȺΩABCDEFGHIJKLMNOPꞍaȾ",
           "TEST_WRONG_UTF8_2\xf1\x22",
           "թՓՁՋՐՉՃԺԾՔՈԵՌՏԸՒԻՕՊԱՍԴՖԳՀՅԿԼԽԶՂՑՎԲՆմՇ"};

    std::vector<std::optional<String>> lower_case_strings
        = {"one week's time test",
           "abc测试def",
           "abcテストabc",
           "ѐёђѓєѕіїјљњћќѝўџ",
           "+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……љ&њ（ћ）ќ￥ѝ#ў@џ！^",
           "iasdiiiidds",
           "αβγδεζηθικλμνξοπρστυφχψωσ",
           "ⱥdⱥiⱥaⱥⱦoiⱥ",
           "test_wrong_utf8_1\x80\xe0\x21",
           "▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★σ✕",
           "ɱɑⱥωabcdefghijklmnopɥaⱦ",
           "test_wrong_utf8_2\xf1\x22",
           "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"};

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
