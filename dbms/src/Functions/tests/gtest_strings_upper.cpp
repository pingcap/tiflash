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

    static ColumnWithTypeAndName toConst(const String & s) { return createConstColumn<String>(1, s); }
};

TEST_F(StringUpper, upperAll)
{
    std::vector<std::optional<String>> candidate_strings
        = {"one week's time TEST",
           "abc测试DeF",
           "AbCテストAbC",
           "ѐёђѓєѕіїјЉЊЋЌЍЎЏ",
           "+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^",
           "ſⱥⱦⱥaſfɫoomɑɱɒ",
           "αβγδεζηθικλμνξοπρστυφχψως",
           "test_wrong_utf8_1\x80\xe0\x21",
           "▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★ς✕",
           "ȿɀabcdefghijklmnopɥı",
           "test_wrong_utf8_2\xf1\x22",
           "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"};

    std::vector<std::optional<String>> upper_case_strings
        = {"ONE WEEK'S TIME TEST",
           "ABC测试DEF",
           "ABCテストABC",
           "ЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏ",
           "+Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^",
           "SȺȾȺASFⱢOOMⱭⱮⱰ",
           "ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩΣ",
           "TEST_WRONG_UTF8_1\x80\xe0\x21",
           "▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕",
           "ⱾⱿABCDEFGHIJKLMNOPꞍI",
           "TEST_WRONG_UTF8_2\xf1\x22",
           "ԹՓՁՋՐՉՃԺԾՔՈԵՌՏԸՒԻՕՊԱՍԴՖԳՀՅԿԼԽԶՂՑՎԲՆՄՇ"};

    ASSERT_COLUMN_EQ(toNullableVec(upper_case_strings), executeFunction("upperUTF8", toNullableVec(candidate_strings)));

    ASSERT_COLUMN_EQ(toVec(upper_case_strings), executeFunction("upperUTF8", toVec(candidate_strings)));

    ASSERT_COLUMN_EQ(
        toNullableVec(candidate_strings),
        executeFunction("upperBinary", toNullableVec(candidate_strings)));

    ASSERT_COLUMN_EQ(toVec(candidate_strings), executeFunction("upperBinary", toVec(candidate_strings)));

    ASSERT_COLUMN_EQ(toConst("ONE WEEK’S TIME TEST"), executeFunction("upperUTF8", toConst("one week’s time TEST")));

    ASSERT_COLUMN_EQ(
        toConst("+Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^"),
        executeFunction("upperUTF8", toConst("+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^")));

    ASSERT_COLUMN_EQ(
        toConst("▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕"),
        executeFunction("upperUTF8", toConst("▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★ς✕")));

    ASSERT_COLUMN_EQ(
        toConst("թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"),
        executeFunction("upperBinary", toConst("թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ")));
}


} // namespace DB::tests
