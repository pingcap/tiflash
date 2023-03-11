// Copyright 2022 PingCAP, Ltd.
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

    static ColumnWithTypeAndName toVec(const std::vector<String> & v)
    {
        return createColumn<String>(v);
    }

    static ColumnWithTypeAndName toConst(const String & s)
    {
        return createConstColumn<String>(1, s);
    }
};

TEST_F(StringUpper, upperAll)
{
    ASSERT_COLUMN_EQ(
        toNullableVec({"ONE WEEK’S TIME TEST", "ABC测试DEF", "ABCテストABC", "ЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏ", "+Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^", "ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩΣ", "▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕", "ԹՓՁՋՐՉՃԺԾՔՈԵՌՏԸՒԻՕՊԱՍԴՖԳՀՅԿԼԽԶՂՑՎԲՆՄՇ"}),
        executeFunction(
            "upperUTF8",
            toNullableVec({"one week’s time TEST", "abc测试DeF", "AbCテストAbC", "ѐёђѓєѕіїјЉЊЋЌЍЎЏ", "+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^", "αβγδεζηθικλμνξοπρστυφχψως", "▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★ς✕", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"})));

    ASSERT_COLUMN_EQ(
        toVec({"ONE WEEK’S TIME TEST", "ABC测试DEF", "ABCテストABC", "ЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏ", "+Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^", "ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩΣ", "▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕", "ԹՓՁՋՐՉՃԺԾՔՈԵՌՏԸՒԻՕՊԱՍԴՖԳՀՅԿԼԽԶՂՑՎԲՆՄՇ"}),
        executeFunction(
            "upperUTF8",
            toVec({"one week’s time TEST", "abc测试DeF", "AbCテストAbC", "ѐёђѓєѕіїјЉЊЋЌЍЎЏ", "+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^", "αβγδεζηθικλμνξοπρστυφχψως", "▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★ς✕", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"})));

    ASSERT_COLUMN_EQ(
        toNullableVec({"one week’s time TEST", "abc测试DeF", "AbCテストAbC", "ѐёђѓєѕіїјЉЊЋЌЍЎЏ", "αβγδεζηθικλμνξοπρστυφχψως", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"}),
        executeFunction(
            "upperBinary",
            toNullableVec({"one week’s time TEST", "abc测试DeF", "AbCテストAbC", "ѐёђѓєѕіїјЉЊЋЌЍЎЏ", "αβγδεζηθικλμνξοπρστυφχψως", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"})));

    ASSERT_COLUMN_EQ(
        toVec({"one week’s time TEST", "abc测试DeF", "AbCテストAbC", "ѐёђѓєѕіїјЉЊЋЌЍЎЏ", "αβγδεζηθικλμνξοπρστυφχψως", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"}),
        executeFunction(
            "upperBinary",
            toVec({"one week’s time TEST", "abc测试DeF", "AbCテストAbC", "ѐёђѓєѕіїјЉЊЋЌЍЎЏ", "αβγδεζηθικλμνξοπρστυφχψως", "թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"})));

    ASSERT_COLUMN_EQ(
        toConst("ONE WEEK’S TIME TEST"),
        executeFunction(
            "upperUTF8",
            toConst("one week’s time TEST")));

    ASSERT_COLUMN_EQ(
        toConst("+Ѐ-Ё*Ђ/Ѓ!Є@Ѕ#І$@Ї%Ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^"),
        executeFunction(
            "upperUTF8",
            toConst("+ѐ-ё*ђ/ѓ!є@ѕ#і$@ї%ј……Љ&Њ（Ћ）Ќ￥Ѝ#Ў@Џ！^")));

    ASSERT_COLUMN_EQ(
        toConst("▲Α▼ΒΓ➨ΔΕ☎ΖΗ✂ΘΙ€ΚΛ♫ΜΝ✓ΞΟ✚ΠΡ℉ΣΤ♥ΥΦ♖ΧΨ♘Ω★Σ✕"),
        executeFunction(
            "upperUTF8",
            toConst("▲α▼βγ➨δε☎ζη✂θι€κλ♫μν✓ξο✚πρ℉στ♥υφ♖χψ♘ω★ς✕")));

    ASSERT_COLUMN_EQ(
        toConst("թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ"),
        executeFunction(
            "upperBinary",
            toConst("թփձջրչճժծքոեռտըւիօպասդֆգհյկլխզղցվբնմշ")));
}


} // namespace DB::tests
